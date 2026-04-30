use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use clap::Args;

use crate::adapters::br_models::{BeadStatus, ReadyBead};
use crate::adapters::br_process::{BrAdapter, BrCommand, BrMutationAdapter, OsProcessRunner};
use crate::adapters::fs::{FsJournalStore, FsProjectStore, FsRunSnapshotStore};
use crate::cli::run::{execute_resume, execute_start, execute_stop, RunBackendOverrideArgs};
use crate::contexts::bead_workflow::create_project::{
    create_project_from_bead, CreateProjectFromBeadInput, GitFeatureBranchPort,
    ProcessBeadProjectBrPort,
};
use crate::contexts::bead_workflow::drain::{
    drain_bead_queue, DrainCreatedProject, DrainError, DrainOptions, DrainPort, DrainPrOpenOutcome,
    DrainPrOpenOutput, DrainRunCompletion, DrainRunOutcome,
};
use crate::contexts::bead_workflow::drain_failure::{
    FailureObservation, FailureObservationKind, RecoveryAction, VerificationFailureSource,
};
use crate::contexts::bead_workflow::pr_open::{
    open_pr_for_completed_run, Gate, PrOpenError, PrOpenRequest, PrOpenStores, ProcessPrToolPort,
};
use crate::contexts::bead_workflow::pr_watch::{
    watch_pr, PrWatchClock, PrWatchRequest, WatchOutcome, DEFAULT_MAX_WAIT, DEFAULT_POLL_INTERVAL,
};
use crate::contexts::project_run_record::model::{
    JournalEvent, JournalEventType, RunSnapshot, RunStatus,
};
use crate::contexts::project_run_record::{JournalStorePort, RunSnapshotPort};
use crate::contexts::workspace_governance;
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::shared::domain::FailureClass;
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
#[command(about = "Drain the ready bead queue one bead at a time.")]
pub struct DrainCommand {
    #[arg(long = "max-cycles")]
    pub max_cycles: Option<u32>,
    #[arg(
        long = "stop-on-p0",
        default_value_t = true,
        conflicts_with = "no_stop_on_p0"
    )]
    pub stop_on_p0: bool,
    #[arg(long = "no-stop-on-p0")]
    pub no_stop_on_p0: bool,
    #[arg(long = "max-consecutive-failures", default_value_t = 3)]
    pub max_consecutive_failures: u32,
}

pub async fn handle(command: DrainCommand) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let mut ports = ProcessDrainPorts::new(current_dir);
    let report = drain_bead_queue(
        &mut ports,
        DrainOptions {
            max_cycles: command.max_cycles,
            stop_on_p0: command.stop_on_p0 && !command.no_stop_on_p0,
            max_consecutive_failures: command.max_consecutive_failures,
        },
    )
    .await
    .map_err(|error| AppError::DrainFailed {
        reason: error.to_string(),
    })?;

    for cycle in &report.cycles {
        println!("{}", cycle.render());
    }
    println!("{}", report.render_summary());
    Ok(())
}

struct ProcessDrainPorts {
    base_dir: PathBuf,
    active_project: Option<crate::shared::domain::ProjectId>,
    interrupted: Arc<AtomicBool>,
}

impl ProcessDrainPorts {
    fn new(base_dir: PathBuf) -> Self {
        let interrupted = Arc::new(AtomicBool::new(false));
        install_ctrl_c_handler(Arc::clone(&interrupted));
        Self {
            base_dir,
            active_project: None,
            interrupted,
        }
    }

    fn br_read(&self) -> BrAdapter<OsProcessRunner> {
        BrAdapter::with_runner(OsProcessRunner::new()).with_working_dir(self.base_dir.clone())
    }

    fn br_mutation(&self) -> BrMutationAdapter<OsProcessRunner> {
        BrMutationAdapter::with_adapter_id(self.br_read(), "drain-loop".to_owned())
    }

    fn project_id(&self) -> Result<&crate::shared::domain::ProjectId, DrainError> {
        self.active_project.as_ref().ok_or_else(|| {
            DrainError::operation("resolve active drain project", "no project created")
        })
    }

    fn active_run_snapshot(&self) -> Result<RunSnapshot, DrainError> {
        FsRunSnapshotStore
            .read_run_snapshot(&self.base_dir, self.project_id()?)
            .map_err(|error| DrainError::operation("read run snapshot", error.to_string()))
    }

    fn terminal_failure_observation(
        &self,
        bead_id: &str,
        snapshot: &RunSnapshot,
    ) -> Result<Option<FailureObservation>, DrainError> {
        let events = FsJournalStore
            .read_journal(&self.base_dir, self.project_id()?)
            .map_err(|error| DrainError::operation("read run journal", error.to_string()))?;
        Ok(failure_observation_from_journal(bead_id, snapshot, &events))
    }

    fn outcome_from_active_snapshot(
        &self,
        bead_id: &str,
        completed_pattern: &str,
        snapshot: &RunSnapshot,
        fallback: Option<FailureObservation>,
    ) -> Result<DrainRunOutcome, DrainError> {
        let journal_observation = if snapshot.status == RunStatus::Failed {
            self.terminal_failure_observation(bead_id, snapshot)?
        } else {
            None
        };
        Ok(outcome_from_run_snapshot(
            bead_id,
            completed_pattern,
            snapshot,
            journal_observation.or(fallback),
        ))
    }

    fn outcome_from_resume_error(
        &self,
        bead_id: &str,
        error: &AppError,
    ) -> Result<DrainRunOutcome, DrainError> {
        match self.active_run_snapshot() {
            Ok(snapshot) if snapshot.status == RunStatus::Completed => {
                self.outcome_from_active_snapshot(bead_id, "resumed", &snapshot, None)
            }
            Ok(snapshot) => self.outcome_from_active_snapshot(
                bead_id,
                "resumed",
                &snapshot,
                Some(observation_from_app_error(bead_id, error)),
            ),
            _ => Ok(DrainRunOutcome::Failed(observation_from_app_error(
                bead_id, error,
            ))),
        }
    }
}

impl DrainPort for ProcessDrainPorts {
    async fn sync_master_and_sanity_check(&mut self) -> Result<(), DrainError> {
        run_process(&self.base_dir, "git", &["fetch", "origin", "master"])?;
        run_process(
            &self.base_dir,
            "git",
            &["checkout", "-B", "master", "origin/master"],
        )?;
        run_process(&self.base_dir, "git", &["reset", "--hard", "origin/master"])?;
        run_process(&self.base_dir, "nix", &["build"])?;
        Ok(())
    }

    async fn sync_master_and_sanity_check_preserving_resume(
        &mut self,
        _bead_id: &str,
        _created_project: &DrainCreatedProject,
    ) -> Result<(), DrainError> {
        run_process(&self.base_dir, "git", &["fetch", "origin", "master"])?;
        run_process(
            &self.base_dir,
            "git",
            &["update-ref", "refs/heads/master", "FETCH_HEAD"],
        )?;

        let temp_dir = tempfile::tempdir()
            .map_err(|error| DrainError::operation("create temp worktree", error.to_string()))?;
        let worktree_path = temp_dir.path().join("master-sanity");
        let worktree_arg = worktree_path.to_string_lossy().into_owned();
        run_process(
            &self.base_dir,
            "git",
            &["worktree", "add", "--detach", &worktree_arg, "FETCH_HEAD"],
        )?;

        let build_result = run_process(&worktree_path, "nix", &["build"]);
        let cleanup_result = run_process(
            &self.base_dir,
            "git",
            &["worktree", "remove", "--force", &worktree_arg],
        );
        build_result?;
        cleanup_result?;
        Ok(())
    }

    async fn ready_beads(&mut self) -> Result<Vec<ReadyBead>, DrainError> {
        self.br_read()
            .exec_json(&BrCommand::ready())
            .await
            .map_err(|error| DrainError::operation("br ready", error.to_string()))
    }

    async fn bead_status(&mut self, bead_id: &str) -> Result<BeadStatus, DrainError> {
        let bead = self
            .br_read()
            .exec_json::<crate::adapters::br_models::BeadDetail>(&BrCommand::show(bead_id))
            .await
            .map_err(|error| DrainError::operation("br show", error.to_string()))?;
        Ok(bead.status)
    }

    async fn create_project_from_bead(
        &mut self,
        bead_id: &str,
    ) -> Result<DrainCreatedProject, DrainError> {
        let effective_config = EffectiveConfig::load(&self.base_dir)
            .map_err(|error| DrainError::operation("load config", error.to_string()))?;
        let output = create_project_from_bead(
            &FsProjectStore,
            &FsJournalStore,
            &ProcessBeadProjectBrPort::new(self.base_dir.clone()),
            &GitFeatureBranchPort,
            &self.base_dir,
            CreateProjectFromBeadInput {
                bead_id: bead_id.to_owned(),
                flow: effective_config.default_flow(),
                // Drain-created work must be on a feature branch so the PR
                // opener can squash and publish the completed run.
                branch: Some(String::new()),
                created_at: Utc::now(),
            },
        )
        .await
        .map_err(|error| DrainError::operation("create project from bead", error.to_string()))?;
        workspace_governance::set_active_project(&self.base_dir, &output.project.id)
            .map_err(|error| DrainError::operation("select created project", error.to_string()))?;
        self.active_project = Some(output.project.id);
        Ok(DrainCreatedProject {
            branch_name: output.branch_name,
        })
    }

    async fn restore_resume_context(
        &mut self,
        _bead_id: &str,
        created_project: &DrainCreatedProject,
    ) -> Result<(), DrainError> {
        if let Some(branch_name) = &created_project.branch_name {
            run_process(&self.base_dir, "git", &["switch", branch_name])?;
        }
        let project_id = self.project_id()?.clone();
        workspace_governance::set_active_project(&self.base_dir, &project_id)
            .map_err(|error| DrainError::operation("select resumed project", error.to_string()))?;
        Ok(())
    }

    async fn start_run(&mut self, bead_id: &str) -> Result<DrainRunOutcome, DrainError> {
        let result = execute_start(RunBackendOverrideArgs::default(), false).await;
        match result {
            Ok(()) => {
                let snapshot = self.active_run_snapshot()?;
                self.outcome_from_active_snapshot(bead_id, "completed", &snapshot, None)
            }
            Err(error) => match self.active_run_snapshot() {
                Ok(snapshot) if snapshot.status != RunStatus::Completed => self
                    .outcome_from_active_snapshot(
                        bead_id,
                        "completed",
                        &snapshot,
                        Some(observation_from_app_error(bead_id, &error)),
                    ),
                _ => Ok(DrainRunOutcome::Failed(observation_from_app_error(
                    bead_id, &error,
                ))),
            },
        }
    }

    async fn resume_run(&mut self, bead_id: &str) -> Result<DrainRunOutcome, DrainError> {
        let result = execute_resume(RunBackendOverrideArgs::default(), false).await;
        match result {
            Ok(()) => {
                let snapshot = self.active_run_snapshot()?;
                self.outcome_from_active_snapshot(bead_id, "resumed", &snapshot, None)
            }
            Err(error) => self.outcome_from_resume_error(bead_id, &error),
        }
    }

    async fn stop_run(&mut self, _bead_id: &str) -> Result<(), DrainError> {
        match execute_stop().await {
            Ok(()) => Ok(()),
            Err(AppError::RunStopFailed { reason })
                if reason.contains("project is not currently running") =>
            {
                Ok(())
            }
            Err(error) => Err(DrainError::operation("stop run", error.to_string())),
        }
    }

    async fn open_pr(&mut self, bead_id: &str) -> Result<DrainPrOpenOutcome, DrainError> {
        let project_id = self.project_id()?.clone();
        let output = match open_pr_for_completed_run(
            PrOpenRequest {
                base_dir: &self.base_dir,
                project_id: &project_id,
                bead_id_override: Some(bead_id),
                skip_gates: false,
            },
            PrOpenStores {
                project_store: &FsProjectStore,
                run_store: &FsRunSnapshotStore,
                journal_store: &FsJournalStore,
            },
            &self.br_read(),
            &ProcessPrToolPort,
        )
        .await
        {
            Ok(output) => output,
            Err(PrOpenError::GateFailed {
                gate,
                stderr_excerpt,
            }) => {
                let Some(gate) = gate_from_name(&gate) else {
                    return Err(DrainError::operation(
                        "classify PR gate failure",
                        format!("unknown gate '{gate}'"),
                    ));
                };
                return Ok(DrainPrOpenOutcome::Failed(FailureObservation::new(
                    FailureObservationKind::VerificationFailure {
                        source: VerificationFailureSource::Gate(gate),
                        failing: gate_failure_details(gate, &stderr_excerpt),
                        reruns_attempted_for_pr: 0,
                    },
                )));
            }
            Err(error) => return Err(DrainError::operation("open PR", error.to_string())),
        };
        let pr_number = parse_pr_number(&output.pr_url)
            .ok_or_else(|| DrainError::operation("parse PR number", output.pr_url.clone()))?;
        Ok(DrainPrOpenOutcome::Opened(DrainPrOpenOutput {
            pr_number,
            pr_url: output.pr_url,
        }))
    }

    async fn watch_pr(
        &mut self,
        _bead_id: &str,
        pr_number: u64,
    ) -> Result<WatchOutcome, DrainError> {
        watch_pr(
            PrWatchRequest {
                base_dir: &self.base_dir,
                pr_number,
                max_wait: DEFAULT_MAX_WAIT,
                poll_interval: DEFAULT_POLL_INTERVAL,
            },
            &ProcessPrToolPort,
            &InterruptiblePrWatchClock::started_now(
                Arc::clone(&self.interrupted),
                DEFAULT_MAX_WAIT,
            ),
        )
        .await
        .map_err(|error| DrainError::operation("watch PR", error.to_string()))
    }

    async fn prepare_bead_mutation_base(&mut self) -> Result<(), DrainError> {
        run_process(&self.base_dir, "git", &["fetch", "origin", "master"])?;
        run_process(&self.base_dir, "git", &["reset", "--hard"])?;
        run_process(
            &self.base_dir,
            "git",
            &["checkout", "-B", "master", "origin/master"],
        )?;
        run_process(&self.base_dir, "git", &["reset", "--hard", "origin/master"])?;
        Ok(())
    }

    async fn close_bead(&mut self, bead_id: &str, reason: &str) -> Result<(), DrainError> {
        let mutation = self.br_mutation();
        mutation
            .close_bead(bead_id, reason)
            .await
            .map_err(|error| DrainError::operation("close bead", error.to_string()))?;
        mutation
            .sync_own_dirty_if_beads_healthy(&self.base_dir)
            .await
            .map_err(|error| DrainError::operation("sync beads", error.to_string()))?;
        Ok(())
    }

    async fn persist_bead_mutations(&mut self) -> Result<(), DrainError> {
        if git_status_porcelain(&self.base_dir, &["--", ".beads"])?
            .trim()
            .is_empty()
        {
            return Ok(());
        }

        run_process(&self.base_dir, "git", &["add", ".beads/"])?;
        if !git_cached_beads_changed(&self.base_dir)? {
            return Ok(());
        }

        run_process(
            &self.base_dir,
            "git",
            &["commit", "-m", "sync beads after drain"],
        )?;
        run_process(&self.base_dir, "git", &["push", "origin", "HEAD:master"])?;
        Ok(())
    }

    async fn file_follow_up_bead(
        &mut self,
        bead_id: &str,
        action: &RecoveryAction,
        observation: &FailureObservation,
    ) -> Result<(), DrainError> {
        let mutation = self.br_mutation();
        let description = follow_up_description(bead_id, action, observation);
        mutation
            .create_bead(
                &format!("Drain follow-up for {bead_id}"),
                "bug",
                "1",
                &["drain-follow-up".to_owned()],
                Some(&description),
            )
            .await
            .map_err(|error| DrainError::operation("file follow-up bead", error.to_string()))?;
        mutation
            .sync_own_dirty_if_beads_healthy(&self.base_dir)
            .await
            .map_err(|error| DrainError::operation("sync beads", error.to_string()))?;
        Ok(())
    }

    fn should_interrupt(&self) -> bool {
        self.interrupted.load(Ordering::Relaxed)
    }
}

struct InterruptiblePrWatchClock {
    started_at: std::time::Instant,
    interrupted: Arc<AtomicBool>,
    max_wait: Duration,
}

impl InterruptiblePrWatchClock {
    fn started_now(interrupted: Arc<AtomicBool>, max_wait: Duration) -> Self {
        Self {
            started_at: std::time::Instant::now(),
            interrupted,
            max_wait,
        }
    }
}

impl PrWatchClock for InterruptiblePrWatchClock {
    fn elapsed(&self) -> Duration {
        if self.interrupted.load(Ordering::Relaxed) {
            self.max_wait
        } else {
            self.started_at.elapsed()
        }
    }

    async fn sleep(&self, duration: Duration) {
        tokio::select! {
            _ = tokio::time::sleep(duration) => {}
            _ = wait_until_interrupted(Arc::clone(&self.interrupted)) => {}
        }
    }
}

async fn wait_until_interrupted(interrupted: Arc<AtomicBool>) {
    while !interrupted.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn install_ctrl_c_handler(interrupted: Arc<AtomicBool>) {
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            interrupted.store(true, Ordering::Relaxed);
        }
    });
}

fn run_process(base_dir: &Path, program: &str, args: &[&str]) -> Result<(), DrainError> {
    let output = Command::new(program)
        .args(args)
        .current_dir(base_dir)
        .output()
        .map_err(|error| DrainError::operation("spawn process", error.to_string()))?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    Err(DrainError::operation(
        "process",
        format!(
            "{} {} failed with status {}: {}",
            program,
            args.join(" "),
            output.status,
            stderr
        ),
    ))
}

fn git_status_porcelain(base_dir: &Path, args: &[&str]) -> Result<String, DrainError> {
    let output = Command::new("git")
        .arg("status")
        .arg("--porcelain")
        .args(args)
        .current_dir(base_dir)
        .output()
        .map_err(|error| DrainError::operation("spawn process", error.to_string()))?;
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).into_owned());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    Err(DrainError::operation(
        "git status",
        format!(
            "git status --porcelain failed with status {}: {}",
            output.status, stderr
        ),
    ))
}

fn git_cached_beads_changed(base_dir: &Path) -> Result<bool, DrainError> {
    let output = Command::new("git")
        .args(["diff", "--cached", "--quiet", "--", ".beads"])
        .current_dir(base_dir)
        .output()
        .map_err(|error| DrainError::operation("spawn process", error.to_string()))?;
    match output.status.code() {
        Some(0) => Ok(false),
        Some(1) => Ok(true),
        _ => {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            Err(DrainError::operation(
                "git diff",
                format!(
                    "git diff --cached --quiet failed with status {}: {}",
                    output.status, stderr
                ),
            ))
        }
    }
}

fn parse_pr_number(pr_url: &str) -> Option<u64> {
    pr_url
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .and_then(|value| value.parse::<u64>().ok())
}

fn gate_from_name(name: &str) -> Option<Gate> {
    Gate::all().into_iter().find(|gate| gate.name() == name)
}

fn gate_failure_details(gate: Gate, stderr_excerpt: &str) -> Vec<String> {
    let excerpt = stderr_excerpt.trim();
    if excerpt.is_empty() {
        vec![format!("{} failed", gate.name())]
    } else {
        vec![format!("{} failed: {excerpt}", gate.name())]
    }
}

fn follow_up_description(
    bead_id: &str,
    action: &RecoveryAction,
    observation: &FailureObservation,
) -> String {
    format!(
        "Drain follow-up for bead `{bead_id}`.\n\nRecovery action: `{action:?}`.\n\nFailure observation:\n{}\n\n## Acceptance Criteria\n",
        render_failure_observation(observation)
    )
}

fn render_failure_observation(observation: &FailureObservation) -> String {
    match &observation.kind {
        FailureObservationKind::VerificationFailure {
            source,
            failing,
            reruns_attempted_for_pr,
        } => format!(
            "- kind: verification failure\n- source: {}\n- failing: {}\n- reruns attempted for PR: {}",
            render_verification_source(source),
            list_or_none(failing),
            reruns_attempted_for_pr
        ),
        FailureObservationKind::BotLineCommentsAfterLatestPush { comment_count } => format!(
            "- kind: bot line comments after latest push\n- comment count: {comment_count}"
        ),
        FailureObservationKind::BotRejectedReaction => "- kind: bot rejected reaction".to_owned(),
        FailureObservationKind::AmendmentOscillationLimitReached {
            completion_rounds,
            max_completion_rounds,
        } => format!(
            "- kind: amendment oscillation limit reached\n- completion rounds: {completion_rounds}\n- max completion rounds: {max_completion_rounds}"
        ),
        FailureObservationKind::SameBeadFailedTwiceInRow { bead_id } => format!(
            "- kind: same bead failed twice in a row\n- bead id: {bead_id}"
        ),
        FailureObservationKind::RunInterrupted { run_id } => {
            format!("- kind: run interrupted\n- run id: {run_id}")
        }
        FailureObservationKind::BackendFailure {
            bead_id,
            failure_class,
        } => format!(
            "- kind: backend failure\n- bead id: {bead_id}\n- failure class: {failure_class:?}"
        ),
    }
}

fn render_verification_source(source: &VerificationFailureSource) -> String {
    match source {
        VerificationFailureSource::Ci => "CI".to_owned(),
        VerificationFailureSource::Gate(gate) => format!("gate: {}", gate.name()),
    }
}

fn list_or_none(items: &[String]) -> String {
    if items.is_empty() {
        "none".to_owned()
    } else {
        items.join("; ")
    }
}

fn observation_from_app_error(bead_id: &str, error: &AppError) -> FailureObservation {
    let failure_class = error
        .failure_class()
        .unwrap_or(FailureClass::TransportFailure);
    if matches!(failure_class, FailureClass::Cancellation) {
        return interrupted_observation(bead_id);
    }
    FailureObservation::new(FailureObservationKind::BackendFailure {
        bead_id: bead_id.to_owned(),
        failure_class,
    })
}

fn interrupted_observation(bead_id: &str) -> FailureObservation {
    FailureObservation::new(FailureObservationKind::RunInterrupted {
        run_id: format!("interrupted:{bead_id}"),
    })
}

fn outcome_from_run_snapshot(
    bead_id: &str,
    completed_pattern: &str,
    snapshot: &RunSnapshot,
    failure_observation: Option<FailureObservation>,
) -> DrainRunOutcome {
    match snapshot.status {
        RunStatus::Completed => DrainRunOutcome::Completed(DrainRunCompletion {
            convergence_pattern: completed_pattern.to_owned(),
        }),
        RunStatus::Paused => DrainRunOutcome::Failed(FailureObservation::new(
            FailureObservationKind::RunInterrupted {
                run_id: snapshot
                    .interrupted_run
                    .as_ref()
                    .or(snapshot.active_run.as_ref())
                    .map(|run| run.run_id.clone())
                    .unwrap_or_else(|| format!("paused:{bead_id}")),
            },
        )),
        RunStatus::Running => DrainRunOutcome::Failed(failure_observation.unwrap_or_else(|| {
            FailureObservation::new(FailureObservationKind::BackendFailure {
                bead_id: bead_id.to_owned(),
                failure_class: FailureClass::Timeout,
            })
        })),
        RunStatus::Failed | RunStatus::NotStarted => {
            DrainRunOutcome::Failed(failure_observation.unwrap_or_else(|| {
                FailureObservation::new(FailureObservationKind::BackendFailure {
                    bead_id: bead_id.to_owned(),
                    failure_class: FailureClass::DomainValidationFailure,
                })
            }))
        }
    }
}

fn failure_observation_from_journal(
    bead_id: &str,
    snapshot: &RunSnapshot,
    events: &[JournalEvent],
) -> Option<FailureObservation> {
    let preferred_run_id = snapshot
        .interrupted_run
        .as_ref()
        .or(snapshot.active_run.as_ref())
        .map(|run| run.run_id.as_str());
    let mut latest_stage_failure = None;
    for event in events.iter().rev() {
        if !matches!(
            event.event_type,
            JournalEventType::RunFailed | JournalEventType::StageFailed
        ) {
            continue;
        }
        if !preferred_run_id.is_none_or(|run_id| {
            event.details.get("run_id").and_then(|value| value.as_str()) == Some(run_id)
        }) {
            continue;
        }

        if let Some(observation) =
            failure_observation_from_journal_event(bead_id, preferred_run_id, event)
        {
            match event.event_type {
                JournalEventType::RunFailed => return Some(observation),
                JournalEventType::StageFailed => latest_stage_failure.get_or_insert(observation),
                _ => unreachable!("filtered above"),
            };
        }
    }

    latest_stage_failure
}

fn failure_observation_from_journal_event(
    bead_id: &str,
    preferred_run_id: Option<&str>,
    event: &JournalEvent,
) -> Option<FailureObservation> {
    if let Some(observation) = amendment_oscillation_observation_from_journal_event(event) {
        return Some(observation);
    }
    let failure_class = event
        .details
        .get("failure_class")
        .and_then(|value| value.as_str())?;
    let run_id = event
        .details
        .get("run_id")
        .and_then(|value| value.as_str())
        .unwrap_or_else(|| preferred_run_id.unwrap_or("unknown-run"));
    observation_from_failure_class_label(bead_id, run_id, failure_class)
}

fn observation_from_failure_class_label(
    bead_id: &str,
    run_id: &str,
    failure_class: &str,
) -> Option<FailureObservation> {
    if matches!(failure_class, "cancellation" | "interruption") {
        return Some(FailureObservation::new(
            FailureObservationKind::RunInterrupted {
                run_id: run_id.to_owned(),
            },
        ));
    }

    let failure_class =
        serde_json::from_value::<FailureClass>(serde_json::Value::String(failure_class.to_owned()))
            .ok()?;
    Some(FailureObservation::new(
        FailureObservationKind::BackendFailure {
            bead_id: bead_id.to_owned(),
            failure_class,
        },
    ))
}

fn amendment_oscillation_observation_from_journal_event(
    event: &JournalEvent,
) -> Option<FailureObservation> {
    let (completion_rounds, max_completion_rounds) = event
        .details
        .get("completion_rounds_display")
        .and_then(|value| value.as_str())
        .and_then(parse_completion_round_pair)
        .or_else(|| {
            event
                .details
                .get("message")
                .and_then(|value| value.as_str())
                .and_then(parse_max_completion_rounds_message)
        })?;

    Some(FailureObservation::new(
        FailureObservationKind::AmendmentOscillationLimitReached {
            completion_rounds,
            max_completion_rounds,
        },
    ))
}

fn parse_max_completion_rounds_message(message: &str) -> Option<(u32, u32)> {
    const PREFIX: &str = "max completion rounds exceeded: ";
    let (_, remainder) = message.split_once(PREFIX)?;
    let token = remainder
        .split_whitespace()
        .next()?
        .trim_matches(|ch: char| !ch.is_ascii_digit() && ch != '/');
    parse_completion_round_pair(token)
}

fn parse_completion_round_pair(value: &str) -> Option<(u32, u32)> {
    let mut parts = value.split('/');
    let completion_rounds = parts.next()?.parse::<u32>().ok()?;
    let max_completion_rounds = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((completion_rounds, max_completion_rounds))
}

#[allow(dead_code)]
fn _duration_defaults() -> (Duration, Duration) {
    (DEFAULT_POLL_INTERVAL, DEFAULT_MAX_WAIT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pr_number_from_github_url() {
        assert_eq!(
            parse_pr_number("https://github.com/acme/repo/pull/123"),
            Some(123)
        );
        assert_eq!(
            parse_pr_number("https://github.com/acme/repo/pull/123/"),
            Some(123)
        );
        assert_eq!(parse_pr_number("not-a-pr"), None);
    }

    #[test]
    fn completed_snapshot_maps_to_completed_drain_run() {
        let mut snapshot = RunSnapshot::initial(3);
        snapshot.status = RunStatus::Completed;

        assert_eq!(
            outcome_from_run_snapshot("abc", "completed", &snapshot, None),
            DrainRunOutcome::Completed(DrainRunCompletion {
                convergence_pattern: "completed".to_owned()
            })
        );
    }

    #[test]
    fn paused_snapshot_maps_to_interrupted_failure_for_classifier() {
        let mut snapshot = RunSnapshot::initial(3);
        snapshot.status = RunStatus::Paused;

        assert_eq!(
            outcome_from_run_snapshot("abc", "completed", &snapshot, None),
            DrainRunOutcome::Failed(FailureObservation::new(
                FailureObservationKind::RunInterrupted {
                    run_id: "paused:abc".to_owned()
                }
            ))
        );
    }

    #[test]
    fn failed_snapshot_uses_journal_backend_failure_class_for_classifier() {
        let mut snapshot = RunSnapshot::initial(3);
        snapshot.status = RunStatus::Failed;
        let events = vec![JournalEvent {
            sequence: 1,
            timestamp: Utc::now(),
            event_type: JournalEventType::RunFailed,
            details: serde_json::json!({
                "run_id": "run-1",
                "stage_id": "implementation",
                "failure_class": "backend_exhausted",
                "message": "quota exhausted",
                "completion_rounds": 0,
                "max_completion_rounds": 3
            }),
        }];

        assert_eq!(
            outcome_from_run_snapshot(
                "abc",
                "completed",
                &snapshot,
                failure_observation_from_journal("abc", &snapshot, &events),
            ),
            DrainRunOutcome::Failed(FailureObservation::new(
                FailureObservationKind::BackendFailure {
                    bead_id: "abc".to_owned(),
                    failure_class: FailureClass::BackendExhausted,
                }
            ))
        );
    }

    #[test]
    fn failed_snapshot_falls_back_to_latest_stage_failure_class_for_classifier() {
        let mut snapshot = RunSnapshot::initial(3);
        snapshot.status = RunStatus::Failed;
        let events = vec![
            JournalEvent {
                sequence: 1,
                timestamp: Utc::now(),
                event_type: JournalEventType::StageFailed,
                details: serde_json::json!({
                    "run_id": "run-1",
                    "stage_id": "implementation",
                    "failure_class": "backend_exhausted",
                    "message": "quota exhausted",
                    "will_retry": false
                }),
            },
            JournalEvent {
                sequence: 2,
                timestamp: Utc::now(),
                event_type: JournalEventType::RunFailed,
                details: serde_json::json!({
                    "run_id": "run-1",
                    "stage_id": "implementation",
                    "failure_class": "stage_failure",
                    "message": "stage failed",
                    "completion_rounds": 0,
                    "max_completion_rounds": 3
                }),
            },
        ];

        assert_eq!(
            outcome_from_run_snapshot(
                "abc",
                "completed",
                &snapshot,
                failure_observation_from_journal("abc", &snapshot, &events),
            ),
            DrainRunOutcome::Failed(FailureObservation::new(
                FailureObservationKind::BackendFailure {
                    bead_id: "abc".to_owned(),
                    failure_class: FailureClass::BackendExhausted,
                }
            ))
        );
    }

    #[test]
    fn failed_run_with_max_completion_round_shape_maps_to_amendment_oscillation() {
        let mut snapshot = RunSnapshot::initial(3);
        snapshot.status = RunStatus::Failed;
        let events = vec![JournalEvent {
            sequence: 1,
            timestamp: Utc::now(),
            event_type: JournalEventType::RunFailed,
            details: serde_json::json!({
                "run_id": "run-1",
                "stage_id": "final_review",
                "failure_class": "unknown",
                "message": "stage commit failed for stage 'final_review': max completion rounds exceeded: 3/3",
                "completion_rounds": 3,
                "max_completion_rounds": 3,
                "completion_rounds_display": "3/3"
            }),
        }];

        assert_eq!(
            outcome_from_run_snapshot(
                "abc",
                "completed",
                &snapshot,
                failure_observation_from_journal("abc", &snapshot, &events),
            ),
            DrainRunOutcome::Failed(FailureObservation::new(
                FailureObservationKind::AmendmentOscillationLimitReached {
                    completion_rounds: 3,
                    max_completion_rounds: 3,
                }
            ))
        );
    }

    #[test]
    fn pending_resume_completed_snapshot_after_resume_error_maps_to_completed_drain_run() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let project_id =
            crate::shared::domain::ProjectId::new("resume-completed".to_owned()).unwrap();
        let project_root = temp_dir
            .path()
            .join(".ralph-burning")
            .join("projects")
            .join(project_id.as_str());
        std::fs::create_dir_all(&project_root).expect("create project root");

        let mut snapshot = RunSnapshot::initial(3);
        snapshot.status = RunStatus::Completed;
        std::fs::write(
            project_root.join("run.json"),
            serde_json::to_string_pretty(&snapshot).expect("serialize snapshot"),
        )
        .expect("write run snapshot");

        let ports = ProcessDrainPorts {
            base_dir: temp_dir.path().to_path_buf(),
            active_project: Some(project_id),
            interrupted: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        let error = AppError::ResumeFailed {
            reason: "project is already completed per journal; there is nothing to resume"
                .to_owned(),
        };

        assert_eq!(
            ports
                .outcome_from_resume_error("abc", &error)
                .expect("resume error outcome"),
            DrainRunOutcome::Completed(DrainRunCompletion {
                convergence_pattern: "resumed".to_owned(),
            })
        );
    }

    #[test]
    fn running_snapshot_preserves_original_error_failure_class_for_classifier() {
        let mut snapshot = RunSnapshot::initial(3);
        snapshot.status = RunStatus::Running;
        let observation = FailureObservation::new(FailureObservationKind::BackendFailure {
            bead_id: "abc".to_owned(),
            failure_class: FailureClass::BackendExhausted,
        });

        assert_eq!(
            outcome_from_run_snapshot("abc", "completed", &snapshot, Some(observation.clone())),
            DrainRunOutcome::Failed(observation)
        );
    }

    #[test]
    fn failed_snapshot_maps_journal_cancellation_to_run_interrupted() {
        let mut snapshot = RunSnapshot::initial(3);
        snapshot.status = RunStatus::Failed;
        let events = vec![JournalEvent {
            sequence: 1,
            timestamp: Utc::now(),
            event_type: JournalEventType::RunFailed,
            details: serde_json::json!({
                "run_id": "run-1",
                "stage_id": "implementation",
                "failure_class": "cancellation",
                "message": "interrupted",
                "completion_rounds": 0,
                "max_completion_rounds": 3
            }),
        }];

        assert_eq!(
            failure_observation_from_journal("abc", &snapshot, &events),
            Some(FailureObservation::new(
                FailureObservationKind::RunInterrupted {
                    run_id: "run-1".to_owned(),
                }
            ))
        );
    }
}
