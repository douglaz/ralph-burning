use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Args, Subcommand};
use notify::{Config as NotifyConfig, RecommendedWatcher, RecursiveMode, Watcher};
use serde::Serialize;
use serde_json::Value;

use crate::adapters::fs::{
    FsAmendmentQueueStore, FsArtifactStore, FsDaemonStore, FsJournalStore,
    FsPayloadArtifactWriteStore, FsProjectStore, FsRollbackPointStore, FsRunSnapshotStore,
    FsRunSnapshotWriteStore, FsRuntimeLogStore, FsRuntimeLogWriteStore,
};
use crate::adapters::worktree::WorktreeAdapter;
use crate::composition::agent_execution_builder;
use crate::contexts::automation_runtime::cli_writer_lease::{
    CliWriterLeaseGuard, CLI_LEASE_HEARTBEAT_CADENCE_SECONDS, CLI_LEASE_TTL_SECONDS,
};
use crate::contexts::project_run_record::model::RunStatus;
use crate::contexts::project_run_record::service::{
    self, ArtifactStorePort, JournalStorePort, ProjectStorePort, RunSnapshotPort,
    RuntimeLogStorePort,
};
use crate::contexts::workflow_composition::engine;
use crate::contexts::workspace_governance;
use crate::contexts::workspace_governance::config::{CliBackendOverrides, EffectiveConfig};
use crate::shared::domain::{BackendSelection, ExecutionMode, StageId};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Args)]
pub struct RunCommand {
    #[command(subcommand)]
    pub command: RunSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum RunSubcommand {
    Start(RunBackendOverrideArgs),
    Resume(RunBackendOverrideArgs),
    /// Attach to the active tmux-backed invocation for the selected project.
    Attach,
    /// Show canonical run status for the active project.
    Status {
        /// Emit a stable JSON object for scripts.
        #[arg(long)]
        json: bool,
    },
    /// Show durable run history for the active project.
    History {
        /// Include full event details, payload metadata, and artifact previews.
        #[arg(long)]
        verbose: bool,
        /// Emit a stable JSON object for scripts.
        #[arg(long)]
        json: bool,
        /// Filter events, payloads, and artifacts to a single stage.
        #[arg(long, value_name = "STAGE")]
        stage: Option<String>,
    },
    /// Show durable history tail, optionally including runtime logs.
    Tail {
        /// Include runtime log entries from the newest runtime log file.
        #[arg(long)]
        logs: bool,
        /// Limit output to the most recent N visible journal events.
        #[arg(long, value_name = "N", conflicts_with = "follow")]
        last: Option<usize>,
        /// Poll for new journal events every 2 seconds until interrupted.
        #[arg(long, conflicts_with = "last")]
        follow: bool,
        /// Test-only: delay in ms before follow-mode baseline snapshot.
        #[arg(long, hide = true, env = "RALPH_BURNING_TEST_FOLLOW_BASELINE_DELAY_MS")]
        follow_baseline_delay_ms: Option<u64>,
    },
    /// Show or perform run rollback operations.
    Rollback {
        /// List visible rollback targets instead of performing a rollback.
        #[arg(long, conflicts_with = "to")]
        list: bool,
        /// Roll back to the latest visible checkpoint for a stage.
        #[arg(long, required_unless_present = "list")]
        to: Option<String>,
        /// Also reset the repository to the rollback point git SHA.
        #[arg(long, requires = "to")]
        hard: bool,
    },
    /// Show a visible payload record by ID.
    ShowPayload {
        /// The payload ID to print as pretty JSON.
        payload_id: String,
    },
    /// Show a visible artifact record by ID.
    ShowArtifact {
        /// The artifact ID to print as rendered markdown.
        artifact_id: String,
    },
}

#[derive(Debug, Args, Clone, Default)]
pub struct RunBackendOverrideArgs {
    #[arg(long = "backend")]
    pub backend: Option<String>,
    #[arg(long = "planner-backend")]
    pub planner_backend: Option<String>,
    #[arg(long = "implementer-backend")]
    pub implementer_backend: Option<String>,
    #[arg(long = "reviewer-backend")]
    pub reviewer_backend: Option<String>,
    #[arg(long = "qa-backend")]
    pub qa_backend: Option<String>,
    #[arg(long = "execution-mode")]
    pub execution_mode: Option<String>,
    #[arg(long = "stream-output")]
    pub stream_output: Option<bool>,
}

pub async fn handle(command: RunCommand) -> AppResult<()> {
    match command.command {
        RunSubcommand::Status { json } => handle_status(json).await,
        RunSubcommand::History {
            verbose,
            json,
            stage,
        } => handle_history(verbose, json, stage).await,
        RunSubcommand::Tail {
            logs,
            last,
            follow,
            follow_baseline_delay_ms,
        } => handle_tail(logs, last, follow, follow_baseline_delay_ms).await,
        RunSubcommand::Start(args) => handle_start(args).await,
        RunSubcommand::Resume(args) => handle_resume(args).await,
        RunSubcommand::Attach => handle_attach().await,
        RunSubcommand::Rollback { list, to, hard } => handle_rollback(list, to, hard).await,
        RunSubcommand::ShowPayload { payload_id } => handle_show_payload(payload_id).await,
        RunSubcommand::ShowArtifact { artifact_id } => handle_show_artifact(artifact_id).await,
    }
}

const FOLLOW_POLL_INTERVAL: Duration = Duration::from_secs(2);
const FOLLOW_TRANSIENT_PARTIAL_PAIR_GRACE_PERIOD: Duration = Duration::from_secs(2);

async fn handle_start(overrides: RunBackendOverrideArgs) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    // Validate workspace version
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    // Resolve active project
    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    // Validate canonical project record
    let project_store = FsProjectStore;
    let project_record = project_store.read_project_record(&current_dir, &project_id)?;

    // Validate run snapshot integrity
    let run_snapshot_read = FsRunSnapshotStore;
    let run_snapshot = run_snapshot_read.read_run_snapshot(&current_dir, &project_id)?;

    // Check preconditions before engine call (fail fast with clear errors)
    match run_snapshot.status {
        RunStatus::NotStarted => {}
        RunStatus::Failed | RunStatus::Paused => {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "project snapshot status is '{}'; use `ralph-burning run resume`",
                    run_snapshot.status
                ),
            });
        }
        status => {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "project snapshot status is '{}'; run start requires 'not_started'",
                    status
                ),
            });
        }
    }
    if run_snapshot.has_active_run() {
        return Err(AppError::RunStartFailed {
            reason: "project already has an active run".to_owned(),
        });
    }

    // Acquire per-project writer lock with lease record before any run-state mutation
    let daemon_store: Arc<dyn crate::contexts::automation_runtime::DaemonStorePort + Send + Sync> =
        Arc::new(FsDaemonStore);
    let lock_guard = CliWriterLeaseGuard::acquire(
        Arc::clone(&daemon_store),
        &current_dir,
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )?;

    let cli_overrides = parse_cli_backend_overrides(&overrides)?;
    let effective_config =
        EffectiveConfig::load_for_project(&current_dir, Some(&project_id), cli_overrides)?;

    let agent_service =
        agent_execution_builder::build_agent_execution_service_for_config(&effective_config)?;
    let run_snapshot_write = FsRunSnapshotWriteStore;
    let journal_store = FsJournalStore;
    let artifact_write = FsPayloadArtifactWriteStore;
    let log_write = FsRuntimeLogWriteStore;

    println!("Starting run for project '{}'...", project_id);

    let amendment_queue = FsAmendmentQueueStore;

    engine::execute_run(
        &agent_service,
        &run_snapshot_read,
        &run_snapshot_write,
        &journal_store,
        &artifact_write,
        &log_write,
        &amendment_queue,
        &current_dir,
        &project_id,
        project_record.flow,
        &effective_config,
    )
    .await?;

    // Test-only injection seam: delete the writer lock file before close()
    // to exercise close-failure handling at the CLI level.
    if std::env::var("RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE").is_ok() {
        let lock_path = current_dir.join(format!(
            ".ralph-burning/daemon/leases/writer-{}.lock",
            project_id.as_str()
        ));
        let _ = std::fs::remove_file(&lock_path);
    }

    // Explicit guard shutdown before printing success — surfaces cleanup
    // failures as non-zero exit instead of silently succeeding.
    lock_guard.close()?;

    let final_snapshot = run_snapshot_read.read_run_snapshot(&current_dir, &project_id)?;
    match final_snapshot.status {
        RunStatus::Completed => println!("Run completed successfully."),
        RunStatus::Paused => println!("{}", final_snapshot.status_summary),
        status => println!("Run finished with status '{}'.", status),
    }
    Ok(())
}

async fn handle_resume(overrides: RunBackendOverrideArgs) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    let project_store = FsProjectStore;
    let project_record = project_store.read_project_record(&current_dir, &project_id)?;
    let run_snapshot_read = FsRunSnapshotStore;
    let run_snapshot = run_snapshot_read.read_run_snapshot(&current_dir, &project_id)?;
    match run_snapshot.status {
        RunStatus::Failed | RunStatus::Paused => {}
        RunStatus::NotStarted => {
            return Err(AppError::ResumeFailed {
                reason: "project has not started a run yet; use `ralph-burning run start`"
                    .to_owned(),
            });
        }
        RunStatus::Running => {
            return Err(AppError::ResumeFailed {
                reason: "project already has a running run; `run resume` only works from failed or paused snapshots".to_owned(),
            });
        }
        RunStatus::Completed => {
            return Err(AppError::ResumeFailed {
                reason: "project is already completed; there is nothing to resume".to_owned(),
            });
        }
    }
    if run_snapshot.has_active_run() {
        return Err(AppError::ResumeFailed {
            reason: "failed or paused snapshots must not retain an active run".to_owned(),
        });
    }

    // Acquire per-project writer lock with lease record before any run-state mutation
    let daemon_store: Arc<dyn crate::contexts::automation_runtime::DaemonStorePort + Send + Sync> =
        Arc::new(FsDaemonStore);
    let lock_guard = CliWriterLeaseGuard::acquire(
        Arc::clone(&daemon_store),
        &current_dir,
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )?;

    let cli_overrides = parse_cli_backend_overrides(&overrides)?;
    let effective_config =
        EffectiveConfig::load_for_project(&current_dir, Some(&project_id), cli_overrides)?;
    let agent_service =
        agent_execution_builder::build_agent_execution_service_for_config(&effective_config)?;
    let run_snapshot_write = FsRunSnapshotWriteStore;
    let journal_store = FsJournalStore;
    let artifact_write = FsPayloadArtifactWriteStore;
    let log_write = FsRuntimeLogWriteStore;

    println!("Resuming run for project '{}'...", project_id);

    let amendment_queue = FsAmendmentQueueStore;

    engine::resume_run(
        &agent_service,
        &run_snapshot_read,
        &run_snapshot_write,
        &journal_store,
        &FsArtifactStore,
        &artifact_write,
        &log_write,
        &amendment_queue,
        &current_dir,
        &project_id,
        project_record.flow,
        &effective_config,
    )
    .await?;

    // Explicit guard shutdown before printing success — surfaces cleanup
    // failures as non-zero exit instead of silently succeeding.
    lock_guard.close()?;

    let final_snapshot = run_snapshot_read.read_run_snapshot(&current_dir, &project_id)?;
    match final_snapshot.status {
        RunStatus::Completed => println!("Run completed successfully."),
        RunStatus::Paused => println!("{}", final_snapshot.status_summary),
        status => println!("Run finished with status '{}'.", status),
    }

    Ok(())
}

async fn handle_attach() -> AppResult<()> {
    let (current_dir, project_id) = load_active_project_context()?;
    let project_root = crate::adapters::fs::FileSystem::project_root(&current_dir, &project_id);
    let Some(active_session) =
        crate::adapters::tmux::TmuxAdapter::read_active_session(&project_root)?
    else {
        println!("No active tmux session exists for the current invocation.");
        return Ok(());
    };

    crate::adapters::tmux::TmuxAdapter::check_tmux_available()?;
    if !crate::adapters::tmux::TmuxAdapter::session_exists(&active_session.session_name)? {
        crate::adapters::tmux::TmuxAdapter::clear_active_session(
            &project_root,
            &active_session.invocation_id,
        )?;
        println!("No active tmux session exists for the current invocation.");
        return Ok(());
    }

    println!(
        "Attaching to tmux session '{}'. Detach with Ctrl-b d.",
        active_session.session_name
    );
    crate::adapters::tmux::TmuxAdapter::attach_to_session(&active_session.session_name)
}

async fn handle_status(as_json: bool) -> AppResult<()> {
    let (current_dir, project_id) = load_active_project_context()?;

    if as_json {
        let status = service::run_status_json(&FsRunSnapshotStore, &current_dir, &project_id)?;
        println!("{}", format_json_status(&status)?);
        return Ok(());
    }

    let status = service::run_status(&FsRunSnapshotStore, &current_dir, &project_id)?;
    println!("Project: {}", status.project_id);
    println!("Status: {}", status.status);
    if let Some(ref stage) = status.stage {
        println!("Stage: {}", stage);
    }
    if let Some(cycle) = status.cycle {
        println!("Cycle: {}", cycle);
    }
    if let Some(round) = status.completion_round {
        println!("Completion round: {}", round);
    }
    println!("Summary: {}", status.summary);

    Ok(())
}

async fn handle_history(verbose: bool, as_json: bool, stage: Option<String>) -> AppResult<()> {
    let (current_dir, project_id) = load_active_project_context()?;
    let history =
        service::run_history(&FsJournalStore, &FsArtifactStore, &current_dir, &project_id)?;
    let history = maybe_filter_history_by_stage(history, stage)?;

    if as_json {
        println!("{}", format_json_history(&history, verbose)?);
    } else {
        print_history_text(&history, verbose);
    }

    Ok(())
}

async fn handle_tail(
    include_logs: bool,
    last: Option<usize>,
    follow: bool,
    follow_baseline_delay_ms: Option<u64>,
) -> AppResult<()> {
    let (current_dir, project_id) = load_active_project_context()?;
    let effective_config = EffectiveConfig::load_for_project(
        &current_dir,
        Some(&project_id),
        CliBackendOverrides::default(),
    )?;

    if follow {
        return handle_tail_follow(
            &current_dir,
            &project_id,
            include_logs,
            effective_config.effective_stream_output(),
            follow_baseline_delay_ms,
        )
        .await;
    }

    let tail = service::run_tail(
        &FsJournalStore,
        &FsArtifactStore,
        &FsRuntimeLogStore,
        &current_dir,
        &project_id,
        include_logs,
    )?;
    let tail = if let Some(count) = last {
        let (events, payloads, artifacts) =
            crate::contexts::project_run_record::queries::tail_last_n(
                &tail.events,
                &tail.payloads,
                &tail.artifacts,
                count,
            );
        crate::contexts::project_run_record::queries::build_tail_view(
            &tail.project_id,
            events,
            payloads,
            artifacts,
            include_logs,
            tail.runtime_logs.clone().unwrap_or_default(),
        )
    } else {
        tail
    };

    print_tail_text(&tail);
    Ok(())
}

async fn handle_tail_follow(
    current_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
    include_logs: bool,
    stream_output: bool,
    follow_baseline_delay_ms: Option<u64>,
) -> AppResult<()> {
    let mut follow_state = load_follow_baseline(
        current_dir,
        project_id,
        include_logs,
        follow_baseline_delay_ms,
    )?;

    println!(
        "Following project '{}' for new durable history{}; press Ctrl-C to stop.",
        project_id,
        if include_logs {
            " and runtime logs"
        } else {
            ""
        }
    );

    let mut watcher = if include_logs && stream_output {
        build_follow_watcher(current_dir, project_id)?
    } else {
        None
    };

    if watcher.is_some() {
        render_follow_delta(current_dir, project_id, include_logs, &mut follow_state)?;
    }

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result?;
                println!("Stopped following.");
                return Ok(());
            }
            maybe_event = async {
                match &mut watcher {
                    Some(watcher) => watcher.rx.recv().await,
                    None => std::future::pending::<Option<notify::Event>>().await,
                }
            } => {
                if maybe_event.is_none() {
                    watcher = None;
                    continue;
                }
                render_follow_delta(
                    current_dir,
                    project_id,
                    include_logs,
                    &mut follow_state,
                )?;
            }
            _ = tokio::time::sleep(FOLLOW_POLL_INTERVAL) => {
                render_follow_delta(
                    current_dir,
                    project_id,
                    include_logs,
                    &mut follow_state,
                )?;
            }
        }
    }
}

struct FollowState {
    last_seen_sequence: u64,
    last_runtime_log_count: usize,
    seen_payload_files: HashSet<String>,
    seen_artifact_files: HashSet<String>,
    visible_payload_files: HashSet<String>,
    visible_artifact_files: HashSet<String>,
    transient_partial_history_files: HashMap<String, Instant>,
}

struct FollowSnapshot {
    tail: crate::contexts::project_run_record::queries::RunTailView,
    visible_payload_files: HashSet<String>,
    visible_artifact_files: HashSet<String>,
}

fn load_follow_baseline(
    current_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
    include_logs: bool,
    follow_baseline_delay_ms: Option<u64>,
) -> AppResult<FollowState> {
    let mut transient_partial_history_files = HashMap::new();
    let (seen_payload_files, seen_artifact_files) =
        list_history_record_files(current_dir, project_id)?;
    maybe_sleep_before_follow_baseline_snapshot(follow_baseline_delay_ms);
    let previously_visible_payload_files = HashSet::new();
    let previously_visible_artifact_files = HashSet::new();
    let FollowSnapshot {
        tail,
        visible_payload_files,
        visible_artifact_files,
    } = load_follow_snapshot_resilient(
        current_dir,
        project_id,
        include_logs,
        &previously_visible_payload_files,
        &previously_visible_artifact_files,
        &mut transient_partial_history_files,
    )?;
    let (payload_files, artifact_files) = list_history_record_files(current_dir, project_id)?;
    retain_pending_partial_history_files(
        &mut transient_partial_history_files,
        &payload_files,
        &artifact_files,
        &visible_payload_files,
        &visible_artifact_files,
    );

    Ok(FollowState {
        last_seen_sequence: tail.events.last().map(|event| event.sequence).unwrap_or(0),
        last_runtime_log_count: tail.runtime_logs.as_ref().map_or(0, std::vec::Vec::len),
        // Seed file diffs from the raw pre-follow directory snapshot so records
        // that land between startup scanning and the first strict tail read are
        // still surfaced on the first follow delta.
        seen_payload_files,
        seen_artifact_files,
        visible_payload_files,
        visible_artifact_files,
        transient_partial_history_files,
    })
}

async fn handle_show_payload(payload_id: String) -> AppResult<()> {
    let (current_dir, project_id) = load_active_project_context()?;
    let payload = service::get_payload_by_id(
        &FsJournalStore,
        &FsArtifactStore,
        &current_dir,
        &project_id,
        &payload_id,
    )?;
    println!("{}", serde_json::to_string_pretty(&payload.payload)?);
    Ok(())
}

async fn handle_show_artifact(artifact_id: String) -> AppResult<()> {
    let (current_dir, project_id) = load_active_project_context()?;
    let artifact = service::get_artifact_by_id(
        &FsJournalStore,
        &FsArtifactStore,
        &current_dir,
        &project_id,
        &artifact_id,
    )?;
    println!("{}", artifact.content);
    Ok(())
}

async fn handle_rollback(list: bool, target: Option<String>, hard: bool) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;

    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let project_id = workspace_governance::resolve_active_project(&current_dir)?;

    let project_store = FsProjectStore;
    let project_record = project_store.read_project_record(&current_dir, &project_id)?;

    if list {
        return handle_rollback_list(&current_dir, &project_id).await;
    }

    let target = target.expect("clap enforces rollback target when --list is absent");
    let target_stage = match target.parse::<StageId>() {
        Ok(stage_id) => stage_id,
        Err(_) => {
            return Err(AppError::RollbackStageNotInFlow {
                project_id: project_id.to_string(),
                stage_id: target,
                flow: project_record.flow.to_string(),
            });
        }
    };

    let rollback_point = service::perform_rollback(
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsRollbackPointStore,
        Some(&WorktreeAdapter),
        &current_dir,
        &project_id,
        project_record.flow,
        target_stage,
        hard,
    )?;

    println!(
        "Rollback complete: project '{}' paused at {} cycle {}.",
        project_id, rollback_point.stage_id, rollback_point.cycle
    );
    if hard {
        println!("Repository reset to recorded git SHA.");
    }

    Ok(())
}

async fn handle_rollback_list(
    current_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
) -> AppResult<()> {
    let targets = service::list_rollback_targets(
        &FsRollbackPointStore,
        &FsJournalStore,
        current_dir,
        project_id,
    )?;

    if targets.is_empty() {
        println!("No rollback targets available.");
        return Ok(());
    }

    println!(
        "{:<24} {:<20} {:<5} {:<25} Git SHA",
        "Rollback ID", "Stage", "Cycle", "Created At"
    );
    for target in targets {
        println!(
            "{:<24} {:<20} {:<5} {:<25} {}",
            target.rollback_id,
            target.stage_id,
            target.cycle,
            target.created_at.to_rfc3339(),
            target.git_sha.unwrap_or_else(|| "-".to_owned()),
        );
    }

    Ok(())
}

fn load_active_project_context() -> AppResult<(std::path::PathBuf, crate::shared::domain::ProjectId)>
{
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let project_id = workspace_governance::resolve_active_project(&current_dir)?;
    let project_store = FsProjectStore;
    let _ = project_store.read_project_record(&current_dir, &project_id)?;

    Ok((current_dir, project_id))
}

struct FollowWatcher {
    _watcher: RecommendedWatcher,
    rx: tokio::sync::mpsc::UnboundedReceiver<notify::Event>,
}

fn build_follow_watcher(
    current_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
) -> AppResult<Option<FollowWatcher>> {
    let project_root = current_dir
        .join(".ralph-burning/projects")
        .join(project_id.as_str());
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let callback = move |result: Result<notify::Event, notify::Error>| {
        if let Ok(event) = result {
            let _ = tx.send(event);
        }
    };

    let mut watcher = match RecommendedWatcher::new(callback, NotifyConfig::default()) {
        Ok(watcher) => watcher,
        Err(_) => return Ok(None),
    };

    if watcher
        .watch(&project_root, RecursiveMode::Recursive)
        .is_err()
    {
        return Ok(None);
    }

    Ok(Some(FollowWatcher {
        _watcher: watcher,
        rx,
    }))
}

fn render_follow_delta(
    current_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
    include_logs: bool,
    follow_state: &mut FollowState,
) -> AppResult<()> {
    let FollowSnapshot {
        tail,
        visible_payload_files,
        visible_artifact_files,
    } = load_follow_snapshot_resilient(
        current_dir,
        project_id,
        include_logs,
        &follow_state.visible_payload_files,
        &follow_state.visible_artifact_files,
        &mut follow_state.transient_partial_history_files,
    )?;
    let (payload_files, artifact_files) = list_history_record_files(current_dir, project_id)?;
    let new_payload_files: HashSet<_> = visible_payload_files
        .difference(&follow_state.seen_payload_files)
        .cloned()
        .collect();
    let new_artifact_files: HashSet<_> = visible_artifact_files
        .difference(&follow_state.seen_artifact_files)
        .cloned()
        .collect();
    let newly_visible_payload_files: HashSet<_> = visible_payload_files
        .difference(&follow_state.visible_payload_files)
        .cloned()
        .collect();
    let newly_visible_artifact_files: HashSet<_> = visible_artifact_files
        .difference(&follow_state.visible_artifact_files)
        .cloned()
        .collect();
    let new_event_count = tail
        .events
        .iter()
        .filter(|event| event.sequence > follow_state.last_seen_sequence)
        .count();
    let (events, event_payloads, event_artifacts) =
        crate::contexts::project_run_record::queries::tail_last_n(
            &tail.events,
            &tail.payloads,
            &tail.artifacts,
            new_event_count,
        );
    let mut artifact_ids: HashSet<_> = event_artifacts
        .iter()
        .map(|artifact| artifact.artifact_id.clone())
        .collect();
    artifact_ids.extend(new_artifact_files.iter().cloned());
    artifact_ids.extend(newly_visible_artifact_files.iter().cloned());
    let artifacts = tail
        .artifacts
        .iter()
        .filter(|artifact| artifact_ids.contains(artifact.artifact_id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let mut payload_ids: HashSet<_> = event_payloads
        .iter()
        .map(|payload| payload.payload_id.clone())
        .collect();
    payload_ids.extend(new_payload_files.iter().cloned());
    payload_ids.extend(newly_visible_payload_files.iter().cloned());
    payload_ids.extend(artifacts.iter().map(|artifact| artifact.payload_id.clone()));
    let payloads = tail
        .payloads
        .iter()
        .filter(|payload| payload_ids.contains(payload.payload_id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    let new_logs = match tail.runtime_logs.as_ref() {
        Some(logs) if logs.len() >= follow_state.last_runtime_log_count => {
            logs[follow_state.last_runtime_log_count..].to_vec()
        }
        Some(logs) => logs.clone(),
        None => Vec::new(),
    };

    if !events.is_empty() || !payloads.is_empty() || !artifacts.is_empty() || !new_logs.is_empty() {
        print_follow_update(&events, &payloads, &artifacts, &new_logs);
    }

    follow_state.last_seen_sequence = tail
        .events
        .last()
        .map(|event| event.sequence)
        .unwrap_or(follow_state.last_seen_sequence);
    follow_state.last_runtime_log_count = tail.runtime_logs.as_ref().map_or(0, std::vec::Vec::len);
    follow_state.seen_payload_files = payload_files;
    follow_state.seen_artifact_files = artifact_files;
    follow_state.visible_payload_files = visible_payload_files;
    follow_state.visible_artifact_files = visible_artifact_files;
    retain_pending_partial_history_files(
        &mut follow_state.transient_partial_history_files,
        &follow_state.seen_payload_files,
        &follow_state.seen_artifact_files,
        &follow_state.visible_payload_files,
        &follow_state.visible_artifact_files,
    );
    Ok(())
}

struct FollowHistory {
    events: Vec<crate::contexts::project_run_record::model::JournalEvent>,
    payloads: Vec<crate::contexts::project_run_record::model::PayloadRecord>,
    artifacts: Vec<crate::contexts::project_run_record::model::ArtifactRecord>,
    runtime_logs: Vec<crate::contexts::project_run_record::model::RuntimeLogEntry>,
}

struct TransientPartialHistoryIds {
    payload_ids: HashSet<String>,
    artifact_ids: HashSet<String>,
}

fn load_follow_snapshot_resilient(
    current_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
    include_logs: bool,
    visible_payload_files: &HashSet<String>,
    visible_artifact_files: &HashSet<String>,
    transient_partial_history_files: &mut HashMap<String, Instant>,
) -> AppResult<FollowSnapshot> {
    match load_follow_snapshot_strict(current_dir, project_id, include_logs) {
        Ok(snapshot) => Ok(snapshot),
        Err(error) => load_follow_snapshot_for_transient_partial_pair(
            current_dir,
            project_id,
            include_logs,
            visible_payload_files,
            visible_artifact_files,
            transient_partial_history_files,
            error,
        ),
    }
}

fn load_follow_snapshot_strict(
    current_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
    include_logs: bool,
) -> AppResult<FollowSnapshot> {
    let tail = service::run_tail(
        &FsJournalStore,
        &FsArtifactStore,
        &FsRuntimeLogStore,
        current_dir,
        project_id,
        include_logs,
    )?;

    Ok(FollowSnapshot {
        visible_payload_files: tail
            .payloads
            .iter()
            .map(|payload| payload.payload_id.clone())
            .collect(),
        visible_artifact_files: tail
            .artifacts
            .iter()
            .map(|artifact| artifact.artifact_id.clone())
            .collect(),
        tail,
    })
}

fn load_follow_snapshot_for_transient_partial_pair(
    current_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
    include_logs: bool,
    visible_payload_files: &HashSet<String>,
    visible_artifact_files: &HashSet<String>,
    transient_partial_history_files: &mut HashMap<String, Instant>,
    error: AppError,
) -> AppResult<FollowSnapshot> {
    if partial_history_pair_error_file(&error).is_none() {
        return Err(error);
    }

    let history = load_follow_history(current_dir, project_id, include_logs)?;
    match crate::contexts::project_run_record::queries::validate_history_consistency(
        &history.payloads,
        &history.artifacts,
    ) {
        Ok(()) => Ok(build_follow_snapshot_from_history(
            project_id,
            include_logs,
            history,
        )),
        Err(current_error) => {
            let Some(error_file) = partial_history_pair_error_file(&current_error) else {
                return Err(current_error);
            };
            let transient_ids = collect_transient_partial_history_ids(
                &history.payloads,
                &history.artifacts,
                visible_payload_files,
                visible_artifact_files,
                transient_partial_history_files,
            );
            let transient_files = transient_partial_history_file_keys(&transient_ids);
            if !transient_files.contains(&error_file) {
                return Err(current_error);
            }

            let now = Instant::now();
            for file in &transient_files {
                transient_partial_history_files
                    .entry(file.clone())
                    .or_insert(now);
            }
            if transient_files.iter().any(|file| {
                transient_partial_history_files
                    .get(file)
                    .is_some_and(|seen_at| {
                        now.duration_since(*seen_at) >= FOLLOW_TRANSIENT_PARTIAL_PAIR_GRACE_PERIOD
                    })
            }) {
                return Err(current_error);
            }

            // Follow-mode still validates against the canonical record set first.
            // Only files that have never been visible in a successful snapshot,
            // or are already inside the transient-write grace window, are
            // filtered out. This covers pairs that straddle follow startup and
            // pairs that are still mid-write after follow has already started.
            let payloads = history
                .payloads
                .into_iter()
                .filter(|payload| {
                    !transient_ids
                        .payload_ids
                        .contains(payload.payload_id.as_str())
                })
                .collect::<Vec<_>>();
            let artifacts = history
                .artifacts
                .into_iter()
                .filter(|artifact| {
                    !transient_ids
                        .artifact_ids
                        .contains(artifact.artifact_id.as_str())
                })
                .collect::<Vec<_>>();
            crate::contexts::project_run_record::queries::validate_history_consistency(
                &payloads, &artifacts,
            )?;

            Ok(build_follow_snapshot_from_history(
                project_id,
                include_logs,
                FollowHistory {
                    events: history.events,
                    payloads,
                    artifacts,
                    runtime_logs: history.runtime_logs,
                },
            ))
        }
    }
}

fn load_follow_history(
    current_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
    include_logs: bool,
) -> AppResult<FollowHistory> {
    let events = crate::contexts::project_run_record::queries::visible_journal_events(
        &FsJournalStore.read_journal(current_dir, project_id)?,
    )?;
    let (payloads, artifacts) =
        crate::contexts::project_run_record::queries::filter_history_records(
            &events,
            FsArtifactStore.list_payloads(current_dir, project_id)?,
            FsArtifactStore.list_artifacts(current_dir, project_id)?,
        )?;
    let runtime_logs = if include_logs {
        FsRuntimeLogStore.read_runtime_logs(current_dir, project_id)?
    } else {
        Vec::new()
    };

    Ok(FollowHistory {
        events,
        payloads,
        artifacts,
        runtime_logs,
    })
}

fn build_follow_snapshot_from_history(
    project_id: &crate::shared::domain::ProjectId,
    include_logs: bool,
    history: FollowHistory,
) -> FollowSnapshot {
    let visible_payload_files = history
        .payloads
        .iter()
        .map(|payload| payload.payload_id.clone())
        .collect();
    let visible_artifact_files = history
        .artifacts
        .iter()
        .map(|artifact| artifact.artifact_id.clone())
        .collect();

    FollowSnapshot {
        tail: crate::contexts::project_run_record::queries::build_tail_view(
            project_id.as_str(),
            history.events,
            history.payloads,
            history.artifacts,
            include_logs,
            history.runtime_logs,
        ),
        visible_payload_files,
        visible_artifact_files,
    }
}

fn list_history_record_files(
    current_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
) -> AppResult<(HashSet<String>, HashSet<String>)> {
    let project_root = current_dir
        .join(".ralph-burning/projects")
        .join(project_id.as_str());
    Ok((
        list_json_file_stems(&project_root.join("history/payloads"))?,
        list_json_file_stems(&project_root.join("history/artifacts"))?,
    ))
}

fn list_json_file_stems(dir: &std::path::Path) -> AppResult<HashSet<String>> {
    if !dir.is_dir() {
        return Ok(HashSet::new());
    }

    let mut stems = HashSet::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            // History records are persisted as `<record_id>.json`, so the stem is
            // the durable identifier used for follow-mode file diffs.
            if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
                stems.insert(stem.to_owned());
            }
        }
    }

    Ok(stems)
}

fn maybe_sleep_before_follow_baseline_snapshot(delay_ms: Option<u64>) {
    // Test-only injection seam: pause between the startup file scan and the
    // first strict tail snapshot so integration coverage can exercise records
    // that land in that narrow window.  The delay is provided via a hidden
    // clap arg (with env fallback) at the CLI boundary.
    if let Some(delay_ms) = delay_ms {
        std::thread::sleep(Duration::from_millis(delay_ms));
    }
}

fn partial_history_pair_error_file(error: &AppError) -> Option<String> {
    match error {
        AppError::CorruptRecord { file, details } if is_partial_history_pair_details(details) => {
            normalize_history_file_key(file)
        }
        _ => None,
    }
}

fn is_partial_history_pair_details(details: &str) -> bool {
    details == "payload has no matching artifact"
        || (details.contains("artifact references payload") && details.contains("does not exist"))
}

fn normalize_history_file_key(file: &str) -> Option<String> {
    let normalized = file.strip_suffix(".json").unwrap_or(file);
    if normalized.starts_with("history/payloads/") || normalized.starts_with("history/artifacts/") {
        Some(normalized.to_owned())
    } else {
        None
    }
}

fn collect_transient_partial_history_ids(
    payloads: &[crate::contexts::project_run_record::model::PayloadRecord],
    artifacts: &[crate::contexts::project_run_record::model::ArtifactRecord],
    visible_payload_files: &HashSet<String>,
    visible_artifact_files: &HashSet<String>,
    transient_partial_history_files: &HashMap<String, Instant>,
) -> TransientPartialHistoryIds {
    // Classify transient partial pairs from the history snapshot we just loaded.
    // A file stays transient while it has never been visible in a successful
    // follow snapshot, or while it is already inside the bounded grace window.
    let payload_ids_with_artifacts: HashSet<_> = artifacts
        .iter()
        .map(|artifact| artifact.payload_id.as_str())
        .collect();
    let snapshot_payload_ids: HashSet<_> = payloads
        .iter()
        .map(|payload| payload.payload_id.as_str())
        .collect();

    TransientPartialHistoryIds {
        payload_ids: payloads
            .iter()
            .filter(|payload| {
                !payload_ids_with_artifacts.contains(payload.payload_id.as_str())
                    && (!visible_payload_files.contains(payload.payload_id.as_str())
                        || transient_partial_history_files
                            .contains_key(&format!("history/payloads/{}", payload.payload_id)))
            })
            .map(|payload| payload.payload_id.clone())
            .collect(),
        artifact_ids: artifacts
            .iter()
            .filter(|artifact| {
                !snapshot_payload_ids.contains(artifact.payload_id.as_str())
                    && (!visible_artifact_files.contains(artifact.artifact_id.as_str())
                        || transient_partial_history_files
                            .contains_key(&format!("history/artifacts/{}", artifact.artifact_id)))
            })
            .map(|artifact| artifact.artifact_id.clone())
            .collect(),
    }
}

fn transient_partial_history_file_keys(
    transient_ids: &TransientPartialHistoryIds,
) -> HashSet<String> {
    let mut files = transient_ids
        .payload_ids
        .iter()
        .map(|payload_id| format!("history/payloads/{payload_id}"))
        .collect::<HashSet<_>>();
    files.extend(
        transient_ids
            .artifact_ids
            .iter()
            .map(|artifact_id| format!("history/artifacts/{artifact_id}")),
    );
    files
}

fn retain_pending_partial_history_files(
    transient_partial_history_files: &mut HashMap<String, Instant>,
    payload_files: &HashSet<String>,
    artifact_files: &HashSet<String>,
    visible_payload_files: &HashSet<String>,
    visible_artifact_files: &HashSet<String>,
) {
    transient_partial_history_files.retain(|file, _| {
        let normalized = file.strip_suffix(".json").unwrap_or(file.as_str());
        if let Some(payload_id) = normalized.strip_prefix("history/payloads/") {
            payload_files.contains(payload_id) && !visible_payload_files.contains(payload_id)
        } else if let Some(artifact_id) = normalized.strip_prefix("history/artifacts/") {
            artifact_files.contains(artifact_id) && !visible_artifact_files.contains(artifact_id)
        } else {
            false
        }
    });
}

fn maybe_filter_history_by_stage(
    history: crate::contexts::project_run_record::queries::RunHistoryView,
    stage: Option<String>,
) -> AppResult<crate::contexts::project_run_record::queries::RunHistoryView> {
    let Some(stage) = stage else {
        return Ok(history);
    };
    let stage_id = stage.parse::<StageId>()?;
    let (events, payloads, artifacts) =
        crate::contexts::project_run_record::queries::filter_by_stage(
            &history.events,
            &history.payloads,
            &history.artifacts,
            stage_id,
        );

    Ok(
        crate::contexts::project_run_record::queries::build_history_view(
            &history.project_id,
            events,
            payloads,
            artifacts,
        ),
    )
}

fn format_json_status(
    status: &crate::contexts::project_run_record::queries::RunStatusJsonView,
) -> AppResult<String> {
    Ok(serde_json::to_string_pretty(status)?)
}

fn format_json_history(
    history: &crate::contexts::project_run_record::queries::RunHistoryView,
    verbose: bool,
) -> AppResult<String> {
    #[derive(Serialize)]
    struct HistoryPayloadJsonView {
        payload_id: String,
        stage_id: String,
        cycle: u32,
        attempt: u32,
        created_at: chrono::DateTime<chrono::Utc>,
        record_kind: String,
        producer: Option<String>,
        completion_round: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
    }

    #[derive(Serialize)]
    struct HistoryArtifactJsonView {
        artifact_id: String,
        payload_id: String,
        stage_id: String,
        created_at: chrono::DateTime<chrono::Utc>,
        record_kind: String,
        producer: Option<String>,
        completion_round: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        content: Option<String>,
    }

    #[derive(Serialize)]
    struct HistoryJsonView {
        project_id: String,
        events: Vec<crate::contexts::project_run_record::model::JournalEvent>,
        payloads: Vec<HistoryPayloadJsonView>,
        artifacts: Vec<HistoryArtifactJsonView>,
    }

    let payloads = history
        .payloads
        .iter()
        .map(|payload| HistoryPayloadJsonView {
            payload_id: payload.payload_id.clone(),
            stage_id: payload.stage_id.as_str().to_owned(),
            cycle: payload.cycle,
            attempt: payload.attempt,
            created_at: payload.created_at,
            record_kind: payload.record_kind.to_string(),
            producer: payload.producer.as_ref().map(ToString::to_string),
            completion_round: payload.completion_round,
            payload: verbose.then(|| payload.payload.clone()),
        })
        .collect();
    let artifacts = history
        .artifacts
        .iter()
        .map(|artifact| HistoryArtifactJsonView {
            artifact_id: artifact.artifact_id.clone(),
            payload_id: artifact.payload_id.clone(),
            stage_id: artifact.stage_id.as_str().to_owned(),
            created_at: artifact.created_at,
            record_kind: artifact.record_kind.to_string(),
            producer: artifact.producer.as_ref().map(ToString::to_string),
            completion_round: artifact.completion_round,
            content: verbose.then(|| artifact.content.clone()),
        })
        .collect();
    let output = HistoryJsonView {
        project_id: history.project_id.clone(),
        events: history.events.clone(),
        payloads,
        artifacts,
    };

    Ok(serde_json::to_string_pretty(&output)?)
}

fn print_history_text(
    history: &crate::contexts::project_run_record::queries::RunHistoryView,
    verbose: bool,
) {
    println!("Project: {}", history.project_id);
    print_durable_records(
        &history.events,
        &history.payloads,
        &history.artifacts,
        verbose,
        false,
    );
}

fn print_tail_text(tail: &crate::contexts::project_run_record::queries::RunTailView) {
    println!("Project: {}", tail.project_id);
    print_durable_records(&tail.events, &tail.payloads, &tail.artifacts, false, true);
    print_runtime_logs(tail.runtime_logs.as_deref());
}

fn print_follow_update(
    events: &[crate::contexts::project_run_record::model::JournalEvent],
    payloads: &[crate::contexts::project_run_record::model::PayloadRecord],
    artifacts: &[crate::contexts::project_run_record::model::ArtifactRecord],
    runtime_logs: &[crate::contexts::project_run_record::model::RuntimeLogEntry],
) {
    if !events.is_empty() || !payloads.is_empty() || !artifacts.is_empty() {
        print_durable_records(events, payloads, artifacts, false, true);
    }
    if !runtime_logs.is_empty() {
        println!("--- Runtime Logs ---");
        for log in runtime_logs {
            println!(
                "  [{}] {:?} [{}] {}",
                log.timestamp, log.level, log.source, log.message
            );
        }
    }
}

fn print_durable_records(
    events: &[crate::contexts::project_run_record::model::JournalEvent],
    payloads: &[crate::contexts::project_run_record::model::PayloadRecord],
    artifacts: &[crate::contexts::project_run_record::model::ArtifactRecord],
    verbose: bool,
    durable_heading: bool,
) {
    println!(
        "{}",
        if durable_heading {
            "--- Durable History ---"
        } else {
            "--- Journal Events ---"
        }
    );
    for event in events {
        println!(
            "  [{}] {} - {:?}",
            event.sequence, event.timestamp, event.event_type
        );
        if verbose {
            print_json_block("    details:", &event.details);
        }
    }

    if !payloads.is_empty() {
        println!("--- Payloads ---");
        for payload in payloads {
            let producer_str = payload
                .producer
                .as_ref()
                .map(|p| format!(" producer={p}"))
                .unwrap_or_default();
            println!(
                "  {} ({}, cycle {}, attempt {}, kind={}, round={}{})",
                payload.payload_id,
                payload.stage_id,
                payload.cycle,
                payload.attempt,
                payload.record_kind,
                payload.completion_round,
                producer_str,
            );
            if verbose {
                print_json_block(
                    "    metadata:",
                    &serde_json::json!({
                        "payload_id": payload.payload_id,
                        "stage_id": payload.stage_id.as_str(),
                        "cycle": payload.cycle,
                        "attempt": payload.attempt,
                        "created_at": payload.created_at,
                        "record_kind": payload.record_kind.to_string(),
                        "producer": payload.producer.as_ref().map(ToString::to_string),
                        "completion_round": payload.completion_round,
                    }),
                );
            }
        }
    }

    if !artifacts.is_empty() {
        println!("--- Artifacts ---");
        for artifact in artifacts {
            let producer_str = artifact
                .producer
                .as_ref()
                .map(|p| format!(" producer={p}"))
                .unwrap_or_default();
            println!(
                "  {} (payload: {}, stage: {}, kind={}{})",
                artifact.artifact_id,
                artifact.payload_id,
                artifact.stage_id,
                artifact.record_kind,
                producer_str,
            );
            if verbose {
                print_json_block(
                    "    metadata:",
                    &serde_json::json!({
                        "artifact_id": artifact.artifact_id,
                        "payload_id": artifact.payload_id,
                        "stage_id": artifact.stage_id.as_str(),
                        "created_at": artifact.created_at,
                        "record_kind": artifact.record_kind.to_string(),
                        "producer": artifact.producer.as_ref().map(ToString::to_string),
                        "completion_round": artifact.completion_round,
                    }),
                );
                println!("    preview: {}", truncate_preview(&artifact.content, 120));
            }
        }
    }
}

fn print_runtime_logs(
    runtime_logs: Option<&[crate::contexts::project_run_record::model::RuntimeLogEntry]>,
) {
    if let Some(logs) = runtime_logs {
        println!("--- Runtime Logs ---");
        if logs.is_empty() {
            println!("  (no runtime logs)");
        } else {
            for log in logs {
                println!(
                    "  [{}] {:?} [{}] {}",
                    log.timestamp, log.level, log.source, log.message
                );
            }
        }
    }
}

fn print_json_block(label: &str, value: &Value) {
    println!("{label}");
    let rendered = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    for line in rendered.lines() {
        println!("      {line}");
    }
}

fn truncate_preview(content: &str, max_chars: usize) -> String {
    let preview: String = content.chars().take(max_chars).collect();
    if content.chars().count() > max_chars {
        format!("{preview}...")
    } else {
        preview
    }
}

fn parse_cli_backend_overrides(args: &RunBackendOverrideArgs) -> AppResult<CliBackendOverrides> {
    Ok(CliBackendOverrides {
        backend: parse_backend_selection_arg("backend", args.backend.as_deref())?,
        planner_backend: parse_backend_selection_arg(
            "planner_backend",
            args.planner_backend.as_deref(),
        )?,
        implementer_backend: parse_backend_selection_arg(
            "implementer_backend",
            args.implementer_backend.as_deref(),
        )?,
        reviewer_backend: parse_backend_selection_arg(
            "reviewer_backend",
            args.reviewer_backend.as_deref(),
        )?,
        qa_backend: parse_backend_selection_arg("qa_backend", args.qa_backend.as_deref())?,
        execution_mode: args
            .execution_mode
            .as_deref()
            .map(str::parse::<ExecutionMode>)
            .transpose()?,
        stream_output: args.stream_output,
    })
}

fn parse_backend_selection_arg(
    _key: &str,
    raw: Option<&str>,
) -> AppResult<Option<BackendSelection>> {
    raw.map(BackendSelection::from_backend_name).transpose()
}
