#![forbid(unsafe_code)]

//! Sequential top-level drain orchestration for bead-backed Ralph work.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::Serialize;
use thiserror::Error;

use crate::adapters::br_models::{BeadPriority, BeadStatus, ReadyBead};

use super::drain_failure::{
    classify_drain_failure, FailureObservation, FailureObservationKind, PostBeadAction,
    RecoveryAction, VerificationFailureSource,
};
use super::pr_watch::WatchOutcome;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainOptions {
    pub max_cycles: Option<u32>,
    pub stop_on_p0: bool,
    pub max_consecutive_failures: u32,
}

impl Default for DrainOptions {
    fn default() -> Self {
        Self {
            max_cycles: None,
            stop_on_p0: true,
            max_consecutive_failures: 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum DrainOutcome {
    Drained {
        cycles: u32,
    },
    CycleLimitReached {
        cycles: u32,
    },
    TooManyFailures {
        count: u32,
    },
    P0Encountered {
        bead_id: String,
    },
    Interrupted {
        cycles: u32,
        last_bead: Option<String>,
    },
    Failed {
        bead_id: String,
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainRunCompletion {
    pub convergence_pattern: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainCreatedProject {
    pub branch_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainRunOutcome {
    Completed(DrainRunCompletion),
    Failed(FailureObservation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainPrOpenOutput {
    pub pr_number: u64,
    pub pr_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DrainPrOpenOutcome {
    Opened(DrainPrOpenOutput),
    Failed(FailureObservation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CycleSummary {
    pub cycle: u32,
    pub bead_id: String,
    pub convergence_pattern: String,
    pub pr_number: Option<u64>,
    pub outcome: String,
    pub duration: Duration,
}

impl CycleSummary {
    pub fn render(&self) -> String {
        let pr = self
            .pr_number
            .map(|number| format!("PR #{number}"))
            .unwrap_or_else(|| "no PR".to_owned());
        format!(
            "[cycle {}] {}: {} -> {} -> {} ({})",
            self.cycle,
            self.bead_id,
            self.convergence_pattern,
            pr,
            self.outcome,
            format_duration(self.duration)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainReport {
    pub outcome: DrainOutcome,
    pub cycles: Vec<CycleSummary>,
    pub landed: Vec<String>,
    pub failed: Vec<String>,
    pub skipped: Vec<String>,
    pub wall_time: Duration,
}

impl DrainReport {
    pub fn render_summary(&self) -> String {
        format!(
            "Drain summary\nlanded: {}\nfailed: {}\nskipped: {}\ntotal wall time: {}\noutcome: {}",
            list_or_none(&self.landed),
            list_or_none(&self.failed),
            list_or_none(&self.skipped),
            format_duration(self.wall_time),
            render_outcome(&self.outcome)
        )
    }
}

#[derive(Debug, Error)]
pub enum DrainError {
    #[error("{operation} failed: {details}")]
    Operation {
        operation: &'static str,
        details: String,
    },
}

impl DrainError {
    pub fn operation(operation: &'static str, details: impl Into<String>) -> Self {
        Self::Operation {
            operation,
            details: details.into(),
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait DrainPort {
    async fn sync_master_and_sanity_check(&mut self) -> Result<(), DrainError>;
    async fn sync_master_and_sanity_check_preserving_resume(
        &mut self,
        _bead_id: &str,
        _created_project: &DrainCreatedProject,
    ) -> Result<(), DrainError> {
        self.sync_master_and_sanity_check().await
    }
    async fn ready_beads(&mut self) -> Result<Vec<ReadyBead>, DrainError>;
    async fn bead_status(&mut self, bead_id: &str) -> Result<BeadStatus, DrainError>;
    async fn create_project_from_bead(
        &mut self,
        bead_id: &str,
    ) -> Result<DrainCreatedProject, DrainError>;
    /// Re-create a project for a bead after its prior attempt was discarded
    /// via `tear_down_failed_project`, injecting the prior failure log into
    /// the new project's prompt as additional context. Default impl ignores
    /// the failure log and falls back to the regular create path — adapters
    /// that support context injection should override.
    async fn create_project_from_bead_with_failure_context(
        &mut self,
        bead_id: &str,
        _prior_failure_log: &str,
    ) -> Result<DrainCreatedProject, DrainError> {
        self.create_project_from_bead(bead_id).await
    }
    /// Discard the live project state for a bead whose run produced a
    /// cleanable verification failure, so the next iteration can start
    /// fresh. Implementations should reset the working tree to master,
    /// remove the live project directory, and stop any associated run.
    async fn tear_down_failed_project(&mut self, _bead_id: &str) -> Result<(), DrainError> {
        Ok(())
    }
    async fn restore_resume_context(
        &mut self,
        _bead_id: &str,
        _created_project: &DrainCreatedProject,
    ) -> Result<(), DrainError> {
        Ok(())
    }
    async fn start_run(&mut self, bead_id: &str) -> Result<DrainRunOutcome, DrainError>;
    async fn resume_run(&mut self, bead_id: &str) -> Result<DrainRunOutcome, DrainError>;
    async fn stop_run(&mut self, _bead_id: &str) -> Result<(), DrainError> {
        Ok(())
    }
    async fn open_pr(&mut self, bead_id: &str) -> Result<DrainPrOpenOutcome, DrainError>;
    async fn watch_pr(&mut self, bead_id: &str, pr_number: u64)
        -> Result<WatchOutcome, DrainError>;
    async fn close_bead(&mut self, bead_id: &str, reason: &str) -> Result<(), DrainError>;
    async fn file_follow_up_bead(
        &mut self,
        bead_id: &str,
        action: &RecoveryAction,
        observation: &FailureObservation,
    ) -> Result<(), DrainError>;
    async fn prepare_bead_mutation_base(&mut self) -> Result<(), DrainError> {
        Ok(())
    }
    async fn persist_bead_mutations(&mut self) -> Result<(), DrainError> {
        Ok(())
    }
    fn should_interrupt(&self) -> bool {
        false
    }
}

pub async fn drain_bead_queue<P: DrainPort>(
    ports: &mut P,
    options: DrainOptions,
) -> Result<DrainReport, DrainError> {
    let started = Instant::now();
    let mut report = DrainReport {
        outcome: DrainOutcome::Drained { cycles: 0 },
        cycles: Vec::new(),
        landed: Vec::new(),
        failed: Vec::new(),
        skipped: Vec::new(),
        wall_time: Duration::ZERO,
    };
    let mut cycles = 0;
    let mut consecutive_failures = 0;
    let mut last_bead = None;
    let mut pending_resume: Option<PendingResume> = None;
    let mut pending_retry: Option<PendingRetry> = None;
    // Per-bead remaining RetryBeadFresh budget for the lifetime of this drain.
    // Each bead gets `RETRY_BEAD_FRESH_BUDGET` chances to clear a cleanable
    // verification failure via a fresh implementation round before falling
    // through to FileBead{AbortDrain}. The budget is local to one drain run —
    // a future drain that re-picks the same bead starts with a full budget.
    let mut retry_budget: HashMap<String, u32> = HashMap::new();

    loop {
        if check_interrupt(ports, &mut report, started, cycles, last_bead.clone()) {
            return Ok(report);
        }

        if let Some(max_cycles) = options.max_cycles {
            if cycles >= max_cycles {
                report.outcome = DrainOutcome::CycleLimitReached { cycles };
                report.wall_time = started.elapsed();
                return Ok(report);
            }
        }

        if let Some(pending) = pending_resume.as_ref() {
            ports
                .sync_master_and_sanity_check_preserving_resume(
                    &pending.bead_id,
                    &pending.created_project,
                )
                .await?;
        } else {
            ports.sync_master_and_sanity_check().await?;
        }
        if check_interrupt(ports, &mut report, started, cycles, last_bead.clone()) {
            return Ok(report);
        }

        let ready_beads = ports.ready_beads().await?;
        let landed_in_session = report.landed.clone();
        let selected = if let Some(pending) = pending_retry.take() {
            // A prior cycle hit a cleanable verification failure on this
            // bead. Skip the ready-bead pick and re-enter project_create on
            // the same bead with the prior failure log injected as context.
            // The teardown was already performed in handle_recovery_action.
            if check_interrupt(
                ports,
                &mut report,
                started,
                cycles,
                Some(pending.bead_id.clone()),
            ) {
                return Ok(report);
            }
            SelectedDrainWork::Retry {
                bead_id: pending.bead_id,
                prior_failure_log: pending.prior_failure_log,
            }
        } else if let Some(pending) = pending_resume.take() {
            if options.stop_on_p0 {
                if let Some(bead) =
                    next_open_ready_bead_from(ports, ready_beads, &landed_in_session).await?
                {
                    if is_p0(&bead.priority) {
                        report.outcome = DrainOutcome::P0Encountered { bead_id: bead.id };
                        report.wall_time = started.elapsed();
                        return Ok(report);
                    }
                }
            }
            if !is_resume_candidate_status(&ports.bead_status(&pending.bead_id).await?) {
                continue;
            }
            ports
                .restore_resume_context(&pending.bead_id, &pending.created_project)
                .await?;
            SelectedDrainWork::Resume {
                bead_id: pending.bead_id,
                created_project: pending.created_project,
            }
        } else {
            let Some(bead) =
                next_open_ready_bead_from(ports, ready_beads, &landed_in_session).await?
            else {
                report.outcome = DrainOutcome::Drained { cycles };
                report.wall_time = started.elapsed();
                return Ok(report);
            };
            if check_interrupt(ports, &mut report, started, cycles, Some(bead.id.clone())) {
                return Ok(report);
            }

            if options.stop_on_p0 && is_p0(&bead.priority) {
                report.outcome = DrainOutcome::P0Encountered { bead_id: bead.id };
                report.wall_time = started.elapsed();
                return Ok(report);
            }
            SelectedDrainWork::Start { bead_id: bead.id }
        };

        cycles += 1;
        let bead_id = selected.bead_id().to_owned();
        last_bead = Some(bead_id.clone());
        let cycle_started = Instant::now();
        let resumed = matches!(selected, SelectedDrainWork::Resume { .. });
        let mut next_run_outcome = match selected {
            SelectedDrainWork::Start { .. } => {
                let created_project = ports.create_project_from_bead(&bead_id).await?;
                if check_interrupt(ports, &mut report, started, cycles, last_bead.clone()) {
                    return Ok(report);
                }
                Some((
                    drive_run_to_completion(ports, &bead_id).await?,
                    Some(created_project),
                ))
            }
            SelectedDrainWork::Resume {
                created_project, ..
            } => {
                if check_interrupt(ports, &mut report, started, cycles, last_bead.clone()) {
                    return Ok(report);
                }
                Some((ports.resume_run(&bead_id).await?, Some(created_project)))
            }
            SelectedDrainWork::Retry {
                prior_failure_log, ..
            } => {
                let created_project = ports
                    .create_project_from_bead_with_failure_context(&bead_id, &prior_failure_log)
                    .await?;
                if check_interrupt(ports, &mut report, started, cycles, last_bead.clone()) {
                    return Ok(report);
                }
                Some((
                    drive_run_to_completion(ports, &bead_id).await?,
                    Some(created_project),
                ))
            }
        };
        while let Some((run_outcome, created_project)) = next_run_outcome.take() {
            match run_outcome {
                DrainRunOutcome::Completed(completion) => {
                    let landed = match complete_success_path(
                        ports,
                        &bead_id,
                        completion.convergence_pattern,
                        if resumed {
                            "landed after resume"
                        } else {
                            "landed"
                        },
                        SuccessPathContext {
                            report: &mut report,
                            started,
                            cycle: cycles,
                            last_bead: last_bead.clone(),
                            cycle_started,
                        },
                    )
                    .await?
                    {
                        SuccessPathDisposition::Landed => {
                            consecutive_failures = 0;
                            true
                        }
                        SuccessPathDisposition::Recover(observation) => {
                            next_run_outcome = Some((DrainRunOutcome::Failed(observation), None));
                            false
                        }
                    };
                    if landed {
                        break;
                    }
                }
                DrainRunOutcome::Failed(observation) => {
                    if check_interrupt(ports, &mut report, started, cycles, last_bead.clone()) {
                        return Ok(report);
                    }
                    consecutive_failures += 1;
                    let action = classify_drain_failure(&observation);
                    match handle_recovery_action(
                        ports,
                        &bead_id,
                        action,
                        &observation,
                        &mut report,
                        &mut retry_budget,
                    )
                    .await?
                    {
                        RecoveryDisposition::Continue { skipped } => {
                            if skipped {
                                report.cycles.push(CycleSummary {
                                    cycle: cycles,
                                    bead_id: bead_id.clone(),
                                    convergence_pattern: "failed".to_owned(),
                                    pr_number: None,
                                    outcome: "skipped".to_owned(),
                                    duration: cycle_started.elapsed(),
                                });
                            }
                            if consecutive_failures >= options.max_consecutive_failures {
                                report.outcome = DrainOutcome::TooManyFailures {
                                    count: consecutive_failures,
                                };
                                report.wall_time = started.elapsed();
                                return Ok(report);
                            }
                            break;
                        }
                        RecoveryDisposition::ResumeNextCycle { stop_first } => {
                            if stop_first {
                                ports.stop_run(&bead_id).await?;
                            }
                            if consecutive_failures >= options.max_consecutive_failures {
                                report.outcome = DrainOutcome::TooManyFailures {
                                    count: consecutive_failures,
                                };
                                report.wall_time = started.elapsed();
                                return Ok(report);
                            }
                            if check_interrupt(
                                ports,
                                &mut report,
                                started,
                                cycles,
                                last_bead.clone(),
                            ) {
                                return Ok(report);
                            }
                            let Some(created_project) = created_project else {
                                report.failed.push(bead_id.clone());
                                report.outcome = DrainOutcome::Failed {
                                    bead_id: bead_id.clone(),
                                    reason: "resume failure requested another resume cycle"
                                        .to_owned(),
                                };
                                report.wall_time = started.elapsed();
                                return Ok(report);
                            };
                            pending_resume = Some(PendingResume {
                                bead_id: bead_id.clone(),
                                created_project,
                            });
                            report.cycles.push(CycleSummary {
                                cycle: cycles,
                                bead_id: bead_id.clone(),
                                convergence_pattern: "interrupted".to_owned(),
                                pr_number: None,
                                outcome: "resume queued".to_owned(),
                                duration: cycle_started.elapsed(),
                            });
                            break;
                        }
                        RecoveryDisposition::Abort(reason) => {
                            report.failed.push(bead_id.clone());
                            report.outcome = DrainOutcome::Failed {
                                bead_id: bead_id.clone(),
                                reason,
                            };
                            report.wall_time = started.elapsed();
                            return Ok(report);
                        }
                        RecoveryDisposition::ForceComplete => {
                            let landed = match complete_success_path(
                                ports,
                                &bead_id,
                                "force-completed".to_owned(),
                                "landed",
                                SuccessPathContext {
                                    report: &mut report,
                                    started,
                                    cycle: cycles,
                                    last_bead: last_bead.clone(),
                                    cycle_started,
                                },
                            )
                            .await?
                            {
                                SuccessPathDisposition::Landed => {
                                    consecutive_failures = 0;
                                    true
                                }
                                SuccessPathDisposition::Recover(observation) => {
                                    next_run_outcome =
                                        Some((DrainRunOutcome::Failed(observation), None));
                                    false
                                }
                            };
                            if landed {
                                break;
                            }
                        }
                        RecoveryDisposition::RetryBeadFresh { failure_log } => {
                            ports.tear_down_failed_project(&bead_id).await?;
                            if consecutive_failures >= options.max_consecutive_failures {
                                report.outcome = DrainOutcome::TooManyFailures {
                                    count: consecutive_failures,
                                };
                                report.wall_time = started.elapsed();
                                return Ok(report);
                            }
                            pending_retry = Some(PendingRetry {
                                bead_id: bead_id.clone(),
                                prior_failure_log: failure_log,
                            });
                            report.cycles.push(CycleSummary {
                                cycle: cycles,
                                bead_id: bead_id.clone(),
                                convergence_pattern: "failed".to_owned(),
                                pr_number: None,
                                outcome: "retry queued (cleanable)".to_owned(),
                                duration: cycle_started.elapsed(),
                            });
                            break;
                        }
                    }
                }
            }
        }

        if check_interrupt(ports, &mut report, started, cycles, last_bead.clone()) {
            return Ok(report);
        }
    }
}

enum SelectedDrainWork {
    Start {
        bead_id: String,
    },
    Resume {
        bead_id: String,
        created_project: DrainCreatedProject,
    },
    Retry {
        bead_id: String,
        prior_failure_log: String,
    },
}

struct PendingResume {
    bead_id: String,
    created_project: DrainCreatedProject,
}

struct PendingRetry {
    bead_id: String,
    prior_failure_log: String,
}

/// Default per-bead RetryBeadFresh budget for a single drain run. One retry
/// is enough to recover from a typical lint-only verification failure; a
/// pattern of repeated failures is a stronger signal that a follow-up bead
/// (and human attention) is the right path.
const RETRY_BEAD_FRESH_BUDGET: u32 = 1;

impl SelectedDrainWork {
    fn bead_id(&self) -> &str {
        match self {
            Self::Start { bead_id }
            | Self::Resume { bead_id, .. }
            | Self::Retry { bead_id, .. } => bead_id,
        }
    }
}

enum SuccessPathDisposition {
    Landed,
    Recover(FailureObservation),
}

struct SuccessPathContext<'a> {
    report: &'a mut DrainReport,
    started: Instant,
    cycle: u32,
    last_bead: Option<String>,
    cycle_started: Instant,
}

async fn complete_success_path<P: DrainPort>(
    ports: &mut P,
    bead_id: &str,
    convergence_pattern: String,
    landed_outcome: &str,
    ctx: SuccessPathContext<'_>,
) -> Result<SuccessPathDisposition, DrainError> {
    if check_interrupt(
        ports,
        ctx.report,
        ctx.started,
        ctx.cycle,
        ctx.last_bead.clone(),
    ) {
        return Ok(SuccessPathDisposition::Landed);
    }
    let pr = match ports.open_pr(bead_id).await? {
        DrainPrOpenOutcome::Opened(pr) => pr,
        DrainPrOpenOutcome::Failed(observation) => {
            return Ok(SuccessPathDisposition::Recover(observation));
        }
    };
    if check_interrupt(
        ports,
        ctx.report,
        ctx.started,
        ctx.cycle,
        ctx.last_bead.clone(),
    ) {
        return Ok(SuccessPathDisposition::Landed);
    }
    let watch = ports.watch_pr(bead_id, pr.pr_number).await?;
    match watch {
        WatchOutcome::Merged { .. } => {
            let reason = format!("Landed via PR #{}", pr.pr_number);
            ports.prepare_bead_mutation_base().await?;
            ports.close_bead(bead_id, &reason).await?;
            ports.persist_bead_mutations().await?;
            ctx.report.landed.push(bead_id.to_owned());
            ctx.report.cycles.push(CycleSummary {
                cycle: ctx.cycle,
                bead_id: bead_id.to_owned(),
                convergence_pattern,
                pr_number: Some(pr.pr_number),
                outcome: landed_outcome.to_owned(),
                duration: ctx.cycle_started.elapsed(),
            });
            Ok(SuccessPathDisposition::Landed)
        }
        WatchOutcome::BlockedByCi {
            failing,
            after_rerun,
        } => {
            if check_interrupt(ports, ctx.report, ctx.started, ctx.cycle, ctx.last_bead) {
                return Ok(SuccessPathDisposition::Landed);
            }
            Ok(SuccessPathDisposition::Recover(FailureObservation::new(
                FailureObservationKind::VerificationFailure {
                    source: VerificationFailureSource::Ci,
                    failing,
                    reruns_attempted_for_pr: u32::from(after_rerun),
                },
            )))
        }
        WatchOutcome::BlockedByBot { reasons } => {
            if check_interrupt(ports, ctx.report, ctx.started, ctx.cycle, ctx.last_bead) {
                return Ok(SuccessPathDisposition::Landed);
            }
            if reasons.is_empty() {
                Ok(SuccessPathDisposition::Recover(FailureObservation::new(
                    FailureObservationKind::BotRejectedReaction,
                )))
            } else {
                Ok(SuccessPathDisposition::Recover(FailureObservation::new(
                    FailureObservationKind::BotLineCommentsAfterLatestPush {
                        comment_count: reasons.len(),
                    },
                )))
            }
        }
        WatchOutcome::Timeout { .. } => {
            if check_interrupt(ports, ctx.report, ctx.started, ctx.cycle, ctx.last_bead) {
                return Ok(SuccessPathDisposition::Landed);
            }
            Ok(SuccessPathDisposition::Recover(FailureObservation::new(
                FailureObservationKind::BackendFailure {
                    bead_id: bead_id.to_owned(),
                    failure_class: crate::shared::domain::FailureClass::Timeout,
                },
            )))
        }
        WatchOutcome::Aborted { reason } => {
            if check_interrupt(ports, ctx.report, ctx.started, ctx.cycle, ctx.last_bead) {
                return Ok(SuccessPathDisposition::Landed);
            }
            Ok(SuccessPathDisposition::Recover(FailureObservation::new(
                FailureObservationKind::BackendFailure {
                    bead_id: bead_id.to_owned(),
                    failure_class: if reason.contains("mergeable") {
                        crate::shared::domain::FailureClass::DomainValidationFailure
                    } else {
                        crate::shared::domain::FailureClass::TransportFailure
                    },
                },
            )))
        }
    }
}

fn check_interrupt<P: DrainPort>(
    ports: &P,
    report: &mut DrainReport,
    started: Instant,
    cycles: u32,
    last_bead: Option<String>,
) -> bool {
    if !ports.should_interrupt() {
        return false;
    }

    report.outcome = DrainOutcome::Interrupted { cycles, last_bead };
    report.wall_time = started.elapsed();
    true
}

async fn next_open_ready_bead_from<P: DrainPort>(
    ports: &mut P,
    ready_beads: Vec<ReadyBead>,
    landed_in_session: &[String],
) -> Result<Option<ReadyBead>, DrainError> {
    for bead in ready_beads {
        // Skip beads we just landed in this drain session, even if they
        // appear in `br ready` again. This guards against the bot's
        // PR #215 finding: when the bead-sync PR fails to merge, the
        // next cycle's `git reset --hard origin/master` discards the
        // local `br close` from this session, so `br ready` re-surfaces
        // the same bead. Without this guard, drain would re-process it.
        if landed_in_session.iter().any(|landed| landed == &bead.id) {
            continue;
        }
        if ports.bead_status(&bead.id).await? == BeadStatus::Open {
            return Ok(Some(bead));
        }
    }
    Ok(None)
}

async fn drive_run_to_completion<P: DrainPort>(
    ports: &mut P,
    bead_id: &str,
) -> Result<DrainRunOutcome, DrainError> {
    // The concrete run port delegates to the same engine path as `run start`;
    // that path owns run-status polling and durable status transitions.
    ports.start_run(bead_id).await
}

enum RecoveryDisposition {
    Continue { skipped: bool },
    ResumeNextCycle { stop_first: bool },
    Abort(String),
    ForceComplete,
    RetryBeadFresh { failure_log: String },
}

async fn handle_recovery_action<P: DrainPort>(
    ports: &mut P,
    bead_id: &str,
    action: RecoveryAction,
    observation: &FailureObservation,
    report: &mut DrainReport,
    retry_budget: &mut HashMap<String, u32>,
) -> Result<RecoveryDisposition, DrainError> {
    match action {
        RecoveryAction::Rerun { .. } => Ok(RecoveryDisposition::Continue { skipped: false }),
        RecoveryAction::ResumeRun { stop_first } => {
            Ok(RecoveryDisposition::ResumeNextCycle { stop_first })
        }
        RecoveryAction::RetryBeadFresh => {
            // Check the per-bead retry budget. If this bead has a remaining
            // retry, consume one and route to the fresh-retry path. If the
            // budget is exhausted, fall through to the same FileBead{AbortDrain}
            // flow we'd have taken without the cleanable-failure heuristic —
            // the failure looks cleanable but a previous fresh attempt already
            // failed, so a follow-up bead is the right escalation.
            let remaining = retry_budget
                .entry(bead_id.to_owned())
                .or_insert(RETRY_BEAD_FRESH_BUDGET);
            if *remaining == 0 {
                return file_follow_up_and_abort(ports, bead_id, observation, report).await;
            }
            *remaining -= 1;
            let failure_log = render_failure_log(observation);
            Ok(RecoveryDisposition::RetryBeadFresh { failure_log })
        }
        RecoveryAction::FileBead { after_filing } => {
            ports.prepare_bead_mutation_base().await?;
            ports
                .file_follow_up_bead(
                    bead_id,
                    &RecoveryAction::FileBead { after_filing },
                    observation,
                )
                .await?;
            match after_filing {
                PostBeadAction::AbortDrain => {
                    ports.persist_bead_mutations().await?;
                    Ok(RecoveryDisposition::Abort(
                        "filed follow-up bead and aborted drain".to_owned(),
                    ))
                }
                PostBeadAction::SkipBeadAndContinueDrain => {
                    let reason = "skipped: filed follow-up bead";
                    ports.close_bead(bead_id, reason).await?;
                    ports.persist_bead_mutations().await?;
                    report.skipped.push(bead_id.to_owned());
                    Ok(RecoveryDisposition::Continue { skipped: true })
                }
            }
        }
        RecoveryAction::AbortDrain => Ok(RecoveryDisposition::Abort(
            "failure policy aborted drain".to_owned(),
        )),
        RecoveryAction::ForceCompleteRun => Ok(RecoveryDisposition::ForceComplete),
    }
}

async fn file_follow_up_and_abort<P: DrainPort>(
    ports: &mut P,
    bead_id: &str,
    observation: &FailureObservation,
    _report: &mut DrainReport,
) -> Result<RecoveryDisposition, DrainError> {
    ports.prepare_bead_mutation_base().await?;
    ports
        .file_follow_up_bead(
            bead_id,
            &RecoveryAction::FileBead {
                after_filing: PostBeadAction::AbortDrain,
            },
            observation,
        )
        .await?;
    ports.persist_bead_mutations().await?;
    Ok(RecoveryDisposition::Abort(
        "retry budget exhausted; filed follow-up bead and aborted drain".to_owned(),
    ))
}

fn render_failure_log(observation: &FailureObservation) -> String {
    match &observation.kind {
        FailureObservationKind::VerificationFailure {
            failing, source, ..
        } => {
            let header = match source {
                VerificationFailureSource::Ci => "CI verification",
                VerificationFailureSource::Gate(gate) => match gate {
                    super::pr_open::Gate::CargoTest => "gate: cargo test",
                    super::pr_open::Gate::CargoClippy => "gate: cargo clippy",
                    super::pr_open::Gate::CargoFmt => "gate: cargo fmt",
                    super::pr_open::Gate::NixBuild => "gate: nix build",
                },
            };
            format!("{header}\n\n{}", failing.join("\n\n"))
        }
        // Other observation kinds reach this path only via the retry-budget
        // exhausted fall-through; the FileBead path renders its own context.
        _ => format!("{observation:#?}"),
    }
}

fn is_p0(priority: &BeadPriority) -> bool {
    priority.value() == 0
}

fn is_resume_candidate_status(status: &BeadStatus) -> bool {
    matches!(status, BeadStatus::Open | BeadStatus::InProgress)
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds < 60 {
        format!("{seconds}s")
    } else {
        format!("{}m{}s", seconds / 60, seconds % 60)
    }
}

fn list_or_none(items: &[String]) -> String {
    if items.is_empty() {
        "none".to_owned()
    } else {
        items.join(", ")
    }
}

fn render_outcome(outcome: &DrainOutcome) -> String {
    match outcome {
        DrainOutcome::Drained { cycles } => format!("Drained{{cycles={cycles}}}"),
        DrainOutcome::CycleLimitReached { cycles } => {
            format!("CycleLimitReached{{cycles={cycles}}}")
        }
        DrainOutcome::TooManyFailures { count } => format!("TooManyFailures{{count={count}}}"),
        DrainOutcome::P0Encountered { bead_id } => format!("P0Encountered{{bead_id={bead_id}}}"),
        DrainOutcome::Interrupted { cycles, last_bead } => {
            format!("Interrupted{{cycles={cycles}, last_bead={last_bead:?}}}")
        }
        DrainOutcome::Failed { bead_id, reason } => {
            format!("Failed{{bead_id={bead_id}, reason={reason}}}")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};

    use crate::adapters::br_models::{BeadPriority, BeadStatus, BeadType};
    use crate::contexts::bead_workflow::drain_failure::{
        FailureObservationKind, VerificationFailureSource,
    };
    use crate::contexts::bead_workflow::pr_open::Gate;
    use crate::contexts::bead_workflow::pr_watch::KNOWN_CI_FLAKES;
    use crate::shared::domain::FailureClass;

    use super::*;

    #[derive(Default)]
    struct MockDrainPort {
        ready: VecDeque<Vec<ReadyBead>>,
        statuses: HashMap<String, BeadStatus>,
        status_sequences: HashMap<String, VecDeque<BeadStatus>>,
        starts: VecDeque<DrainRunOutcome>,
        resumes: VecDeque<DrainRunOutcome>,
        pr_opens: VecDeque<DrainPrOpenOutcome>,
        watches: VecDeque<WatchOutcome>,
        opened: Vec<String>,
        resumed_beads: Vec<String>,
        restored_resume_contexts: Vec<(String, DrainCreatedProject)>,
        watched: Vec<u64>,
        closed: Vec<(String, String)>,
        filed: Vec<String>,
        filed_observations: Vec<FailureObservation>,
        stopped: Vec<String>,
        syncs: u32,
        preserving_resume_syncs: Vec<(String, DrainCreatedProject)>,
        prepared: u32,
        persisted: u32,
        next_pr: u64,
        interrupt: bool,
        interrupt_after_sync: bool,
        interrupt_after_start: bool,
        interrupt_after_watch: bool,
    }

    impl MockDrainPort {
        fn with_ready(mut self, ready: Vec<Vec<ReadyBead>>) -> Self {
            self.ready = ready.into();
            self
        }

        fn with_status(mut self, id: &str, status: BeadStatus) -> Self {
            self.statuses.insert(id.to_owned(), status);
            self
        }

        fn with_status_sequence(mut self, id: &str, statuses: Vec<BeadStatus>) -> Self {
            self.status_sequences.insert(id.to_owned(), statuses.into());
            self
        }

        fn with_start(mut self, outcome: DrainRunOutcome) -> Self {
            self.starts.push_back(outcome);
            self
        }

        fn with_resume(mut self, outcome: DrainRunOutcome) -> Self {
            self.resumes.push_back(outcome);
            self
        }

        fn with_pr_open(mut self, outcome: DrainPrOpenOutcome) -> Self {
            self.pr_opens.push_back(outcome);
            self
        }

        fn with_watch(mut self, outcome: WatchOutcome) -> Self {
            self.watches.push_back(outcome);
            self
        }
    }

    impl DrainPort for MockDrainPort {
        async fn sync_master_and_sanity_check(&mut self) -> Result<(), DrainError> {
            self.syncs += 1;
            if self.interrupt_after_sync {
                self.interrupt = true;
            }
            Ok(())
        }

        async fn sync_master_and_sanity_check_preserving_resume(
            &mut self,
            bead_id: &str,
            created_project: &DrainCreatedProject,
        ) -> Result<(), DrainError> {
            self.preserving_resume_syncs
                .push((bead_id.to_owned(), created_project.clone()));
            if self.interrupt_after_sync {
                self.interrupt = true;
            }
            Ok(())
        }

        async fn ready_beads(&mut self) -> Result<Vec<ReadyBead>, DrainError> {
            Ok(self.ready.pop_front().unwrap_or_default())
        }

        async fn bead_status(&mut self, bead_id: &str) -> Result<BeadStatus, DrainError> {
            if let Some(statuses) = self.status_sequences.get_mut(bead_id) {
                if let Some(status) = statuses.pop_front() {
                    return Ok(status);
                }
            }
            Ok(self
                .statuses
                .get(bead_id)
                .cloned()
                .unwrap_or(BeadStatus::Open))
        }

        async fn create_project_from_bead(
            &mut self,
            bead_id: &str,
        ) -> Result<DrainCreatedProject, DrainError> {
            Ok(DrainCreatedProject {
                branch_name: Some(format!("feat/{bead_id}")),
            })
        }

        async fn restore_resume_context(
            &mut self,
            bead_id: &str,
            created_project: &DrainCreatedProject,
        ) -> Result<(), DrainError> {
            self.restored_resume_contexts
                .push((bead_id.to_owned(), created_project.clone()));
            Ok(())
        }

        async fn start_run(&mut self, _bead_id: &str) -> Result<DrainRunOutcome, DrainError> {
            if self.interrupt_after_start {
                self.interrupt = true;
            }
            Ok(self.starts.pop_front().unwrap_or_else(completed))
        }

        async fn resume_run(&mut self, bead_id: &str) -> Result<DrainRunOutcome, DrainError> {
            self.resumed_beads.push(bead_id.to_owned());
            Ok(self.resumes.pop_front().unwrap_or_else(completed))
        }

        async fn stop_run(&mut self, bead_id: &str) -> Result<(), DrainError> {
            self.stopped.push(bead_id.to_owned());
            Ok(())
        }

        async fn open_pr(&mut self, bead_id: &str) -> Result<DrainPrOpenOutcome, DrainError> {
            self.opened.push(bead_id.to_owned());
            if let Some(outcome) = self.pr_opens.pop_front() {
                return Ok(outcome);
            }
            self.next_pr += 1;
            Ok(DrainPrOpenOutcome::Opened(DrainPrOpenOutput {
                pr_number: self.next_pr,
                pr_url: format!("https://example.test/pull/{}", self.next_pr),
            }))
        }

        async fn watch_pr(
            &mut self,
            _bead_id: &str,
            pr_number: u64,
        ) -> Result<WatchOutcome, DrainError> {
            self.watched.push(pr_number);
            let outcome = self.watches.pop_front().unwrap_or(WatchOutcome::Merged {
                sha: "abc123".to_owned(),
                pr_url: format!("https://example.test/pull/{pr_number}"),
            });
            if self.interrupt_after_watch {
                self.interrupt = true;
            }
            Ok(outcome)
        }

        async fn close_bead(&mut self, bead_id: &str, reason: &str) -> Result<(), DrainError> {
            self.closed.push((bead_id.to_owned(), reason.to_owned()));
            Ok(())
        }

        async fn file_follow_up_bead(
            &mut self,
            bead_id: &str,
            _action: &RecoveryAction,
            observation: &FailureObservation,
        ) -> Result<(), DrainError> {
            self.filed.push(bead_id.to_owned());
            self.filed_observations.push(observation.clone());
            Ok(())
        }

        async fn prepare_bead_mutation_base(&mut self) -> Result<(), DrainError> {
            self.prepared += 1;
            Ok(())
        }

        async fn persist_bead_mutations(&mut self) -> Result<(), DrainError> {
            self.persisted += 1;
            Ok(())
        }

        fn should_interrupt(&self) -> bool {
            self.interrupt
        }
    }

    #[tokio::test]
    async fn happy_path_lands_two_beads_and_drains() {
        let mut port = MockDrainPort::default().with_ready(vec![
            vec![ready("a", 2)],
            vec![ready("b", 2)],
            vec![],
        ]);
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert_eq!(report.outcome, DrainOutcome::Drained { cycles: 2 });
        assert_eq!(report.landed, vec!["a", "b"]);
        assert_eq!(port.closed[0].1, "Landed via PR #1");
        assert_eq!(port.closed[1].1, "Landed via PR #2");
        assert_eq!(port.prepared, 2);
        assert_eq!(port.persisted, 2);
    }

    #[tokio::test]
    async fn empty_queue_drains_without_cycles() {
        let mut port = MockDrainPort::default().with_ready(vec![vec![]]);
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert_eq!(report.outcome, DrainOutcome::Drained { cycles: 0 });
    }

    #[tokio::test]
    async fn p0_bead_stops_by_default() {
        let mut port = MockDrainPort::default().with_ready(vec![vec![ready("urgent", 0)]]);
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert_eq!(
            report.outcome,
            DrainOutcome::P0Encountered {
                bead_id: "urgent".to_owned()
            }
        );
    }

    #[tokio::test]
    async fn user_interrupt_before_work_reports_zero_cycles() {
        let mut port = MockDrainPort {
            interrupt: true,
            ..MockDrainPort::default()
        }
        .with_ready(vec![vec![ready("a", 2)]]);
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert_eq!(
            report.outcome,
            DrainOutcome::Interrupted {
                cycles: 0,
                last_bead: None
            }
        );
    }

    #[tokio::test]
    async fn user_interrupt_after_sync_stops_before_selecting_bead() {
        let mut port = MockDrainPort {
            interrupt_after_sync: true,
            ..MockDrainPort::default()
        }
        .with_ready(vec![vec![ready("a", 2)]]);
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert_eq!(
            report.outcome,
            DrainOutcome::Interrupted {
                cycles: 0,
                last_bead: None
            }
        );
    }

    #[tokio::test]
    async fn user_interrupt_after_run_stops_before_recovery_handling() {
        let mut port = MockDrainPort {
            interrupt_after_start: true,
            ..MockDrainPort::default()
        }
        .with_ready(vec![vec![ready("a", 2)]])
        .with_start(DrainRunOutcome::Failed(FailureObservation::new(
            FailureObservationKind::RunInterrupted {
                run_id: "run-1".to_owned(),
            },
        )));
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert_eq!(
            report.outcome,
            DrainOutcome::Interrupted {
                cycles: 1,
                last_bead: Some("a".to_owned())
            }
        );
        assert!(port.closed.is_empty());
        assert!(port.opened.is_empty());
    }

    #[tokio::test]
    async fn user_interrupt_after_pr_merge_still_closes_landed_bead() {
        let mut port = MockDrainPort {
            interrupt_after_watch: true,
            ..MockDrainPort::default()
        }
        .with_ready(vec![vec![ready("a", 2)]])
        .with_watch(WatchOutcome::Merged {
            sha: "abc123".to_owned(),
            pr_url: "https://example.test/pull/1".to_owned(),
        });

        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");

        assert_eq!(
            report.outcome,
            DrainOutcome::Interrupted {
                cycles: 1,
                last_bead: Some("a".to_owned())
            }
        );
        assert_eq!(report.landed, vec!["a"]);
        assert_eq!(
            port.closed,
            vec![("a".to_owned(), "Landed via PR #1".to_owned())]
        );
        assert_eq!(port.persisted, 1);
    }

    #[tokio::test]
    async fn max_consecutive_failures_stops_after_skips() {
        let mut port = MockDrainPort::default()
            .with_ready(vec![vec![ready("a", 2)], vec![ready("b", 2)]])
            .with_start(backend_exhausted("a"))
            .with_start(backend_exhausted("b"));
        let report = drain_bead_queue(
            &mut port,
            DrainOptions {
                max_consecutive_failures: 2,
                ..DrainOptions::default()
            },
        )
        .await
        .expect("drain");
        assert_eq!(report.outcome, DrainOutcome::TooManyFailures { count: 2 });
        assert_eq!(report.skipped, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn resume_action_queues_resume_for_next_cycle_then_lands() {
        let mut port = MockDrainPort::default()
            .with_ready(vec![vec![ready("a", 2)], vec![]])
            .with_status_sequence("a", vec![BeadStatus::Open, BeadStatus::InProgress])
            .with_start(DrainRunOutcome::Failed(FailureObservation::new(
                FailureObservationKind::RunInterrupted {
                    run_id: "run-1".to_owned(),
                },
            )))
            .with_resume(completed());
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert_eq!(report.outcome, DrainOutcome::Drained { cycles: 2 });
        assert_eq!(report.landed, vec!["a"]);
        assert_eq!(port.stopped, vec!["a"]);
        assert_eq!(port.resumed_beads, vec!["a"]);
        assert_eq!(port.syncs, 2);
        assert_eq!(
            port.preserving_resume_syncs,
            vec![(
                "a".to_owned(),
                DrainCreatedProject {
                    branch_name: Some("feat/a".to_owned())
                }
            )]
        );
        assert_eq!(
            port.restored_resume_contexts,
            vec![(
                "a".to_owned(),
                DrainCreatedProject {
                    branch_name: Some("feat/a".to_owned())
                }
            )]
        );
        assert_eq!(report.cycles[0].outcome, "resume queued");
        assert_eq!(report.cycles[1].outcome, "landed after resume");
    }

    #[tokio::test]
    async fn next_cycle_resume_failure_is_reclassified_instead_of_aborting_immediately() {
        let mut port = MockDrainPort::default()
            .with_ready(vec![vec![ready("a", 2)], vec![]])
            .with_start(DrainRunOutcome::Failed(FailureObservation::new(
                FailureObservationKind::RunInterrupted {
                    run_id: "run-1".to_owned(),
                },
            )))
            .with_resume(backend_exhausted("a"));
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert_eq!(report.outcome, DrainOutcome::Drained { cycles: 2 });
        assert_eq!(port.stopped, vec!["a"]);
        assert_eq!(port.resumed_beads, vec!["a"]);
        assert_eq!(report.skipped, vec!["a"]);
        assert_eq!(port.filed, vec!["a"]);
    }

    #[tokio::test]
    async fn p0_stop_wins_before_queued_resume_cycle() {
        let mut port = MockDrainPort::default()
            .with_ready(vec![vec![ready("a", 2)], vec![ready("urgent", 0)]])
            .with_start(DrainRunOutcome::Failed(FailureObservation::new(
                FailureObservationKind::RunInterrupted {
                    run_id: "run-1".to_owned(),
                },
            )))
            .with_resume(completed());
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert_eq!(
            report.outcome,
            DrainOutcome::P0Encountered {
                bead_id: "urgent".to_owned()
            }
        );
        assert_eq!(port.stopped, vec!["a"]);
        assert!(port.resumed_beads.is_empty());
    }

    #[tokio::test]
    async fn file_bead_skip_closes_current_and_continues() {
        let mut port = MockDrainPort::default()
            .with_ready(vec![vec![ready("a", 2)], vec![]])
            .with_start(backend_exhausted("a"));
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert_eq!(report.outcome, DrainOutcome::Drained { cycles: 1 });
        assert_eq!(report.skipped, vec!["a"]);
        assert_eq!(port.filed, vec!["a"]);
        assert_eq!(port.closed[0].1, "skipped: filed follow-up bead");
        assert_eq!(port.prepared, 1);
        assert_eq!(port.persisted, 1);
    }

    #[tokio::test]
    async fn file_bead_abort_exits_failed() {
        let mut port = MockDrainPort::default()
            .with_ready(vec![vec![ready("a", 2)]])
            .with_start(DrainRunOutcome::Failed(FailureObservation::new(
                FailureObservationKind::VerificationFailure {
                    source: VerificationFailureSource::Gate(Gate::CargoTest),
                    failing: vec!["unit::test".to_owned()],
                    reruns_attempted_for_pr: 0,
                },
            )));
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert!(matches!(report.outcome, DrainOutcome::Failed { .. }));
        assert_eq!(port.filed, vec!["a"]);
    }

    #[tokio::test]
    async fn pr_open_gate_failure_is_classified_and_files_follow_up() {
        let observation = FailureObservation::new(FailureObservationKind::VerificationFailure {
            source: VerificationFailureSource::Gate(Gate::CargoTest),
            failing: vec!["unit::test failed".to_owned()],
            reruns_attempted_for_pr: 0,
        });
        let mut port = MockDrainPort::default()
            .with_ready(vec![vec![ready("a", 2)]])
            .with_pr_open(DrainPrOpenOutcome::Failed(observation.clone()));
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert!(matches!(report.outcome, DrainOutcome::Failed { .. }));
        assert_eq!(port.filed, vec!["a"]);
        assert_eq!(port.filed_observations, vec![observation]);
    }

    #[tokio::test]
    async fn pr_watch_ci_failure_is_classified_and_files_follow_up() {
        let mut port = MockDrainPort::default()
            .with_ready(vec![vec![ready("a", 2)]])
            .with_watch(WatchOutcome::BlockedByCi {
                failing: vec!["integration::fails".to_owned()],
                after_rerun: true,
            });
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert!(matches!(report.outcome, DrainOutcome::Failed { .. }));
        assert_eq!(port.filed, vec!["a"]);
        assert_eq!(
            port.filed_observations,
            vec![FailureObservation::new(
                FailureObservationKind::VerificationFailure {
                    source: VerificationFailureSource::Ci,
                    failing: vec!["integration::fails".to_owned()],
                    reruns_attempted_for_pr: 1,
                }
            )]
        );
    }

    #[tokio::test]
    async fn abort_action_exits_failed() {
        let mut port = MockDrainPort::default()
            .with_ready(vec![vec![ready("a", 2)]])
            .with_start(DrainRunOutcome::Failed(FailureObservation::new(
                FailureObservationKind::BotRejectedReaction,
            )));
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert!(matches!(report.outcome, DrainOutcome::Failed { .. }));
    }

    #[tokio::test]
    async fn rerun_action_is_left_to_pr_watch_and_fails_run_path() {
        let mut port = MockDrainPort::default()
            .with_ready(vec![vec![ready("a", 2)]])
            .with_start(DrainRunOutcome::Failed(FailureObservation::new(
                FailureObservationKind::VerificationFailure {
                    source: VerificationFailureSource::Ci,
                    failing: vec![KNOWN_CI_FLAKES[0].to_owned()],
                    reruns_attempted_for_pr: 0,
                },
            )));
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert_eq!(report.outcome, DrainOutcome::Drained { cycles: 1 });
        assert!(report.failed.is_empty());
        assert!(port.closed.is_empty());
    }

    #[tokio::test]
    async fn force_complete_action_continues_to_pr_path() {
        let mut port = MockDrainPort::default()
            .with_ready(vec![vec![ready("a", 2)], vec![]])
            .with_start(DrainRunOutcome::Failed(FailureObservation::new(
                FailureObservationKind::AmendmentOscillationLimitReached {
                    completion_rounds: 3,
                    max_completion_rounds: 3,
                },
            )));
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert_eq!(report.landed, vec!["a"]);
        assert_eq!(port.opened, vec!["a"]);
    }

    #[tokio::test]
    async fn skips_ready_beads_that_are_no_longer_open() {
        let mut port = MockDrainPort::default()
            .with_ready(vec![vec![ready("closed", 2), ready("open", 2)], vec![]])
            .with_status("closed", BeadStatus::InProgress);
        let report = drain_bead_queue(&mut port, DrainOptions::default())
            .await
            .expect("drain");
        assert_eq!(report.landed, vec!["open"]);
    }

    fn ready(id: &str, priority: u32) -> ReadyBead {
        ReadyBead {
            id: id.to_owned(),
            title: format!("title {id}"),
            priority: BeadPriority::new(priority),
            bead_type: BeadType::Task,
            labels: Vec::new(),
        }
    }

    fn completed() -> DrainRunOutcome {
        DrainRunOutcome::Completed(DrainRunCompletion {
            convergence_pattern: "clean".to_owned(),
        })
    }

    fn backend_exhausted(bead_id: &str) -> DrainRunOutcome {
        DrainRunOutcome::Failed(FailureObservation::new(
            FailureObservationKind::BackendFailure {
                bead_id: bead_id.to_owned(),
                failure_class: FailureClass::BackendExhausted,
            },
        ))
    }
}
