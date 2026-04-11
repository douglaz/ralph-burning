use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use clap::{Args, Subcommand};
#[cfg(target_os = "linux")]
use nix::sys::signal::{kill, Signal};
#[cfg(all(unix, not(target_os = "linux")))]
use nix::sys::signal::{kill, Signal};
#[cfg(target_os = "linux")]
use nix::unistd::Pid;
#[cfg(all(unix, not(target_os = "linux")))]
use nix::unistd::Pid;
use notify::{Config as NotifyConfig, RecommendedWatcher, RecursiveMode, Watcher};
#[cfg(target_os = "linux")]
use rustix::event::{poll, PollFd, PollFlags};
#[cfg(target_os = "linux")]
use rustix::fd::OwnedFd;
#[cfg(target_os = "linux")]
use rustix::process::{
    pidfd_open, pidfd_send_signal, Pid as RustixPid, PidfdFlags, Signal as RustixSignal,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

use crate::adapters::br_models::{BeadDetail, BeadStatus, DependencyKind, ReadyBead};
use crate::adapters::br_process::{BrAdapter, BrCommand, BrError, ProcessRunner};
use crate::adapters::bv_process::{BvAdapter, BvCommand, BvProcessRunner, NextBeadResponse};
use crate::adapters::fs::{
    FileSystem, FsAmendmentQueueStore, FsArtifactStore, FsDaemonStore, FsJournalStore,
    FsMilestoneControllerStore, FsMilestoneJournalStore, FsMilestonePlanStore,
    FsMilestoneSnapshotStore, FsPayloadArtifactWriteStore, FsProjectStore, FsRollbackPointStore,
    FsRunSnapshotStore, FsRunSnapshotWriteStore, FsRuntimeLogStore, FsRuntimeLogWriteStore,
    FsTaskRunLineageStore,
};
use crate::adapters::worktree::WorktreeAdapter;
use crate::composition::agent_execution_builder;
use crate::contexts::agent_execution::model::CancellationToken;
use crate::contexts::automation_runtime::cli_writer_lease::{
    cleanup_detached_project_writer_owner, find_detached_project_writer_owner,
    persist_cli_writer_cleanup_handoff, read_project_writer_lease_record,
    read_project_writer_lock_owner, reclaim_specific_project_writer_owner, CliWriterLeaseGuard,
    CLI_LEASE_HEARTBEAT_CADENCE_SECONDS, CLI_LEASE_TTL_SECONDS,
};
use crate::contexts::automation_runtime::model::CliWriterCleanupHandoff;
use crate::contexts::automation_runtime::LeaseRecord;
use crate::contexts::milestone_record::bundle::{planned_bead_membership_refs, MilestoneBundle};
use crate::contexts::milestone_record::controller::{
    self as milestone_controller, ControllerBeadStatus, ControllerTaskStatus,
    MilestoneControllerResumePort, MilestoneControllerState,
};
use crate::contexts::milestone_record::model::{MilestoneId, MilestoneStatus, TaskRunOutcome};
use crate::contexts::milestone_record::service as milestone_service;
use crate::contexts::milestone_record::service::{
    CompletionMilestoneDisposition, MilestonePlanPort,
};
use crate::contexts::project_run_record::model::{
    JournalEvent, JournalEventType, ProjectRecord, RunSnapshot, RunStatus,
};
use crate::contexts::project_run_record::queries::{RunStatusJsonView, RunStatusView};
use crate::contexts::project_run_record::service::{
    self, ArtifactStorePort, JournalStorePort, ProjectStorePort, RunSnapshotPort,
    RunSnapshotWritePort, RuntimeLogStorePort, RuntimeLogWritePort,
};
use crate::contexts::workflow_composition::engine;
use crate::contexts::workflow_composition::retry_policy::RetryPolicy;
use crate::contexts::workspace_governance;
use crate::contexts::workspace_governance::config::{CliBackendOverrides, EffectiveConfig};
use crate::shared::domain::{BackendSelection, ExecutionMode, ProjectId, StageId};
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
    /// Gracefully stop the running orchestrator for the active project.
    Stop,
    /// Reconcile milestone lineage from the current terminal project snapshot.
    SyncMilestone,
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
        RunSubcommand::Stop => handle_stop().await,
        RunSubcommand::SyncMilestone => handle_sync_milestone().await,
        RunSubcommand::Attach => handle_attach().await,
        RunSubcommand::Rollback { list, to, hard } => handle_rollback(list, to, hard).await,
        RunSubcommand::ShowPayload { payload_id } => handle_show_payload(payload_id).await,
        RunSubcommand::ShowArtifact { artifact_id } => handle_show_artifact(artifact_id).await,
    }
}

const FOLLOW_POLL_INTERVAL: Duration = Duration::from_secs(2);
const FOLLOW_TRANSIENT_PARTIAL_PAIR_GRACE_PERIOD: Duration = Duration::from_secs(2);
const RUN_STOP_GRACE_PERIOD: Duration = Duration::from_secs(5);
#[cfg(not(target_os = "linux"))]
const RUN_STOP_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Copy)]
enum MilestoneControllerExecutionOrigin {
    Start,
    Resume,
}

impl MilestoneControllerExecutionOrigin {
    fn error(self, reason: impl Into<String>) -> AppError {
        match self {
            Self::Start => AppError::RunStartFailed {
                reason: reason.into(),
            },
            Self::Resume => AppError::ResumeFailed {
                reason: reason.into(),
            },
        }
    }
}

struct ProjectMilestoneControllerRuntime<'a> {
    base_dir: &'a std::path::Path,
    milestone_id: &'a MilestoneId,
}

impl ProjectMilestoneControllerRuntime<'_> {
    fn query_br_json<T: DeserializeOwned>(&self, args: &[&str], context: &str) -> AppResult<T> {
        let output = Command::new("br")
            .args(args)
            .current_dir(self.base_dir)
            .output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            let details = if !stderr.is_empty() {
                stderr
            } else if !stdout.is_empty() {
                stdout
            } else {
                format!("exit status {}", output.status)
            };
            return Err(AppError::ResumeFailed {
                reason: format!("{context}: br {} failed: {details}", args.join(" ")),
            });
        }

        serde_json::from_slice(&output.stdout).map_err(|error| AppError::ResumeFailed {
            reason: format!(
                "{context}: failed to parse br {} JSON: {error}",
                args.join(" ")
            ),
        })
    }

    fn planned_bead_membership_refs(&self) -> AppResult<HashSet<String>> {
        load_planned_bead_membership_refs(self.base_dir, self.milestone_id)
    }
}

#[derive(Debug, Deserialize)]
struct BvMessageOnlyResponse {
    #[allow(dead_code)]
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
enum BrShowResponse {
    Single(BeadDetail),
    Many(Vec<BeadDetail>),
}

#[derive(Debug, Clone)]
enum NextRecommendationOutcome {
    Recommended(NextBeadResponse),
    NoRecommendation,
}

fn load_planned_bead_membership_refs(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
) -> AppResult<HashSet<String>> {
    let plan_json = FsMilestonePlanStore.read_plan_json(base_dir, milestone_id)?;
    let bundle: MilestoneBundle =
        serde_json::from_str(&plan_json).map_err(|error| AppError::CorruptRecord {
            file: format!("milestones/{}/plan.json", milestone_id),
            details: error.to_string(),
        })?;

    if bundle.identity.id != milestone_id.as_str() {
        return Err(AppError::CorruptRecord {
            file: format!("milestones/{}/plan.json", milestone_id),
            details: format!(
                "bundle identity '{}' does not match milestone '{}'",
                bundle.identity.id, milestone_id
            ),
        });
    }

    planned_bead_membership_refs(&bundle)
        .map(|refs| refs.into_iter().collect())
        .map_err(|errors| AppError::CorruptRecord {
            file: format!("milestones/{}/plan.json", milestone_id),
            details: errors.join("; "),
        })
}

fn canonicalize_milestone_bead_ref(milestone_id: &MilestoneId, bead_id: &str) -> String {
    let trimmed = bead_id.trim();
    let qualified_prefix = format!("{}.", milestone_id.as_str());
    if trimmed.starts_with(&qualified_prefix) {
        trimmed.to_owned()
    } else {
        format!("{qualified_prefix}{trimmed}")
    }
}

fn milestone_planned_refs_contain(
    planned_refs: &HashSet<String>,
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> bool {
    planned_refs.contains(bead_id)
        || planned_refs.contains(&canonicalize_milestone_bead_ref(milestone_id, bead_id))
        || bead_id
            .strip_prefix(&format!("{}.", milestone_id.as_str()))
            .is_some_and(|short_ref| planned_refs.contains(short_ref))
}

fn milestone_bead_refs_match(milestone_id: &MilestoneId, left: &str, right: &str) -> bool {
    canonicalize_milestone_bead_ref(milestone_id, left)
        == canonicalize_milestone_bead_ref(milestone_id, right)
}

fn summarize_bead_ids(ids: &[String]) -> String {
    const MAX_IDS: usize = 5;

    if ids.is_empty() {
        return "none".to_owned();
    }

    let shown = ids.iter().take(MAX_IDS).cloned().collect::<Vec<_>>();
    if ids.len() <= MAX_IDS {
        shown.join(", ")
    } else {
        format!("{} (and {} more)", shown.join(", "), ids.len() - MAX_IDS)
    }
}

fn br_show_error_is_missing(error: &BrError) -> bool {
    fn message_describes_missing_bead(message: &str) -> bool {
        let message = message.to_ascii_lowercase();
        (message.contains("bead not found") || message.contains("issue not found"))
            || (message.contains("not found")
                && (message.contains("bead") || message.contains("issue")))
    }

    match error {
        BrError::BrExitError { stderr, stdout, .. } => {
            message_describes_missing_bead(stderr) || message_describes_missing_bead(stdout)
        }
        _ => false,
    }
}

async fn request_next_recommendation<V: BvProcessRunner>(
    bv: &BvAdapter<V>,
) -> Result<NextRecommendationOutcome, String> {
    let output = bv
        .exec_read(&BvCommand::robot_next())
        .await
        .map_err(|error| {
            format!("milestone controller selection could not query bv --robot-next: {error}")
        })?;

    serde_json::from_str::<NextBeadResponse>(&output.stdout)
        .map(NextRecommendationOutcome::Recommended)
        .or_else(|next_error| {
            serde_json::from_str::<BvMessageOnlyResponse>(&output.stdout)
                .map(|_| NextRecommendationOutcome::NoRecommendation)
                .map_err(|_| next_error)
        })
        .map_err(|error| {
            format!(
                "milestone controller selection could not parse bv --robot-next output: {error}"
            )
        })
}

fn persist_next_step_recommendation(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    recommendation: &NextBeadResponse,
) {
    let milestone_dir = FileSystem::milestone_root(base_dir, milestone_id);
    if let Err(error) = std::fs::create_dir_all(&milestone_dir) {
        tracing::warn!(
            error = %error,
            milestone_id = %milestone_id,
            path = %milestone_dir.display(),
            "failed to create milestone directory for next-step recommendation persistence"
        );
        return;
    }

    let hint_path = milestone_dir.join("next_step_hint.json");
    let tmp_path = milestone_dir.join("next_step_hint.json.tmp");
    let serialized = match serde_json::to_string_pretty(recommendation) {
        Ok(serialized) => serialized,
        Err(error) => {
            tracing::warn!(
                error = %error,
                milestone_id = %milestone_id,
                bead_id = %recommendation.id,
                "failed to serialize next-step recommendation"
            );
            return;
        }
    };

    if let Err(error) = std::fs::write(&tmp_path, serialized) {
        tracing::warn!(
            error = %error,
            milestone_id = %milestone_id,
            path = %tmp_path.display(),
            "failed to write next-step recommendation temp file"
        );
        return;
    }

    if let Err(error) = std::fs::rename(&tmp_path, &hint_path) {
        tracing::warn!(
            error = %error,
            milestone_id = %milestone_id,
            path = %hint_path.display(),
            "failed to persist next-step recommendation file"
        );
        let _ = std::fs::remove_file(&tmp_path);
    }
}

fn delete_persisted_next_step_recommendation(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
) {
    let hint_path = FileSystem::milestone_root(base_dir, milestone_id).join("next_step_hint.json");
    if hint_path.exists() {
        if let Err(error) = std::fs::remove_file(&hint_path) {
            tracing::warn!(
                error = %error,
                milestone_id = %milestone_id,
                path = %hint_path.display(),
                "failed to remove stale next-step recommendation file"
            );
        }
    }
}

async fn request_ready_milestone_beads<R: ProcessRunner>(
    br: &BrAdapter<R>,
    milestone_id: &MilestoneId,
    planned_refs: &HashSet<String>,
) -> Result<Vec<ReadyBead>, String> {
    let ready: Vec<ReadyBead> = br.exec_json(&BrCommand::ready()).await.map_err(|error| {
        format!("milestone controller selection could not query br ready: {error}")
    })?;

    Ok(ready
        .into_iter()
        .filter(|bead| milestone_planned_refs_contain(planned_refs, milestone_id, &bead.id))
        .collect())
}

async fn request_bead_detail<R: ProcessRunner>(
    br: &BrAdapter<R>,
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> Result<Option<BeadDetail>, String> {
    let response = match br
        .exec_json::<BrShowResponse>(&BrCommand::show(bead_id))
        .await
    {
        Ok(response) => response,
        Err(error) if br_show_error_is_missing(&error) => return Ok(None),
        Err(error) => {
            return Err(format!(
                "milestone controller selection could not inspect bead '{bead_id}': {error}"
            ));
        }
    };

    let matching = match response {
        BrShowResponse::Single(detail) => {
            milestone_bead_refs_match(milestone_id, &detail.id, bead_id).then_some(detail)
        }
        BrShowResponse::Many(details) => details
            .into_iter()
            .find(|detail| milestone_bead_refs_match(milestone_id, &detail.id, bead_id)),
    };

    Ok(matching)
}

fn blocked_reason_for_recommendation_mismatch(
    milestone_id: &MilestoneId,
    recommendation: &NextBeadResponse,
    ready_beads: &[ReadyBead],
    bead_detail: Option<&BeadDetail>,
) -> String {
    let ready_ids = ready_beads
        .iter()
        .map(|bead| bead.id.clone())
        .collect::<Vec<_>>();
    let ready_summary = if ready_ids.is_empty() {
        "no milestone beads are ready".to_owned()
    } else {
        format!("ready milestone beads: {}", summarize_bead_ids(&ready_ids))
    };

    let recommendation_id = recommendation.id.as_str();

    let detail_summary = match bead_detail {
        Some(detail) if detail.status == BeadStatus::InProgress => {
            format!("br shows it is already {}", detail.status)
        }
        Some(detail) if matches!(detail.status, BeadStatus::Closed | BeadStatus::Deferred) => {
            format!("br shows it is already {}", detail.status)
        }
        Some(detail) => {
            let blockers = detail
                .dependencies
                .iter()
                .filter(|dependency| {
                    dependency.kind == DependencyKind::Blocks
                        && dependency.status != Some(BeadStatus::Closed)
                })
                .map(|dependency| dependency.id.clone())
                .collect::<Vec<_>>();
            if blockers.is_empty() {
                "br ready did not confirm it as actionable".to_owned()
            } else {
                format!(
                    "br shows it is blocked by {}",
                    summarize_bead_ids(&blockers)
                )
            }
        }
        None => "br show could not find the recommended bead".to_owned(),
    };

    format!(
        "bv recommended bead '{recommendation_id}', but {detail_summary}; {ready_summary} for milestone '{milestone_id}'"
    )
}

fn transition_controller_for_selection_outcome(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    state: MilestoneControllerState,
    bead_id: Option<&str>,
    reason: String,
    now: chrono::DateTime<Utc>,
) -> AppResult<milestone_controller::MilestoneControllerRecord> {
    let mut request = milestone_controller::ControllerTransitionRequest::new(state, reason);
    if let Some(bead_id) = bead_id {
        request = request.with_bead(bead_id);
    }

    milestone_controller::sync_controller_state(
        &FsMilestoneControllerStore,
        base_dir,
        milestone_id,
        request,
        now,
    )
}

async fn complete_next_milestone_bead_selection<R: ProcessRunner>(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    br: &BrAdapter<R>,
    planned_refs: &HashSet<String>,
    recommendation: NextRecommendationOutcome,
    now: chrono::DateTime<Utc>,
) -> AppResult<milestone_controller::MilestoneControllerRecord> {
    match &recommendation {
        NextRecommendationOutcome::Recommended(recommendation) => {
            // Persist the raw bv recommendation before validation so operators
            // can inspect the exact candidate even if `br ready` fails later.
            persist_next_step_recommendation(base_dir, milestone_id, recommendation);
        }
        NextRecommendationOutcome::NoRecommendation => {
            delete_persisted_next_step_recommendation(base_dir, milestone_id);
        }
    }

    let ready_beads = match request_ready_milestone_beads(br, milestone_id, planned_refs).await {
        Ok(ready_beads) => ready_beads,
        Err(reason) => {
            let reason = match &recommendation {
                NextRecommendationOutcome::Recommended(recommendation) => format!(
                    "{reason}; bv recommended bead '{}' before validation failed",
                    recommendation.id
                ),
                NextRecommendationOutcome::NoRecommendation => reason,
            };
            return transition_controller_for_selection_outcome(
                base_dir,
                milestone_id,
                MilestoneControllerState::NeedsOperator,
                None,
                reason,
                now,
            );
        }
    };

    match recommendation {
        NextRecommendationOutcome::Recommended(recommendation) => {
            let matching_ready_bead = ready_beads.iter().find(|ready_bead| {
                milestone_bead_refs_match(milestone_id, &ready_bead.id, &recommendation.id)
            });

            if let Some(ready_bead) = matching_ready_bead {
                return transition_controller_for_selection_outcome(
                    base_dir,
                    milestone_id,
                    MilestoneControllerState::Claimed,
                    Some(&ready_bead.id),
                    format!(
                        "bv recommended bead '{}' and br confirmed it is ready to claim",
                        recommendation.id
                    ),
                    now,
                );
            }

            let reason = if !milestone_planned_refs_contain(
                planned_refs,
                milestone_id,
                &recommendation.id,
            ) {
                let ready_ids = ready_beads
                    .iter()
                    .map(|bead| bead.id.clone())
                    .collect::<Vec<_>>();
                let ready_summary = if ready_ids.is_empty() {
                    "no milestone beads are ready".to_owned()
                } else {
                    format!("ready milestone beads: {}", summarize_bead_ids(&ready_ids))
                };
                format!(
                    "bv recommended bead '{}', but it is not part of milestone '{}'; {}",
                    recommendation.id, milestone_id, ready_summary
                )
            } else {
                let bead_detail =
                    match request_bead_detail(br, milestone_id, &recommendation.id).await {
                        Ok(bead_detail) => bead_detail,
                        Err(reason) => {
                            return transition_controller_for_selection_outcome(
                                base_dir,
                                milestone_id,
                                MilestoneControllerState::NeedsOperator,
                                None,
                                reason,
                                now,
                            )
                        }
                    };
                blocked_reason_for_recommendation_mismatch(
                    milestone_id,
                    &recommendation,
                    &ready_beads,
                    bead_detail.as_ref(),
                )
            };

            transition_controller_for_selection_outcome(
                base_dir,
                milestone_id,
                MilestoneControllerState::Blocked,
                None,
                reason,
                now,
            )
        }
        NextRecommendationOutcome::NoRecommendation => {
            let ready_ids = ready_beads
                .iter()
                .map(|bead| bead.id.clone())
                .collect::<Vec<_>>();
            let reason = if ready_ids.is_empty() {
                format!(
                    "bv reported no actionable bead and br confirmed that milestone '{}' has no ready beads",
                    milestone_id
                )
            } else {
                // Treat this as blocked instead of needs-operator: both tools
                // executed successfully, no incorrect claim was made, and the
                // safe response is to stop automatic claiming until the graph
                // or recommendation data converges on a later retry.
                format!(
                    "bv reported no actionable bead, but br ready found milestone beads: {}",
                    summarize_bead_ids(&ready_ids)
                )
            };

            transition_controller_for_selection_outcome(
                base_dir,
                milestone_id,
                MilestoneControllerState::Blocked,
                None,
                reason,
                now,
            )
        }
    }
}

pub(crate) async fn select_next_milestone_bead_from_recommendation<R: ProcessRunner>(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    br: &BrAdapter<R>,
    recommendation: Option<NextBeadResponse>,
    now: chrono::DateTime<Utc>,
) -> AppResult<milestone_controller::MilestoneControllerRecord> {
    let planned_refs = load_planned_bead_membership_refs(base_dir, milestone_id)?;

    milestone_controller::sync_controller_state(
        &FsMilestoneControllerStore,
        base_dir,
        milestone_id,
        milestone_controller::ControllerTransitionRequest::new(
            MilestoneControllerState::Selecting,
            "validating a pre-fetched bv recommendation against br readiness before any claim",
        ),
        now,
    )?;

    let recommendation = match recommendation {
        Some(recommendation) => NextRecommendationOutcome::Recommended(recommendation),
        None => NextRecommendationOutcome::NoRecommendation,
    };

    complete_next_milestone_bead_selection(
        base_dir,
        milestone_id,
        br,
        &planned_refs,
        recommendation,
        now,
    )
    .await
}

pub(crate) async fn select_next_milestone_bead<R: ProcessRunner, V: BvProcessRunner>(
    base_dir: &std::path::Path,
    milestone_id: &MilestoneId,
    br: &BrAdapter<R>,
    bv: &BvAdapter<V>,
    now: chrono::DateTime<Utc>,
) -> AppResult<milestone_controller::MilestoneControllerRecord> {
    let planned_refs = load_planned_bead_membership_refs(base_dir, milestone_id)?;

    milestone_controller::sync_controller_state(
        &FsMilestoneControllerStore,
        base_dir,
        milestone_id,
        milestone_controller::ControllerTransitionRequest::new(
            MilestoneControllerState::Selecting,
            "requesting the next bead recommendation from bv before any claim",
        ),
        now,
    )?;

    let recommendation = match request_next_recommendation(bv).await {
        Ok(recommendation) => recommendation,
        Err(reason) => {
            return transition_controller_for_selection_outcome(
                base_dir,
                milestone_id,
                MilestoneControllerState::NeedsOperator,
                None,
                reason,
                now,
            )
        }
    };

    complete_next_milestone_bead_selection(
        base_dir,
        milestone_id,
        br,
        &planned_refs,
        recommendation,
        now,
    )
    .await
}

fn controller_requires_terminal_task_sync(
    controller: &milestone_controller::MilestoneControllerRecord,
    bead_id: &str,
    task_id: &str,
) -> bool {
    match controller.state {
        MilestoneControllerState::Idle => true,
        MilestoneControllerState::Selecting
        | MilestoneControllerState::Blocked
        | MilestoneControllerState::Completed => false,
        MilestoneControllerState::Claimed
        | MilestoneControllerState::Running
        | MilestoneControllerState::Reconciling
        | MilestoneControllerState::NeedsOperator => {
            controller.active_bead_id.as_deref() == Some(bead_id)
                && controller.active_task_id.as_deref() == Some(task_id)
        }
    }
}

async fn continue_selecting_milestone_bead_if_needed<R: ProcessRunner, V: BvProcessRunner>(
    base_dir: &std::path::Path,
    project_record: &ProjectRecord,
    final_snapshot: &RunSnapshot,
    br: &BrAdapter<R>,
    bv: &BvAdapter<V>,
    now: chrono::DateTime<Utc>,
) -> AppResult<()> {
    if final_snapshot.status != RunStatus::Completed {
        return Ok(());
    }

    let Some(task_source) = project_record.task_source.as_ref() else {
        return Ok(());
    };

    let milestone_id = MilestoneId::new(&task_source.milestone_id)?;
    let Some(controller) = milestone_controller::load_controller(
        &FsMilestoneControllerStore,
        base_dir,
        &milestone_id,
    )?
    else {
        return Ok(());
    };

    if !matches!(
        controller.state,
        MilestoneControllerState::Selecting | MilestoneControllerState::Blocked
    ) {
        return Ok(());
    }

    let selection_at = std::cmp::max(now, controller.last_transition_at);
    if let Err(error) =
        select_next_milestone_bead(base_dir, &milestone_id, br, bv, selection_at).await
    {
        let reason = format!("post-sync bead selection failed: {error}");
        tracing::warn!(
            milestone_id = milestone_id.as_str(),
            %error,
            "persisting needs_operator after selection failure on CLI sync path"
        );
        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
            milestone_controller::ControllerTransitionRequest::new(
                MilestoneControllerState::NeedsOperator,
                reason,
            ),
            selection_at,
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn sync_terminal_milestone_task_and_continue_selection_with_options<
    R: ProcessRunner,
    V: BvProcessRunner,
>(
    base_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
    project_record: &ProjectRecord,
    final_snapshot: &RunSnapshot,
    allow_missing_lineage_repair: bool,
    br: &BrAdapter<R>,
    bv: &BvAdapter<V>,
    now: chrono::DateTime<Utc>,
) -> AppResult<bool> {
    let synced = sync_terminal_milestone_task_with_options(
        base_dir,
        project_id,
        project_record,
        final_snapshot,
        allow_missing_lineage_repair,
    )?;
    if synced {
        continue_selecting_milestone_bead_if_needed(
            base_dir,
            project_record,
            final_snapshot,
            br,
            bv,
            now,
        )
        .await?;
    }
    Ok(synced)
}

async fn sync_terminal_milestone_task_and_continue_selection<
    R: ProcessRunner,
    V: BvProcessRunner,
>(
    base_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
    project_record: &ProjectRecord,
    final_snapshot: &RunSnapshot,
    br: &BrAdapter<R>,
    bv: &BvAdapter<V>,
    now: chrono::DateTime<Utc>,
) -> AppResult<bool> {
    sync_terminal_milestone_task_and_continue_selection_with_options(
        base_dir,
        project_id,
        project_record,
        final_snapshot,
        true,
        br,
        bv,
        now,
    )
    .await
}

impl MilestoneControllerResumePort for ProjectMilestoneControllerRuntime<'_> {
    fn bead_status(&self, bead_id: &str) -> AppResult<ControllerBeadStatus> {
        let output = Command::new("br")
            .args(["show", bead_id, "--json"])
            .current_dir(self.base_dir)
            .output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
            let stdout = String::from_utf8_lossy(&output.stdout).to_ascii_lowercase();
            if stderr.contains("not found")
                || stderr.contains("unknown")
                || stdout.contains("not found")
                || stdout.contains("unknown")
            {
                return Ok(ControllerBeadStatus::Missing);
            }
            return Err(AppError::ResumeFailed {
                reason: format!(
                    "milestone controller resume could not query bead '{bead_id}': {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            });
        }

        let detail: BeadDetail = serde_json::from_slice::<BrShowResponse>(&output.stdout)
            .map(|response| match response {
                BrShowResponse::Single(detail) => detail,
                BrShowResponse::Many(mut details) => details.remove(0),
            })
            .map_err(|error| AppError::ResumeFailed {
                reason: format!(
                    "milestone controller resume could not parse bead '{bead_id}': {error}"
                ),
            })?;
        Ok(match detail.status {
            BeadStatus::Closed => ControllerBeadStatus::Closed,
            _ => ControllerBeadStatus::Open,
        })
    }

    fn task_status(&self, task_id: &str) -> AppResult<ControllerTaskStatus> {
        let project_id =
            ProjectId::new(task_id.to_owned()).map_err(|error| AppError::ResumeFailed {
                reason: format!("controller task identifier '{task_id}' is invalid: {error}"),
            })?;
        if !FsProjectStore.project_exists(self.base_dir, &project_id)? {
            return Ok(ControllerTaskStatus::Missing);
        }

        let snapshot = FsRunSnapshotStore.read_run_snapshot(self.base_dir, &project_id)?;
        Ok(match snapshot.status {
            RunStatus::Running => ControllerTaskStatus::Running,
            RunStatus::Completed => ControllerTaskStatus::Succeeded,
            RunStatus::NotStarted | RunStatus::Paused | RunStatus::Failed => {
                ControllerTaskStatus::Pending
            }
        })
    }

    fn has_ready_beads(&self) -> AppResult<bool> {
        let ready: Vec<ReadyBead> =
            self.query_br_json(&["ready", "--json"], "milestone controller resume")?;
        let planned_refs = self.planned_bead_membership_refs()?;
        Ok(ready.iter().any(|bead| {
            milestone_planned_refs_contain(&planned_refs, self.milestone_id, bead.id.as_str())
        }))
    }

    fn all_beads_closed(&self) -> AppResult<bool> {
        let snapshot = milestone_service::load_snapshot(
            &FsMilestoneSnapshotStore,
            self.base_dir,
            self.milestone_id,
        )?;
        let closed_beads = snapshot
            .progress
            .completed_beads
            .saturating_add(snapshot.progress.skipped_beads);
        Ok(snapshot.status == MilestoneStatus::Completed
            || (snapshot.progress.total_beads > 0
                && closed_beads >= snapshot.progress.total_beads
                && snapshot.progress.in_progress_beads == 0
                && snapshot.progress.failed_beads == 0))
    }
}

fn prepare_milestone_controller_for_execution(
    base_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
    project_record: &ProjectRecord,
    origin: MilestoneControllerExecutionOrigin,
) -> AppResult<()> {
    let Some(task_source) = project_record.task_source.as_ref() else {
        return Ok(());
    };

    let milestone_id = MilestoneId::new(&task_source.milestone_id)
        .map_err(|error| origin.error(error.to_string()))?;
    let runtime = ProjectMilestoneControllerRuntime {
        base_dir,
        milestone_id: &milestone_id,
    };
    let controller = milestone_controller::resume_controller(
        &FsMilestoneControllerStore,
        &runtime,
        base_dir,
        &milestone_id,
        chrono::Utc::now(),
    )
    .map_err(|error| origin.error(error.to_string()))?;
    if controller.state == MilestoneControllerState::NeedsOperator {
        return Err(origin.error(
            controller.last_transition_reason.unwrap_or_else(|| {
                "milestone controller requires operator intervention".to_owned()
            }),
        ));
    }

    milestone_controller::sync_controller_task_claimed(
        &FsMilestoneControllerStore,
        base_dir,
        &milestone_id,
        &task_source.bead_id,
        project_id.as_str(),
        match origin {
            MilestoneControllerExecutionOrigin::Start => {
                "preparing the bead-linked project for execution"
            }
            MilestoneControllerExecutionOrigin::Resume => {
                "preparing the bead-linked project to resume execution"
            }
        },
        chrono::Utc::now(),
    )
    .map_err(|error| origin.error(error.to_string()))?;

    Ok(())
}

#[allow(dead_code)]
fn sync_terminal_milestone_task(
    base_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
    project_record: &ProjectRecord,
    final_snapshot: &RunSnapshot,
) -> AppResult<bool> {
    sync_terminal_milestone_task_with_options(
        base_dir,
        project_id,
        project_record,
        final_snapshot,
        true,
    )
}

enum MissingLineageRepairGuard {
    Allow,
    BlockedByActiveAttempt,
    AmbiguousActiveAttempts,
}

fn sync_terminal_milestone_task_with_options(
    base_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
    project_record: &ProjectRecord,
    final_snapshot: &RunSnapshot,
    allow_missing_lineage_repair: bool,
) -> AppResult<bool> {
    let Some(task_source) = project_record.task_source.as_ref() else {
        return Ok(false);
    };

    let milestone_id = MilestoneId::new(&task_source.milestone_id)?;
    if final_snapshot.status == RunStatus::Paused {
        milestone_controller::sync_controller_task_claimed(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "workflow execution paused; the bead-linked project remains claimed for resume",
            chrono::Utc::now(),
        )?;
        return Ok(false);
    }

    let (outcome, outcome_detail, disposition) = match final_snapshot.status {
        RunStatus::Completed => (
            TaskRunOutcome::Succeeded,
            None,
            CompletionMilestoneDisposition::ReconcileFromLineage,
        ),
        RunStatus::Failed => {
            let detail = final_snapshot.status_summary.trim();
            let detail = (!detail.is_empty()).then(|| detail.to_owned());
            (
                TaskRunOutcome::Failed,
                detail,
                CompletionMilestoneDisposition::ReconcileFromLineage,
            )
        }
        RunStatus::NotStarted | RunStatus::Running | RunStatus::Paused => return Ok(false),
    };
    let _ = repair_missing_signal_handoff_run_failed_event(base_dir, project_id, final_snapshot)?;
    let journal_events = FsJournalStore.read_journal(base_dir, project_id)?;
    let matching_lineage_run = milestone_service::find_runs_for_bead(
        &FsTaskRunLineageStore,
        base_dir,
        &milestone_id,
        &task_source.bead_id,
    )?;
    let run_started = journal_events
        .iter()
        .rev()
        .find(|event| event.event_type == JournalEventType::RunStarted);
    let mut same_named_terminal_attempt_exists = false;
    let (run_id, started_at) = match run_started {
        Some(run_started) => {
            let run_id = run_started
                .details
                .get("run_id")
                .and_then(Value::as_str)
                .ok_or_else(|| AppError::CorruptRecord {
                    file: format!("projects/{}/journal.ndjson", project_id),
                    details: "run_started event is missing string run_id details".to_owned(),
                })?;
            let attempt_started_at =
                effective_attempt_started_at(&journal_events, run_id, run_started.timestamp);
            let has_exact_lineage = matching_lineage_run.iter().any(|entry| {
                lineage_entry_matches_attempt(
                    entry,
                    project_id.as_str(),
                    run_id,
                    attempt_started_at,
                )
            });
            same_named_terminal_attempt_exists = matching_lineage_run.iter().any(|entry| {
                entry.project_id == project_id.as_str()
                    && entry.run_id.as_deref() == Some(run_id)
                    && entry.outcome.is_terminal()
            });
            if !has_exact_lineage {
                if !allow_missing_lineage_repair {
                    return Ok(false);
                }
                if !same_named_terminal_attempt_exists {
                    match missing_lineage_repair_guard(
                        &matching_lineage_run,
                        project_id,
                        run_id,
                        attempt_started_at,
                    ) {
                        MissingLineageRepairGuard::Allow => {}
                        MissingLineageRepairGuard::BlockedByActiveAttempt => return Ok(false),
                        MissingLineageRepairGuard::AmbiguousActiveAttempts => {
                            return Err(AppError::CorruptRecord {
                                file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                                details: format!(
                                    "cannot repair missing lineage for bead={} project={} run_id={}: multiple active lineage rows exist; manual cleanup required",
                                    task_source.bead_id, project_id, run_id
                                ),
                            });
                        }
                    }
                    let plan_hash = engine::milestone_lineage_plan_hash(
                        project_record,
                        base_dir,
                        project_id,
                        &milestone_id,
                        &task_source.bead_id,
                        run_id,
                    )?;
                    milestone_service::record_bead_start(
                        &FsMilestoneSnapshotStore,
                        &FsMilestoneJournalStore,
                        &FsTaskRunLineageStore,
                        base_dir,
                        &milestone_id,
                        &task_source.bead_id,
                        project_id.as_str(),
                        run_id,
                        &plan_hash,
                        attempt_started_at,
                    )?;
                }
            }
            (run_id, attempt_started_at)
        }
        None => {
            let mut repairable_entries = matching_lineage_run.iter().filter(|entry| {
                entry.project_id == project_id.as_str() && !entry.outcome.is_terminal()
            });
            let Some(entry) = repairable_entries.next() else {
                return Ok(false);
            };
            if repairable_entries.next().is_some() {
                return Err(AppError::CorruptRecord {
                    file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                    details: format!(
                        "multiple active lineage rows found for bead={} project={}",
                        task_source.bead_id, project_id
                    ),
                });
            }
            let run_id = entry
                .run_id
                .as_deref()
                .ok_or_else(|| AppError::CorruptRecord {
                    file: format!("milestones/{}/task-runs.ndjson", milestone_id),
                    details: format!(
                        "active lineage row for bead={} project={} is missing run_id",
                        task_source.bead_id, project_id
                    ),
                })?;
            (run_id, entry.started_at)
        }
    };
    let finished_at = match final_snapshot.status {
        RunStatus::Completed => {
            let Some(timestamp) =
                terminal_run_event_timestamp(&journal_events, run_id, RunStatus::Completed)
            else {
                return Err(AppError::CorruptRecord {
                    file: format!("projects/{}/journal.ndjson", project_id),
                    details: format!(
                        "run snapshot is completed but missing durable run_completed event for run_id={run_id}"
                    ),
                });
            };
            timestamp
        }
        RunStatus::Failed => {
            let Some(timestamp) =
                terminal_run_event_timestamp(&journal_events, run_id, RunStatus::Failed)
            else {
                return Err(AppError::CorruptRecord {
                    file: format!("projects/{}/journal.ndjson", project_id),
                    details: format!(
                        "run snapshot is failed but missing durable run_failed event for run_id={run_id}"
                    ),
                });
            };
            timestamp
        }
        RunStatus::NotStarted | RunStatus::Running | RunStatus::Paused => unreachable!(),
    };

    let exact_attempt_already_terminal = same_named_terminal_attempt_exists
        || matching_lineage_run.iter().any(|entry| {
            lineage_entry_matches_attempt(entry, project_id.as_str(), run_id, started_at)
                && entry.outcome.is_terminal()
        });
    let reconcile_controller = if exact_attempt_already_terminal {
        match milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
        )? {
            Some(controller) => controller_requires_terminal_task_sync(
                &controller,
                &task_source.bead_id,
                project_id.as_str(),
            ),
            None => true,
        }
    } else {
        true
    };

    if reconcile_controller {
        match final_snapshot.status {
            RunStatus::Completed => {
                milestone_controller::sync_controller_task_reconciling(
                    &FsMilestoneControllerStore,
                    base_dir,
                    &milestone_id,
                    &task_source.bead_id,
                    project_id.as_str(),
                    "workflow execution completed; reconciling milestone state",
                    finished_at,
                )?;
            }
            RunStatus::Failed => {
                milestone_controller::sync_controller_task_claimed(
                    &FsMilestoneControllerStore,
                    base_dir,
                    &milestone_id,
                    &task_source.bead_id,
                    project_id.as_str(),
                    "workflow attempt failed; the bead-linked project remains claimed for resume",
                    finished_at,
                )?;
            }
            RunStatus::NotStarted | RunStatus::Running | RunStatus::Paused => unreachable!(),
        }
    }

    if exact_attempt_already_terminal {
        milestone_service::repair_task_run_with_disposition(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            run_id,
            task_source.plan_hash.as_deref(),
            started_at,
            outcome,
            outcome_detail,
            finished_at,
            disposition,
        )?;
    } else {
        milestone_service::record_bead_completion_with_disposition(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            run_id,
            task_source.plan_hash.as_deref(),
            outcome,
            outcome_detail.as_deref(),
            started_at,
            finished_at,
            disposition,
        )?;
    }

    if final_snapshot.status == RunStatus::Completed && reconcile_controller {
        let snapshot =
            milestone_service::load_snapshot(&FsMilestoneSnapshotStore, base_dir, &milestone_id)?;
        let (next_state, reason) = if snapshot.status == MilestoneStatus::Completed {
            (
                MilestoneControllerState::Completed,
                "reconciliation closed the final bead and completed the milestone",
            )
        } else {
            (
                MilestoneControllerState::Selecting,
                "reconciliation recorded the bead outcome and returned the controller to bead selection",
            )
        };
        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
            milestone_controller::ControllerTransitionRequest::new(next_state, reason),
            finished_at,
        )?;
    }

    Ok(true)
}

fn missing_lineage_repair_guard(
    matching_lineage_run: &[crate::contexts::milestone_record::model::TaskRunEntry],
    project_id: &crate::shared::domain::ProjectId,
    run_id: &str,
    started_at: chrono::DateTime<chrono::Utc>,
) -> MissingLineageRepairGuard {
    let mut active_attempts = matching_lineage_run.iter().filter(|entry| {
        !entry.outcome.is_terminal()
            && (entry.project_id != project_id.as_str() || entry.run_id.as_deref() != Some(run_id))
    });
    let Some(first_active_attempt) = active_attempts.next() else {
        return MissingLineageRepairGuard::Allow;
    };
    if active_attempts.next().is_some() {
        return MissingLineageRepairGuard::AmbiguousActiveAttempts;
    }

    if first_active_attempt.started_at > started_at
        || (first_active_attempt.started_at == started_at
            && (first_active_attempt.project_id != project_id.as_str()
                || first_active_attempt.run_id.as_deref() != Some(run_id)))
    {
        MissingLineageRepairGuard::BlockedByActiveAttempt
    } else {
        MissingLineageRepairGuard::Allow
    }
}

fn journal_run_id(event: &JournalEvent) -> Option<&str> {
    event.details.get("run_id").and_then(Value::as_str)
}

fn lineage_entry_matches_attempt(
    entry: &crate::contexts::milestone_record::model::TaskRunEntry,
    project_id: &str,
    run_id: &str,
    started_at: chrono::DateTime<chrono::Utc>,
) -> bool {
    entry.project_id == project_id
        && entry.run_id.as_deref() == Some(run_id)
        && entry.started_at == started_at
}

fn effective_attempt_started_at(
    journal_events: &[JournalEvent],
    run_id: &str,
    run_started_at: chrono::DateTime<chrono::Utc>,
) -> chrono::DateTime<chrono::Utc> {
    journal_events
        .iter()
        .rev()
        .find(|event| {
            event.event_type == JournalEventType::RunResumed
                && journal_run_id(event) == Some(run_id)
        })
        .map(|event| event.timestamp)
        .unwrap_or(run_started_at)
}

fn terminal_run_event_timestamp(
    journal_events: &[JournalEvent],
    run_id: &str,
    status: RunStatus,
) -> Option<chrono::DateTime<chrono::Utc>> {
    let terminal_event_type = match status {
        RunStatus::Completed => JournalEventType::RunCompleted,
        RunStatus::Failed => JournalEventType::RunFailed,
        RunStatus::NotStarted | RunStatus::Running | RunStatus::Paused => return None,
    };

    journal_events
        .iter()
        .rev()
        .find(|event| {
            event.event_type == terminal_event_type && journal_run_id(event) == Some(run_id)
        })
        .map(|event| event.timestamp)
}

fn decorate_sync_error(error: AppError, resume: bool) -> AppError {
    let reason = format!("milestone task sync failed: {error}");
    if resume {
        AppError::ResumeFailed { reason }
    } else {
        AppError::RunStartFailed { reason }
    }
}

fn combine_run_and_sync_error(run_error: AppError, sync_error: AppError, resume: bool) -> AppError {
    let reason = format!("{run_error}; milestone task sync also failed: {sync_error}");
    if resume {
        AppError::ResumeFailed { reason }
    } else {
        AppError::RunStartFailed { reason }
    }
}

fn resume_attempt_has_exact_lineage(
    base_dir: &std::path::Path,
    project_id: &crate::shared::domain::ProjectId,
    project_record: &ProjectRecord,
    final_snapshot: &RunSnapshot,
) -> AppResult<bool> {
    let Some(task_source) = project_record.task_source.as_ref() else {
        return Ok(false);
    };
    let Some(run_id) = final_snapshot
        .interrupted_run
        .as_ref()
        .map(|run| run.run_id.as_str())
    else {
        return Ok(false);
    };
    let milestone_id = MilestoneId::new(&task_source.milestone_id)?;
    let matching_lineage_run = milestone_service::find_runs_for_bead(
        &FsTaskRunLineageStore,
        base_dir,
        &milestone_id,
        &task_source.bead_id,
    )?;
    let started_at = final_snapshot
        .interrupted_run
        .as_ref()
        .map(|run| run.started_at)
        .expect("interrupted run presence already checked");
    Ok(matching_lineage_run
        .iter()
        .any(|entry| lineage_entry_matches_attempt(entry, project_id.as_str(), run_id, started_at)))
}

enum RunningSnapshotLiveness {
    CliProcess(crate::adapters::fs::RunPidRecord),
    DaemonProcess(crate::adapters::fs::RunPidRecord),
    LegacyCliProcess,
    LegacyDaemonProcess,
    LegacyCliLease,
    LegacyDaemonLease,
    Stale,
}

fn pid_record_has_attempt_metadata(record: &crate::adapters::fs::RunPidRecord) -> bool {
    record.run_id.is_some() && record.run_started_at.is_some()
}

fn classify_running_snapshot_liveness_with_ignored_cli_lease_owner(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    ignored_cli_lease_owner: Option<&str>,
) -> AppResult<RunningSnapshotLiveness> {
    let pid_record = FileSystem::read_pid_file(base_dir, project_id)?;
    if let Some(record) = pid_record.as_ref() {
        if FileSystem::pid_record_matches_live_process(record) {
            if FileSystem::pid_record_is_authoritative(record) {
                return Ok(
                    match (
                        record.owner.clone(),
                        pid_record_has_attempt_metadata(record),
                    ) {
                        (crate::adapters::fs::RunPidOwner::Cli, true) => {
                            RunningSnapshotLiveness::CliProcess(record.clone())
                        }
                        (crate::adapters::fs::RunPidOwner::Daemon, true) => {
                            RunningSnapshotLiveness::DaemonProcess(record.clone())
                        }
                        (crate::adapters::fs::RunPidOwner::Cli, false) => {
                            RunningSnapshotLiveness::LegacyCliProcess
                        }
                        (crate::adapters::fs::RunPidOwner::Daemon, false) => {
                            RunningSnapshotLiveness::LegacyDaemonProcess
                        }
                    },
                );
            }
            return Ok(match record.owner {
                crate::adapters::fs::RunPidOwner::Cli => RunningSnapshotLiveness::LegacyCliProcess,
                crate::adapters::fs::RunPidOwner::Daemon => {
                    RunningSnapshotLiveness::LegacyDaemonProcess
                }
            });
        }

        if FileSystem::pid_record_is_authoritative(record) {
            return Ok(RunningSnapshotLiveness::Stale);
        }
    }

    if let Some((_owner, lease_record)) =
        read_project_writer_lease_record(&FsDaemonStore, base_dir, project_id)?
    {
        match lease_record {
            LeaseRecord::Worktree(lease) if lease.project_id == project_id.as_str() => {
                return Ok(if lease.is_stale_at(Utc::now()) {
                    RunningSnapshotLiveness::Stale
                } else {
                    RunningSnapshotLiveness::LegacyDaemonLease
                });
            }
            LeaseRecord::CliWriter(lease)
                if lease.project_id == project_id.as_str()
                    && ignored_cli_lease_owner != Some(lease.lease_id.as_str()) =>
            {
                // Mixed-version CLI runs may still hold a fresh writer lease
                // without an authoritative pid tuple. Treat them as running so
                // resume/stop never reclaim the lock or rewrite state until the
                // lease itself goes stale.
                return Ok(if lease.is_stale_at(Utc::now()) {
                    RunningSnapshotLiveness::Stale
                } else {
                    RunningSnapshotLiveness::LegacyCliLease
                });
            }
            _ => {}
        }
    }

    Ok(RunningSnapshotLiveness::Stale)
}

fn classify_running_snapshot_liveness(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
) -> AppResult<RunningSnapshotLiveness> {
    classify_running_snapshot_liveness_with_ignored_cli_lease_owner(base_dir, project_id, None)
}

fn running_snapshot_attempt(snapshot: &RunSnapshot) -> AppResult<engine::RunningAttemptIdentity> {
    let Some(active_run) = snapshot.active_run.as_ref() else {
        return Err(AppError::CorruptRecord {
            file: "run.json".to_owned(),
            details: "running snapshot is missing active_run".to_owned(),
        });
    };
    Ok(engine::RunningAttemptIdentity::from_active_run(active_run))
}

fn liveness_matches_running_attempt(
    liveness: &RunningSnapshotLiveness,
    expected_attempt: &engine::RunningAttemptIdentity,
) -> bool {
    match liveness {
        RunningSnapshotLiveness::CliProcess(record)
        | RunningSnapshotLiveness::DaemonProcess(record) => FileSystem::pid_record_matches_attempt(
            record,
            &expected_attempt.run_id,
            expected_attempt.started_at,
        ),
        RunningSnapshotLiveness::LegacyCliProcess
        | RunningSnapshotLiveness::LegacyDaemonProcess
        | RunningSnapshotLiveness::LegacyCliLease
        | RunningSnapshotLiveness::LegacyDaemonLease
        | RunningSnapshotLiveness::Stale => true,
    }
}

fn observed_running_attempt_owner(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    expected_attempt: &engine::RunningAttemptIdentity,
) -> AppResult<Option<String>> {
    if let Some(pid_record) = FileSystem::read_pid_file(base_dir, project_id)? {
        if FileSystem::pid_record_matches_attempt(
            &pid_record,
            &expected_attempt.run_id,
            expected_attempt.started_at,
        ) && pid_record.writer_owner.is_some()
        {
            return Ok(pid_record.writer_owner);
        }
    }
    read_project_writer_lock_owner(base_dir, project_id)
}

fn stop_target_pid_record_is_unchanged(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    expected_attempt: &engine::RunningAttemptIdentity,
    observed_pid_record: &crate::adapters::fs::RunPidRecord,
) -> AppResult<bool> {
    let Some(current_pid_record) = FileSystem::read_pid_file(base_dir, project_id)? else {
        // PID file was removed — the orchestrator exited cleanly between
        // liveness classification and this revalidation.  Treat as
        // unchanged so the caller falls through to the post-exit path
        // instead of reporting a spurious handoff error.
        return Ok(true);
    };

    Ok(current_pid_record == *observed_pid_record
        && FileSystem::pid_record_matches_attempt(
            &current_pid_record,
            &expected_attempt.run_id,
            expected_attempt.started_at,
        ))
}

fn observed_writer_owner_is_stale(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    expected_owner: &str,
) -> AppResult<bool> {
    let Some(current_owner) = read_project_writer_lock_owner(base_dir, project_id)? else {
        return Ok(false);
    };
    if current_owner != expected_owner {
        return Ok(false);
    }

    let Some((owner, record)) =
        read_project_writer_lease_record(&FsDaemonStore, base_dir, project_id)?
    else {
        return Ok(true);
    };
    if owner != expected_owner {
        return Ok(false);
    }

    let now = Utc::now();
    Ok(match record {
        LeaseRecord::CliWriter(lease) => {
            lease.project_id == project_id.as_str() && lease.is_stale_at(now)
        }
        LeaseRecord::Worktree(lease) => {
            lease.project_id == project_id.as_str() && lease.is_stale_at(now)
        }
    })
}

fn observed_cli_cleanup_owner_has_dead_pid(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    expected_owner: &str,
) -> AppResult<bool> {
    let Some((owner, record)) =
        read_project_writer_lease_record(&FsDaemonStore, base_dir, project_id)?
    else {
        return Ok(false);
    };
    if owner != expected_owner {
        return Ok(false);
    }

    let LeaseRecord::CliWriter(lease) = record else {
        return Ok(false);
    };
    if lease.project_id != project_id.as_str() {
        return Ok(false);
    }

    if let Some(pid_record) = FileSystem::read_pid_file(base_dir, project_id)? {
        if pid_record.owner != crate::adapters::fs::RunPidOwner::Cli {
            return Ok(false);
        }
        if pid_record.writer_owner.as_deref() != Some(expected_owner) {
            return Ok(false);
        }
        if !FileSystem::pid_record_is_authoritative(&pid_record) {
            return Ok(false);
        }

        return Ok(!FileSystem::is_pid_alive(&pid_record));
    }

    let Some(handoff) = lease.cleanup_handoff.as_ref() else {
        return Ok(false);
    };
    if !FileSystem::process_identity_is_authoritative(
        handoff.proc_start_ticks,
        handoff.proc_start_marker.as_deref(),
    ) {
        return Ok(false);
    }

    Ok(!FileSystem::process_identity_matches_live_process(
        handoff.pid,
        handoff.proc_start_ticks,
        handoff.proc_start_marker.as_deref(),
    ))
}

fn observed_stale_running_owner_still_matches_attempt(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    expected_owner: &str,
    expected_attempt: &engine::RunningAttemptIdentity,
) -> AppResult<bool> {
    let Some(current_owner) = read_project_writer_lock_owner(base_dir, project_id)? else {
        return Ok(false);
    };
    if current_owner != expected_owner {
        return Ok(false);
    }

    if let Some(pid_record) = FileSystem::read_pid_file(base_dir, project_id)? {
        if FileSystem::pid_record_matches_attempt(
            &pid_record,
            &expected_attempt.run_id,
            expected_attempt.started_at,
        ) {
            return Ok(match pid_record.writer_owner.as_deref() {
                Some(writer_owner) => writer_owner == expected_owner,
                None => true,
            });
        }

        if FileSystem::pid_record_is_authoritative(&pid_record)
            && FileSystem::is_pid_alive(&pid_record)
        {
            return Ok(false);
        }
    }

    observed_writer_owner_is_stale(base_dir, project_id, expected_owner)
}

fn acquire_recovery_writer_guard(
    base_dir: &std::path::Path,
    repo_root: &std::path::Path,
    project_id: &ProjectId,
    observed_owner: Option<&str>,
    stale_running_attempt: Option<&engine::RunningAttemptIdentity>,
    observed_owner_proven_stale: bool,
    allow_dead_cli_cleanup_reclaim: bool,
) -> AppResult<CliWriterLeaseGuard> {
    let daemon_store: Arc<dyn crate::contexts::automation_runtime::DaemonStorePort + Send + Sync> =
        Arc::new(FsDaemonStore);
    let acquire = || {
        CliWriterLeaseGuard::acquire(
            Arc::clone(&daemon_store),
            base_dir,
            project_id.clone(),
            CLI_LEASE_TTL_SECONDS,
            CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
        )
    };

    let initial_error = match acquire() {
        Ok(guard) => {
            if let Some(owner) = observed_owner {
                if owner != guard.lease_id() {
                    if let Err(error) = cleanup_detached_project_writer_owner(
                        &FsDaemonStore,
                        &WorktreeAdapter,
                        base_dir,
                        repo_root,
                        project_id,
                        owner,
                    ) {
                        let _ = guard.close();
                        return Err(error);
                    }
                }
            }
            return Ok(guard);
        }
        Err(error) => error,
    };
    if !matches!(initial_error, AppError::ProjectWriterLockHeld { .. }) {
        return Err(initial_error);
    }

    let Some(owner) = observed_owner else {
        return Err(initial_error);
    };
    let owner_is_reclaimable = (if observed_owner_proven_stale {
        match stale_running_attempt {
            Some(expected_attempt) => observed_stale_running_owner_still_matches_attempt(
                base_dir,
                project_id,
                owner,
                expected_attempt,
            )?,
            None => true,
        }
    } else {
        false
    }) || observed_writer_owner_is_stale(base_dir, project_id, owner)?
        || (allow_dead_cli_cleanup_reclaim
            && observed_cli_cleanup_owner_has_dead_pid(base_dir, project_id, owner)?);
    if !owner_is_reclaimable {
        return Err(initial_error);
    }

    let _ = reclaim_specific_project_writer_owner(
        &FsDaemonStore,
        &WorktreeAdapter,
        base_dir,
        repo_root,
        project_id,
        owner,
    )?;

    acquire()
}

struct ResumeRecoveryObservation {
    observed_owner: Option<String>,
    should_reconcile_claimed_running_snapshot: bool,
    allow_dead_cli_cleanup_reclaim: bool,
}

fn observed_resume_recovery_owner(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    snapshot: &RunSnapshot,
) -> AppResult<ResumeRecoveryObservation> {
    match snapshot.status {
        RunStatus::Running => {
            let expected_attempt = running_snapshot_attempt(snapshot)?;
            match classify_running_snapshot_liveness(base_dir, project_id)? {
                RunningSnapshotLiveness::CliProcess(record)
                    if !FileSystem::pid_record_matches_attempt(
                        &record,
                        &expected_attempt.run_id,
                        expected_attempt.started_at,
                    ) =>
                {
                    Err(AppError::ResumeFailed {
                        reason: "project run changed while checking stale state; retry `ralph-burning run resume`".to_owned(),
                    })
                }
                RunningSnapshotLiveness::CliProcess(_) => Err(AppError::ResumeFailed {
                    reason: "project already has a running run; `run resume` only works from failed or paused snapshots".to_owned(),
                }),
                RunningSnapshotLiveness::LegacyCliProcess
                | RunningSnapshotLiveness::LegacyCliLease => Err(AppError::ResumeFailed {
                    reason: "project has a legacy CLI-owned running run without current-version attempt tracking; wait for it to finish or for stale recovery to become safe before resuming".to_owned(),
                }),
                RunningSnapshotLiveness::DaemonProcess(record)
                    if !FileSystem::pid_record_matches_attempt(
                        &record,
                        &expected_attempt.run_id,
                        expected_attempt.started_at,
                    ) =>
                {
                    Err(AppError::ResumeFailed {
                        reason: "project run changed while checking stale state; retry `ralph-burning run resume`".to_owned(),
                    })
                }
                RunningSnapshotLiveness::DaemonProcess(_)
                | RunningSnapshotLiveness::LegacyDaemonProcess
                | RunningSnapshotLiveness::LegacyDaemonLease => Err(AppError::ResumeFailed {
                    reason: "project has a daemon-owned running run; wait for the daemon task to finish or stop it through daemon controls".to_owned(),
                }),
                RunningSnapshotLiveness::Stale => Ok(ResumeRecoveryObservation {
                    observed_owner: observed_running_attempt_owner(
                        base_dir,
                        project_id,
                        &expected_attempt,
                    )?,
                    should_reconcile_claimed_running_snapshot: true,
                    allow_dead_cli_cleanup_reclaim: false,
                }),
            }
        }
        RunStatus::Failed | RunStatus::Paused => Ok(ResumeRecoveryObservation {
            observed_owner: read_project_writer_lock_owner(base_dir, project_id)?.or(
                find_detached_project_writer_owner(&FsDaemonStore, base_dir, project_id)?,
            ),
            should_reconcile_claimed_running_snapshot: false,
            allow_dead_cli_cleanup_reclaim: true,
        }),
        RunStatus::NotStarted | RunStatus::Completed => Ok(ResumeRecoveryObservation {
            observed_owner: None,
            should_reconcile_claimed_running_snapshot: false,
            allow_dead_cli_cleanup_reclaim: false,
        }),
    }
}

fn stale_status_summary() -> String {
    "stale running snapshot: process not found; run `ralph-burning run resume` to recover"
        .to_owned()
}

fn repair_missing_signal_handoff_run_failed_event(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    snapshot: &RunSnapshot,
) -> AppResult<bool> {
    if snapshot.status != RunStatus::Failed
        || snapshot.active_run.is_some()
        || snapshot.status_summary != "failed (interrupted by termination signal)"
    {
        return Ok(false);
    }

    let Some(interrupted_run) = snapshot.interrupted_run.as_ref() else {
        return Ok(false);
    };

    engine::finalize_interrupted_run_failure_if_missing(
        engine::InterruptedRunContext {
            run_snapshot_read: &FsRunSnapshotStore,
            run_snapshot_write: &FsRunSnapshotWriteStore,
            journal_store: &FsJournalStore,
            log_write: &FsRuntimeLogWriteStore,
            base_dir,
            project_id,
        },
        &engine::RunningAttemptIdentity {
            run_id: interrupted_run.run_id.clone(),
            started_at: interrupted_run.started_at,
        },
        "cancellation",
    )
}

fn stale_status_view(project_id: &ProjectId, snapshot: &RunSnapshot) -> RunStatusView {
    let mut status = RunStatusView::from_snapshot(project_id.as_str(), snapshot);
    status.status = "stale (process not found)".to_owned();
    status.summary = stale_status_summary();
    status
}

fn stale_status_json_view(project_id: &ProjectId, snapshot: &RunSnapshot) -> RunStatusJsonView {
    let mut status = RunStatusJsonView::from_snapshot(project_id.as_str(), snapshot);
    status.status = "stale".to_owned();
    status.summary = stale_status_summary();
    status
}

#[derive(Clone, Copy)]
enum RunSignalCleanupOrigin {
    Start,
    Resume,
}

impl RunSignalCleanupOrigin {
    fn error(self, reason: impl Into<String>) -> AppError {
        match self {
            Self::Start => AppError::RunStartFailed {
                reason: reason.into(),
            },
            Self::Resume => AppError::ResumeFailed {
                reason: reason.into(),
            },
        }
    }
}

struct RunSignalCleanupContext<'a> {
    origin: RunSignalCleanupOrigin,
    run_snapshot_read: &'a dyn RunSnapshotPort,
    run_snapshot_write: &'a dyn RunSnapshotWritePort,
    journal_store: &'a dyn JournalStorePort,
    log_write: &'a dyn RuntimeLogWritePort,
    base_dir: &'a std::path::Path,
    project_id: &'a ProjectId,
    writer_owner: Option<&'a str>,
}

fn reconcile_signal_interruption_for_attempt(
    context: &RunSignalCleanupContext<'_>,
    expected_attempt: Option<&engine::RunningAttemptIdentity>,
    update: engine::InterruptedRunUpdate<'_>,
) -> AppResult<bool> {
    let interrupted_context = engine::InterruptedRunContext {
        run_snapshot_read: context.run_snapshot_read,
        run_snapshot_write: context.run_snapshot_write,
        journal_store: context.journal_store,
        log_write: context.log_write,
        base_dir: context.base_dir,
        project_id: context.project_id,
    };

    match expected_attempt {
        Some(expected_attempt) => {
            engine::mark_running_run_interrupted(interrupted_context, expected_attempt, update)
        }
        None => engine::mark_current_process_running_run_interrupted(
            interrupted_context,
            context.writer_owner,
            update,
        ),
    }
}

fn persist_signal_interruption_marker(
    context: &RunSignalCleanupContext<'_>,
    expected_attempt: Option<&engine::RunningAttemptIdentity>,
) -> AppResult<bool> {
    reconcile_signal_interruption_for_attempt(
        context,
        expected_attempt,
        engine::InterruptedRunUpdate {
            summary: "failed (interrupted by termination signal)",
            log_message:
                "termination signal interrupted the orchestrator before graceful shutdown completed",
            failure_class: None,
        },
    )
}

fn persist_signal_cleanup_handoff(
    context: &RunSignalCleanupContext<'_>,
    pid_record: &crate::adapters::fs::RunPidRecord,
) -> AppResult<bool> {
    let Some(writer_owner) = context.writer_owner else {
        return Ok(false);
    };

    persist_cli_writer_cleanup_handoff(
        &FsDaemonStore,
        context.base_dir,
        context.project_id,
        writer_owner,
        CliWriterCleanupHandoff {
            pid: pid_record.pid,
            run_id: pid_record.run_id.clone(),
            run_started_at: pid_record.run_started_at,
            proc_start_ticks: pid_record.proc_start_ticks,
            proc_start_marker: pid_record.proc_start_marker.clone(),
        },
    )
}

fn finalize_signal_interruption_marker(
    context: &RunSignalCleanupContext<'_>,
    expected_attempt: Option<&engine::RunningAttemptIdentity>,
) -> AppResult<bool> {
    let Some(expected_attempt) = expected_attempt else {
        return Ok(false);
    };
    let snapshot = context
        .run_snapshot_read
        .read_run_snapshot(context.base_dir, context.project_id)?;
    if !(snapshot.status == RunStatus::Failed
        && snapshot.active_run.is_none()
        && snapshot.interrupted_run.as_ref().is_some_and(|run| {
            run.run_id == expected_attempt.run_id && run.started_at == expected_attempt.started_at
        }))
    {
        return Ok(false);
    }

    engine::finalize_interrupted_run_failure_if_missing(
        engine::InterruptedRunContext {
            run_snapshot_read: context.run_snapshot_read,
            run_snapshot_write: context.run_snapshot_write,
            journal_store: context.journal_store,
            log_write: context.log_write,
            base_dir: context.base_dir,
            project_id: context.project_id,
        },
        expected_attempt,
        "cancellation",
    )
}

struct PreparedSignalInterruptionHandoff {
    expected_attempt: Option<engine::RunningAttemptIdentity>,
    interrupted_marker_persisted: bool,
}

fn prepare_signal_interruption_handoff(
    context: &RunSignalCleanupContext<'_>,
) -> AppResult<PreparedSignalInterruptionHandoff> {
    let snapshot = context
        .run_snapshot_read
        .read_run_snapshot(context.base_dir, context.project_id)?;
    if snapshot.status != RunStatus::Running {
        return Ok(PreparedSignalInterruptionHandoff {
            expected_attempt: None,
            interrupted_marker_persisted: false,
        });
    }

    let Some(active_run) = snapshot.active_run.as_ref() else {
        return Ok(PreparedSignalInterruptionHandoff {
            expected_attempt: None,
            interrupted_marker_persisted: false,
        });
    };
    let expected_attempt = engine::RunningAttemptIdentity::from_active_run(active_run);
    let Some(pid_record) = FileSystem::read_pid_file(context.base_dir, context.project_id)? else {
        return Ok(PreparedSignalInterruptionHandoff {
            expected_attempt: Some(expected_attempt),
            interrupted_marker_persisted: false,
        });
    };
    if pid_record.pid != std::process::id() {
        return Ok(PreparedSignalInterruptionHandoff {
            expected_attempt: Some(expected_attempt),
            interrupted_marker_persisted: false,
        });
    }
    if let Some(writer_owner) = context.writer_owner {
        if pid_record.writer_owner.as_deref() != Some(writer_owner) {
            return Ok(PreparedSignalInterruptionHandoff {
                expected_attempt: Some(expected_attempt),
                interrupted_marker_persisted: false,
            });
        }
    }
    if !FileSystem::pid_record_matches_attempt(
        &pid_record,
        &expected_attempt.run_id,
        expected_attempt.started_at,
    ) {
        return Ok(PreparedSignalInterruptionHandoff {
            expected_attempt: Some(expected_attempt),
            interrupted_marker_persisted: false,
        });
    }

    let _ = persist_signal_cleanup_handoff(context, &pid_record)?;

    let interrupted_marker_persisted =
        persist_signal_interruption_marker(context, Some(&expected_attempt))?;
    Ok(PreparedSignalInterruptionHandoff {
        expected_attempt: Some(expected_attempt),
        interrupted_marker_persisted,
    })
}

async fn run_with_termination_signal<F>(
    cancellation_token: CancellationToken,
    cleanup_context: RunSignalCleanupContext<'_>,
    future: F,
) -> AppResult<()>
where
    F: Future<Output = AppResult<()>>,
{
    run_with_termination_signal_waiter(
        cancellation_token,
        cleanup_context,
        wait_for_run_termination_signal(),
        future,
    )
    .await
}

async fn run_with_termination_signal_waiter<F, S>(
    cancellation_token: CancellationToken,
    cleanup_context: RunSignalCleanupContext<'_>,
    signal_waiter: S,
    future: F,
) -> AppResult<()>
where
    F: Future<Output = AppResult<()>>,
    S: Future<Output = AppResult<()>>,
{
    tokio::pin!(future);
    tokio::pin!(signal_waiter);

    tokio::select! {
        result = &mut future => result,
        signal = &mut signal_waiter => {
            signal?;
            eprintln!("termination signal received; stopping run gracefully...");
            cancellation_token.cancel();
            let interrupted_handoff = prepare_signal_interruption_handoff(&cleanup_context)
                .map_err(|error| cleanup_context.origin.error(format!(
                    "termination signal received; failed to persist signal-cleanup handoff before graceful shutdown: {error}"
                )))?;
            match tokio::time::timeout(RUN_STOP_GRACE_PERIOD, &mut future).await {
                Ok(result) => {
                    if interrupted_handoff.interrupted_marker_persisted {
                        match finalize_signal_interruption_marker(
                            &cleanup_context,
                            interrupted_handoff.expected_attempt.as_ref(),
                        ) {
                            Ok(true) => Err(cleanup_context.origin.error(
                                "run interrupted by termination signal",
                            )),
                            Ok(false) => result,
                            Err(error) => Err(cleanup_context.origin.error(format!(
                                "termination signal received; interrupted-state cleanup failed: {error}"
                            ))),
                        }
                    } else {
                        match reconcile_signal_interruption_for_attempt(
                            &cleanup_context,
                            interrupted_handoff.expected_attempt.as_ref(),
                            engine::InterruptedRunUpdate {
                                summary: "failed (interrupted by termination signal)",
                                log_message:
                                    "termination signal interrupted the orchestrator before graceful shutdown completed",
                                failure_class: Some("cancellation"),
                            },
                        ) {
                            Ok(true) => Err(cleanup_context.origin.error(
                                "run interrupted by termination signal",
                            )),
                            Ok(false) => result,
                            Err(error) => Err(cleanup_context.origin.error(format!(
                                "termination signal received; interrupted-state cleanup failed: {error}"
                            ))),
                        }
                    }
                }
                Err(_) => {
                    if interrupted_handoff.interrupted_marker_persisted {
                        match finalize_signal_interruption_marker(
                            &cleanup_context,
                            interrupted_handoff.expected_attempt.as_ref(),
                        ) {
                            Ok(true) => Err(cleanup_context.origin.error(
                                "run interrupted by termination signal after graceful shutdown timed out",
                            )),
                            Ok(false) => Err(cleanup_context.origin.error(
                                "termination signal received and graceful shutdown timed out before interrupted state could be confirmed",
                            )),
                            Err(error) => Err(cleanup_context.origin.error(format!(
                                "termination signal received and graceful shutdown timed out; interrupted-state cleanup also failed: {error}"
                            ))),
                        }
                    } else {
                        let cleanup_result = reconcile_signal_interruption_for_attempt(
                            &cleanup_context,
                            interrupted_handoff.expected_attempt.as_ref(),
                            engine::InterruptedRunUpdate {
                                summary: "failed (interrupted by termination signal)",
                                log_message:
                                    "termination signal interrupted the orchestrator before graceful shutdown completed",
                                failure_class: Some("cancellation"),
                            },
                        );
                        let reason = match cleanup_result {
                            Ok(true) => "run interrupted by termination signal after graceful shutdown timed out".to_owned(),
                            Ok(false) => "termination signal received and graceful shutdown timed out before interrupted state could be confirmed".to_owned(),
                            Err(error) => format!(
                                "termination signal received and graceful shutdown timed out; interrupted-state cleanup also failed: {error}"
                            ),
                        };
                        Err(cleanup_context.origin.error(reason))
                    }
                }
            }
        }
    }
}

#[cfg(unix)]
fn unix_process_group_members(pgid: u32) -> AppResult<Vec<u32>> {
    let output = Command::new("ps")
        .args(["-ax", "-o", "pid=", "-o", "pgid="])
        .output()
        .map_err(|error| {
            AppError::Io(std::io::Error::other(format!(
                "failed to enumerate process groups via ps: {error}"
            )))
        })?;
    if !output.status.success() {
        return Err(AppError::Io(std::io::Error::other(format!(
            "failed to enumerate process groups via ps: exit status {}",
            output.status
        ))));
    }

    let stdout = String::from_utf8(output.stdout).map_err(|error| {
        AppError::Io(std::io::Error::other(format!(
            "ps returned non-UTF-8 output while enumerating process groups: {error}"
        )))
    })?;
    Ok(stdout
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.parse::<u32>().ok()?;
            let member_pgid = fields.next()?.parse::<u32>().ok()?;
            (member_pgid == pgid).then_some(pid)
        })
        .collect())
}

async fn wait_for_run_termination_signal() -> AppResult<()> {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => Ok(()),
            _ = sigterm.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.map_err(AppError::from)
    }
}

#[cfg(unix)]
enum SignalSendOutcome {
    Delivered,
    AlreadyExited,
}

#[cfg(unix)]
enum StopProcessHandle {
    #[cfg(target_os = "linux")]
    Linux(OwnedFd),
    #[cfg(not(target_os = "linux"))]
    Legacy(crate::adapters::fs::RunPidRecord),
}

#[cfg(unix)]
fn open_stop_process_handle(
    record: &crate::adapters::fs::RunPidRecord,
) -> AppResult<Option<StopProcessHandle>> {
    #[cfg(target_os = "linux")]
    {
        let Some(_expected_ticks) = record.proc_start_ticks else {
            return Err(AppError::RunStopFailed {
                reason: format!(
                    "run.pid for pid {} is missing proc_start_ticks; refusing unsafe stop of a legacy pid record",
                    record.pid
                ),
            });
        };

        let pid = i32::try_from(record.pid).map_err(|_| AppError::RunStopFailed {
            reason: format!("process id {} exceeds libc::pid_t range", record.pid),
        })?;
        let pid = RustixPid::from_raw(pid).ok_or_else(|| AppError::RunStopFailed {
            reason: format!("process id {} is not a valid positive pid", record.pid),
        })?;
        let pidfd = match pidfd_open(pid, PidfdFlags::empty()) {
            Ok(pidfd) => pidfd,
            Err(rustix::io::Errno::SRCH) => return Ok(None),
            Err(error) => {
                return Err(AppError::Io(std::io::Error::other(format!(
                    "failed to open pidfd for pid {}: {error}",
                    record.pid
                ))))
            }
        };
        if !FileSystem::is_pid_alive(record) {
            return Ok(None);
        }
        Ok(Some(StopProcessHandle::Linux(pidfd)))
    }

    #[cfg(not(target_os = "linux"))]
    {
        if !FileSystem::is_pid_alive(record) {
            return Ok(None);
        }
        Ok(Some(StopProcessHandle::Legacy(record.clone())))
    }
}

#[cfg(unix)]
fn send_signal_to_handle(
    handle: &StopProcessHandle,
    signal: Signal,
) -> AppResult<SignalSendOutcome> {
    #[cfg(target_os = "linux")]
    {
        let StopProcessHandle::Linux(pidfd) = handle;
        let signal = match signal {
            Signal::SIGTERM => RustixSignal::Term,
            Signal::SIGKILL => RustixSignal::Kill,
            other => {
                return Err(AppError::Io(std::io::Error::other(format!(
                    "unsupported stop signal on Linux pidfd path: {other:?}"
                ))))
            }
        };
        match pidfd_send_signal(pidfd, signal) {
            Ok(()) => Ok(SignalSendOutcome::Delivered),
            Err(rustix::io::Errno::SRCH) => Ok(SignalSendOutcome::AlreadyExited),
            Err(error) => Err(AppError::Io(std::io::Error::other(format!(
                "failed to send {signal:?} via pidfd: {error}"
            )))),
        }
    }

    #[cfg(not(target_os = "linux"))]
    if let StopProcessHandle::Legacy(record) = handle {
        if !FileSystem::is_pid_alive(record) {
            return Ok(SignalSendOutcome::AlreadyExited);
        }

        match kill(Pid::from_raw(record.pid as i32), signal) {
            Ok(()) => Ok(SignalSendOutcome::Delivered),
            Err(nix::errno::Errno::ESRCH) => Ok(SignalSendOutcome::AlreadyExited),
            Err(error) => Err(AppError::Io(std::io::Error::other(format!(
                "failed to send {signal:?} to pid {}: {error}",
                record.pid
            )))),
        }
    }
}

#[cfg(unix)]
async fn wait_for_handle_exit(handle: &StopProcessHandle, timeout: Duration) -> AppResult<bool> {
    #[cfg(target_os = "linux")]
    {
        let StopProcessHandle::Linux(pidfd) = handle;
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let timeout_ms = remaining.as_millis().min(i32::MAX as u128) as i32;
            let mut poll_fd = [PollFd::new(pidfd, PollFlags::IN)];
            match poll(&mut poll_fd, timeout_ms) {
                Ok(0) => return Ok(false),
                Ok(_) => return Ok(poll_fd[0].revents().contains(PollFlags::IN)),
                Err(rustix::io::Errno::INTR) => {
                    if Instant::now() >= deadline {
                        return Ok(false);
                    }
                    continue;
                }
                Err(error) => {
                    return Err(AppError::Io(std::io::Error::other(format!(
                        "failed while waiting on pidfd: {error}"
                    ))))
                }
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    if let StopProcessHandle::Legacy(record) = handle {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if !FileSystem::is_pid_alive(record) {
                return Ok(true);
            }
            tokio::time::sleep(RUN_STOP_POLL_INTERVAL).await;
        }
        return Ok(!FileSystem::is_pid_alive(record));
    }

    #[cfg(not(target_os = "linux"))]
    {
        Ok(false)
    }
}

#[cfg(target_os = "linux")]
fn linux_child_pids(pid: u32) -> Vec<u32> {
    let children_path = format!("/proc/{pid}/task/{pid}/children");
    match std::fs::read_to_string(children_path) {
        Ok(children) => children
            .split_whitespace()
            .filter_map(|value| value.parse::<u32>().ok())
            .collect(),
        Err(_) => Vec::new(),
    }
}

#[cfg(target_os = "linux")]
fn linux_descendant_pids(root_pid: u32) -> Vec<u32> {
    let mut stack = vec![root_pid];
    let mut seen_pids = HashSet::from([root_pid]);

    while let Some(pid) = stack.pop() {
        for child_pid in linux_child_pids(pid) {
            if seen_pids.insert(child_pid) {
                stack.push(child_pid);
            }
        }
    }

    seen_pids.remove(&root_pid);
    seen_pids.into_iter().collect()
}

#[cfg(all(unix, not(target_os = "linux")))]
fn unix_process_snapshot() -> AppResult<Vec<(u32, u32, u32)>> {
    let output = Command::new("ps")
        .args(["-ax", "-o", "pid=", "-o", "ppid=", "-o", "pgid="])
        .output()
        .map_err(|error| {
            AppError::Io(std::io::Error::other(format!(
                "failed to enumerate processes via ps: {error}"
            )))
        })?;
    if !output.status.success() {
        return Err(AppError::Io(std::io::Error::other(format!(
            "failed to enumerate processes via ps: exit status {}",
            output.status
        ))));
    }

    let stdout = String::from_utf8(output.stdout).map_err(|error| {
        AppError::Io(std::io::Error::other(format!(
            "ps returned non-UTF-8 output while enumerating processes: {error}"
        )))
    })?;
    Ok(stdout
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let pid = fields.next()?.parse::<u32>().ok()?;
            let ppid = fields.next()?.parse::<u32>().ok()?;
            let pgid = fields.next()?.parse::<u32>().ok()?;
            Some((pid, ppid, pgid))
        })
        .collect())
}

#[cfg(all(unix, not(target_os = "linux")))]
fn unix_descendant_pids(root_pid: u32) -> AppResult<Vec<u32>> {
    let process_snapshot = unix_process_snapshot()?;
    let mut children_by_parent: HashMap<u32, Vec<u32>> = HashMap::new();

    for (pid, ppid, _pgid) in process_snapshot {
        children_by_parent.entry(ppid).or_default().push(pid);
    }

    let mut stack = vec![root_pid];
    let mut seen_pids = HashSet::from([root_pid]);

    while let Some(pid) = stack.pop() {
        if let Some(child_pids) = children_by_parent.get(&pid) {
            for child_pid in child_pids {
                if seen_pids.insert(*child_pid) {
                    stack.push(*child_pid);
                }
            }
        }
    }

    seen_pids.remove(&root_pid);
    Ok(seen_pids.into_iter().collect())
}

#[cfg(unix)]
#[derive(Clone)]
struct TrackedProcess {
    pid: u32,
    proc_start_ticks: Option<u64>,
    proc_start_marker: Option<String>,
}

#[cfg(unix)]
impl TrackedProcess {
    fn from_pid(pid: u32) -> Self {
        Self {
            pid,
            proc_start_ticks: FileSystem::proc_start_ticks_for_pid(pid),
            proc_start_marker: FileSystem::proc_start_marker_for_pid(pid),
        }
    }

    fn matches_live_process(&self) -> bool {
        FileSystem::is_pid_alive(&crate::adapters::fs::RunPidRecord {
            pid: self.pid,
            started_at: Utc::now(),
            owner: crate::adapters::fs::RunPidOwner::Cli,
            writer_owner: None,
            run_id: None,
            run_started_at: None,
            proc_start_ticks: self.proc_start_ticks,
            proc_start_marker: self.proc_start_marker.clone(),
        })
    }
}

#[cfg(unix)]
fn snapshot_descendant_processes(root_pid: u32) -> AppResult<Vec<TrackedProcess>> {
    #[cfg(target_os = "linux")]
    let descendant_pids = linux_descendant_pids(root_pid);
    #[cfg(all(unix, not(target_os = "linux")))]
    let descendant_pids = unix_descendant_pids(root_pid)?;

    Ok(descendant_pids
        .into_iter()
        .map(TrackedProcess::from_pid)
        .collect())
}

#[cfg(unix)]
fn kill_tracked_descendant_processes(processes: &[TrackedProcess]) -> AppResult<usize> {
    let mut signaled = 0usize;
    for process in processes {
        if !process.matches_live_process() {
            continue;
        }
        let pid = i32::try_from(process.pid).map_err(|_| AppError::RunStopFailed {
            reason: format!("descendant pid {} exceeds libc::pid_t range", process.pid),
        })?;
        match kill(Pid::from_raw(pid), Signal::SIGKILL) {
            Ok(()) => signaled += 1,
            Err(nix::errno::Errno::ESRCH) => {}
            Err(error) => {
                return Err(AppError::Io(std::io::Error::other(format!(
                    "failed to SIGKILL descendant pid {pid}: {error}"
                ))))
            }
        }
    }

    Ok(signaled)
}

#[cfg(unix)]
fn tracked_processes_still_alive(processes: &[TrackedProcess]) -> Vec<u32> {
    let mut live_pids = processes
        .iter()
        .filter(|process| process.matches_live_process())
        .map(|process| process.pid)
        .collect::<Vec<_>>();
    live_pids.sort_unstable();
    live_pids.dedup();
    live_pids
}

fn stale_backend_processes_for_attempt(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    expected_attempt: &engine::RunningAttemptIdentity,
) -> AppResult<(
    Vec<crate::adapters::fs::RunBackendProcessRecord>,
    Vec<crate::adapters::fs::RunBackendProcessRecord>,
)> {
    let processes = FileSystem::read_backend_processes(base_dir, project_id)?;
    let (matching, remaining): (Vec<_>, Vec<_>) = processes.into_iter().partition(|record| {
        FileSystem::backend_process_matches_attempt(
            record,
            &expected_attempt.run_id,
            expected_attempt.started_at,
        )
    });
    Ok((matching, remaining))
}

#[cfg(unix)]
fn kill_stale_backend_process_group(
    record: &crate::adapters::fs::RunBackendProcessRecord,
) -> AppResult<(bool, Vec<TrackedProcess>)> {
    let leader_is_authoritative = FileSystem::backend_process_is_authoritative(record);
    let leader_matches_record = FileSystem::is_backend_process_alive(record);
    let raw_pid_running = FileSystem::is_pid_running_unchecked(record.pid);

    if !leader_is_authoritative && raw_pid_running {
        return Err(AppError::Io(std::io::Error::other(format!(
            "refusing stale backend cleanup for pid {} because the recorded process identity is not authoritative and the pid is still live",
            record.pid
        ))));
    }
    if !leader_is_authoritative {
        return Ok((false, Vec::new()));
    }
    if !leader_matches_record && raw_pid_running {
        return Err(AppError::Io(std::io::Error::other(format!(
            "refusing stale backend cleanup for pid {} because the recorded backend pid now belongs to a different live process",
            record.pid
        ))));
    }

    let mut tracked_group_members = unix_process_group_members(record.pid)?
        .into_iter()
        .filter(|pid| FileSystem::is_pid_running_unchecked(*pid))
        .map(TrackedProcess::from_pid)
        .collect::<Vec<_>>();

    if leader_matches_record
        && tracked_group_members
            .iter()
            .all(|process| process.pid != record.pid)
    {
        tracked_group_members.push(TrackedProcess::from_pid(record.pid));
    }

    if !leader_matches_record && tracked_group_members.is_empty() {
        return Ok((false, tracked_group_members));
    }

    let pid = i32::try_from(record.pid).map_err(|_| {
        AppError::Io(std::io::Error::other(format!(
            "backend pid {} exceeds libc::pid_t range",
            record.pid
        )))
    })?;
    match kill(Pid::from_raw(-pid), Signal::SIGKILL) {
        Ok(()) => Ok((true, tracked_group_members)),
        Err(nix::errno::Errno::ESRCH) if leader_matches_record => {
            match kill(Pid::from_raw(pid), Signal::SIGKILL) {
                Ok(()) => Ok((true, tracked_group_members)),
                Err(nix::errno::Errno::ESRCH) => Ok((false, tracked_group_members)),
                Err(error) => Err(AppError::Io(std::io::Error::other(format!(
                    "failed to SIGKILL stale backend pid {}: {error}",
                    record.pid
                )))),
            }
        }
        Err(nix::errno::Errno::ESRCH) => Ok((false, tracked_group_members)),
        Err(error) => Err(AppError::Io(std::io::Error::other(format!(
            "failed to SIGKILL stale backend process group {}: {error}",
            record.pid
        )))),
    }
}

#[cfg(unix)]
fn stale_backend_processes_still_alive(processes: &[TrackedProcess]) -> Vec<u32> {
    tracked_processes_still_alive(processes)
}

fn cleanup_stale_backend_process_groups(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    expected_attempt: &engine::RunningAttemptIdentity,
) -> AppResult<usize> {
    let (matching, _remaining) =
        stale_backend_processes_for_attempt(base_dir, project_id, expected_attempt)?;
    if matching.is_empty() {
        return Ok(0);
    }

    #[cfg(unix)]
    {
        let mut signaled = 0usize;
        let mut tracked_group_members = Vec::new();
        for record in &matching {
            let (killed, tracked_members) = kill_stale_backend_process_group(record)?;
            tracked_group_members.extend(tracked_members);
            if killed {
                signaled += 1;
            }
        }

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let live_pids = stale_backend_processes_still_alive(&tracked_group_members);
            if live_pids.is_empty() {
                break;
            }
            if Instant::now() >= deadline {
                let live_pids = live_pids
                    .into_iter()
                    .map(|pid| pid.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(AppError::Io(std::io::Error::other(format!(
                    "stale backend subprocesses are still alive after SIGKILL: {live_pids}"
                ))));
            }
            std::thread::sleep(Duration::from_millis(25));
        }

        FileSystem::remove_backend_processes_for_attempt(
            base_dir,
            project_id,
            &expected_attempt.run_id,
            expected_attempt.started_at,
        )?;
        Ok(signaled)
    }

    #[cfg(not(unix))]
    {
        if matching.iter().any(FileSystem::is_backend_process_alive) {
            return Err(AppError::Io(std::io::Error::other(
                "stale backend subprocesses remain alive and cannot be cleaned automatically on this platform",
            )));
        }
        FileSystem::remove_backend_processes_for_attempt(
            base_dir,
            project_id,
            &expected_attempt.run_id,
            expected_attempt.started_at,
        )?;
        Ok(0)
    }
}

fn recover_stale_running_snapshot(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    expected_attempt: &engine::RunningAttemptIdentity,
    summary: impl Into<String>,
    log_message: impl Into<String>,
    failure_class: &str,
) -> AppResult<bool> {
    let summary = summary.into();
    let log_message = log_message.into();
    engine::mark_running_run_interrupted(
        engine::InterruptedRunContext {
            run_snapshot_read: &FsRunSnapshotStore,
            run_snapshot_write: &FsRunSnapshotWriteStore,
            journal_store: &FsJournalStore,
            log_write: &FsRuntimeLogWriteStore,
            base_dir,
            project_id,
        },
        expected_attempt,
        engine::InterruptedRunUpdate {
            summary: &summary,
            log_message: &log_message,
            failure_class: Some(failure_class),
        },
    )
}

enum ClaimedRunningSnapshotOutcome {
    NotRunning(RunSnapshot),
    AttemptChanged(RunSnapshot),
    JournalFailed,
    JournalCompleted(RunSnapshot),
    RecoveredAsInterrupted,
}

fn reconcile_or_recover_claimed_running_snapshot_with_cleanup_policy(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    expected_attempt: &engine::RunningAttemptIdentity,
    stale_summary: impl Into<String>,
    stale_log_message: impl Into<String>,
    stale_failure_class: &str,
    cleanup_stale_backends: bool,
) -> AppResult<ClaimedRunningSnapshotOutcome> {
    let snapshot = FsRunSnapshotStore.read_run_snapshot(base_dir, project_id)?;
    if snapshot.status != RunStatus::Running {
        return Ok(ClaimedRunningSnapshotOutcome::NotRunning(snapshot));
    }
    if !snapshot.active_run.as_ref().is_some_and(|active_run| {
        active_run.run_id == expected_attempt.run_id
            && active_run.started_at == expected_attempt.started_at
    }) {
        return Ok(ClaimedRunningSnapshotOutcome::AttemptChanged(snapshot));
    }

    let journal_events = FsJournalStore.read_journal(base_dir, project_id)?;
    let outcome =
        match crate::contexts::project_run_record::queries::terminal_status_for_running_attempt(
            &snapshot,
            &journal_events,
        ) {
            Some(RunStatus::Failed) => {
                eprintln!(
                    "run lifecycle: snapshot shows Running but journal has run_failed — \
                 reconciling snapshot to Failed"
                );
                engine::mark_running_run_interrupted(
                engine::InterruptedRunContext {
                    run_snapshot_read: &FsRunSnapshotStore,
                    run_snapshot_write: &FsRunSnapshotWriteStore,
                    journal_store: &FsJournalStore,
                    log_write: &FsRuntimeLogWriteStore,
                    base_dir,
                    project_id,
                },
                expected_attempt,
                engine::InterruptedRunUpdate {
                    summary: "failed (reconciled from journal)",
                    log_message:
                        "reconciled stale running snapshot from durable run_failed journal event",
                    failure_class: None,
                },
            )?;
                let _ = FsRunSnapshotStore.read_run_snapshot(base_dir, project_id)?;
                ClaimedRunningSnapshotOutcome::JournalFailed
            }
            Some(RunStatus::Completed) => {
                eprintln!(
                    "run lifecycle: snapshot shows Running but journal has run_completed — \
                 reconciling snapshot to Completed"
                );
                let mut reconciled = snapshot;
                if let Some(resolution) = reconciled
                    .active_run
                    .as_ref()
                    .and_then(|active_run| active_run.stage_resolution_snapshot.clone())
                {
                    reconciled.last_stage_resolution_snapshot = Some(resolution);
                }
                reconciled.status = RunStatus::Completed;
                reconciled.active_run = None;
                reconciled.status_summary = "completed (reconciled from journal)".to_owned();
                FsRunSnapshotWriteStore.write_run_snapshot(base_dir, project_id, &reconciled)?;
                let _ = FileSystem::remove_pid_file(base_dir, project_id);
                let _ = FsRuntimeLogWriteStore.append_runtime_log(
                base_dir,
                project_id,
                &crate::contexts::project_run_record::model::RuntimeLogEntry {
                    timestamp: Utc::now(),
                    level: crate::contexts::project_run_record::model::LogLevel::Warn,
                    source: "run-cli".to_owned(),
                    message:
                        "reconciled stale running snapshot from durable run_completed journal event"
                            .to_owned(),
                },
            );
                ClaimedRunningSnapshotOutcome::JournalCompleted(
                    FsRunSnapshotStore.read_run_snapshot(base_dir, project_id)?,
                )
            }
            _ => {
                recover_stale_running_snapshot(
                    base_dir,
                    project_id,
                    expected_attempt,
                    stale_summary,
                    stale_log_message,
                    stale_failure_class,
                )?;
                let _ = FsRunSnapshotStore.read_run_snapshot(base_dir, project_id)?;
                ClaimedRunningSnapshotOutcome::RecoveredAsInterrupted
            }
        };

    if cleanup_stale_backends {
        let _ = cleanup_stale_backend_process_groups(base_dir, project_id, expected_attempt)?;
    }

    Ok(outcome)
}

fn reconcile_or_recover_claimed_running_snapshot(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    expected_attempt: &engine::RunningAttemptIdentity,
    stale_summary: impl Into<String>,
    stale_log_message: impl Into<String>,
    stale_failure_class: &str,
) -> AppResult<ClaimedRunningSnapshotOutcome> {
    reconcile_or_recover_claimed_running_snapshot_with_cleanup_policy(
        base_dir,
        project_id,
        expected_attempt,
        stale_summary,
        stale_log_message,
        stale_failure_class,
        true,
    )
}

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
    let lock_guard_lease_id = lock_guard.lease_id().to_owned();

    let cli_overrides = parse_cli_backend_overrides(&overrides)?;
    let effective_config =
        EffectiveConfig::load_for_project(&current_dir, Some(&project_id), cli_overrides)?;
    prepare_milestone_controller_for_execution(
        &current_dir,
        &project_id,
        &project_record,
        MilestoneControllerExecutionOrigin::Start,
    )?;

    let agent_service =
        agent_execution_builder::build_agent_execution_service_for_config(&effective_config)?;
    let run_snapshot_write = FsRunSnapshotWriteStore;
    let journal_store = FsJournalStore;
    let artifact_write = FsPayloadArtifactWriteStore;
    let log_write = FsRuntimeLogWriteStore;
    let cancellation_token = CancellationToken::new();

    println!("Starting run for project '{}'...", project_id);

    let amendment_queue = FsAmendmentQueueStore;
    let retry_policy = RetryPolicy::default_policy()
        .with_max_remediation_cycles(effective_config.run_policy().max_review_iterations);
    let run_result = run_with_termination_signal(
        cancellation_token.clone(),
        RunSignalCleanupContext {
            origin: RunSignalCleanupOrigin::Start,
            run_snapshot_read: &run_snapshot_read,
            run_snapshot_write: &run_snapshot_write,
            journal_store: &journal_store,
            log_write: &log_write,
            base_dir: &current_dir,
            project_id: &project_id,
            writer_owner: Some(lock_guard_lease_id.as_str()),
        },
        engine::execute_run_with_retry(
            &agent_service,
            &run_snapshot_read,
            &run_snapshot_write,
            &journal_store,
            &artifact_write,
            &log_write,
            &amendment_queue,
            &current_dir,
            None,
            &project_id,
            Some(lock_guard_lease_id.as_str()),
            project_record.flow,
            &effective_config,
            &retry_policy,
            cancellation_token,
        ),
    )
    .await;

    let final_snapshot = run_snapshot_read.read_run_snapshot(&current_dir, &project_id)?;
    let br = BrAdapter::new().with_working_dir(current_dir.clone());
    let bv = BvAdapter::new().with_working_dir(current_dir.clone());
    let milestone_sync_result = sync_terminal_milestone_task_and_continue_selection(
        &current_dir,
        &project_id,
        &project_record,
        &final_snapshot,
        &br,
        &bv,
        chrono::Utc::now(),
    )
    .await
    .map_err(|error| decorate_sync_error(error, false));

    // Test-only injection seam: delete the writer lock file before close()
    // to exercise close-failure handling at the CLI level.
    if std::env::var("RALPH_BURNING_TEST_DELETE_LOCK_BEFORE_CLOSE").is_ok() {
        let lock_path = FileSystem::live_workspace_root_path(&current_dir)
            .join("daemon/leases")
            .join(format!("writer-{}.lock", project_id.as_str()));
        let _ = std::fs::remove_file(&lock_path);
    }

    // Explicit guard shutdown before printing success — surfaces cleanup
    // failures as non-zero exit instead of silently succeeding.
    let pid_record_before_close = FileSystem::read_pid_file(&current_dir, &project_id)?;
    let close_result = lock_guard.close();
    if close_result.is_ok() {
        if let Some(pid_record) = pid_record_before_close.as_ref() {
            let _ = FileSystem::remove_pid_file_if_matches(&current_dir, &project_id, pid_record);
        }
    }
    match (run_result, milestone_sync_result) {
        (Err(run_error), Err(sync_error)) => {
            return Err(combine_run_and_sync_error(run_error, sync_error, false));
        }
        (Err(run_error), Ok(_)) => return Err(run_error),
        (Ok(_), Err(sync_error)) => return Err(sync_error),
        (Ok(_), Ok(_)) => {}
    }
    close_result?;

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
        RunStatus::Failed | RunStatus::Paused | RunStatus::Running => {}
        RunStatus::NotStarted => {
            return Err(AppError::ResumeFailed {
                reason: "project has not started a run yet; use `ralph-burning run start`"
                    .to_owned(),
            });
        }
        RunStatus::Completed => {
            return Err(AppError::ResumeFailed {
                reason: "project is already completed; there is nothing to resume".to_owned(),
            });
        }
    }
    if matches!(run_snapshot.status, RunStatus::Failed | RunStatus::Paused)
        && run_snapshot.has_active_run()
    {
        return Err(AppError::ResumeFailed {
            reason: "failed or paused snapshots must not retain an active run".to_owned(),
        });
    }
    let observed_running_attempt = if run_snapshot.status == RunStatus::Running {
        Some(running_snapshot_attempt(&run_snapshot)?)
    } else {
        None
    };
    let recovery_observation =
        observed_resume_recovery_owner(&current_dir, &project_id, &run_snapshot)?;

    // Acquire the writer lock before any stale-run mutation so recovery never
    // tears down a fresh owner that wins the race after we detect staleness or
    // leaves behind a stranded post-run cleanup lease after a crash.
    let mut lock_guard = Some(
        acquire_recovery_writer_guard(
            &current_dir,
            &current_dir,
            &project_id,
            recovery_observation.observed_owner.as_deref(),
            observed_running_attempt.as_ref(),
            recovery_observation.should_reconcile_claimed_running_snapshot,
            recovery_observation.allow_dead_cli_cleanup_reclaim,
        )
        .map_err(|error| AppError::ResumeFailed {
            reason: format!("failed to acquire writer lease for resume: {error}"),
        })?,
    );
    let lock_guard_lease_id = lock_guard
        .as_ref()
        .expect("writer guard just created")
        .lease_id()
        .to_owned();
    if recovery_observation.should_reconcile_claimed_running_snapshot {
        match reconcile_or_recover_claimed_running_snapshot(
            &current_dir,
            &project_id,
            observed_running_attempt
                .as_ref()
                .expect("running recovery must capture observed attempt"),
            "failed (stale running snapshot recovered for resume)",
            "run resume reconciled a stale running snapshot after claiming the writer lease",
            "interruption",
        )
        .map_err(|error| AppError::ResumeFailed {
            reason: format!("stale run recovery failed before resume: {error}"),
        })? {
            ClaimedRunningSnapshotOutcome::JournalCompleted(snapshot)
                if snapshot.status == RunStatus::Completed =>
            {
                lock_guard
                    .take()
                    .expect("writer guard still held")
                    .close()
                    .map_err(|error| AppError::ResumeFailed {
                        reason: format!(
                        "failed to release writer lease after completed-run reconciliation: {error}"
                    ),
                    })?;
                return Err(AppError::ResumeFailed {
                    reason: "project is already completed per journal; there is nothing to resume"
                        .to_owned(),
                });
            }
            ClaimedRunningSnapshotOutcome::AttemptChanged(snapshot) => {
                lock_guard
                    .take()
                    .expect("writer guard still held")
                    .close()
                    .map_err(|error| AppError::ResumeFailed {
                        reason: format!(
                            "failed to release writer lease after run-attempt handoff: {error}"
                        ),
                    })?;
                match snapshot.status {
                    RunStatus::Completed => {
                        return Err(AppError::ResumeFailed {
                            reason:
                                "project is already completed per journal; there is nothing to resume"
                                    .to_owned(),
                        });
                    }
                    RunStatus::Running => {
                        return Err(AppError::ResumeFailed {
                            reason: "project run changed while recovering stale state; retry `ralph-burning run resume`".to_owned(),
                        });
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }

    let cli_overrides = parse_cli_backend_overrides(&overrides)?;
    let effective_config =
        EffectiveConfig::load_for_project(&current_dir, Some(&project_id), cli_overrides)?;
    prepare_milestone_controller_for_execution(
        &current_dir,
        &project_id,
        &project_record,
        MilestoneControllerExecutionOrigin::Resume,
    )?;
    let agent_service =
        agent_execution_builder::build_agent_execution_service_for_config(&effective_config)?;
    let run_snapshot_write = FsRunSnapshotWriteStore;
    let journal_store = FsJournalStore;
    let artifact_write = FsPayloadArtifactWriteStore;
    let log_write = FsRuntimeLogWriteStore;
    let cancellation_token = CancellationToken::new();

    println!("Resuming run for project '{}'...", project_id);

    let amendment_queue = FsAmendmentQueueStore;
    let retry_policy = RetryPolicy::default_policy()
        .with_max_remediation_cycles(effective_config.run_policy().max_review_iterations);
    let run_result = run_with_termination_signal(
        cancellation_token.clone(),
        RunSignalCleanupContext {
            origin: RunSignalCleanupOrigin::Resume,
            run_snapshot_read: &run_snapshot_read,
            run_snapshot_write: &run_snapshot_write,
            journal_store: &journal_store,
            log_write: &log_write,
            base_dir: &current_dir,
            project_id: &project_id,
            writer_owner: Some(lock_guard_lease_id.as_str()),
        },
        engine::resume_run_with_retry(
            &agent_service,
            &run_snapshot_read,
            &run_snapshot_write,
            &journal_store,
            &FsArtifactStore,
            &artifact_write,
            &log_write,
            &amendment_queue,
            &current_dir,
            None,
            &project_id,
            Some(lock_guard_lease_id.as_str()),
            project_record.flow,
            &effective_config,
            &retry_policy,
            cancellation_token,
        ),
    )
    .await;

    let final_snapshot = run_snapshot_read.read_run_snapshot(&current_dir, &project_id)?;
    let allow_missing_lineage_repair = run_result.is_ok()
        || resume_attempt_has_exact_lineage(
            &current_dir,
            &project_id,
            &project_record,
            &final_snapshot,
        )?;
    let br = BrAdapter::new().with_working_dir(current_dir.clone());
    let bv = BvAdapter::new().with_working_dir(current_dir.clone());
    let milestone_sync_result = sync_terminal_milestone_task_and_continue_selection_with_options(
        &current_dir,
        &project_id,
        &project_record,
        &final_snapshot,
        allow_missing_lineage_repair,
        &br,
        &bv,
        chrono::Utc::now(),
    )
    .await
    .map_err(|error| decorate_sync_error(error, true));

    // Explicit guard shutdown before printing success — surfaces cleanup
    // failures as non-zero exit instead of silently succeeding.
    let pid_record_before_close = FileSystem::read_pid_file(&current_dir, &project_id)?;
    let close_result = lock_guard
        .take()
        .expect("writer guard should still be held before final close")
        .close();
    if close_result.is_ok() {
        if let Some(pid_record) = pid_record_before_close.as_ref() {
            let _ = FileSystem::remove_pid_file_if_matches(&current_dir, &project_id, pid_record);
        }
    }
    match (run_result, milestone_sync_result) {
        (Err(run_error), Err(sync_error)) => {
            return Err(combine_run_and_sync_error(run_error, sync_error, true));
        }
        (Err(run_error), Ok(_)) => return Err(run_error),
        (Ok(_), Err(sync_error)) => return Err(sync_error),
        (Ok(_), Ok(_)) => {}
    }
    close_result?;

    match final_snapshot.status {
        RunStatus::Completed => println!("Run completed successfully."),
        RunStatus::Paused => println!("{}", final_snapshot.status_summary),
        status => println!("Run finished with status '{}'.", status),
    }

    Ok(())
}

async fn handle_stop() -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let project_id = workspace_governance::resolve_active_project(&current_dir)?;
    let run_snapshot_read = FsRunSnapshotStore;
    let run_snapshot = run_snapshot_read.read_run_snapshot(&current_dir, &project_id)?;

    if run_snapshot.status != RunStatus::Running {
        return Err(AppError::RunStopFailed {
            reason: format!(
                "project is not currently running; current status is '{}'",
                run_snapshot.status
            ),
        });
    }

    let observed_attempt = running_snapshot_attempt(&run_snapshot)?;
    let running_liveness = classify_running_snapshot_liveness(&current_dir, &project_id)?;
    if !liveness_matches_running_attempt(&running_liveness, &observed_attempt) {
        return Err(AppError::RunStopFailed {
            reason: "project run changed while preparing stop; retry `ralph-burning run stop`"
                .to_owned(),
        });
    }

    match running_liveness {
        RunningSnapshotLiveness::DaemonProcess(_)
        | RunningSnapshotLiveness::LegacyDaemonProcess
        | RunningSnapshotLiveness::LegacyDaemonLease => Err(AppError::RunStopFailed {
            reason:
                "run stop only supports CLI-owned runs; the active run is owned by a daemon task"
                    .to_owned(),
        }),
        RunningSnapshotLiveness::LegacyCliProcess
        | RunningSnapshotLiveness::LegacyCliLease => Err(AppError::RunStopFailed {
            reason: "run stop cannot safely target a legacy CLI-owned run without current-version attempt tracking; wait for the original process to exit or for stale recovery to become safe".to_owned(),
        }),
        RunningSnapshotLiveness::Stale => {
            let observed_owner =
                observed_running_attempt_owner(&current_dir, &project_id, &observed_attempt)?;
            let guard =
                acquire_recovery_writer_guard(
                    &current_dir,
                    &current_dir,
                    &project_id,
                    observed_owner.as_deref(),
                    Some(&observed_attempt),
                    true,
                    false,
                )
                    .map_err(|error| AppError::RunStopFailed {
                        reason: format!(
                            "failed to acquire writer lease for stale stop recovery: {error}"
                        ),
                    })?;
            let outcome = reconcile_or_recover_claimed_running_snapshot(
                &current_dir,
                &project_id,
                &observed_attempt,
                "failed (stale running snapshot recovered by stop)",
                "run stop reconciled a stale running snapshot after claiming the writer lease",
                "interruption",
            )
            .map_err(|error| AppError::RunStopFailed {
                reason: format!("failed to recover stale run before stop: {error}"),
            })?;
            guard.close().map_err(|error| AppError::RunStopFailed {
                reason: format!("failed to release recovery writer lease after stop: {error}"),
            })?;
            match outcome {
                ClaimedRunningSnapshotOutcome::RecoveredAsInterrupted => println!(
                    "Run was already stale for project '{}'; snapshot marked failed and ready for resume.",
                    project_id
                ),
                ClaimedRunningSnapshotOutcome::AttemptChanged(snapshot) => match snapshot.status {
                    RunStatus::Running => {
                        return Err(AppError::RunStopFailed {
                            reason:
                                "project run changed while recovering stale state; retry `ralph-burning run stop`"
                                    .to_owned(),
                        });
                    }
                    RunStatus::Completed => println!(
                        "Run for project '{}' was already completed; no stop was required.",
                        project_id
                    ),
                    RunStatus::Failed => println!(
                        "Run for project '{}' was already failed and remains ready for resume.",
                        project_id
                    ),
                    status => println!(
                        "Run for project '{}' exited during stop with status '{}'.",
                        project_id, status
                    ),
                },
                ClaimedRunningSnapshotOutcome::JournalCompleted(_)
                | ClaimedRunningSnapshotOutcome::NotRunning(RunSnapshot {
                    status: RunStatus::Completed,
                    ..
                }) => println!(
                    "Run for project '{}' was already completed; no stop was required.",
                    project_id
                ),
                ClaimedRunningSnapshotOutcome::JournalFailed
                | ClaimedRunningSnapshotOutcome::NotRunning(RunSnapshot {
                    status: RunStatus::Failed,
                    ..
                }) => println!(
                    "Run for project '{}' was already failed and remains ready for resume.",
                    project_id
                ),
                ClaimedRunningSnapshotOutcome::NotRunning(snapshot) => println!(
                    "Run for project '{}' exited during stop with status '{}'.",
                    project_id, snapshot.status
                ),
            }
            Ok(())
        }
        RunningSnapshotLiveness::CliProcess(pid_record) => {
            #[cfg(unix)]
            {
                if !stop_target_pid_record_is_unchanged(
                    &current_dir,
                    &project_id,
                    &observed_attempt,
                    &pid_record,
                )? {
                    return Err(AppError::RunStopFailed {
                        reason:
                            "project run changed while preparing stop; retry `ralph-burning run stop`"
                                .to_owned(),
                    });
                }
                let observed_owner = pid_record
                    .writer_owner
                    .clone()
                    .or(read_project_writer_lock_owner(&current_dir, &project_id)?);
                let tracked_descendants = snapshot_descendant_processes(pid_record.pid)?;
                let describe_stopped_process =
                    |base: &'static str,
                     killed_backend_groups: usize,
                     killed_descendants: usize|
                     -> &'static str {
                        if killed_backend_groups == 0 && killed_descendants == 0 {
                            base
                        } else {
                            match base {
                                "process already exited before SIGTERM" => {
                                    "process already exited before SIGTERM and killed backend subprocesses"
                                }
                                "terminated gracefully with SIGTERM" => {
                                    "terminated gracefully with SIGTERM and killed backend subprocesses"
                                }
                                "process exited before SIGKILL" => {
                                    "process exited before SIGKILL and killed backend subprocesses"
                                }
                                "required SIGKILL after timeout" => {
                                    "required SIGKILL after timeout and killed backend subprocesses"
                                }
                                _ => base,
                            }
                        }
                    };
                let finalize_cleanup = || {
                    let mut killed_backend_groups = 0usize;
                    let mut killed_descendants = 0usize;
                    let mut cleanup_errors = Vec::new();

                    match cleanup_stale_backend_process_groups(
                        &current_dir,
                        &project_id,
                        &observed_attempt,
                    ) {
                        Ok(count) => killed_backend_groups = count,
                        Err(error) => {
                            cleanup_errors.push(format!("backend cleanup failed: {error}"))
                        }
                    }

                    match kill_tracked_descendant_processes(&tracked_descendants) {
                        Ok(count) => killed_descendants = count,
                        Err(error) => {
                            cleanup_errors.push(format!("descendant cleanup failed: {error}"))
                        }
                    }

                    (killed_backend_groups, killed_descendants, cleanup_errors)
                };
                let cleanup_backend_processes = || {
                    let (killed_backend_groups, killed_descendants, cleanup_errors) =
                        finalize_cleanup();
                    (
                        describe_stopped_process(
                            "process already exited before SIGTERM",
                            killed_backend_groups,
                            killed_descendants,
                        ),
                        cleanup_errors,
                    )
                };
                let (stop_outcome, cleanup_errors) = match open_stop_process_handle(&pid_record)? {
                    None => cleanup_backend_processes(),
                    Some(handle) => match send_signal_to_handle(&handle, Signal::SIGTERM)? {
                        SignalSendOutcome::AlreadyExited => cleanup_backend_processes(),
                        SignalSendOutcome::Delivered => {
                            if wait_for_handle_exit(&handle, RUN_STOP_GRACE_PERIOD).await? {
                                let (
                                    killed_backend_groups,
                                    killed_descendants,
                                    cleanup_errors,
                                ) = finalize_cleanup();
                                (
                                    describe_stopped_process(
                                        "terminated gracefully with SIGTERM",
                                        killed_backend_groups,
                                        killed_descendants,
                                    ),
                                    cleanup_errors,
                                )
                            } else {
                                match send_signal_to_handle(&handle, Signal::SIGKILL)? {
                                    SignalSendOutcome::AlreadyExited => {
                                        let (
                                            killed_backend_groups,
                                            killed_descendants,
                                            cleanup_errors,
                                        ) = finalize_cleanup();
                                        (
                                            describe_stopped_process(
                                                "process exited before SIGKILL",
                                                killed_backend_groups,
                                                killed_descendants,
                                            ),
                                            cleanup_errors,
                                        )
                                    }
                                    SignalSendOutcome::Delivered => {
                                        if !wait_for_handle_exit(&handle, Duration::from_secs(1))
                                            .await?
                                        {
                                            return Err(AppError::RunStopFailed {
                                                reason: format!(
                                                    "sent SIGKILL to pid {} but the orchestrator is still alive; refusing to rewrite run state",
                                                    pid_record.pid
                                                ),
                                            });
                                        }
                                        let (
                                            killed_backend_groups,
                                            killed_descendants,
                                            cleanup_errors,
                                        ) = finalize_cleanup();
                                        (
                                            describe_stopped_process(
                                                "required SIGKILL after timeout",
                                                killed_backend_groups,
                                                killed_descendants,
                                            ),
                                            cleanup_errors,
                                        )
                                    }
                                }
                            }
                        }
                    },
                };
                let guard = acquire_recovery_writer_guard(
                    &current_dir,
                    &current_dir,
                    &project_id,
                    observed_owner.as_deref(),
                    None,
                    true,
                    false,
                )
                .map_err(|error| AppError::RunStopFailed {
                    reason: format!(
                        "failed to acquire writer lease after stopping the orchestrator: {error}"
                    ),
                })?;
                let final_snapshot =
                    run_snapshot_read.read_run_snapshot(&current_dir, &project_id)?;
                let should_remove_pid_after_close = final_snapshot.status != RunStatus::Running
                    && FileSystem::read_pid_file(&current_dir, &project_id)?
                        .as_ref()
                        .is_some_and(|current_record| current_record == &pid_record);
                let stopped_liveness = if final_snapshot.status == RunStatus::Running {
                    Some(classify_running_snapshot_liveness_with_ignored_cli_lease_owner(
                        &current_dir,
                        &project_id,
                        Some(guard.lease_id()),
                    )?)
                } else {
                    None
                };
                let status_message = if final_snapshot.status == RunStatus::Running
                    && matches!(stopped_liveness, Some(RunningSnapshotLiveness::Stale))
                {
                    match reconcile_or_recover_claimed_running_snapshot_with_cleanup_policy(
                        &current_dir,
                        &project_id,
                        &observed_attempt,
                        "failed (stopped by user; run `ralph-burning run resume` to continue)",
                        format!("run stop interrupted the orchestrator; outcome={stop_outcome}"),
                        "cancellation",
                        false,
                    )
                    .map_err(|error| AppError::RunStopFailed {
                        reason: format!("failed to finalize stopped run state: {error}"),
                    })? {
                        ClaimedRunningSnapshotOutcome::RecoveredAsInterrupted => format!(
                            "Stopped run for project '{}' ({stop_outcome}); run `ralph-burning run resume` to continue.",
                            project_id
                        ),
                        ClaimedRunningSnapshotOutcome::AttemptChanged(snapshot) => {
                            if snapshot.status == RunStatus::Running {
                                return Err(AppError::RunStopFailed {
                                    reason:
                                        "project run changed while finalizing stop; retry `ralph-burning run stop`"
                                            .to_owned(),
                                });
                            }
                            format!(
                                "Run for project '{}' exited during stop with status '{}'.",
                                project_id, snapshot.status
                            )
                        }
                        ClaimedRunningSnapshotOutcome::JournalCompleted(_)
                        | ClaimedRunningSnapshotOutcome::NotRunning(RunSnapshot {
                            status: RunStatus::Completed,
                            ..
                        }) => format!(
                            "Run for project '{}' exited during stop with status 'completed'.",
                            project_id
                        ),
                        ClaimedRunningSnapshotOutcome::JournalFailed
                        | ClaimedRunningSnapshotOutcome::NotRunning(RunSnapshot {
                            status: RunStatus::Failed,
                            ..
                        }) => format!(
                            "Run for project '{}' exited during stop with status 'failed'.",
                            project_id
                        ),
                        ClaimedRunningSnapshotOutcome::NotRunning(snapshot) => format!(
                            "Run for project '{}' exited during stop with status '{}'.",
                            project_id, snapshot.status
                        ),
                    }
                } else {
                    format!(
                        "Run for project '{}' exited during stop with status '{}'.",
                        project_id, final_snapshot.status
                    )
                };
                let close_result = guard.close();
                if close_result.is_ok() && should_remove_pid_after_close {
                    let _ = FileSystem::remove_pid_file_if_matches(
                        &current_dir,
                        &project_id,
                        &pid_record,
                    );
                }
                close_result.map_err(|error| AppError::RunStopFailed {
                    reason: format!("failed to release recovery writer lease after stop: {error}"),
                })?;
                if !cleanup_errors.is_empty() {
                    return Err(AppError::RunStopFailed {
                        reason: format!(
                            "{status_message} Cleanup after stop failed: {}",
                            cleanup_errors.join("; ")
                        ),
                    });
                }
                println!("{status_message}");
                Ok(())
            }

            #[cfg(not(unix))]
            {
                let _ = pid_record;
                Err(AppError::RunStopFailed {
                    reason: "run stop is only supported on unix-like platforms".to_owned(),
                })
            }
        }
    }
}

async fn handle_sync_milestone() -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let project_id = workspace_governance::resolve_active_project(&current_dir)?;
    let project_record = FsProjectStore.read_project_record(&current_dir, &project_id)?;
    let final_snapshot = FsRunSnapshotStore.read_run_snapshot(&current_dir, &project_id)?;

    let br = BrAdapter::new().with_working_dir(current_dir.clone());
    let bv = BvAdapter::new().with_working_dir(current_dir.clone());
    let synced = sync_terminal_milestone_task_and_continue_selection(
        &current_dir,
        &project_id,
        &project_record,
        &final_snapshot,
        &br,
        &bv,
        chrono::Utc::now(),
    )
    .await?;
    if synced {
        println!("Milestone task state synced for project '{}'.", project_id);
    } else {
        println!(
            "No terminal milestone sync needed for project '{}'.",
            project_id
        );
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::{
        persist_next_step_recommendation, prepare_milestone_controller_for_execution,
        resume_attempt_has_exact_lineage, run_with_termination_signal_waiter,
        select_next_milestone_bead, select_next_milestone_bead_from_recommendation,
        sync_terminal_milestone_task, sync_terminal_milestone_task_and_continue_selection,
        sync_terminal_milestone_task_with_options, MilestoneControllerExecutionOrigin,
        ProjectMilestoneControllerRuntime, RunSignalCleanupContext, RunSignalCleanupOrigin,
    };
    use chrono::Utc;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use crate::adapters::bv_process::NextBeadResponse;
    use crate::adapters::fs::{
        FileSystem, FsDaemonStore, FsJournalStore, FsMilestoneControllerStore,
        FsMilestoneJournalStore, FsMilestonePlanStore, FsMilestoneSnapshotStore, FsMilestoneStore,
        FsRunSnapshotStore, FsRunSnapshotWriteStore, FsRuntimeLogWriteStore, FsTaskRunLineageStore,
        RunPidOwner,
    };
    use crate::contexts::agent_execution::model::CancellationToken;
    use crate::contexts::automation_runtime::model::{
        CliWriterCleanupHandoff, CliWriterLease, DaemonTask, DispatchMode, RoutingSource,
        TaskStatus, WorktreeLease,
    };
    use crate::contexts::automation_runtime::{DaemonStorePort, LeaseRecord};
    use crate::contexts::milestone_record::bundle::{
        AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
    };
    use crate::contexts::milestone_record::controller::{
        self as milestone_controller, MilestoneControllerResumePort,
    };
    use crate::contexts::milestone_record::model::{
        MilestoneEventType, MilestoneStatus, TaskRunOutcome,
    };
    use crate::contexts::milestone_record::service::{
        create_milestone, load_snapshot, persist_plan, read_journal, read_task_runs,
        record_bead_completion, record_bead_start, CreateMilestoneInput,
    };
    use crate::contexts::project_run_record::model::{
        ActiveRun, JournalEventType, ProjectRecord, ProjectStatusSummary, RunSnapshot, RunStatus,
        TaskOrigin, TaskSource,
    };
    use crate::contexts::project_run_record::service::{
        JournalStorePort, RunSnapshotPort, RunSnapshotWritePort,
    };
    use crate::shared::domain::{FlowPreset, ProjectId, StageCursor, StageId};
    use crate::shared::error::AppError;
    use crate::test_support::br::{MockBrAdapter, MockBrResponse};
    use crate::test_support::bv::{MockBvAdapter, MockBvResponse};
    use crate::test_support::env::{lock_path_mutex, PathGuard};
    use tokio::sync::oneshot;

    fn sample_bundle(id: &str, name: &str) -> MilestoneBundle {
        MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: id.to_owned(),
                name: name.to_owned(),
            },
            executive_summary: "CLI sync test plan.".to_owned(),
            goals: vec!["Keep milestone sync fixtures planned.".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Bead is executable".to_owned(),
                covered_by: vec!["ms-alpha.bead-2".to_owned(), "ms-alpha.bead-3".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: None,
                beads: vec![
                    BeadProposal {
                        bead_id: Some("ms-alpha.bead-2".to_owned()),
                        explicit_id: Some(true),
                        title: "Primary bead".to_owned(),
                        description: None,
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec![],
                        depends_on: vec![],
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                    BeadProposal {
                        bead_id: Some("ms-alpha.bead-3".to_owned()),
                        explicit_id: Some(true),
                        title: "Follow-up bead".to_owned(),
                        description: None,
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec![],
                        depends_on: vec![],
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                ],
            }],
            default_flow: FlowPreset::DocsChange,
            agents_guidance: None,
        }
    }

    fn single_bead_bundle(id: &str, name: &str) -> MilestoneBundle {
        MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: id.to_owned(),
                name: name.to_owned(),
            },
            executive_summary: "CLI sync test plan.".to_owned(),
            goals: vec!["Keep milestone sync fixtures planned.".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Bead is executable".to_owned(),
                covered_by: vec!["ms-alpha.bead-2".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some("ms-alpha.bead-2".to_owned()),
                    explicit_id: Some(true),
                    title: "Primary bead".to_owned(),
                    description: None,
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec![],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: FlowPreset::DocsChange,
            agents_guidance: None,
        }
    }

    fn explicit_bead_bundle(id: &str, name: &str, bead_id: &str) -> MilestoneBundle {
        MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: id.to_owned(),
                name: name.to_owned(),
            },
            executive_summary: "CLI sync test plan.".to_owned(),
            goals: vec!["Keep milestone sync fixtures planned.".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Bead is executable".to_owned(),
                covered_by: vec![bead_id.to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some(bead_id.to_owned()),
                    explicit_id: Some(true),
                    title: "Explicit bead".to_owned(),
                    description: None,
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec![],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: FlowPreset::DocsChange,
            agents_guidance: None,
        }
    }

    fn create_milestone_with_plan(
        base_dir: &std::path::Path,
        now: chrono::DateTime<Utc>,
    ) -> crate::contexts::milestone_record::model::MilestoneRecord {
        let milestone = create_milestone(
            &FsMilestoneStore,
            base_dir,
            CreateMilestoneInput {
                id: "ms-alpha".to_owned(),
                name: "Alpha".to_owned(),
                description: "Test milestone".to_owned(),
            },
            now,
        )
        .expect("create milestone");
        persist_plan(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base_dir,
            &milestone.id,
            &sample_bundle("ms-alpha", "Alpha"),
            now,
        )
        .expect("persist milestone plan");
        milestone
    }

    fn create_single_bead_milestone_with_plan(
        base_dir: &std::path::Path,
        now: chrono::DateTime<Utc>,
    ) -> crate::contexts::milestone_record::model::MilestoneRecord {
        let milestone = create_milestone(
            &FsMilestoneStore,
            base_dir,
            CreateMilestoneInput {
                id: "ms-alpha".to_owned(),
                name: "Alpha".to_owned(),
                description: "Test milestone".to_owned(),
            },
            now,
        )
        .expect("create milestone");
        persist_plan(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base_dir,
            &milestone.id,
            &single_bead_bundle("ms-alpha", "Alpha"),
            now,
        )
        .expect("persist milestone plan");
        milestone
    }

    fn create_explicit_bead_milestone_with_plan(
        base_dir: &std::path::Path,
        now: chrono::DateTime<Utc>,
        milestone_id: &str,
        bead_id: &str,
    ) -> crate::contexts::milestone_record::model::MilestoneRecord {
        let milestone = create_milestone(
            &FsMilestoneStore,
            base_dir,
            CreateMilestoneInput {
                id: milestone_id.to_owned(),
                name: "Explicit".to_owned(),
                description: "Explicit bead test milestone".to_owned(),
            },
            now,
        )
        .expect("create milestone");
        persist_plan(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base_dir,
            &milestone.id,
            &explicit_bead_bundle(milestone_id, "Explicit", bead_id),
            now,
        )
        .expect("persist milestone plan");
        milestone
    }

    fn bv_next_json(bead_id: &str, title: &str) -> String {
        serde_json::json!({
            "id": bead_id,
            "title": title,
            "score": 9.2,
            "reasons": ["high impact", "ready"],
            "action": "implement"
        })
        .to_string()
    }

    fn bv_no_recommendation_json() -> String {
        serde_json::json!({
            "message": "No actionable items available"
        })
        .to_string()
    }

    fn ready_beads_json(ids: &[&str]) -> String {
        serde_json::Value::Array(
            ids.iter()
                .map(|id| {
                    serde_json::json!({
                        "id": id,
                        "title": format!("Ready {id}"),
                        "priority": 1,
                        "bead_type": "task",
                        "labels": []
                    })
                })
                .collect(),
        )
        .to_string()
    }

    fn bead_detail_json(id: &str, status: &str, blockers: &[&str]) -> String {
        serde_json::json!({
            "id": id,
            "title": format!("Detail {id}"),
            "status": status,
            "priority": 1,
            "bead_type": "task",
            "labels": [],
            "dependencies": blockers
                .iter()
                .map(|blocker| {
                    serde_json::json!({
                        "id": blocker,
                        "dependency_type": "blocks",
                        "status": "open"
                    })
                })
                .collect::<Vec<_>>(),
            "dependents": [],
            "acceptance_criteria": []
        })
        .to_string()
    }

    #[cfg(unix)]
    fn install_fake_br_ready_script(base_dir: &std::path::Path, ready_json: &str) {
        let fake_bin = base_dir.join("fake-bin");
        std::fs::create_dir_all(&fake_bin).expect("create fake bin");

        let escaped_ready_json = ready_json.replace('\'', "'\"'\"'");
        let script = format!(
            "#!/bin/sh\nif [ \"$1\" = \"ready\" ] && [ \"$2\" = \"--json\" ]; then\n  printf '%s\\n' '{escaped_ready_json}'\n  exit 0\nfi\nprintf 'unexpected br invocation: %s\\n' \"$*\" >&2\nexit 1\n"
        );
        let br_path = fake_bin.join("br");
        std::fs::write(&br_path, script).expect("write fake br");
        std::fs::set_permissions(&br_path, std::fs::Permissions::from_mode(0o755))
            .expect("chmod fake br");
    }

    #[test]
    fn prepare_milestone_controller_for_execution_initializes_claimed_state() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_milestone_with_plan(base_dir, now);
        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: now,
            status_summary: ProjectStatusSummary::Created,
            task_source: Some(TaskSource {
                milestone_id: milestone.id.to_string(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };

        prepare_milestone_controller_for_execution(
            base_dir,
            &project_id,
            &project_record,
            MilestoneControllerExecutionOrigin::Start,
        )
        .expect("prepare milestone controller");

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
        )
        .expect("load controller")
        .expect("controller exists");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Claimed
        );
        assert_eq!(
            controller.active_bead_id.as_deref(),
            Some("ms-alpha.bead-2")
        );
        assert_eq!(
            controller.active_task_id.as_deref(),
            Some(project_id.as_str())
        );
    }

    #[cfg(unix)]
    #[test]
    fn milestone_controller_ready_check_matches_explicit_planned_bead_ids() {
        let _path_lock = lock_path_mutex();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone =
            create_explicit_bead_milestone_with_plan(base_dir, now, "ms-explicit", "bead-e2e");
        install_fake_br_ready_script(
            base_dir,
            r#"[{"id":"bead-e2e","title":"Explicit bead","priority":2,"bead_type":"task","labels":[]}]"#,
        );
        let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

        let runtime = ProjectMilestoneControllerRuntime {
            base_dir,
            milestone_id: &milestone.id,
        };

        assert!(runtime.has_ready_beads().expect("query ready beads"));
    }

    #[cfg(unix)]
    #[test]
    fn resume_blocked_controller_matches_mixed_form_ready_bead_ids() {
        let _path_lock = lock_path_mutex();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone =
            create_explicit_bead_milestone_with_plan(base_dir, now, "ms-explicit", "bead-e2e");
        install_fake_br_ready_script(
            base_dir,
            r#"[{"id":"ms-explicit.bead-e2e","title":"Explicit bead","priority":2,"bead_type":"task","labels":[]}]"#,
        );
        let _path_guard = PathGuard::prepend(&base_dir.join("fake-bin"));

        milestone_controller::initialize_controller_with_state(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
            milestone_controller::MilestoneControllerState::Blocked,
            now,
        )
        .expect("initialize blocked controller");

        let runtime = ProjectMilestoneControllerRuntime {
            base_dir,
            milestone_id: &milestone.id,
        };
        let resumed = milestone_controller::resume_controller(
            &FsMilestoneControllerStore,
            &runtime,
            base_dir,
            &milestone.id,
            now + chrono::Duration::seconds(1),
        )
        .expect("resume controller");

        assert_eq!(
            resumed.state,
            milestone_controller::MilestoneControllerState::Selecting
        );
        assert!(resumed
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("ready beads")));
    }

    #[tokio::test]
    async fn select_next_milestone_bead_claims_a_single_ready_recommendation() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_milestone_with_plan(base_dir, now);
        let br = MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&[
            "ms-alpha.bead-2",
        ]))]);
        let bv = MockBvAdapter::from_responses([MockBvResponse::success(bv_next_json(
            "ms-alpha.bead-2",
            "Primary bead",
        ))]);

        let controller = select_next_milestone_bead(
            base_dir,
            &milestone.id,
            &br.as_br_adapter(),
            &bv.as_bv_adapter(),
            now,
        )
        .await
        .expect("selection should succeed");

        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Claimed
        );
        assert_eq!(
            controller.active_bead_id.as_deref(),
            Some("ms-alpha.bead-2")
        );
        assert_eq!(controller.active_task_id, None);
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("bv recommended bead 'ms-alpha.bead-2'")));
        assert_eq!(bv.calls()[0].args, vec!["--robot-next"]);
        assert_eq!(br.calls()[0].args, vec!["ready", "--json"]);
        assert_eq!(br.calls().len(), 1);
    }

    #[tokio::test]
    async fn select_next_milestone_bead_preserves_raw_bead_id_from_br_ready() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_milestone_with_plan(base_dir, now);
        // br ready reports the unqualified form "bead-2" instead of "ms-alpha.bead-2".
        let br =
            MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&["bead-2"]))]);
        let bv = MockBvAdapter::from_responses([MockBvResponse::success(bv_next_json(
            "bead-2",
            "Primary bead",
        ))]);

        let controller = select_next_milestone_bead(
            base_dir,
            &milestone.id,
            &br.as_br_adapter(),
            &bv.as_bv_adapter(),
            now,
        )
        .await
        .expect("selection should succeed with unqualified bead ID");

        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Claimed
        );
        // The stored bead ID must be the raw form from br ready so downstream
        // comparisons in sync_controller_task_claimed and
        // validate_active_context_alignment match the form in task_source.bead_id.
        assert_eq!(
            controller.active_bead_id.as_deref(),
            Some("bead-2"),
            "bead ID from br ready should be stored as-is without canonicalization"
        );
    }

    #[tokio::test]
    async fn select_next_milestone_bead_from_recommendation_records_prefetched_validation_reason() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_milestone_with_plan(base_dir, now);
        let br = MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&[
            "ms-alpha.bead-2",
        ]))]);

        let controller = select_next_milestone_bead_from_recommendation(
            base_dir,
            &milestone.id,
            &br.as_br_adapter(),
            Some(NextBeadResponse {
                id: "ms-alpha.bead-2".to_owned(),
                title: "Primary bead".to_owned(),
                score: 9.2,
                reasons: vec!["ready".to_owned()],
                action: "implement".to_owned(),
            }),
            now,
        )
        .await
        .expect("selection should succeed");

        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Claimed
        );
        let journal = crate::contexts::milestone_record::controller::MilestoneControllerPort::read_transition_journal(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
        )
        .expect("read controller journal");
        let selecting_event = journal
            .iter()
            .find(|event| {
                event.to_state == milestone_controller::MilestoneControllerState::Selecting
            })
            .expect("selecting transition");
        assert_eq!(
            selecting_event.reason,
            "validating a pre-fetched bv recommendation against br readiness before any claim"
        );
        assert_eq!(br.calls().len(), 1);
        assert_eq!(br.calls()[0].args, vec!["ready", "--json"]);
    }

    #[tokio::test]
    async fn select_next_milestone_bead_records_request_reason_when_already_selecting() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_milestone_with_plan(base_dir, now);
        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
            milestone_controller::ControllerTransitionRequest::new(
                milestone_controller::MilestoneControllerState::Selecting,
                "reconciliation recorded the bead outcome and returned the controller to bead selection",
            ),
            now,
        )
        .expect("seed selecting controller");

        let br = MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&[
            "ms-alpha.bead-2",
        ]))]);
        let bv = MockBvAdapter::from_responses([MockBvResponse::success(bv_next_json(
            "ms-alpha.bead-2",
            "Primary bead",
        ))]);

        select_next_milestone_bead(
            base_dir,
            &milestone.id,
            &br.as_br_adapter(),
            &bv.as_bv_adapter(),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("selection should succeed");

        let journal = crate::contexts::milestone_record::controller::MilestoneControllerPort::read_transition_journal(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
        )
        .expect("read controller journal");
        let selecting_reasons = journal
            .iter()
            .filter(|event| {
                event.to_state == milestone_controller::MilestoneControllerState::Selecting
            })
            .map(|event| event.reason.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            selecting_reasons,
            vec![
                "reconciliation recorded the bead outcome and returned the controller to bead selection",
                "requesting the next bead recommendation from bv before any claim",
            ]
        );
    }

    #[tokio::test]
    async fn select_next_milestone_bead_from_recommendation_records_prefetched_reason_when_already_selecting(
    ) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_milestone_with_plan(base_dir, now);
        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
            milestone_controller::ControllerTransitionRequest::new(
                milestone_controller::MilestoneControllerState::Selecting,
                "reconciliation recorded the bead outcome and returned the controller to bead selection",
            ),
            now,
        )
        .expect("seed selecting controller");

        let br = MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&[
            "ms-alpha.bead-2",
        ]))]);

        select_next_milestone_bead_from_recommendation(
            base_dir,
            &milestone.id,
            &br.as_br_adapter(),
            Some(NextBeadResponse {
                id: "ms-alpha.bead-2".to_owned(),
                title: "Primary bead".to_owned(),
                score: 9.2,
                reasons: vec!["ready".to_owned()],
                action: "implement".to_owned(),
            }),
            now + chrono::Duration::seconds(1),
        )
        .await
        .expect("selection should succeed");

        let journal = crate::contexts::milestone_record::controller::MilestoneControllerPort::read_transition_journal(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
        )
        .expect("read controller journal");
        let selecting_reasons = journal
            .iter()
            .filter(|event| {
                event.to_state == milestone_controller::MilestoneControllerState::Selecting
            })
            .map(|event| event.reason.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            selecting_reasons,
            vec![
                "reconciliation recorded the bead outcome and returned the controller to bead selection",
                "validating a pre-fetched bv recommendation against br readiness before any claim",
            ]
        );
    }

    #[tokio::test]
    async fn select_next_milestone_bead_blocks_when_bv_recommendation_is_not_ready() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_milestone_with_plan(base_dir, now);
        let br = MockBrAdapter::from_responses([
            MockBrResponse::success(ready_beads_json(&["ms-alpha.bead-2"])),
            MockBrResponse::success(bead_detail_json(
                "ms-alpha.bead-3",
                "open",
                &["ms-alpha.bead-2"],
            )),
        ]);
        let bv = MockBvAdapter::from_responses([MockBvResponse::success(bv_next_json(
            "ms-alpha.bead-3",
            "Follow-up bead",
        ))]);

        let controller = select_next_milestone_bead(
            base_dir,
            &milestone.id,
            &br.as_br_adapter(),
            &bv.as_bv_adapter(),
            now,
        )
        .await
        .expect("selection should succeed");

        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Blocked
        );
        assert_eq!(controller.active_bead_id, None);
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| {
                reason.contains("ms-alpha.bead-3") && reason.contains("blocked by ms-alpha.bead-2")
            }));
        assert_eq!(br.calls().len(), 2);
        assert_eq!(br.calls()[0].args, vec!["ready", "--json"]);
        assert_eq!(
            br.calls()[1].args,
            vec!["show", "ms-alpha.bead-3", "--json"]
        );
    }

    #[tokio::test]
    async fn select_next_milestone_bead_blocks_when_bv_recommendation_is_outside_the_milestone_plan(
    ) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_milestone_with_plan(base_dir, now);
        let br = MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&[
            "ms-alpha.bead-2",
        ]))]);
        let bv = MockBvAdapter::from_responses([MockBvResponse::success(bv_next_json(
            "ms-foreign.bead-9",
            "Foreign bead",
        ))]);

        let controller = select_next_milestone_bead(
            base_dir,
            &milestone.id,
            &br.as_br_adapter(),
            &bv.as_bv_adapter(),
            now,
        )
        .await
        .expect("selection should succeed");

        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Blocked
        );
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| {
                reason.contains("ms-foreign.bead-9")
                    && reason.contains("not part of milestone 'ms-alpha'")
                    && reason.contains("ready milestone beads: ms-alpha.bead-2")
            }));
        assert_eq!(br.calls().len(), 1);
        assert_eq!(br.calls()[0].args, vec!["ready", "--json"]);
    }

    #[tokio::test]
    async fn select_next_milestone_bead_blocks_when_no_ready_beads_exist() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_single_bead_milestone_with_plan(base_dir, now);
        let br = MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&[]))]);
        let bv =
            MockBvAdapter::from_responses([MockBvResponse::success(bv_no_recommendation_json())]);

        let controller = select_next_milestone_bead(
            base_dir,
            &milestone.id,
            &br.as_br_adapter(),
            &bv.as_bv_adapter(),
            now,
        )
        .await
        .expect("selection should succeed");

        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Blocked
        );
        assert_eq!(controller.active_bead_id, None);
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("no ready beads")));
        assert_eq!(br.calls()[0].args, vec!["ready", "--json"]);
    }

    #[tokio::test]
    async fn select_next_milestone_bead_blocks_when_bv_has_no_recommendation_but_br_ready_is_nonempty(
    ) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_milestone_with_plan(base_dir, now);
        let br = MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&[
            "ms-alpha.bead-2",
        ]))]);
        let bv =
            MockBvAdapter::from_responses([MockBvResponse::success(bv_no_recommendation_json())]);

        let controller = select_next_milestone_bead(
            base_dir,
            &milestone.id,
            &br.as_br_adapter(),
            &bv.as_bv_adapter(),
            now,
        )
        .await
        .expect("selection should succeed");

        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Blocked
        );
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| {
                reason.contains("bv reported no actionable bead")
                    && reason.contains("ms-alpha.bead-2")
            }));
        assert_eq!(br.calls().len(), 1);
        assert_eq!(br.calls()[0].args, vec!["ready", "--json"]);
    }

    #[tokio::test]
    async fn select_next_milestone_bead_moves_to_needs_operator_on_bv_failure() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_single_bead_milestone_with_plan(base_dir, now);
        let br = MockBrAdapter::new();
        let bv =
            MockBvAdapter::from_responses([MockBvResponse::timeout("bv --robot-next", 30_000)]);

        let controller = select_next_milestone_bead(
            base_dir,
            &milestone.id,
            &br.as_br_adapter(),
            &bv.as_bv_adapter(),
            now,
        )
        .await
        .expect("selection should succeed");

        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::NeedsOperator
        );
        assert_eq!(controller.active_bead_id, None);
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("could not query bv --robot-next")));
        assert!(
            br.calls().is_empty(),
            "br should not be queried after bv failure"
        );
    }

    #[tokio::test]
    async fn select_next_milestone_bead_moves_to_needs_operator_on_br_ready_failure() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_single_bead_milestone_with_plan(base_dir, now);
        let br =
            MockBrAdapter::from_responses([MockBrResponse::timeout("br ready --json", 30_000)]);
        let bv = MockBvAdapter::from_responses([MockBvResponse::success(bv_next_json(
            "ms-alpha.bead-2",
            "Primary bead",
        ))]);

        let controller = select_next_milestone_bead(
            base_dir,
            &milestone.id,
            &br.as_br_adapter(),
            &bv.as_bv_adapter(),
            now,
        )
        .await
        .expect("selection should succeed");

        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::NeedsOperator
        );
        assert_eq!(controller.active_bead_id, None);
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| {
                reason.contains("could not query br ready") && reason.contains("ms-alpha.bead-2")
            }));
        assert_eq!(bv.calls()[0].args, vec!["--robot-next"]);
        assert_eq!(br.calls()[0].args, vec!["ready", "--json"]);
        assert_eq!(br.calls().len(), 1);

        let hint_path = crate::adapters::fs::FileSystem::milestone_root(base_dir, &milestone.id)
            .join("next_step_hint.json");
        assert!(
            hint_path.exists(),
            "recommendation should be persisted before br ready"
        );
        let hint_json = std::fs::read_to_string(&hint_path).expect("read persisted hint");
        let persisted_hint: NextBeadResponse =
            serde_json::from_str(&hint_json).expect("parse persisted hint");
        assert_eq!(persisted_hint.id, "ms-alpha.bead-2");
    }

    #[tokio::test]
    async fn select_next_milestone_bead_moves_to_needs_operator_when_br_show_is_not_missing() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_milestone_with_plan(base_dir, now);
        let br = MockBrAdapter::from_responses([
            MockBrResponse::success(ready_beads_json(&["ms-alpha.bead-2"])),
            MockBrResponse::exit_failure(2, "unknown flag: --json"),
        ]);
        let bv = MockBvAdapter::from_responses([MockBvResponse::success(bv_next_json(
            "ms-alpha.bead-3",
            "Follow-up bead",
        ))]);

        let controller = select_next_milestone_bead(
            base_dir,
            &milestone.id,
            &br.as_br_adapter(),
            &bv.as_bv_adapter(),
            now,
        )
        .await
        .expect("selection should succeed");

        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::NeedsOperator
        );
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| {
                reason.contains("could not inspect bead 'ms-alpha.bead-3'")
                    && reason.contains("unknown flag")
            }));
        assert_eq!(br.calls().len(), 2);
        assert_eq!(
            br.calls()[1].args,
            vec!["show", "ms-alpha.bead-3", "--json"]
        );
    }

    #[test]
    fn persist_next_step_recommendation_cleans_up_tmp_file_on_rename_failure() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        let milestone = create_single_bead_milestone_with_plan(base_dir, now);
        let milestone_dir =
            crate::adapters::fs::FileSystem::milestone_root(base_dir, &milestone.id);
        let hint_path = milestone_dir.join("next_step_hint.json");
        std::fs::create_dir_all(&hint_path).expect("create conflicting hint directory");

        persist_next_step_recommendation(
            base_dir,
            &milestone.id,
            &NextBeadResponse {
                id: "ms-alpha.bead-2".to_owned(),
                title: "Primary bead".to_owned(),
                score: 9.2,
                reasons: vec!["ready".to_owned()],
                action: "implement".to_owned(),
            },
        );

        assert!(
            !milestone_dir.join("next_step_hint.json.tmp").exists(),
            "temp recommendation file should be removed after rename failure"
        );
        assert!(
            hint_path.is_dir(),
            "the conflicting directory should remain"
        );
    }

    #[tokio::test]
    async fn sync_terminal_milestone_task_and_continue_selection_claims_next_bead_once() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse started_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:30:00Z","event_type":"run_completed","details":{"run_id":"run-1","completion_rounds":0,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            started_at,
        )
        .expect("record bead start");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Completed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "completed".to_owned(),
            last_stage_resolution_snapshot: None,
        };
        let br = MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&[
            "ms-alpha.bead-3",
        ]))]);
        let bv = MockBvAdapter::from_responses([MockBvResponse::success(bv_next_json(
            "ms-alpha.bead-3",
            "Follow-up bead",
        ))]);

        let first = sync_terminal_milestone_task_and_continue_selection(
            base_dir,
            &project_id,
            &project_record,
            &final_snapshot,
            &br.as_br_adapter(),
            &bv.as_bv_adapter(),
            started_at,
        )
        .await
        .expect("first sync should succeed");
        assert!(first);

        let replay = sync_terminal_milestone_task_and_continue_selection(
            base_dir,
            &project_id,
            &project_record,
            &final_snapshot,
            &br.as_br_adapter(),
            &bv.as_bv_adapter(),
            started_at,
        )
        .await
        .expect("replay sync should succeed");
        assert!(replay);

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
        )
        .expect("load controller")
        .expect("controller exists");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Claimed
        );
        assert_eq!(
            controller.active_bead_id.as_deref(),
            Some("ms-alpha.bead-3")
        );
        assert_eq!(controller.active_task_id, None);
        assert_eq!(bv.calls().len(), 1);
        assert_eq!(br.calls().len(), 1);
        assert_eq!(bv.calls()[0].args, vec!["--robot-next"]);
        assert_eq!(br.calls()[0].args, vec!["ready", "--json"]);
    }

    #[tokio::test]
    async fn sync_terminal_milestone_task_and_continue_selection_retries_blocked_controller_on_replay(
    ) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse started_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:30:00Z","event_type":"run_completed","details":{"run_id":"run-1","completion_rounds":0,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            started_at,
        )
        .expect("record bead start");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Completed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "completed".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let first_br =
            MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&[]))]);
        let first_bv =
            MockBvAdapter::from_responses([MockBvResponse::success(bv_no_recommendation_json())]);

        let first = sync_terminal_milestone_task_and_continue_selection(
            base_dir,
            &project_id,
            &project_record,
            &final_snapshot,
            &first_br.as_br_adapter(),
            &first_bv.as_bv_adapter(),
            started_at,
        )
        .await
        .expect("first sync should succeed");
        assert!(first);

        let blocked = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
        )
        .expect("load blocked controller")
        .expect("controller exists");
        assert_eq!(
            blocked.state,
            milestone_controller::MilestoneControllerState::Blocked
        );

        let replay_br =
            MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&[
                "ms-alpha.bead-3",
            ]))]);
        let replay_bv = MockBvAdapter::from_responses([MockBvResponse::success(bv_next_json(
            "ms-alpha.bead-3",
            "Follow-up bead",
        ))]);

        let replay = sync_terminal_milestone_task_and_continue_selection(
            base_dir,
            &project_id,
            &project_record,
            &final_snapshot,
            &replay_br.as_br_adapter(),
            &replay_bv.as_bv_adapter(),
            started_at,
        )
        .await
        .expect("replay sync should succeed");
        assert!(replay);

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
        )
        .expect("load controller")
        .expect("controller exists");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Claimed
        );
        assert_eq!(
            controller.active_bead_id.as_deref(),
            Some("ms-alpha.bead-3")
        );
        assert_eq!(controller.active_task_id, None);
        assert_eq!(first_bv.calls().len(), 1);
        assert_eq!(first_br.calls().len(), 1);
        assert_eq!(replay_bv.calls().len(), 1);
        assert_eq!(replay_br.calls().len(), 1);
    }

    #[tokio::test]
    async fn sync_terminal_milestone_task_and_continue_selection_persists_needs_operator_on_plan_failure(
    ) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse started_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:30:00Z","event_type":"run_completed","details":{"run_id":"run-1","completion_rounds":0,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            started_at,
        )
        .expect("record bead start");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Completed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "completed".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        // Corrupt plan.json after milestone sync transitions controller to selecting.
        // The sync itself reads the snapshot, not the plan, so it succeeds, but the
        // follow-up selection will fail when it tries to load planned bead membership.
        let plan_path = base_dir.join(".ralph-burning/milestones/ms-alpha/plan.json");
        std::fs::write(&plan_path, "NOT VALID JSON").expect("corrupt plan");

        // Empty mock adapters — selection should fail before reaching bv/br.
        let br = MockBrAdapter::from_responses([] as [MockBrResponse; 0]);
        let bv = MockBvAdapter::from_responses([] as [MockBvResponse; 0]);

        let result = sync_terminal_milestone_task_and_continue_selection(
            base_dir,
            &project_id,
            &project_record,
            &final_snapshot,
            &br.as_br_adapter(),
            &bv.as_bv_adapter(),
            started_at,
        )
        .await;

        // The function should succeed (not propagate the selection error).
        assert!(
            result.is_ok(),
            "sync should succeed despite selection failure: {result:?}"
        );

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
        )
        .expect("load controller")
        .expect("controller exists");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::NeedsOperator,
            "controller should land in needs_operator, not stay stuck in selecting"
        );
        assert!(
            controller
                .last_transition_reason
                .as_deref()
                .unwrap_or("")
                .contains("post-sync bead selection failed"),
            "reason should mention selection failure: {:?}",
            controller.last_transition_reason
        );
        // bv/br should never be called since the plan couldn't even be loaded.
        assert_eq!(bv.calls().len(), 0);
        assert_eq!(br.calls().len(), 0);
    }

    #[test]
    fn sync_terminal_milestone_task_leaves_failed_runs_paused_for_resume() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:05:00Z","event_type":"run_failed","details":{"run_id":"run-1","stage_id":"review","failure_class":"stage_failure","message":"failed after review","completion_rounds":0,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, now);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            now,
        )
        .expect("record bead start");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: now,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed after review".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let synced =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("sync should succeed");
        assert!(synced);

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
        )
        .expect("load controller")
        .expect("controller exists");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Claimed
        );
        assert_eq!(
            controller.active_bead_id.as_deref(),
            Some("ms-alpha.bead-2")
        );
        assert_eq!(controller.active_task_id.as_deref(), Some("bead-run"));

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(task_runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert!(task_runs[0].finished_at.is_some());
        assert_eq!(
            task_runs[0].outcome_detail.as_deref(),
            Some("failed after review")
        );

        let snapshot = load_snapshot(&FsMilestoneSnapshotStore, base_dir, &milestone.id)
            .expect("load milestone snapshot");
        assert_eq!(snapshot.status, MilestoneStatus::Paused);
        assert_eq!(snapshot.active_bead, None);

        let journal =
            read_journal(&FsMilestoneJournalStore, base_dir, &milestone.id).expect("journal");
        assert!(journal.iter().any(|event| {
            event.event_type == MilestoneEventType::StatusChanged
                && event.from_state == Some(MilestoneStatus::Running)
                && event.to_state == Some(MilestoneStatus::Paused)
        }));
    }

    #[test]
    fn sync_terminal_milestone_task_skips_paused_runs() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, now);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            now,
        )
        .expect("record bead start");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: now,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Paused,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "paused for prompt review".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let synced =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("sync should succeed");
        assert!(!synced);

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
        )
        .expect("load controller")
        .expect("controller exists");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Claimed
        );
        assert_eq!(
            controller.active_bead_id.as_deref(),
            Some("ms-alpha.bead-2")
        );
        assert_eq!(controller.active_task_id.as_deref(), Some("bead-run"));

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Running);
        assert!(task_runs[0].finished_at.is_none());

        let milestone_snapshot =
            load_snapshot(&FsMilestoneSnapshotStore, base_dir, &milestone.id).expect("snapshot");
        assert_eq!(
            milestone_snapshot.active_bead.as_deref(),
            Some("ms-alpha.bead-2")
        );
    }

    #[test]
    fn sync_terminal_milestone_task_errors_when_completed_run_lacks_run_completed_event() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, now);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            now,
        )
        .expect("record bead start");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: now,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Completed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "completed".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let error =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect_err("sync should fail");
        assert!(error
            .to_string()
            .contains("missing durable run_completed event"));

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Running);
        assert!(task_runs[0].finished_at.is_none());
    }

    #[test]
    fn resume_attempt_has_exact_lineage_only_after_durable_resume_sync() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let original_started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse original_started_at")
            .with_timezone(&Utc);
        let resumed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:20:00Z")
            .expect("parse resumed_at")
            .with_timezone(&Utc);

        let milestone = create_milestone_with_plan(base_dir, original_started_at);

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: original_started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: milestone.id.to_string(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-1".to_owned(),
                stage_cursor: StageCursor::new(StageId::Implementation, 1, 1, 1)
                    .expect("stage cursor"),
                started_at: resumed_at,
                prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
                prompt_hash_at_stage_start: "prompt-hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed before run_resumed persisted".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        assert!(
            !resume_attempt_has_exact_lineage(
                base_dir,
                &project_id,
                &project_record,
                &final_snapshot
            )
            .expect("lineage query"),
            "resume path must not synthesize milestone lineage until the resumed run is durable"
        );

        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            resumed_at,
        )
        .expect("record bead start");

        assert!(
            resume_attempt_has_exact_lineage(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("lineage query"),
            "resume path should sync terminal state once the exact run has durable milestone lineage"
        );
    }

    #[test]
    fn failed_resume_sync_uses_resumed_attempt_timestamp_for_exact_lineage() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let original_started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse original_started_at")
            .with_timezone(&Utc);
        let resumed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:20:00Z")
            .expect("parse resumed_at")
            .with_timezone(&Utc);
        let failed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:25:00Z")
            .expect("parse failed_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:20:00Z","event_type":"run_resumed","details":{"run_id":"run-1","resume_stage":"implementation","cycle":2,"completion_round":1,"max_completion_rounds":20}}
{"sequence":4,"timestamp":"2026-04-01T10:25:00Z","event_type":"run_failed","details":{"run_id":"run-1","stage_id":"implementation","failure_class":"stage_failure","message":"resume failed after durable start sync","completion_rounds":1,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, original_started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            resumed_at,
        )
        .expect("record resumed bead start");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: original_started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: milestone.id.to_string(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-1".to_owned(),
                stage_cursor: StageCursor::new(StageId::Implementation, 2, 1, 1)
                    .expect("stage cursor"),
                started_at: resumed_at,
                prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
                prompt_hash_at_stage_start: "prompt-hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "resume failed after durable start sync".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        assert!(
            resume_attempt_has_exact_lineage(
                base_dir,
                &project_id,
                &project_record,
                &final_snapshot
            )
            .expect("lineage query"),
            "failed resume should recognize durable lineage for the resumed attempt"
        );

        let synced = sync_terminal_milestone_task_with_options(
            base_dir,
            &project_id,
            &project_record,
            &final_snapshot,
            false,
        )
        .expect("sync should succeed");
        assert!(synced);

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(task_runs[0].started_at, resumed_at);
        assert_eq!(task_runs[0].finished_at, Some(failed_at));
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Failed);
    }

    #[test]
    fn failed_resume_same_run_id_retargets_lineage_to_resumed_attempt() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let original_started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse original_started_at")
            .with_timezone(&Utc);
        let first_failed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:05:00Z")
            .expect("parse first_failed_at")
            .with_timezone(&Utc);
        let resumed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:20:00Z")
            .expect("parse resumed_at")
            .with_timezone(&Utc);
        let second_failed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:25:00Z")
            .expect("parse second_failed_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:05:00Z","event_type":"run_failed","details":{"run_id":"run-1","stage_id":"review","failure_class":"stage_failure","message":"first attempt failed","completion_rounds":0,"max_completion_rounds":20}}
{"sequence":4,"timestamp":"2026-04-01T10:20:00Z","event_type":"run_resumed","details":{"run_id":"run-1","resume_stage":"implementation","cycle":2,"completion_round":1,"max_completion_rounds":20}}
{"sequence":5,"timestamp":"2026-04-01T10:25:00Z","event_type":"run_failed","details":{"run_id":"run-1","stage_id":"implementation","failure_class":"stage_failure","message":"resumed attempt failed","completion_rounds":1,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_single_bead_milestone_with_plan(base_dir, original_started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            original_started_at,
        )
        .expect("record initial bead start");
        record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("first attempt failed"),
            original_started_at,
            first_failed_at,
        )
        .expect("record initial failed completion");
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            resumed_at,
        )
        .expect("record resumed bead start");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: original_started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: milestone.id.to_string(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-1".to_owned(),
                stage_cursor: StageCursor::new(StageId::Implementation, 2, 1, 1)
                    .expect("stage cursor"),
                started_at: resumed_at,
                prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
                prompt_hash_at_stage_start: "prompt-hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "resumed attempt failed".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        assert!(
            resume_attempt_has_exact_lineage(
                base_dir,
                &project_id,
                &project_record,
                &final_snapshot
            )
            .expect("lineage query"),
            "reopened same-run attempts should retarget lineage to the resumed start time"
        );

        let synced = sync_terminal_milestone_task_with_options(
            base_dir,
            &project_id,
            &project_record,
            &final_snapshot,
            false,
        )
        .expect("sync should succeed");
        assert!(synced);

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(task_runs[0].started_at, resumed_at);
        assert_eq!(task_runs[0].finished_at, Some(second_failed_at));
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(
            task_runs[0].outcome_detail.as_deref(),
            Some("resumed attempt failed")
        );

        let snapshot = load_snapshot(&FsMilestoneSnapshotStore, base_dir, &milestone.id)
            .expect("load milestone snapshot");
        assert_eq!(snapshot.status, MilestoneStatus::Paused);
    }

    #[test]
    fn sync_terminal_milestone_task_repairs_legacy_failed_start_without_run_started() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:05:00Z","event_type":"run_failed","details":{"run_id":"run-1","stage_id":"planning","failure_class":"stage_failure","message":"failed before run_started persisted","completion_rounds":0,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, now);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            now,
        )
        .expect("record bead start");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: now,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed before run_started persisted".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let synced =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("sync should succeed");
        assert!(synced);

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Failed);
        assert!(task_runs[0].finished_at.is_some());

        let milestone_snapshot =
            load_snapshot(&FsMilestoneSnapshotStore, base_dir, &milestone.id).expect("snapshot");
        assert_eq!(milestone_snapshot.active_bead, None);
    }

    #[test]
    fn sync_terminal_milestone_task_repairs_missing_lineage_from_run_started_event() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let now = Utc::now();

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:05:00Z","event_type":"run_failed","details":{"run_id":"run-1","stage_id":"planning","failure_class":"stage_failure","message":"failed before milestone sync","completion_rounds":0,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, now);

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: now,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed before milestone sync".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let synced =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("sync should succeed");
        assert!(synced);

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(task_runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert!(task_runs[0].finished_at.is_some());
    }

    #[test]
    fn sync_terminal_milestone_task_repairs_resumed_attempt_from_run_resumed_timestamp() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let original_started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse original_started_at")
            .with_timezone(&Utc);
        let paused_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:05:00Z")
            .expect("parse paused_at")
            .with_timezone(&Utc);
        let resumed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:20:00Z")
            .expect("parse resumed_at")
            .with_timezone(&Utc);
        let completed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:30:00Z")
            .expect("parse completed_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:20:00Z","event_type":"run_resumed","details":{"run_id":"run-1","resume_stage":"implementation","cycle":2,"completion_round":1,"max_completion_rounds":20}}
{"sequence":4,"timestamp":"2026-04-01T10:30:00Z","event_type":"run_completed","details":{"run_id":"run-1","stage_id":"implementation","completion_rounds":1,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_single_bead_milestone_with_plan(base_dir, original_started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            original_started_at,
        )
        .expect("record initial bead start");
        record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("failed before durable resume sync"),
            original_started_at,
            paused_at,
        )
        .expect("record initial failed completion");
        std::fs::remove_file(base_dir.join(".ralph-burning/milestones/ms-alpha/task-runs.ndjson"))
            .expect("remove stale lineage file");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: original_started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Completed,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "completed after resume".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let synced =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("sync should succeed");
        assert!(synced);

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(task_runs[0].started_at, resumed_at);
        assert_eq!(task_runs[0].finished_at, Some(completed_at));
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Succeeded);

        let journal =
            read_journal(&FsMilestoneJournalStore, base_dir, &milestone.id).expect("journal");
        let resumed_event = journal
            .iter()
            .find(|event| {
                event.event_type == MilestoneEventType::StatusChanged
                    && event.from_state == Some(MilestoneStatus::Paused)
                    && event.to_state == Some(MilestoneStatus::Running)
            })
            .expect("repair should synthesize a paused -> running bridge");
        assert_eq!(resumed_event.timestamp, resumed_at);

        let completed_event = journal
            .iter()
            .find(|event| {
                event.event_type == MilestoneEventType::StatusChanged
                    && event.from_state == Some(MilestoneStatus::Running)
                    && event.to_state == Some(MilestoneStatus::Completed)
                    && event.timestamp == completed_at
            })
            .expect("repair should record the completed transition");
        let metadata = completed_event
            .metadata
            .as_ref()
            .expect("completed transition should include metadata");
        assert_eq!(
            metadata.get("duration_seconds"),
            Some(&serde_json::json!(900))
        );
    }

    #[test]
    fn sync_terminal_milestone_task_repairs_missing_resumed_start_when_stale_failed_lineage_exists()
    {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let original_started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse original_started_at")
            .with_timezone(&Utc);
        let failed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:05:00Z")
            .expect("parse failed_at")
            .with_timezone(&Utc);
        let resumed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:20:00Z")
            .expect("parse resumed_at")
            .with_timezone(&Utc);
        let completed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:30:00Z")
            .expect("parse completed_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:05:00Z","event_type":"run_failed","details":{"run_id":"run-1","stage_id":"review","failure_class":"stage_failure","message":"failed before resume","completion_rounds":0,"max_completion_rounds":20}}
{"sequence":4,"timestamp":"2026-04-01T10:20:00Z","event_type":"run_resumed","details":{"run_id":"run-1","resume_stage":"implementation","cycle":2,"completion_round":1,"max_completion_rounds":20}}
{"sequence":5,"timestamp":"2026-04-01T10:30:00Z","event_type":"run_completed","details":{"run_id":"run-1","stage_id":"implementation","completion_rounds":1,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_single_bead_milestone_with_plan(base_dir, original_started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            original_started_at,
        )
        .expect("record initial bead start");
        record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("failed before resume"),
            original_started_at,
            failed_at,
        )
        .expect("record initial failed completion");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: original_started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Completed,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "completed after resume".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let synced =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("sync should succeed");
        assert!(synced);

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(task_runs[0].started_at, resumed_at);
        assert_eq!(task_runs[0].finished_at, Some(completed_at));
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Succeeded);

        let journal =
            read_journal(&FsMilestoneJournalStore, base_dir, &milestone.id).expect("journal");
        assert!(journal.iter().any(|event| {
            event.event_type == MilestoneEventType::BeadStarted
                && event.bead_id.as_deref() == Some("ms-alpha.bead-2")
                && event.timestamp == resumed_at
        }));

        let resumed_event = journal
            .iter()
            .find(|event| {
                event.event_type == MilestoneEventType::StatusChanged
                    && event.from_state == Some(MilestoneStatus::Paused)
                    && event.to_state == Some(MilestoneStatus::Running)
            })
            .expect("repair should synthesize a paused -> running bridge");
        assert_eq!(resumed_event.timestamp, resumed_at);

        let completed_event = journal
            .iter()
            .find(|event| {
                event.event_type == MilestoneEventType::StatusChanged
                    && event.from_state == Some(MilestoneStatus::Running)
                    && event.to_state == Some(MilestoneStatus::Completed)
                    && event.timestamp == completed_at
            })
            .expect("repair should record the completed transition");
        let metadata = completed_event
            .metadata
            .as_ref()
            .expect("completed transition should include metadata");
        assert_eq!(
            metadata.get("duration_seconds"),
            Some(&serde_json::json!(900))
        );
    }

    #[test]
    fn sync_terminal_milestone_task_does_not_repair_missing_lineage_over_active_retry() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let original_started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse original_started_at")
            .with_timezone(&Utc);
        let retry_started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:20:00Z")
            .expect("parse retry_started_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, original_started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-2",
            "plan-v1",
            retry_started_at,
        )
        .expect("record active retry");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: original_started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "older run failed after retry already started".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let synced =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("sync should not fail");
        assert!(!synced, "sync should not synthesize stale lineage");

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].run_id.as_deref(), Some("run-2"));
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Running);
        assert_eq!(task_runs[0].finished_at, None);
    }

    #[test]
    fn sync_terminal_milestone_task_errors_when_missing_lineage_is_ambiguous() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let first_started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T09:40:00Z")
            .expect("parse first_started_at")
            .with_timezone(&Utc);
        let second_started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T09:50:00Z")
            .expect("parse second_started_at")
            .with_timezone(&Utc);
        let current_started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse current_started_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-3","first_stage":"planning","max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, first_started_at);
        std::fs::write(
            base_dir.join(".ralph-burning/milestones/ms-alpha/task-runs.ndjson"),
            format!(
                concat!(
                    "{{\"milestone_id\":\"ms-alpha\",\"bead_id\":\"ms-alpha.bead-2\",",
                    "\"project_id\":\"older-project-a\",\"run_id\":\"run-1\",\"plan_hash\":\"plan-v1\",",
                    "\"outcome\":\"running\",\"started_at\":\"{}\"}}\n",
                    "{{\"milestone_id\":\"ms-alpha\",\"bead_id\":\"ms-alpha.bead-2\",",
                    "\"project_id\":\"older-project-b\",\"run_id\":\"run-2\",\"plan_hash\":\"plan-v1\",",
                    "\"outcome\":\"running\",\"started_at\":\"{}\"}}"
                ),
                first_started_at.to_rfc3339(),
                second_started_at.to_rfc3339(),
            ),
        )
        .expect("write ambiguous active task-runs");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: current_started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "current run failed with ambiguous stale lineage".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let error =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect_err("sync should fail when stale lineage is ambiguous");
        match error {
            AppError::CorruptRecord { file, details } => {
                assert!(file.ends_with("milestones/ms-alpha/task-runs.ndjson"));
                assert!(details.contains("multiple active lineage rows exist"));
                assert!(details.contains("manual cleanup required"));
            }
            other => panic!("expected CorruptRecord, got {other:?}"),
        }

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 2);
        assert_eq!(task_runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Running);
        assert_eq!(task_runs[1].run_id.as_deref(), Some("run-2"));
        assert_eq!(task_runs[1].outcome, TaskRunOutcome::Running);
    }

    #[test]
    fn sync_terminal_milestone_task_repairs_missing_lineage_over_older_active_attempt() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let original_started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T09:40:00Z")
            .expect("parse original_started_at")
            .with_timezone(&Utc);
        let current_started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse current_started_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-2","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:05:00Z","event_type":"run_failed","details":{"run_id":"run-2","stage_id":"planning","failure_class":"stage_failure","message":"current run failed after older dangling lineage","completion_rounds":0,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, original_started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            original_started_at,
        )
        .expect("record stale older attempt");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: current_started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "current run failed after older dangling lineage".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let synced =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("sync should succeed");
        assert!(synced, "sync should repair the missing newer lineage");

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 2);
        assert_eq!(task_runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(task_runs[0].finished_at, Some(current_started_at));
        assert_eq!(
            task_runs[0].outcome_detail.as_deref(),
            Some("superseded by retry started at 2026-04-01T10:00:00+00:00")
        );
        assert_eq!(task_runs[1].run_id.as_deref(), Some("run-2"));
        assert_eq!(task_runs[1].outcome, TaskRunOutcome::Failed);
        assert_eq!(task_runs[1].started_at, current_started_at);
    }

    #[test]
    fn sync_terminal_milestone_task_repairs_stale_terminal_outcome_with_durable_timestamp() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse start")
            .with_timezone(&Utc);
        let failed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:12:00Z")
            .expect("parse failed_at")
            .with_timezone(&Utc);
        let completed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:30:00Z")
            .expect("parse completed_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:12:00Z","event_type":"run_failed","details":{"run_id":"run-1","stage_id":"review","failure_class":"stage_failure","message":"stale failure","completion_rounds":0,"max_completion_rounds":20}}
{"sequence":4,"timestamp":"2026-04-01T10:30:00Z","event_type":"run_completed","details":{"run_id":"run-1","completion_rounds":0,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            started_at,
        )
        .expect("record bead start");
        record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("stale failure"),
            started_at,
            failed_at,
        )
        .expect("record stale failed completion");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Completed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "completed".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let synced =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("sync should succeed");
        assert!(synced);

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(task_runs[0].finished_at, Some(completed_at));
        assert_eq!(task_runs[0].outcome_detail, None);

        let journal = read_journal(&FsMilestoneJournalStore, base_dir, &milestone.id)
            .expect("read milestone journal");
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| {
                matches!(
                    event.event_type,
                    crate::contexts::milestone_record::model::MilestoneEventType::BeadCompleted
                        | crate::contexts::milestone_record::model::MilestoneEventType::BeadFailed
                )
            })
            .collect();
        assert_eq!(completion_events.len(), 1);
        assert_eq!(
            completion_events[0].event_type,
            crate::contexts::milestone_record::model::MilestoneEventType::BeadCompleted
        );
        assert_eq!(completion_events[0].timestamp, completed_at);
    }

    #[test]
    fn sync_terminal_milestone_task_errors_when_failed_run_lacks_run_failed_event() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse started_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            started_at,
        )
        .expect("record bead start");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed without durable run_failed event".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let error =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect_err("sync should fail without a durable run_failed timestamp");
        match error {
            AppError::CorruptRecord { file, details } => {
                assert!(file.ends_with("projects/bead-run/journal.ndjson"));
                assert!(details.contains("missing durable run_failed event"));
            }
            other => panic!("expected CorruptRecord, got {other:?}"),
        }

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs after failed sync");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Running);
        assert_eq!(task_runs[0].finished_at, None);

        let journal = read_journal(&FsMilestoneJournalStore, base_dir, &milestone.id)
            .expect("read milestone journal");
        let failure_events: Vec<_> = journal
            .iter()
            .filter(|event| {
                event.event_type
                    == crate::contexts::milestone_record::model::MilestoneEventType::BeadFailed
            })
            .collect();
        assert!(failure_events.is_empty());
    }

    #[test]
    fn sync_terminal_milestone_task_repairs_signal_handoff_failed_snapshot_without_run_failed_event(
    ) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse started_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            started_at,
        )
        .expect("record bead start");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-1".to_owned(),
                stage_cursor: StageCursor::new(StageId::Implementation, 1, 1, 1)
                    .expect("stage cursor"),
                started_at,
                prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
                prompt_hash_at_stage_start: "prompt-hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed (interrupted by termination signal)".to_owned(),
            last_stage_resolution_snapshot: None,
        };
        FsRunSnapshotWriteStore
            .write_run_snapshot(base_dir, &project_id, &final_snapshot)
            .expect("write interrupted run snapshot");

        let synced =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("sync should repair the missing signal-handoff run_failed event");
        assert!(synced);

        let journal_events = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("read repaired project journal");
        assert_eq!(
            journal_events
                .iter()
                .filter(|event| event.event_type == JournalEventType::RunFailed)
                .count(),
            1,
            "signal-handoff repair should append exactly one missing run_failed event"
        );

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs after repaired sync");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(
            task_runs[0].outcome_detail.as_deref(),
            Some("failed (interrupted by termination signal)")
        );
    }

    #[test]
    fn sync_terminal_milestone_task_does_not_trust_stale_failed_lineage_without_run_failed_event() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse started_at")
            .with_timezone(&Utc);
        let stale_failed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:12:00Z")
            .expect("parse stale_failed_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            started_at,
        )
        .expect("record bead start");
        record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("stale failed lineage"),
            started_at,
            stale_failed_at,
        )
        .expect("record stale failed lineage");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed without durable run_failed event".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let error =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect_err("sync should fail without a durable run_failed timestamp");
        match error {
            AppError::CorruptRecord { file, details } => {
                assert!(file.ends_with("projects/bead-run/journal.ndjson"));
                assert!(details.contains("missing durable run_failed event"));
            }
            other => panic!("expected CorruptRecord, got {other:?}"),
        }

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs after failed sync");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(task_runs[0].finished_at, Some(stale_failed_at));
        assert_eq!(
            task_runs[0].outcome_detail.as_deref(),
            Some("stale failed lineage")
        );

        let journal = read_journal(&FsMilestoneJournalStore, base_dir, &milestone.id)
            .expect("read milestone journal");
        let failure_events: Vec<_> = journal
            .iter()
            .filter(|event| {
                event.event_type
                    == crate::contexts::milestone_record::model::MilestoneEventType::BeadFailed
            })
            .collect();
        assert_eq!(failure_events.len(), 1);
        assert_eq!(failure_events[0].timestamp, stale_failed_at);
    }

    #[test]
    fn sync_terminal_milestone_task_repairs_same_outcome_timestamp_with_durable_timestamp() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse start")
            .with_timezone(&Utc);
        let stale_completed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:12:00Z")
            .expect("parse stale_completed_at")
            .with_timezone(&Utc);
        let durable_completed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:30:00Z")
            .expect("parse durable_completed_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:30:00Z","event_type":"run_completed","details":{"run_id":"run-1","completion_rounds":0,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_milestone_with_plan(base_dir, started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            started_at,
        )
        .expect("record bead start");
        record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            None,
            started_at,
            stale_completed_at,
        )
        .expect("record stale succeeded completion");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Completed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "completed".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let synced =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("sync should succeed");
        assert!(synced);

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(task_runs[0].finished_at, Some(durable_completed_at));

        let journal = read_journal(&FsMilestoneJournalStore, base_dir, &milestone.id)
            .expect("read milestone journal");
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| {
                event.event_type
                    == crate::contexts::milestone_record::model::MilestoneEventType::BeadCompleted
            })
            .collect();
        assert_eq!(completion_events.len(), 1);
        assert_eq!(completion_events[0].timestamp, durable_completed_at);
    }

    #[test]
    fn sync_terminal_milestone_task_replay_is_idempotent_after_final_completion() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:00:00Z")
            .expect("parse started_at")
            .with_timezone(&Utc);
        let completed_at = chrono::DateTime::parse_from_rfc3339("2026-04-01T10:30:00Z")
            .expect("parse completed_at")
            .with_timezone(&Utc);

        std::fs::create_dir_all(base_dir.join(".ralph-burning/projects/bead-run"))
            .expect("create project dir");
        std::fs::write(
            base_dir.join(".ralph-burning/projects/bead-run/journal.ndjson"),
            r#"{"sequence":1,"timestamp":"2026-04-01T09:59:00Z","event_type":"project_created","details":{"project_id":"bead-run","flow":"docs_change"}}
{"sequence":2,"timestamp":"2026-04-01T10:00:00Z","event_type":"run_started","details":{"run_id":"run-1","first_stage":"planning","max_completion_rounds":20}}
{"sequence":3,"timestamp":"2026-04-01T10:30:00Z","event_type":"run_completed","details":{"run_id":"run-1","completion_rounds":0,"max_completion_rounds":20}}"#,
        )
        .expect("write journal");

        let milestone = create_single_bead_milestone_with_plan(base_dir, started_at);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            &milestone.id,
            "ms-alpha.bead-2",
            "bead-run",
            "run-1",
            "plan-v1",
            started_at,
        )
        .expect("record bead start");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::DocsChange,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: started_at,
            status_summary: ProjectStatusSummary::Active,
            task_source: Some(TaskSource {
                milestone_id: "ms-alpha".to_owned(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: Some("plan-v1".to_owned()),
                plan_version: Some(2),
            }),
        };
        let final_snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Completed,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "completed".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let first =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("first sync should succeed");
        assert!(first);

        let controller_journal_before_replay =
            crate::contexts::milestone_record::controller::MilestoneControllerPort::read_transition_journal(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone.id,
            )
            .expect("read controller journal")
            .len();

        let replay =
            sync_terminal_milestone_task(base_dir, &project_id, &project_record, &final_snapshot)
                .expect("replay sync should succeed");
        assert!(replay);

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
        )
        .expect("load controller")
        .expect("controller exists");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Completed
        );
        assert_eq!(
            crate::contexts::milestone_record::controller::MilestoneControllerPort::read_transition_journal(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone.id,
            )
            .expect("read controller journal after replay")
            .len(),
            controller_journal_before_replay
        );

        let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone.id)
            .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(task_runs[0].finished_at, Some(completed_at));

        let journal = read_journal(&FsMilestoneJournalStore, base_dir, &milestone.id)
            .expect("read milestone journal");
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| {
                event.event_type
                    == crate::contexts::milestone_record::model::MilestoneEventType::BeadCompleted
            })
            .collect();
        assert_eq!(completion_events.len(), 1);
    }

    #[test]
    fn acquire_recovery_writer_guard_does_not_reclaim_fresh_worktree_owner_for_stale_running_attempt(
    ) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let project_id = ProjectId::new("stale-running-owner-race").expect("project id");
        let now = Utc::now();
        let worktree_lease = WorktreeLease {
            lease_id: "lease-fresh-running-owner".to_owned(),
            task_id: "task-fresh-running-owner".to_owned(),
            project_id: project_id.to_string(),
            worktree_path: base_dir.join("worktrees/task-fresh-running-owner"),
            branch_name: "task-fresh-running-owner".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
        };
        <FsDaemonStore as DaemonStorePort>::write_lease_record(
            &FsDaemonStore,
            base_dir,
            &LeaseRecord::Worktree(worktree_lease.clone()),
        )
        .expect("write fresh worktree lease");
        <FsDaemonStore as DaemonStorePort>::acquire_writer_lock(
            &FsDaemonStore,
            base_dir,
            &project_id,
            &worktree_lease.lease_id,
        )
        .expect("acquire writer lock");
        <FsDaemonStore as DaemonStorePort>::write_task(
            &FsDaemonStore,
            base_dir,
            &DaemonTask {
                task_id: worktree_lease.task_id.clone(),
                issue_ref: "repo#42".to_owned(),
                project_id: project_id.to_string(),
                project_name: Some("fresh owner".to_owned()),
                prompt: Some("prompt".to_owned()),
                routing_command: None,
                routing_labels: vec![],
                resolved_flow: Some(FlowPreset::Standard),
                routing_source: Some(RoutingSource::DefaultFlow),
                routing_warnings: vec![],
                status: TaskStatus::Active,
                created_at: now,
                updated_at: now,
                attempt_count: 0,
                lease_id: Some(worktree_lease.lease_id.clone()),
                failure_class: None,
                failure_message: None,
                dispatch_mode: DispatchMode::Workflow,
                source_revision: None,
                requirements_run_id: None,
                workflow_run_id: None,
                repo_slug: None,
                issue_number: None,
                pr_url: None,
                last_seen_comment_id: None,
                last_seen_review_id: None,
                label_dirty: false,
            },
        )
        .expect("write daemon task");

        let stale_attempt = crate::contexts::workflow_composition::engine::RunningAttemptIdentity {
            run_id: "run-stale-attempt".to_owned(),
            started_at: now - chrono::Duration::minutes(5),
        };

        let result = super::acquire_recovery_writer_guard(
            base_dir,
            base_dir,
            &project_id,
            Some(worktree_lease.lease_id.as_str()),
            Some(&stale_attempt),
            true,
            false,
        );
        assert!(
            matches!(result, Err(AppError::ProjectWriterLockHeld { .. })),
            "fresh owner should not be reclaimed just because stale proof was observed earlier"
        );

        let writer_lock_path = FileSystem::daemon_root(base_dir)
            .join("leases")
            .join(format!("writer-{}.lock", project_id.as_str()));
        assert_eq!(
            std::fs::read_to_string(&writer_lock_path)
                .expect("read writer lock")
                .trim(),
            worktree_lease.lease_id
        );
        assert!(
            <FsDaemonStore as DaemonStorePort>::read_lease_record(
                &FsDaemonStore,
                base_dir,
                &worktree_lease.lease_id,
            )
            .is_ok(),
            "fresh worktree lease must remain intact"
        );
        let task = <FsDaemonStore as DaemonStorePort>::read_task(
            &FsDaemonStore,
            base_dir,
            &worktree_lease.task_id,
        )
        .expect("read daemon task");
        assert_eq!(task.status, TaskStatus::Active);
        assert_eq!(
            task.lease_id.as_deref(),
            Some(worktree_lease.lease_id.as_str())
        );
    }

    #[test]
    fn stop_target_pid_record_is_unchanged_rejects_pid_handoff_to_new_attempt() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let project_id = ProjectId::new("stop-target-handoff").expect("project id");
        let stale_attempt_started_at = Utc::now() - chrono::Duration::minutes(5);
        let fresh_attempt_started_at = stale_attempt_started_at + chrono::Duration::minutes(1);
        let stale_pid_record = FileSystem::write_pid_file(
            base_dir,
            &project_id,
            crate::adapters::fs::RunPidOwner::Cli,
            Some("writer-stale"),
            Some("run-stale"),
            Some(stale_attempt_started_at),
        )
        .expect("write stale pid file");
        let stale_attempt = crate::contexts::workflow_composition::engine::RunningAttemptIdentity {
            run_id: "run-stale".to_owned(),
            started_at: stale_attempt_started_at,
        };

        assert!(super::stop_target_pid_record_is_unchanged(
            base_dir,
            &project_id,
            &stale_attempt,
            &stale_pid_record,
        )
        .expect("stale pid record should match before handoff"));

        let fresh_pid_record = crate::adapters::fs::RunPidRecord {
            pid: stale_pid_record.pid,
            started_at: stale_pid_record.started_at,
            owner: crate::adapters::fs::RunPidOwner::Cli,
            writer_owner: Some("writer-fresh".to_owned()),
            run_id: Some("run-fresh".to_owned()),
            run_started_at: Some(fresh_attempt_started_at),
            proc_start_ticks: stale_pid_record.proc_start_ticks,
            proc_start_marker: stale_pid_record.proc_start_marker.clone(),
        };
        std::fs::write(
            FileSystem::live_project_root(base_dir, &project_id).join("run.pid"),
            serde_json::to_string_pretty(&fresh_pid_record).expect("serialize fresh pid record"),
        )
        .expect("overwrite run.pid with fresh attempt");

        assert!(
            !super::stop_target_pid_record_is_unchanged(
                base_dir,
                &project_id,
                &stale_attempt,
                &stale_pid_record,
            )
            .expect("handoff should be detected"),
            "stop must refuse to signal after run.pid moves to a different attempt"
        );
    }

    #[test]
    fn observed_cli_cleanup_owner_has_dead_pid_uses_persisted_cleanup_handoff_without_run_pid() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let base_dir = temp_dir.path();
        let project_id = ProjectId::new("signal-handoff-lease-reclaim").expect("project id");
        let pid_record = FileSystem::write_pid_file(
            base_dir,
            &project_id,
            RunPidOwner::Cli,
            Some("cli-signal-handoff"),
            Some("run-signal-handoff"),
            Some(Utc::now()),
        )
        .expect("write pid file");
        FileSystem::remove_pid_file(base_dir, &project_id).expect("remove run.pid");

        let live_handoff = CliWriterCleanupHandoff {
            pid: pid_record.pid,
            run_id: pid_record.run_id.clone(),
            run_started_at: pid_record.run_started_at,
            proc_start_ticks: pid_record.proc_start_ticks,
            proc_start_marker: pid_record.proc_start_marker.clone(),
        };
        let lease = CliWriterLease {
            lease_id: "cli-signal-handoff".to_owned(),
            project_id: project_id.to_string(),
            owner: "cli".to_owned(),
            acquired_at: Utc::now(),
            ttl_seconds: super::CLI_LEASE_TTL_SECONDS,
            last_heartbeat: Utc::now(),
            cleanup_handoff: Some(live_handoff.clone()),
        };
        <FsDaemonStore as DaemonStorePort>::write_lease_record(
            &FsDaemonStore,
            base_dir,
            &LeaseRecord::CliWriter(lease),
        )
        .expect("write cli lease");
        <FsDaemonStore as DaemonStorePort>::acquire_writer_lock(
            &FsDaemonStore,
            base_dir,
            &project_id,
            "cli-signal-handoff",
        )
        .expect("acquire writer lock");

        assert!(
            !super::observed_cli_cleanup_owner_has_dead_pid(
                base_dir,
                &project_id,
                "cli-signal-handoff",
            )
            .expect("live cleanup handoff should not reclaim"),
            "live cleanup handoff must not be treated as dead while the process still matches"
        );

        let reused_identity_handoff = CliWriterCleanupHandoff {
            proc_start_ticks: Some(u64::MAX),
            ..live_handoff
        };
        let replaced_lease = CliWriterLease {
            lease_id: "cli-signal-handoff".to_owned(),
            project_id: project_id.to_string(),
            owner: "cli".to_owned(),
            acquired_at: Utc::now(),
            ttl_seconds: super::CLI_LEASE_TTL_SECONDS,
            last_heartbeat: Utc::now(),
            cleanup_handoff: Some(reused_identity_handoff),
        };
        <FsDaemonStore as DaemonStorePort>::write_lease_record(
            &FsDaemonStore,
            base_dir,
            &LeaseRecord::CliWriter(replaced_lease),
        )
        .expect("rewrite cli lease with dead handoff identity");

        assert!(
            super::observed_cli_cleanup_owner_has_dead_pid(
                base_dir,
                &project_id,
                "cli-signal-handoff",
            )
            .expect("dead cleanup handoff should be reclaimable"),
            "cleanup handoff must let recovery reclaim the lock after run.pid is removed"
        );
    }

    #[cfg(unix)]
    #[test]
    fn kill_stale_backend_process_group_refuses_exited_authoritative_leader_without_safe_proof() {
        let record = crate::adapters::fs::RunBackendProcessRecord {
            pid: std::process::id(),
            recorded_at: Utc::now(),
            run_id: Some("run-backend".to_owned()),
            run_started_at: Some(Utc::now()),
            proc_start_ticks: Some(u64::MAX),
            proc_start_marker: FileSystem::proc_start_marker_for_pid(std::process::id()),
        };

        let error = match super::kill_stale_backend_process_group(&record) {
            Ok(_) => panic!("authoritative leader reuse without a live identity must be refused"),
            Err(error) => error,
        };
        assert!(
            error.to_string().contains("different live process")
                || error
                    .to_string()
                    .contains("PID/PGID reuse cannot be ruled out"),
            "unexpected refusal error: {error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn kill_tracked_descendant_processes_survives_parent_exit() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let child_pid_path = temp_dir.path().join("descendant.pid");
        let script = format!(
            "setsid sh -c 'echo $$ > \"{}\"; exec sleep 60' & while [ ! -s \"{}\" ]; do sleep 0.05; done",
            child_pid_path.display(),
            child_pid_path.display(),
        );
        let mut parent = std::process::Command::new("bash")
            .args(["-lc", &script])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn parent");
        let parent_pid = parent.id();

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while !child_pid_path.exists() || std::fs::read_to_string(&child_pid_path).is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "descendant pid file was never written"
            );
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        let child_pid = std::fs::read_to_string(&child_pid_path)
            .expect("read child pid")
            .trim()
            .parse::<u32>()
            .expect("parse child pid");
        assert!(
            FileSystem::is_pid_running_unchecked(child_pid),
            "descendant should be alive before cleanup"
        );

        let tracked = super::snapshot_descendant_processes(parent_pid)
            .expect("capture descendant processes before parent exit");
        let status = parent.wait().expect("wait for parent");
        assert!(
            status.success(),
            "parent helper should exit successfully: {status:?}"
        );
        assert!(
            FileSystem::is_pid_running_unchecked(child_pid),
            "descendant should remain alive after the parent exits"
        );

        let killed = super::kill_tracked_descendant_processes(&tracked)
            .expect("kill tracked descendants after parent exit");
        assert!(
            killed >= 1,
            "tracked cleanup should kill the orphaned child"
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while FileSystem::is_pid_running_unchecked(child_pid)
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        assert!(
            !FileSystem::is_pid_running_unchecked(child_pid),
            "tracked cleanup should remove the orphaned child process"
        );
    }

    #[tokio::test]
    async fn termination_signal_wrapper_reconciles_interrupted_snapshot_when_future_returns_without_cleanup(
    ) {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        crate::contexts::workspace_governance::initialize_workspace(temp_dir.path(), Utc::now())
            .expect("initialize workspace");
        let project_id = ProjectId::new("signal-wrapper-reconcile".to_owned()).expect("project id");
        let run_started_at = Utc::now();
        let mut snapshot = RunSnapshot::initial(20);
        snapshot.status = RunStatus::Running;
        snapshot.completion_rounds = 1;
        snapshot.status_summary = "running: Implementation".to_owned();
        snapshot.active_run = Some(ActiveRun {
            run_id: "run-signal-wrapper".to_owned(),
            stage_cursor: StageCursor::initial(StageId::Implementation),
            started_at: run_started_at,
            prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
            prompt_hash_at_stage_start: "prompt-hash".to_owned(),
            qa_iterations_current_cycle: 0,
            review_iterations_current_cycle: 0,
            final_review_restart_count: 0,
            stage_resolution_snapshot: None,
        });
        FsRunSnapshotWriteStore
            .write_run_snapshot(temp_dir.path(), &project_id, &snapshot)
            .expect("write running snapshot");
        FsJournalStore
            .append_event(
                temp_dir.path(),
                &project_id,
                &format!(
                    "{{\"sequence\":1,\"timestamp\":\"{}\",\"event_type\":\"project_created\",\"details\":{{\"project_id\":\"{}\",\"flow\":\"standard\"}}}}",
                    Utc::now().to_rfc3339(),
                    project_id
                ),
            )
            .expect("append project_created event");
        FileSystem::write_pid_file(
            temp_dir.path(),
            &project_id,
            RunPidOwner::Cli,
            Some("writer-signal-wrapper"),
            Some("run-signal-wrapper"),
            Some(run_started_at),
        )
        .expect("write pid file");

        let cancellation_token = CancellationToken::new();
        let cancellation_for_future = cancellation_token.clone();
        let future_base_dir = temp_dir.path().to_path_buf();
        let future_project_id = project_id.clone();
        let (signal_tx, signal_rx) = oneshot::channel::<()>();
        let cleanup_context = RunSignalCleanupContext {
            origin: RunSignalCleanupOrigin::Start,
            run_snapshot_read: &FsRunSnapshotStore,
            run_snapshot_write: &FsRunSnapshotWriteStore,
            journal_store: &FsJournalStore,
            log_write: &FsRuntimeLogWriteStore,
            base_dir: temp_dir.path(),
            project_id: &project_id,
            writer_owner: Some("writer-signal-wrapper"),
        };
        let future = async move {
            cancellation_for_future.cancelled().await;
            let running_snapshot = FsRunSnapshotStore
                .read_run_snapshot(&future_base_dir, &future_project_id)
                .expect("read snapshot during signal handoff");
            assert_eq!(
                running_snapshot.status,
                RunStatus::Failed,
                "signal handoff should persist an interrupted snapshot before the run future settles"
            );
            assert!(
                running_snapshot.interrupted_run.is_some(),
                "signal handoff should preserve interrupted_run before graceful shutdown waits"
            );
            assert!(
                FileSystem::read_pid_file(&future_base_dir, &future_project_id)
                    .expect("read pid file during signal handoff")
                    .is_none(),
                "signal handoff should remove run.pid before the run future settles"
            );
            let journal_events = FsJournalStore
                .read_journal(&future_base_dir, &future_project_id)
                .expect("read journal during signal handoff");
            assert_eq!(
                journal_events.len(),
                1,
                "signal handoff must not append run_failed before the engine future settles"
            );
            assert!(
                !journal_events
                    .iter()
                    .any(|event| event.event_type == JournalEventType::RunFailed),
                "signal handoff should not durably publish run_failed before the engine future settles"
            );
            FsJournalStore
                .append_event(
                    &future_base_dir,
                    &future_project_id,
                    &format!(
                        "{{\"sequence\":2,\"timestamp\":\"{}\",\"event_type\":\"run_failed\",\"details\":{{\"run_id\":\"run-signal-wrapper\",\"stage_id\":\"implementation\",\"failure_class\":\"stage_failure\",\"message\":\"future failed after signal\",\"completion_rounds\":1,\"max_completion_rounds\":20}}}}",
                        Utc::now().to_rfc3339(),
                    ),
                )
                .expect("append engine-style run_failed event");
            Err(AppError::RunStartFailed {
                reason: "future observed cancellation after publishing run_failed".to_owned(),
            })
        };

        signal_tx.send(()).expect("deliver synthetic signal");
        let error = run_with_termination_signal_waiter(
            cancellation_token,
            cleanup_context,
            async move {
                signal_rx.await.expect("receive synthetic signal");
                Ok(())
            },
            future,
        )
        .await
        .expect_err("signal wrapper should surface interruption");

        assert!(
            error.to_string().contains("termination signal"),
            "signal wrapper should surface interruption-specific context: {error}"
        );
        let updated = FsRunSnapshotStore
            .read_run_snapshot(temp_dir.path(), &project_id)
            .expect("read updated snapshot");
        assert_eq!(updated.status, RunStatus::Failed);
        assert!(updated.active_run.is_none());
        assert!(
            updated.interrupted_run.is_some(),
            "interrupted run metadata should be preserved after signal reconciliation"
        );
        assert!(
            FileSystem::read_pid_file(temp_dir.path(), &project_id)
                .expect("read pid file")
                .is_none(),
            "signal reconciliation should remove run.pid even when the engine future skipped cleanup"
        );
        let journal_events = FsJournalStore
            .read_journal(temp_dir.path(), &project_id)
            .expect("read journal after signal reconciliation");
        assert_eq!(
            journal_events
                .iter()
                .filter(|event| event.event_type == JournalEventType::RunFailed)
                .count(),
            1,
            "signal reconciliation should not append a duplicate run_failed event after the future writes one"
        );
        assert_eq!(
            journal_events.last().expect("last journal event").sequence,
            2,
            "signal reconciliation should preserve journal sequencing when the future publishes its own terminal event"
        );
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    #[test]
    fn tracked_descendant_process_matches_live_marker_only_identity() {
        let tracked = super::TrackedProcess {
            pid: std::process::id(),
            proc_start_ticks: None,
            proc_start_marker: FileSystem::proc_start_marker_for_pid(std::process::id()),
        };
        assert!(tracked.matches_live_process());

        let mismatched = super::TrackedProcess {
            proc_start_marker: Some("mismatched-start-marker".to_owned()),
            ..tracked
        };
        assert!(!mismatched.matches_live_process());
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    #[test]
    fn classify_running_snapshot_liveness_treats_marker_only_current_pid_file_as_cli_process() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let project_id = ProjectId::new("marker-only-current-run".to_owned()).expect("project id");
        let run_started_at = Utc::now();
        let pid_record = FileSystem::write_pid_file(
            temp_dir.path(),
            &project_id,
            crate::adapters::fs::RunPidOwner::Cli,
            Some("writer-alpha"),
            Some("run-1"),
            Some(run_started_at),
        )
        .expect("write pid file");

        assert!(pid_record.proc_start_ticks.is_none());
        assert!(pid_record.proc_start_marker.is_some());

        match super::classify_running_snapshot_liveness(temp_dir.path(), &project_id)
            .expect("classify liveness")
        {
            super::RunningSnapshotLiveness::CliProcess(record) => {
                assert_eq!(record.run_id.as_deref(), Some("run-1"));
            }
            _ => panic!("expected marker-only pid file to be treated as the live CLI process"),
        }
    }
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
    let mut snapshot = FsRunSnapshotStore.read_run_snapshot(&current_dir, &project_id)?;
    if let Ok(events) = FsJournalStore.read_journal(&current_dir, &project_id) {
        crate::contexts::project_run_record::queries::reconcile_snapshot_status(
            &mut snapshot,
            &events,
        );
    }

    let running_liveness = if snapshot.status == RunStatus::Running {
        let expected_attempt = running_snapshot_attempt(&snapshot)?;
        let mut liveness = classify_running_snapshot_liveness(&current_dir, &project_id)?;
        if !liveness_matches_running_attempt(&liveness, &expected_attempt) {
            snapshot = FsRunSnapshotStore.read_run_snapshot(&current_dir, &project_id)?;
            if let Ok(events) = FsJournalStore.read_journal(&current_dir, &project_id) {
                crate::contexts::project_run_record::queries::reconcile_snapshot_status(
                    &mut snapshot,
                    &events,
                );
            }
            liveness = if snapshot.status == RunStatus::Running {
                classify_running_snapshot_liveness(&current_dir, &project_id)?
            } else {
                RunningSnapshotLiveness::Stale
            };
        }
        Some(liveness)
    } else {
        None
    };

    if as_json {
        let status = if snapshot.status == RunStatus::Running
            && matches!(running_liveness, Some(RunningSnapshotLiveness::Stale))
        {
            stale_status_json_view(&project_id, &snapshot)
        } else {
            RunStatusJsonView::from_snapshot(project_id.as_str(), &snapshot)
        };
        println!("{}", format_json_status(&status)?);
        return Ok(());
    }

    let status = if snapshot.status == RunStatus::Running
        && matches!(running_liveness, Some(RunningSnapshotLiveness::Stale))
    {
        stale_status_view(&project_id, &snapshot)
    } else {
        RunStatusView::from_snapshot(project_id.as_str(), &snapshot)
    };
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
    let project_root = FileSystem::project_root(current_dir, project_id);
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
    let project_root = FileSystem::project_root(current_dir, project_id);
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
        } else if let Some(summary) = summarize_event(event) {
            println!("    {summary}");
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

fn summarize_event(event: &JournalEvent) -> Option<String> {
    match event.event_type {
        JournalEventType::ReviewerStarted => Some(format!(
            "{} {} {} {} [{} / {}]",
            event_detail_string(event, "panel")?,
            event_detail_string(event, "role")?,
            event_detail_string(event, "reviewer_id")?,
            event_detail_string(event, "phase")?,
            event_detail_string(event, "backend_family")?,
            event_detail_string(event, "model_id")?,
        )),
        JournalEventType::ReviewerCompleted => Some(format!(
            "{} {} {} {} completed in {}ms outcome={} amendments={}",
            event_detail_string(event, "panel")?,
            event_detail_string(event, "role")?,
            event_detail_string(event, "reviewer_id")?,
            event_detail_string(event, "phase")?,
            event.details.get("duration_ms")?.as_u64()?,
            event_detail_string(event, "outcome")?,
            event.details.get("amendment_count")?.as_u64()?,
        )),
        JournalEventType::AmendmentQueued => summarize_amendment_queued_event(event),
        _ => None,
    }
}

fn summarize_amendment_queued_event(event: &JournalEvent) -> Option<String> {
    let amendment_id = event_detail_string(event, "amendment_id")?;
    let source_stage = event_detail_string(event, "source_stage")?;
    let reviewer_sources = event
        .details
        .get("reviewer_sources")
        .and_then(Value::as_array)
        .map(|sources| {
            sources
                .iter()
                .filter_map(|source| {
                    Some(format!(
                        "{} [{} / {}]",
                        source.get("reviewer_id")?.as_str()?,
                        source.get("backend_family")?.as_str()?,
                        source.get("model_id")?.as_str()?,
                    ))
                })
                .collect::<Vec<_>>()
        })
        .filter(|sources| !sources.is_empty());
    Some(match reviewer_sources {
        Some(reviewer_sources) => format!(
            "{} queued from {} via {}",
            amendment_id,
            reviewer_sources.join(", "),
            source_stage
        ),
        None => format!("{amendment_id} queued via {source_stage}"),
    })
}

fn event_detail_string<'a>(event: &'a JournalEvent, key: &str) -> Option<&'a str> {
    event.details.get(key).and_then(Value::as_str)
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
