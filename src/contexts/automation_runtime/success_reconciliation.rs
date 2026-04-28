#![forbid(unsafe_code)]

//! Success reconciliation handler for completed bead tasks.
//!
//! After a bead-linked task finishes successfully, this handler:
//! 1. Moves the milestone controller into `reconciling`
//! 2. Closes the bead in `br` with a success reason (idempotently)
//! 3. Runs `br sync --flush-only` to persist the mutation
//! 4. Updates milestone state via `record_bead_completion`
//! 5. Captures next-step hints from `bv --robot-next` (informational)
//! 6. Continues milestone bead selection for non-final milestones
//! 7. Records the task-to-bead linkage outcome

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};
use std::path::Path;

use chrono::{DateTime, Utc};
use tracing::Instrument;

use crate::adapters::br_health::{beads_health_failure_details, check_beads_health};
use crate::adapters::br_models::{BeadDetail, BeadStatus, BeadSummary};
use crate::adapters::br_process::{
    BrAdapter, BrCommand, BrError, BrMutationAdapter, ProcessRunner, SyncIfDirtyHealthError,
};
use crate::adapters::bv_process::{BvAdapter, BvProcessRunner, NextBeadResponse};
use crate::adapters::fs::{
    FileSystem, FsArtifactStore, FsMilestoneControllerStore, FsMilestoneJournalStore,
    FsMilestoneSnapshotStore, FsPlannedElsewhereMappingStore, FsTaskRunLineageStore,
};
use crate::cli::run::{select_next_milestone_bead, select_next_milestone_bead_from_recommendation};
use crate::contexts::milestone_record::bead_refs::br_show_output_indicates_missing;
use crate::contexts::milestone_record::bundle::canonicalize_bead_reference;
use crate::contexts::milestone_record::controller as milestone_controller;
use crate::contexts::milestone_record::model::{
    MilestoneEventType, MilestoneId, MilestoneJournalEvent, MilestoneStatus,
    PlannedElsewhereMapping, TaskRunOutcome,
};
use crate::contexts::milestone_record::queries::bead_ownership_text_similarity;
use crate::contexts::milestone_record::service::{
    self as milestone_service, CompletionMilestoneDisposition, MilestoneJournalPort,
};
use crate::contexts::project_run_record::service::ArtifactStorePort;
use crate::contexts::project_run_record::task_prompt_contract::milestone_prefix_of;
use crate::contexts::workflow_composition::panel_contracts::{
    AmendmentClassification, FinalReviewAggregatePayload, FinalReviewCanonicalAmendment, RecordKind,
};
use crate::contexts::workflow_composition::payloads::{ClassifiedFinding, StagePayload};
use crate::contexts::workflow_composition::review_classification::Severity;
use crate::contexts::workspace_governance::config::{
    CliBackendOverrides, EffectiveConfig, DEFAULT_EXISTING_BEAD_MATCH_THRESHOLD_SCORE,
    DEFAULT_NEW_BEAD_PROPOSAL_THRESHOLD, DEFAULT_PARSIMONIOUS_BEAD_CREATION_ENABLED,
};
use crate::shared::domain::{ProjectId, StageId};
/// Outcome of the success reconciliation process.
#[derive(Debug, Clone)]
pub struct ReconciliationOutcome {
    /// The bead that was closed.
    pub bead_id: String,
    /// The task that completed.
    pub task_id: String,
    /// Whether the bead was already closed (idempotent re-run).
    pub was_already_closed: bool,
    /// Next-step hint from bv, if available.
    pub next_step_hint: Option<NextBeadResponse>,
    /// Operator-visible issue raised while advancing to the next bead.
    /// When present, reconciliation either moved the controller into a safe
    /// state or returned an error if even that fallback persistence failed.
    pub next_step_selection_warning: Option<String>,
    /// Timestamp of the reconciliation.
    pub reconciled_at: DateTime<Utc>,
}

#[derive(Debug, Default, Clone, Copy)]
struct PlannedElsewhereVerificationSummary {
    mappings_verified: usize,
    comments_posted: usize,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ProposedBeadReconciliationSummary {
    amendments_processed: usize,
    records_written: usize,
}

/// Error conditions that require operator intervention.
#[derive(Debug)]
pub enum ReconciliationError {
    /// `br close` failed — bead left open, controller should transition to
    /// needs-operator state.
    BrCloseFailed {
        bead_id: String,
        task_id: String,
        details: String,
    },
    /// `br sync --flush-only` failed after a successful close.
    BrSyncFailed {
        bead_id: String,
        task_id: String,
        details: String,
    },
    /// Milestone state update failed.
    MilestoneUpdateFailed {
        bead_id: String,
        task_id: String,
        details: String,
    },
}

impl std::error::Error for ReconciliationError {}

impl std::fmt::Display for ReconciliationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BrCloseFailed {
                bead_id,
                task_id,
                details,
            } => write!(
                f,
                "br close failed for bead={bead_id} task={task_id}: {details}"
            ),
            Self::BrSyncFailed {
                bead_id,
                task_id,
                details,
            } => write!(
                f,
                "br sync failed after closing bead={bead_id} task={task_id}: {details}"
            ),
            Self::MilestoneUpdateFailed {
                bead_id,
                task_id,
                details,
            } => write!(
                f,
                "milestone update failed for bead={bead_id} task={task_id}: {details}"
            ),
        }
    }
}

fn ensure_beads_mutation_health(
    base_dir: &Path,
    bead_id: &str,
    task_id: &str,
) -> Result<(), ReconciliationError> {
    if let Some(details) = beads_health_failure_details(&check_beads_health(base_dir)) {
        return Err(ReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: format!("refusing to mutate beads because bead state is unsafe: {details}"),
        });
    }

    Ok(())
}

fn make_beads_sync_health_error(
    bead_id: &str,
    task_id: &str,
    details: &str,
) -> ReconciliationError {
    ReconciliationError::BrSyncFailed {
        bead_id: bead_id.to_owned(),
        task_id: task_id.to_owned(),
        details: format!(
            "bead '{bead_id}' was locally closed but bead state became unsafe before br sync --flush-only: {details}. The bead remains locally closed in br; resolve the bead-state issue and rerun `br sync --flush-only`."
        ),
    }
}

fn beads_mutation_health_warning(base_dir: &Path) -> Option<String> {
    beads_health_failure_details(&check_beads_health(base_dir))
        .map(|details| format!("refusing to mutate beads because bead state is unsafe: {details}"))
}

/// Check whether a bead is already closed by querying `br show <id> --json`.
///
/// Returns `true` if the bead status is `Closed`, `false` otherwise.
/// Returns `Err` if the query itself fails.
async fn is_bead_already_closed<R: ProcessRunner>(
    br: &BrAdapter<R>,
    bead_id: &str,
) -> Result<bool, BrError> {
    use crate::adapters::br_models::BeadDetail;
    let cmd = BrCommand::show(bead_id);
    let detail: BeadDetail = br.exec_json(&cmd).await?;
    Ok(detail.status == BeadStatus::Closed)
}

/// Run the success reconciliation handler after a bead task completes.
///
/// This is the main entry point. It performs all steps in order:
/// 1. Move the controller into `reconciling`
/// 2. Close bead (idempotent — skips if already closed)
/// 3. Sync flush
/// 4. Update milestone state
/// 5. Capture next-step hints (best-effort)
/// 6. Continue selecting the next bead for non-final milestones
/// 7. Return the linkage outcome
///
/// On `br close` or `br sync` failure, returns `ReconciliationError` so the
/// caller can transition the controller to needs-operator state.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_success<R: ProcessRunner, V: BvProcessRunner>(
    br_mutation: &BrMutationAdapter<R>,
    br_read: &BrAdapter<R>,
    bv: Option<&BvAdapter<V>>,
    base_dir: &Path,
    bead_id: &str,
    task_id: &str,
    project_id: &str,
    milestone_id_str: &str,
    run_id: &str,
    plan_hash: Option<&str>,
    started_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<ReconciliationOutcome, ReconciliationError> {
    async move {
        let milestone_id = MilestoneId::new(milestone_id_str).map_err(|e| {
            ReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!("invalid milestone id: {e}"),
            }
        })?;

        // Guard: if a previous reconciliation already succeeded and the selector
        // advanced the controller to the next bead, `sync_controller_task_reconciling`
        // would reject the replay because the active bead no longer matches.
        // Detect this case and skip the reconciling transition — the rest of the
        // reconciliation steps (close, sync, milestone update) are already idempotent.
        let controller_already_advanced = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone_id,
        )
        .map_err(|e| ReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: format!("failed to load controller for replay guard: {e}"),
        })?
        .is_some_and(|c| {
            c.active_bead_id.as_deref() != Some(bead_id)
                && c.active_bead_id.is_some()
                && !matches!(
                    c.state,
                    milestone_controller::MilestoneControllerState::Idle
                )
        });

        if !controller_already_advanced {
            milestone_controller::sync_controller_task_reconciling(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone_id,
                bead_id,
                project_id,
                "workflow execution completed successfully; reconciling milestone state",
                now,
            )
            .map_err(|e| ReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: e.to_string(),
            })?;
        }

        // Step 1: Close the bead idempotently.
        let was_already_closed =
            close_bead_idempotent(base_dir, br_mutation, br_read, bead_id, task_id).await?;

        // Step 2: Sync flush — always runs, even if bead was already closed.
        // A crash between br close and sync would leave local bead state dirty.
        // On re-run the bead appears closed but the flush never happened, so we
        // must sync unconditionally to guarantee crash-safe idempotency.
        //
        // Note: was_already_closed is NOT a safe proxy for "sync already completed".
        // A crash between close and sync produces was_already_closed=true with an
        // un-flushed local state. Sync failures must remain fatal regardless of
        // was_already_closed to prevent proceeding with an un-synced bead close.
        sync_after_close(base_dir, br_mutation, bead_id, task_id).await?;

        // Step 3: Update milestone state.
        let milestone_status = update_milestone_state(
            base_dir,
            bead_id,
            task_id,
            project_id,
            &milestone_id,
            run_id,
            plan_hash,
            started_at,
            now,
            controller_already_advanced,
        )?;

        // Step 3b: Verify planned-elsewhere mappings and post comments (best-effort).
        // The engine records unverified mappings during final-review; this step
        // performs the actual stale-bead lookup and optional br comment posting
        // that the engine cannot do (it lacks BrAdapter access).
        let planned_elsewhere_summary = verify_planned_elsewhere_after_success(
            br_mutation,
            br_read,
            base_dir,
            bead_id,
            milestone_id_str,
            project_id,
            run_id,
        )
        .await;

        let proposed_bead_summary = reconcile_terminal_review_classifications_for_milestone(
            br_mutation,
            br_read,
            base_dir,
            bead_id,
            task_id,
            milestone_id_str,
            project_id,
            run_id,
        )
        .await?;

        // Step 4: Capture next-step hints (best-effort, never blocks reconciliation).
        let (next_step_hint, prefetched_selection) = if let Some(bv_adapter) = bv {
            match capture_next_step_hint(bv_adapter).await {
                HintCaptureOutcome::Captured(hint) => {
                    // Step 4b: Persist hint to disk so downstream selection logic
                    // can read it in a later daemon cycle. Overwrites any stale hint
                    // from a previous bead's run.
                    persist_next_step_hint(base_dir, milestone_id_str, &hint);
                    (Some(hint.clone()), Some(Some(hint)))
                }
                HintCaptureOutcome::NoRecommendation => {
                    // bv succeeded but has no actionable recommendation.
                    // Remove any previously persisted hint so downstream
                    // selection does not act on a stale pointer to an
                    // already-completed bead.
                    delete_stale_hint(base_dir, milestone_id_str);
                    (None, Some(None))
                }
                HintCaptureOutcome::BvFailed => {
                    // bv failed (transient error, binary not found, etc.).
                    // Leave any existing hint untouched — a transient bv outage
                    // should not erase a previously persisted valid hint.
                    (None, None)
                }
            }
        } else {
            // bv not configured — leave any existing hint untouched.
            (None, None)
        };

        let mut next_step_selection_warning = None;
        if milestone_status != MilestoneStatus::Completed && !controller_already_advanced {
            if let Some(bv_adapter) = bv {
                // Reconciliation already closed and synced the completed bead, so
                // it is safe to continue directly into the same bv/br-validated
                // selection flow that the CLI uses. This keeps daemon-driven
                // milestones from stalling in `selecting` until an operator reruns
                // the CLI helper manually.
                let selection_result = match prefetched_selection {
                    Some(recommendation) => {
                        select_next_milestone_bead_from_recommendation(
                            base_dir,
                            &milestone_id,
                            br_read,
                            recommendation,
                            now,
                        )
                        .await
                    }
                    None => {
                        select_next_milestone_bead(
                            base_dir,
                            &milestone_id,
                            br_read,
                            bv_adapter,
                            now,
                        )
                        .await
                    }
                };

                if let Err(error) = selection_result {
                    let warning = persist_selection_failure_after_reconciliation(
                        base_dir,
                        &milestone_id,
                        bead_id,
                        task_id,
                        error.to_string(),
                        now,
                    )?;
                    tracing::warn!(
                        bead_id = bead_id,
                        task_id = task_id,
                        warning = %warning,
                        "post-reconciliation selection failed after bead close+sync"
                    );
                    next_step_selection_warning = Some(warning);
                }
            }
        }

        tracing::info!(
            operation = "reconcile_success",
            outcome = "success",
            amendments_processed = proposed_bead_summary.amendments_processed,
            proposed_bead_records_written = proposed_bead_summary.records_written,
            planned_elsewhere_mappings = planned_elsewhere_summary.mappings_verified,
            comments_posted = planned_elsewhere_summary.comments_posted,
            "success reconciliation completed"
        );

        Ok(ReconciliationOutcome {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            was_already_closed,
            next_step_hint,
            next_step_selection_warning,
            reconciled_at: now,
        })
    }
    .instrument(tracing::info_span!(
        "reconcile_success",
        milestone_id = milestone_id_str,
        bead_id = bead_id,
        task_id = task_id
    ))
    .await
}

fn persist_selection_failure_after_reconciliation(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    task_id: &str,
    details: String,
    now: DateTime<Utc>,
) -> Result<String, ReconciliationError> {
    let warning = format!("next-bead selection after reconciliation failed: {details}");
    milestone_controller::sync_controller_state(
        &FsMilestoneControllerStore,
        base_dir,
        milestone_id,
        milestone_controller::ControllerTransitionRequest::new(
            milestone_controller::MilestoneControllerState::NeedsOperator,
            warning.clone(),
        ),
        now,
    )
    .map_err(|error| ReconciliationError::MilestoneUpdateFailed {
        bead_id: bead_id.to_owned(),
        task_id: task_id.to_owned(),
        details: format!(
            "{warning}; additionally failed to persist needs_operator controller state: {error}"
        ),
    })?;

    Ok(warning)
}

/// Close a bead idempotently. If the bead is already closed, returns
/// `Ok(true)`. If the status update succeeds, returns `Ok(false)`.
///
/// On failure, returns `ReconciliationError::BrCloseFailed`.
async fn close_bead_idempotent<R: ProcessRunner>(
    base_dir: &Path,
    br_mutation: &BrMutationAdapter<R>,
    br_read: &BrAdapter<R>,
    bead_id: &str,
    task_id: &str,
) -> Result<bool, ReconciliationError> {
    // Check current status for idempotency.
    match is_bead_already_closed(br_read, bead_id).await {
        Ok(true) => return Ok(true),
        Ok(false) => {}
        Err(e) => {
            // If we can't read status, try the status update anyway; br update
            // is idempotent for an already-closed bead.
            tracing::warn!(
                bead_id = bead_id,
                task_id = task_id,
                error = %e,
                "could not read bead status for idempotency check, proceeding with status update"
            );
        }
    }

    ensure_beads_mutation_health(base_dir, bead_id, task_id)?;
    match br_mutation.update_bead_status(bead_id, "closed").await {
        Ok(_) => Ok(false),
        Err(e) => {
            // Check if the failure is because the bead is already closed.
            // Some br implementations return an error for idempotent updates.
            if let Ok(true) = is_bead_already_closed(br_read, bead_id).await {
                br_mutation
                    .restore_pending_status_update(bead_id, "closed")
                    .await
                    .map_err(|restore_error| ReconciliationError::BrCloseFailed {
                        bead_id: bead_id.to_owned(),
                        task_id: task_id.to_owned(),
                        details: format!(
                            "{e}; additionally failed to restore pending closed-status mutation after observing the bead closed: {restore_error}"
                        ),
                    })?;
                return Ok(true);
            }
            Err(ReconciliationError::BrCloseFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: e.to_string(),
            })
        }
    }
}

/// Run `br sync --flush-only` after a successful close.
async fn sync_after_close<R: ProcessRunner>(
    base_dir: &Path,
    br_mutation: &BrMutationAdapter<R>,
    bead_id: &str,
    task_id: &str,
) -> Result<(), ReconciliationError> {
    match br_mutation.sync_own_dirty_if_beads_healthy(base_dir).await {
        Ok(crate::adapters::br_process::SyncIfDirtyOutcome::Clean) => {
            tracing::debug!(
                bead_id = bead_id,
                task_id = task_id,
                "no pending local bead mutations remain after close replay; skipping br sync --flush-only"
            );
        }
        Ok(crate::adapters::br_process::SyncIfDirtyOutcome::Flushed { .. }) => {}
        Err(SyncIfDirtyHealthError::UnsafeBeadsState { details }) => {
            return Err(make_beads_sync_health_error(bead_id, task_id, &details));
        }
        Err(SyncIfDirtyHealthError::Br(error)) => {
            return Err(ReconciliationError::BrSyncFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: error.to_string(),
            });
        }
    }
    Ok(())
}

/// Update milestone state: record bead completion and reconcile progress.
#[allow(clippy::too_many_arguments)]
fn update_milestone_state(
    base_dir: &Path,
    bead_id: &str,
    task_id: &str,
    project_id: &str,
    milestone_id: &MilestoneId,
    run_id: &str,
    plan_hash: Option<&str>,
    started_at: DateTime<Utc>,
    now: DateTime<Utc>,
    controller_already_advanced: bool,
) -> Result<MilestoneStatus, ReconciliationError> {
    // Record the task-to-bead linkage as outcome_detail so the durable
    // lineage row and journal payload include the daemon task_id.
    let linkage_detail = format!("task_id={task_id}");

    // Check whether a terminal lineage row already exists for this
    // run, mirroring the CLI pattern in cli/run.rs:236-240, 347-369.
    //
    // Matches project_id + run_id (ignoring started_at) with a terminal
    // outcome. The started_at-insensitive check handles resumed runs
    // where started_at may differ between the original record_bead_start
    // and this reconciliation call. A stricter (project_id, run_id,
    // started_at) check is subsumed by this broader one.
    //
    // When true, routes through `repair_task_run_with_disposition`
    // which tolerates mismatched outcome_detail (e.g. "first bead
    // completed" from the CLI vs "task_id=..." from the daemon).
    let already_terminal_for_run = milestone_service::find_runs_for_bead(
        &FsTaskRunLineageStore,
        base_dir,
        milestone_id,
        bead_id,
    )
    .unwrap_or_default()
    .iter()
    .any(|entry| {
        entry.project_id == project_id
            && entry.run_id.as_deref() == Some(run_id)
            && entry.outcome.is_terminal()
    });

    if already_terminal_for_run {
        milestone_service::repair_task_run_with_disposition(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            milestone_id,
            bead_id,
            project_id,
            run_id,
            plan_hash,
            started_at,
            TaskRunOutcome::Succeeded,
            Some(linkage_detail),
            now,
            CompletionMilestoneDisposition::ReconcileFromLineage,
        )
    } else {
        milestone_service::record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            milestone_id,
            bead_id,
            project_id,
            run_id,
            plan_hash,
            TaskRunOutcome::Succeeded,
            Some(&linkage_detail),
            started_at,
            now,
        )
    }
    .map_err(|e| ReconciliationError::MilestoneUpdateFailed {
        bead_id: bead_id.to_owned(),
        task_id: task_id.to_owned(),
        details: e.to_string(),
    })?;

    let snapshot =
        milestone_service::load_snapshot(&FsMilestoneSnapshotStore, base_dir, milestone_id)
            .map_err(|e| ReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: e.to_string(),
            })?;
    // Skip controller transition on replay when the controller has already
    // advanced to the next bead — the transition would be illegal (e.g.,
    // Claimed -> Selecting).
    if !controller_already_advanced {
        let (next_state, reason) = if snapshot.status == MilestoneStatus::Completed {
            (
                milestone_controller::MilestoneControllerState::Completed,
                "reconciliation closed the final bead and completed the milestone",
            )
        } else {
            (
                milestone_controller::MilestoneControllerState::Selecting,
                "reconciliation recorded the bead outcome and returned the controller to bead selection",
            )
        };
        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base_dir,
            milestone_id,
            milestone_controller::ControllerTransitionRequest::new(next_state, reason),
            now,
        )
        .map_err(|e| ReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: e.to_string(),
        })?;
    }

    Ok(snapshot.status)
}

/// Outcome of attempting to capture a next-step hint from bv.
#[derive(Debug)]
enum HintCaptureOutcome {
    /// bv returned a valid recommendation.
    Captured(NextBeadResponse),
    /// bv succeeded but returned no actionable recommendation (e.g.
    /// `{"message":"No actionable items available"}`).  Any previously
    /// persisted hint is stale and should be removed.
    NoRecommendation,
    /// bv failed to execute (transient error, binary not found).
    /// Existing hints should be left untouched.
    BvFailed,
}

/// A message-only response from `bv --robot-next` when no actionable beads
/// remain (e.g. `{"message":"No actionable items available"}`).
#[derive(serde::Deserialize)]
struct BvMessageOnlyResponse {
    #[allow(dead_code)]
    message: String,
}

/// Capture next-step hints from `bv --robot-next`. Best-effort: never blocks
/// reconciliation.
///
/// Returns:
/// - `Captured` when bv returned a valid `NextBeadResponse`.
/// - `NoRecommendation` when bv succeeded but returned a message-only
///   response (no actionable beads). Stale hints should be removed.
/// - `BvFailed` on transient errors (binary not found, exit error).
///   Existing hints must be left untouched.
async fn capture_next_step_hint<V: BvProcessRunner>(bv: &BvAdapter<V>) -> HintCaptureOutcome {
    let cmd = crate::adapters::bv_process::BvCommand::robot_next();
    match bv.exec_json::<NextBeadResponse>(&cmd).await {
        Ok(response) => HintCaptureOutcome::Captured(response),
        Err(e) => {
            // Only check for a message-only "no recommendation" response
            // on BvParseError (bv exited 0 but returned unexpected JSON).
            // BvExitError means bv genuinely failed — even if its stdout
            // happens to contain {"message":"..."}, treating it as
            // NoRecommendation would incorrectly delete a valid hint.
            if let crate::adapters::bv_process::BvError::BvParseError { ref raw_output, .. } = e {
                if serde_json::from_str::<BvMessageOnlyResponse>(raw_output).is_ok() {
                    return HintCaptureOutcome::NoRecommendation;
                }
            }
            tracing::debug!(
                error = %e,
                "bv --robot-next hint capture failed (non-blocking)"
            );
            HintCaptureOutcome::BvFailed
        }
    }
}

/// Persist the next-step hint to `{base_dir}/.ralph-burning/milestones/{milestone_id}/next_step_hint.json`
/// so downstream selection logic can read it in a later daemon cycle.
/// Best-effort: failures are logged but never block reconciliation.
///
/// # Directory precondition
/// The milestone directory is guaranteed to exist at this point because
/// `update_milestone_state` (step 3) already wrote to the same directory via
/// `FsMilestoneSnapshotStore`/`FsMilestoneJournalStore`/`FsTaskRunLineageStore`.
/// If step 3 failed the reconciliation would have returned early before reaching
/// this function.
/// Atomically persist a next-step hint to `next_step_hint.json`.
///
/// Uses write-to-tmpfile + rename to guarantee readers never see truncated
/// JSON (torn-read protection). Note: on Linux the rename is only durable
/// after an `fsync` on the parent directory; without it a crash after rename
/// could lose the file entry. Since hints are best-effort and non-blocking
/// (a missing hint simply means the operator decides the next bead manually),
/// full crash durability is not required here.
fn persist_next_step_hint(base_dir: &Path, milestone_id_str: &str, hint: &NextBeadResponse) {
    // Atomic write (tmp + rename) prevents torn reads, but the rename is not
    // crash-durable on all Linux filesystems without an fsync on the parent
    // directory. Hints are best-effort and non-blocking, so losing one on
    // crash is acceptable — we intentionally skip the fsync.
    let Ok(milestone_id) = MilestoneId::new(milestone_id_str) else {
        return;
    };
    let milestone_dir = crate::adapters::fs::FileSystem::milestone_root(base_dir, &milestone_id);
    let hint_path = milestone_dir.join("next_step_hint.json");
    match serde_json::to_string_pretty(hint) {
        Ok(json) => {
            // Atomic write: write to temp file, then rename (POSIX atomic).
            // Prevents readers from seeing truncated JSON on crash.
            let tmp_path = milestone_dir.join("next_step_hint.json.tmp");
            if let Err(e) = std::fs::write(&tmp_path, &json) {
                tracing::debug!(
                    error = %e,
                    path = %tmp_path.display(),
                    "failed to write next_step_hint temp file (non-blocking)"
                );
                return;
            }
            if let Err(e) = std::fs::rename(&tmp_path, &hint_path) {
                tracing::debug!(
                    error = %e,
                    path = %hint_path.display(),
                    "failed to rename next_step_hint temp file (non-blocking)"
                );
                // Clean up temp file on rename failure.
                let _ = std::fs::remove_file(&tmp_path);
            }
        }
        Err(e) => {
            tracing::debug!(
                error = %e,
                "failed to serialize next_step_hint (non-blocking)"
            );
        }
    }
}

/// Remove a previously persisted `next_step_hint.json` when bv explicitly
/// reports no actionable recommendation. Best-effort: failures are logged
/// but never block reconciliation.
fn delete_stale_hint(base_dir: &Path, milestone_id_str: &str) {
    let Ok(milestone_id) = MilestoneId::new(milestone_id_str) else {
        return;
    };
    let hint_path = crate::adapters::fs::FileSystem::milestone_root(base_dir, &milestone_id)
        .join("next_step_hint.json");
    if hint_path.exists() {
        if let Err(e) = std::fs::remove_file(&hint_path) {
            tracing::debug!(
                error = %e,
                path = %hint_path.display(),
                "failed to remove stale next_step_hint (non-blocking)"
            );
        }
    }
}

/// Best-effort verification and commenting for planned-elsewhere mappings.
///
/// Loads mappings for this milestone from the journal, filters for unverified
/// ones belonging to this bead, then runs four phases:
/// 1. Verify mapped-to beads exist via `br show`
/// 2. Persist verified records to journal (durable before commenting)
/// 3. Post `br comments add` on verified beads (only if Phase 2 succeeded)
/// 4. Flush br mutations
///
/// Failures are logged but never block reconciliation.
///
/// If no mappings exist at all for this bead (e.g. because
/// `record_planned_elsewhere_amendments` in engine.rs failed after the stage
/// commit), attempts to reconstruct them from the persisted final-review
/// aggregate payload.
async fn verify_planned_elsewhere_after_success<R: ProcessRunner>(
    br_mutation: &BrMutationAdapter<R>,
    br_read: &BrAdapter<R>,
    base_dir: &Path,
    bead_id: &str,
    milestone_id_str: &str,
    project_id: &str,
    run_id: &str,
) -> PlannedElsewhereVerificationSummary {
    let Ok(milestone_id) = MilestoneId::new(milestone_id_str) else {
        return PlannedElsewhereVerificationSummary::default();
    };
    let mappings = match milestone_service::load_planned_elsewhere_mappings(
        &FsPlannedElsewhereMappingStore,
        &FsMilestoneJournalStore,
        base_dir,
        &milestone_id,
    ) {
        Ok(m) => m,
        Err(e) => {
            tracing::debug!(
                error = %e,
                "failed to load planned-elsewhere mappings for verification (non-blocking)"
            );
            return PlannedElsewhereVerificationSummary::default();
        }
    };

    let all_bead_mappings: Vec<_> = mappings
        .iter()
        .filter(|m| m.active_bead_id == bead_id && m.run_id.as_deref() == Some(run_id))
        .cloned()
        .collect();

    // Reconstruct any planned-elsewhere amendments from persisted final-review
    // aggregates that are missing from the journal.  Also returns the
    // authoritative max completion_round from the aggregates — this is the
    // source of truth for which round is "latest", even if that round wrote
    // zero PE mappings (meaning the finding was fixed/rejected).
    let (reconstructed, authoritative_max_round) = reconstruct_missing_pe_mappings(
        base_dir,
        project_id,
        bead_id,
        &milestone_id,
        &all_bead_mappings,
        run_id,
    );

    // Fall back to legacy mappings (run_id: None) when no current-run
    // mappings exist and reconstruction found nothing.  Without this
    // fallback, legacy unverified PE mappings would never be verified
    // or receive comments.
    let all_bead_mappings = if all_bead_mappings.is_empty() && reconstructed.is_empty() {
        mappings
            .into_iter()
            .filter(|m| m.active_bead_id == bead_id && m.run_id.is_none())
            .collect()
    } else {
        all_bead_mappings
    };

    // Filter journal mappings to only the authoritative round.  If the
    // aggregates tell us the latest round is N, only mappings from round N
    // survive — earlier rounds' PE decisions are superseded.  If no aggregate
    // was found, fall back to the max round from the journal mappings
    // themselves (legacy / no-aggregate scenario).
    let effective_max_round = authoritative_max_round.or_else(|| {
        all_bead_mappings
            .iter()
            .filter_map(|m| m.completion_round)
            .max()
    });
    let bead_mappings: Vec<_> = if let Some(max_round) = effective_max_round {
        all_bead_mappings
            .into_iter()
            .filter(|m| m.completion_round == Some(max_round))
            .collect()
    } else {
        all_bead_mappings
    };

    let mut unverified: Vec<_> = bead_mappings
        .into_iter()
        .filter(|m| !m.mapped_bead_verified)
        .collect();
    unverified.extend(reconstructed);

    if unverified.is_empty() {
        return PlannedElsewhereVerificationSummary::default();
    }

    let post_comments = std::env::var("RALPH_BURNING_PLANNED_ELSEWHERE_COMMENTS")
        .map(|v| v != "0" && v.to_lowercase() != "false")
        .unwrap_or(true);

    // Phase 1: Verify only — no comments. Comment posting happens in Phase 3
    // after verified records are durably persisted, preventing duplicate
    // comments on replay.
    let outcomes = super::planned_elsewhere::verify_mappings(br_read, bead_id, &unverified).await;

    for outcome in &outcomes {
        if let Some(warning) = &outcome.warning {
            tracing::warn!(
                mapped_to_bead_id = outcome.mapping.mapped_to_bead_id.as_str(),
                warning = warning.as_str(),
                "planned-elsewhere verification warning"
            );
        }
    }

    // Phase 2: Persist verified mappings to the journal BEFORE posting any
    // comments. Track which mappings were durably persisted so Phase 3 only
    // posts comments for those — if persist fails, skipping the comment
    // prevents duplicates on replay (the mapping stays unverified so replay
    // will re-verify and re-attempt both persist and comment).
    // Gate by outcome index (not just mapped_to_bead_id) so two findings
    // mapped to the same bead are tracked independently.
    let now = Utc::now();
    let mut durably_verified_indices: std::collections::HashSet<usize> =
        std::collections::HashSet::new();
    for (idx, outcome) in outcomes.iter().enumerate() {
        if outcome.verified {
            let verified_mapping = PlannedElsewhereMapping {
                active_bead_id: outcome.mapping.active_bead_id.clone(),
                finding_summary: outcome.mapping.finding_summary.clone(),
                mapped_to_bead_id: outcome.mapping.mapped_to_bead_id.clone(),
                recorded_at: now,
                mapped_bead_verified: true,
                run_id: outcome.mapping.run_id.clone(),
                completion_round: outcome.mapping.completion_round,
            };
            if let Err(e) = milestone_service::record_planned_elsewhere_mapping(
                &FsMilestoneJournalStore,
                &FsPlannedElsewhereMappingStore,
                base_dir,
                &milestone_id,
                &verified_mapping,
            ) {
                tracing::warn!(
                    mapped_to_bead_id = outcome.mapping.mapped_to_bead_id.as_str(),
                    error = %e,
                    "failed to persist verified planned-elsewhere mapping (non-blocking)"
                );
            } else {
                durably_verified_indices.insert(idx);
            }
        }
    }

    // Phase 3: Post comments only for mappings whose verified state was
    // durably recorded in Phase 2. This prevents duplicate comments on
    // replay: if persist failed, the mapping stays unverified and replay
    // will re-attempt both persist and comment together.
    let mut commented_count = 0usize;
    if post_comments {
        for (idx, outcome) in outcomes.iter().enumerate() {
            if !durably_verified_indices.contains(&idx) {
                continue;
            }
            if let Some(details) = beads_mutation_health_warning(base_dir) {
                tracing::warn!(
                    active_bead_id = bead_id,
                    mapped_to_bead_id = outcome.mapping.mapped_to_bead_id.as_str(),
                    details = details.as_str(),
                    "skipping planned-elsewhere comment because bead state is unsafe"
                );
                break;
            }
            let comment_text = format!(
                "Planned-elsewhere mapping from {}: {}",
                outcome.mapping.active_bead_id, outcome.mapping.finding_summary
            );
            // Use the resolved bead ID (which may be the short-form alias
            // that `br show` succeeded with) for the comment target.
            let comment_target = outcome
                .resolved_bead_id
                .as_deref()
                .unwrap_or(&outcome.mapping.mapped_to_bead_id);
            match br_mutation
                .comment_bead(comment_target, &comment_text)
                .await
            {
                Ok(_) => {
                    tracing::info!(
                        mapped_to_bead_id = outcome.mapping.mapped_to_bead_id.as_str(),
                        active_bead_id = bead_id,
                        "posted planned-elsewhere comment on mapped-to bead"
                    );
                    commented_count += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        mapped_to_bead_id = outcome.mapping.mapped_to_bead_id.as_str(),
                        error = %e,
                        "failed to post planned-elsewhere comment (non-blocking)"
                    );
                }
            }
        }
    }

    // Phase 4: Flush any br mutations (comments) so they're persisted upstream.
    // Best-effort: if flush fails, comments may be lost but the mapping is
    // already recorded as verified above.
    if commented_count > 0 {
        if let Some(details) = beads_mutation_health_warning(base_dir) {
            tracing::warn!(
                active_bead_id = bead_id,
                details = details.as_str(),
                "skipping br sync after planned-elsewhere comments because bead state is unsafe"
            );
        } else if let Err(e) = br_mutation.sync_flush().await {
            tracing::warn!(
                error = %e,
                "failed to flush br mutations after planned-elsewhere comments (non-blocking)"
            );
        }
    }

    let verified_count = outcomes.iter().filter(|o| o.verified).count();
    let summary = PlannedElsewhereVerificationSummary {
        mappings_verified: durably_verified_indices.len(),
        comments_posted: commented_count,
    };
    if !outcomes.is_empty() {
        tracing::info!(
            bead_id = bead_id,
            total = outcomes.len(),
            verified = verified_count,
            commented = commented_count,
            "planned-elsewhere post-run verification complete"
        );
    }
    summary
}

#[derive(Debug, Clone)]
struct TerminalReviewFinding {
    amendment_id: String,
    source_identities: Vec<String>,
    normalized_body: String,
    mapped_to_bead_id: Option<String>,
    covered_by_bead_id: Option<String>,
    classification: AmendmentClassification,
    rationale: Option<String>,
    proposed_title: Option<String>,
    proposed_scope: Option<String>,
    proposed_bead_summary: Option<String>,
    severity: Option<Severity>,
}

impl TerminalReviewFinding {
    fn from_final_review(amendment: &FinalReviewCanonicalAmendment) -> Self {
        let source_identities = if amendment.sources.is_empty() {
            vec!["final_review:unknown_source".to_owned()]
        } else {
            amendment
                .sources
                .iter()
                .map(|source| {
                    format!(
                        "final_review:{}:{}:{}",
                        source.reviewer_id, source.backend_family, source.model_id
                    )
                })
                .collect()
        };
        Self {
            amendment_id: amendment.amendment_id.clone(),
            source_identities,
            normalized_body: amendment.normalized_body.clone(),
            mapped_to_bead_id: amendment.mapped_to_bead_id.clone(),
            covered_by_bead_id: amendment.covered_by_bead_id.clone(),
            classification: amendment.classification,
            rationale: amendment.rationale.clone(),
            proposed_title: amendment.proposed_title.clone(),
            proposed_scope: amendment.proposed_scope.clone(),
            proposed_bead_summary: amendment.proposed_bead_summary.clone(),
            severity: amendment.severity,
        }
    }

    fn from_review_payload(payload_id: &str, index: usize, finding: &ClassifiedFinding) -> Self {
        Self {
            amendment_id: format!("review:{payload_id}:{index}"),
            source_identities: vec![format!("review_payload:{payload_id}")],
            normalized_body: finding.body.clone(),
            mapped_to_bead_id: finding.mapped_to_bead_id.clone(),
            covered_by_bead_id: finding.covered_by_bead_id.clone(),
            classification: finding.classification,
            rationale: None,
            proposed_title: None,
            proposed_scope: None,
            proposed_bead_summary: finding.proposed_bead_summary.clone(),
            severity: None,
        }
    }
}

fn distinct_proposal_source_count(findings: &[&TerminalReviewFinding]) -> usize {
    findings
        .iter()
        .flat_map(|finding| finding.source_identities.iter().map(String::as_str))
        .collect::<HashSet<_>>()
        .len()
}

#[derive(Debug, Default)]
struct TerminalReviewFindings {
    findings: Vec<TerminalReviewFinding>,
    final_review_rounds: usize,
    review_payloads: usize,
}

fn terminal_review_findings_for_run(
    base_dir: &Path,
    project_id: &str,
    bead_id: &str,
    task_id: &str,
    run_id: &str,
) -> Result<TerminalReviewFindings, ReconciliationError> {
    let milestone_error = |details: String| ReconciliationError::MilestoneUpdateFailed {
        bead_id: bead_id.to_owned(),
        task_id: task_id.to_owned(),
        details,
    };
    let pid = match ProjectId::new(project_id) {
        Ok(pid) => pid,
        Err(_) => return Ok(TerminalReviewFindings::default()),
    };
    let payloads = FsArtifactStore
        .list_payloads(base_dir, &pid)
        .map_err(|error| {
            milestone_error(format!(
                "failed to list payloads for review-classification reconciliation: {error}"
            ))
        })?;

    let mut output = TerminalReviewFindings::default();
    let mut latest_final_review_by_round = HashMap::new();
    for payload in &payloads {
        if !payload.payload_id.starts_with(&format!("{run_id}-")) {
            continue;
        }

        match (payload.stage_id, payload.record_kind) {
            (StageId::Review, RecordKind::StagePrimary) => {
                let stage_payload: StagePayload =
                    serde_json::from_value(payload.payload.clone()).map_err(|error| {
                        milestone_error(format!(
                            "failed to parse review payload for review-classification reconciliation from {}: {error}",
                            payload.payload_id
                        ))
                    })?;
                let StagePayload::Validation(validation) = stage_payload else {
                    continue;
                };
                output
                    .findings
                    .extend(validation.classified_findings.iter().enumerate().map(
                        |(index, finding)| {
                            TerminalReviewFinding::from_review_payload(
                                &payload.payload_id,
                                index,
                                finding,
                            )
                        },
                    ));
                output.review_payloads += 1;
            }
            (StageId::FinalReview, RecordKind::StageAggregate) => {
                let entry = latest_final_review_by_round
                    .entry(payload.completion_round)
                    .or_insert(payload);
                if payload.created_at > entry.created_at {
                    *entry = payload;
                }
            }
            _ => {}
        }
    }

    for (completion_round, payload) in latest_final_review_by_round {
        let aggregate: FinalReviewAggregatePayload =
            serde_json::from_value(payload.payload.clone()).map_err(|error| {
                milestone_error(format!(
                    "failed to parse final-review aggregate for review-classification reconciliation at completion round {completion_round}: {error}"
                ))
            })?;
        output.findings.extend(
            aggregate
                .final_accepted_amendments
                .iter()
                .map(TerminalReviewFinding::from_final_review),
        );
        output.final_review_rounds += 1;
    }

    Ok(output)
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ProposedBeadRecord {
    amendment_id: String,
    source_run_id: String,
    current_bead_id: String,
    summary: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    proposed_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    proposed_scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    severity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    rationale: Option<String>,
    count: usize,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
struct TerminalParsimoniousBeadCreationPolicy {
    enabled: bool,
    existing_bead_match_threshold_score: f64,
    proposal_threshold: usize,
}

fn terminal_parsimonious_bead_creation_policy(
    base_dir: &Path,
    project_id: &str,
) -> TerminalParsimoniousBeadCreationPolicy {
    let project_id = ProjectId::new(project_id).ok();
    EffectiveConfig::load_for_project(
        base_dir,
        project_id.as_ref(),
        CliBackendOverrides::default(),
    )
    .map(|config| {
        let policy = config.run_policy().parsimonious_bead_creation.clone();
        TerminalParsimoniousBeadCreationPolicy {
            enabled: policy.enabled,
            existing_bead_match_threshold_score: policy.existing_bead_match_threshold_score,
            proposal_threshold: policy.proposal_threshold.max(1) as usize,
        }
    })
    .unwrap_or(TerminalParsimoniousBeadCreationPolicy {
        enabled: DEFAULT_PARSIMONIOUS_BEAD_CREATION_ENABLED,
        existing_bead_match_threshold_score: DEFAULT_EXISTING_BEAD_MATCH_THRESHOLD_SCORE,
        proposal_threshold: DEFAULT_NEW_BEAD_PROPOSAL_THRESHOLD.max(1) as usize,
    })
}

fn proposal_record_path(base_dir: &Path, project_id: &str) -> Option<std::path::PathBuf> {
    ProjectId::new(project_id)
        .ok()
        .map(|pid| FileSystem::project_root(base_dir, &pid).join("proposed-beads.ndjson"))
}

#[derive(Debug, Default)]
struct ExistingProposalRecords {
    amendment_ids: HashSet<String>,
}

fn existing_proposal_records(path: &Path) -> Result<ExistingProposalRecords, std::io::Error> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ExistingProposalRecords::default());
        }
        Err(error) => return Err(error),
    };

    let reader = std::io::BufReader::new(file);
    let mut records = ExistingProposalRecords::default();
    for line in reader.lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Ok(record) = serde_json::from_str::<ProposedBeadRecord>(trimmed) {
            records.amendment_ids.insert(record.amendment_id);
        }
    }
    Ok(records)
}

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
enum TerminalBrListSummariesResponse {
    Envelope { issues: Vec<BeadSummary> },
    Many(Vec<BeadSummary>),
}

impl TerminalBrListSummariesResponse {
    fn into_issues(self) -> Vec<BeadSummary> {
        match self {
            Self::Envelope { issues } => issues,
            Self::Many(issues) => issues,
        }
    }
}

#[derive(Debug)]
struct TerminalProposeNewBeadInput {
    active_bead_id: String,
    finding_summary: String,
    proposed_title: String,
    proposed_scope: String,
    severity: String,
    rationale: Option<String>,
    run_id: String,
}

impl TerminalProposeNewBeadInput {
    fn from_group(bead_id: &str, run_id: &str, findings: &[&TerminalReviewFinding]) -> Self {
        let proposed_scope = display_proposal_scope(findings);
        Self {
            active_bead_id: bead_id.to_owned(),
            finding_summary: proposed_scope.clone(),
            proposed_title: display_proposal_summary(findings),
            proposed_scope,
            severity: display_proposal_severity(findings),
            rationale: display_proposal_rationale(findings),
            run_id: run_id.to_owned(),
        }
    }
}

fn nonblank_text(value: Option<&String>) -> Option<&str> {
    value
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn proposal_title_for_finding(finding: &TerminalReviewFinding) -> Option<&str> {
    nonblank_text(finding.proposed_title.as_ref())
        .or_else(|| nonblank_text(finding.proposed_bead_summary.as_ref()))
}

fn normalized_proposal_summary(finding: &TerminalReviewFinding) -> Option<String> {
    proposal_title_for_finding(finding).map(str::to_ascii_lowercase)
}

fn display_proposal_summary(findings: &[&TerminalReviewFinding]) -> String {
    findings
        .iter()
        .find_map(|finding| proposal_title_for_finding(finding))
        .unwrap_or_default()
        .to_owned()
}

fn display_proposal_scope(findings: &[&TerminalReviewFinding]) -> String {
    let proposed_scopes = findings
        .iter()
        .filter_map(|finding| nonblank_text(finding.proposed_scope.as_ref()))
        .collect::<Vec<_>>();
    if !proposed_scopes.is_empty() {
        return proposed_scopes.join("\n");
    }

    findings
        .iter()
        .map(|finding| finding.normalized_body.trim())
        .filter(|summary| !summary.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

fn display_proposal_severity(findings: &[&TerminalReviewFinding]) -> String {
    findings
        .iter()
        .find_map(|finding| finding.severity.map(|severity| severity.as_str()))
        .unwrap_or("unknown")
        .to_owned()
}

fn display_proposal_rationale(findings: &[&TerminalReviewFinding]) -> Option<String> {
    findings
        .iter()
        .find_map(|finding| nonblank_text(finding.rationale.as_ref()))
        .map(str::to_owned)
}

fn comment_summary_line(summary: &str) -> String {
    summary.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn amendment_marker_line(amendment_id: &str) -> String {
    format!("amendment_id={amendment_id}")
}

fn has_exact_amendment_marker(comment_text: &str, amendment_id: &str) -> bool {
    let marker = amendment_marker_line(amendment_id);
    comment_text.lines().any(|line| line.trim() == marker)
}

async fn list_all_bead_summaries<Q: ProcessRunner>(
    br_read: &BrAdapter<Q>,
) -> Result<Vec<BeadSummary>, BrError> {
    Ok(br_read
        .exec_json::<TerminalBrListSummariesResponse>(&BrCommand::list_all())
        .await?
        .into_issues())
}

fn proposed_work_match_score(detail: &BeadDetail, input: &TerminalProposeNewBeadInput) -> f64 {
    let proposed_text = format!("{} {}", input.proposed_title, input.proposed_scope);
    let candidate_text = format!(
        "{} {}",
        detail.title,
        detail.description.as_deref().unwrap_or_default()
    );
    bead_ownership_text_similarity(&proposed_text, &candidate_text)
}

async fn load_active_bead_match<Q: ProcessRunner>(
    br_read: &BrAdapter<Q>,
    summaries: &[BeadSummary],
    input: &TerminalProposeNewBeadInput,
    threshold_score: f64,
) -> Result<Option<(BeadDetail, f64)>, BrError> {
    let Some(summary) = summaries
        .iter()
        .find(|summary| summary.id == input.active_bead_id)
    else {
        return Ok(None);
    };

    let detail = br_read
        .exec_json::<BeadDetail>(&BrCommand::show(summary.id.clone()))
        .await?;
    let score = proposed_work_match_score(&detail, input);
    Ok((score >= threshold_score).then_some((detail, score)))
}

async fn find_existing_open_bead_match<Q: ProcessRunner>(
    br_read: &BrAdapter<Q>,
    summaries: &[BeadSummary],
    input: &TerminalProposeNewBeadInput,
    threshold_score: f64,
) -> Result<Option<(BeadDetail, f64)>, BrError> {
    let mut best: Option<(BeadDetail, f64)> = None;

    for summary in summaries.iter().filter(|summary| {
        summary.id != input.active_bead_id
            && matches!(summary.status, BeadStatus::Open | BeadStatus::InProgress)
    }) {
        let detail = br_read
            .exec_json::<BeadDetail>(&BrCommand::show(summary.id.clone()))
            .await?;
        let score = proposed_work_match_score(&detail, input);
        if score >= threshold_score
            && best
                .as_ref()
                .map(|(_, best_score)| score > *best_score)
                .unwrap_or(true)
        {
            best = Some((detail, score));
        }
    }

    Ok(best)
}

async fn comment_propose_new_bead_covered_by_existing<M: ProcessRunner>(
    br_mutation: &BrMutationAdapter<M>,
    base_dir: &Path,
    bead_id: &str,
    task_id: &str,
    input: &TerminalProposeNewBeadInput,
    target: &BeadDetail,
    match_score: f64,
) -> Result<(), ReconciliationError> {
    let finding_line = format!("Finding: {}", comment_summary_line(&input.finding_summary));
    let proposed_scope_line = format!(
        "Proposed scope: {}",
        comment_summary_line(&input.proposed_scope)
    );
    if target.comments.iter().any(|comment| {
        comment
            .text
            .contains("classification=covered_by_existing_bead")
            && comment
                .text
                .contains(&format!("active_bead_id={}", input.active_bead_id))
            && comment.text.contains(&format!("run_id={}", input.run_id))
            && comment.text.lines().any(|line| line.trim() == finding_line)
            && comment
                .text
                .lines()
                .any(|line| line.trim() == proposed_scope_line)
    }) {
        return Ok(());
    }

    ensure_beads_mutation_health(base_dir, bead_id, task_id)?;
    let comment = format!(
        "Review finding from bead {}\nclassification=covered_by_existing_bead\nactive_bead_id={}\nrun_id={}\nmatch_score={:.3}\nFinding: {}\nProposed scope: {}",
        input.active_bead_id,
        input.active_bead_id,
        input.run_id,
        match_score,
        comment_summary_line(&input.finding_summary),
        comment_summary_line(&input.proposed_scope)
    );
    br_mutation
        .comment_bead(&target.id, &comment)
        .await
        .map_err(|error| {
            ReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!(
                    "failed to post propose_new_bead covered_by_existing_bead comment for target '{}': {error}",
                    target.id
                ),
            }
        })?;
    match br_mutation.sync_own_dirty_if_beads_healthy(base_dir).await {
        Ok(crate::adapters::br_process::SyncIfDirtyOutcome::Clean)
        | Ok(crate::adapters::br_process::SyncIfDirtyOutcome::Flushed { .. }) => Ok(()),
        Err(SyncIfDirtyHealthError::UnsafeBeadsState { details }) => {
            Err(ReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!("failed to flush propose_new_bead covered_by_existing_bead comment because bead state became unsafe before br sync --flush-only: {details}"),
            })
        }
        Err(SyncIfDirtyHealthError::Br(error)) => {
            Err(ReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!(
                    "failed to flush propose_new_bead covered_by_existing_bead comment after posting comment: {error}"
                ),
            })
        }
    }
}

fn pending_event_metadata_matches(
    event_metadata: &serde_json::Map<String, serde_json::Value>,
    expected_metadata: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    [
        "sub_type",
        "active_bead_id",
        "run_id",
        "proposed_title",
        "proposed_scope",
        "current_count",
        "threshold_count",
        "amendment_ids",
    ]
    .into_iter()
    .all(|key| event_metadata.get(key) == expected_metadata.get(key))
}

fn propose_new_bead_pending_event_exists(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    metadata: &serde_json::Map<String, serde_json::Value>,
    details: &str,
) -> Result<bool, ReconciliationError> {
    let milestone_error = |details: String| ReconciliationError::MilestoneUpdateFailed {
        bead_id: metadata
            .get("active_bead_id")
            .and_then(|value| value.as_str())
            .unwrap_or("<unknown>")
            .to_owned(),
        task_id: milestone_id.to_string(),
        details,
    };
    let journal = FsMilestoneJournalStore
        .read_journal(base_dir, milestone_id)
        .map_err(|error| {
            milestone_error(format!(
                "failed to read journal before deduping propose_new_bead_pending event: {error}"
            ))
        })?;
    Ok(journal.iter().any(|event| {
        event.details.as_deref() == Some(details)
            && event.metadata.as_ref().is_some_and(|event_metadata| {
                pending_event_metadata_matches(event_metadata, metadata)
            })
    }))
}

#[allow(clippy::too_many_arguments)]
fn record_propose_new_bead_pending_event(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    run_id: &str,
    findings: &[&TerminalReviewFinding],
    current_count: usize,
    threshold: usize,
    now: DateTime<Utc>,
) -> Result<(), ReconciliationError> {
    let milestone_error = |details: String| ReconciliationError::MilestoneUpdateFailed {
        bead_id: bead_id.to_owned(),
        task_id: milestone_id.to_string(),
        details,
    };
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "sub_type".to_owned(),
        serde_json::Value::String("propose_new_bead_pending".to_owned()),
    );
    metadata.insert(
        "active_bead_id".to_owned(),
        serde_json::Value::String(bead_id.to_owned()),
    );
    metadata.insert(
        "proposed_title".to_owned(),
        serde_json::Value::String(display_proposal_summary(findings)),
    );
    metadata.insert(
        "proposed_scope".to_owned(),
        serde_json::Value::String(display_proposal_scope(findings)),
    );
    metadata.insert(
        "severity".to_owned(),
        serde_json::Value::String(display_proposal_severity(findings)),
    );
    metadata.insert(
        "current_count".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(current_count as u64)),
    );
    metadata.insert(
        "threshold_count".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(threshold as u64)),
    );
    metadata.insert(
        "existing_bead_lookup_ran".to_owned(),
        serde_json::Value::Bool(false),
    );
    metadata.insert(
        "run_id".to_owned(),
        serde_json::Value::String(run_id.to_owned()),
    );
    metadata.insert(
        "amendment_ids".to_owned(),
        serde_json::Value::Array(
            findings
                .iter()
                .map(|finding| serde_json::Value::String(finding.amendment_id.clone()))
                .collect(),
        ),
    );
    let details = display_proposal_summary(findings);
    if propose_new_bead_pending_event_exists(base_dir, milestone_id, &metadata, &details)? {
        return Ok(());
    }

    let mut event = MilestoneJournalEvent::new(MilestoneEventType::ProgressUpdated, now)
        .with_bead(bead_id.to_owned())
        .with_details(details);
    event.metadata = Some(metadata);
    let line = event.to_ndjson_line().map_err(|error| {
        milestone_error(format!(
            "failed to serialize propose_new_bead_pending event: {error}"
        ))
    })?;
    FsMilestoneJournalStore
        .append_event(base_dir, milestone_id, &line)
        .map_err(|error| {
            milestone_error(format!(
                "failed to append propose_new_bead_pending event: {error}"
            ))
        })
}

fn created_event_metadata_matches(
    event_metadata: &serde_json::Map<String, serde_json::Value>,
    expected_metadata: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    [
        "sub_type",
        "active_bead_id",
        "run_id",
        "proposed_title",
        "proposed_scope",
        "severity",
        "current_count",
        "threshold_count",
        "existing_bead_lookup_ran",
        "amendment_ids",
    ]
    .into_iter()
    .all(|key| event_metadata.get(key) == expected_metadata.get(key))
}

fn propose_new_bead_created_event_exists(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    metadata: &serde_json::Map<String, serde_json::Value>,
    details: &str,
) -> Result<bool, ReconciliationError> {
    let milestone_error = |details: String| ReconciliationError::MilestoneUpdateFailed {
        bead_id: metadata
            .get("active_bead_id")
            .and_then(|value| value.as_str())
            .unwrap_or("<unknown>")
            .to_owned(),
        task_id: milestone_id.to_string(),
        details,
    };
    let journal = FsMilestoneJournalStore
        .read_journal(base_dir, milestone_id)
        .map_err(|error| {
            milestone_error(format!(
                "failed to read journal before deduping propose_new_bead_created event: {error}"
            ))
        })?;
    Ok(journal.iter().any(|event| {
        event.details.as_deref() == Some(details)
            && event.metadata.as_ref().is_some_and(|event_metadata| {
                created_event_metadata_matches(event_metadata, metadata)
            })
    }))
}

#[allow(clippy::too_many_arguments)]
fn record_propose_new_bead_created_event(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    input: &TerminalProposeNewBeadInput,
    amendment_ids: &[String],
    current_count: usize,
    threshold: usize,
    existing_bead_lookup_ran: bool,
    now: DateTime<Utc>,
) -> Result<(), ReconciliationError> {
    let milestone_error = |details: String| ReconciliationError::MilestoneUpdateFailed {
        bead_id: input.active_bead_id.clone(),
        task_id: milestone_id.to_string(),
        details,
    };
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "sub_type".to_owned(),
        serde_json::Value::String("propose_new_bead_created".to_owned()),
    );
    metadata.insert(
        "active_bead_id".to_owned(),
        serde_json::Value::String(input.active_bead_id.clone()),
    );
    metadata.insert(
        "proposed_title".to_owned(),
        serde_json::Value::String(input.proposed_title.clone()),
    );
    metadata.insert(
        "proposed_scope".to_owned(),
        serde_json::Value::String(input.proposed_scope.clone()),
    );
    metadata.insert(
        "severity".to_owned(),
        serde_json::Value::String(input.severity.clone()),
    );
    if let Some(rationale) = &input.rationale {
        metadata.insert(
            "rationale".to_owned(),
            serde_json::Value::String(rationale.clone()),
        );
    }
    metadata.insert(
        "current_count".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(current_count as u64)),
    );
    metadata.insert(
        "threshold_count".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(threshold as u64)),
    );
    metadata.insert(
        "existing_bead_lookup_ran".to_owned(),
        serde_json::Value::Bool(existing_bead_lookup_ran),
    );
    metadata.insert(
        "run_id".to_owned(),
        serde_json::Value::String(input.run_id.clone()),
    );
    metadata.insert(
        "amendment_ids".to_owned(),
        serde_json::Value::Array(
            amendment_ids
                .iter()
                .cloned()
                .map(serde_json::Value::String)
                .collect(),
        ),
    );
    let details = input.finding_summary.clone();
    if propose_new_bead_created_event_exists(base_dir, milestone_id, &metadata, &details)? {
        return Ok(());
    }

    let mut event = MilestoneJournalEvent::new(MilestoneEventType::ProposedBeadCreated, now)
        .with_bead(input.active_bead_id.clone())
        .with_details(details);
    event.metadata = Some(metadata);
    let line = event.to_ndjson_line().map_err(|error| {
        milestone_error(format!(
            "failed to serialize propose_new_bead_created event: {error}"
        ))
    })?;
    FsMilestoneJournalStore
        .append_event(base_dir, milestone_id, &line)
        .map_err(|error| {
            milestone_error(format!(
                "failed to append propose_new_bead_created event: {error}"
            ))
        })
}

fn is_missing_bead_error(error: &BrError) -> bool {
    match error {
        BrError::BrExitError { stdout, stderr, .. } => {
            br_show_output_indicates_missing(stderr, stdout)
        }
        _ => false,
    }
}

fn bead_reference_matches(left: &str, right: &str) -> bool {
    let left = left.trim();
    let right = right.trim();
    if left == right {
        return true;
    }
    canonicalized_bead_reference_matches(left, right)
        || canonicalized_bead_reference_matches(right, left)
}

fn canonicalized_bead_reference_matches(canonical_candidate: &str, other: &str) -> bool {
    let Some(prefix) = milestone_prefix_of(canonical_candidate) else {
        return false;
    };
    canonicalize_bead_reference(prefix, other)
        .is_ok_and(|canonical| canonical == canonical_candidate)
}

async fn reconcile_covered_by_existing_beads<M: ProcessRunner, Q: ProcessRunner>(
    br_mutation: &BrMutationAdapter<M>,
    br_read: &BrAdapter<Q>,
    base_dir: &Path,
    bead_id: &str,
    task_id: &str,
    run_id: &str,
    findings: &[TerminalReviewFinding],
) -> Result<(), ReconciliationError> {
    use crate::adapters::br_models::BeadDetail;

    let mutation_error = |details: String| ReconciliationError::MilestoneUpdateFailed {
        bead_id: bead_id.to_owned(),
        task_id: task_id.to_owned(),
        details,
    };
    let mut comments_posted = 0usize;
    for finding in findings {
        if finding.classification != AmendmentClassification::CoveredByExistingBead {
            continue;
        }
        let target = finding
            .covered_by_bead_id
            .as_deref()
            .or(finding.mapped_to_bead_id.as_deref())
            .map(str::trim)
            .filter(|target| !target.is_empty());
        let Some(target) = target else {
            tracing::warn!(
                amendment_id = finding.amendment_id.as_str(),
                "covered_by_existing_bead amendment missing target bead during reconciliation"
            );
            continue;
        };
        if bead_reference_matches(target, bead_id) {
            tracing::warn!(
                amendment_id = finding.amendment_id.as_str(),
                covered_by_bead_id = target,
                bead_id = bead_id,
                task_id = task_id,
                "covered_by_existing_bead target is the active bead; skipping reconciliation comment to avoid mutating the current bead"
            );
            continue;
        }

        let detail = match br_read
            .exec_json::<BeadDetail>(&BrCommand::show(target))
            .await
        {
            Ok(detail) => detail,
            Err(error) if is_missing_bead_error(&error) => {
                tracing::warn!(
                    amendment_id = finding.amendment_id.as_str(),
                    covered_by_bead_id = target,
                    error = %error,
                    "covered_by_existing_bead target bead not found; skipping reconciliation comment"
                );
                continue;
            }
            Err(error) => {
                return Err(mutation_error(format!(
                    "failed to inspect covered_by_existing_bead target '{target}' before posting reconciliation comment: {error}"
                )));
            }
        };
        if bead_reference_matches(&detail.id, bead_id) {
            tracing::warn!(
                amendment_id = finding.amendment_id.as_str(),
                covered_by_bead_id = target,
                resolved_bead_id = detail.id.as_str(),
                bead_id = bead_id,
                task_id = task_id,
                "covered_by_existing_bead target resolved to the active bead; skipping reconciliation comment to avoid mutating the current bead"
            );
            continue;
        }
        if detail
            .comments
            .iter()
            .any(|comment| has_exact_amendment_marker(&comment.text, &finding.amendment_id))
        {
            continue;
        }

        ensure_beads_mutation_health(base_dir, bead_id, task_id)?;
        let comment = format!(
            "Review finding from bead {bead_id}; source run_id={run_id}\namendment_id={}\nFinding: {}",
            finding.amendment_id,
            comment_summary_line(&finding.normalized_body)
        );
        match br_mutation.comment_bead(&detail.id, &comment).await {
            Ok(_) => {
                comments_posted += 1;
            }
            Err(error) => {
                return Err(mutation_error(format!(
                    "failed to post covered_by_existing_bead reconciliation comment for target '{}', amendment_id={}: {error}",
                    detail.id, finding.amendment_id
                )));
            }
        }
    }

    let own_pending_comment_mutation = br_mutation.has_own_pending_comment_mutation();
    if comments_posted > 0 || own_pending_comment_mutation {
        match br_mutation.sync_own_dirty_if_beads_healthy(base_dir).await {
            Ok(crate::adapters::br_process::SyncIfDirtyOutcome::Clean) => {
                tracing::debug!(
                    bead_id = bead_id,
                    task_id = task_id,
                    comments_posted = comments_posted,
                    own_pending_comment_mutation = own_pending_comment_mutation,
                    "no pending local covered_by_existing_bead comment mutations remain; skipping br sync --flush-only"
                );
            }
            Ok(crate::adapters::br_process::SyncIfDirtyOutcome::Flushed { .. }) => {}
            Err(SyncIfDirtyHealthError::UnsafeBeadsState { details }) => {
                return Err(mutation_error(format!(
                    "failed to flush covered_by_existing_bead reconciliation comments because bead state became unsafe before br sync --flush-only: {details}"
                )));
            }
            Err(SyncIfDirtyHealthError::Br(error)) => {
                return Err(mutation_error(format!(
                    "failed to flush covered_by_existing_bead reconciliation comments after posting {comments_posted} comment(s): {error}"
                )));
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_proposed_bead_records<M: ProcessRunner, Q: ProcessRunner>(
    br_mutation: &BrMutationAdapter<M>,
    br_read: &BrAdapter<Q>,
    base_dir: &Path,
    bead_id: &str,
    task_id: &str,
    milestone_id: &MilestoneId,
    project_id: &str,
    run_id: &str,
    findings: &[TerminalReviewFinding],
    now: DateTime<Utc>,
) -> Result<ProposedBeadReconciliationSummary, ReconciliationError> {
    let milestone_error = |details: String| ReconciliationError::MilestoneUpdateFailed {
        bead_id: bead_id.to_owned(),
        task_id: task_id.to_owned(),
        details,
    };
    let Some(path) = proposal_record_path(base_dir, project_id) else {
        return Ok(ProposedBeadReconciliationSummary::default());
    };

    let policy = terminal_parsimonious_bead_creation_policy(base_dir, project_id);
    let threshold = policy.proposal_threshold;
    let mut groups: HashMap<String, Vec<&TerminalReviewFinding>> = HashMap::new();
    let mut amendments_processed = 0usize;
    for finding in findings {
        if finding.classification == AmendmentClassification::ProposeNewBead {
            amendments_processed += 1;
            if let Some(summary_key) = normalized_proposal_summary(finding) {
                groups.entry(summary_key).or_default().push(finding);
            }
        }
    }

    let mut threshold_met_groups = Vec::new();
    for (summary_key, findings) in &groups {
        let source_count = distinct_proposal_source_count(findings);
        if source_count >= threshold {
            threshold_met_groups.push((summary_key, findings));
        } else {
            record_propose_new_bead_pending_event(
                base_dir,
                milestone_id,
                bead_id,
                run_id,
                findings,
                source_count,
                threshold,
                now,
            )?;
            tracing::info!(
                bead_id = bead_id,
                run_id = run_id,
                proposed_bead_summary = summary_key.as_str(),
                current_count = source_count,
                threshold = threshold,
                "terminal propose-new-bead amendment pending threshold"
            );
        }
    }
    if threshold_met_groups.is_empty() {
        return Ok(ProposedBeadReconciliationSummary {
            amendments_processed,
            records_written: 0,
        });
    }

    let threshold_met_groups: Vec<_> = threshold_met_groups
        .iter()
        .map(|(summary, findings)| (*summary, *findings))
        .collect();

    let mut threshold_met_record_groups = Vec::new();
    let mut all_beads = None;
    for (summary_key, findings) in threshold_met_groups {
        let input = TerminalProposeNewBeadInput::from_group(bead_id, run_id, findings);
        let mut existing_bead_lookup_ran = false;
        if policy.enabled {
            let summaries = match &all_beads {
                Some(summaries) => summaries,
                None => {
                    all_beads = Some(list_all_bead_summaries(br_read).await.map_err(|error| {
                        milestone_error(format!(
                            "failed to query existing beads before terminal propose_new_bead record routing: {error}"
                        ))
                    })?);
                    all_beads.as_ref().expect("summaries just initialized")
                }
            };
            existing_bead_lookup_ran = true;

            if let Some((matched, score)) = load_active_bead_match(
                br_read,
                summaries,
                &input,
                policy.existing_bead_match_threshold_score,
            )
            .await
            .map_err(|error| {
                milestone_error(format!(
                    "failed to inspect active bead ownership before terminal propose_new_bead record routing: {error}"
                ))
            })? {
                tracing::info!(
                    bead_id = bead_id,
                    run_id = run_id,
                    proposed_bead_summary = summary_key.as_str(),
                    active_bead_id = matched.id.as_str(),
                    match_score = score,
                    "terminal propose-new-bead amendment is owned by the active bead; skipping proposed-bead record"
                );
                continue;
            }

            if let Some((matched, score)) = find_existing_open_bead_match(
                br_read,
                summaries,
                &input,
                policy.existing_bead_match_threshold_score,
            )
            .await
            .map_err(|error| {
                milestone_error(format!(
                    "failed to lookup existing bead ownership before terminal propose_new_bead record routing: {error}"
                ))
            })? {
                comment_propose_new_bead_covered_by_existing(
                    br_mutation,
                    base_dir,
                    bead_id,
                    task_id,
                    &input,
                    &matched,
                    score,
                )
                .await?;
                tracing::info!(
                    bead_id = bead_id,
                    run_id = run_id,
                    proposed_bead_summary = summary_key.as_str(),
                    covered_by_bead_id = matched.id.as_str(),
                    match_score = score,
                    "terminal propose-new-bead amendment covered by existing bead; skipping proposed-bead record"
                );
                continue;
            }
        }

        threshold_met_record_groups.push((input, findings.clone(), existing_bead_lookup_ran));
    }

    if threshold_met_record_groups.is_empty() {
        return Ok(ProposedBeadReconciliationSummary {
            amendments_processed,
            records_written: 0,
        });
    }

    let existing = existing_proposal_records(&path).map_err(|error| {
        milestone_error(format!(
            "failed to read proposed-beads.ndjson for reconciliation: {error}"
        ))
    })?;

    let mut records = Vec::new();
    let mut created_events = Vec::new();
    for (input, findings, existing_bead_lookup_ran) in threshold_met_record_groups {
        let source_count = distinct_proposal_source_count(&findings);
        let amendment_ids = findings
            .iter()
            .map(|finding| finding.amendment_id.clone())
            .collect::<Vec<_>>();
        let amendment_id = findings
            .iter()
            .map(|finding| finding.amendment_id.as_str())
            .min()
            .unwrap_or_default()
            .to_owned();
        let record = ProposedBeadRecord {
            amendment_id: amendment_id.clone(),
            source_run_id: run_id.to_owned(),
            current_bead_id: bead_id.to_owned(),
            summary: input.proposed_title.clone(),
            proposed_title: Some(input.proposed_title.clone()),
            proposed_scope: Some(input.proposed_scope.clone()),
            severity: Some(input.severity.clone()),
            rationale: input.rationale.clone(),
            count: source_count,
            timestamp: now,
        };
        created_events.push((input, amendment_ids, source_count, existing_bead_lookup_ran));
        if !existing.amendment_ids.contains(&amendment_id) {
            records.push(record);
        }
    }

    if records.is_empty() && created_events.is_empty() {
        return Ok(ProposedBeadReconciliationSummary {
            amendments_processed,
            records_written: 0,
        });
    }

    if !records.is_empty() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                milestone_error(format!(
                    "failed to create proposed-beads.ndjson parent directory: {error}"
                ))
            })?;
        }
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| {
                milestone_error(format!(
                    "failed to open proposed-beads.ndjson for append: {error}"
                ))
            })?;
        for record in &records {
            serde_json::to_writer(&mut file, record).map_err(|error| {
                milestone_error(format!("failed to serialize proposed bead record: {error}"))
            })?;
            file.write_all(b"\n").map_err(|error| {
                milestone_error(format!("failed to append proposed bead record: {error}"))
            })?;
        }
    }
    for (input, amendment_ids, source_count, existing_bead_lookup_ran) in &created_events {
        record_propose_new_bead_created_event(
            base_dir,
            milestone_id,
            input,
            amendment_ids,
            *source_count,
            threshold,
            *existing_bead_lookup_ran,
            now,
        )?;
    }

    Ok(ProposedBeadReconciliationSummary {
        amendments_processed,
        records_written: records.len(),
    })
}

#[cfg(test)]
pub(crate) async fn reconcile_terminal_review_classifications<
    M: ProcessRunner,
    Q: ProcessRunner,
>(
    br_mutation: &BrMutationAdapter<M>,
    br_read: &BrAdapter<Q>,
    base_dir: &Path,
    bead_id: &str,
    task_id: &str,
    project_id: &str,
    run_id: &str,
) -> Result<ProposedBeadReconciliationSummary, ReconciliationError> {
    reconcile_terminal_review_classifications_for_milestone(
        br_mutation,
        br_read,
        base_dir,
        bead_id,
        task_id,
        task_id,
        project_id,
        run_id,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn reconcile_terminal_review_classifications_for_milestone<
    M: ProcessRunner,
    Q: ProcessRunner,
>(
    br_mutation: &BrMutationAdapter<M>,
    br_read: &BrAdapter<Q>,
    base_dir: &Path,
    bead_id: &str,
    task_id: &str,
    milestone_id_str: &str,
    project_id: &str,
    run_id: &str,
) -> Result<ProposedBeadReconciliationSummary, ReconciliationError> {
    let milestone_id = MilestoneId::new(milestone_id_str).map_err(|error| {
        ReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: format!("invalid milestone id for terminal review reconciliation: {error}"),
        }
    })?;
    let terminal =
        terminal_review_findings_for_run(base_dir, project_id, bead_id, task_id, run_id)?;
    if terminal.findings.is_empty() {
        return Ok(ProposedBeadReconciliationSummary::default());
    }

    reconcile_covered_by_existing_beads(
        br_mutation,
        br_read,
        base_dir,
        bead_id,
        task_id,
        run_id,
        &terminal.findings,
    )
    .await?;
    let summary = reconcile_proposed_bead_records(
        br_mutation,
        br_read,
        base_dir,
        bead_id,
        task_id,
        &milestone_id,
        project_id,
        run_id,
        &terminal.findings,
        Utc::now(),
    )
    .await?;
    tracing::info!(
        bead_id = bead_id,
        run_id = run_id,
        review_payloads = terminal.review_payloads,
        final_review_rounds = terminal.final_review_rounds,
        propose_new_bead_amendments = summary.amendments_processed,
        proposed_bead_records_written = summary.records_written,
        "terminal review-classification reconciliation complete"
    );
    Ok(summary)
}

/// Reconstruct any planned-elsewhere mappings from persisted final-review
/// aggregate payloads that are missing from the journal. Only considers
/// aggregates from the current run (payload_id starts with run_id) and,
/// for each completion_round, only uses the latest aggregate (by
/// `created_at`) to skip pre-rollback or abandoned review attempts.
///
/// This covers the failure window where the durable stage commit (which
/// includes the aggregate) succeeded but `record_planned_elsewhere_amendments`
/// in engine.rs failed or was interrupted — including for earlier restart
/// rounds within the same run when the bead already has mappings from other
/// rounds.
/// Returns `(reconstructed_mappings, authoritative_max_round)`.
/// `authoritative_max_round` is the highest `completion_round` among all
/// final-review aggregates for this run — the source of truth for which
/// round is latest, even if that round contains no PE amendments.
fn reconstruct_missing_pe_mappings(
    base_dir: &Path,
    project_id: &str,
    bead_id: &str,
    milestone_id: &MilestoneId,
    existing_mappings: &[PlannedElsewhereMapping],
    run_id: &str,
) -> (Vec<PlannedElsewhereMapping>, Option<u32>) {
    let pid = match ProjectId::new(project_id) {
        Ok(pid) => pid,
        Err(_) => return (Vec::new(), None),
    };
    let payloads = match FsArtifactStore.list_payloads(base_dir, &pid) {
        Ok(p) => p,
        Err(_) => return (Vec::new(), None),
    };

    // Collect final-review aggregates from the current run. For each
    // completion_round, keep only the latest aggregate (by created_at)
    // so pre-rollback or abandoned attempts are skipped.
    let mut latest_by_round: std::collections::HashMap<u32, _> = std::collections::HashMap::new();
    for payload in &payloads {
        if payload.stage_id != StageId::FinalReview
            || payload.record_kind != RecordKind::StageAggregate
            || !payload.payload_id.starts_with(&format!("{run_id}-"))
        {
            continue;
        }
        let entry = latest_by_round
            .entry(payload.completion_round)
            .or_insert(payload);
        if payload.created_at > entry.created_at {
            *entry = payload;
        }
    }

    // Build a mutable set of existing identity keys for dedup. Updated as
    // new mappings are reconstructed so the same identity appearing in
    // multiple aggregates is only reconstructed once. Includes
    // completion_round so that later rounds can reconstruct the same
    // finding/target pair independently (e.g. round N succeeded in journal
    // but round N+1's journal write failed).
    let mut seen_keys: std::collections::HashSet<(String, String, String, Option<u32>)> =
        existing_mappings
            .iter()
            .map(|m| {
                (
                    m.active_bead_id.clone(),
                    m.finding_summary.clone(),
                    m.mapped_to_bead_id.clone(),
                    m.completion_round,
                )
            })
            .collect();

    // The authoritative max round is the highest completion_round among all
    // final-review aggregates for this run — even if that round's aggregate
    // contains zero PE amendments (meaning findings were fixed/rejected).
    let authoritative_max_round = latest_by_round.keys().copied().max();

    let now = Utc::now();
    let mut reconstructed = Vec::new();

    // PE validation is authoritative in final_review.rs (lines 644-656 and
    // 1526-1536) which strips invalid mapped_to_bead_id values before
    // acceptance.  The aggregate already contains validated data — no
    // redundant re-read of the mutable prompt here.

    // Only reconstruct from the highest completion_round — earlier rounds'
    // PE decisions may have been superseded by the latest final-review
    // aggregate (e.g. a finding was planned-elsewhere in round 1 but
    // fixed/rejected in round 2).
    let rounds_to_scan: Vec<u32> = authoritative_max_round.into_iter().collect();
    for round in rounds_to_scan {
        let payload = latest_by_round[&round];
        let legacy_planned_elsewhere_ids = legacy_planned_elsewhere_amendment_ids(&payload.payload);

        let aggregate: FinalReviewAggregatePayload =
            match serde_json::from_value(payload.payload.clone()) {
                Ok(a) => a,
                Err(_) => continue,
            };

        for amendment in &aggregate.final_accepted_amendments {
            let is_legacy_planned_elsewhere =
                legacy_planned_elsewhere_ids.contains(&amendment.amendment_id);
            if amendment.classification != AmendmentClassification::FixCurrentBead
                && !is_legacy_planned_elsewhere
            {
                tracing::info!(
                    amendment_id = amendment.amendment_id.as_str(),
                    classification = %amendment.classification,
                    mapped_to_bead_id = ?amendment.mapped_to_bead_id,
                    "final-review classification metadata observed; planned-elsewhere reconstruction is deferred to the routing bead"
                );
                continue;
            }
            let mapped_to = match amendment.mapped_to_bead_id.as_deref() {
                Some(id) => {
                    let trimmed = id.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    trimmed
                }
                None => continue,
            };

            let identity_key = (
                bead_id.to_owned(),
                amendment.normalized_body.clone(),
                mapped_to.to_owned(),
                Some(round),
            );
            if seen_keys.contains(&identity_key) {
                continue;
            }
            seen_keys.insert(identity_key);

            let mapping = PlannedElsewhereMapping {
                active_bead_id: bead_id.to_owned(),
                finding_summary: amendment.normalized_body.clone(),
                mapped_to_bead_id: mapped_to.to_owned(),
                recorded_at: now,
                mapped_bead_verified: false,
                run_id: Some(run_id.to_owned()),
                completion_round: Some(round),
            };

            // Record the reconstructed mapping to the journal so subsequent
            // replays don't need to reconstruct again.
            if let Err(e) = milestone_service::record_planned_elsewhere_mapping(
                &FsMilestoneJournalStore,
                &FsPlannedElsewhereMappingStore,
                base_dir,
                milestone_id,
                &mapping,
            ) {
                tracing::warn!(
                    mapped_to_bead_id = mapped_to,
                    error = %e,
                    "failed to persist reconstructed planned-elsewhere mapping (non-blocking)"
                );
            }

            reconstructed.push(mapping);
        }
    }

    if !reconstructed.is_empty() {
        tracing::info!(
            bead_id = bead_id,
            count = reconstructed.len(),
            "reconstructed missing planned-elsewhere mappings from final-review aggregates"
        );
    }

    (reconstructed, authoritative_max_round)
}

fn legacy_planned_elsewhere_amendment_ids(
    aggregate_payload: &serde_json::Value,
) -> HashSet<String> {
    aggregate_payload
        .get("final_accepted_amendments")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|amendment| {
            amendment
                .get("classification")
                .and_then(serde_json::Value::as_str)
                == Some("planned-elsewhere")
        })
        .filter_map(|amendment| {
            amendment
                .get("amendment_id")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::br_process::{BrOutput, ProcessRunner};
    use crate::adapters::bv_process::{BvError, BvOutput, BvProcessRunner};
    use crate::contexts::milestone_record::service::MilestoneSnapshotPort;
    use crate::contexts::project_run_record::model::PayloadRecord;
    use crate::contexts::workflow_composition::panel_contracts::FinalReviewCanonicalAmendment;
    use crate::contexts::workflow_composition::payloads::{
        ClassifiedFinding, ReviewOutcome, ValidationPayload,
    };
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    // ── Mock BR runner ─────────────────────────────────────────────────

    struct MockBrRunner {
        responses: Mutex<Vec<Result<BrOutput, BrError>>>,
    }

    impl MockBrRunner {
        fn new(responses: Vec<Result<BrOutput, BrError>>) -> Self {
            Self {
                responses: Mutex::new(responses),
            }
        }

        fn success(stdout: &str) -> Result<BrOutput, BrError> {
            Ok(BrOutput {
                stdout: stdout.to_owned(),
                stderr: String::new(),
                exit_code: 0,
            })
        }

        fn error(exit_code: i32, stderr: &str) -> Result<BrOutput, BrError> {
            Err(BrError::BrExitError {
                exit_code,
                stdout: String::new(),
                stderr: stderr.to_owned(),
                command: "br mock".to_owned(),
            })
        }
    }

    impl ProcessRunner for MockBrRunner {
        async fn run(
            &self,
            _args: Vec<String>,
            _timeout: Duration,
            _working_dir: Option<&Path>,
        ) -> Result<BrOutput, BrError> {
            self.responses
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| panic!("MockBrRunner: no more responses"))
        }
    }

    fn write_beads_export(base_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
        std::fs::create_dir_all(base_dir.join(".beads"))?;
        std::fs::write(
            base_dir.join(".beads/issues.jsonl"),
            "{\"id\":\"seed-bead\"}\n",
        )?;
        Ok(())
    }

    // ── Mock BV runner ─────────────────────────────────────────────────

    struct MockBvRunner {
        responses: Mutex<Vec<Result<BvOutput, BvError>>>,
    }

    impl MockBvRunner {
        fn new(responses: Vec<Result<BvOutput, BvError>>) -> Self {
            Self {
                responses: Mutex::new(responses),
            }
        }

        fn success(stdout: &str) -> Result<BvOutput, BvError> {
            Ok(BvOutput {
                stdout: stdout.to_owned(),
                stderr: String::new(),
                exit_code: 0,
            })
        }
    }

    impl BvProcessRunner for MockBvRunner {
        async fn run(
            &self,
            _args: Vec<String>,
            _timeout: Duration,
            _working_dir: Option<&Path>,
        ) -> Result<BvOutput, BvError> {
            self.responses
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| panic!("MockBvRunner: no more responses"))
        }
    }

    struct DeletingBvRunner {
        responses: Mutex<Vec<Result<BvOutput, BvError>>>,
        path_to_remove: PathBuf,
    }

    impl DeletingBvRunner {
        fn new(path_to_remove: PathBuf, responses: Vec<Result<BvOutput, BvError>>) -> Self {
            Self {
                responses: Mutex::new(responses),
                path_to_remove,
            }
        }
    }

    impl BvProcessRunner for DeletingBvRunner {
        async fn run(
            &self,
            _args: Vec<String>,
            _timeout: Duration,
            _working_dir: Option<&Path>,
        ) -> Result<BvOutput, BvError> {
            let response = self
                .responses
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| panic!("DeletingBvRunner: no more responses"));
            std::fs::remove_dir_all(&self.path_to_remove)
                .expect("DeletingBvRunner should remove the milestone root");
            response
        }
    }

    type BrHook = Arc<dyn Fn(&[String], Option<&Path>) + Send + Sync>;

    struct RecordingBrRunner {
        responses: Mutex<Vec<Result<BrOutput, BrError>>>,
        invocations: Arc<Mutex<Vec<Vec<String>>>>,
        after_run: Option<BrHook>,
    }

    impl RecordingBrRunner {
        fn new(
            responses: Vec<Result<BrOutput, BrError>>,
            invocations: Arc<Mutex<Vec<Vec<String>>>>,
            after_run: Option<BrHook>,
        ) -> Self {
            Self {
                responses: Mutex::new(responses),
                invocations,
                after_run,
            }
        }
    }

    impl ProcessRunner for RecordingBrRunner {
        async fn run(
            &self,
            args: Vec<String>,
            _timeout: Duration,
            working_dir: Option<&Path>,
        ) -> Result<BrOutput, BrError> {
            self.invocations.lock().unwrap().push(args.clone());
            if let Some(after_run) = &self.after_run {
                after_run(&args, working_dir);
            }
            self.responses
                .lock()
                .unwrap()
                .pop()
                .unwrap_or_else(|| panic!("RecordingBrRunner: no more responses"))
        }
    }

    fn seed_planned_elsewhere_mapping(
        base_dir: &Path,
        milestone_id: &str,
        active_bead_id: &str,
        mapped_to_bead_id: &str,
        run_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::FsMilestoneStore;
        use crate::contexts::milestone_record::service::{create_milestone, CreateMilestoneInput};

        let now = Utc::now();
        let record = create_milestone(
            &FsMilestoneStore,
            base_dir,
            CreateMilestoneInput {
                id: milestone_id.to_owned(),
                name: format!("Milestone {milestone_id}"),
                description: "planned-elsewhere reconciliation test".to_owned(),
            },
            now,
        )?;
        let mapping = PlannedElsewhereMapping {
            active_bead_id: active_bead_id.to_owned(),
            finding_summary: "needs follow-up elsewhere".to_owned(),
            mapped_to_bead_id: mapped_to_bead_id.to_owned(),
            recorded_at: now,
            mapped_bead_verified: false,
            run_id: Some(run_id.to_owned()),
            completion_round: Some(1),
        };
        milestone_service::record_planned_elsewhere_mapping(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            base_dir,
            &record.id,
            &mapping,
        )?;
        Ok(())
    }

    // ── Tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn close_bead_idempotent_already_closed() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        // br show returns closed status
        let show_json =
            r#"{"id":"b1","title":"Test","status":"closed","priority":2,"bead_type":"task"}"#;
        let runner = MockBrRunner::new(vec![MockBrRunner::success(show_json)]);
        let br_adapter = BrAdapter::with_runner(runner);
        // Mutation adapter won't be called
        let mutation_runner = MockBrRunner::new(vec![]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let result =
            close_bead_idempotent(temp_dir.path(), &br_mutation, &br_adapter, "b1", "task-1")
                .await?;
        assert!(result, "should report bead was already closed");
        Ok(())
    }

    #[tokio::test]
    async fn close_bead_idempotent_open_then_closed() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        write_beads_export(temp_dir.path())?;
        // br show returns open, then close succeeds
        let show_json =
            r#"{"id":"b1","title":"Test","status":"open","priority":2,"bead_type":"task"}"#;
        // Responses are popped from the end (stack order)
        let runner = MockBrRunner::new(vec![MockBrRunner::success(show_json)]);
        let br_adapter = BrAdapter::with_runner(runner);

        let close_output = MockBrRunner::success("");
        let mutation_runner = MockBrRunner::new(vec![close_output]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let result =
            close_bead_idempotent(temp_dir.path(), &br_mutation, &br_adapter, "b1", "task-1")
                .await?;
        assert!(!result, "should report bead was freshly closed");
        Ok(())
    }

    #[tokio::test]
    async fn close_bead_failure_returns_error() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        write_beads_export(temp_dir.path())?;
        // br show returns open, close fails, second show also returns open
        let show_open =
            r#"{"id":"b1","title":"Test","status":"open","priority":2,"bead_type":"task"}"#;
        // Stack order: second show (check after failure), first show (initial check)
        let runner = MockBrRunner::new(vec![
            MockBrRunner::success(show_open),
            MockBrRunner::success(show_open),
        ]);
        let br_adapter = BrAdapter::with_runner(runner);

        let mutation_runner = MockBrRunner::new(vec![MockBrRunner::error(1, "close failed")]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let result =
            close_bead_idempotent(temp_dir.path(), &br_mutation, &br_adapter, "b1", "task-1").await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, ReconciliationError::BrCloseFailed { .. }),
            "expected BrCloseFailed, got: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn close_failure_but_already_closed_is_idempotent(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        write_beads_export(temp_dir.path())?;
        // br show returns open, close fails, second show returns closed
        let show_open =
            r#"{"id":"b1","title":"Test","status":"open","priority":2,"bead_type":"task"}"#;
        let show_closed =
            r#"{"id":"b1","title":"Test","status":"closed","priority":2,"bead_type":"task"}"#;
        // Stack order: second show (closed), first show (open)
        let runner = MockBrRunner::new(vec![
            MockBrRunner::success(show_closed),
            MockBrRunner::success(show_open),
        ]);
        let br_adapter = BrAdapter::with_runner(runner);

        let mutation_runner = MockBrRunner::new(vec![MockBrRunner::error(1, "already closed")]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let result =
            close_bead_idempotent(temp_dir.path(), &br_mutation, &br_adapter, "b1", "task-1")
                .await?;
        assert!(
            result,
            "should be idempotent when close fails but bead is closed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn close_failure_but_already_closed_restores_pending_close_record(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        write_beads_export(temp_dir.path())?;
        let show_open =
            r#"{"id":"b1","title":"Test","status":"open","priority":2,"bead_type":"task"}"#;
        let show_closed =
            r#"{"id":"b1","title":"Test","status":"closed","priority":2,"bead_type":"task"}"#;
        let runner = MockBrRunner::new(vec![
            MockBrRunner::success(show_closed),
            MockBrRunner::success(show_open),
        ]);
        let br_adapter = BrAdapter::with_runner(runner);

        let mutation_runner = MockBrRunner::new(vec![MockBrRunner::error(1, "already closed")]);
        let br_mutation = BrMutationAdapter::with_adapter_id(
            BrAdapter::with_runner(mutation_runner).with_working_dir(temp_dir.path().to_path_buf()),
            "close-owner",
        );

        let result =
            close_bead_idempotent(temp_dir.path(), &br_mutation, &br_adapter, "b1", "task-1")
                .await?;
        assert!(
            result,
            "should be idempotent when close fails but bead is closed"
        );

        let pending = std::fs::read_to_string(
            temp_dir
                .path()
                .join(".beads/.br-unsynced-mutations.d/close-owner.json"),
        )?;
        assert!(pending.contains(r#""operation":"update_bead_status""#));
        assert!(pending.contains(r#""bead_id":"b1""#));
        assert!(pending.contains(r#""status":"closed""#));
        Ok(())
    }

    #[tokio::test]
    async fn sync_after_close_success() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        write_beads_export(temp_dir.path())?;
        let adapter_id = "reconciliation-owner";
        let own_record = temp_dir
            .path()
            .join(format!(".beads/.br-unsynced-mutations.d/{adapter_id}.json"));
        std::fs::create_dir_all(
            own_record
                .parent()
                .expect("own pending record must have a parent dir"),
        )?;
        std::fs::write(
            &own_record,
            r#"{"adapter_id":"reconciliation-owner","operation":"update_bead_status","bead_id":"b1","status":"closed"}"#,
        )?;
        let runner = MockBrRunner::new(vec![MockBrRunner::success("")]);
        let br_mutation = BrMutationAdapter::with_adapter_id(
            BrAdapter::with_runner(runner).with_working_dir(temp_dir.path().to_path_buf()),
            adapter_id,
        );

        sync_after_close(temp_dir.path(), &br_mutation, "b1", "task-1").await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_after_close_failure() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        write_beads_export(temp_dir.path())?;
        let adapter_id = "reconciliation-owner";
        let own_record = temp_dir
            .path()
            .join(format!(".beads/.br-unsynced-mutations.d/{adapter_id}.json"));
        std::fs::create_dir_all(
            own_record
                .parent()
                .expect("own pending record must have a parent dir"),
        )?;
        std::fs::write(
            &own_record,
            r#"{"adapter_id":"reconciliation-owner","operation":"update_bead_status","bead_id":"b1","status":"closed"}"#,
        )?;
        let runner = MockBrRunner::new(vec![MockBrRunner::error(1, "sync failed")]);
        let br_mutation = BrMutationAdapter::with_adapter_id(
            BrAdapter::with_runner(runner).with_working_dir(temp_dir.path().to_path_buf()),
            adapter_id,
        );

        let result = sync_after_close(temp_dir.path(), &br_mutation, "b1", "task-1").await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ReconciliationError::BrSyncFailed { .. }
        ));
        Ok(())
    }

    #[tokio::test]
    async fn sync_after_close_rechecks_beads_health_before_flush(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        std::fs::create_dir_all(temp_dir.path().join(".beads"))?;
        std::fs::write(
            temp_dir.path().join(".beads/.br-unsynced-mutations"),
            "pending\n",
        )?;
        std::fs::write(
            temp_dir.path().join(".beads/issues.jsonl"),
            "<<<<<<< HEAD\n{\"id\":\"b1\"}\n=======\n{\"id\":\"b2\"}\n>>>>>>> branch\n",
        )?;
        let runner = MockBrRunner::new(vec![]);
        let br_mutation = BrMutationAdapter::with_adapter(
            BrAdapter::with_runner(runner).with_working_dir(temp_dir.path().to_path_buf()),
        );

        let result = sync_after_close(temp_dir.path(), &br_mutation, "b1", "task-1").await;
        let error = result.expect_err("unsafe beads export should block sync flush");
        match error {
            ReconciliationError::BrSyncFailed { details, .. } => {
                assert!(
                    details.contains("bead state became unsafe before br sync --flush-only"),
                    "error should explain the blocked sync: {details}"
                );
                assert!(
                    details.contains("resolve the conflict"),
                    "error should direct the operator to resolve the conflict: {details}"
                );
            }
            other => panic!("expected BrSyncFailed, got {other}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn sync_after_close_rechecks_repo_pending_state_before_guarded_flush(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        write_beads_export(temp_dir.path())?;
        let runner = MockBrRunner::new(vec![]);
        let br_mutation = BrMutationAdapter::with_adapter(
            BrAdapter::with_runner(runner).with_working_dir(temp_dir.path().to_path_buf()),
        );

        std::fs::write(
            temp_dir.path().join(".beads/.br-unsynced-mutations"),
            "pending\n",
        )?;
        std::fs::remove_file(temp_dir.path().join(".beads/issues.jsonl"))?;

        let result = sync_after_close(temp_dir.path(), &br_mutation, "b1", "task-1").await;
        let error = result.expect_err("recovered pending sync should recheck bead export health");
        match error {
            ReconciliationError::BrSyncFailed { details, .. } => {
                assert!(
                    details.contains("missing .beads/issues.jsonl"),
                    "error should explain the blocked replay sync: {details}"
                );
            }
            other => panic!("expected BrSyncFailed, got {other}"),
        }
        assert!(
            temp_dir
                .path()
                .join(".beads/.br-unsynced-mutations")
                .exists(),
            "blocked replay must preserve the pending marker for a later clean retry"
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_after_close_rejects_foreign_pending_mutation_replay(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        write_beads_export(temp_dir.path())?;
        let journal_path = temp_dir
            .path()
            .join(".beads/.br-unsynced-mutations.d/foreign.json");
        std::fs::create_dir_all(
            journal_path
                .parent()
                .expect("journal path must have a parent dir"),
        )?;
        std::fs::write(
            &journal_path,
            r#"{"adapter_id":"other-workflow","operation":"create_bead","bead_id":"bead-foreign","status":null}"#,
        )?;
        let runner = MockBrRunner::new(vec![]);
        let br_mutation = BrMutationAdapter::with_adapter_id(
            BrAdapter::with_runner(runner).with_working_dir(temp_dir.path().to_path_buf()),
            "reconcile-success-owner",
        );

        let result = sync_after_close(temp_dir.path(), &br_mutation, "b1", "task-1").await;
        let error = result.expect_err("foreign pending mutation should block replay sync");
        match error {
            ReconciliationError::BrSyncFailed { details, .. } => {
                assert!(
                    details.contains("another local bead workflow still has pending `create_bead`"),
                    "error should explain the blocked foreign replay: {details}"
                );
            }
            other => panic!("expected BrSyncFailed, got {other}"),
        }
        assert!(
            journal_path.exists(),
            "blocking the replay must leave the foreign journal record in place"
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_after_close_skips_unhealthy_export_when_no_pending_mutations(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        std::fs::create_dir_all(temp_dir.path().join(".beads"))?;
        std::fs::write(
            temp_dir.path().join(".beads/issues.jsonl"),
            "<<<<<<< HEAD\n{\"id\":\"b1\"}\n=======\n{\"id\":\"b2\"}\n>>>>>>> branch\n",
        )?;
        let runner = MockBrRunner::new(vec![]);
        let br_mutation = BrMutationAdapter::with_adapter(
            BrAdapter::with_runner(runner).with_working_dir(temp_dir.path().to_path_buf()),
        );

        sync_after_close(temp_dir.path(), &br_mutation, "b1", "task-1").await?;
        Ok(())
    }

    /// Sync failure when bead was already closed (replay scenario) must still
    /// be treated as a fatal error. `was_already_closed` is not a sound proxy
    /// for "sync already completed" — a crash between close and sync produces
    /// the same flag but with an un-flushed local state.
    #[tokio::test]
    async fn sync_failure_on_replay_is_still_fatal() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        write_beads_export(temp_dir.path())?;
        std::fs::write(
            temp_dir.path().join(".beads/.br-unsynced-mutations"),
            "pending\n",
        )?;
        std::fs::create_dir_all(temp_dir.path().join(".ralph-burning/milestones/ms-1"))?;

        // br show returns closed (bead already closed from prior attempt)
        let show_closed =
            r#"{"id":"b1","title":"Test","status":"closed","priority":2,"bead_type":"task"}"#;
        let read_runner = MockBrRunner::new(vec![MockBrRunner::success(show_closed)]);
        let br_read = BrAdapter::with_runner(read_runner);

        // Mutation adapter: sync fails (no close needed since bead already closed)
        let mutation_runner = MockBrRunner::new(vec![MockBrRunner::error(1, "sync failed")]);
        let br_mutation = BrMutationAdapter::with_adapter(
            BrAdapter::with_runner(mutation_runner).with_working_dir(temp_dir.path().to_path_buf()),
        );

        let now = chrono::Utc::now();
        let result = reconcile_success(
            &br_mutation,
            &br_read,
            None::<&BvAdapter<MockBvRunner>>,
            temp_dir.path(),
            "b1",
            "task-1",
            "proj-1",
            "ms-1",
            "run-1",
            None,
            now - chrono::Duration::seconds(10),
            now,
        )
        .await;

        assert!(result.is_err(), "sync failure on replay should be fatal");
        assert!(
            matches!(
                result.unwrap_err(),
                ReconciliationError::BrSyncFailed { .. }
            ),
            "should return BrSyncFailed even when bead was already closed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_success_rejects_conflicted_beads_before_close(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        std::fs::create_dir_all(temp_dir.path().join(".beads"))?;
        std::fs::write(
            temp_dir.path().join(".beads/issues.jsonl"),
            r#"<<<<<<< HEAD
{"id":"b1"}
=======
{"id":"b2"}
>>>>>>> branch
"#,
        )?;
        std::fs::create_dir_all(temp_dir.path().join(".ralph-burning/milestones/ms-1"))?;

        let show_open =
            r#"{"id":"b1","title":"Test","status":"open","priority":2,"bead_type":"task"}"#;
        let br_read =
            BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success(show_open)]));
        let br_mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));

        let now = chrono::Utc::now();
        let result = reconcile_success(
            &br_mutation,
            &br_read,
            None::<&BvAdapter<MockBvRunner>>,
            temp_dir.path(),
            "b1",
            "task-1",
            "proj-1",
            "ms-1",
            "run-1",
            None,
            now - chrono::Duration::seconds(10),
            now,
        )
        .await;

        let error = result.expect_err("conflicted beads export should block reconciliation");
        assert!(
            matches!(error, ReconciliationError::MilestoneUpdateFailed { .. }),
            "expected milestone update failure, got {error}"
        );
        assert!(
            error.to_string().contains("conflict"),
            "error should direct the operator to resolve conflicts: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_success_rejects_malformed_beads_before_close(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        std::fs::create_dir_all(temp_dir.path().join(".beads"))?;
        std::fs::write(
            temp_dir.path().join(".beads/issues.jsonl"),
            "{\"id\":\"b1\"}\n{\"id\": }\n",
        )?;
        std::fs::create_dir_all(temp_dir.path().join(".ralph-burning/milestones/ms-1"))?;

        let show_open =
            r#"{"id":"b1","title":"Test","status":"open","priority":2,"bead_type":"task"}"#;
        let br_read =
            BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success(show_open)]));
        let br_mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));

        let now = chrono::Utc::now();
        let result = reconcile_success(
            &br_mutation,
            &br_read,
            None::<&BvAdapter<MockBvRunner>>,
            temp_dir.path(),
            "b1",
            "task-1",
            "proj-1",
            "ms-1",
            "run-1",
            None,
            now - chrono::Duration::seconds(10),
            now,
        )
        .await;

        let error = result.expect_err("malformed beads export should block reconciliation");
        assert!(
            matches!(error, ReconciliationError::MilestoneUpdateFailed { .. }),
            "expected milestone update failure, got {error}"
        );
        assert!(
            error
                .to_string()
                .contains("malformed .beads/issues.jsonl line 2"),
            "error should direct the operator to repair malformed JSONL: {error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn verify_planned_elsewhere_skips_comment_mutations_when_beads_export_is_unhealthy(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let base_dir = temp_dir.path();
        seed_planned_elsewhere_mapping(base_dir, "ms-pe-conflict", "bead-A", "bead-B", "run-1")?;
        std::fs::create_dir_all(base_dir.join(".beads"))?;
        std::fs::write(
            base_dir.join(".beads/issues.jsonl"),
            r#"<<<<<<< HEAD
{"id":"bead-A"}
=======
{"id":"bead-B"}
>>>>>>> branch
"#,
        )?;

        let read_invocations = Arc::new(Mutex::new(Vec::new()));
        let read_runner = RecordingBrRunner::new(
            vec![MockBrRunner::success(
                r#"{"id":"bead-B","title":"Target bead","status":"open","priority":1,"bead_type":"task","labels":[],"dependencies":[],"dependents":[],"acceptance_criteria":[]}"#,
            )],
            read_invocations,
            None,
        );
        let br_read = BrAdapter::with_runner(read_runner).with_working_dir(base_dir.to_path_buf());
        let mutation_invocations = Arc::new(Mutex::new(Vec::new()));
        let mutation_runner =
            RecordingBrRunner::new(Vec::new(), mutation_invocations.clone(), None);
        let br_mutation = BrMutationAdapter::with_adapter(
            BrAdapter::with_runner(mutation_runner).with_working_dir(base_dir.to_path_buf()),
        );

        verify_planned_elsewhere_after_success(
            &br_mutation,
            &br_read,
            base_dir,
            "bead-A",
            "ms-pe-conflict",
            "proj-1",
            "run-1",
        )
        .await;

        assert!(
            mutation_invocations.lock().unwrap().is_empty(),
            "unhealthy bead exports should block planned-elsewhere comment mutations"
        );
        Ok(())
    }

    #[tokio::test]
    async fn verify_planned_elsewhere_rechecks_health_before_comment_flush(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let base_dir = temp_dir.path();
        seed_planned_elsewhere_mapping(base_dir, "ms-pe-flush", "bead-A", "bead-B", "run-1")?;
        write_beads_export(base_dir)?;

        let read_invocations = Arc::new(Mutex::new(Vec::new()));
        let read_runner = RecordingBrRunner::new(
            vec![MockBrRunner::success(
                r#"{"id":"bead-B","title":"Target bead","status":"open","priority":1,"bead_type":"task","labels":[],"dependencies":[],"dependents":[],"acceptance_criteria":[]}"#,
            )],
            read_invocations,
            None,
        );
        let br_read = BrAdapter::with_runner(read_runner).with_working_dir(base_dir.to_path_buf());
        let mutation_invocations = Arc::new(Mutex::new(Vec::new()));
        let issues_path = base_dir.join(".beads/issues.jsonl");
        let after_run: BrHook = Arc::new(move |args, _working_dir| {
            if args.first().map(String::as_str) == Some("comments") {
                std::fs::write(
                    &issues_path,
                    r#"<<<<<<< HEAD
{"id":"bead-A"}
=======
{"id":"bead-B"}
>>>>>>> branch
"#,
                )
                .expect("rewrite issues.jsonl with conflict markers");
            }
        });
        let mutation_runner = RecordingBrRunner::new(
            vec![MockBrRunner::success("commented")],
            mutation_invocations.clone(),
            Some(after_run),
        );
        let br_mutation = BrMutationAdapter::with_adapter(
            BrAdapter::with_runner(mutation_runner).with_working_dir(base_dir.to_path_buf()),
        );

        verify_planned_elsewhere_after_success(
            &br_mutation,
            &br_read,
            base_dir,
            "bead-A",
            "ms-pe-flush",
            "proj-1",
            "run-1",
        )
        .await;

        let invocations = mutation_invocations.lock().unwrap();
        assert_eq!(
            invocations.len(),
            1,
            "health should be rechecked before flush so only the comment mutation runs"
        );
        assert_eq!(
            invocations[0].first().map(String::as_str),
            Some("comments"),
            "the only planned-elsewhere mutation should be the comment itself"
        );
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_success_persists_reconciling_before_close_failure(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::{FsMilestonePlanStore, FsMilestoneStore};
        use crate::contexts::milestone_record::bundle::{
            AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
        };
        use crate::contexts::milestone_record::controller as milestone_controller;
        use crate::contexts::milestone_record::service::{
            create_milestone, persist_plan, CreateMilestoneInput,
        };
        use chrono::Utc;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        std::fs::create_dir_all(base.join(".ralph-burning/milestones"))?;

        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;

        let now = Utc::now();
        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "ms-close-failure".to_owned(),
                name: "Close failure test".to_owned(),
                description: "Verifies controller state is durable before br close".to_owned(),
            },
            now,
        )?;

        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-close-failure".to_owned(),
                name: "Close failure test".to_owned(),
            },
            executive_summary: "Close failure test.".to_owned(),
            goals: vec!["Verify controller ordering".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Bead completes".to_owned(),
                covered_by: vec!["bead-close".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: Some("Fixture description.".to_owned()),
                beads: vec![BeadProposal {
                    bead_id: Some("bead-close".to_owned()),
                    explicit_id: Some(true),
                    title: "Close bead".to_owned(),
                    description: Some("Fixture description.".to_owned()),
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec!["fixture".to_owned()],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::Minimal,
            agents_guidance: None,
        };
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle,
            now + chrono::Duration::milliseconds(1),
        )?;

        let started_at = now + chrono::Duration::seconds(1);
        milestone_service::record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-close",
            "proj-close",
            "run-close",
            "plan-v1",
            started_at,
        )?;
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base,
            &record.id,
            "bead-close",
            "proj-close",
            "workflow execution started",
            started_at,
        )?;

        let show_open = r#"{"id":"bead-close","title":"Close bead","status":"open","priority":2,"bead_type":"task"}"#;
        let read_runner = MockBrRunner::new(vec![
            MockBrRunner::success(show_open),
            MockBrRunner::success(show_open),
        ]);
        let br_read = BrAdapter::with_runner(read_runner);

        let mutation_runner = MockBrRunner::new(vec![MockBrRunner::error(1, "close failed")]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let result = reconcile_success(
            &br_mutation,
            &br_read,
            None::<&BvAdapter<MockBvRunner>>,
            base,
            "bead-close",
            "task-close-1",
            "proj-close",
            "ms-close-failure",
            "run-close",
            Some("plan-v1"),
            started_at,
            now + chrono::Duration::seconds(10),
        )
        .await;

        assert!(matches!(
            result,
            Err(ReconciliationError::BrCloseFailed { .. })
        ));

        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &record.id)?
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Reconciling
        );
        assert_eq!(controller.active_bead_id.as_deref(), Some("bead-close"));
        assert_eq!(controller.active_task_id.as_deref(), Some("proj-close"));
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("reconciling milestone state")));

        Ok(())
    }

    #[tokio::test]
    async fn capture_hint_success() -> Result<(), Box<dyn std::error::Error>> {
        let hint_json = r#"{"id":"b2","title":"Next task","score":0.9,"reasons":["dependency resolved"],"action":"start"}"#;
        let runner = MockBvRunner::new(vec![MockBvRunner::success(hint_json)]);
        let bv = BvAdapter::with_runner(runner);

        let result = capture_next_step_hint(&bv).await;
        let HintCaptureOutcome::Captured(hint) = result else {
            panic!("expected Captured, got BvFailed");
        };
        assert_eq!(hint.id, "b2");
        assert_eq!(hint.title, "Next task");
        Ok(())
    }

    #[tokio::test]
    async fn capture_hint_failure_returns_bv_failed() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockBvRunner::new(vec![Err(BvError::BvNotFound {
            details: "bv not found".to_owned(),
        })]);
        let bv = BvAdapter::with_runner(runner);

        let result = capture_next_step_hint(&bv).await;
        assert!(
            matches!(result, HintCaptureOutcome::BvFailed),
            "hint failure should return BvFailed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn capture_hint_no_recommendation_on_message_only_response(
    ) -> Result<(), Box<dyn std::error::Error>> {
        // bv --robot-next returns a message-only JSON when no beads are actionable.
        // The runner returns a successful exit code with message-only stdout;
        // exec_json fails to parse as NextBeadResponse, but the raw output is
        // a valid BvMessageOnlyResponse — should produce NoRecommendation.
        let message_json = r#"{"message":"No actionable items available"}"#;
        let runner = MockBvRunner::new(vec![MockBvRunner::success(message_json)]);
        let bv = BvAdapter::with_runner(runner);

        let result = capture_next_step_hint(&bv).await;
        assert!(
            matches!(result, HintCaptureOutcome::NoRecommendation),
            "message-only bv response should return NoRecommendation, got: {result:?}"
        );
        Ok(())
    }

    /// A BvExitError (non-zero exit) whose stdout happens to contain
    /// message-only JSON must produce BvFailed, NOT NoRecommendation.
    /// The pattern match in capture_next_step_hint only checks BvParseError
    /// for the message-only fallback; BvExitError is always treated as a
    /// genuine failure regardless of stdout content.
    #[tokio::test]
    async fn capture_hint_exit_error_with_message_stdout_returns_bv_failed(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockBvRunner::new(vec![Err(BvError::BvExitError {
            exit_code: 1,
            stdout: r#"{"message":"No actionable items available"}"#.to_owned(),
            stderr: "internal error".to_owned(),
            command: "bv --robot-next".to_owned(),
        })]);
        let bv = BvAdapter::with_runner(runner);

        let result = capture_next_step_hint(&bv).await;
        assert!(
            matches!(result, HintCaptureOutcome::BvFailed),
            "BvExitError with message-only stdout must return BvFailed, got: {result:?}"
        );
        Ok(())
    }

    /// After a previous bead persisted a hint, if bv returns "no actionable
    /// items", `reconcile_success` should delete the stale hint file so
    /// downstream selection does not act on a pointer to an already-completed
    /// bead.
    #[tokio::test]
    async fn reconcile_success_clears_stale_hint_on_no_recommendation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::{FsMilestonePlanStore, FsMilestoneStore};
        use crate::contexts::milestone_record::bundle::{
            AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
        };
        use crate::contexts::milestone_record::service::{
            create_milestone, persist_plan, CreateMilestoneInput,
        };
        use chrono::Utc;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        std::fs::create_dir_all(base.join(".ralph-burning/milestones"))?;

        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;

        let now = Utc::now();

        // 1. Create milestone with one bead
        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "ms-hint-clear".to_owned(),
                name: "Hint clear test".to_owned(),
                description: "Tests stale hint removal".to_owned(),
            },
            now,
        )?;

        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-hint-clear".to_owned(),
                name: "Hint clear test".to_owned(),
            },
            executive_summary: "Hint clear test.".to_owned(),
            goals: vec!["Test hint clearing".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Bead completes".to_owned(),
                covered_by: vec!["bead-hint".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: Some("Fixture description.".to_owned()),
                beads: vec![BeadProposal {
                    bead_id: Some("bead-hint".to_owned()),
                    explicit_id: Some(true),
                    title: "Hint bead".to_owned(),
                    description: Some("Fixture description.".to_owned()),
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec!["fixture".to_owned()],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::Minimal,
            agents_guidance: None,
        };
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle,
            now + chrono::Duration::milliseconds(1),
        )?;

        // 2. Start the bead
        let started_at = now + chrono::Duration::seconds(1);
        milestone_service::record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-hint",
            "proj-hint",
            "run-hint",
            "plan-v1",
            started_at,
        )?;

        // 3. Pre-persist a stale hint file (simulating a prior bead's hint)
        let milestone_id = MilestoneId::new("ms-hint-clear")?;
        let hint_path = crate::adapters::fs::FileSystem::milestone_root(base, &milestone_id)
            .join("next_step_hint.json");
        std::fs::write(
            &hint_path,
            r#"{"id":"stale-bead","title":"Stale","score":0.5,"reasons":[],"action":"start"}"#,
        )?;
        assert!(hint_path.exists(), "stale hint should be pre-persisted");

        // 4. Set up mock BR (bead open → close → sync) and BV (message-only)
        let show_open = r#"{"id":"bead-hint","title":"Hint bead","status":"open","priority":2,"bead_type":"task"}"#;
        let read_runner = MockBrRunner::new(vec![MockBrRunner::success(show_open)]);
        let br_read = BrAdapter::with_runner(read_runner);

        let mutation_runner = MockBrRunner::new(vec![
            MockBrRunner::success(""), // sync
            MockBrRunner::success(""), // close
        ]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        // bv returns message-only "no actionable items"
        let bv_message = r#"{"message":"No actionable items available"}"#;
        let bv_runner = MockBvRunner::new(vec![MockBvRunner::success(bv_message)]);
        let bv = BvAdapter::with_runner(bv_runner);

        // 5. Run reconcile_success
        let completed_at = now + chrono::Duration::seconds(10);
        let outcome = reconcile_success(
            &br_mutation,
            &br_read,
            Some(&bv),
            base,
            "bead-hint",
            "task-hint-789",
            "proj-hint",
            "ms-hint-clear",
            "run-hint",
            Some("plan-v1"),
            started_at,
            completed_at,
        )
        .await?;

        // 6. Assert hint is None and file was deleted
        assert!(
            outcome.next_step_hint.is_none(),
            "next_step_hint should be None for message-only bv response"
        );
        assert!(
            !hint_path.exists(),
            "stale next_step_hint.json should be deleted when bv returns no recommendation"
        );

        Ok(())
    }

    // ── End-to-end test ───────────────────────────────────────────────

    /// Exercises the full `reconcile_success` path including the milestone
    /// update step (step 3) that writes to real FS stores. Validates that
    /// snapshot progress advances, journal records the completion, and
    /// lineage includes the task_id linkage.
    #[tokio::test]
    async fn reconcile_success_end_to_end_updates_milestone(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::{
            FsMilestoneControllerStore, FsMilestonePlanStore, FsMilestoneStore,
        };
        use crate::contexts::milestone_record::bundle::{
            AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
        };
        use crate::contexts::milestone_record::controller as milestone_controller;
        use crate::contexts::milestone_record::model::{
            MilestoneEventType, MilestoneId, TaskRunOutcome,
        };
        use crate::contexts::milestone_record::service::{
            create_milestone, persist_plan, read_journal, read_task_runs, CreateMilestoneInput,
        };
        use chrono::Utc;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        std::fs::create_dir_all(base.join(".ralph-burning/milestones"))?;

        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;

        let now = Utc::now();

        // 1. Create milestone
        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "ms-e2e".to_owned(),
                name: "E2E test milestone".to_owned(),
                description: "End-to-end reconciliation test".to_owned(),
            },
            now,
        )?;

        // 2. Persist a plan with one bead ("bead-e2e")
        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-e2e".to_owned(),
                name: "E2E test milestone".to_owned(),
            },
            executive_summary: "Test milestone for reconciliation.".to_owned(),
            goals: vec!["Validate reconciliation".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Bead completes".to_owned(),
                covered_by: vec!["bead-e2e".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: Some("Fixture description.".to_owned()),
                beads: vec![BeadProposal {
                    bead_id: Some("bead-e2e".to_owned()),
                    explicit_id: Some(true),
                    title: "E2E bead".to_owned(),
                    description: Some("Fixture description.".to_owned()),
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec!["fixture".to_owned()],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::Minimal,
            agents_guidance: None,
        };
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle,
            now + chrono::Duration::milliseconds(1),
        )?;

        // 3. Record bead start (creates lineage row, sets active_bead)
        let started_at = now + chrono::Duration::seconds(1);
        milestone_service::record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-e2e",
            "proj-e2e",
            "run-e2e",
            "plan-v1",
            started_at,
        )?;

        // 4. Set up mock BR runners:
        //    - br show (open status for idempotency check)
        //    - br close (success)
        //    - br sync (success)
        let show_open = r#"{"id":"bead-e2e","title":"E2E bead","status":"open","priority":2,"bead_type":"task"}"#;

        // Read adapter: serves the show query (stack-popped, so last pushed = first used)
        let read_runner = MockBrRunner::new(vec![MockBrRunner::success(show_open)]);
        let br_read = BrAdapter::with_runner(read_runner);

        // Mutation adapter: close then sync (stack order: sync first, close second)
        let mutation_runner = MockBrRunner::new(vec![
            MockBrRunner::success(""), // sync
            MockBrRunner::success(""), // close
        ]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        // BV adapter: robot-next hint
        let hint_json = r#"{"id":"bead-next","title":"Next bead","score":0.8,"reasons":["dep done"],"action":"start"}"#;
        let bv_runner = MockBvRunner::new(vec![MockBvRunner::success(hint_json)]);
        let bv = BvAdapter::with_runner(bv_runner);

        // 5. Run reconcile_success
        let completed_at = now + chrono::Duration::seconds(10);
        let outcome = reconcile_success(
            &br_mutation,
            &br_read,
            Some(&bv),
            base,
            "bead-e2e",
            "task-e2e-123",
            "proj-e2e",
            "ms-e2e",
            "run-e2e",
            Some("plan-v1"),
            started_at,
            completed_at,
        )
        .await?;

        // 6. Assert outcome fields
        assert_eq!(outcome.bead_id, "bead-e2e");
        assert_eq!(outcome.task_id, "task-e2e-123");
        assert!(!outcome.was_already_closed);
        assert!(outcome.next_step_hint.is_some());
        assert_eq!(outcome.next_step_hint.as_ref().unwrap().id, "bead-next");
        assert_eq!(outcome.next_step_selection_warning, None);

        // 7. Assert milestone snapshot has updated progress
        let snapshot = snapshot_store.read_snapshot(base, &record.id)?;
        assert_eq!(
            snapshot.progress.completed_beads, 1,
            "completed_beads should be 1 after success reconciliation"
        );
        assert_eq!(
            snapshot.progress.in_progress_beads, 0,
            "in_progress_beads should be 0 after completion"
        );
        // active_bead should be cleared
        assert_eq!(
            snapshot.active_bead, None,
            "active_bead should be None after completion"
        );
        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &record.id)?
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Completed
        );
        assert_eq!(controller.active_bead_id, None);
        assert_eq!(controller.active_task_id, None);

        // 8. Assert journal contains a BeadCompleted event
        let milestone_id = MilestoneId::new("ms-e2e")?;
        let journal = read_journal(&journal_store, base, &milestone_id)?;
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|e| e.event_type == MilestoneEventType::BeadCompleted)
            .collect();
        assert!(
            !completion_events.is_empty(),
            "journal should contain at least one BeadCompleted event"
        );
        let completion_event = completion_events.last().unwrap();
        assert_eq!(completion_event.bead_id.as_deref(), Some("bead-e2e"));

        // 9. Assert lineage includes the entry with task_id linkage
        let task_runs = read_task_runs(&lineage_store, base, &milestone_id)?;
        let completed_runs: Vec<_> = task_runs
            .iter()
            .filter(|r| r.bead_id == "bead-e2e" && r.outcome == TaskRunOutcome::Succeeded)
            .collect();
        assert!(
            !completed_runs.is_empty(),
            "lineage should have a succeeded entry for bead-e2e"
        );
        let entry = completed_runs.last().unwrap();
        // outcome_detail should contain the task_id linkage
        assert!(
            entry
                .outcome_detail
                .as_deref()
                .unwrap_or("")
                .contains("task_id=task-e2e-123"),
            "outcome_detail should contain task_id linkage, got: {:?}",
            entry.outcome_detail
        );
        // Structural task_id field should be populated from outcome_detail
        assert_eq!(
            entry.task_id.as_deref(),
            Some("task-e2e-123"),
            "task_id field should be structurally populated, got: {:?}",
            entry.task_id
        );

        // 10. Assert next_step_hint was persisted to disk
        let hint_path = crate::adapters::fs::FileSystem::milestone_root(base, &milestone_id)
            .join("next_step_hint.json");
        assert!(hint_path.exists(), "next_step_hint.json should be written");
        let hint_json = std::fs::read_to_string(&hint_path)?;
        let persisted_hint: NextBeadResponse = serde_json::from_str(&hint_json)?;
        assert_eq!(persisted_hint.id, "bead-next");
        assert!((persisted_hint.score - 0.8).abs() < f64::EPSILON);

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_success_nonfinal_milestone_selects_the_next_bead(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::{
            FsMilestoneControllerStore, FsMilestonePlanStore, FsMilestoneStore,
        };
        use crate::contexts::milestone_record::bundle::{
            AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
        };
        use crate::contexts::milestone_record::controller as milestone_controller;
        use crate::contexts::milestone_record::service::{
            create_milestone, persist_plan, CreateMilestoneInput,
        };
        use chrono::Utc;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        std::fs::create_dir_all(base.join(".ralph-burning/milestones"))?;

        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;

        let now = Utc::now();
        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "ms-next".to_owned(),
                name: "Next-bead milestone".to_owned(),
                description: "Non-final reconciliation should continue selection".to_owned(),
            },
            now,
        )?;

        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-next".to_owned(),
                name: "Next-bead milestone".to_owned(),
            },
            executive_summary: "Verify post-reconciliation selection.".to_owned(),
            goals: vec!["Close one bead and select the next".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Both beads are represented in the plan".to_owned(),
                covered_by: vec!["bead-current".to_owned(), "bead-next".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: Some("Fixture description.".to_owned()),
                beads: vec![
                    BeadProposal {
                        bead_id: Some("bead-current".to_owned()),
                        explicit_id: Some(true),
                        title: "Current bead".to_owned(),
                        description: Some("Fixture description.".to_owned()),
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec!["fixture".to_owned()],
                        depends_on: vec![],
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                    BeadProposal {
                        bead_id: Some("bead-next".to_owned()),
                        explicit_id: Some(true),
                        title: "Next bead".to_owned(),
                        description: Some("Fixture description.".to_owned()),
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec!["fixture".to_owned()],
                        depends_on: vec![],
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                ],
            }],
            default_flow: crate::shared::domain::FlowPreset::Minimal,
            agents_guidance: None,
        };
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle,
            now + chrono::Duration::milliseconds(1),
        )?;

        let started_at = now + chrono::Duration::seconds(1);
        milestone_service::record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-current",
            "proj-next",
            "run-next",
            "plan-v1",
            started_at,
        )?;

        let show_open = r#"{"id":"bead-current","title":"Current bead","status":"open","priority":2,"bead_type":"task"}"#;
        let ready_next = serde_json::json!([
            {
                "id": "bead-next",
                "title": "Next bead",
                "priority": 1,
                "bead_type": "task",
                "labels": []
            }
        ])
        .to_string();
        let read_runner = MockBrRunner::new(vec![
            MockBrRunner::success(&ready_next),
            MockBrRunner::success(show_open),
        ]);
        let br_read = BrAdapter::with_runner(read_runner);

        let mutation_runner =
            MockBrRunner::new(vec![MockBrRunner::success(""), MockBrRunner::success("")]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let next_hint = r#"{"id":"bead-next","title":"Next bead","score":0.8,"reasons":["ready"],"action":"start"}"#;
        let bv_runner = MockBvRunner::new(vec![MockBvRunner::success(next_hint)]);
        let bv = BvAdapter::with_runner(bv_runner);

        let completed_at = now + chrono::Duration::seconds(10);
        let outcome = reconcile_success(
            &br_mutation,
            &br_read,
            Some(&bv),
            base,
            "bead-current",
            "task-next-123",
            "proj-next",
            "ms-next",
            "run-next",
            Some("plan-v1"),
            started_at,
            completed_at,
        )
        .await?;

        assert_eq!(
            outcome.next_step_hint.as_ref().map(|hint| hint.id.as_str()),
            Some("bead-next")
        );
        assert_eq!(outcome.next_step_selection_warning, None);

        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &record.id)?
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Claimed
        );
        assert_eq!(
            controller.active_bead_id.as_deref(),
            Some("bead-next"),
            "selected bead ID should preserve the raw form from br ready"
        );
        assert_eq!(controller.active_task_id, None);
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("bv recommended bead 'bead-next'")));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_success_nonfinal_selection_failure_moves_controller_to_needs_operator(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::{
            FileSystem, FsMilestoneControllerStore, FsMilestonePlanStore, FsMilestoneStore,
        };
        use crate::contexts::milestone_record::bundle::{
            AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
        };
        use crate::contexts::milestone_record::controller as milestone_controller;
        use crate::contexts::milestone_record::service::{
            create_milestone, persist_plan, CreateMilestoneInput,
        };
        use chrono::Utc;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        std::fs::create_dir_all(base.join(".ralph-burning/milestones"))?;

        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;

        let now = Utc::now();
        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "ms-selection-warning".to_owned(),
                name: "Selection warning milestone".to_owned(),
                description: "Selection failures after reconciliation should be non-fatal"
                    .to_owned(),
            },
            now,
        )?;

        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-selection-warning".to_owned(),
                name: "Selection warning milestone".to_owned(),
            },
            executive_summary: "Keep reconciliation successful when next-bead selection fails."
                .to_owned(),
            goals: vec!["Close the current bead".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Both beads are represented in the plan".to_owned(),
                covered_by: vec!["bead-current".to_owned(), "bead-next".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: Some("Fixture description.".to_owned()),
                beads: vec![
                    BeadProposal {
                        bead_id: Some("bead-current".to_owned()),
                        explicit_id: Some(true),
                        title: "Current bead".to_owned(),
                        description: Some("Fixture description.".to_owned()),
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec!["fixture".to_owned()],
                        depends_on: vec![],
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                    BeadProposal {
                        bead_id: Some("bead-next".to_owned()),
                        explicit_id: Some(true),
                        title: "Next bead".to_owned(),
                        description: Some("Fixture description.".to_owned()),
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec!["fixture".to_owned()],
                        depends_on: vec![],
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                ],
            }],
            default_flow: crate::shared::domain::FlowPreset::Minimal,
            agents_guidance: None,
        };
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle,
            now + chrono::Duration::milliseconds(1),
        )?;

        let started_at = now + chrono::Duration::seconds(1);
        milestone_service::record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-current",
            "proj-warning",
            "run-warning",
            "plan-v1",
            started_at,
        )?;

        std::fs::write(
            FileSystem::milestone_root(base, &record.id).join("plan.json"),
            "{not valid json",
        )?;

        let show_open = r#"{"id":"bead-current","title":"Current bead","status":"open","priority":2,"bead_type":"task"}"#;
        let read_runner = MockBrRunner::new(vec![MockBrRunner::success(show_open)]);
        let br_read = BrAdapter::with_runner(read_runner);

        let mutation_runner =
            MockBrRunner::new(vec![MockBrRunner::success(""), MockBrRunner::success("")]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let next_hint = r#"{"id":"bead-next","title":"Next bead","score":0.8,"reasons":["ready"],"action":"start"}"#;
        let bv = BvAdapter::with_runner(MockBvRunner::new(vec![MockBvRunner::success(next_hint)]));

        let outcome = reconcile_success(
            &br_mutation,
            &br_read,
            Some(&bv),
            base,
            "bead-current",
            "task-warning-123",
            "proj-warning",
            "ms-selection-warning",
            "run-warning",
            Some("plan-v1"),
            started_at,
            now + chrono::Duration::seconds(10),
        )
        .await?;

        assert_eq!(
            outcome.next_step_hint.as_ref().map(|hint| hint.id.as_str()),
            Some("bead-next")
        );
        assert!(outcome
            .next_step_selection_warning
            .as_deref()
            .is_some_and(|warning| {
                warning.contains("next-bead selection after reconciliation failed")
                    && warning.contains("plan.json")
            }));

        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &record.id)?
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::NeedsOperator
        );
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| {
                reason.contains("next-bead selection after reconciliation failed")
                    && reason.contains("plan.json")
            }));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_success_selection_failure_returns_error_when_safe_state_persistence_fails(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::{FileSystem, FsMilestonePlanStore, FsMilestoneStore};
        use crate::contexts::milestone_record::bundle::{
            AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
        };
        use crate::contexts::milestone_record::service::{
            create_milestone, persist_plan, CreateMilestoneInput,
        };
        use chrono::Utc;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        std::fs::create_dir_all(base.join(".ralph-burning/milestones"))?;

        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;

        let now = Utc::now();
        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "ms-selection-warning-fallback".to_owned(),
                name: "Selection warning fallback milestone".to_owned(),
                description:
                    "Selection fallback persistence should fail loudly when the controller root disappears."
                        .to_owned(),
            },
            now,
        )?;

        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-selection-warning-fallback".to_owned(),
                name: "Selection warning fallback milestone".to_owned(),
            },
            executive_summary:
                "If the milestone root disappears after reconciliation, surface the selection failure."
                    .to_owned(),
            goals: vec!["Close the current bead".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Both beads are represented in the plan".to_owned(),
                covered_by: vec!["bead-current".to_owned(), "bead-next".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: Some("Fixture description.".to_owned()),
                beads: vec![
                    BeadProposal {
                        bead_id: Some("bead-current".to_owned()),
                        explicit_id: Some(true),
                        title: "Current bead".to_owned(),
                        description: Some("Fixture description.".to_owned()),
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec!["fixture".to_owned()],
                        depends_on: vec![],
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                    BeadProposal {
                        bead_id: Some("bead-next".to_owned()),
                        explicit_id: Some(true),
                        title: "Next bead".to_owned(),
                        description: Some("Fixture description.".to_owned()),
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec!["fixture".to_owned()],
                        depends_on: vec![],
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                ],
            }],
            default_flow: crate::shared::domain::FlowPreset::Minimal,
            agents_guidance: None,
        };
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle,
            now + chrono::Duration::milliseconds(1),
        )?;

        let started_at = now + chrono::Duration::seconds(1);
        milestone_service::record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-current",
            "proj-warning",
            "run-warning",
            "plan-v1",
            started_at,
        )?;

        let show_open = r#"{"id":"bead-current","title":"Current bead","status":"open","priority":2,"bead_type":"task"}"#;
        let read_runner = MockBrRunner::new(vec![MockBrRunner::success(show_open)]);
        let br_read = BrAdapter::with_runner(read_runner);

        let mutation_runner =
            MockBrRunner::new(vec![MockBrRunner::success(""), MockBrRunner::success("")]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let next_hint = r#"{"id":"bead-next","title":"Next bead","score":0.8,"reasons":["ready"],"action":"start"}"#;
        let milestone_root = FileSystem::milestone_root(base, &record.id);
        let bv = BvAdapter::with_runner(DeletingBvRunner::new(
            milestone_root,
            vec![MockBvRunner::success(next_hint)],
        ));

        let outcome = reconcile_success(
            &br_mutation,
            &br_read,
            Some(&bv),
            base,
            "bead-current",
            "task-warning-456",
            "proj-warning",
            "ms-selection-warning-fallback",
            "run-warning",
            Some("plan-v1"),
            started_at,
            now + chrono::Duration::seconds(10),
        )
        .await;

        match outcome {
            Err(ReconciliationError::MilestoneUpdateFailed {
                bead_id,
                task_id,
                details,
            }) => {
                assert_eq!(bead_id, "bead-current");
                assert_eq!(task_id, "task-warning-456");
                assert!(details.contains("next-bead selection after reconciliation failed"));
                assert!(details.contains("failed to persist needs_operator controller state"));
            }
            other => panic!("expected milestone update failure, got {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_success_replay_after_final_completion_is_idempotent(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::{
            FsMilestoneControllerStore, FsMilestonePlanStore, FsMilestoneStore,
        };
        use crate::contexts::milestone_record::bundle::{
            AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
        };
        use crate::contexts::milestone_record::controller as milestone_controller;
        use crate::contexts::milestone_record::model::{MilestoneEventType, MilestoneId};
        use crate::contexts::milestone_record::service::{
            create_milestone, persist_plan, read_journal, read_task_runs, CreateMilestoneInput,
        };
        use chrono::Utc;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        std::fs::create_dir_all(base.join(".ralph-burning/milestones"))?;

        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;

        let now = Utc::now();
        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "ms-replay-e2e".to_owned(),
                name: "Replay E2E milestone".to_owned(),
                description: "Replays a completed success reconciliation".to_owned(),
            },
            now,
        )?;

        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-replay-e2e".to_owned(),
                name: "Replay E2E milestone".to_owned(),
            },
            executive_summary: "Replay E2E test.".to_owned(),
            goals: vec!["Verify replay idempotency".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Bead completes".to_owned(),
                covered_by: vec!["bead-replay".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: Some("Fixture description.".to_owned()),
                beads: vec![BeadProposal {
                    bead_id: Some("bead-replay".to_owned()),
                    explicit_id: Some(true),
                    title: "Replay bead".to_owned(),
                    description: Some("Fixture description.".to_owned()),
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec!["fixture".to_owned()],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::Minimal,
            agents_guidance: None,
        };
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle,
            now + chrono::Duration::milliseconds(1),
        )?;

        let started_at = now + chrono::Duration::seconds(1);
        milestone_service::record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-replay",
            "proj-replay",
            "run-replay",
            "plan-v1",
            started_at,
        )?;
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base,
            &record.id,
            "bead-replay",
            "proj-replay",
            "workflow execution started",
            started_at,
        )?;

        let show_open = r#"{"id":"bead-replay","title":"Replay bead","status":"open","priority":2,"bead_type":"task"}"#;
        let show_closed = r#"{"id":"bead-replay","title":"Replay bead","status":"closed","priority":2,"bead_type":"task"}"#;
        let read_runner = MockBrRunner::new(vec![
            MockBrRunner::success(show_closed),
            MockBrRunner::success(show_open),
        ]);
        let br_read = BrAdapter::with_runner(read_runner);

        let mutation_runner = MockBrRunner::new(vec![
            MockBrRunner::success(""),
            MockBrRunner::success(""),
            MockBrRunner::success(""),
        ]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let first = reconcile_success(
            &br_mutation,
            &br_read,
            None::<&BvAdapter<MockBvRunner>>,
            base,
            "bead-replay",
            "task-replay-1",
            "proj-replay",
            "ms-replay-e2e",
            "run-replay",
            Some("plan-v1"),
            started_at,
            now + chrono::Duration::seconds(10),
        )
        .await?;
        assert!(!first.was_already_closed);

        let controller_journal_before_replay =
            crate::contexts::milestone_record::controller::MilestoneControllerPort::read_transition_journal(
                &FsMilestoneControllerStore,
                base,
                &record.id,
            )?
            .len();

        let replay = reconcile_success(
            &br_mutation,
            &br_read,
            None::<&BvAdapter<MockBvRunner>>,
            base,
            "bead-replay",
            "task-replay-1",
            "proj-replay",
            "ms-replay-e2e",
            "run-replay",
            Some("plan-v1"),
            started_at,
            now + chrono::Duration::seconds(11),
        )
        .await?;
        assert!(replay.was_already_closed);

        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &record.id)?
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Completed
        );
        assert_eq!(
            crate::contexts::milestone_record::controller::MilestoneControllerPort::read_transition_journal(
                &FsMilestoneControllerStore,
                base,
                &record.id,
            )?
            .len(),
            controller_journal_before_replay
        );

        let milestone_id = MilestoneId::new("ms-replay-e2e")?;
        let journal = read_journal(&journal_store, base, &milestone_id)?;
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadCompleted)
            .collect();
        assert_eq!(completion_events.len(), 1);

        let task_runs = read_task_runs(&lineage_store, base, &milestone_id)?;
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].run_id.as_deref(), Some("run-replay"));
        assert_eq!(task_runs[0].task_id.as_deref(), Some("task-replay-1"));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_success_replay_after_final_completion_skips_unnecessary_sync_when_beads_export_is_now_unhealthy(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::{
            FsMilestoneControllerStore, FsMilestonePlanStore, FsMilestoneStore,
        };
        use crate::contexts::milestone_record::bundle::{
            AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
        };
        use crate::contexts::milestone_record::controller as milestone_controller;
        use crate::contexts::milestone_record::model::{MilestoneEventType, MilestoneId};
        use crate::contexts::milestone_record::service::{
            create_milestone, persist_plan, read_journal, read_task_runs, CreateMilestoneInput,
        };
        use chrono::Utc;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        std::fs::create_dir_all(base.join(".ralph-burning/milestones"))?;

        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;

        let now = Utc::now();
        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "ms-replay-unhealthy".to_owned(),
                name: "Replay unhealthy milestone".to_owned(),
                description: "Replays after bead export corruption.".to_owned(),
            },
            now,
        )?;

        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-replay-unhealthy".to_owned(),
                name: "Replay unhealthy milestone".to_owned(),
            },
            executive_summary: "Replay unhealthy test.".to_owned(),
            goals: vec!["Verify replay idempotency".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Bead completes".to_owned(),
                covered_by: vec!["bead-replay-unhealthy".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: Some("Fixture description.".to_owned()),
                beads: vec![BeadProposal {
                    bead_id: Some("bead-replay-unhealthy".to_owned()),
                    explicit_id: Some(true),
                    title: "Replay bead".to_owned(),
                    description: Some("Fixture description.".to_owned()),
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec!["fixture".to_owned()],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::Minimal,
            agents_guidance: None,
        };
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle,
            now + chrono::Duration::milliseconds(1),
        )?;

        let started_at = now + chrono::Duration::seconds(1);
        milestone_service::record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-replay-unhealthy",
            "proj-replay-unhealthy",
            "run-replay-unhealthy",
            "plan-v1",
            started_at,
        )?;
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base,
            &record.id,
            "bead-replay-unhealthy",
            "proj-replay-unhealthy",
            "workflow execution started",
            started_at,
        )?;

        let show_open = r#"{"id":"bead-replay-unhealthy","title":"Replay bead","status":"open","priority":2,"bead_type":"task"}"#;
        let show_closed = r#"{"id":"bead-replay-unhealthy","title":"Replay bead","status":"closed","priority":2,"bead_type":"task"}"#;
        let read_runner = MockBrRunner::new(vec![
            MockBrRunner::success(show_closed),
            MockBrRunner::success(show_open),
        ]);
        let br_read = BrAdapter::with_runner(read_runner);

        let mutation_runner = MockBrRunner::new(vec![
            MockBrRunner::success(""),
            MockBrRunner::success(""),
            MockBrRunner::success(""),
        ]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let first = reconcile_success(
            &br_mutation,
            &br_read,
            None::<&BvAdapter<MockBvRunner>>,
            base,
            "bead-replay-unhealthy",
            "task-replay-unhealthy",
            "proj-replay-unhealthy",
            "ms-replay-unhealthy",
            "run-replay-unhealthy",
            Some("plan-v1"),
            started_at,
            now + chrono::Duration::seconds(10),
        )
        .await?;
        assert!(!first.was_already_closed);

        std::fs::write(
            base.join(".beads/issues.jsonl"),
            r#"<<<<<<< HEAD
{"id":"bead-replay-unhealthy"}
=======
{"id":"other"}
>>>>>>> branch
"#,
        )?;

        let replay = reconcile_success(
            &br_mutation,
            &br_read,
            None::<&BvAdapter<MockBvRunner>>,
            base,
            "bead-replay-unhealthy",
            "task-replay-unhealthy",
            "proj-replay-unhealthy",
            "ms-replay-unhealthy",
            "run-replay-unhealthy",
            Some("plan-v1"),
            started_at,
            now + chrono::Duration::seconds(11),
        )
        .await?;
        assert!(replay.was_already_closed);

        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &record.id)?
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Completed
        );

        let milestone_id = MilestoneId::new("ms-replay-unhealthy")?;
        let journal = read_journal(&journal_store, base, &milestone_id)?;
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadCompleted)
            .collect();
        assert_eq!(completion_events.len(), 1);

        let task_runs = read_task_runs(&lineage_store, base, &milestone_id)?;
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].run_id.as_deref(), Some("run-replay-unhealthy"));
        assert_eq!(
            task_runs[0].task_id.as_deref(),
            Some("task-replay-unhealthy")
        );

        Ok(())
    }

    /// When the exact attempt was already finalized with a different
    /// outcome_detail (e.g. "first bead completed" from the CLI path),
    /// `update_milestone_state` must succeed by routing through
    /// `repair_task_run_with_disposition` instead of failing with a
    /// mismatched outcome_detail error.
    #[tokio::test]
    async fn reconcile_success_tolerates_already_terminal_with_different_detail(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::{FsMilestonePlanStore, FsMilestoneStore};
        use crate::contexts::milestone_record::bundle::{
            AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
        };
        use crate::contexts::milestone_record::model::{MilestoneId, TaskRunOutcome};
        use crate::contexts::milestone_record::service::{
            create_milestone, persist_plan, record_bead_completion, CreateMilestoneInput,
        };
        use chrono::Utc;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        std::fs::create_dir_all(base.join(".ralph-burning/milestones"))?;

        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;

        let now = Utc::now();

        // 1. Create milestone with one bead
        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "ms-replay".to_owned(),
                name: "Replay test milestone".to_owned(),
                description: "Tests terminal replay tolerance".to_owned(),
            },
            now,
        )?;

        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-replay".to_owned(),
                name: "Replay test milestone".to_owned(),
            },
            executive_summary: "Replay tolerance test.".to_owned(),
            goals: vec!["Validate replay tolerance".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Bead completes".to_owned(),
                covered_by: vec!["bead-replay".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: Some("Fixture description.".to_owned()),
                beads: vec![BeadProposal {
                    bead_id: Some("bead-replay".to_owned()),
                    explicit_id: Some(true),
                    title: "Replay bead".to_owned(),
                    description: Some("Fixture description.".to_owned()),
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec!["fixture".to_owned()],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::Minimal,
            agents_guidance: None,
        };
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle,
            now + chrono::Duration::milliseconds(1),
        )?;

        // 2. Start the bead
        let started_at = now + chrono::Duration::seconds(1);
        milestone_service::record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-replay",
            "proj-replay",
            "run-replay",
            "plan-v1",
            started_at,
        )?;

        // 3. Complete it with a DIFFERENT outcome_detail (simulating CLI path)
        let first_completed_at = now + chrono::Duration::seconds(5);
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &MilestoneId::new("ms-replay")?,
            "bead-replay",
            "proj-replay",
            "run-replay",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("first bead completed"),
            started_at,
            first_completed_at,
        )?;

        // 4. Now run reconcile_success — this will try to write
        //    outcome_detail="task_id=task-replay-456" which differs from
        //    "first bead completed".  Without the fix this would fail with
        //    "already finalized with outcome=succeeded".
        let show_closed = r#"{"id":"bead-replay","title":"Replay bead","status":"closed","priority":2,"bead_type":"task"}"#;
        let read_runner = MockBrRunner::new(vec![MockBrRunner::success(show_closed)]);
        let br_read = BrAdapter::with_runner(read_runner);

        // Already closed, so only sync needed
        let mutation_runner = MockBrRunner::new(vec![MockBrRunner::success("")]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let completed_at = now + chrono::Duration::seconds(10);
        let outcome = reconcile_success(
            &br_mutation,
            &br_read,
            None::<&BvAdapter<MockBvRunner>>,
            base,
            "bead-replay",
            "task-replay-456",
            "proj-replay",
            "ms-replay",
            "run-replay",
            Some("plan-v1"),
            started_at,
            completed_at,
        )
        .await;

        assert!(
            outcome.is_ok(),
            "reconcile_success should succeed via repair path when exact attempt is already terminal with different detail, got: {:?}",
            outcome.err()
        );

        let outcome = outcome.unwrap();
        assert_eq!(outcome.bead_id, "bead-replay");
        assert_eq!(outcome.task_id, "task-replay-456");
        assert!(outcome.was_already_closed);

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_success_replay_after_nonfinal_selection_is_idempotent(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::{
            FsMilestoneControllerStore, FsMilestonePlanStore, FsMilestoneStore,
        };
        use crate::contexts::milestone_record::bundle::{
            AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
        };
        use crate::contexts::milestone_record::controller as milestone_controller;
        use crate::contexts::milestone_record::service::{
            create_milestone, persist_plan, CreateMilestoneInput,
        };
        use chrono::Utc;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        std::fs::create_dir_all(base.join(".ralph-burning/milestones"))?;

        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;

        let now = Utc::now();
        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "ms-replay".to_owned(),
                name: "Replay milestone".to_owned(),
                description: "Replay after nonfinal selection should be idempotent".to_owned(),
            },
            now,
        )?;

        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-replay".to_owned(),
                name: "Replay milestone".to_owned(),
            },
            executive_summary: "Verify replay after selection.".to_owned(),
            goals: vec!["Close one bead and replay".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Both beads covered".to_owned(),
                covered_by: vec!["bead-first".to_owned(), "bead-second".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: Some("Fixture description.".to_owned()),
                beads: vec![
                    BeadProposal {
                        bead_id: Some("bead-first".to_owned()),
                        explicit_id: Some(true),
                        title: "First bead".to_owned(),
                        description: Some("Fixture description.".to_owned()),
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec!["fixture".to_owned()],
                        depends_on: vec![],
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                    BeadProposal {
                        bead_id: Some("bead-second".to_owned()),
                        explicit_id: Some(true),
                        title: "Second bead".to_owned(),
                        description: Some("Fixture description.".to_owned()),
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec!["fixture".to_owned()],
                        depends_on: vec![],
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                ],
            }],
            default_flow: crate::shared::domain::FlowPreset::Minimal,
            agents_guidance: None,
        };
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle,
            now + chrono::Duration::milliseconds(1),
        )?;

        let started_at = now + chrono::Duration::seconds(1);
        milestone_service::record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-first",
            "proj-replay",
            "run-replay",
            "plan-v1",
            started_at,
        )?;

        // First reconciliation: close bead-first, select bead-second.
        let show_open = r#"{"id":"bead-first","title":"First bead","status":"open","priority":2,"bead_type":"task"}"#;
        let show_closed = r#"{"id":"bead-first","title":"First bead","status":"closed","priority":2,"bead_type":"task"}"#;
        let ready_second = serde_json::json!([
            {
                "id": "bead-second",
                "title": "Second bead",
                "priority": 1,
                "bead_type": "task",
                "labels": []
            }
        ])
        .to_string();

        let first_read_runner = MockBrRunner::new(vec![
            MockBrRunner::success(&ready_second),
            MockBrRunner::success(show_open),
        ]);
        let first_br_read = BrAdapter::with_runner(first_read_runner);

        let first_mutation_runner =
            MockBrRunner::new(vec![MockBrRunner::success(""), MockBrRunner::success("")]);
        let first_br_mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(first_mutation_runner));

        let next_hint = r#"{"id":"bead-second","title":"Second bead","score":0.8,"reasons":["ready"],"action":"start"}"#;
        let first_bv_runner = MockBvRunner::new(vec![MockBvRunner::success(next_hint)]);
        let first_bv = BvAdapter::with_runner(first_bv_runner);

        let completed_at = now + chrono::Duration::seconds(10);
        let first_outcome = reconcile_success(
            &first_br_mutation,
            &first_br_read,
            Some(&first_bv),
            base,
            "bead-first",
            "task-replay-001",
            "proj-replay",
            "ms-replay",
            "run-replay",
            Some("plan-v1"),
            started_at,
            completed_at,
        )
        .await?;

        assert_eq!(first_outcome.next_step_selection_warning, None);
        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &record.id)?
                .expect("controller should exist after first reconciliation");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Claimed
        );
        assert_eq!(controller.active_bead_id.as_deref(), Some("bead-second"));

        // Replay: reconcile the same bead-first again. The controller has already
        // advanced to bead-second. This should not fail.
        let replay_read_runner = MockBrRunner::new(vec![MockBrRunner::success(show_closed)]);
        let replay_br_read = BrAdapter::with_runner(replay_read_runner);

        let replay_mutation_runner = MockBrRunner::new(vec![MockBrRunner::success("")]);
        let replay_br_mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(replay_mutation_runner));

        let replay_bv_runner = MockBvRunner::new(vec![MockBvRunner::success(next_hint)]);
        let replay_bv = BvAdapter::with_runner(replay_bv_runner);

        let replay_outcome = reconcile_success(
            &replay_br_mutation,
            &replay_br_read,
            Some(&replay_bv),
            base,
            "bead-first",
            "task-replay-001",
            "proj-replay",
            "ms-replay",
            "run-replay",
            Some("plan-v1"),
            started_at,
            completed_at + chrono::Duration::seconds(1),
        )
        .await;

        assert!(
            replay_outcome.is_ok(),
            "replay should succeed after nonfinal selection: {replay_outcome:?}"
        );

        // Controller should still be on bead-second (the replay must not clobber it).
        let post_replay_controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &record.id)?
                .expect("controller should exist after replay");
        assert_eq!(
            post_replay_controller.state,
            milestone_controller::MilestoneControllerState::Claimed,
            "controller should still be claimed for bead-second after replay"
        );
        assert_eq!(
            post_replay_controller.active_bead_id.as_deref(),
            Some("bead-second"),
            "controller should still track bead-second after replay"
        );

        Ok(())
    }

    /// Two legacy aggregates for the same completion_round but different
    /// created_at: only the latest aggregate's PE amendments should be
    /// reconstructed.
    #[test]
    fn reconstruct_pe_mappings_deduplicates_by_round_latest_wins(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::FileSystem;
        use crate::contexts::project_run_record::model::PayloadRecord;
        use crate::contexts::workflow_composition::panel_contracts::{
            FinalReviewAggregatePayload, FinalReviewCanonicalAmendment, RecordKind,
        };
        use chrono::Utc;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();

        // Set up .git dir so FileSystem::project_root resolves
        let git_dir = base.join(".git");
        std::fs::create_dir_all(&git_dir)?;

        let project_id = ProjectId::new("proj-dedup")?;
        let project_root = FileSystem::project_root(base, &project_id);
        let payloads_dir = project_root.join("history/payloads");
        std::fs::create_dir_all(&payloads_dir)?;

        // Also create milestone dir for journal writes during reconstruction
        let milestone_id = MilestoneId::new("ms-dedup")?;
        let milestone_root = base
            .join(".ralph-burning/milestones")
            .join(milestone_id.as_str());
        std::fs::create_dir_all(&milestone_root)?;

        let now = Utc::now();

        // Build two aggregates for the same completion_round (5) with
        // different created_at timestamps and different PE amendments.
        let make_aggregate = |mapped_to: &str, body: &str| -> FinalReviewAggregatePayload {
            FinalReviewAggregatePayload {
                restart_required: false,
                force_completed: false,
                total_reviewers: 1,
                total_proposed_amendments: 1,
                unique_amendment_count: 1,
                accepted_amendment_ids: vec!["a1".to_owned()],
                rejected_amendment_ids: vec![],
                disputed_amendment_ids: vec![],
                amendments: vec![],
                final_accepted_amendments: vec![FinalReviewCanonicalAmendment {
                    amendment_id: "a1".to_owned(),
                    normalized_body: body.to_owned(),
                    sources: vec![],
                    mapped_to_bead_id: Some(mapped_to.to_owned()),
                    covered_by_bead_id: Some(mapped_to.to_owned()),
                    classification: crate::contexts::workflow_composition::panel_contracts::AmendmentClassification::FixCurrentBead,
                    rationale: None,
                    proposed_title: None,
                    proposed_scope: None,
                    proposed_bead_summary: None,
                    severity: None,
                }],
                final_review_restart_count: 0,
                max_restarts: 3,
                summary: "test".to_owned(),
                exhausted_count: 0,
                probe_exhausted_count: 0,
                effective_min_reviewers: 1,
            }
        };

        let earlier = now - chrono::Duration::seconds(60);
        let later = now - chrono::Duration::seconds(10);

        // Earlier aggregate (pre-rollback): maps to "old-bead"
        let payload_old = PayloadRecord {
            payload_id: "run-dedup-final_review-aggregate-c1-a1-cr5-old-payload".to_owned(),
            stage_id: StageId::FinalReview,
            cycle: 1,
            attempt: 1,
            created_at: earlier,
            payload: serde_json::to_value(make_aggregate("old-bead", "old finding"))?,
            record_kind: RecordKind::StageAggregate,
            producer: None,
            completion_round: 5,
        };

        // Later aggregate (post-rollback): maps to "new-bead"
        let payload_new = PayloadRecord {
            payload_id: "run-dedup-final_review-aggregate-c1-a1-cr5-rb1-payload".to_owned(),
            stage_id: StageId::FinalReview,
            cycle: 1,
            attempt: 1,
            created_at: later,
            payload: serde_json::to_value(make_aggregate("new-bead", "new finding"))?,
            record_kind: RecordKind::StageAggregate,
            producer: None,
            completion_round: 5,
        };

        // Write both payload files
        std::fs::write(
            payloads_dir.join("old-aggregate.json"),
            serde_json::to_string(&payload_old)?,
        )?;
        std::fs::write(
            payloads_dir.join("new-aggregate.json"),
            serde_json::to_string(&payload_new)?,
        )?;

        let (reconstructed, authoritative_max_round) = reconstruct_missing_pe_mappings(
            base,
            "proj-dedup",
            "active-bead",
            &milestone_id,
            &[],         // no existing mappings
            "run-dedup", // run_id — must match payload_id prefix with "-"
        );

        assert_eq!(authoritative_max_round, Some(5));
        assert_eq!(
            reconstructed.len(),
            1,
            "should reconstruct exactly one mapping from the latest aggregate"
        );
        assert_eq!(
            reconstructed[0].mapped_to_bead_id, "new-bead",
            "should use amendment from the later aggregate"
        );
        assert_eq!(
            reconstructed[0].finding_summary, "new finding",
            "should use body from the later aggregate"
        );

        Ok(())
    }

    #[test]
    fn reconstruct_pe_mappings_ignores_new_covered_by_classification(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::FileSystem;
        use crate::contexts::project_run_record::model::PayloadRecord;
        use crate::contexts::workflow_composition::panel_contracts::{
            FinalReviewAggregatePayload, FinalReviewCanonicalAmendment, RecordKind,
        };
        use chrono::Utc;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;

        let project_id = ProjectId::new("proj-covered-deferred")?;
        let project_root = FileSystem::project_root(base, &project_id);
        let payloads_dir = project_root.join("history/payloads");
        std::fs::create_dir_all(&payloads_dir)?;

        let milestone_id = MilestoneId::new("ms-covered-deferred")?;
        let now = Utc::now();
        let aggregate = FinalReviewAggregatePayload {
            restart_required: true,
            force_completed: false,
            total_reviewers: 1,
            total_proposed_amendments: 1,
            unique_amendment_count: 1,
            accepted_amendment_ids: vec!["a1".to_owned()],
            rejected_amendment_ids: vec![],
            disputed_amendment_ids: vec![],
            amendments: vec![],
            final_accepted_amendments: vec![FinalReviewCanonicalAmendment {
                amendment_id: "a1".to_owned(),
                normalized_body: "covered by another bead".to_owned(),
                sources: vec![],
                mapped_to_bead_id: Some("existing-bead".to_owned()),
                covered_by_bead_id: Some("existing-bead".to_owned()),
                classification: AmendmentClassification::CoveredByExistingBead,
                rationale: None,
                proposed_title: None,
                proposed_scope: None,
                proposed_bead_summary: None,
                severity: None,
            }],
            final_review_restart_count: 1,
            max_restarts: 3,
            summary: "test".to_owned(),
            exhausted_count: 0,
            probe_exhausted_count: 0,
            effective_min_reviewers: 1,
        };

        let payload = PayloadRecord {
            payload_id: "run-covered-deferred-final_review-aggregate-c1-a1-cr2-payload".to_owned(),
            stage_id: StageId::FinalReview,
            cycle: 1,
            attempt: 1,
            created_at: now,
            payload: serde_json::to_value(&aggregate)?,
            record_kind: RecordKind::StageAggregate,
            producer: None,
            completion_round: 2,
        };
        std::fs::write(
            payloads_dir.join("aggregate.json"),
            serde_json::to_string(&payload)?,
        )?;

        let (reconstructed, authoritative_max_round) = reconstruct_missing_pe_mappings(
            base,
            "proj-covered-deferred",
            "active-bead",
            &milestone_id,
            &[],
            "run-covered-deferred",
        );

        assert_eq!(authoritative_max_round, Some(2));
        assert!(
            reconstructed.is_empty(),
            "new covered_by_existing_bead contract metadata must not activate planned-elsewhere routing yet"
        );

        Ok(())
    }

    #[test]
    fn reconstruct_pe_mappings_preserves_legacy_planned_elsewhere_classification(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::adapters::fs::FileSystem;
        use crate::contexts::project_run_record::model::PayloadRecord;
        use crate::contexts::workflow_composition::panel_contracts::RecordKind;
        use chrono::Utc;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;

        let project_id = ProjectId::new("proj-legacy-pe")?;
        let project_root = FileSystem::project_root(base, &project_id);
        let payloads_dir = project_root.join("history/payloads");
        std::fs::create_dir_all(&payloads_dir)?;

        let milestone_id = MilestoneId::new("ms-legacy-pe")?;
        let now = Utc::now();
        let aggregate = serde_json::json!({
            "restart_required": false,
            "force_completed": false,
            "total_reviewers": 1,
            "total_proposed_amendments": 1,
            "unique_amendment_count": 1,
            "accepted_amendment_ids": ["a1"],
            "rejected_amendment_ids": [],
            "disputed_amendment_ids": [],
            "amendments": [],
            "final_accepted_amendments": [
                {
                    "amendment_id": "a1",
                    "normalized_body": "legacy planned elsewhere",
                    "sources": [],
                    "mapped_to_bead_id": "existing-bead",
                    "classification": "planned-elsewhere"
                }
            ],
            "final_review_restart_count": 0,
            "max_restarts": 3,
            "summary": "legacy aggregate",
            "exhausted_count": 0,
            "probe_exhausted_count": 0,
            "effective_min_reviewers": 1
        });

        let payload = PayloadRecord {
            payload_id: "run-legacy-pe-final_review-aggregate-c1-a1-cr1-payload".to_owned(),
            stage_id: StageId::FinalReview,
            cycle: 1,
            attempt: 1,
            created_at: now,
            payload: aggregate,
            record_kind: RecordKind::StageAggregate,
            producer: None,
            completion_round: 1,
        };
        std::fs::write(
            payloads_dir.join("aggregate.json"),
            serde_json::to_string(&payload)?,
        )?;

        let (reconstructed, authoritative_max_round) = reconstruct_missing_pe_mappings(
            base,
            "proj-legacy-pe",
            "active-bead",
            &milestone_id,
            &[],
            "run-legacy-pe",
        );

        assert_eq!(authoritative_max_round, Some(1));
        assert_eq!(reconstructed.len(), 1);
        assert_eq!(reconstructed[0].finding_summary, "legacy planned elsewhere");
        assert_eq!(reconstructed[0].mapped_to_bead_id, "existing-bead");

        Ok(())
    }

    fn terminal_aggregate(
        amendments: Vec<FinalReviewCanonicalAmendment>,
    ) -> FinalReviewAggregatePayload {
        FinalReviewAggregatePayload {
            restart_required: false,
            force_completed: false,
            total_reviewers: 1,
            total_proposed_amendments: amendments.len(),
            unique_amendment_count: amendments.len(),
            accepted_amendment_ids: amendments
                .iter()
                .map(|amendment| amendment.amendment_id.clone())
                .collect(),
            rejected_amendment_ids: vec![],
            disputed_amendment_ids: vec![],
            amendments: vec![],
            final_accepted_amendments: amendments,
            final_review_restart_count: 0,
            max_restarts: 3,
            summary: "terminal review".to_owned(),
            exhausted_count: 0,
            probe_exhausted_count: 0,
            effective_min_reviewers: 1,
        }
    }

    fn terminal_amendment(
        amendment_id: &str,
        classification: AmendmentClassification,
        body: &str,
    ) -> FinalReviewCanonicalAmendment {
        FinalReviewCanonicalAmendment {
            amendment_id: amendment_id.to_owned(),
            normalized_body: body.to_owned(),
            sources: vec![],
            mapped_to_bead_id: None,
            covered_by_bead_id: None,
            classification,
            rationale: None,
            proposed_title: None,
            proposed_scope: None,
            proposed_bead_summary: None,
            severity: None,
        }
    }

    fn final_review_source(
        reviewer_id: &str,
        model_id: &str,
    ) -> crate::contexts::workflow_composition::panel_contracts::FinalReviewAmendmentSource {
        crate::contexts::workflow_composition::panel_contracts::FinalReviewAmendmentSource {
            reviewer_id: reviewer_id.to_owned(),
            backend_family: "stub".to_owned(),
            model_id: model_id.to_owned(),
        }
    }

    fn final_review_sources(
        specs: &[(&str, &str)],
    ) -> Vec<crate::contexts::workflow_composition::panel_contracts::FinalReviewAmendmentSource>
    {
        specs
            .iter()
            .map(|(reviewer_id, model_id)| final_review_source(reviewer_id, model_id))
            .collect()
    }

    fn write_terminal_aggregate_round(
        base: &Path,
        project_id: &str,
        run_id: &str,
        completion_round: u32,
        aggregate: &FinalReviewAggregatePayload,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let project_id = ProjectId::new(project_id)?;
        let project_root = FileSystem::project_root(base, &project_id);
        let payloads_dir = project_root.join("history/payloads");
        std::fs::create_dir_all(&payloads_dir)?;
        let payload_id =
            format!("{run_id}-final_review-aggregate-c1-a1-cr{completion_round}-payload");
        let payload = PayloadRecord {
            payload_id: payload_id.clone(),
            stage_id: StageId::FinalReview,
            cycle: 1,
            attempt: 1,
            created_at: Utc::now(),
            payload: serde_json::to_value(aggregate)?,
            record_kind: RecordKind::StageAggregate,
            producer: None,
            completion_round,
        };
        std::fs::write(
            payloads_dir.join(format!("{payload_id}.json")),
            serde_json::to_string(&payload)?,
        )?;
        Ok(())
    }

    fn write_terminal_aggregate(
        base: &Path,
        project_id: &str,
        run_id: &str,
        aggregate: &FinalReviewAggregatePayload,
    ) -> Result<(), Box<dyn std::error::Error>> {
        write_terminal_aggregate_round(base, project_id, run_id, 1, aggregate)
    }

    fn write_review_payload(
        base: &Path,
        project_id: &str,
        run_id: &str,
        classified_findings: Vec<ClassifiedFinding>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        write_review_payload_with_id(
            base,
            project_id,
            &format!("{run_id}-review-c1-a1-cr1-payload"),
            classified_findings,
        )
    }

    fn write_review_payload_with_id(
        base: &Path,
        project_id: &str,
        payload_id: &str,
        classified_findings: Vec<ClassifiedFinding>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let project_id = ProjectId::new(project_id)?;
        let project_root = FileSystem::project_root(base, &project_id);
        let payloads_dir = project_root.join("history/payloads");
        std::fs::create_dir_all(&payloads_dir)?;
        let payload = PayloadRecord {
            payload_id: payload_id.to_owned(),
            stage_id: StageId::Review,
            cycle: 1,
            attempt: 1,
            created_at: Utc::now(),
            payload: serde_json::to_value(StagePayload::Validation(ValidationPayload {
                outcome: ReviewOutcome::RequestChanges,
                evidence: vec!["review evidence".to_owned()],
                findings_or_gaps: vec![],
                follow_up_or_amendments: vec![],
                classified_findings,
            }))?,
            record_kind: RecordKind::StagePrimary,
            producer: None,
            completion_round: 1,
        };
        std::fs::write(
            payloads_dir.join(format!("{payload_id}.json")),
            serde_json::to_string(&payload)?,
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn classification_fix_current_bead_is_noop() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let aggregate = terminal_aggregate(vec![terminal_amendment(
            "a1",
            AmendmentClassification::FixCurrentBead,
            "fix now",
        )]);
        write_terminal_aggregate(base, "proj-fix-noop", "run-fix-noop", &aggregate)?;

        let br_mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let br_read = BrAdapter::with_runner(MockBrRunner::new(vec![]));
        let summary = reconcile_terminal_review_classifications(
            &br_mutation,
            &br_read,
            base,
            "active-bead",
            "task-fix",
            "proj-fix-noop",
            "run-fix-noop",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 0);
        assert_eq!(summary.records_written, 0);
        assert!(
            !FileSystem::project_root(base, &ProjectId::new("proj-fix-noop")?)
                .join("proposed-beads.ndjson")
                .exists()
        );
        Ok(())
    }

    #[tokio::test]
    async fn classification_covered_by_existing_bead_writes_comment(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        write_beads_export(base)?;
        let mut amendment = terminal_amendment(
            "amend-covered",
            AmendmentClassification::CoveredByExistingBead,
            "target already\nowns this\nwith context",
        );
        amendment.covered_by_bead_id = Some("target-bead".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(base, "proj-covered", "run-covered", &aggregate)?;

        let read_json = r#"{"id":"target-bead","title":"Target","status":"open","priority":2,"bead_type":"task","comments":[]}"#;
        let read =
            BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success(read_json)]));
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let mutation_runner = RecordingBrRunner::new(
            vec![
                MockBrRunner::success("synced"),
                MockBrRunner::success("commented"),
            ],
            invocations.clone(),
            None,
        );
        let mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-covered",
            "proj-covered",
            "run-covered",
        )
        .await?;

        let calls = invocations.lock().unwrap();
        let comment_calls: Vec<_> = calls
            .iter()
            .filter(|args| {
                args.first().is_some_and(|arg| arg == "comments")
                    && args.get(1).is_some_and(|arg| arg == "add")
            })
            .collect();
        assert_eq!(comment_calls.len(), 1);
        let comment_text = comment_calls[0].last().expect("comment text");
        assert!(comment_text.contains("active-bead"));
        assert!(comment_text.contains("run_id=run-covered"));
        assert!(comment_text.contains("amendment_id=amend-covered"));
        assert!(comment_text.contains("Finding: target already owns this with context"));
        assert!(comment_text.lines().count() <= 3);
        Ok(())
    }

    #[tokio::test]
    async fn classification_covered_by_existing_bead_substring_marker_does_not_skip(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        write_beads_export(base)?;
        let mut amendment = terminal_amendment(
            "amend-covered",
            AmendmentClassification::CoveredByExistingBead,
            "target already owns this",
        );
        amendment.covered_by_bead_id = Some("target-bead".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(
            base,
            "proj-covered-substring",
            "run-covered-substring",
            &aggregate,
        )?;

        let read_json = r#"{"id":"target-bead","title":"Target","status":"open","priority":2,"bead_type":"task","comments":[{"id":1,"issue_id":"target-bead","author":"agent","text":"unrelated prose mentions amend-covered but is not the reconciliation marker"}]}"#;
        let read =
            BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success(read_json)]));
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let mutation_runner = RecordingBrRunner::new(
            vec![
                MockBrRunner::success("synced"),
                MockBrRunner::success("commented"),
            ],
            invocations.clone(),
            None,
        );
        let mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-covered-substring",
            "proj-covered-substring",
            "run-covered-substring",
        )
        .await?;

        let calls = invocations.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|args| {
                    args.first().is_some_and(|arg| arg == "comments")
                        && args.get(1).is_some_and(|arg| arg == "add")
                })
                .count(),
            1
        );
        Ok(())
    }

    #[tokio::test]
    async fn classification_covered_by_existing_bead_exact_marker_is_idempotent(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        write_beads_export(base)?;
        let mut amendment = terminal_amendment(
            "amend-covered",
            AmendmentClassification::CoveredByExistingBead,
            "target already owns this",
        );
        amendment.covered_by_bead_id = Some("target-bead".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(base, "proj-covered-exact", "run-covered-exact", &aggregate)?;

        let read_json = r#"{"id":"target-bead","title":"Target","status":"open","priority":2,"bead_type":"task","comments":[{"id":1,"issue_id":"target-bead","author":"agent","text":"Review finding from bead active-bead; source run_id=run-covered-exact\namendment_id=amend-covered\nFinding: target already owns this"}]}"#;
        let read =
            BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success(read_json)]));
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let mutation_runner = RecordingBrRunner::new(vec![], invocations.clone(), None);
        let mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-covered-exact",
            "proj-covered-exact",
            "run-covered-exact",
        )
        .await?;

        assert!(invocations.lock().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn classification_covered_by_existing_bead_skips_current_bead_target(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        write_beads_export(base)?;
        let mut amendment = terminal_amendment(
            "amend-self-target",
            AmendmentClassification::CoveredByExistingBead,
            "target points back at the active bead",
        );
        amendment.covered_by_bead_id = Some("active-bead".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(base, "proj-covered-self", "run-covered-self", &aggregate)?;

        let read = BrAdapter::with_runner(MockBrRunner::new(vec![]));
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let mutation_runner = RecordingBrRunner::new(vec![], invocations.clone(), None);
        let mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-covered-self",
            "proj-covered-self",
            "run-covered-self",
        )
        .await?;

        assert!(
            invocations.lock().unwrap().is_empty(),
            "self-target covered_by_existing_bead must not mutate the current bead"
        );
        Ok(())
    }

    #[tokio::test]
    async fn classification_covered_by_existing_bead_skips_resolved_current_bead_alias(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        write_beads_export(base)?;
        let mut amendment = terminal_amendment(
            "amend-self-alias",
            AmendmentClassification::CoveredByExistingBead,
            "target alias resolves back to the active bead",
        );
        amendment.covered_by_bead_id = Some("current-bead-alias".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(
            base,
            "proj-covered-self-alias",
            "run-covered-self-alias",
            &aggregate,
        )?;

        let read_json = r#"{"id":"active-bead","title":"Active","status":"open","priority":2,"bead_type":"task","comments":[]}"#;
        let read =
            BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success(read_json)]));
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let mutation_runner = RecordingBrRunner::new(vec![], invocations.clone(), None);
        let mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-covered-self-alias",
            "proj-covered-self-alias",
            "run-covered-self-alias",
        )
        .await?;

        assert!(
            invocations.lock().unwrap().is_empty(),
            "resolved self-target alias must not mutate the current bead"
        );
        Ok(())
    }

    #[tokio::test]
    async fn classification_covered_by_existing_bead_exact_marker_ignores_foreign_pending_mutation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        write_beads_export(base)?;
        let pending_dir = base.join(".beads/.br-unsynced-mutations.d");
        std::fs::create_dir_all(&pending_dir)?;
        std::fs::write(
            pending_dir.join("other-workflow.json"),
            r#"{"adapter_id":"other-workflow","operation":"create_bead","bead_id":"foreign-bead","status":null}"#,
        )?;
        let mut amendment = terminal_amendment(
            "amend-covered",
            AmendmentClassification::CoveredByExistingBead,
            "target already owns this",
        );
        amendment.covered_by_bead_id = Some("target-bead".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(
            base,
            "proj-covered-foreign-pending",
            "run-covered-foreign-pending",
            &aggregate,
        )?;

        let read_json = r#"{"id":"target-bead","title":"Target","status":"open","priority":2,"bead_type":"task","comments":[{"id":1,"issue_id":"target-bead","author":"agent","text":"Review finding from bead active-bead; source run_id=run-covered-foreign-pending\namendment_id=amend-covered\nFinding: target already owns this"}]}"#;
        let read =
            BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success(read_json)]));
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let mutation_runner = RecordingBrRunner::new(vec![], invocations.clone(), None);
        let mutation = BrMutationAdapter::with_adapter_id(
            BrAdapter::with_runner(mutation_runner).with_working_dir(base.to_path_buf()),
            "classification-reconcile-owner",
        );

        reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-covered-foreign-pending",
            "proj-covered-foreign-pending",
            "run-covered-foreign-pending",
        )
        .await?;

        assert!(
            invocations.lock().unwrap().is_empty(),
            "pure exact-marker replay must not try to flush unrelated pending br mutations"
        );
        Ok(())
    }

    #[tokio::test]
    async fn classification_covered_by_existing_bead_exact_marker_ignores_own_non_comment_pending_mutation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        write_beads_export(base)?;
        let pending_dir = base.join(".beads/.br-unsynced-mutations.d");
        std::fs::create_dir_all(&pending_dir)?;
        std::fs::write(
            pending_dir.join("classification-reconcile-owner.json"),
            r#"{"adapter_id":"classification-reconcile-owner","operation":"update_bead_status","bead_id":"active-bead","status":"closed"}"#,
        )?;
        let mut amendment = terminal_amendment(
            "amend-covered",
            AmendmentClassification::CoveredByExistingBead,
            "target already owns this",
        );
        amendment.covered_by_bead_id = Some("target-bead".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(
            base,
            "proj-covered-own-status-pending",
            "run-covered-own-status-pending",
            &aggregate,
        )?;

        let read_json = r#"{"id":"target-bead","title":"Target","status":"open","priority":2,"bead_type":"task","comments":[{"id":1,"issue_id":"target-bead","author":"agent","text":"Review finding from bead active-bead; source run_id=run-covered-own-status-pending\namendment_id=amend-covered\nFinding: target already owns this"}]}"#;
        let read =
            BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success(read_json)]));
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let mutation_runner = RecordingBrRunner::new(vec![], invocations.clone(), None);
        let mutation = BrMutationAdapter::with_adapter_id(
            BrAdapter::with_runner(mutation_runner).with_working_dir(base.to_path_buf()),
            "classification-reconcile-owner",
        );

        reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-covered-own-status-pending",
            "proj-covered-own-status-pending",
            "run-covered-own-status-pending",
        )
        .await?;

        assert!(
            invocations.lock().unwrap().is_empty(),
            "pure exact-marker replay must not flush same-adapter non-comment pending mutations"
        );
        Ok(())
    }

    #[tokio::test]
    async fn classification_covered_by_existing_bead_marker_retry_flushes_pending_comment(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        write_beads_export(base)?;
        let mut amendment = terminal_amendment(
            "amend-covered",
            AmendmentClassification::CoveredByExistingBead,
            "target already owns this",
        );
        amendment.covered_by_bead_id = Some("target-bead".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(base, "proj-covered-retry", "run-covered-retry", &aggregate)?;

        let without_marker = r#"{"id":"target-bead","title":"Target","status":"open","priority":2,"bead_type":"task","comments":[]}"#;
        let with_marker = r#"{"id":"target-bead","title":"Target","status":"open","priority":2,"bead_type":"task","comments":[{"id":1,"issue_id":"target-bead","author":"agent","text":"Review finding from bead active-bead; source run_id=run-covered-retry\namendment_id=amend-covered\nFinding: target already owns this"}]}"#;
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![
            MockBrRunner::success(with_marker),
            MockBrRunner::success(without_marker),
        ]));
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let mutation_runner = RecordingBrRunner::new(
            vec![
                MockBrRunner::success("synced on retry"),
                MockBrRunner::error(1, "sync failed"),
                MockBrRunner::success("commented"),
            ],
            invocations.clone(),
            None,
        );
        let mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let first_error = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-covered-retry",
            "proj-covered-retry",
            "run-covered-retry",
        )
        .await
        .expect_err("first sync failure should fail reconciliation");
        assert!(first_error.to_string().contains("failed to flush"));

        reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-covered-retry",
            "proj-covered-retry",
            "run-covered-retry",
        )
        .await?;

        let calls = invocations.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|args| {
                    args.first().is_some_and(|arg| arg == "comments")
                        && args.get(1).is_some_and(|arg| arg == "add")
                })
                .count(),
            1,
            "retry must not post a duplicate comment when the marker is already visible"
        );
        assert_eq!(
            calls
                .iter()
                .filter(|args| {
                    args.first().is_some_and(|arg| arg == "sync")
                        && args.iter().any(|arg| arg == "--flush-only")
                })
                .count(),
            2,
            "retry must flush the pending comment even though exact-marker idempotency skips reposting"
        );
        Ok(())
    }

    #[tokio::test]
    async fn classification_covered_by_existing_bead_skips_missing_target(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let mut amendment = terminal_amendment(
            "amend-missing",
            AmendmentClassification::CoveredByExistingBead,
            "target missing",
        );
        amendment.covered_by_bead_id = Some("missing-bead".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(base, "proj-missing", "run-missing", &aggregate)?;

        let read = BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::error(
            1,
            "bead not found",
        )]));
        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));

        reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-missing",
            "proj-missing",
            "run-missing",
        )
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn classification_covered_by_existing_bead_non_bead_not_found_returns_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let mut amendment = terminal_amendment(
            "amend-path-missing",
            AmendmentClassification::CoveredByExistingBead,
            "target may exist but storage lookup failed",
        );
        amendment.covered_by_bead_id = Some("target-bead".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(base, "proj-path-missing", "run-path-missing", &aggregate)?;

        let read = BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::error(
            1,
            "database path not found: .beads/issues.jsonl",
        )]));
        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));

        let error = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-path-missing",
            "proj-path-missing",
            "run-path-missing",
        )
        .await
        .expect_err("non-bead not-found errors must not be treated as a missing target");

        assert!(error.to_string().contains("failed to inspect"));
        Ok(())
    }

    #[tokio::test]
    async fn classification_covered_by_existing_bead_checks_beads_health_before_comment(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        std::fs::create_dir_all(base.join(".beads"))?;
        std::fs::write(
            base.join(".beads/issues.jsonl"),
            "<<<<<<< ours\n{\"id\":\"target-bead\"}\n=======\n{\"id\":\"other\"}\n>>>>>>> theirs\n",
        )?;
        let mut amendment = terminal_amendment(
            "amend-conflicted",
            AmendmentClassification::CoveredByExistingBead,
            "target already owns this",
        );
        amendment.covered_by_bead_id = Some("target-bead".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(
            base,
            "proj-covered-conflicted",
            "run-covered-conflicted",
            &aggregate,
        )?;

        let read_json = r#"{"id":"target-bead","title":"Target","status":"open","priority":2,"bead_type":"task","comments":[]}"#;
        let read =
            BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success(read_json)]));
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let mutation_runner = RecordingBrRunner::new(vec![], invocations.clone(), None);
        let mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let error = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-covered-conflicted",
            "proj-covered-conflicted",
            "run-covered-conflicted",
        )
        .await
        .expect_err("unsafe beads export must block comment mutation");

        assert!(error.to_string().contains("refusing to mutate beads"));
        assert!(
            invocations.lock().unwrap().is_empty(),
            "covered-by reconciliation must not call br comments add before health passes"
        );
        Ok(())
    }

    #[tokio::test]
    async fn classification_covered_by_existing_bead_comment_failure_returns_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        write_beads_export(base)?;
        let mut amendment = terminal_amendment(
            "amend-comment-fail",
            AmendmentClassification::CoveredByExistingBead,
            "target already owns this",
        );
        amendment.covered_by_bead_id = Some("target-bead".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(base, "proj-comment-fail", "run-comment-fail", &aggregate)?;

        let read_json = r#"{"id":"target-bead","title":"Target","status":"open","priority":2,"bead_type":"task","comments":[]}"#;
        let read =
            BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success(read_json)]));
        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![
                MockBrRunner::error(1, "permission denied"),
            ])));

        let error = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-comment-fail",
            "proj-comment-fail",
            "run-comment-fail",
        )
        .await
        .expect_err("comment failure should fail reconciliation");

        assert!(error.to_string().contains("failed to post"));
        Ok(())
    }

    #[tokio::test]
    async fn classification_covered_by_existing_bead_sync_failure_returns_error(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        write_beads_export(base)?;
        let mut amendment = terminal_amendment(
            "amend-sync-fail",
            AmendmentClassification::CoveredByExistingBead,
            "target already owns this",
        );
        amendment.covered_by_bead_id = Some("target-bead".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(base, "proj-sync-fail", "run-sync-fail", &aggregate)?;

        let read_json = r#"{"id":"target-bead","title":"Target","status":"open","priority":2,"bead_type":"task","comments":[]}"#;
        let read =
            BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success(read_json)]));
        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![
                MockBrRunner::error(1, "sync failed"),
                MockBrRunner::success("commented"),
            ])));

        let error = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-sync-fail",
            "proj-sync-fail",
            "run-sync-fail",
        )
        .await
        .expect_err("sync failure should fail reconciliation");

        assert!(error.to_string().contains("failed to flush"));
        Ok(())
    }

    #[tokio::test]
    async fn classification_propose_new_bead_below_threshold_writes_no_record(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let mut amendment = terminal_amendment(
            "a1",
            AmendmentClassification::ProposeNewBead,
            "missing work",
        );
        amendment.proposed_bead_summary = Some("Add retry telemetry".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(base, "proj-propose-low", "run-propose-low", &aggregate)?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![]));
        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-propose-low",
            "proj-propose-low",
            "run-propose-low",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 1);
        assert_eq!(summary.records_written, 0);
        assert!(
            !FileSystem::project_root(base, &ProjectId::new("proj-propose-low")?)
                .join("proposed-beads.ndjson")
                .exists()
        );
        let journal =
            FsMilestoneJournalStore.read_journal(base, &MilestoneId::new("task-propose-low")?)?;
        let pending = journal
            .iter()
            .find(|event| {
                event
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("sub_type"))
                    .and_then(|value| value.as_str())
                    == Some("propose_new_bead_pending")
            })
            .expect("pending event");
        let metadata = pending.metadata.as_ref().expect("metadata");
        assert_eq!(metadata["current_count"], 1);
        assert_eq!(metadata["threshold_count"], 2);
        assert_eq!(metadata["proposed_title"], "Add retry telemetry");
        assert_eq!(metadata["existing_bead_lookup_ran"], false);
        Ok(())
    }

    #[tokio::test]
    async fn classification_propose_new_bead_below_threshold_pending_event_is_idempotent(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let mut amendment = terminal_amendment(
            "a1",
            AmendmentClassification::ProposeNewBead,
            "missing work",
        );
        amendment.proposed_bead_summary = Some("Add retry telemetry".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(
            base,
            "proj-propose-low-idempotent",
            "run-propose-low-idempotent",
            &aggregate,
        )?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![]));
        for _ in 0..2 {
            let summary = reconcile_terminal_review_classifications(
                &mutation,
                &read,
                base,
                "active-bead",
                "task-propose-low-idempotent",
                "proj-propose-low-idempotent",
                "run-propose-low-idempotent",
            )
            .await?;
            assert_eq!(summary.amendments_processed, 1);
            assert_eq!(summary.records_written, 0);
        }

        let journal = FsMilestoneJournalStore
            .read_journal(base, &MilestoneId::new("task-propose-low-idempotent")?)?;
        let pending_count = journal
            .iter()
            .filter(|event| {
                event
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("sub_type"))
                    .and_then(|value| value.as_str())
                    == Some("propose_new_bead_pending")
            })
            .count();
        assert_eq!(pending_count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn classification_propose_new_bead_below_threshold_does_not_read_existing_records(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let mut amendment = terminal_amendment(
            "a1",
            AmendmentClassification::ProposeNewBead,
            "missing work",
        );
        amendment.proposed_bead_summary = Some("Add retry telemetry".to_owned());
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(
            base,
            "proj-propose-low-unreadable",
            "run-propose-low-unreadable",
            &aggregate,
        )?;
        let proposed_path =
            FileSystem::project_root(base, &ProjectId::new("proj-propose-low-unreadable")?)
                .join("proposed-beads.ndjson");
        std::fs::create_dir_all(&proposed_path)?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![]));
        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-propose-low-unreadable",
            "proj-propose-low-unreadable",
            "run-propose-low-unreadable",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 1);
        assert_eq!(summary.records_written, 0);
        assert!(
            proposed_path.is_dir(),
            "below-threshold proposals must not open the existing record path"
        );
        Ok(())
    }

    #[tokio::test]
    async fn classification_non_proposal_does_not_read_existing_proposal_records(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let aggregate = terminal_aggregate(vec![terminal_amendment(
            "a1",
            AmendmentClassification::FixCurrentBead,
            "fix now",
        )]);
        write_terminal_aggregate(
            base,
            "proj-fix-unreadable-proposals",
            "run-fix-unreadable-proposals",
            &aggregate,
        )?;
        let proposed_path =
            FileSystem::project_root(base, &ProjectId::new("proj-fix-unreadable-proposals")?)
                .join("proposed-beads.ndjson");
        std::fs::create_dir_all(&proposed_path)?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![]));
        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-fix-unreadable-proposals",
            "proj-fix-unreadable-proposals",
            "run-fix-unreadable-proposals",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 0);
        assert_eq!(summary.records_written, 0);
        assert!(
            proposed_path.is_dir(),
            "non-proposal findings must not open the existing record path"
        );
        Ok(())
    }

    #[tokio::test]
    async fn classification_propose_new_bead_at_threshold_writes_one_record(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let mut first = terminal_amendment(
            "a1",
            AmendmentClassification::ProposeNewBead,
            "missing work 1",
        );
        first.proposed_bead_summary = Some(" Add retry telemetry ".to_owned());
        first.sources = vec![final_review_source("reviewer-a", "a")];
        let mut second = terminal_amendment(
            "a2",
            AmendmentClassification::ProposeNewBead,
            "missing work 2",
        );
        second.proposed_bead_summary = Some("add retry telemetry".to_owned());
        second.sources = vec![final_review_source("reviewer-b", "b")];
        let aggregate = terminal_aggregate(vec![first, second]);
        write_terminal_aggregate(base, "proj-propose-hit", "run-propose-hit", &aggregate)?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success("[]")]));
        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-propose-hit",
            "proj-propose-hit",
            "run-propose-hit",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 2);
        assert_eq!(summary.records_written, 1);
        let path = FileSystem::project_root(base, &ProjectId::new("proj-propose-hit")?)
            .join("proposed-beads.ndjson");
        let lines = std::fs::read_to_string(path)?;
        let records: Vec<serde_json::Value> = lines
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<_, _>>()?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["source_run_id"], "run-propose-hit");
        assert_eq!(records[0]["current_bead_id"], "active-bead");
        assert_eq!(records[0]["summary"], "Add retry telemetry");
        assert_eq!(records[0]["count"], 2);
        assert_eq!(records[0]["amendment_id"], "a1");
        let journal =
            FsMilestoneJournalStore.read_journal(base, &MilestoneId::new("task-propose-hit")?)?;
        let created = journal
            .iter()
            .find(|event| {
                event
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("sub_type"))
                    .and_then(|value| value.as_str())
                    == Some("propose_new_bead_created")
            })
            .expect("created event");
        assert_eq!(created.event_type, MilestoneEventType::ProposedBeadCreated);
        let metadata = created.metadata.as_ref().expect("metadata");
        assert_eq!(metadata["proposed_title"], "Add retry telemetry");
        assert_eq!(metadata["proposed_scope"], "missing work 1\nmissing work 2");
        assert_eq!(metadata["severity"], "unknown");
        assert_eq!(metadata["current_count"], 2);
        assert_eq!(metadata["threshold_count"], 2);
        assert_eq!(metadata["existing_bead_lookup_ran"], true);
        Ok(())
    }

    #[tokio::test]
    async fn classification_propose_new_bead_created_uses_final_review_metadata(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let mut amendment = terminal_amendment(
            "a1",
            AmendmentClassification::ProposeNewBead,
            "legacy body should not drive proposal metadata",
        );
        amendment.proposed_title = Some("Authoritative retry telemetry".to_owned());
        amendment.proposed_scope =
            Some("Instrument retry loops with counters and histograms".to_owned());
        amendment.severity = Some(Severity::High);
        amendment.rationale = Some("No existing bead owns retry observability".to_owned());
        amendment.sources = vec![
            crate::contexts::workflow_composition::panel_contracts::FinalReviewAmendmentSource {
                reviewer_id: "reviewer-a".to_owned(),
                backend_family: "stub".to_owned(),
                model_id: "a".to_owned(),
            },
            crate::contexts::workflow_composition::panel_contracts::FinalReviewAmendmentSource {
                reviewer_id: "reviewer-b".to_owned(),
                backend_family: "stub".to_owned(),
                model_id: "b".to_owned(),
            },
        ];
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(
            base,
            "proj-propose-metadata",
            "run-propose-metadata",
            &aggregate,
        )?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success("[]")]));
        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-propose-metadata",
            "proj-propose-metadata",
            "run-propose-metadata",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 1);
        assert_eq!(summary.records_written, 1);
        let path = FileSystem::project_root(base, &ProjectId::new("proj-propose-metadata")?)
            .join("proposed-beads.ndjson");
        let record: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(path)?.trim())?;
        assert_eq!(record["summary"], "Authoritative retry telemetry");
        assert_eq!(record["proposed_title"], "Authoritative retry telemetry");
        assert_eq!(
            record["proposed_scope"],
            "Instrument retry loops with counters and histograms"
        );
        assert_eq!(record["severity"], "high");
        assert_eq!(
            record["rationale"],
            "No existing bead owns retry observability"
        );

        let journal = FsMilestoneJournalStore
            .read_journal(base, &MilestoneId::new("task-propose-metadata")?)?;
        let created = journal
            .iter()
            .find(|event| {
                event
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("sub_type"))
                    .and_then(|value| value.as_str())
                    == Some("propose_new_bead_created")
            })
            .expect("created event");
        let metadata = created.metadata.as_ref().expect("metadata");
        assert_eq!(metadata["proposed_title"], "Authoritative retry telemetry");
        assert_eq!(
            metadata["proposed_scope"],
            "Instrument retry loops with counters and histograms"
        );
        assert_eq!(metadata["severity"], "high");
        assert_eq!(
            metadata["rationale"],
            "No existing bead owns retry observability"
        );
        assert_eq!(metadata["threshold_count"], 2);
        assert_eq!(metadata["existing_bead_lookup_ran"], true);
        Ok(())
    }

    #[tokio::test]
    async fn classification_propose_new_bead_closed_active_match_writes_no_record(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let mut amendment = terminal_amendment(
            "a1",
            AmendmentClassification::ProposeNewBead,
            "retry telemetry should stay with active bead",
        );
        amendment.proposed_title = Some("Retry telemetry instrumentation".to_owned());
        amendment.proposed_scope =
            Some("Instrument retry loops with counters and histograms".to_owned());
        amendment.sources = vec![
            crate::contexts::workflow_composition::panel_contracts::FinalReviewAmendmentSource {
                reviewer_id: "reviewer-a".to_owned(),
                backend_family: "stub".to_owned(),
                model_id: "a".to_owned(),
            },
            crate::contexts::workflow_composition::panel_contracts::FinalReviewAmendmentSource {
                reviewer_id: "reviewer-b".to_owned(),
                backend_family: "stub".to_owned(),
                model_id: "b".to_owned(),
            },
        ];
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(
            base,
            "proj-propose-closed-active",
            "run-propose-closed-active",
            &aggregate,
        )?;

        let list_json = r#"[{"id":"active-bead","title":"Retry telemetry instrumentation","status":"closed","priority":2,"bead_type":"task"}]"#;
        let show_json = r#"{"id":"active-bead","title":"Retry telemetry instrumentation","status":"closed","priority":2,"bead_type":"task","description":"Instrument retry loops with counters and histograms","comments":[]}"#;
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![
            MockBrRunner::success(show_json),
            MockBrRunner::success(list_json),
        ]));
        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));

        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-propose-closed-active",
            "proj-propose-closed-active",
            "run-propose-closed-active",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 1);
        assert_eq!(summary.records_written, 0);
        assert!(
            !FileSystem::project_root(base, &ProjectId::new("proj-propose-closed-active")?)
                .join("proposed-beads.ndjson")
                .exists()
        );
        let journal = FsMilestoneJournalStore
            .read_journal(base, &MilestoneId::new("task-propose-closed-active")?)?;
        assert!(journal.iter().all(|event| {
            event
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("sub_type"))
                .and_then(|value| value.as_str())
                != Some("propose_new_bead_created")
        }));
        Ok(())
    }

    #[tokio::test]
    async fn classification_propose_new_bead_existing_match_comments_without_record(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        write_beads_export(base)?;
        let mut first = terminal_amendment(
            "a1",
            AmendmentClassification::ProposeNewBead,
            "retry telemetry missing counters",
        );
        first.proposed_bead_summary = Some("Add retry telemetry".to_owned());
        first.sources = vec![final_review_source("reviewer-a", "a")];
        let mut second = terminal_amendment(
            "a2",
            AmendmentClassification::ProposeNewBead,
            "add retry metrics",
        );
        second.proposed_bead_summary = Some("add retry telemetry".to_owned());
        second.sources = vec![final_review_source("reviewer-b", "b")];
        let aggregate = terminal_aggregate(vec![first, second]);
        write_terminal_aggregate(
            base,
            "proj-propose-existing-match",
            "run-propose-existing-match",
            &aggregate,
        )?;

        let list_json = r#"[{"id":"target-bead","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task"}]"#;
        let show_json = r#"{"id":"target-bead","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","description":"retry telemetry missing counters add retry metrics","comments":[]}"#;
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![
            MockBrRunner::success(show_json),
            MockBrRunner::success(list_json),
        ]));
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let mutation_runner = RecordingBrRunner::new(
            vec![
                MockBrRunner::success("synced"),
                MockBrRunner::success("commented"),
            ],
            invocations.clone(),
            None,
        );
        let mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-propose-existing-match",
            "proj-propose-existing-match",
            "run-propose-existing-match",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 2);
        assert_eq!(summary.records_written, 0);
        assert!(
            !FileSystem::project_root(base, &ProjectId::new("proj-propose-existing-match")?)
                .join("proposed-beads.ndjson")
                .exists()
        );
        let calls = invocations.lock().unwrap();
        let comment_call = calls
            .iter()
            .find(|args| {
                args.first().is_some_and(|arg| arg == "comments")
                    && args.get(1).is_some_and(|arg| arg == "add")
            })
            .expect("covered-by-existing comment");
        let comment_text = comment_call.last().expect("comment text");
        assert!(comment_text.contains("classification=covered_by_existing_bead"));
        assert!(comment_text.contains("active_bead_id=active-bead"));
        assert!(comment_text.contains("run_id=run-propose-existing-match"));
        assert!(
            comment_text.contains("Finding: retry telemetry missing counters add retry metrics")
        );
        Ok(())
    }

    #[tokio::test]
    async fn classification_propose_new_bead_counts_final_review_sources(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let mut amendment = terminal_amendment(
            "a1",
            AmendmentClassification::ProposeNewBead,
            "missing work",
        );
        amendment.proposed_bead_summary = Some("Add retry telemetry".to_owned());
        amendment.sources = final_review_sources(&[("reviewer-a", "a"), ("reviewer-b", "b")]);
        let aggregate = terminal_aggregate(vec![amendment]);
        write_terminal_aggregate(
            base,
            "proj-propose-sources",
            "run-propose-sources",
            &aggregate,
        )?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success("[]")]));
        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-propose-sources",
            "proj-propose-sources",
            "run-propose-sources",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 1);
        assert_eq!(summary.records_written, 1);
        let path = FileSystem::project_root(base, &ProjectId::new("proj-propose-sources")?)
            .join("proposed-beads.ndjson");
        let record: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(path)?.trim())?;
        assert_eq!(record["count"], 2);
        assert_eq!(record["amendment_id"], "a1");
        Ok(())
    }

    #[tokio::test]
    async fn classification_propose_new_bead_duplicate_final_review_source_stays_pending(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let source = final_review_source("reviewer-a", "a");
        let mut first = terminal_amendment(
            "a1",
            AmendmentClassification::ProposeNewBead,
            "missing work 1",
        );
        first.proposed_bead_summary = Some("Add retry telemetry".to_owned());
        first.sources = vec![source.clone()];
        let mut second = terminal_amendment(
            "a2",
            AmendmentClassification::ProposeNewBead,
            "missing work 2",
        );
        second.proposed_bead_summary = Some("add retry telemetry".to_owned());
        second.sources = vec![source];
        let aggregate = terminal_aggregate(vec![first, second]);
        write_terminal_aggregate(
            base,
            "proj-propose-single-source",
            "run-propose-single-source",
            &aggregate,
        )?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![]));
        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-propose-single-source",
            "proj-propose-single-source",
            "run-propose-single-source",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 2);
        assert_eq!(summary.records_written, 0);
        assert!(
            !FileSystem::project_root(base, &ProjectId::new("proj-propose-single-source")?)
                .join("proposed-beads.ndjson")
                .exists()
        );
        let journal = FsMilestoneJournalStore
            .read_journal(base, &MilestoneId::new("task-propose-single-source")?)?;
        let pending = journal
            .iter()
            .find(|event| {
                event
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("sub_type"))
                    .and_then(|value| value.as_str())
                    == Some("propose_new_bead_pending")
            })
            .expect("pending event");
        assert_eq!(pending.metadata.as_ref().unwrap()["current_count"], 1);
        Ok(())
    }

    #[tokio::test]
    async fn classification_propose_new_bead_missing_final_review_sources_stay_pending(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let mut first = terminal_amendment(
            "a1",
            AmendmentClassification::ProposeNewBead,
            "missing work 1",
        );
        first.proposed_bead_summary = Some("Add retry telemetry".to_owned());
        let mut second = terminal_amendment(
            "a2",
            AmendmentClassification::ProposeNewBead,
            "missing work 2",
        );
        second.proposed_bead_summary = Some("add retry telemetry".to_owned());
        let aggregate = terminal_aggregate(vec![first, second]);
        write_terminal_aggregate(
            base,
            "proj-propose-missing-sources",
            "run-propose-missing-sources",
            &aggregate,
        )?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![]));
        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-propose-missing-sources",
            "proj-propose-missing-sources",
            "run-propose-missing-sources",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 2);
        assert_eq!(summary.records_written, 0);
        assert!(
            !FileSystem::project_root(base, &ProjectId::new("proj-propose-missing-sources")?)
                .join("proposed-beads.ndjson")
                .exists()
        );
        let journal = FsMilestoneJournalStore
            .read_journal(base, &MilestoneId::new("task-propose-missing-sources")?)?;
        let pending = journal
            .iter()
            .find(|event| {
                event
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("sub_type"))
                    .and_then(|value| value.as_str())
                    == Some("propose_new_bead_pending")
            })
            .expect("pending event");
        assert_eq!(pending.metadata.as_ref().unwrap()["current_count"], 1);
        Ok(())
    }

    #[tokio::test]
    async fn classification_review_stage_duplicate_payload_findings_stay_pending(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let findings = vec![
            ClassifiedFinding {
                body: "missing work 1".to_owned(),
                classification: AmendmentClassification::ProposeNewBead,
                covered_by_bead_id: None,
                mapped_to_bead_id: None,
                proposed_bead_summary: Some("Add retry telemetry".to_owned()),
            },
            ClassifiedFinding {
                body: "missing work 2".to_owned(),
                classification: AmendmentClassification::ProposeNewBead,
                covered_by_bead_id: None,
                mapped_to_bead_id: None,
                proposed_bead_summary: Some(" add retry telemetry ".to_owned()),
            },
        ];
        write_review_payload(base, "proj-review-stage", "run-review-stage", findings)?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success("[]")]));
        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-review-stage",
            "proj-review-stage",
            "run-review-stage",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 2);
        assert_eq!(summary.records_written, 0);
        let path = FileSystem::project_root(base, &ProjectId::new("proj-review-stage")?)
            .join("proposed-beads.ndjson");
        assert!(!path.exists());
        let journal =
            FsMilestoneJournalStore.read_journal(base, &MilestoneId::new("task-review-stage")?)?;
        let pending = journal
            .iter()
            .find(|event| {
                event
                    .metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("sub_type"))
                    .and_then(|value| value.as_str())
                    == Some("propose_new_bead_pending")
            })
            .expect("pending event");
        assert_eq!(pending.metadata.as_ref().unwrap()["current_count"], 1);
        Ok(())
    }

    #[tokio::test]
    async fn classification_reconciles_review_stage_findings_from_distinct_payloads(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        write_review_payload_with_id(
            base,
            "proj-review-stage-distinct",
            "run-review-stage-distinct-reviewer-a-c1-a1-cr1-payload",
            vec![ClassifiedFinding {
                body: "missing work 1".to_owned(),
                classification: AmendmentClassification::ProposeNewBead,
                covered_by_bead_id: None,
                mapped_to_bead_id: None,
                proposed_bead_summary: Some("Add retry telemetry".to_owned()),
            }],
        )?;
        write_review_payload_with_id(
            base,
            "proj-review-stage-distinct",
            "run-review-stage-distinct-reviewer-b-c1-a1-cr1-payload",
            vec![ClassifiedFinding {
                body: "missing work 2".to_owned(),
                classification: AmendmentClassification::ProposeNewBead,
                covered_by_bead_id: None,
                mapped_to_bead_id: None,
                proposed_bead_summary: Some(" add retry telemetry ".to_owned()),
            }],
        )?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success("[]")]));
        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-review-stage-distinct",
            "proj-review-stage-distinct",
            "run-review-stage-distinct",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 2);
        assert_eq!(summary.records_written, 1);
        let path = FileSystem::project_root(base, &ProjectId::new("proj-review-stage-distinct")?)
            .join("proposed-beads.ndjson");
        let record: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(path)?.trim())?;
        assert_eq!(record["summary"], "Add retry telemetry");
        assert_eq!(record["count"], 2);
        assert_eq!(
            record["amendment_id"],
            "review:run-review-stage-distinct-reviewer-a-c1-a1-cr1-payload:0"
        );
        Ok(())
    }

    #[tokio::test]
    async fn classification_reconciles_final_review_each_completion_round(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let mut first = terminal_amendment(
            "round-1",
            AmendmentClassification::ProposeNewBead,
            "missing work 1",
        );
        first.proposed_bead_summary = Some("Add retry telemetry".to_owned());
        first.sources = vec![final_review_source("reviewer-a", "a")];
        let mut second = terminal_amendment(
            "round-2",
            AmendmentClassification::ProposeNewBead,
            "missing work 2",
        );
        second.proposed_bead_summary = Some("add retry telemetry".to_owned());
        second.sources = vec![final_review_source("reviewer-b", "b")];
        write_terminal_aggregate_round(
            base,
            "proj-final-rounds",
            "run-final-rounds",
            1,
            &terminal_aggregate(vec![first]),
        )?;
        write_terminal_aggregate_round(
            base,
            "proj-final-rounds",
            "run-final-rounds",
            2,
            &terminal_aggregate(vec![second]),
        )?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success("[]")]));
        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-final-rounds",
            "proj-final-rounds",
            "run-final-rounds",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 2);
        assert_eq!(summary.records_written, 1);
        let path = FileSystem::project_root(base, &ProjectId::new("proj-final-rounds")?)
            .join("proposed-beads.ndjson");
        let record: serde_json::Value =
            serde_json::from_str(std::fs::read_to_string(path)?.trim())?;
        assert_eq!(record["count"], 2);
        Ok(())
    }

    #[tokio::test]
    async fn classification_propose_new_bead_is_idempotent(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let mut first =
            terminal_amendment("a1", AmendmentClassification::ProposeNewBead, "missing 1");
        first.proposed_bead_summary = Some("Add retry telemetry".to_owned());
        first.sources = vec![final_review_source("reviewer-a", "a")];
        let mut second =
            terminal_amendment("a2", AmendmentClassification::ProposeNewBead, "missing 2");
        second.proposed_bead_summary = Some("add retry telemetry".to_owned());
        second.sources = vec![final_review_source("reviewer-b", "b")];
        let aggregate = terminal_aggregate(vec![first, second]);
        write_terminal_aggregate(
            base,
            "proj-propose-replay",
            "run-propose-replay",
            &aggregate,
        )?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![
            MockBrRunner::success("[]"),
            MockBrRunner::success("[]"),
        ]));
        reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-propose-replay",
            "proj-propose-replay",
            "run-propose-replay",
        )
        .await?;
        reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-propose-replay",
            "proj-propose-replay",
            "run-propose-replay",
        )
        .await?;

        let path = FileSystem::project_root(base, &ProjectId::new("proj-propose-replay")?)
            .join("proposed-beads.ndjson");
        assert_eq!(std::fs::read_to_string(path)?.lines().count(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn classification_propose_new_bead_existing_summary_new_amendment_appends(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let project_id = ProjectId::new("proj-propose-summary-replay")?;
        let project_root = FileSystem::project_root(base, &project_id);
        std::fs::create_dir_all(&project_root)?;
        let existing = ProposedBeadRecord {
            amendment_id: "previous-amendment".to_owned(),
            source_run_id: "previous-run".to_owned(),
            current_bead_id: "active-bead".to_owned(),
            summary: "Add retry telemetry".to_owned(),
            proposed_title: None,
            proposed_scope: None,
            severity: None,
            rationale: None,
            count: 2,
            timestamp: Utc::now(),
        };
        std::fs::write(
            project_root.join("proposed-beads.ndjson"),
            format!("{}\n", serde_json::to_string(&existing)?),
        )?;

        let mut first = terminal_amendment(
            "new-a1",
            AmendmentClassification::ProposeNewBead,
            "missing 1",
        );
        first.proposed_bead_summary = Some(" add retry telemetry ".to_owned());
        first.sources = vec![final_review_source("reviewer-a", "a")];
        let mut second = terminal_amendment(
            "new-a2",
            AmendmentClassification::ProposeNewBead,
            "missing 2",
        );
        second.proposed_bead_summary = Some("ADD RETRY TELEMETRY".to_owned());
        second.sources = vec![final_review_source("reviewer-b", "b")];
        let aggregate = terminal_aggregate(vec![first, second]);
        write_terminal_aggregate(
            base,
            "proj-propose-summary-replay",
            "run-propose-summary-replay",
            &aggregate,
        )?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![MockBrRunner::success("[]")]));
        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-propose-summary-replay",
            "proj-propose-summary-replay",
            "run-propose-summary-replay",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 2);
        assert_eq!(summary.records_written, 1);
        assert_eq!(
            std::fs::read_to_string(project_root.join("proposed-beads.ndjson"))?
                .lines()
                .count(),
            2,
            "same normalized proposed_bead_summary with new amendment ids is a distinct handoff record"
        );
        Ok(())
    }

    #[tokio::test]
    async fn classification_informational_only_is_noop() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        let aggregate = terminal_aggregate(vec![terminal_amendment(
            "info",
            AmendmentClassification::InformationalOnly,
            "just context",
        )]);
        write_terminal_aggregate(base, "proj-info-noop", "run-info-noop", &aggregate)?;

        let mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));
        let read = BrAdapter::with_runner(MockBrRunner::new(vec![]));
        let summary = reconcile_terminal_review_classifications(
            &mutation,
            &read,
            base,
            "active-bead",
            "task-info",
            "proj-info-noop",
            "run-info-noop",
        )
        .await?;

        assert_eq!(summary.amendments_processed, 0);
        assert_eq!(summary.records_written, 0);
        Ok(())
    }
}
