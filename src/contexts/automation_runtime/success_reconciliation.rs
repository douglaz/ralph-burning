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

use std::path::Path;

use chrono::{DateTime, Utc};
use tracing::Instrument;

use crate::adapters::br_health::{beads_health_failure_details, check_beads_health};
use crate::adapters::br_models::BeadStatus;
use crate::adapters::br_process::{
    BrAdapter, BrCommand, BrError, BrMutationAdapter, ProcessRunner, SyncIfDirtyHealthError,
};
use crate::adapters::bv_process::{BvAdapter, BvProcessRunner, NextBeadResponse};
use crate::adapters::fs::{
    FsArtifactStore, FsMilestoneControllerStore, FsMilestoneJournalStore, FsMilestoneSnapshotStore,
    FsPlannedElsewhereMappingStore, FsTaskRunLineageStore,
};
use crate::cli::run::{select_next_milestone_bead, select_next_milestone_bead_from_recommendation};
use crate::contexts::milestone_record::controller as milestone_controller;
use crate::contexts::milestone_record::model::{
    MilestoneId, MilestoneStatus, PlannedElsewhereMapping, TaskRunOutcome,
};
use crate::contexts::milestone_record::service::{
    self as milestone_service, CompletionMilestoneDisposition, ProposeNewBeadInput,
};
use crate::contexts::project_run_record::service::ArtifactStorePort;
use crate::contexts::workflow_composition::panel_contracts::{
    AmendmentClassification, FinalReviewAggregatePayload, RecordKind,
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
struct ProposedBeadReconciliationSummary {
    amendments_processed: usize,
    beads_created: usize,
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

        let proposed_bead_summary = reconcile_proposed_beads_after_success(
            br_mutation,
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
            beads_created = proposed_bead_summary.beads_created,
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
/// `Ok(true)`. If close succeeds, returns `Ok(false)`.
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
            // If we can't read status, try the close anyway — br close may
            // handle the already-closed case internally.
            tracing::warn!(
                bead_id = bead_id,
                task_id = task_id,
                error = %e,
                "could not read bead status for idempotency check, proceeding with close"
            );
        }
    }

    ensure_beads_mutation_health(base_dir, bead_id, task_id)?;
    let reason = format!("task {task_id} completed successfully");
    match br_mutation.close_bead(bead_id, &reason).await {
        Ok(_) => Ok(false),
        Err(e) => {
            // Check if the failure is because the bead is already closed.
            // Some br implementations return an error for double-close.
            if let Ok(true) = is_bead_already_closed(br_read, bead_id).await {
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

async fn reconcile_proposed_beads_after_success<R: ProcessRunner>(
    br_mutation: &BrMutationAdapter<R>,
    base_dir: &Path,
    bead_id: &str,
    task_id: &str,
    milestone_id_str: &str,
    project_id: &str,
    run_id: &str,
) -> Result<ProposedBeadReconciliationSummary, ReconciliationError> {
    let milestone_error = |details: String| ReconciliationError::MilestoneUpdateFailed {
        bead_id: bead_id.to_owned(),
        task_id: task_id.to_owned(),
        details,
    };
    let Ok(milestone_id) = MilestoneId::new(milestone_id_str) else {
        return Ok(ProposedBeadReconciliationSummary::default());
    };
    let pid = match ProjectId::new(project_id) {
        Ok(pid) => pid,
        Err(_) => return Ok(ProposedBeadReconciliationSummary::default()),
    };
    let payloads = match FsArtifactStore.list_payloads(base_dir, &pid) {
        Ok(payloads) => payloads,
        Err(error) => {
            return Err(milestone_error(format!(
                "failed to list payloads for propose-new-bead reconciliation: {error}"
            )));
        }
    };

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

    let Some(authoritative_round) = latest_by_round.keys().copied().max() else {
        return Ok(ProposedBeadReconciliationSummary::default());
    };
    let Some(payload) = latest_by_round.get(&authoritative_round) else {
        return Ok(ProposedBeadReconciliationSummary::default());
    };
    let aggregate: FinalReviewAggregatePayload = match serde_json::from_value(
        payload.payload.clone(),
    ) {
        Ok(aggregate) => aggregate,
        Err(error) => {
            return Err(milestone_error(format!(
                    "failed to parse final-review aggregate for propose-new-bead reconciliation at completion round {}: {}",
                    authoritative_round, error
                )));
        }
    };

    let mut amendments_processed = 0usize;
    let mut created_in_pass = 0usize;
    for amendment in &aggregate.final_accepted_amendments {
        if amendment.classification != AmendmentClassification::ProposeNewBead {
            continue;
        }
        amendments_processed += 1;

        let (Some(proposed_title), Some(proposed_scope), Some(severity), Some(rationale)) = (
            amendment.proposed_title.as_ref(),
            amendment.proposed_scope.as_ref(),
            amendment.severity,
            amendment.rationale.as_ref(),
        ) else {
            return Err(milestone_error(format!(
                "accepted propose-new-bead amendment {} is missing required metadata in completion round {}",
                amendment.amendment_id, authoritative_round
            )));
        };

        let input = ProposeNewBeadInput {
            active_bead_id: bead_id.to_owned(),
            finding_summary: amendment.normalized_body.clone(),
            proposed_title: proposed_title.clone(),
            proposed_scope: proposed_scope.clone(),
            severity,
            rationale: rationale.clone(),
            run_id: Some(run_id.to_owned()),
            completion_round: Some(authoritative_round),
        };

        if let Err(error) = milestone_service::handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            br_mutation,
            base_dir,
            &milestone_id,
            &input,
            &mut created_in_pass,
            Utc::now(),
        )
        .await
        {
            tracing::error!(
                amendment_id = amendment.amendment_id.as_str(),
                error = %error,
                "failed to reconcile propose-new-bead amendment; keeping success reconciliation open for retry"
            );
            return Err(ReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!(
                    "failed to reconcile propose-new-bead amendment {}: {}",
                    amendment.amendment_id, error
                ),
            });
        }
    }

    Ok(ProposedBeadReconciliationSummary {
        amendments_processed,
        beads_created: created_in_pass,
    })
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

        let aggregate: FinalReviewAggregatePayload =
            match serde_json::from_value(payload.payload.clone()) {
                Ok(a) => a,
                Err(_) => continue,
            };

        for amendment in &aggregate.final_accepted_amendments {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::br_process::{BrOutput, ProcessRunner};
    use crate::adapters::bv_process::{BvError, BvOutput, BvProcessRunner};
    use crate::adapters::fs::FsMilestoneStore;
    use crate::contexts::milestone_record::model::MilestoneEventType;
    use crate::contexts::milestone_record::service::MilestoneJournalPort;
    use crate::contexts::milestone_record::service::MilestoneSnapshotPort;
    use crate::contexts::project_run_record::model::PayloadRecord;
    use crate::contexts::workflow_composition::panel_contracts::FinalReviewCanonicalAmendment;
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
            r#"{"adapter_id":"reconciliation-owner","operation":"close_bead","bead_id":"b1","status":null}"#,
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
            r#"{"adapter_id":"reconciliation-owner","operation":"close_bead","bead_id":"b1","status":null}"#,
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

    /// Two aggregates for the same completion_round but different created_at:
    /// only the latest aggregate's PE amendments should be reconstructed.
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
                    classification: crate::contexts::workflow_composition::panel_contracts::AmendmentClassification::CoveredByExistingBead,
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

    #[tokio::test]
    async fn reconcile_proposed_beads_after_success_creates_follow_up_from_latest_aggregate(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        std::fs::create_dir_all(base.join(".beads"))?;
        std::fs::write(base.join(".beads/issues.jsonl"), "")?;

        let store = FsMilestoneStore;
        let now = Utc::now();
        let record = crate::contexts::milestone_record::service::create_milestone(
            &store,
            base,
            crate::contexts::milestone_record::service::CreateMilestoneInput {
                id: "ms-proposed".to_owned(),
                name: "Proposed bead".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let project_id = ProjectId::new("proj-proposed")?;
        let project_root = crate::adapters::fs::FileSystem::project_root(base, &project_id);
        let payloads_dir = project_root.join("history/payloads");
        std::fs::create_dir_all(&payloads_dir)?;

        let aggregate = FinalReviewAggregatePayload {
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
                normalized_body: "Retry paths lack telemetry".to_owned(),
                sources: vec![],
                mapped_to_bead_id: None,
                covered_by_bead_id: None,
                classification: AmendmentClassification::ProposeNewBead,
                rationale: Some("No existing bead covers retry observability".to_owned()),
                proposed_title: Some("Add retry telemetry".to_owned()),
                proposed_scope: Some(
                    "Instrument retry loops with counters and histograms".to_owned(),
                ),
                proposed_bead_summary: Some("Add retry telemetry".to_owned()),
                severity: Some(
                    crate::contexts::workflow_composition::review_classification::Severity::Medium,
                ),
            }],
            final_review_restart_count: 0,
            max_restarts: 3,
            summary: "test".to_owned(),
            exhausted_count: 0,
            probe_exhausted_count: 0,
            effective_min_reviewers: 1,
        };

        let payload = PayloadRecord {
            payload_id: "run-proposed-final_review-aggregate-c1-a1-cr3-payload".to_owned(),
            stage_id: StageId::FinalReview,
            cycle: 1,
            attempt: 1,
            created_at: now,
            payload: serde_json::to_value(&aggregate)?,
            record_kind: RecordKind::StageAggregate,
            producer: None,
            completion_round: 3,
        };
        std::fs::write(
            payloads_dir.join("aggregate.json"),
            serde_json::to_string(&payload)?,
        )?;

        // MockBrRunner::pop() consumes from END (reverse order).
        // Call order:
        // 1. br list --all (empty array — no existing matches)
        // 2. br show active-bead (get labels for new bead)
        // 3. br create (create the new bead)
        // 4. br show bead-proposed (resolve created bead ID — candidate from stdout)
        //    If description mismatch → fallback: 4a. list --all, 4b. br show
        // 5. br update (add dependency on active-bead)
        // 6. br sync --flush-only
        let bead_detail = serde_json::json!({
            "id": "bead-proposed",
            "title": "Add retry telemetry",
            "status": "open",
            "priority": 2,
            "bead_type": "task",
            "labels": ["backend"],
            "description": "## Finding Summary\nRetry paths lack telemetry\n\n## Proposed Scope\nInstrument retry loops with counters and histograms\n\n## Rationale\nNo existing bead covers retry observability",
            "dependencies": [],
            "dependents": []
        })
        .to_string();
        let br_runner = MockBrRunner::new(vec![
            // 6. br sync
            MockBrRunner::success("synced"),
            // 5. br update (add dependency)
            MockBrRunner::success("dependency added"),
            // 4b. br show bead-proposed (resolve fallback)
            MockBrRunner::success(&bead_detail),
            // 4a. br list --all (resolve fallback)
            MockBrRunner::success(
                r#"[{"id":"bead-proposed","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"]}]"#,
            ),
            // 4. br show bead-proposed (resolve primary — candidate from stdout)
            MockBrRunner::success(&bead_detail),
            // 3. br create
            MockBrRunner::success("created bead-proposed"),
            // 2. br show active-bead (get labels)
            MockBrRunner::success(
                r#"{"id":"active-bead","title":"Active bead","status":"open","priority":1,"bead_type":"task","labels":["backend"],"dependencies":[],"dependents":[]}"#,
            ),
            // 1. br list --all (empty)
            MockBrRunner::success("[]"),
        ]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(br_runner));

        reconcile_proposed_beads_after_success(
            &br_mutation,
            base,
            "active-bead",
            "task-proposed",
            record.id.as_str(),
            "proj-proposed",
            "run-proposed",
        )
        .await?;

        // Replay: journal already has ProposedBeadCreated, so handle_propose_new_bead
        // finds the existing creation event and does br show to verify the bead exists.
        // If dependency on active-bead is missing, it adds it (br update + br sync).
        let bead_detail_with_dep = serde_json::json!({
            "id": "bead-proposed",
            "title": "Add retry telemetry",
            "status": "open",
            "priority": 2,
            "bead_type": "task",
            "labels": ["backend"],
            "description": "## Finding Summary\nRetry paths lack telemetry\n\n## Proposed Scope\nInstrument retry loops with counters and histograms\n\n## Rationale\nNo existing bead covers retry observability",
            "dependencies": [{"id": "active-bead", "kind": "blocks"}],
            "dependents": []
        })
        .to_string();
        let replay_mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![
                MockBrRunner::success(&bead_detail_with_dep),
            ])));
        reconcile_proposed_beads_after_success(
            &replay_mutation,
            base,
            "active-bead",
            "task-proposed",
            record.id.as_str(),
            "proj-proposed",
            "run-proposed",
        )
        .await?;

        let journal = FsMilestoneJournalStore.read_journal(base, &record.id)?;
        let created_event = journal
            .iter()
            .find(|event| event.event_type == MilestoneEventType::ProposedBeadCreated)
            .expect("created event");
        let metadata = created_event.metadata.as_ref().expect("metadata");
        assert_eq!(metadata["created_bead_id"], "bead-proposed");
        assert_eq!(metadata["proposed_title"], "Add retry telemetry");
        assert_eq!(
            journal
                .iter()
                .filter(|event| event.event_type == MilestoneEventType::ProposedBeadCreated)
                .count(),
            1
        );
        assert!(journal
            .iter()
            .all(|event| event.event_type != MilestoneEventType::PlannedElsewhereMapped));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_proposed_beads_after_success_returns_error_on_payload_listing_failure(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        std::fs::create_dir_all(base.join(".beads"))?;
        std::fs::write(base.join(".beads/issues.jsonl"), "")?;

        let store = FsMilestoneStore;
        let now = Utc::now();
        let record = crate::contexts::milestone_record::service::create_milestone(
            &store,
            base,
            crate::contexts::milestone_record::service::CreateMilestoneInput {
                id: "ms-proposed-corrupt-payload".to_owned(),
                name: "Proposed bead corrupt payload".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let project_id = ProjectId::new("proj-proposed-corrupt-payload")?;
        let project_root = crate::adapters::fs::FileSystem::project_root(base, &project_id);
        let payloads_dir = project_root.join("history/payloads");
        std::fs::create_dir_all(&payloads_dir)?;
        std::fs::write(payloads_dir.join("aggregate.json"), "{not valid json")?;

        let br_mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));

        let error = reconcile_proposed_beads_after_success(
            &br_mutation,
            base,
            "active-bead",
            "task-proposed-corrupt-payload",
            record.id.as_str(),
            "proj-proposed-corrupt-payload",
            "run-proposed-corrupt-payload",
        )
        .await
        .expect_err("corrupt payload listing should stop reconciliation");

        match error {
            ReconciliationError::MilestoneUpdateFailed { details, .. } => {
                assert!(
                    details.contains("failed to list payloads for propose-new-bead reconciliation")
                );
            }
            other => panic!("unexpected error: {other}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_proposed_beads_after_success_returns_error_on_aggregate_parse_failure(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        std::fs::create_dir_all(base.join(".beads"))?;
        std::fs::write(base.join(".beads/issues.jsonl"), "")?;

        let store = FsMilestoneStore;
        let now = Utc::now();
        let record = crate::contexts::milestone_record::service::create_milestone(
            &store,
            base,
            crate::contexts::milestone_record::service::CreateMilestoneInput {
                id: "ms-proposed-parse-failure".to_owned(),
                name: "Proposed bead parse failure".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let project_id = ProjectId::new("proj-proposed-parse-failure")?;
        let project_root = crate::adapters::fs::FileSystem::project_root(base, &project_id);
        let payloads_dir = project_root.join("history/payloads");
        std::fs::create_dir_all(&payloads_dir)?;

        let payload = PayloadRecord {
            payload_id: "run-proposed-parse-failure-final_review-aggregate-c1-a1-cr4-payload"
                .to_owned(),
            stage_id: StageId::FinalReview,
            cycle: 1,
            attempt: 1,
            created_at: now,
            payload: serde_json::json!({"unexpected": "shape"}),
            record_kind: RecordKind::StageAggregate,
            producer: None,
            completion_round: 4,
        };
        std::fs::write(
            payloads_dir.join("aggregate.json"),
            serde_json::to_string(&payload)?,
        )?;

        let br_mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));

        let error = reconcile_proposed_beads_after_success(
            &br_mutation,
            base,
            "active-bead",
            "task-proposed-parse-failure",
            record.id.as_str(),
            "proj-proposed-parse-failure",
            "run-proposed-parse-failure",
        )
        .await
        .expect_err("aggregate parse failure should stop reconciliation");

        match error {
            ReconciliationError::MilestoneUpdateFailed { details, .. } => {
                assert!(details.contains("failed to parse final-review aggregate"));
                assert!(details.contains("completion round 4"));
            }
            other => panic!("unexpected error: {other}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_proposed_beads_after_success_returns_error_on_missing_metadata(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        std::fs::create_dir_all(base.join(".beads"))?;
        std::fs::write(base.join(".beads/issues.jsonl"), "")?;

        let store = FsMilestoneStore;
        let now = Utc::now();
        let record = crate::contexts::milestone_record::service::create_milestone(
            &store,
            base,
            crate::contexts::milestone_record::service::CreateMilestoneInput {
                id: "ms-proposed-missing-metadata".to_owned(),
                name: "Proposed bead missing metadata".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let project_id = ProjectId::new("proj-proposed-missing-metadata")?;
        let project_root = crate::adapters::fs::FileSystem::project_root(base, &project_id);
        let payloads_dir = project_root.join("history/payloads");
        std::fs::create_dir_all(&payloads_dir)?;

        let aggregate = FinalReviewAggregatePayload {
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
                normalized_body: "Retry paths lack telemetry".to_owned(),
                sources: vec![],
                mapped_to_bead_id: None,
                covered_by_bead_id: None,
                classification: AmendmentClassification::ProposeNewBead,
                rationale: Some("No existing bead covers retry observability".to_owned()),
                proposed_title: Some("Add retry telemetry".to_owned()),
                proposed_scope: None,
                proposed_bead_summary: Some("Add retry telemetry".to_owned()),
                severity: Some(
                    crate::contexts::workflow_composition::review_classification::Severity::Medium,
                ),
            }],
            final_review_restart_count: 0,
            max_restarts: 3,
            summary: "test".to_owned(),
            exhausted_count: 0,
            probe_exhausted_count: 0,
            effective_min_reviewers: 1,
        };

        let payload = PayloadRecord {
            payload_id: "run-proposed-missing-metadata-final_review-aggregate-c1-a1-cr4-payload"
                .to_owned(),
            stage_id: StageId::FinalReview,
            cycle: 1,
            attempt: 1,
            created_at: now,
            payload: serde_json::to_value(&aggregate)?,
            record_kind: RecordKind::StageAggregate,
            producer: None,
            completion_round: 4,
        };
        std::fs::write(
            payloads_dir.join("aggregate.json"),
            serde_json::to_string(&payload)?,
        )?;

        let br_mutation =
            BrMutationAdapter::with_adapter(BrAdapter::with_runner(MockBrRunner::new(vec![])));

        let error = reconcile_proposed_beads_after_success(
            &br_mutation,
            base,
            "active-bead",
            "task-proposed-missing-metadata",
            record.id.as_str(),
            "proj-proposed-missing-metadata",
            "run-proposed-missing-metadata",
        )
        .await
        .expect_err("missing metadata should stop reconciliation");

        match error {
            ReconciliationError::MilestoneUpdateFailed { details, .. } => {
                assert!(details.contains(
                    "accepted propose-new-bead amendment a1 is missing required metadata"
                ));
            }
            other => panic!("unexpected error: {other}"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_proposed_beads_after_success_returns_error_on_creation_failure(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        std::fs::create_dir_all(base.join(".git"))?;
        std::fs::create_dir_all(base.join(".beads"))?;
        std::fs::write(base.join(".beads/issues.jsonl"), "")?;

        let store = FsMilestoneStore;
        let now = Utc::now();
        let record = crate::contexts::milestone_record::service::create_milestone(
            &store,
            base,
            crate::contexts::milestone_record::service::CreateMilestoneInput {
                id: "ms-proposed-failure".to_owned(),
                name: "Proposed bead failure".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let project_id = ProjectId::new("proj-proposed-failure")?;
        let project_root = crate::adapters::fs::FileSystem::project_root(base, &project_id);
        let payloads_dir = project_root.join("history/payloads");
        std::fs::create_dir_all(&payloads_dir)?;

        let aggregate = FinalReviewAggregatePayload {
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
                normalized_body: "Retry paths lack telemetry".to_owned(),
                sources: vec![],
                mapped_to_bead_id: None,
                covered_by_bead_id: None,
                classification: AmendmentClassification::ProposeNewBead,
                rationale: Some("No existing bead covers retry observability".to_owned()),
                proposed_title: Some("Add retry telemetry".to_owned()),
                proposed_scope: Some(
                    "Instrument retry loops with counters and histograms".to_owned(),
                ),
                proposed_bead_summary: Some("Add retry telemetry".to_owned()),
                severity: Some(
                    crate::contexts::workflow_composition::review_classification::Severity::Medium,
                ),
            }],
            final_review_restart_count: 0,
            max_restarts: 3,
            summary: "test".to_owned(),
            exhausted_count: 0,
            probe_exhausted_count: 0,
            effective_min_reviewers: 1,
        };

        let payload = PayloadRecord {
            payload_id: "run-proposed-failure-final_review-aggregate-c1-a1-cr4-payload".to_owned(),
            stage_id: StageId::FinalReview,
            cycle: 1,
            attempt: 1,
            created_at: now,
            payload: serde_json::to_value(&aggregate)?,
            record_kind: RecordKind::StageAggregate,
            producer: None,
            completion_round: 4,
        };
        std::fs::write(
            payloads_dir.join("aggregate.json"),
            serde_json::to_string(&payload)?,
        )?;

        let br_runner = MockBrRunner::new(vec![
            MockBrRunner::error(1, "create failed"),
            MockBrRunner::success(
                r#"{"id":"active-bead","title":"Active bead","status":"open","priority":1,"bead_type":"task","labels":["backend"]}"#,
            ),
            MockBrRunner::success("[]"),
        ]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(br_runner));

        let error = reconcile_proposed_beads_after_success(
            &br_mutation,
            base,
            "active-bead",
            "task-proposed-failure",
            record.id.as_str(),
            "proj-proposed-failure",
            "run-proposed-failure",
        )
        .await
        .expect_err("creation failure should stop success reconciliation");

        match error {
            ReconciliationError::MilestoneUpdateFailed {
                bead_id,
                task_id,
                details,
            } => {
                assert_eq!(bead_id, "active-bead");
                assert_eq!(task_id, "task-proposed-failure");
                assert!(details.contains("failed to reconcile propose-new-bead amendment a1"));
                assert!(details.contains("create proposed bead"));
            }
            other => panic!("unexpected error: {other}"),
        }

        let journal = FsMilestoneJournalStore.read_journal(base, &record.id)?;
        assert!(journal
            .iter()
            .all(|event| event.event_type != MilestoneEventType::ProposedBeadCreated));

        Ok(())
    }
}
