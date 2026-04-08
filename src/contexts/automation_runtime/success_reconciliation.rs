#![forbid(unsafe_code)]

//! Success reconciliation handler for completed bead tasks.
//!
//! After a bead-linked task finishes successfully, this handler:
//! 1. Moves the milestone controller into `reconciling`
//! 2. Closes the bead in `br` with a success reason (idempotently)
//! 3. Runs `br sync --flush-only` to persist the mutation
//! 4. Updates milestone state via `record_bead_completion`
//! 5. Captures next-step hints from `bv --robot-next` (informational)
//! 6. Records the task-to-bead linkage outcome

use std::path::Path;

use chrono::{DateTime, Utc};

use crate::adapters::br_models::BeadStatus;
use crate::adapters::br_process::{
    BrAdapter, BrCommand, BrError, BrMutationAdapter, ProcessRunner,
};
use crate::adapters::bv_process::{BvAdapter, BvProcessRunner, NextBeadResponse};
use crate::adapters::fs::{
    FsMilestoneControllerStore, FsMilestoneJournalStore, FsMilestoneSnapshotStore,
    FsTaskRunLineageStore,
};
use crate::contexts::milestone_record::controller as milestone_controller;
use crate::contexts::milestone_record::model::{MilestoneId, MilestoneStatus, TaskRunOutcome};
use crate::contexts::milestone_record::service::{
    self as milestone_service, CompletionMilestoneDisposition,
};
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
    /// Timestamp of the reconciliation.
    pub reconciled_at: DateTime<Utc>,
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
/// 6. Return the linkage outcome
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
    let milestone_id = MilestoneId::new(milestone_id_str).map_err(|e| {
        ReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: format!("invalid milestone id: {e}"),
        }
    })?;

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

    // Step 1: Close the bead idempotently.
    let was_already_closed = close_bead_idempotent(br_mutation, br_read, bead_id, task_id).await?;

    // Step 2: Sync flush — always runs, even if bead was already closed.
    // A crash between br close and sync would leave local bead state dirty.
    // On re-run the bead appears closed but the flush never happened, so we
    // must sync unconditionally to guarantee crash-safe idempotency.
    //
    // Note: was_already_closed is NOT a safe proxy for "sync already completed".
    // A crash between close and sync produces was_already_closed=true with an
    // un-flushed local state. Sync failures must remain fatal regardless of
    // was_already_closed to prevent proceeding with an un-synced bead close.
    sync_after_close(br_mutation, bead_id, task_id).await?;

    // Step 3: Update milestone state.
    update_milestone_state(
        base_dir,
        bead_id,
        task_id,
        project_id,
        &milestone_id,
        run_id,
        plan_hash,
        started_at,
        now,
    )?;

    // Step 4: Capture next-step hints (best-effort, never blocks reconciliation).
    let next_step_hint = if let Some(bv_adapter) = bv {
        match capture_next_step_hint(bv_adapter).await {
            HintCaptureOutcome::Captured(hint) => {
                // Step 4b: Persist hint to disk so downstream selection logic
                // can read it in a later daemon cycle. Overwrites any stale hint
                // from a previous bead's run.
                persist_next_step_hint(base_dir, milestone_id_str, &hint);
                Some(hint)
            }
            HintCaptureOutcome::NoRecommendation => {
                // bv succeeded but has no actionable recommendation.
                // Remove any previously persisted hint so downstream
                // selection does not act on a stale pointer to an
                // already-completed bead.
                delete_stale_hint(base_dir, milestone_id_str);
                None
            }
            HintCaptureOutcome::BvFailed => {
                // bv failed (transient error, binary not found, etc.).
                // Leave any existing hint untouched — a transient bv outage
                // should not erase a previously persisted valid hint.
                None
            }
        }
    } else {
        // bv not configured — leave any existing hint untouched.
        None
    };

    Ok(ReconciliationOutcome {
        bead_id: bead_id.to_owned(),
        task_id: task_id.to_owned(),
        was_already_closed,
        next_step_hint,
        reconciled_at: now,
    })
}

/// Close a bead idempotently. If the bead is already closed, returns
/// `Ok(true)`. If close succeeds, returns `Ok(false)`.
///
/// On failure, returns `ReconciliationError::BrCloseFailed`.
async fn close_bead_idempotent<R: ProcessRunner>(
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
    br_mutation: &BrMutationAdapter<R>,
    bead_id: &str,
    task_id: &str,
) -> Result<(), ReconciliationError> {
    br_mutation
        .sync_flush()
        .await
        .map_err(|e| ReconciliationError::BrSyncFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: e.to_string(),
        })?;
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
) -> Result<(), ReconciliationError> {
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

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::br_process::{BrOutput, ProcessRunner};
    use crate::adapters::bv_process::{BvError, BvOutput, BvProcessRunner};
    use crate::contexts::milestone_record::service::MilestoneSnapshotPort;
    use std::sync::Mutex;
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

    // ── Tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn close_bead_idempotent_already_closed() -> Result<(), Box<dyn std::error::Error>> {
        // br show returns closed status
        let show_json =
            r#"{"id":"b1","title":"Test","status":"closed","priority":2,"bead_type":"task"}"#;
        let runner = MockBrRunner::new(vec![MockBrRunner::success(show_json)]);
        let br_adapter = BrAdapter::with_runner(runner);
        // Mutation adapter won't be called
        let mutation_runner = MockBrRunner::new(vec![]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let result = close_bead_idempotent(&br_mutation, &br_adapter, "b1", "task-1").await?;
        assert!(result, "should report bead was already closed");
        Ok(())
    }

    #[tokio::test]
    async fn close_bead_idempotent_open_then_closed() -> Result<(), Box<dyn std::error::Error>> {
        // br show returns open, then close succeeds
        let show_json =
            r#"{"id":"b1","title":"Test","status":"open","priority":2,"bead_type":"task"}"#;
        // Responses are popped from the end (stack order)
        let runner = MockBrRunner::new(vec![MockBrRunner::success(show_json)]);
        let br_adapter = BrAdapter::with_runner(runner);

        let close_output = MockBrRunner::success("");
        let mutation_runner = MockBrRunner::new(vec![close_output]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

        let result = close_bead_idempotent(&br_mutation, &br_adapter, "b1", "task-1").await?;
        assert!(!result, "should report bead was freshly closed");
        Ok(())
    }

    #[tokio::test]
    async fn close_bead_failure_returns_error() -> Result<(), Box<dyn std::error::Error>> {
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

        let result = close_bead_idempotent(&br_mutation, &br_adapter, "b1", "task-1").await;
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

        let result = close_bead_idempotent(&br_mutation, &br_adapter, "b1", "task-1").await?;
        assert!(
            result,
            "should be idempotent when close fails but bead is closed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_after_close_success() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockBrRunner::new(vec![MockBrRunner::success("")]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));

        sync_after_close(&br_mutation, "b1", "task-1").await?;
        Ok(())
    }

    #[tokio::test]
    async fn sync_after_close_failure() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockBrRunner::new(vec![MockBrRunner::error(1, "sync failed")]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));

        let result = sync_after_close(&br_mutation, "b1", "task-1").await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ReconciliationError::BrSyncFailed { .. }
        ));
        Ok(())
    }

    /// Sync failure when bead was already closed (replay scenario) must still
    /// be treated as a fatal error. `was_already_closed` is not a sound proxy
    /// for "sync already completed" — a crash between close and sync produces
    /// the same flag but with an un-flushed local state.
    #[tokio::test]
    async fn sync_failure_on_replay_is_still_fatal() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        std::fs::create_dir_all(temp_dir.path().join(".ralph-burning/milestones/ms-1"))?;

        // br show returns closed (bead already closed from prior attempt)
        let show_closed =
            r#"{"id":"b1","title":"Test","status":"closed","priority":2,"bead_type":"task"}"#;
        let read_runner = MockBrRunner::new(vec![MockBrRunner::success(show_closed)]);
        let br_read = BrAdapter::with_runner(read_runner);

        // Mutation adapter: sync fails (no close needed since bead already closed)
        let mutation_runner = MockBrRunner::new(vec![MockBrRunner::error(1, "sync failed")]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(mutation_runner));

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
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some("bead-close".to_owned()),
                    explicit_id: Some(true),
                    title: "Close bead".to_owned(),
                    description: None,
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec![],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::QuickDev,
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
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some("bead-hint".to_owned()),
                    explicit_id: Some(true),
                    title: "Hint bead".to_owned(),
                    description: None,
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec![],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::QuickDev,
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
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some("bead-e2e".to_owned()),
                    explicit_id: Some(true),
                    title: "E2E bead".to_owned(),
                    description: None,
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec![],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::QuickDev,
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
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some("bead-replay".to_owned()),
                    explicit_id: Some(true),
                    title: "Replay bead".to_owned(),
                    description: None,
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec![],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::QuickDev,
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
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some("bead-replay".to_owned()),
                    explicit_id: Some(true),
                    title: "Replay bead".to_owned(),
                    description: None,
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec![],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::QuickDev,
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
}
