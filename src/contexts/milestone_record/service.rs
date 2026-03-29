use std::collections::BTreeSet;
use std::path::Path;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::shared::error::{AppError, AppResult};

use super::bundle::{render_plan_json, render_plan_md, MilestoneBundle};
use super::model::{
    collapse_task_run_attempts, latest_task_runs_per_bead, MilestoneEventType, MilestoneId,
    MilestoneJournalEvent, MilestoneProgress, MilestoneRecord, MilestoneSnapshot, MilestoneStatus,
    TaskRunEntry, TaskRunOutcome,
};

// ── Ports ───────────────────────────────────────────────────────────────────

/// Port for reading and writing milestone records.
pub trait MilestoneStorePort {
    fn milestone_exists(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<bool>;
    fn read_milestone_record(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<MilestoneRecord>;
    fn list_milestone_ids(&self, base_dir: &Path) -> AppResult<Vec<MilestoneId>>;
    fn create_milestone_atomic(
        &self,
        base_dir: &Path,
        record: &MilestoneRecord,
        snapshot: &MilestoneSnapshot,
        initial_journal_line: &str,
    ) -> AppResult<()>;
}

/// Port for reading and writing milestone status snapshots.
pub trait MilestoneSnapshotPort {
    fn read_snapshot(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<MilestoneSnapshot>;
    fn write_snapshot(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        snapshot: &MilestoneSnapshot,
    ) -> AppResult<()>;
    /// Serialize multi-file milestone mutations for a single milestone.
    fn with_milestone_write_lock<T, F>(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        operation: F,
    ) -> AppResult<T>
    where
        F: FnOnce() -> AppResult<T>;
}

/// Port for appending milestone journal events.
pub trait MilestoneJournalPort {
    fn read_journal(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<MilestoneJournalEvent>>;
    fn append_event(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        line: &str,
    ) -> AppResult<()>;
    /// Atomically append an event only when an identical entry is not present.
    fn append_event_if_missing(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        event: &MilestoneJournalEvent,
    ) -> AppResult<bool>;
}

/// Port for reading and writing task-run lineage.
pub trait TaskRunLineagePort {
    fn read_task_runs(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<TaskRunEntry>>;
    fn append_task_run(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        entry: &TaskRunEntry,
    ) -> AppResult<()>;
    /// Atomically reuse an existing running attempt or append a new start row.
    #[allow(clippy::too_many_arguments)]
    fn record_task_run_start(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
        project_id: &str,
        run_id: Option<&str>,
        plan_hash: Option<&str>,
        started_at: DateTime<Utc>,
    ) -> AppResult<TaskRunEntry>;
    /// Update an existing task run entry's outcome by matching bead_id + project_id,
    /// further narrowing by run_id when available. Implementations should reject
    /// ambiguous matches instead of rewriting an arbitrary historical attempt.
    #[allow(clippy::too_many_arguments)]
    fn update_task_run(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
        project_id: &str,
        run_id: Option<&str>,
        plan_hash: Option<&str>,
        outcome: TaskRunOutcome,
        outcome_detail: Option<String>,
        finished_at: DateTime<Utc>,
    ) -> AppResult<TaskRunEntry>;
    /// Find all task run entries for a specific bead, in chronological order.
    fn find_runs_for_bead(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
    ) -> AppResult<Vec<TaskRunEntry>>;
}

/// Port for reading and writing plan artifacts.
pub trait MilestonePlanPort {
    fn read_plan_json(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<String>;
    fn write_plan_json(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        content: &str,
    ) -> AppResult<()>;
    fn read_plan_md(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<String>;
    fn write_plan_md(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        content: &str,
    ) -> AppResult<()>;
}

fn snapshot_corrupt_record(milestone_id: &MilestoneId, details: impl Into<String>) -> AppError {
    AppError::CorruptRecord {
        file: format!("milestones/{}/status.json", milestone_id),
        details: details.into(),
    }
}

fn validate_snapshot(snapshot: &MilestoneSnapshot, milestone_id: &MilestoneId) -> AppResult<()> {
    snapshot
        .validate_semantics()
        .map_err(|details| snapshot_corrupt_record(milestone_id, details))
}

fn reconcile_snapshot_from_lineage(
    snapshot: &mut MilestoneSnapshot,
    milestone_id: &MilestoneId,
    task_runs: Vec<TaskRunEntry>,
) -> AppResult<()> {
    let task_runs = collapse_task_run_attempts(task_runs);
    let current_bead_runs = latest_task_runs_per_bead(&task_runs);
    let has_any_task_runs = !current_bead_runs.is_empty();
    let active_beads: BTreeSet<String> = current_bead_runs
        .iter()
        .filter(|entry| !entry.outcome.is_terminal())
        .map(|entry| entry.bead_id.clone())
        .collect();

    snapshot.active_bead = match active_beads.len() {
        0 => None,
        1 => active_beads.iter().next().cloned(),
        _ => {
            let bead_list = active_beads.into_iter().collect::<Vec<_>>().join(", ");
            return Err(snapshot_corrupt_record(
                milestone_id,
                format!("multiple active beads present in lineage: {bead_list}"),
            ));
        }
    };

    snapshot.progress.in_progress_beads = current_bead_runs
        .iter()
        .filter(|entry| !entry.outcome.is_terminal())
        .count() as u32;
    snapshot.progress.completed_beads = current_bead_runs
        .iter()
        .filter(|entry| entry.outcome == TaskRunOutcome::Succeeded)
        .count() as u32;
    snapshot.progress.failed_beads = current_bead_runs
        .iter()
        .filter(|entry| entry.outcome == TaskRunOutcome::Failed)
        .count() as u32;
    snapshot.progress.skipped_beads = current_bead_runs
        .iter()
        .filter(|entry| entry.outcome == TaskRunOutcome::Skipped)
        .count() as u32;

    if snapshot.active_bead.is_some() {
        if snapshot.status.is_terminal() {
            return Err(snapshot_corrupt_record(
                milestone_id,
                format!(
                    "status is '{}' but lineage still has an active bead",
                    snapshot.status
                ),
            ));
        }
        snapshot.status = MilestoneStatus::Active;
    } else if snapshot.status == MilestoneStatus::Active
        || (!snapshot.status.is_terminal()
            && snapshot.status != MilestoneStatus::Paused
            && has_any_task_runs)
    {
        snapshot.status = MilestoneStatus::Ready;
    }

    Ok(())
}

fn event_type_for_outcome(outcome: TaskRunOutcome) -> MilestoneEventType {
    match outcome {
        TaskRunOutcome::Succeeded => MilestoneEventType::BeadCompleted,
        TaskRunOutcome::Failed => MilestoneEventType::BeadFailed,
        TaskRunOutcome::Skipped => MilestoneEventType::BeadSkipped,
        TaskRunOutcome::Pending | TaskRunOutcome::Running => {
            unreachable!("non-terminal outcomes are rejected before journal construction")
        }
    }
}

#[cfg(test)]
fn journal_contains_event(
    journal: &[MilestoneJournalEvent],
    candidate: &MilestoneJournalEvent,
) -> bool {
    journal.iter().any(|existing| {
        existing.timestamp == candidate.timestamp
            && existing.event_type == candidate.event_type
            && existing.bead_id == candidate.bead_id
            && existing.details == candidate.details
    })
}

// ── Service use cases ───────────────────────────────────────────────────────

/// Input for creating a new milestone.
pub struct CreateMilestoneInput {
    pub id: String,
    pub name: String,
    pub description: String,
}

/// Create a new milestone with initial snapshot and journal event.
pub fn create_milestone(
    store: &impl MilestoneStorePort,
    base_dir: &Path,
    input: CreateMilestoneInput,
    now: DateTime<Utc>,
) -> AppResult<MilestoneRecord> {
    let milestone_id = MilestoneId::new(&input.id)?;

    if store.milestone_exists(base_dir, &milestone_id)? {
        return Err(AppError::DuplicateProject {
            project_id: input.id,
        });
    }

    let record = MilestoneRecord::new(milestone_id, input.name, input.description, now);
    let snapshot = MilestoneSnapshot::initial(now);
    let event = MilestoneJournalEvent::new(MilestoneEventType::Created, now)
        .with_details("Milestone created");
    let journal_line = event.to_ndjson_line().map_err(AppError::SerdeJson)?;

    store.create_milestone_atomic(base_dir, &record, &snapshot, &journal_line)?;
    Ok(record)
}

/// Load a milestone record by ID.
pub fn load_milestone(
    store: &impl MilestoneStorePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneRecord> {
    store.read_milestone_record(base_dir, milestone_id)
}

/// Load a milestone's current status snapshot.
pub fn load_snapshot(
    snapshot_store: &impl MilestoneSnapshotPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneSnapshot> {
    snapshot_store.read_snapshot(base_dir, milestone_id)
}

/// List all milestone IDs in the workspace.
pub fn list_milestones(
    store: &impl MilestoneStorePort,
    base_dir: &Path,
) -> AppResult<Vec<MilestoneId>> {
    store.list_milestone_ids(base_dir)
}

/// Update the milestone status and append a journal event.
pub fn update_status(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    new_status: MilestoneStatus,
    now: DateTime<Utc>,
) -> AppResult<MilestoneSnapshot> {
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, || {
        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        let old_status = snapshot.status;
        snapshot.status = new_status;

        if new_status.is_terminal() {
            snapshot.active_bead = None;
        }

        snapshot.updated_at = now;

        validate_snapshot(&snapshot, milestone_id)?;

        snapshot_store.write_snapshot(base_dir, milestone_id, &snapshot)?;

        let event = MilestoneJournalEvent::new(MilestoneEventType::StatusChanged, now)
            .with_details(format!("{old_status} -> {new_status}"));
        let line = event.to_ndjson_line().map_err(AppError::SerdeJson)?;
        journal_store.append_event(base_dir, milestone_id, &line)?;

        Ok(snapshot)
    })
}

/// Persist a plan bundle: writes plan.json, plan.md, and updates the snapshot.
pub fn persist_plan(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    plan_store: &impl MilestonePlanPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bundle: &MilestoneBundle,
    now: DateTime<Utc>,
) -> AppResult<MilestoneSnapshot> {
    let plan_json = render_plan_json(bundle).map_err(AppError::SerdeJson)?;
    let plan_md = render_plan_md(bundle);

    let plan_hash = {
        let mut hasher = Sha256::new();
        hasher.update(plan_json.as_bytes());
        format!("{:x}", hasher.finalize())
    };

    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, move || {
        plan_store.write_plan_json(base_dir, milestone_id, &plan_json)?;
        plan_store.write_plan_md(base_dir, milestone_id, &plan_md)?;

        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        snapshot.plan_hash = Some(plan_hash);
        snapshot.plan_version = snapshot.plan_version.saturating_add(1);
        snapshot.progress = MilestoneProgress {
            total_beads: bundle.bead_count() as u32,
            ..MilestoneProgress::default()
        };
        snapshot.updated_at = now;

        let event_type = if snapshot.plan_version == 1 {
            MilestoneEventType::PlanDrafted
        } else {
            MilestoneEventType::PlanUpdated
        };

        validate_snapshot(&snapshot, milestone_id)?;
        snapshot_store.write_snapshot(base_dir, milestone_id, &snapshot)?;

        let event = MilestoneJournalEvent::new(event_type, now).with_details(format!(
            "Plan v{} with {} beads",
            snapshot.plan_version,
            bundle.bead_count()
        ));
        let line = event.to_ndjson_line().map_err(AppError::SerdeJson)?;
        journal_store.append_event(base_dir, milestone_id, &line)?;

        Ok(snapshot)
    })
}

/// Record the start of a bead task run.
#[allow(clippy::too_many_arguments)]
pub fn record_bead_start(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: Option<&str>,
    plan_hash: Option<&str>,
    now: DateTime<Utc>,
) -> AppResult<()> {
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, || {
        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        reconcile_snapshot_from_lineage(
            &mut snapshot,
            milestone_id,
            lineage_store.read_task_runs(base_dir, milestone_id)?,
        )?;
        if snapshot.status.is_terminal() {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "cannot start bead '{bead_id}': milestone '{milestone_id}' is already {}",
                    snapshot.status,
                ),
            });
        }

        let started_entry = lineage_store.record_task_run_start(
            base_dir,
            milestone_id,
            bead_id,
            project_id,
            run_id,
            plan_hash,
            now,
        )?;

        reconcile_snapshot_from_lineage(
            &mut snapshot,
            milestone_id,
            lineage_store.read_task_runs(base_dir, milestone_id)?,
        )?;
        snapshot.updated_at = snapshot.updated_at.max(now).max(started_entry.started_at);
        validate_snapshot(&snapshot, milestone_id)?;
        snapshot_store.write_snapshot(base_dir, milestone_id, &snapshot)?;

        let event =
            MilestoneJournalEvent::new(MilestoneEventType::BeadStarted, started_entry.started_at)
                .with_bead(bead_id)
                .with_details(format!("project={project_id}"));
        let _ = journal_store.append_event_if_missing(base_dir, milestone_id, &event)?;

        Ok(())
    })
}

/// Record the completion of a bead task run.
///
/// This finalizes the existing lineage row created by [`record_bead_start`] and
/// updates snapshot + journal state in the same flow. `started_at` is retained
/// for compatibility with existing callers, but the persisted start time is read
/// from the existing lineage row instead of appending a second entry.
#[allow(clippy::too_many_arguments)]
pub fn record_bead_completion(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: Option<&str>,
    plan_hash: Option<&str>,
    outcome: TaskRunOutcome,
    outcome_detail: Option<&str>,
    _started_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> AppResult<()> {
    update_task_run(
        snapshot_store,
        journal_store,
        lineage_store,
        base_dir,
        milestone_id,
        bead_id,
        project_id,
        run_id,
        plan_hash,
        outcome,
        outcome_detail.map(str::to_owned),
        now,
    )
}

/// Read the journal for a milestone.
pub fn read_journal(
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<Vec<MilestoneJournalEvent>> {
    journal_store.read_journal(base_dir, milestone_id)
}

/// Read the task-run lineage for a milestone.
pub fn read_task_runs(
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<Vec<TaskRunEntry>> {
    lineage_store.read_task_runs(base_dir, milestone_id)
}

/// Find all task runs for a specific bead in chronological order.
pub fn find_runs_for_bead(
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> AppResult<Vec<TaskRunEntry>> {
    lineage_store.find_runs_for_bead(base_dir, milestone_id, bead_id)
}

/// Update an existing task run's outcome after completion.
///
/// The lineage row remains the durable source of truth. Snapshot counters and
/// journal events are repaired from canonical lineage state, so replaying the
/// same terminal completion can finish a partially failed write without
/// duplicating counters or events.
#[allow(clippy::too_many_arguments)]
pub fn update_task_run(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: Option<&str>,
    plan_hash: Option<&str>,
    outcome: TaskRunOutcome,
    outcome_detail: Option<String>,
    finished_at: DateTime<Utc>,
) -> AppResult<()> {
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, move || {
        if !outcome.is_terminal() {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "cannot finalize bead '{bead_id}' with non-terminal outcome '{outcome}'"
                ),
            });
        }

        let finalized_run = lineage_store.update_task_run(
            base_dir,
            milestone_id,
            bead_id,
            project_id,
            run_id,
            plan_hash,
            outcome,
            outcome_detail,
            finished_at,
        )?;

        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        reconcile_snapshot_from_lineage(
            &mut snapshot,
            milestone_id,
            lineage_store.read_task_runs(base_dir, milestone_id)?,
        )?;
        let event_timestamp = finalized_run.finished_at.unwrap_or(finished_at);
        snapshot.updated_at = snapshot.updated_at.max(finished_at).max(event_timestamp);
        validate_snapshot(&snapshot, milestone_id)?;
        snapshot_store.write_snapshot(base_dir, milestone_id, &snapshot)?;

        let event = MilestoneJournalEvent::new(
            event_type_for_outcome(finalized_run.outcome),
            event_timestamp,
        )
        .with_bead(&finalized_run.bead_id)
        .with_details(finalized_run.completion_journal_details());
        let _ = journal_store.append_event_if_missing(base_dir, milestone_id, &event)?;

        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::Duration;

    use crate::adapters::fs::{
        FsMilestoneJournalStore, FsMilestonePlanStore, FsMilestoneSnapshotStore, FsMilestoneStore,
        FsTaskRunLineageStore,
    };

    struct FailSecondJournalAppend {
        append_calls: Cell<u32>,
    }

    impl MilestoneJournalPort for FailSecondJournalAppend {
        fn read_journal(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<Vec<MilestoneJournalEvent>> {
            FsMilestoneJournalStore.read_journal(base_dir, milestone_id)
        }

        fn append_event(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            line: &str,
        ) -> AppResult<()> {
            let next_call = self.append_calls.get() + 1;
            self.append_calls.set(next_call);
            if next_call == 2 {
                return Err(std::io::Error::other("simulated completion journal failure").into());
            }

            FsMilestoneJournalStore.append_event(base_dir, milestone_id, line)
        }

        fn append_event_if_missing(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            event: &MilestoneJournalEvent,
        ) -> AppResult<bool> {
            let journal = self.read_journal(base_dir, milestone_id)?;
            if journal_contains_event(&journal, event) {
                return Ok(false);
            }

            let line = event.to_ndjson_line().map_err(AppError::SerdeJson)?;
            self.append_event(base_dir, milestone_id, &line)?;
            Ok(true)
        }
    }

    struct FailFirstJournalAppend {
        append_calls: Cell<u32>,
    }

    impl MilestoneJournalPort for FailFirstJournalAppend {
        fn read_journal(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<Vec<MilestoneJournalEvent>> {
            FsMilestoneJournalStore.read_journal(base_dir, milestone_id)
        }

        fn append_event(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            line: &str,
        ) -> AppResult<()> {
            let next_call = self.append_calls.get() + 1;
            self.append_calls.set(next_call);
            if next_call == 1 {
                return Err(std::io::Error::other("simulated start journal failure").into());
            }

            FsMilestoneJournalStore.append_event(base_dir, milestone_id, line)
        }

        fn append_event_if_missing(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            event: &MilestoneJournalEvent,
        ) -> AppResult<bool> {
            let journal = self.read_journal(base_dir, milestone_id)?;
            if journal_contains_event(&journal, event) {
                return Ok(false);
            }

            let line = event.to_ndjson_line().map_err(AppError::SerdeJson)?;
            self.append_event(base_dir, milestone_id, &line)?;
            Ok(true)
        }
    }

    struct BlockingSnapshotStore {
        entered_tx: Mutex<Option<mpsc::Sender<()>>>,
        release_rx: Mutex<Option<mpsc::Receiver<()>>>,
    }

    impl BlockingSnapshotStore {
        fn new(entered_tx: mpsc::Sender<()>, release_rx: mpsc::Receiver<()>) -> Self {
            Self {
                entered_tx: Mutex::new(Some(entered_tx)),
                release_rx: Mutex::new(Some(release_rx)),
            }
        }
    }

    /// Single-use test helper that coordinates exactly one contended lock
    /// acquisition. A second use indicates the test setup is no longer matching
    /// its intended one-shot synchronization contract.
    struct LockAttemptSnapshotStore {
        attempted_tx: Mutex<Option<mpsc::Sender<()>>>,
        proceed_rx: Mutex<Option<mpsc::Receiver<()>>>,
    }

    impl LockAttemptSnapshotStore {
        fn new(attempted_tx: mpsc::Sender<()>, proceed_rx: mpsc::Receiver<()>) -> Self {
            Self {
                attempted_tx: Mutex::new(Some(attempted_tx)),
                proceed_rx: Mutex::new(Some(proceed_rx)),
            }
        }
    }

    impl MilestoneSnapshotPort for LockAttemptSnapshotStore {
        fn read_snapshot(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<MilestoneSnapshot> {
            FsMilestoneSnapshotStore.read_snapshot(base_dir, milestone_id)
        }

        fn write_snapshot(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            snapshot: &MilestoneSnapshot,
        ) -> AppResult<()> {
            FsMilestoneSnapshotStore.write_snapshot(base_dir, milestone_id, snapshot)
        }

        fn with_milestone_write_lock<T, F>(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            operation: F,
        ) -> AppResult<T>
        where
            F: FnOnce() -> AppResult<T>,
        {
            let attempted_tx = self
                .attempted_tx
                .lock()
                .expect("lock attempted_tx")
                .take()
                .expect("LockAttemptSnapshotStore is single-use");
            attempted_tx
                .send(())
                .expect("signal lock attempt before contention");

            let proceed_rx = self
                .proceed_rx
                .lock()
                .expect("lock proceed_rx")
                .take()
                .expect("LockAttemptSnapshotStore is single-use");
            proceed_rx.recv().expect("allow lock contention");

            FsMilestoneSnapshotStore.with_milestone_write_lock(base_dir, milestone_id, operation)
        }
    }

    impl MilestoneSnapshotPort for BlockingSnapshotStore {
        fn read_snapshot(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<MilestoneSnapshot> {
            FsMilestoneSnapshotStore.read_snapshot(base_dir, milestone_id)
        }

        fn write_snapshot(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            snapshot: &MilestoneSnapshot,
        ) -> AppResult<()> {
            if snapshot.status == MilestoneStatus::Ready && snapshot.active_bead.is_none() {
                if let Some(entered_tx) = self.entered_tx.lock().expect("lock entered_tx").take() {
                    entered_tx.send(()).expect("signal blocked snapshot write");
                }
                if let Some(release_rx) = self.release_rx.lock().expect("lock release_rx").take() {
                    release_rx.recv().expect("release blocked snapshot write");
                }
            }

            FsMilestoneSnapshotStore.write_snapshot(base_dir, milestone_id, snapshot)
        }

        fn with_milestone_write_lock<T, F>(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            operation: F,
        ) -> AppResult<T>
        where
            F: FnOnce() -> AppResult<T>,
        {
            FsMilestoneSnapshotStore.with_milestone_write_lock(base_dir, milestone_id, operation)
        }
    }

    fn setup_workspace(dir: &Path) {
        std::fs::create_dir_all(dir.join(".ralph-burning/milestones")).unwrap();
    }

    #[test]
    fn create_and_load_milestone() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "test-ms".to_owned(),
                name: "Test Milestone".to_owned(),
                description: "A test milestone".to_owned(),
            },
            now,
        )?;

        assert_eq!(record.id.as_str(), "test-ms");

        let loaded = load_milestone(&store, base, &record.id)?;
        assert_eq!(loaded.name, "Test Milestone");

        let ids = list_milestones(&store, base)?;
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].as_str(), "test-ms");
        Ok(())
    }

    #[test]
    fn create_duplicate_fails() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "dup".to_owned(),
                name: "First".to_owned(),
                description: "first".to_owned(),
            },
            now,
        )?;

        let result = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "dup".to_owned(),
                name: "Second".to_owned(),
                description: "second".to_owned(),
            },
            now,
        );
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn update_status_transitions() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "status-test".to_owned(),
                name: "Status Test".to_owned(),
                description: "testing status".to_owned(),
            },
            now,
        )?;

        let snapshot = update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Ready,
            now,
        )?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);

        let journal = read_journal(&journal_store, base, &record.id)?;
        assert!(journal.len() >= 2);
        Ok(())
    }

    #[test]
    fn persist_plan_updates_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        use crate::contexts::milestone_record::bundle::*;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "plan-test".to_owned(),
                name: "Plan Test".to_owned(),
                description: "testing plans".to_owned(),
            },
            now,
        )?;

        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "plan-test".to_owned(),
                name: "Plan Test".to_owned(),
            },
            executive_summary: "Test plan.".to_owned(),
            goals: vec!["Goal 1".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Tests pass".to_owned(),
                covered_by: vec![],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: None,
                beads: vec![BeadProposal {
                    title: "Implement feature".to_owned(),
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

        let snapshot = persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle,
            now,
        )?;

        assert_eq!(snapshot.plan_version, 1);
        assert!(snapshot.plan_hash.is_some());
        assert_eq!(snapshot.progress.total_beads, 1);

        let plan_json = plan_store.read_plan_json(base, &record.id)?;
        assert!(plan_json.contains("Plan Test"));

        let plan_md = plan_store.read_plan_md(base, &record.id)?;
        assert!(plan_md.contains("# Plan Test"));
        Ok(())
    }

    #[test]
    fn bead_start_and_completion_tracking() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "bead-track".to_owned(),
                name: "Bead Tracking".to_owned(),
                description: "testing bead tracking".to_owned(),
            },
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("abc123"),
            now,
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Active);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(snapshot.progress.in_progress_beads, 1);

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("abc123"),
            TaskRunOutcome::Succeeded,
            Some("All tests passed"),
            now,
            now,
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);
        assert_eq!(snapshot.progress.skipped_beads, 0);

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].milestone_id.as_deref(), Some(record.id.as_str()));
        assert_eq!(runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(runs[0].plan_hash.as_deref(), Some("abc123"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(runs[0].outcome_detail.as_deref(), Some("All tests passed"));

        let journal = read_journal(&journal_store, base, &record.id)?;
        assert_eq!(
            journal.last().map(|event| event.event_type),
            Some(MilestoneEventType::BeadCompleted)
        );
        Ok(())
    }

    #[test]
    fn paused_milestone_stays_paused_when_completion_reconciles_lineage(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "paused-completion-test".to_owned(),
                name: "Paused Completion Test".to_owned(),
                description: "preserve paused milestone state during completion repair".to_owned(),
            },
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            now,
        )?;
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Paused,
            now + chrono::Duration::seconds(1),
        )?;

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("completed while paused"),
            now,
            now + chrono::Duration::seconds(2),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Paused);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);
        Ok(())
    }

    #[test]
    fn terminal_milestone_rejects_bead_start_without_mutating_lineage(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "terminal-start-test".to_owned(),
                name: "Terminal Start Test".to_owned(),
                description: "reject starts on terminal milestones".to_owned(),
            },
            now,
        )?;

        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Completed,
            now + chrono::Duration::seconds(1),
        )?;

        let error = record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            now + chrono::Duration::seconds(2),
        )
        .expect_err("terminal milestones must reject new bead starts");
        assert!(error.to_string().contains("already completed"));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Completed);
        assert_eq!(snapshot.active_bead, None);

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert!(runs.is_empty());

        let journal = read_journal(&journal_store, base, &record.id)?;
        assert!(journal
            .iter()
            .all(|event| event.event_type != MilestoneEventType::BeadStarted));
        Ok(())
    }

    #[test]
    fn task_run_entry_serialization_with_new_fields() -> Result<(), Box<dyn std::error::Error>> {
        let now = Utc::now();
        let entry = TaskRunEntry {
            milestone_id: Some("ms-1".to_owned()),
            bead_id: "bead-1".to_owned(),
            project_id: "proj-1".to_owned(),
            run_id: Some("run-42".to_owned()),
            plan_hash: Some("sha256-abc".to_owned()),
            outcome: TaskRunOutcome::Succeeded,
            outcome_detail: Some("Completed with 3 warnings".to_owned()),
            started_at: now,
            finished_at: Some(now),
        };
        let json = serde_json::to_string(&entry)?;
        let parsed: TaskRunEntry = serde_json::from_str(&json)?;
        assert_eq!(parsed.milestone_id.as_deref(), Some("ms-1"));
        assert_eq!(parsed.run_id.as_deref(), Some("run-42"));
        assert_eq!(parsed.plan_hash.as_deref(), Some("sha256-abc"));
        assert_eq!(
            parsed.outcome_detail.as_deref(),
            Some("Completed with 3 warnings")
        );
        Ok(())
    }

    #[test]
    fn task_run_entry_backward_compat_without_new_fields() -> Result<(), Box<dyn std::error::Error>>
    {
        // Simulate old-format JSON without milestone_id, run_id, plan_hash, outcome_detail
        let old_json = r#"{"bead_id":"b1","project_id":"p1","outcome":"running","started_at":"2025-01-01T00:00:00Z"}"#;
        let parsed: TaskRunEntry = serde_json::from_str(old_json)?;
        assert_eq!(parsed.bead_id, "b1");
        assert!(parsed.milestone_id.is_none());
        assert!(parsed.run_id.is_none());
        assert!(parsed.plan_hash.is_none());
        assert!(parsed.outcome_detail.is_none());
        assert!(parsed.finished_at.is_none());
        Ok(())
    }

    #[test]
    fn read_task_runs_backfills_milestone_id_for_legacy_entries(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "legacy-run-test".to_owned(),
                name: "Legacy Run Test".to_owned(),
                description: "testing legacy task run backfill".to_owned(),
            },
            now,
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        std::fs::write(
            &task_runs_path,
            r#"{"bead_id":"bead-1","project_id":"project-1","outcome":"running","started_at":"2025-01-01T00:00:00Z"}"#,
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].milestone_id.as_deref(), Some(record.id.as_str()));
        assert_eq!(runs[0].bead_id, "bead-1");
        Ok(())
    }

    #[test]
    fn append_task_run_persists_milestone_id_for_new_entries(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "append-run-milestone-id-test".to_owned(),
                name: "Append Run Milestone ID Test".to_owned(),
                description: "testing append_task_run milestone_id persistence".to_owned(),
            },
            now,
        )?;

        lineage_store.append_task_run(
            base,
            &record.id,
            &TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now,
                finished_at: None,
            },
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        let raw = std::fs::read_to_string(task_runs_path)?;
        assert!(raw.contains(&format!(r#""milestone_id":"{}""#, record.id)));
        Ok(())
    }

    #[test]
    fn find_runs_for_bead_filters_correctly() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "find-bead-test".to_owned(),
                name: "Find Bead Test".to_owned(),
                description: "testing find_runs_for_bead".to_owned(),
            },
            now,
        )?;

        // Start and complete bead-1
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            None,
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            None,
            TaskRunOutcome::Failed,
            Some("Build error"),
            now,
            now,
        )?;

        // Start and complete bead-2 (different bead)
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-2",
            "project-2",
            None,
            None,
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-2",
            "project-2",
            None,
            None,
            TaskRunOutcome::Succeeded,
            None,
            now,
            now,
        )?;

        // Retry bead-1 (second attempt)
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-3",
            Some("run-2"),
            None,
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-3",
            Some("run-2"),
            None,
            TaskRunOutcome::Succeeded,
            Some("Passed on retry"),
            now,
            now,
        )?;

        // find_runs_for_bead should return only bead-1 entries
        let bead1_runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(bead1_runs.len(), 2);
        for run in &bead1_runs {
            assert_eq!(run.milestone_id.as_deref(), Some(record.id.as_str()));
            assert_eq!(run.bead_id, "bead-1");
        }
        assert_eq!(bead1_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(bead1_runs[1].outcome, TaskRunOutcome::Succeeded);

        // bead-2 should have exactly 1 entry
        let bead2_runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-2")?;
        assert_eq!(bead2_runs.len(), 1);
        assert_eq!(bead2_runs[0].outcome, TaskRunOutcome::Succeeded);

        // Non-existent bead should return empty
        let empty_runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-999")?;
        assert!(empty_runs.is_empty());

        Ok(())
    }

    #[test]
    fn find_runs_for_bead_collapses_legacy_start_completion_pairs(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "legacy-query-test".to_owned(),
                name: "Legacy Query Test".to_owned(),
                description: "testing legacy task-run query collapse".to_owned(),
            },
            now,
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        let first_started = now;
        let second_started = now + chrono::Duration::seconds(10);
        let legacy_lines = [
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: first_started,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("legacy failure".to_owned()),
                started_at: first_started,
                finished_at: Some(first_started + chrono::Duration::seconds(5)),
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-2".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: second_started,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-2".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: Some("legacy success".to_owned()),
                started_at: second_started,
                finished_at: Some(second_started + chrono::Duration::seconds(5)),
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-2".to_owned(),
                project_id: "other-project".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now + chrono::Duration::seconds(20),
                finished_at: None,
            })?,
        ]
        .join("\n");
        std::fs::write(&task_runs_path, format!("{legacy_lines}\n"))?;

        let runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(runs[0].outcome_detail.as_deref(), Some("legacy failure"));
        assert_eq!(runs[1].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(runs[1].outcome_detail.as_deref(), Some("legacy success"));
        assert!(runs.iter().all(|run| run.outcome.is_terminal()));
        assert!(runs
            .iter()
            .all(|run| run.milestone_id.as_deref() == Some(record.id.as_str())));
        Ok(())
    }

    #[test]
    fn find_runs_for_bead_deduplicates_replayed_terminal_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "duplicate-terminal-query-test".to_owned(),
                name: "Duplicate Terminal Query Test".to_owned(),
                description: "dedupe replayed completion rows in bead audit queries".to_owned(),
            },
            now,
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        let started_at = now;
        let finished_at = now + chrono::Duration::seconds(5);
        let duplicated_lines = [
            serde_json::to_string(&TaskRunEntry {
                milestone_id: Some(record.id.to_string()),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: Some(record.id.to_string()),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: None,
                started_at,
                finished_at: Some(finished_at),
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: None,
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: Some("replayed completion".to_owned()),
                started_at,
                finished_at: Some(finished_at),
            })?,
        ]
        .join("\n");
        std::fs::write(&task_runs_path, format!("{duplicated_lines}\n"))?;

        let runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(
            runs[0].outcome_detail.as_deref(),
            Some("replayed completion")
        );
        Ok(())
    }

    #[test]
    fn find_runs_for_bead_preserves_same_timestamp_legacy_retries(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "same-timestamp-legacy-query-test".to_owned(),
                name: "Same Timestamp Legacy Query Test".to_owned(),
                description: "same-timestamp legacy retries stay visible in bead audit queries"
                    .to_owned(),
            },
            now,
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        let started_at = now;
        let raw_lines = [
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("first retry".to_owned()),
                started_at,
                finished_at: Some(started_at + chrono::Duration::seconds(1)),
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("second retry".to_owned()),
                started_at,
                finished_at: Some(started_at + chrono::Duration::seconds(2)),
            })?,
        ]
        .join("\n");
        std::fs::write(&task_runs_path, format!("{raw_lines}\n"))?;

        let runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].outcome_detail.as_deref(), Some("first retry"));
        assert_eq!(runs[1].outcome_detail.as_deref(), Some("second retry"));
        Ok(())
    }

    #[test]
    fn update_task_run_modifies_outcome() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "update-run-test".to_owned(),
                name: "Update Run Test".to_owned(),
                description: "testing update_task_run".to_owned(),
            },
            now,
        )?;

        // Start bead-1 (appends a Running entry)
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            None,
            None,
            now,
        )?;

        // Update the running entry via update_task_run
        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("All checks passed".to_owned()),
            now,
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].milestone_id.as_deref(), Some(record.id.as_str()));
        assert_eq!(runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(runs[0].outcome_detail.as_deref(), Some("All checks passed"));
        assert!(runs[0].finished_at.is_some());

        let journal = read_journal(&journal_store, base, &record.id)?;
        assert_eq!(
            journal.last().map(|event| event.event_type),
            Some(MilestoneEventType::BeadCompleted)
        );
        Ok(())
    }

    #[test]
    fn completion_rejects_conflicting_plan_hash() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "completion-plan-conflict-test".to_owned(),
                name: "Completion Plan Conflict Test".to_owned(),
                description: "reject conflicting plan hashes during completion".to_owned(),
            },
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            now,
        )?;

        let error = record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("should fail"),
            now,
            now + chrono::Duration::seconds(5),
        )
        .expect_err("conflicting plan hashes must be rejected");
        assert!(error.to_string().contains("conflicting plan_hash"));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Active);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(snapshot.progress.in_progress_beads, 1);
        assert_eq!(snapshot.progress.completed_beads, 0);

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Running);
        assert!(runs[0].finished_at.is_none());

        let journal = read_journal(&journal_store, base, &record.id)?;
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| {
                matches!(
                    event.event_type,
                    MilestoneEventType::BeadCompleted
                        | MilestoneEventType::BeadFailed
                        | MilestoneEventType::BeadSkipped
                )
            })
            .collect();
        assert!(completion_events.is_empty());
        Ok(())
    }

    #[test]
    fn multiple_retries_visible_for_same_bead() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "retry-test".to_owned(),
                name: "Retry Test".to_owned(),
                description: "testing multiple retries".to_owned(),
            },
            now,
        )?;

        // Attempt 1: fail
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-a",
            Some("run-1"),
            Some("plan-v1"),
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-a",
            Some("run-1"),
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("Compile error"),
            now,
            now,
        )?;

        // Attempt 2: succeed
        let retry_started = now + chrono::Duration::seconds(10);
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-b",
            Some("run-2"),
            Some("plan-v2"),
            retry_started,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-b",
            Some("run-2"),
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("All green"),
            retry_started,
            retry_started + chrono::Duration::seconds(1),
        )?;

        let bead1_runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(bead1_runs.len(), 2);
        assert_eq!(bead1_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(bead1_runs[0].plan_hash.as_deref(), Some("plan-v1"));
        // Second attempt succeeded
        assert_eq!(bead1_runs[1].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(bead1_runs[1].plan_hash.as_deref(), Some("plan-v2"));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);
        assert_eq!(snapshot.progress.failed_beads, 0);
        assert_eq!(snapshot.progress.skipped_beads, 0);
        Ok(())
    }

    #[test]
    fn legacy_paired_rows_do_not_block_no_run_id_retry_finalization(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "legacy-update-test".to_owned(),
                name: "Legacy Update Test".to_owned(),
                description: "testing legacy paired rows during retry finalization".to_owned(),
            },
            now,
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        let legacy_started = now;
        let legacy_lines = [
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: legacy_started,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("legacy failure".to_owned()),
                started_at: legacy_started,
                finished_at: Some(legacy_started + chrono::Duration::seconds(5)),
            })?,
        ]
        .join("\n");
        std::fs::write(&task_runs_path, format!("{legacy_lines}\n"))?;

        let retry_started = now + chrono::Duration::seconds(30);
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            None,
            Some("plan-retry"),
            retry_started,
        )?;
        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            None,
            Some("plan-retry"),
            TaskRunOutcome::Succeeded,
            Some("retry completed".to_owned()),
            retry_started + chrono::Duration::seconds(1),
        )?;

        let raw_runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(raw_runs.len(), 3);

        let bead_runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(bead_runs.len(), 2);
        assert_eq!(bead_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(bead_runs[1].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(bead_runs[1].plan_hash.as_deref(), Some("plan-retry"));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);
        assert_eq!(snapshot.progress.failed_beads, 0);
        Ok(())
    }

    #[test]
    fn legacy_terminal_replay_without_run_id_repairs_milestone_state(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "legacy-replay-test".to_owned(),
                name: "Legacy Replay Test".to_owned(),
                description: "testing replay repair against legacy paired rows".to_owned(),
            },
            now,
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        let started_at = now;
        let finished_at = now + chrono::Duration::seconds(5);
        let legacy_lines = [
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("legacy failure".to_owned()),
                started_at,
                finished_at: Some(finished_at),
            })?,
        ]
        .join("\n");
        std::fs::write(&task_runs_path, format!("{legacy_lines}\n"))?;

        let mut stale_snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        stale_snapshot.status = MilestoneStatus::Active;
        stale_snapshot.active_bead = Some("bead-1".to_owned());
        stale_snapshot.progress.in_progress_beads = 1;
        stale_snapshot.updated_at = started_at;
        snapshot_store.write_snapshot(base, &record.id, &stale_snapshot)?;

        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            None,
            Some("plan-replayed"),
            TaskRunOutcome::Failed,
            Some("legacy failure".to_owned()),
            now + chrono::Duration::seconds(20),
        )?;

        let runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-replayed"));
        assert_eq!(runs[0].finished_at, Some(finished_at));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.failed_beads, 1);
        assert_eq!(snapshot.updated_at, now + chrono::Duration::seconds(20));

        let journal = read_journal(&journal_store, base, &record.id)?;
        let failed_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .collect();
        assert_eq!(failed_events.len(), 1);
        assert_eq!(failed_events[0].timestamp, finished_at);
        Ok(())
    }

    #[test]
    fn completion_repair_moves_stale_planning_snapshot_to_ready(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "planning-repair-test".to_owned(),
                name: "Planning Repair Test".to_owned(),
                description: "testing stale planning snapshot repair after completion".to_owned(),
            },
            now,
        )?;

        lineage_store.append_task_run(
            base,
            &record.id,
            &TaskRunEntry {
                milestone_id: Some(record.id.to_string()),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now,
                finished_at: None,
            },
        )?;

        let stale_snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(stale_snapshot.status, MilestoneStatus::Planning);

        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("planning snapshot repaired".to_owned()),
            now + chrono::Duration::seconds(5),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);
        Ok(())
    }

    #[test]
    fn skipped_runs_are_tracked_without_counting_as_failures(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "skip-test".to_owned(),
                name: "Skip Test".to_owned(),
                description: "testing skipped task runs".to_owned(),
            },
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-skip"),
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-skip"),
            TaskRunOutcome::Skipped,
            Some("Handled by another milestone"),
            now,
            now,
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.failed_beads, 0);
        assert_eq!(snapshot.progress.skipped_beads, 1);

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Skipped);

        let journal = read_journal(&journal_store, base, &record.id)?;
        assert_eq!(
            journal.last().map(|event| event.event_type),
            Some(MilestoneEventType::BeadSkipped)
        );
        Ok(())
    }

    #[test]
    fn completion_flow_rejects_duplicate_finalization() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "duplicate-finalize-test".to_owned(),
                name: "Duplicate Finalize Test".to_owned(),
                description: "testing duplicate completion protection".to_owned(),
            },
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            None,
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            None,
            TaskRunOutcome::Succeeded,
            Some("first completion"),
            now,
            now,
        )?;

        let error = update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            None,
            TaskRunOutcome::Failed,
            Some("second completion".to_owned()),
            now,
        )
        .expect_err("duplicate terminal updates should fail");
        assert!(error.to_string().contains("already finalized"));
        assert!(error.to_string().contains("run=run-1"));
        Ok(())
    }

    #[test]
    fn completion_retry_repairs_snapshot_and_journal_without_double_counting(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let flaky_journal_store = FailSecondJournalAppend {
            append_calls: Cell::new(0),
        };
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "completion-retry-test".to_owned(),
                name: "Completion Retry Test".to_owned(),
                description: "testing completion repair after journal failure".to_owned(),
            },
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &flaky_journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            now,
        )?;

        let failure = record_bead_completion(
            &snapshot_store,
            &flaky_journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("journal failed once"),
            now,
            now,
        )
        .expect_err("completion should fail when the journal append fails");
        assert!(failure
            .to_string()
            .contains("simulated completion journal failure"));

        let runs_after_failure = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs_after_failure.len(), 1);
        assert_eq!(runs_after_failure[0].outcome, TaskRunOutcome::Succeeded);

        let snapshot_after_failure = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot_after_failure
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot_after_failure.status, MilestoneStatus::Ready);
        assert_eq!(snapshot_after_failure.progress.in_progress_beads, 0);
        assert_eq!(snapshot_after_failure.progress.completed_beads, 1);
        let repaired_updated_at = now + chrono::Duration::seconds(10);
        let mut drifted_snapshot = snapshot_after_failure.clone();
        drifted_snapshot.updated_at = repaired_updated_at;
        snapshot_store.write_snapshot(base, &record.id, &drifted_snapshot)?;

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("journal failed once"),
            now,
            now + chrono::Duration::seconds(5),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);
        assert_eq!(snapshot.updated_at, repaired_updated_at);

        let journal = read_journal(&journal_store, base, &record.id)?;
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadCompleted)
            .collect();
        assert_eq!(completion_events.len(), 1);
        assert_eq!(completion_events[0].timestamp, now);
        let details: serde_json::Value =
            serde_json::from_str(completion_events[0].details.as_deref().unwrap())?;
        assert_eq!(
            details,
            serde_json::json!({
                "project_id": "project-1",
                "run_id": "run-1",
                "plan_hash": "plan-v1",
                "outcome": "succeeded",
                "outcome_detail": "journal failed once",
            })
        );
        Ok(())
    }

    #[test]
    fn completion_replay_backfills_existing_journal_event_without_duplicate(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "completion-backfill-test".to_owned(),
                name: "Completion Backfill Test".to_owned(),
                description: "repair completion journal details on terminal replay".to_owned(),
            },
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            None,
            now,
        )?;

        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            None,
            TaskRunOutcome::Succeeded,
            None,
            now + chrono::Duration::seconds(1),
        )?;
        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("backfilled detail".to_owned()),
            now + chrono::Duration::seconds(2),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v2"));
        assert_eq!(runs[0].outcome_detail.as_deref(), Some("backfilled detail"));

        let journal = read_journal(&journal_store, base, &record.id)?;
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadCompleted)
            .collect();
        assert_eq!(completion_events.len(), 1);
        assert_eq!(
            completion_events[0].timestamp,
            now + chrono::Duration::seconds(1)
        );
        let details: serde_json::Value =
            serde_json::from_str(completion_events[0].details.as_deref().unwrap())?;
        assert_eq!(
            details,
            serde_json::json!({
                "project_id": "project-1",
                "run_id": "run-1",
                "plan_hash": "plan-v2",
                "outcome": "succeeded",
                "outcome_detail": "backfilled detail",
            })
        );
        Ok(())
    }

    #[test]
    fn completion_replay_repairs_legacy_journal_event_with_delimited_identifiers(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "legacy-delimited-completion-test".to_owned(),
                name: "Legacy Delimited Completion Test".to_owned(),
                description: "repair malformed legacy completion journal details on replay"
                    .to_owned(),
            },
            now,
        )?;

        let finished_at = now + chrono::Duration::seconds(5);
        lineage_store.append_task_run(
            base,
            &record.id,
            &TaskRunEntry {
                milestone_id: Some(record.id.to_string()),
                bead_id: "bead-1".to_owned(),
                project_id: "project, one".to_owned(),
                run_id: Some("run, 1".to_owned()),
                plan_hash: None,
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: None,
                started_at: now,
                finished_at: Some(finished_at),
            },
        )?;

        let journal_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("journal.ndjson");
        let legacy_completion =
            MilestoneJournalEvent::new(MilestoneEventType::BeadCompleted, finished_at)
                .with_bead("bead-1")
                .with_details("project=project, one, run=run, 1, outcome=succeeded");
        std::fs::write(
            &journal_path,
            format!("{}\n", legacy_completion.to_ndjson_line()?),
        )?;

        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project, one",
            Some("run, 1"),
            Some("plan, v2"),
            TaskRunOutcome::Succeeded,
            Some("backfilled detail".to_owned()),
            finished_at + chrono::Duration::seconds(1),
        )?;

        let journal = read_journal(&journal_store, base, &record.id)?;
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadCompleted)
            .collect();
        assert_eq!(completion_events.len(), 1);
        assert_eq!(completion_events[0].timestamp, finished_at);
        let details: serde_json::Value =
            serde_json::from_str(completion_events[0].details.as_deref().unwrap())?;
        assert_eq!(
            details,
            serde_json::json!({
                "project_id": "project, one",
                "run_id": "run, 1",
                "plan_hash": "plan, v2",
                "outcome": "succeeded",
                "outcome_detail": "backfilled detail",
            })
        );
        Ok(())
    }

    #[test]
    fn start_retry_with_same_run_id_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let flaky_journal_store = FailFirstJournalAppend {
            append_calls: Cell::new(0),
        };
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "start-retry-test".to_owned(),
                name: "Start Retry Test".to_owned(),
                description: "testing idempotent bead starts".to_owned(),
            },
            now,
        )?;

        let failure = record_bead_start(
            &snapshot_store,
            &flaky_journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            now,
        )
        .expect_err("start should fail when the journal append fails");
        assert!(failure
            .to_string()
            .contains("simulated start journal failure"));

        let runs_after_failure = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs_after_failure.len(), 1);
        assert_eq!(runs_after_failure[0].outcome, TaskRunOutcome::Running);

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            now + chrono::Duration::seconds(5),
        )?;

        let runs_after_retry = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs_after_retry.len(), 1);
        assert_eq!(runs_after_retry[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(runs_after_retry[0].started_at, now);

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Active);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(snapshot.progress.in_progress_beads, 1);
        assert_eq!(snapshot.updated_at, now + chrono::Duration::seconds(5));

        let journal = read_journal(&journal_store, base, &record.id)?;
        let start_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadStarted)
            .collect();
        assert_eq!(start_events.len(), 1);
        assert_eq!(start_events[0].timestamp, now);

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("completed after retry"),
            now,
            now + chrono::Duration::seconds(6),
        )?;

        let finalized_runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(finalized_runs.len(), 1);
        assert_eq!(finalized_runs[0].outcome, TaskRunOutcome::Succeeded);
        Ok(())
    }

    #[test]
    fn start_retry_rejects_conflicting_plan_hash() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "start-plan-conflict-test".to_owned(),
                name: "Start Plan Conflict Test".to_owned(),
                description: "reject conflicting plan hashes on idempotent start".to_owned(),
            },
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            now,
        )?;

        let error = record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v2"),
            now + chrono::Duration::seconds(5),
        )
        .expect_err("conflicting plan hashes must be rejected");
        assert!(error.to_string().contains("conflicting plan_hash"));

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Running);

        let journal = read_journal(&journal_store, base, &record.id)?;
        let start_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadStarted)
            .collect();
        assert_eq!(start_events.len(), 1);
        Ok(())
    }

    #[test]
    fn start_rejects_mixed_legacy_and_named_open_attempts() -> Result<(), Box<dyn std::error::Error>>
    {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "mixed-start-open-test".to_owned(),
                name: "Mixed Start Open Test".to_owned(),
                description:
                    "reject ambiguous start fallback when legacy and named attempts coexist"
                        .to_owned(),
            },
            now,
        )?;

        for entry in [
            TaskRunEntry {
                milestone_id: Some(record.id.to_string()),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now,
                finished_at: None,
            },
            TaskRunEntry {
                milestone_id: Some(record.id.to_string()),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-2".to_owned()),
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now + chrono::Duration::seconds(1),
                finished_at: None,
            },
        ] {
            lineage_store.append_task_run(base, &record.id, &entry)?;
        }

        let error = record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-3"),
            Some("plan-v3"),
            now + chrono::Duration::seconds(2),
        )
        .expect_err("mixed runless/named open attempts must be rejected as ambiguous");
        assert!(error
            .to_string()
            .contains("ambiguous existing running attempts"));

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 2);
        assert!(runs
            .iter()
            .all(|run| run.outcome == TaskRunOutcome::Running));

        let journal = read_journal(&journal_store, base, &record.id)?;
        let start_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadStarted)
            .collect();
        assert!(start_events.is_empty());
        Ok(())
    }

    #[test]
    fn explicit_retry_after_legacy_open_appends_new_attempt(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "legacy-open-retry-test".to_owned(),
                name: "Legacy Open Retry Test".to_owned(),
                description: "new explicit runs stay distinct from stale legacy open attempts"
                    .to_owned(),
            },
            now,
        )?;

        lineage_store.append_task_run(
            base,
            &record.id,
            &TaskRunEntry {
                milestone_id: None,
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now,
                finished_at: None,
            },
        )?;

        let retry_started_at = now + chrono::Duration::seconds(10);
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-2"),
            Some("plan-v2"),
            retry_started_at,
        )?;

        let running_attempts = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(running_attempts.len(), 2);
        assert_eq!(running_attempts[0].run_id, None);
        assert_eq!(running_attempts[1].run_id.as_deref(), Some("run-2"));

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-2"),
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("retry completed"),
            retry_started_at,
            retry_started_at + chrono::Duration::seconds(5),
        )?;

        let finalized_attempts = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(finalized_attempts.len(), 2);
        assert_eq!(finalized_attempts[0].run_id, None);
        assert_eq!(finalized_attempts[0].outcome, TaskRunOutcome::Running);
        assert_eq!(finalized_attempts[1].run_id.as_deref(), Some("run-2"));
        assert_eq!(finalized_attempts[1].outcome, TaskRunOutcome::Succeeded);

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.completed_beads, 1);
        Ok(())
    }

    #[test]
    fn concurrent_start_retries_share_one_lineage_row_and_start_event(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "concurrent-start-test".to_owned(),
                name: "Concurrent Start Test".to_owned(),
                description: "testing concurrent idempotent bead starts".to_owned(),
            },
            now,
        )?;

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let base_dir = base.to_path_buf();
        let milestone_id = record.id.clone();
        let mut handles = Vec::new();

        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            let base_dir = base_dir.clone();
            let milestone_id = milestone_id.clone();
            handles.push(std::thread::spawn(move || -> AppResult<()> {
                let snapshot_store = FsMilestoneSnapshotStore;
                let journal_store = FsMilestoneJournalStore;
                let lineage_store = FsTaskRunLineageStore;

                barrier.wait();
                record_bead_start(
                    &snapshot_store,
                    &journal_store,
                    &lineage_store,
                    &base_dir,
                    &milestone_id,
                    "bead-1",
                    "project-1",
                    Some("run-1"),
                    Some("plan-v1"),
                    now,
                )
            }));
        }

        for handle in handles {
            handle.join().expect("start worker panicked")?;
        }

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Running);

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Active);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(snapshot.progress.in_progress_beads, 1);

        let journal = read_journal(&journal_store, base, &record.id)?;
        let start_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadStarted)
            .collect();
        assert_eq!(start_events.len(), 1);
        assert_eq!(start_events[0].timestamp, now);
        Ok(())
    }

    #[test]
    fn start_and_completion_serialize_milestone_snapshot_updates(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "serialized-mutation-test".to_owned(),
                name: "Serialized Mutation Test".to_owned(),
                description: "serialize completion and new start snapshot writes".to_owned(),
            },
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            now,
        )?;

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (start_attempted_tx, start_attempted_rx) = mpsc::channel();
        let (start_proceed_tx, start_proceed_rx) = mpsc::channel();
        let (start_finished_tx, start_finished_rx) = mpsc::channel();
        let blocking_snapshot_store = Arc::new(BlockingSnapshotStore::new(entered_tx, release_rx));
        let start_snapshot_store = Arc::new(LockAttemptSnapshotStore::new(
            start_attempted_tx,
            start_proceed_rx,
        ));
        let base_dir = base.to_path_buf();
        let milestone_id = record.id.clone();

        let completion_store = Arc::clone(&blocking_snapshot_store);
        let completion_handle = std::thread::spawn(move || -> AppResult<()> {
            let journal_store = FsMilestoneJournalStore;
            let lineage_store = FsTaskRunLineageStore;
            update_task_run(
                completion_store.as_ref(),
                &journal_store,
                &lineage_store,
                &base_dir,
                &milestone_id,
                "bead-1",
                "project-1",
                Some("run-1"),
                Some("plan-v1"),
                TaskRunOutcome::Succeeded,
                Some("finished bead-1".to_owned()),
                now + chrono::Duration::seconds(5),
            )
        });

        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("completion should block on the ready snapshot write");

        let start_store = Arc::clone(&start_snapshot_store);
        let base_dir = base.to_path_buf();
        let milestone_id = record.id.clone();
        let start_handle = std::thread::spawn(move || -> AppResult<()> {
            let journal_store = FsMilestoneJournalStore;
            let lineage_store = FsTaskRunLineageStore;
            let result = record_bead_start(
                start_store.as_ref(),
                &journal_store,
                &lineage_store,
                &base_dir,
                &milestone_id,
                "bead-2",
                "project-2",
                Some("run-2"),
                Some("plan-v2"),
                now + chrono::Duration::seconds(10),
            );
            let _ = start_finished_tx.send(());
            result
        });

        start_attempted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("new bead start should reach the write-lock contention point");
        start_proceed_tx
            .send(())
            .expect("allow new bead start to contend on the write lock");

        assert!(
            start_finished_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "new bead start should wait for the in-flight completion write lock",
        );

        let runs_while_blocked = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs_while_blocked.len(), 1);
        assert_eq!(runs_while_blocked[0].bead_id, "bead-1");
        assert_eq!(runs_while_blocked[0].outcome, TaskRunOutcome::Succeeded);

        release_tx
            .send(())
            .expect("release blocked completion snapshot write");

        completion_handle
            .join()
            .expect("completion worker panicked")?;
        start_handle.join().expect("start worker panicked")?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Active);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-2"));
        assert_eq!(snapshot.progress.completed_beads, 1);
        assert_eq!(snapshot.progress.in_progress_beads, 1);

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 2);
        assert!(runs
            .iter()
            .any(|run| { run.bead_id == "bead-1" && run.outcome == TaskRunOutcome::Succeeded }));
        assert!(runs
            .iter()
            .any(|run| { run.bead_id == "bead-2" && run.outcome == TaskRunOutcome::Running }));
        Ok(())
    }

    #[test]
    fn duplicate_running_rows_with_same_run_id_can_be_finalized(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "duplicate-run-id-test".to_owned(),
                name: "Duplicate Run ID Test".to_owned(),
                description: "testing duplicate running row repair".to_owned(),
            },
            now,
        )?;

        for started_at in [now, now + chrono::Duration::seconds(1)] {
            lineage_store.append_task_run(
                base,
                &record.id,
                &TaskRunEntry {
                    milestone_id: Some(record.id.to_string()),
                    bead_id: "bead-1".to_owned(),
                    project_id: "project-1".to_owned(),
                    run_id: Some("run-1".to_owned()),
                    plan_hash: Some("plan-v1".to_owned()),
                    outcome: TaskRunOutcome::Running,
                    outcome_detail: None,
                    started_at,
                    finished_at: None,
                },
            )?;
        }

        let mut stale_snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        stale_snapshot.status = MilestoneStatus::Active;
        stale_snapshot.active_bead = Some("bead-1".to_owned());
        stale_snapshot.progress.in_progress_beads = 2;
        stale_snapshot.updated_at = now + chrono::Duration::seconds(1);
        snapshot_store.write_snapshot(base, &record.id, &stale_snapshot)?;

        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("deduped finalization".to_owned()),
            now + chrono::Duration::seconds(10),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(runs[0].started_at, now);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Succeeded);

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);
        Ok(())
    }

    #[test]
    fn lineage_updates_require_run_id_when_multiple_open_attempts_exist(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "ambiguous-run-test".to_owned(),
                name: "Ambiguous Run Test".to_owned(),
                description: "testing run-id disambiguation".to_owned(),
            },
            now,
        )?;

        for run_id in ["run-1", "run-2"] {
            lineage_store.append_task_run(
                base,
                &record.id,
                &TaskRunEntry {
                    milestone_id: Some(record.id.to_string()),
                    bead_id: "bead-1".to_owned(),
                    project_id: "project-1".to_owned(),
                    run_id: Some(run_id.to_owned()),
                    plan_hash: None,
                    outcome: TaskRunOutcome::Running,
                    outcome_detail: None,
                    started_at: now,
                    finished_at: None,
                },
            )?;
        }

        let error = lineage_store
            .update_task_run(
                base,
                &record.id,
                "bead-1",
                "project-1",
                None,
                None,
                TaskRunOutcome::Succeeded,
                Some("missing run id".to_owned()),
                now,
            )
            .expect_err("ambiguous updates should require run_id");
        assert!(error.to_string().contains("provide run_id"));

        lineage_store.update_task_run(
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-2"),
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("selected exact run".to_owned()),
            now,
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs[0].outcome, TaskRunOutcome::Running);
        assert_eq!(runs[1].run_id.as_deref(), Some("run-2"));
        assert_eq!(runs[1].plan_hash.as_deref(), Some("plan-v2"));
        assert_eq!(runs[1].outcome, TaskRunOutcome::Succeeded);
        Ok(())
    }

    #[test]
    fn lineage_update_with_unknown_run_id_rejects_mixed_legacy_open_attempts(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "mixed-legacy-open-test".to_owned(),
                name: "Mixed Legacy Open Test".to_owned(),
                description:
                    "reject ambiguous run-id fallback when legacy and named attempts coexist"
                        .to_owned(),
            },
            now,
        )?;

        for entry in [
            TaskRunEntry {
                milestone_id: Some(record.id.to_string()),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now,
                finished_at: None,
            },
            TaskRunEntry {
                milestone_id: Some(record.id.to_string()),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-2".to_owned()),
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now + chrono::Duration::seconds(1),
                finished_at: None,
            },
        ] {
            lineage_store.append_task_run(base, &record.id, &entry)?;
        }

        let error = lineage_store
            .update_task_run(
                base,
                &record.id,
                "bead-1",
                "project-1",
                Some("run-3"),
                Some("plan-v3"),
                TaskRunOutcome::Succeeded,
                Some("wrong target".to_owned()),
                now + chrono::Duration::seconds(2),
            )
            .expect_err("mixed runless/named open attempts must be rejected as ambiguous");
        assert!(error.to_string().contains("ambiguous task run update"));

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 2);
        assert!(runs
            .iter()
            .all(|run| run.outcome == TaskRunOutcome::Running));
        Ok(())
    }
}
