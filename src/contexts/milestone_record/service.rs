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
        started_at: DateTime<Utc>,
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
        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        plan_store.write_plan_json(base_dir, milestone_id, &plan_json)?;
        plan_store.write_plan_md(base_dir, milestone_id, &plan_md)?;

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
                .with_details(started_entry.start_journal_details());
        let _ = journal_store.append_event_if_missing(base_dir, milestone_id, &event)?;

        Ok(())
    })
}

/// Record the completion of a bead task run.
///
/// This finalizes the existing lineage row created by [`record_bead_start`] and
/// updates snapshot + journal state in the same flow. `started_at` is forwarded
/// to the lineage layer so runless retries on the same project can still replay
/// completion deterministically after a partial write.
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
    started_at: DateTime<Utc>,
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
        started_at,
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
    started_at: DateTime<Utc>,
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

        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        let finalized_run = lineage_store.update_task_run(
            base_dir,
            milestone_id,
            bead_id,
            project_id,
            run_id,
            plan_hash,
            started_at,
            outcome,
            outcome_detail,
            finished_at,
        )?;

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

    fn sample_bundle(
        id: &str,
        name: &str,
    ) -> crate::contexts::milestone_record::bundle::MilestoneBundle {
        use crate::contexts::milestone_record::bundle::{
            AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
        };

        MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: id.to_owned(),
                name: name.to_owned(),
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
        }
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

        let bundle = sample_bundle("plan-test", "Plan Test");

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
    fn persist_plan_missing_milestone_does_not_block_future_create(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();
        let missing_id = MilestoneId::new("missing-plan-test")?;

        let error = persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &missing_id,
            &sample_bundle("missing-plan-test", "Missing Plan Test"),
            now,
        )
        .expect_err("persisting a plan for a missing milestone must fail");
        assert!(
            error.to_string().contains("No such file") || error.to_string().contains("status.json")
        );

        let created = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: missing_id.to_string(),
                name: "Missing Plan Test".to_owned(),
                description: "creation should still work after a typoed persist_plan".to_owned(),
            },
            now,
        )?;
        assert_eq!(created.id, missing_id);
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
        assert_eq!(runs[0].milestone_id, record.id.to_string());
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
            milestone_id: "ms-1".to_owned(),
            bead_id: "bead-1".to_owned(),
            project_id: "proj-1".to_owned(),
            run_id: Some("run-42".to_owned()),
            plan_hash: Some("sha256-abc".to_owned()),
            snapshot_plan_hash_at_creation: None,
            outcome: TaskRunOutcome::Succeeded,
            outcome_detail: Some("Completed with 3 warnings".to_owned()),
            started_at: now,
            finished_at: Some(now),
        };
        let json = serde_json::to_string(&entry)?;
        let parsed: TaskRunEntry = serde_json::from_str(&json)?;
        assert_eq!(parsed.milestone_id, "ms-1");
        assert_eq!(parsed.run_id.as_deref(), Some("run-42"));
        assert_eq!(parsed.plan_hash.as_deref(), Some("sha256-abc"));
        assert_eq!(
            parsed.outcome_detail.as_deref(),
            Some("Completed with 3 warnings")
        );
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
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                snapshot_plan_hash_at_creation: None,
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
    fn misfiled_task_run_rows_are_rejected_by_queries_and_reconciliation(
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
                id: "misfiled-task-run-test".to_owned(),
                name: "Misfiled Task Run Test".to_owned(),
                description: "reject rows whose milestone_id does not match the file path"
                    .to_owned(),
            },
            now,
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        std::fs::write(
            &task_runs_path,
            format!(
                "{}\n",
                serde_json::to_string(&TaskRunEntry {
                    milestone_id: "other-milestone".to_owned(),
                    bead_id: "bead-1".to_owned(),
                    project_id: "project-1".to_owned(),
                    run_id: None,
                    plan_hash: None,
                    snapshot_plan_hash_at_creation: None,
                    outcome: TaskRunOutcome::Running,
                    outcome_detail: None,
                    started_at: now,
                    finished_at: None,
                })?
            ),
        )?;

        let query_error = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")
            .expect_err("misfiled rows must fail bead queries");
        assert!(query_error
            .to_string()
            .contains("does not match milestone path"));

        let start_error = record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-2",
            "project-2",
            Some("run-2"),
            Some("plan-v2"),
            now + chrono::Duration::seconds(1),
        )
        .expect_err("snapshot reconciliation must reject misfiled lineage rows");
        assert!(start_error
            .to_string()
            .contains("does not match milestone path"));
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
            assert_eq!(run.milestone_id, record.id.to_string());
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
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: first_started,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("legacy failure".to_owned()),
                started_at: first_started,
                finished_at: Some(first_started + chrono::Duration::seconds(5)),
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-2".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: second_started,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-2".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: Some("legacy success".to_owned()),
                started_at: second_started,
                finished_at: Some(second_started + chrono::Duration::seconds(5)),
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-2".to_owned(),
                project_id: "other-project".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
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
            .all(|run| run.milestone_id == record.id.to_string()));
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
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: None,
                started_at,
                finished_at: Some(finished_at),
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
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
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("first retry".to_owned()),
                started_at,
                finished_at: Some(started_at + chrono::Duration::seconds(1)),
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
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
            Some("run-1"),
            Some("plan-v1"),
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
            now,
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
        assert_eq!(runs[0].milestone_id, record.id.to_string());
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
    fn update_task_run_missing_milestone_does_not_block_future_create(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();
        let missing_id = MilestoneId::new("missing-update-test")?;

        let error = update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &missing_id,
            "bead-1",
            "project-1",
            None,
            None,
            now,
            TaskRunOutcome::Succeeded,
            Some("missing milestone".to_owned()),
            now,
        )
        .expect_err("updating a missing milestone must fail");
        assert!(
            error.to_string().contains("No such file") || error.to_string().contains("status.json")
        );

        let created = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: missing_id.to_string(),
                name: "Missing Update Test".to_owned(),
                description: "creation should still work after a typoed update_task_run".to_owned(),
            },
            now,
        )?;
        assert_eq!(created.id, missing_id);
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
    fn same_timestamp_retry_keeps_snapshot_active() -> Result<(), Box<dyn std::error::Error>> {
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
                id: "same-timestamp-retry-state-test".to_owned(),
                name: "Same Timestamp Retry State Test".to_owned(),
                description: "same-timestamp retries should keep the newest active attempt visible"
                    .to_owned(),
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
            Some("first attempt failed"),
            now,
            now + chrono::Duration::seconds(1),
        )?;

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
            now,
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Active);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(snapshot.progress.in_progress_beads, 1);
        assert_eq!(snapshot.progress.failed_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 0);

        let bead_runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(bead_runs.len(), 2);
        assert!(bead_runs
            .iter()
            .any(|run| run.run_id.as_deref() == Some("run-1")
                && run.outcome == TaskRunOutcome::Failed));
        assert!(bead_runs
            .iter()
            .any(|run| run.run_id.as_deref() == Some("run-2")
                && run.outcome == TaskRunOutcome::Running));
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
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: legacy_started,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
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
            retry_started,
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
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
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
            started_at,
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
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                snapshot_plan_hash_at_creation: None,
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
            now,
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
            now,
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
                "started_at": now,
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
            now,
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
            now,
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
                "started_at": now,
                "outcome": "succeeded",
                "outcome_detail": "backfilled detail",
            })
        );
        Ok(())
    }

    #[test]
    fn completion_journal_dedupe_keeps_same_timestamp_attempts_separate(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let journal_store = FsMilestoneJournalStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "completion-journal-dedupe-test".to_owned(),
                name: "Completion Journal Dedupe Test".to_owned(),
                description: "same-timestamp completion events keep distinct attempts".to_owned(),
            },
            now,
        )?;

        let shared_finished_at = now + chrono::Duration::seconds(30);
        let first_attempt = TaskRunEntry {
            milestone_id: record.id.to_string(),
            bead_id: "bead-1".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: None,
            plan_hash: Some("plan-v1".to_owned()),
            snapshot_plan_hash_at_creation: None,
            outcome: TaskRunOutcome::Succeeded,
            outcome_detail: Some("legacy completion".to_owned()),
            started_at: now,
            finished_at: Some(shared_finished_at),
        };
        let second_attempt = TaskRunEntry {
            milestone_id: record.id.to_string(),
            bead_id: "bead-1".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: Some("run-2".to_owned()),
            plan_hash: Some("plan-v2".to_owned()),
            snapshot_plan_hash_at_creation: None,
            outcome: TaskRunOutcome::Succeeded,
            outcome_detail: Some("retry completion".to_owned()),
            started_at: now + chrono::Duration::seconds(10),
            finished_at: Some(shared_finished_at),
        };

        let first_event =
            MilestoneJournalEvent::new(MilestoneEventType::BeadCompleted, shared_finished_at)
                .with_bead("bead-1")
                .with_details(first_attempt.completion_journal_details());
        let second_event =
            MilestoneJournalEvent::new(MilestoneEventType::BeadCompleted, shared_finished_at)
                .with_bead("bead-1")
                .with_details(second_attempt.completion_journal_details());

        assert!(journal_store.append_event_if_missing(base, &record.id, &first_event)?);
        assert!(journal_store.append_event_if_missing(base, &record.id, &second_event)?);

        let completion_events: Vec<_> = read_journal(&journal_store, base, &record.id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadCompleted)
            .collect();
        assert_eq!(completion_events.len(), 2);
        assert!(completion_events
            .iter()
            .any(|event| event.details.as_deref() == first_event.details.as_deref()));
        assert!(completion_events
            .iter()
            .any(|event| event.details.as_deref() == second_event.details.as_deref()));
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
    fn start_retry_without_run_id_repairs_partial_start_write_for_same_attempt(
    ) -> Result<(), Box<dyn std::error::Error>> {
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
                id: "runless-start-retry-test".to_owned(),
                name: "Runless Start Retry Test".to_owned(),
                description: "legacy start retries should repair partial writes without run ids"
                    .to_owned(),
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
            None,
            Some("plan-v1"),
            now,
        )
        .expect_err("start should fail when the journal append fails");
        assert!(failure
            .to_string()
            .contains("simulated start journal failure"));

        let runs_after_failure = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs_after_failure.len(), 1);
        assert_eq!(runs_after_failure[0].run_id, None);
        assert_eq!(runs_after_failure[0].started_at, now);
        assert_eq!(runs_after_failure[0].outcome, TaskRunOutcome::Running);

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            None,
            Some("plan-v1"),
            now,
        )?;

        let runs_after_retry = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs_after_retry.len(), 1);
        assert_eq!(runs_after_retry[0].run_id, None);
        assert_eq!(runs_after_retry[0].started_at, now);
        assert_eq!(runs_after_retry[0].plan_hash.as_deref(), Some("plan-v1"));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Active);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(snapshot.progress.in_progress_beads, 1);
        assert_eq!(snapshot.updated_at, now);

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
            None,
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
    fn replaying_a_finalized_runless_start_does_not_reactivate_the_milestone(
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
                id: "runless-finalized-replay-test".to_owned(),
                name: "Runless Finalized Replay Test".to_owned(),
                description:
                    "replaying a completed runless start must not append a new running attempt"
                        .to_owned(),
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
            None,
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
            "project-1",
            None,
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("completed"),
            now,
            now + chrono::Duration::seconds(5),
        )?;

        let error = record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            None,
            Some("plan-v1"),
            now,
        )
        .expect_err("replaying a finalized runless start should fail");
        assert!(error.to_string().contains("already finalized"));

        let runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_id, None);
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
        assert_eq!(snapshot.progress.failed_beads, 0);

        let journal = read_journal(&journal_store, base, &record.id)?;
        let start_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadStarted)
            .collect();
        assert_eq!(start_events.len(), 1);
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadCompleted)
            .collect();
        assert_eq!(completion_events.len(), 1);
        Ok(())
    }

    #[test]
    fn runless_retry_with_new_started_at_auto_fails_prior_attempt_and_stays_completable(
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
                id: "runless-new-attempt-test".to_owned(),
                name: "Runless New Attempt Test".to_owned(),
                description: "new runless retries stay visible as separate attempts".to_owned(),
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
            None,
            Some("plan-v1"),
            now,
        )?;

        let retry_started_at = now + chrono::Duration::seconds(5);
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            None,
            Some("plan-v2"),
            retry_started_at,
        )?;

        let runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].run_id, None);
        assert_eq!(runs[0].started_at, now);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(runs[0].finished_at, Some(retry_started_at));
        assert!(runs[0]
            .outcome_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("superseded by retry")));
        assert_eq!(runs[1].run_id, None);
        assert_eq!(runs[1].started_at, retry_started_at);
        assert_eq!(runs[1].plan_hash.as_deref(), Some("plan-v2"));
        assert_eq!(runs[1].outcome, TaskRunOutcome::Running);

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Active);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(snapshot.progress.in_progress_beads, 1);
        assert_eq!(snapshot.updated_at, retry_started_at);

        let journal = read_journal(&journal_store, base, &record.id)?;
        let start_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadStarted)
            .collect();
        assert_eq!(start_events.len(), 2);
        assert_eq!(start_events[0].timestamp, now);
        assert_eq!(start_events[1].timestamp, retry_started_at);

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            None,
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("retry succeeded"),
            retry_started_at,
            retry_started_at + chrono::Duration::seconds(5),
        )?;

        let finalized_runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(finalized_runs.len(), 2);
        assert_eq!(finalized_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(finalized_runs[1].outcome, TaskRunOutcome::Succeeded);

        let finalized_snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        finalized_snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(finalized_snapshot.status, MilestoneStatus::Ready);
        assert_eq!(finalized_snapshot.progress.in_progress_beads, 0);
        assert_eq!(finalized_snapshot.progress.completed_beads, 1);
        assert_eq!(finalized_snapshot.progress.failed_beads, 0);
        Ok(())
    }

    #[test]
    fn runless_completion_replay_for_same_project_retry_repairs_journal(
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
                id: "runless-completion-replay-test".to_owned(),
                name: "Runless Completion Replay Test".to_owned(),
                description: "same-project runless retries can replay completion via started_at"
                    .to_owned(),
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
            None,
            Some("plan-v1"),
            now,
        )?;

        let retry_started_at = now + chrono::Duration::seconds(5);
        let retry_finished_at = retry_started_at + chrono::Duration::seconds(5);
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            None,
            Some("plan-v2"),
            retry_started_at,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            None,
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("retry succeeded"),
            retry_started_at,
            retry_finished_at,
        )?;

        let journal_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("journal.ndjson");
        let repaired_journal = read_journal(&journal_store, base, &record.id)?
            .into_iter()
            .filter(|event| {
                !(event.event_type == MilestoneEventType::BeadCompleted
                    && event.bead_id.as_deref() == Some("bead-1"))
            })
            .map(|event| event.to_ndjson_line().map_err(AppError::SerdeJson))
            .collect::<AppResult<Vec<_>>>()?;
        let journal_content = if repaired_journal.is_empty() {
            String::new()
        } else {
            format!("{}\n", repaired_journal.join("\n"))
        };
        std::fs::write(&journal_path, journal_content)?;

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            None,
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("retry succeeded"),
            retry_started_at,
            retry_finished_at + chrono::Duration::seconds(10),
        )?;

        let runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(runs[1].started_at, retry_started_at);
        assert_eq!(runs[1].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(runs[1].plan_hash.as_deref(), Some("plan-v2"));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.progress.completed_beads, 1);
        assert_eq!(snapshot.progress.failed_beads, 0);

        let journal = read_journal(&journal_store, base, &record.id)?;
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadCompleted)
            .collect();
        assert_eq!(completion_events.len(), 1);
        assert_eq!(completion_events[0].timestamp, retry_finished_at);
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
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now,
                finished_at: None,
            },
            TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-2".to_owned()),
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
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
    fn explicit_retry_after_legacy_open_supersedes_old_attempt(
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
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
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

        let attempts_after_retry = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(attempts_after_retry.len(), 2);
        assert_eq!(attempts_after_retry[0].run_id, None);
        assert_eq!(attempts_after_retry[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(attempts_after_retry[0].finished_at, Some(retry_started_at));
        assert!(attempts_after_retry[0]
            .outcome_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("superseded by retry")));
        assert_eq!(attempts_after_retry[1].run_id.as_deref(), Some("run-2"));
        assert_eq!(attempts_after_retry[1].outcome, TaskRunOutcome::Running);

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
        assert_eq!(finalized_attempts[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(finalized_attempts[1].run_id.as_deref(), Some("run-2"));
        assert_eq!(finalized_attempts[1].outcome, TaskRunOutcome::Succeeded);

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.completed_beads, 1);
        assert_eq!(snapshot.progress.failed_beads, 0);
        Ok(())
    }

    #[test]
    fn retry_start_on_same_bead_supersedes_running_attempt_from_other_project(
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
                id: "same-bead-cross-project-retry-test".to_owned(),
                name: "Same Bead Cross Project Retry Test".to_owned(),
                description:
                    "starting a retry on the same bead supersedes the older running attempt"
                        .to_owned(),
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

        let retry_started_at = now + chrono::Duration::seconds(10);
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-2",
            Some("run-2"),
            Some("plan-v2"),
            retry_started_at,
        )?;

        let runs_after_retry = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs_after_retry.len(), 2);
        assert_eq!(runs_after_retry[0].project_id, "project-1");
        assert_eq!(runs_after_retry[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(runs_after_retry[0].finished_at, Some(retry_started_at));
        assert!(runs_after_retry[0]
            .outcome_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("superseded by retry")));
        assert_eq!(runs_after_retry[1].project_id, "project-2");
        assert_eq!(runs_after_retry[1].run_id.as_deref(), Some("run-2"));
        assert_eq!(runs_after_retry[1].outcome, TaskRunOutcome::Running);

        let active_snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        active_snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(active_snapshot.status, MilestoneStatus::Active);
        assert_eq!(active_snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(active_snapshot.progress.in_progress_beads, 1);
        assert_eq!(active_snapshot.progress.failed_beads, 0);

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-2",
            Some("run-2"),
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("retry completed"),
            retry_started_at,
            retry_started_at + chrono::Duration::seconds(5),
        )?;

        let final_snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        final_snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(final_snapshot.status, MilestoneStatus::Ready);
        assert_eq!(final_snapshot.active_bead, None);
        assert_eq!(final_snapshot.progress.in_progress_beads, 0);
        assert_eq!(final_snapshot.progress.completed_beads, 1);
        assert_eq!(final_snapshot.progress.failed_beads, 0);
        Ok(())
    }

    /// Runless-to-named completion is now accepted (Issue 2), but conflicting
    /// plan_hash values still prevent finalization.
    #[test]
    fn completion_with_conflicting_plan_hash_rejects_runless_match(
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
                id: "exact-completion-match-test".to_owned(),
                name: "Exact Completion Match Test".to_owned(),
                description: "conflicting plan_hash must still prevent finalization".to_owned(),
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
            None,
            Some("plan-v1"),
            now,
        )?;

        // Use matching started_at so the runless match is accepted (Amendment 2
        // requires started_at to match). The plan_hash conflict then rejects.
        let error = record_bead_completion(
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
            Some("should fail due to plan_hash conflict"),
            now,
            now + chrono::Duration::seconds(15),
        )
        .expect_err("conflicting plan_hash should prevent finalization");
        assert!(
            error.to_string().contains("conflicting plan_hash"),
            "expected plan_hash conflict error, got: {error}"
        );

        let runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs.len(), 1);
        // run_id is NOT backfilled because the operation failed
        assert_eq!(runs[0].run_id, None);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Running);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Active);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));

        let journal = read_journal(&journal_store, base, &record.id)?;
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadCompleted)
            .collect();
        assert!(completion_events.is_empty());
        Ok(())
    }

    #[test]
    fn named_replay_with_matching_started_at_backfills_runless_attempt(
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
                id: "runless-backfill-test".to_owned(),
                name: "Runless Backfill Test".to_owned(),
                description: "same-attempt replays can backfill a newly known run id".to_owned(),
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
            None,
            None,
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
            Some("run-2"),
            Some("plan-v1"),
            now,
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_id.as_deref(), Some("run-2"));
        assert_eq!(runs[0].started_at, now);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Running);

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-2"),
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("backfilled run completed"),
            now,
            now + chrono::Duration::seconds(5),
        )?;

        let finalized_runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(finalized_runs.len(), 1);
        assert_eq!(finalized_runs[0].run_id.as_deref(), Some("run-2"));
        assert_eq!(finalized_runs[0].outcome, TaskRunOutcome::Succeeded);

        let journal = read_journal(&journal_store, base, &record.id)?;
        let start_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadStarted)
            .collect();
        assert_eq!(start_events.len(), 1);
        assert_eq!(start_events[0].timestamp, now);
        let start_details: serde_json::Value = serde_json::from_str(
            start_events[0]
                .details
                .as_deref()
                .expect("start event should carry details"),
        )?;
        assert_eq!(
            start_details,
            serde_json::json!({
                "project_id": "project-1",
                "run_id": "run-2",
                "plan_hash": "plan-v1",
            })
        );
        Ok(())
    }

    #[test]
    fn distinct_named_run_with_same_started_at_as_finalized_attempt_can_start(
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
                id: "same-started-at-named-retry-test".to_owned(),
                name: "Same Started At Named Retry Test".to_owned(),
                description:
                    "distinct named runs should not be blocked by a finalized attempt that shares a timestamp"
                        .to_owned(),
            },
            now,
        )?;

        lineage_store.append_task_run(
            base,
            &record.id,
            &TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-2".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: Some("first attempt finished".to_owned()),
                started_at: now,
                finished_at: Some(now + chrono::Duration::seconds(5)),
            },
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-3"),
            Some("plan-v2"),
            now,
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 2);
        assert!(runs.iter().any(|entry| {
            entry.run_id.as_deref() == Some("run-2") && entry.outcome == TaskRunOutcome::Succeeded
        }));
        assert!(runs.iter().any(|entry| {
            entry.run_id.as_deref() == Some("run-3")
                && entry.started_at == now
                && entry.outcome == TaskRunOutcome::Running
        }));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Active);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        Ok(())
    }

    #[test]
    fn start_journal_dedupe_keeps_same_timestamp_named_attempts_separate(
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
                id: "same-started-at-start-journal-test".to_owned(),
                name: "Same Started At Start Journal Test".to_owned(),
                description:
                    "distinct named starts at the same timestamp must keep separate journal rows"
                        .to_owned(),
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
            Some("run-2"),
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
            "project-1",
            Some("run-2"),
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("first run finished"),
            now,
            now + chrono::Duration::seconds(5),
        )?;
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-3"),
            Some("plan-v2"),
            now,
        )?;

        let start_events: Vec<_> = read_journal(&journal_store, base, &record.id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadStarted)
            .collect();
        assert_eq!(start_events.len(), 2);

        let start_details: Vec<serde_json::Value> = start_events
            .iter()
            .map(|event| {
                serde_json::from_str(
                    event
                        .details
                        .as_deref()
                        .expect("start event should carry details"),
                )
            })
            .collect::<Result<_, _>>()?;
        assert!(start_details.iter().any(|details| {
            details
                == &serde_json::json!({
                    "project_id": "project-1",
                    "run_id": "run-2",
                    "plan_hash": "plan-v1",
                })
        }));
        assert!(start_details.iter().any(|details| {
            details
                == &serde_json::json!({
                    "project_id": "project-1",
                    "run_id": "run-3",
                    "plan_hash": "plan-v2",
                })
        }));
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
                now,
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
                    milestone_id: record.id.to_string(),
                    bead_id: "bead-1".to_owned(),
                    project_id: "project-1".to_owned(),
                    run_id: Some("run-1".to_owned()),
                    plan_hash: Some("plan-v1".to_owned()),
                    snapshot_plan_hash_at_creation: None,
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
            now,
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
                    milestone_id: record.id.to_string(),
                    bead_id: "bead-1".to_owned(),
                    project_id: "project-1".to_owned(),
                    run_id: Some(run_id.to_owned()),
                    plan_hash: None,
                    snapshot_plan_hash_at_creation: None,
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
                now,
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
            now,
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
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now,
                finished_at: None,
            },
            TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-2".to_owned()),
                plan_hash: None,
                snapshot_plan_hash_at_creation: None,
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
                now + chrono::Duration::seconds(2),
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

    /// Issue 2: A start recorded without run_id can be completed with a
    /// subsequently known run_id.
    #[test]
    fn runless_start_completed_with_named_run_id() -> Result<(), Box<dyn std::error::Error>> {
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
                id: "runless-named-test".to_owned(),
                name: "Runless to Named".to_owned(),
                description: "test runless start finalized with run_id".to_owned(),
            },
            now,
        )?;

        // Start bead WITHOUT run_id or plan_hash (early/legacy path)
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

        let runs_before = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs_before.len(), 1);
        assert_eq!(runs_before[0].run_id, None);

        // Complete bead WITH a subsequently discovered run_id and plan_hash
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-late"),
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("completed with late run_id"),
            now,
            now + chrono::Duration::seconds(1),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_id.as_deref(), Some("run-late"));
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(
            runs[0].outcome_detail.as_deref(),
            Some("completed with late run_id")
        );

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.progress.completed_beads, 1);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        Ok(())
    }

    /// Issue 3: plan_hash is auto-populated from the milestone snapshot when
    /// callers omit it.
    #[test]
    fn plan_hash_auto_populated_from_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
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
                id: "plan-hash-auto".to_owned(),
                name: "Plan Hash Auto".to_owned(),
                description: "test plan_hash auto-population from snapshot".to_owned(),
            },
            now,
        )?;

        // Persist a plan — this stores the plan_hash in the snapshot
        let snapshot = persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &sample_bundle("plan-hash-auto", "Plan Hash Auto"),
            now,
        )?;
        let expected_plan_hash = snapshot.plan_hash.clone().expect("plan_hash must be set");

        // Start bead WITHOUT plan_hash — should auto-populate from snapshot
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            None, // plan_hash omitted
            now,
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].plan_hash.as_deref(),
            Some(expected_plan_hash.as_str()),
            "plan_hash should be auto-populated from snapshot on start"
        );

        // Complete bead WITHOUT plan_hash — should also auto-populate
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            None, // plan_hash omitted
            TaskRunOutcome::Succeeded,
            Some("auto plan_hash test"),
            now,
            now + chrono::Duration::seconds(1),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].plan_hash.as_deref(),
            Some(expected_plan_hash.as_str()),
            "plan_hash should be preserved through completion"
        );
        assert_eq!(runs[0].outcome, TaskRunOutcome::Succeeded);
        Ok(())
    }

    /// Issue 3 (variant): When callers provide an explicit plan_hash, it should
    /// take precedence over the snapshot value.
    #[test]
    fn explicit_plan_hash_takes_precedence_over_snapshot() -> Result<(), Box<dyn std::error::Error>>
    {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
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
                id: "plan-hash-explicit".to_owned(),
                name: "Plan Hash Explicit".to_owned(),
                description: "test explicit plan_hash precedence".to_owned(),
            },
            now,
        )?;

        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &sample_bundle("plan-hash-explicit", "Plan Hash Explicit"),
            now,
        )?;

        // Start bead WITH explicit plan_hash
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-1"),
            Some("caller-provided-hash"),
            now,
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(
            runs[0].plan_hash.as_deref(),
            Some("caller-provided-hash"),
            "explicit plan_hash must take precedence over snapshot"
        );
        Ok(())
    }

    /// Amendment 1: Terminal replay after plan evolution must not false-positive
    /// reject due to auto-populated plan_hash from a newer snapshot.
    #[test]
    fn terminal_replay_after_plan_evolution_succeeds() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
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
                id: "terminal-replay-evolve".to_owned(),
                name: "Terminal Replay Evolve".to_owned(),
                description: "replay after plan evolution must not conflict".to_owned(),
            },
            now,
        )?;

        // Persist plan v1
        let snap_v1 = persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &sample_bundle("terminal-replay-evolve", "Plan v1"),
            now,
        )?;
        let hash_v1 = snap_v1.plan_hash.clone().expect("plan_hash v1");

        // Start and complete bead with plan_hash=None (auto-populated to hash_v1)
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
        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(
            runs[0].plan_hash.as_deref(),
            Some(hash_v1.as_str()),
            "start should auto-populate plan_hash from snapshot"
        );

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
            Some("completed under plan v1"),
            now,
            now + chrono::Duration::seconds(1),
        )?;

        // Persist plan v2 — snapshot.plan_hash advances
        let mut bundle_v2 = sample_bundle("terminal-replay-evolve", "Plan v2");
        bundle_v2.executive_summary = "Updated plan".to_owned();
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle_v2,
            now + chrono::Duration::seconds(2),
        )?;

        // Replay the SAME terminal completion with plan_hash=None.
        // Must not fail even though snapshot now has hash_v2.
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
            Some("completed under plan v1"),
            now,
            now + chrono::Duration::seconds(1),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].plan_hash.as_deref(),
            Some(hash_v1.as_str()),
            "terminal entry must retain original plan_hash, not snapshot's newer value"
        );
        Ok(())
    }

    /// Amendment 2: Mismatched started_at must reject runless-to-named fallback.
    #[test]
    fn runless_match_rejected_when_started_at_mismatches() -> Result<(), Box<dyn std::error::Error>>
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
                id: "started-at-mismatch".to_owned(),
                name: "Started At Mismatch".to_owned(),
                description: "reject runless match with wrong started_at".to_owned(),
            },
            now,
        )?;

        // Start bead WITHOUT run_id
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

        // Try to complete with a run_id but DIFFERENT started_at
        let error = record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-late"),
            None,
            TaskRunOutcome::Succeeded,
            Some("wrong timestamp"),
            now + chrono::Duration::seconds(10),
            now + chrono::Duration::seconds(15),
        )
        .expect_err("mismatched started_at should reject runless fallback");
        assert!(
            error
                .to_string()
                .contains("does not match requested started_at"),
            "expected started_at mismatch error, got: {error}"
        );

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_id, None);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Running);
        Ok(())
    }

    /// Amendment 3: Replay of start after plan evolution must not conflict with
    /// plan_hash already stored in the row from the previous snapshot.
    #[test]
    fn start_replay_after_plan_evolution_does_not_conflict(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
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
                id: "start-replay-evolve".to_owned(),
                name: "Start Replay Evolve".to_owned(),
                description: "start replay after plan change must not conflict".to_owned(),
            },
            now,
        )?;

        // Persist plan v1
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &sample_bundle("start-replay-evolve", "Plan v1"),
            now,
        )?;

        // Start bead with plan_hash=None (auto-populated to hash_v1)
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

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        let hash_v1 = runs[0]
            .plan_hash
            .clone()
            .expect("plan_hash should be auto-populated");

        // Persist plan v2 — snapshot.plan_hash advances
        let mut bundle_v2 = sample_bundle("start-replay-evolve", "Plan v2");
        bundle_v2.executive_summary = "Updated plan".to_owned();
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle_v2,
            now + chrono::Duration::seconds(1),
        )?;

        // Replay the same start with plan_hash=None — must not conflict.
        // The lineage layer should see the row already has hash_v1 and skip
        // auto-population, avoiding option_conflicts(hash_v1, hash_v2).
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

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].plan_hash.as_deref(),
            Some(hash_v1.as_str()),
            "row must retain original plan_hash from v1, not be overwritten by v2"
        );
        Ok(())
    }

    /// Amendment 1 regression: A legacy open row with plan_hash=None must NOT
    /// get the current snapshot hash stamped on it when replayed after a plan
    /// evolution, because the row may have originally run under an earlier plan.
    #[test]
    fn legacy_open_row_without_plan_hash_not_mislabeled_after_plan_evolution(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
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
                id: "legacy-open-evolve".to_owned(),
                name: "Legacy Open Evolve".to_owned(),
                description: "legacy open row must not be mislabeled".to_owned(),
            },
            now,
        )?;

        // Preseed a legacy running entry with plan_hash=None (simulates a row
        // created before auto-population existed).
        let legacy_entry = TaskRunEntry {
            milestone_id: record.id.to_string(),
            bead_id: "bead-1".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: Some("run-1".to_owned()),
            plan_hash: None, // legacy: no plan_hash
            snapshot_plan_hash_at_creation: None,
            outcome: TaskRunOutcome::Running,
            outcome_detail: None,
            started_at: now,
            finished_at: None,
        };
        lineage_store.append_task_run(base, &record.id, &legacy_entry)?;

        // Now persist a plan — snapshot.plan_hash becomes hash_v1
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &sample_bundle("legacy-open-evolve", "Plan v1"),
            now + chrono::Duration::seconds(1),
        )?;

        // Replay the start with plan_hash=None. The existing row has
        // plan_hash=None but the snapshot now has hash_v1. The row must NOT
        // be stamped with hash_v1 because it originally ran before any plan.
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

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].plan_hash, None,
            "legacy open row must NOT be mislabeled with snapshot plan_hash"
        );

        // Finalize the legacy row with plan_hash=None — must also not mislabel.
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
            Some("completed legacy row"),
            now,
            now + chrono::Duration::seconds(2),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].plan_hash, None,
            "finalized legacy row must NOT be mislabeled with snapshot plan_hash"
        );
        assert_eq!(runs[0].outcome, TaskRunOutcome::Succeeded);
        Ok(())
    }

    /// Amendment 2 regression: A preseeded terminal row with plan_hash=None
    /// replayed with plan_hash=None must NOT stamp the current snapshot hash,
    /// for the same plan-evolution safety reason.
    #[test]
    fn terminal_replay_legacy_row_without_plan_hash_stays_unset(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
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
                id: "terminal-legacy-replay".to_owned(),
                name: "Terminal Legacy Replay".to_owned(),
                description: "terminal replay must not stamp legacy plan_hash".to_owned(),
            },
            now,
        )?;

        // Preseed a legacy finalized entry with plan_hash=None and
        // outcome_detail=None (simulates incomplete legacy terminal row).
        let legacy_terminal = TaskRunEntry {
            milestone_id: record.id.to_string(),
            bead_id: "bead-1".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: Some("run-1".to_owned()),
            plan_hash: None,
            snapshot_plan_hash_at_creation: None,
            outcome: TaskRunOutcome::Succeeded,
            outcome_detail: None,
            started_at: now,
            finished_at: Some(now + chrono::Duration::seconds(1)),
        };
        lineage_store.append_task_run(base, &record.id, &legacy_terminal)?;

        // Persist a plan so snapshot.plan_hash is set
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &sample_bundle("terminal-legacy-replay", "Plan v1"),
            now + chrono::Duration::seconds(2),
        )?;

        // Replay the terminal completion with plan_hash=None.
        // backfill_terminal_entry receives plan_hash=None from caller, so
        // plan_hash stays None. Snapshot backfill is NOT applied.
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
            now,
            TaskRunOutcome::Succeeded,
            Some("repair detail".to_owned()),
            now + chrono::Duration::seconds(1),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].plan_hash, None,
            "terminal replay must NOT stamp legacy row with snapshot plan_hash"
        );
        // outcome_detail should be backfilled though
        assert_eq!(
            runs[0].outcome_detail.as_deref(),
            Some("repair detail"),
            "outcome_detail should be backfilled by terminal replay"
        );
        Ok(())
    }

    /// Regression: a terminal runless replay with the wrong started_at must be
    /// rejected, not backfilled onto the wrong legacy terminal row.
    #[test]
    fn terminal_runless_replay_rejected_when_started_at_mismatches(
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
                id: "terminal-started-at-guard".to_owned(),
                name: "Terminal StartedAt Guard".to_owned(),
                description: "terminal replay started_at guard".to_owned(),
            },
            now,
        )?;

        // Preseed a legacy finalized runless entry at time T.
        let legacy_terminal = TaskRunEntry {
            milestone_id: record.id.to_string(),
            bead_id: "bead-1".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: None,
            plan_hash: None,
            snapshot_plan_hash_at_creation: None,
            outcome: TaskRunOutcome::Succeeded,
            outcome_detail: None,
            started_at: now,
            finished_at: Some(now + chrono::Duration::seconds(1)),
        };
        lineage_store.append_task_run(base, &record.id, &legacy_terminal)?;

        // Attempt to replay with a run_id but a different started_at.
        // Must be rejected — wrong legacy attempt.
        let result = update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            Some("run-wrong"),
            None,
            now + chrono::Duration::seconds(10),
            TaskRunOutcome::Succeeded,
            None,
            now + chrono::Duration::seconds(11),
        );
        assert!(
            result.is_err(),
            "terminal runless replay with mismatched started_at must be rejected"
        );
        Ok(())
    }

    /// Regression: safe_plan_hash_backfill fills plan_hash on an existing
    /// entry when the snapshot has not evolved since creation.
    #[test]
    fn safe_backfill_fills_plan_hash_when_snapshot_unchanged(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
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
                id: "safe-backfill".to_owned(),
                name: "Safe Backfill".to_owned(),
                description: "safe_plan_hash_backfill test".to_owned(),
            },
            now,
        )?;

        // Persist a plan so snapshot has a hash.
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &sample_bundle("safe-backfill", "Plan v1"),
            now,
        )?;

        let snapshot = snapshot_store.read_snapshot(base, &record.id)?;
        let expected_hash = snapshot.plan_hash.clone().expect("snapshot should have plan_hash");

        // Start a bead: plan_hash auto-populated from snapshot, and
        // snapshot_plan_hash_at_creation records the same hash.
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-backfill",
            "project-1",
            Some("run-1"),
            None, // plan_hash omitted — auto-populated
            now + chrono::Duration::seconds(1),
        )?;
        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        let entry = runs
            .iter()
            .find(|e| e.bead_id == "bead-backfill")
            .expect("entry must exist");
        assert_eq!(
            entry.plan_hash.as_deref(),
            Some(expected_hash.as_str()),
            "new entry should auto-populate plan_hash from snapshot"
        );
        assert_eq!(
            entry.snapshot_plan_hash_at_creation.as_deref(),
            Some(expected_hash.as_str()),
            "new entry should record snapshot hash at creation"
        );

        // Preseed an entry with provenance from the current snapshot but
        // plan_hash=None (simulates a row where plan_hash was lost or
        // stripped but provenance is available).
        let entry_with_provenance = TaskRunEntry {
            milestone_id: record.id.to_string(),
            bead_id: "bead-provenance".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: Some("run-prov".to_owned()),
            plan_hash: None,
            snapshot_plan_hash_at_creation: Some(expected_hash.clone()),
            outcome: TaskRunOutcome::Running,
            outcome_detail: None,
            started_at: now + chrono::Duration::seconds(2),
            finished_at: None,
        };
        lineage_store.append_task_run(base, &record.id, &entry_with_provenance)?;

        // Complete the bead — safe_plan_hash_backfill should fill plan_hash
        // because snapshot hasn't evolved.
        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-provenance",
            "project-1",
            Some("run-prov"),
            None, // plan_hash omitted
            now + chrono::Duration::seconds(2),
            TaskRunOutcome::Succeeded,
            None,
            now + chrono::Duration::seconds(3),
        )?;
        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        let completed = runs
            .iter()
            .find(|e| e.bead_id == "bead-provenance")
            .expect("entry must exist");
        assert_eq!(
            completed.plan_hash.as_deref(),
            Some(expected_hash.as_str()),
            "safe backfill should fill plan_hash when snapshot unchanged"
        );
        Ok(())
    }

    /// Regression: safe_plan_hash_backfill does NOT fill when the snapshot
    /// has evolved since entry creation.
    #[test]
    fn safe_backfill_skips_when_snapshot_evolved() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
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
                id: "safe-backfill-evolved".to_owned(),
                name: "Safe Backfill Evolved".to_owned(),
                description: "safe backfill after plan evolution".to_owned(),
            },
            now,
        )?;

        // Persist plan v1.
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &sample_bundle("safe-backfill-evolved", "Plan v1"),
            now,
        )?;
        let v1_snapshot = snapshot_store.read_snapshot(base, &record.id)?;
        let v1_hash = v1_snapshot.plan_hash.clone().expect("v1 hash");

        // Preseed an entry with provenance from v1 but plan_hash=None.
        let entry = TaskRunEntry {
            milestone_id: record.id.to_string(),
            bead_id: "bead-evolved".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: Some("run-evolved".to_owned()),
            plan_hash: None,
            snapshot_plan_hash_at_creation: Some(v1_hash),
            outcome: TaskRunOutcome::Running,
            outcome_detail: None,
            started_at: now + chrono::Duration::seconds(1),
            finished_at: None,
        };
        lineage_store.append_task_run(base, &record.id, &entry)?;

        // Evolve plan to v2.
        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &sample_bundle("safe-backfill-evolved", "Plan v2"),
            now + chrono::Duration::seconds(2),
        )?;

        // Complete the bead — safe_plan_hash_backfill should NOT fill
        // because snapshot has evolved (v1 → v2).
        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-evolved",
            "project-1",
            Some("run-evolved"),
            None,
            now + chrono::Duration::seconds(1),
            TaskRunOutcome::Succeeded,
            None,
            now + chrono::Duration::seconds(3),
        )?;
        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        let completed = runs
            .iter()
            .find(|e| e.bead_id == "bead-evolved")
            .expect("entry must exist");
        assert_eq!(
            completed.plan_hash, None,
            "safe backfill must NOT fill plan_hash when snapshot has evolved"
        );
        Ok(())
    }

    /// Regression: named terminal replay succeeds when the same bead/project
    /// has multiple completed attempts (amendment 4 — pre-filter by started_at).
    #[test]
    fn named_terminal_replay_succeeds_with_multiple_attempts(
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
                id: "multi-attempt-replay".to_owned(),
                name: "Multi-Attempt Replay".to_owned(),
                description: "terminal replay with multiple attempts".to_owned(),
            },
            now,
        )?;

        // Preseed two completed runless attempts for the same bead/project
        // at different started_at times.
        let attempt_a = TaskRunEntry {
            milestone_id: record.id.to_string(),
            bead_id: "bead-multi".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: None,
            plan_hash: None,
            snapshot_plan_hash_at_creation: None,
            outcome: TaskRunOutcome::Succeeded,
            outcome_detail: None,
            started_at: now,
            finished_at: Some(now + chrono::Duration::seconds(1)),
        };
        let attempt_b = TaskRunEntry {
            milestone_id: record.id.to_string(),
            bead_id: "bead-multi".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: None,
            plan_hash: None,
            snapshot_plan_hash_at_creation: None,
            outcome: TaskRunOutcome::Failed,
            outcome_detail: None,
            started_at: now + chrono::Duration::seconds(5),
            finished_at: Some(now + chrono::Duration::seconds(6)),
        };
        lineage_store.append_task_run(base, &record.id, &attempt_a)?;
        lineage_store.append_task_run(base, &record.id, &attempt_b)?;

        // Named terminal replay targeting attempt A by started_at should
        // succeed — the started_at pre-filter excludes attempt B.
        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-multi",
            "project-1",
            Some("run-a"),
            None,
            now, // matches attempt_a's started_at
            TaskRunOutcome::Succeeded,
            Some("repaired A".to_owned()),
            now + chrono::Duration::seconds(1),
        )?;
        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        let result = runs
            .iter()
            .find(|e| e.started_at == now && e.bead_id == "bead-multi")
            .expect("attempt A must exist");
        assert_eq!(result.run_id.as_deref(), Some("run-a"));
        assert_eq!(
            result.outcome_detail.as_deref(),
            Some("repaired A"),
            "named terminal replay should backfill attempt A"
        );
        Ok(())
    }
}
