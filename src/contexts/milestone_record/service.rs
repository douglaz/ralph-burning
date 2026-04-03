use std::collections::BTreeSet;
use std::path::Path;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::adapters::fs::FileSystem;
use crate::shared::error::{AppError, AppResult};

use super::bundle::{progress_shape_signature, render_plan_json, render_plan_md, MilestoneBundle};
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
    fn write_milestone_record(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        record: &MilestoneRecord,
    ) -> AppResult<()>;
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
    /// Explicitly repair a completion event for an already-known attempt.
    /// Unlike `append_event_if_missing`, this path may replace a stale
    /// completion row when a narrower repair flow has already established that
    /// the exact bead/project/run attempt should be corrected.
    fn repair_completion_event(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        event: &MilestoneJournalEvent,
    ) -> AppResult<bool> {
        self.append_event_if_missing(base_dir, milestone_id, event)
    }
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
        run_id: &str,
        plan_hash: &str,
        started_at: DateTime<Utc>,
    ) -> AppResult<TaskRunEntry>;
    /// Update an existing task run entry's outcome by matching bead_id + project_id + run_id.
    /// Implementations should reject ambiguous matches instead of rewriting an
    /// arbitrary historical attempt.
    #[allow(clippy::too_many_arguments)]
    fn update_task_run(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
        project_id: &str,
        run_id: &str,
        plan_hash: Option<&str>,
        started_at: DateTime<Utc>,
        outcome: TaskRunOutcome,
        outcome_detail: Option<String>,
        finished_at: DateTime<Utc>,
    ) -> AppResult<TaskRunEntry>;
    /// Explicitly repair the terminal state of an exact historical attempt.
    /// This is narrower than `update_task_run`: callers must already know they
    /// are correcting the same bead/project/run attempt instead of replaying a
    /// generic terminal completion.
    #[allow(clippy::too_many_arguments)]
    fn repair_task_run_terminal(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
        project_id: &str,
        run_id: &str,
        plan_hash: Option<&str>,
        started_at: DateTime<Utc>,
        outcome: TaskRunOutcome,
        outcome_detail: Option<String>,
        finished_at: DateTime<Utc>,
    ) -> AppResult<TaskRunEntry> {
        self.update_task_run(
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
        )
    }
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

/// Materialize a milestone bundle into milestone record + plan files.
///
/// This is retry-safe for planning/ready milestones: an existing milestone with
/// the same ID can be re-used to finish a partial handoff or refresh the plan.
pub fn materialize_bundle(
    store: &impl MilestoneStorePort,
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    plan_store: &impl MilestonePlanPort,
    base_dir: &Path,
    bundle: &MilestoneBundle,
    now: DateTime<Utc>,
) -> AppResult<MilestoneRecord> {
    let milestone_id = MilestoneId::new(bundle.identity.id.clone())?;
    let expected_plan_json = render_plan_json(bundle).map_err(AppError::SerdeJson)?;
    let expected_plan_hash = {
        let mut hasher = Sha256::new();
        hasher.update(expected_plan_json.as_bytes());
        format!("{:x}", hasher.finalize())
    };

    let mut record = if store.milestone_exists(base_dir, &milestone_id)? {
        let existing_record = store.read_milestone_record(base_dir, &milestone_id)?;
        if existing_record.name != bundle.identity.name {
            return Err(AppError::CorruptRecord {
                file: format!("milestones/{}/milestone.toml", milestone_id),
                details: format!(
                    "existing milestone name '{}' does not match bundle identity '{}'",
                    existing_record.name, bundle.identity.name
                ),
            });
        }

        existing_record
    } else {
        create_milestone(
            store,
            base_dir,
            CreateMilestoneInput {
                id: milestone_id.to_string(),
                name: bundle.identity.name.clone(),
                description: bundle.executive_summary.clone(),
            },
            now,
        )?
    };

    snapshot_store.with_milestone_write_lock(base_dir, &milestone_id, || {
        let mut snapshot = snapshot_store.read_snapshot(base_dir, &milestone_id)?;
        if matches!(
            snapshot.status,
            MilestoneStatus::Active
                | MilestoneStatus::Paused
                | MilestoneStatus::Completed
                | MilestoneStatus::Abandoned
        ) {
            return Err(AppError::InvalidConfigValue {
                key: "milestone_status".to_owned(),
                value: snapshot.status.to_string(),
                reason: format!(
                    "cannot materialize milestone bundle into milestone '{}'",
                    milestone_id
                ),
            });
        }

        if snapshot.plan_hash.as_deref() != Some(expected_plan_hash.as_str()) {
            persist_plan_locked(
                snapshot_store,
                journal_store,
                plan_store,
                base_dir,
                &milestone_id,
                bundle,
                &mut snapshot,
                now,
            )?;
        }

        reconcile_record_description(
            store,
            base_dir,
            &milestone_id,
            &mut record,
            &bundle.executive_summary,
        )?;

        if snapshot.status != MilestoneStatus::Ready {
            update_status_locked(
                snapshot_store,
                journal_store,
                base_dir,
                &milestone_id,
                &mut snapshot,
                MilestoneStatus::Ready,
                now,
            )?;
        }

        Ok(record)
    })
}

fn reconcile_record_description(
    store: &impl MilestoneStorePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    record: &mut MilestoneRecord,
    expected_description: &str,
) -> AppResult<()> {
    if record.description == expected_description {
        return Ok(());
    }

    let mut updated_record = record.clone();
    updated_record.description = expected_description.to_owned();
    store.write_milestone_record(base_dir, milestone_id, &updated_record)?;
    *record = updated_record;
    Ok(())
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
        update_status_locked(
            snapshot_store,
            journal_store,
            base_dir,
            milestone_id,
            &mut snapshot,
            new_status,
            now,
        )?;
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
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, move || {
        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        persist_plan_locked(
            snapshot_store,
            journal_store,
            plan_store,
            base_dir,
            milestone_id,
            bundle,
            &mut snapshot,
            now,
        )?;
        Ok(snapshot)
    })
}

fn update_status_locked(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    snapshot: &mut MilestoneSnapshot,
    new_status: MilestoneStatus,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let old_status = snapshot.status;
    snapshot.status = new_status;

    if new_status.is_terminal() {
        snapshot.active_bead = None;
    }

    snapshot.updated_at = now;

    validate_snapshot(snapshot, milestone_id)?;
    snapshot_store.write_snapshot(base_dir, milestone_id, snapshot)?;

    let event = MilestoneJournalEvent::new(MilestoneEventType::StatusChanged, now)
        .with_details(format!("{old_status} -> {new_status}"));
    let line = event.to_ndjson_line().map_err(AppError::SerdeJson)?;
    journal_store.append_event(base_dir, milestone_id, &line)?;
    Ok(())
}

fn persist_plan_locked(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    plan_store: &impl MilestonePlanPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bundle: &MilestoneBundle,
    snapshot: &mut MilestoneSnapshot,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let plan_json = render_plan_json(bundle).map_err(AppError::SerdeJson)?;
    let plan_md = render_plan_md(bundle);
    let plan_hash = {
        let mut hasher = Sha256::new();
        hasher.update(plan_json.as_bytes());
        format!("{:x}", hasher.finalize())
    };
    let shape_matches = plan_shape_matches(plan_store, base_dir, milestone_id, bundle)?;

    if !shape_matches {
        clear_task_run_lineage(base_dir, milestone_id)?;
    }

    let mut progress = reconcile_progress_for_new_plan(shape_matches, bundle, snapshot);
    progress.total_beads = bundle.bead_count() as u32;

    plan_store.write_plan_json(base_dir, milestone_id, &plan_json)?;
    plan_store.write_plan_md(base_dir, milestone_id, &plan_md)?;

    snapshot.plan_hash = Some(plan_hash);
    snapshot.plan_version = snapshot.plan_version.saturating_add(1);
    snapshot.progress = progress;
    snapshot.updated_at = now;

    let event_type = if snapshot.plan_version == 1 {
        MilestoneEventType::PlanDrafted
    } else {
        MilestoneEventType::PlanUpdated
    };

    validate_snapshot(snapshot, milestone_id)?;
    snapshot_store.write_snapshot(base_dir, milestone_id, snapshot)?;

    let event = MilestoneJournalEvent::new(event_type, now).with_details(format!(
        "Plan v{} with {} beads",
        snapshot.plan_version,
        bundle.bead_count()
    ));
    let line = event.to_ndjson_line().map_err(AppError::SerdeJson)?;
    journal_store.append_event(base_dir, milestone_id, &line)?;
    Ok(())
}

fn reconcile_progress_for_new_plan(
    shape_matches: bool,
    bundle: &MilestoneBundle,
    snapshot: &MilestoneSnapshot,
) -> MilestoneProgress {
    let mut progress = if shape_matches {
        snapshot.progress.clone()
    } else {
        MilestoneProgress::default()
    };

    let completed_or_terminal = progress
        .completed_beads
        .saturating_add(progress.failed_beads)
        .saturating_add(progress.skipped_beads)
        .saturating_add(progress.in_progress_beads);
    let total_beads = bundle.bead_count() as u32;
    if completed_or_terminal > total_beads {
        progress = MilestoneProgress::default();
    }

    progress
}

fn plan_shape_matches(
    plan_store: &impl MilestonePlanPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bundle: &MilestoneBundle,
) -> AppResult<bool> {
    let Ok(existing_plan_json) = plan_store.read_plan_json(base_dir, milestone_id) else {
        return Ok(false);
    };
    let Ok(existing_bundle) = serde_json::from_str::<MilestoneBundle>(&existing_plan_json) else {
        return Ok(false);
    };
    let Ok(existing_shape) = progress_shape_signature(&existing_bundle) else {
        return Ok(false);
    };
    let current_shape = progress_shape_signature(bundle)
        .map_err(|errors| snapshot_corrupt_record(milestone_id, errors.join("; ")))?;
    Ok(existing_shape == current_shape)
}

fn clear_task_run_lineage(base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<()> {
    let task_runs_path =
        FileSystem::milestone_root(base_dir, milestone_id).join("task-runs.ndjson");
    if !task_runs_path.exists() {
        return Ok(());
    }

    FileSystem::write_atomic(&task_runs_path, "")
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
    run_id: &str,
    plan_hash: &str,
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
/// updates snapshot + journal state in the same flow.
#[allow(clippy::too_many_arguments)]
pub fn record_bead_completion(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: &str,
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
    run_id: &str,
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

/// Explicitly repair an already-terminal task run for an exact attempt.
///
/// This narrow path is reserved for milestone reconciliation when the project
/// journal proves the canonical terminal outcome and earlier sync logic wrote a
/// stale terminal row. Generic duplicate finalization continues to be rejected
/// by [`update_task_run`].
#[allow(clippy::too_many_arguments)]
pub fn repair_task_run(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: &str,
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
                    "cannot repair bead '{bead_id}' with non-terminal outcome '{outcome}'"
                ),
            });
        }

        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        let repaired_run = lineage_store.repair_task_run_terminal(
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
        let event_timestamp = repaired_run.finished_at.unwrap_or(finished_at);
        snapshot.updated_at = snapshot.updated_at.max(finished_at).max(event_timestamp);
        validate_snapshot(&snapshot, milestone_id)?;
        snapshot_store.write_snapshot(base_dir, milestone_id, &snapshot)?;

        let event = MilestoneJournalEvent::new(
            event_type_for_outcome(repaired_run.outcome),
            event_timestamp,
        )
        .with_bead(&repaired_run.bead_id)
        .with_details(repaired_run.completion_journal_details());
        let _ = journal_store.repair_completion_event(base_dir, milestone_id, &event)?;

        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool, Ordering};
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
                    bead_id: None,
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
    fn materialize_bundle_refreshes_record_description_on_rematerialize(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let bundle = sample_bundle("materialize-refresh", "Materialize Refresh");
        let initial = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;
        assert_eq!(initial.description, "Test plan.");

        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &initial.id,
            MilestoneStatus::Planning,
            now + chrono::Duration::seconds(1),
        )?;

        let mut updated_bundle = bundle.clone();
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        let refreshed = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(2),
        )?;

        assert_eq!(refreshed.description, "Updated milestone summary.");
        let loaded = load_milestone(&store, base, &initial.id)?;
        assert_eq!(loaded.description, "Updated milestone summary.");
        Ok(())
    }

    struct FailOnceRecordWriteStore {
        fail_next_record_write: AtomicBool,
    }

    impl FailOnceRecordWriteStore {
        fn new() -> Self {
            Self {
                fail_next_record_write: AtomicBool::new(true),
            }
        }
    }

    struct BlockingRecordWriteStore {
        entered_tx: Mutex<Option<mpsc::Sender<()>>>,
        release_rx: Mutex<Option<mpsc::Receiver<()>>>,
    }

    impl BlockingRecordWriteStore {
        fn new(entered_tx: mpsc::Sender<()>, release_rx: mpsc::Receiver<()>) -> Self {
            Self {
                entered_tx: Mutex::new(Some(entered_tx)),
                release_rx: Mutex::new(Some(release_rx)),
            }
        }
    }

    impl MilestoneStorePort for BlockingRecordWriteStore {
        fn milestone_exists(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<bool> {
            FsMilestoneStore.milestone_exists(base_dir, milestone_id)
        }

        fn read_milestone_record(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<MilestoneRecord> {
            FsMilestoneStore.read_milestone_record(base_dir, milestone_id)
        }

        fn write_milestone_record(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            record: &MilestoneRecord,
        ) -> AppResult<()> {
            if let Some(entered_tx) = self.entered_tx.lock().expect("lock entered_tx").take() {
                entered_tx.send(()).expect("signal blocked record write");
            }
            if let Some(release_rx) = self.release_rx.lock().expect("lock release_rx").take() {
                release_rx.recv().expect("release blocked record write");
            }

            FsMilestoneStore.write_milestone_record(base_dir, milestone_id, record)
        }

        fn list_milestone_ids(&self, base_dir: &Path) -> AppResult<Vec<MilestoneId>> {
            FsMilestoneStore.list_milestone_ids(base_dir)
        }

        fn create_milestone_atomic(
            &self,
            base_dir: &Path,
            record: &MilestoneRecord,
            snapshot: &MilestoneSnapshot,
            initial_journal_line: &str,
        ) -> AppResult<()> {
            FsMilestoneStore.create_milestone_atomic(
                base_dir,
                record,
                snapshot,
                initial_journal_line,
            )
        }
    }

    impl MilestoneStorePort for FailOnceRecordWriteStore {
        fn milestone_exists(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<bool> {
            FsMilestoneStore.milestone_exists(base_dir, milestone_id)
        }

        fn read_milestone_record(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<MilestoneRecord> {
            FsMilestoneStore.read_milestone_record(base_dir, milestone_id)
        }

        fn write_milestone_record(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            record: &MilestoneRecord,
        ) -> AppResult<()> {
            if self.fail_next_record_write.swap(false, Ordering::SeqCst) {
                return Err(AppError::Io(std::io::Error::other(
                    "simulated milestone record write failure",
                )));
            }

            FsMilestoneStore.write_milestone_record(base_dir, milestone_id, record)
        }

        fn list_milestone_ids(&self, base_dir: &Path) -> AppResult<Vec<MilestoneId>> {
            FsMilestoneStore.list_milestone_ids(base_dir)
        }

        fn create_milestone_atomic(
            &self,
            base_dir: &Path,
            record: &MilestoneRecord,
            snapshot: &MilestoneSnapshot,
            initial_journal_line: &str,
        ) -> AppResult<()> {
            FsMilestoneStore.create_milestone_atomic(
                base_dir,
                record,
                snapshot,
                initial_journal_line,
            )
        }
    }

    #[test]
    fn materialize_bundle_retry_repairs_description_after_record_write_failure(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let failing_store = FailOnceRecordWriteStore::new();
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let bundle = sample_bundle("materialize-retry", "Materialize Retry");
        let initial = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;
        assert_eq!(initial.description, "Test plan.");

        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &initial.id,
            MilestoneStatus::Planning,
            now + chrono::Duration::seconds(1),
        )?;

        let mut updated_bundle = bundle.clone();
        updated_bundle.executive_summary = "Retried summary repair.".to_owned();
        let error = materialize_bundle(
            &failing_store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(2),
        )
        .expect_err("first rematerialize should fail during milestone.toml rewrite");
        assert!(error
            .to_string()
            .contains("simulated milestone record write failure"));

        let stale_record = load_milestone(&store, base, &initial.id)?;
        assert_eq!(stale_record.description, "Test plan.");

        let repaired = materialize_bundle(
            &failing_store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(3),
        )?;
        assert_eq!(repaired.description, "Retried summary repair.");

        let loaded = load_milestone(&store, base, &initial.id)?;
        assert_eq!(loaded.description, "Retried summary repair.");

        let snapshot = load_snapshot(&snapshot_store, base, &initial.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        Ok(())
    }

    #[test]
    fn materialize_bundle_holds_write_lock_while_repairing_record_description(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let bundle = sample_bundle("materialize-lock", "Materialize Lock");
        let initial = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;
        assert_eq!(initial.description, "Test plan.");

        let mut first_update = bundle.clone();
        first_update.executive_summary = "First rematerialized summary.".to_owned();
        let mut second_update = bundle.clone();
        second_update.executive_summary = "Second rematerialized summary.".to_owned();

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let blocking_store = Arc::new(BlockingRecordWriteStore::new(entered_tx, release_rx));
        let base_dir = base.to_path_buf();

        let first_store = Arc::clone(&blocking_store);
        let first_snapshot_store = snapshot_store;
        let first_journal_store = journal_store;
        let first_plan_store = plan_store;
        let first_handle = std::thread::spawn(move || -> AppResult<MilestoneRecord> {
            materialize_bundle(
                first_store.as_ref(),
                &first_snapshot_store,
                &first_journal_store,
                &first_plan_store,
                &base_dir,
                &first_update,
                now + chrono::Duration::seconds(1),
            )
        });

        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first rematerialization should block while rewriting milestone.toml");

        let base_dir = base.to_path_buf();
        let (finished_tx, finished_rx) = mpsc::channel();
        let second_handle = std::thread::spawn(move || -> AppResult<MilestoneRecord> {
            let result = materialize_bundle(
                &FsMilestoneStore,
                &FsMilestoneSnapshotStore,
                &FsMilestoneJournalStore,
                &FsMilestonePlanStore,
                &base_dir,
                &second_update,
                now + chrono::Duration::seconds(2),
            );
            let _ = finished_tx.send(());
            result
        });

        assert!(
            finished_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "concurrent rematerialization should wait for the in-flight record repair lock"
        );

        release_tx
            .send(())
            .expect("release blocked record write under the lock");

        let first_record = first_handle
            .join()
            .expect("first rematerialization thread should join")?;
        assert_eq!(first_record.description, "First rematerialized summary.");

        let second_record = second_handle
            .join()
            .expect("second rematerialization thread should join")?;
        assert_eq!(second_record.description, "Second rematerialized summary.");

        let final_record = load_milestone(&store, base, &initial.id)?;
        assert_eq!(final_record.description, "Second rematerialized summary.");
        Ok(())
    }

    #[test]
    fn materialize_bundle_preserves_lineage_progress_on_rematerialize(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::contexts::milestone_record::bundle::BeadProposal;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle("materialize-progress", "Materialize Progress");
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            title: "Handle retry flow".to_owned(),
            description: None,
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec![],
            depends_on: vec![],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            title: "Document skip path".to_owned(),
            description: None,
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec![],
            depends_on: vec![],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });

        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
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
            "run-1",
            "plan-v1",
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
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("first bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-2",
            "project-2",
            "run-2",
            "plan-v1",
            now + chrono::Duration::seconds(3),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-2",
            "project-2",
            "run-2",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("second bead failed"),
            now + chrono::Duration::seconds(3),
            now + chrono::Duration::seconds(4),
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-3",
            "project-3",
            "run-3",
            "plan-v1",
            now + chrono::Duration::seconds(5),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-3",
            "project-3",
            "run-3",
            Some("plan-v1"),
            TaskRunOutcome::Skipped,
            Some("third bead skipped"),
            now + chrono::Duration::seconds(5),
            now + chrono::Duration::seconds(6),
        )?;

        let snapshot_before = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_before.progress.total_beads, 3);
        assert_eq!(snapshot_before.progress.completed_beads, 1);
        assert_eq!(snapshot_before.progress.failed_beads, 1);
        assert_eq!(snapshot_before.progress.skipped_beads, 1);

        let mut updated_bundle = bundle.clone();
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(7),
        )?;

        let snapshot_after = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after.status, MilestoneStatus::Ready);
        assert_eq!(snapshot_after.progress.total_beads, 3);
        assert_eq!(snapshot_after.progress.completed_beads, 1);
        assert_eq!(snapshot_after.progress.failed_beads, 1);
        assert_eq!(snapshot_after.progress.skipped_beads, 1);
        Ok(())
    }

    #[test]
    fn materialize_bundle_resets_progress_when_plan_shape_changes_and_clears_old_lineage(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::contexts::milestone_record::bundle::BeadProposal;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle("materialize-progress-reset", "Materialize Progress Reset");
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            title: "Handle retry flow".to_owned(),
            description: None,
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec![],
            depends_on: vec![],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });

        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
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
            "run-1",
            "plan-v1",
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
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("first bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        let mut updated_bundle = bundle.clone();
        updated_bundle.workstreams[0].beads.remove(0);
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(3),
        )?;

        let snapshot_after = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after.status, MilestoneStatus::Ready);
        assert_eq!(snapshot_after.progress.total_beads, 1);
        assert_eq!(snapshot_after.progress.completed_beads, 0);
        assert_eq!(snapshot_after.progress.failed_beads, 0);
        assert_eq!(snapshot_after.progress.skipped_beads, 0);

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-2",
            "run-2",
            "plan-v2",
            now + chrono::Duration::seconds(4),
        )?;
        let snapshot_during_retry = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_during_retry.progress.completed_beads, 0);
        assert_eq!(snapshot_during_retry.progress.in_progress_beads, 1);

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-2",
            "run-2",
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("replacement bead completed"),
            now + chrono::Duration::seconds(4),
            now + chrono::Duration::seconds(5),
        )?;

        let snapshot_after_retry = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after_retry.progress.total_beads, 1);
        assert_eq!(snapshot_after_retry.progress.completed_beads, 1);
        assert_eq!(snapshot_after_retry.progress.failed_beads, 0);
        assert_eq!(snapshot_after_retry.progress.skipped_beads, 0);
        Ok(())
    }

    #[test]
    fn materialize_bundle_resets_progress_when_implicit_beads_reorder(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::contexts::milestone_record::bundle::BeadProposal;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle(
            "materialize-progress-reorder",
            "Materialize Progress Reorder",
        );
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            title: "Second implicit bead".to_owned(),
            description: Some("Runs after the first bead.".to_owned()),
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec!["second".to_owned()],
            depends_on: vec!["bead-1".to_owned()],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });

        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
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
            "run-1",
            "plan-v1",
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
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("first implicit bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        let mut reordered_bundle = bundle.clone();
        reordered_bundle.workstreams[0].beads.swap(0, 1);
        reordered_bundle.executive_summary = "Reordered milestone summary.".to_owned();
        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &reordered_bundle,
            now + chrono::Duration::seconds(3),
        )?;

        let snapshot_after = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after.progress.total_beads, 2);
        assert_eq!(snapshot_after.progress.completed_beads, 0);
        assert_eq!(snapshot_after.progress.failed_beads, 0);
        assert_eq!(snapshot_after.progress.skipped_beads, 0);
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
            "run-1",
            "abc123",
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
            "run-1",
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
            "run-1",
            "plan-v1",
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
            "run-1",
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
            "run-1",
            "plan-v1",
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
            "run-2",
            "plan-v2",
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
            "run-1",
            "plan-hash",
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
            "run-1",
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
            "run-id",
            "plan-hash",
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
            "run-id",
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
            "run-2",
            "plan-hash",
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
            "run-2",
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
            "run-1",
            "plan-v1",
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
            "run-1",
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
            "run-id",
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
            "run-1",
            "plan-v1",
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
            "run-1",
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
            "run-1",
            "plan-v1",
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
            "run-1",
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
            "run-2",
            "plan-v2",
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
            "run-2",
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
            "run-1",
            "plan-v1",
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
            "run-1",
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
            "run-2",
            "plan-v2",
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
            "run-1",
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
            "run-1",
            "plan-skip",
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
            "run-1",
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
            "run-1",
            "plan-hash",
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
            "run-1",
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
            "run-1",
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
            "run-1",
            "plan-v1",
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
            "run-1",
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
            "run-1",
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
            "run-1",
            "plan-hash",
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
            "run-1",
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
            "run-1",
            None,
            now,
            TaskRunOutcome::Succeeded,
            Some("backfilled detail".to_owned()),
            now + chrono::Duration::seconds(2),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-hash"));
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
                "plan_hash": "plan-hash",
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
            "run-1",
            "plan-v1",
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
            "run-1",
            "plan-v1",
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
            "run-1",
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
            "run-1",
            "plan-v1",
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
            "run-1",
            "plan-v2",
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
    fn start_retry_reopens_failed_attempt_for_same_run_id() -> Result<(), Box<dyn std::error::Error>>
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
                id: "start-reopen-failed-run".to_owned(),
                name: "Start Reopen Failed Run".to_owned(),
                description: "reopen a failed attempt when the same run resumes".to_owned(),
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
            "run-1",
            "plan-v1",
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
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("interrupted before resume"),
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
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(10),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Running);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert!(runs[0].finished_at.is_none());
        assert!(runs[0].outcome_detail.is_none());

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Active);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
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
            "run-1",
            "plan-v1",
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
            "run-2",
            "plan-v2",
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
            "run-2",
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
            "run-3",
            "plan-v2",
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
            "run-2",
            "plan-v1",
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
            "run-2",
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
            "run-3",
            "plan-v2",
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
                    "run-1",
                    "plan-v1",
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
            "run-1",
            "plan-v1",
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
                "run-1",
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
                "run-2",
                "plan-v2",
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
            "run-1",
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

    /// When callers provide an explicit plan_hash, it should
    /// take precedence over whatever the snapshot may contain.
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
            "run-1",
            "caller-provided-hash",
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

    /// Required run_id and plan_hash are stored in the lineage entry.
    #[test]
    fn required_run_id_populates_lineage_entry() -> Result<(), Box<dyn std::error::Error>> {
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
                id: "run-id-required".to_owned(),
                name: "Run ID Required".to_owned(),
                description: "test required run_id".to_owned(),
            },
            now,
        )?;

        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &sample_bundle("run-id-required", "Run ID Required"),
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
            "my-run-42",
            "hash-abc",
            now,
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].run_id.as_deref(),
            Some("my-run-42"),
            "run_id must be stored in the lineage entry"
        );
        assert_eq!(
            runs[0].plan_hash.as_deref(),
            Some("hash-abc"),
            "plan_hash must be stored in the lineage entry"
        );
        Ok(())
    }
}
