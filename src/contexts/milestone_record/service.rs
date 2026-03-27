use std::path::Path;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use crate::shared::error::{AppError, AppResult};

use super::bundle::{render_plan_json, render_plan_md, MilestoneBundle};
use super::model::{
    MilestoneEventType, MilestoneId, MilestoneJournalEvent, MilestoneProgress, MilestoneRecord,
    MilestoneSnapshot, MilestoneStatus, TaskRunEntry, TaskRunOutcome,
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
    let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
    let old_status = snapshot.status;
    snapshot.status = new_status;

    if new_status.is_terminal() {
        snapshot.active_bead = None;
    }

    snapshot.updated_at = now;

    snapshot
        .validate_semantics()
        .map_err(|details| AppError::CorruptRecord {
            file: format!("milestones/{}/status.json", milestone_id),
            details,
        })?;

    snapshot_store.write_snapshot(base_dir, milestone_id, &snapshot)?;

    let event = MilestoneJournalEvent::new(MilestoneEventType::StatusChanged, now)
        .with_details(format!("{old_status} -> {new_status}"));
    let line = event.to_ndjson_line().map_err(AppError::SerdeJson)?;
    journal_store.append_event(base_dir, milestone_id, &line)?;

    Ok(snapshot)
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

    plan_store.write_plan_json(base_dir, milestone_id, &plan_json)?;
    plan_store.write_plan_md(base_dir, milestone_id, &plan_md)?;

    let plan_hash = {
        let mut hasher = Sha256::new();
        hasher.update(plan_json.as_bytes());
        format!("{:x}", hasher.finalize())
    };

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

    snapshot_store.write_snapshot(base_dir, milestone_id, &snapshot)?;

    let event = MilestoneJournalEvent::new(event_type, now).with_details(format!(
        "Plan v{} with {} beads",
        snapshot.plan_version,
        bundle.bead_count()
    ));
    let line = event.to_ndjson_line().map_err(AppError::SerdeJson)?;
    journal_store.append_event(base_dir, milestone_id, &line)?;

    Ok(snapshot)
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
    now: DateTime<Utc>,
) -> AppResult<()> {
    let snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
    if let Some(existing) = &snapshot.active_bead {
        if existing != bead_id {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "cannot start bead '{bead_id}': bead '{existing}' is already active"
                ),
            });
        }
    }

    let entry = TaskRunEntry {
        bead_id: bead_id.to_owned(),
        project_id: project_id.to_owned(),
        outcome: TaskRunOutcome::Running,
        started_at: now,
        finished_at: None,
    };
    lineage_store.append_task_run(base_dir, milestone_id, &entry)?;

    let mut snapshot = snapshot;
    snapshot.active_bead = Some(bead_id.to_owned());
    snapshot.progress.in_progress_beads = snapshot.progress.in_progress_beads.saturating_add(1);
    if snapshot.status == MilestoneStatus::Ready || snapshot.status == MilestoneStatus::Planning {
        snapshot.status = MilestoneStatus::Active;
    }
    snapshot.updated_at = now;
    snapshot_store.write_snapshot(base_dir, milestone_id, &snapshot)?;

    let event = MilestoneJournalEvent::new(MilestoneEventType::BeadStarted, now)
        .with_bead(bead_id)
        .with_details(format!("project={project_id}"));
    let line = event.to_ndjson_line().map_err(AppError::SerdeJson)?;
    journal_store.append_event(base_dir, milestone_id, &line)?;

    Ok(())
}

/// Record the completion of a bead task run.
///
/// `started_at` should be the original start time from `record_bead_start`;
/// `now` is the completion timestamp.
#[allow(clippy::too_many_arguments)]
pub fn record_bead_completion(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    outcome: TaskRunOutcome,
    started_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let entry = TaskRunEntry {
        bead_id: bead_id.to_owned(),
        project_id: project_id.to_owned(),
        outcome,
        started_at,
        finished_at: Some(now),
    };
    lineage_store.append_task_run(base_dir, milestone_id, &entry)?;

    let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
    snapshot.progress.in_progress_beads = snapshot.progress.in_progress_beads.saturating_sub(1);
    match outcome {
        TaskRunOutcome::Succeeded => {
            snapshot.progress.completed_beads = snapshot.progress.completed_beads.saturating_add(1);
        }
        TaskRunOutcome::Failed => {
            snapshot.progress.failed_beads = snapshot.progress.failed_beads.saturating_add(1);
        }
        _ => {}
    }
    if snapshot
        .active_bead
        .as_deref()
        .is_some_and(|id| id == bead_id)
    {
        snapshot.active_bead = None;
    }
    snapshot.updated_at = now;

    let event_type = if outcome == TaskRunOutcome::Succeeded {
        MilestoneEventType::BeadCompleted
    } else {
        MilestoneEventType::BeadFailed
    };

    snapshot_store.write_snapshot(base_dir, milestone_id, &snapshot)?;

    let event = MilestoneJournalEvent::new(event_type, now)
        .with_bead(bead_id)
        .with_details(format!("project={project_id}, outcome={outcome}"));
    let line = event.to_ndjson_line().map_err(AppError::SerdeJson)?;
    journal_store.append_event(base_dir, milestone_id, &line)?;

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::fs::{
        FsMilestoneJournalStore, FsMilestonePlanStore, FsMilestoneSnapshotStore, FsMilestoneStore,
        FsTaskRunLineageStore,
    };

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
            TaskRunOutcome::Succeeded,
            now,
            now,
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 2);
        Ok(())
    }
}
