use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::{json, Value};

use ralph_burning::adapters::fs::{
    FileSystem, FsMilestoneJournalStore, FsMilestonePlanStore, FsMilestoneSnapshotStore,
    FsMilestoneStore, FsTaskRunLineageStore,
};
use ralph_burning::contexts::milestone_record::model::{
    MilestoneEventType, MilestoneId, MilestoneJournalEvent, MilestoneSnapshot, MilestoneStatus,
    TaskRunOutcome,
};
use ralph_burning::contexts::milestone_record::service::{
    create_milestone, find_runs_for_bead, list_milestones, load_milestone, load_snapshot,
    persist_plan, read_journal, read_task_runs, record_bead_completion, record_bead_start,
    update_status, CreateMilestoneInput, MilestoneJournalPort, MilestoneStorePort,
};
use ralph_burning::shared::error::AppError;
use ralph_burning::test_support::fixtures::{
    MilestoneFixtureBuilder, TaskRunFixture, TempWorkspace, TempWorkspaceBuilder,
};

fn ts(minute_offset: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 4, 18, 12, 0, 0)
        .single()
        .expect("valid test timestamp")
        + Duration::minutes(minute_offset)
}

fn empty_workspace() -> TempWorkspace {
    TempWorkspaceBuilder::new()
        .build()
        .expect("empty temp workspace")
}

fn milestone_root(base_dir: &Path, milestone_id: &MilestoneId) -> PathBuf {
    FileSystem::audit_workspace_root_path(base_dir)
        .join("milestones")
        .join(milestone_id.as_str())
}

fn milestone_paths(base_dir: &Path, milestone_id: &MilestoneId) -> (PathBuf, PathBuf, PathBuf) {
    let root = milestone_root(base_dir, milestone_id);
    (
        root.join("milestone.toml"),
        root.join("status.json"),
        root.join("journal.ndjson"),
    )
}

fn create_planning_milestone(
    workspace: &TempWorkspace,
    milestone_slug: &str,
) -> ralph_burning::contexts::milestone_record::model::MilestoneRecord {
    let bundle = MilestoneFixtureBuilder::new(milestone_slug).bundle();
    create_milestone(
        &FsMilestoneStore,
        workspace.path(),
        CreateMilestoneInput {
            id: milestone_slug.to_owned(),
            name: bundle.identity.name.clone(),
            description: bundle.executive_summary.clone(),
        },
        ts(0),
    )
    .expect("create planning milestone")
}

fn ready_workspace(milestone_slug: &str) -> TempWorkspace {
    TempWorkspaceBuilder::new()
        .with_milestone(MilestoneFixtureBuilder::new(milestone_slug))
        .build()
        .expect("ready milestone workspace")
}

fn completed_workspace(milestone_slug: &str) -> TempWorkspace {
    TempWorkspaceBuilder::new()
        .with_milestone(
            MilestoneFixtureBuilder::new(milestone_slug)
                .with_task_run(TaskRunFixture::succeeded(
                    format!("{milestone_slug}.bead-1"),
                    format!("{milestone_slug}-project"),
                    "run-1",
                ))
                .with_status(MilestoneStatus::Completed),
        )
        .build()
        .expect("completed milestone workspace")
}

fn raw_lines(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .expect("read file")
        .lines()
        .map(str::to_owned)
        .collect()
}

fn latest_json_line(path: &Path) -> Value {
    let line = raw_lines(path).pop().expect("expected at least one line");
    serde_json::from_str(&line).expect("parse json line")
}

fn plan_hash(snapshot: &MilestoneSnapshot) -> String {
    snapshot
        .plan_hash
        .clone()
        .expect("fixture milestones should have a plan hash")
}

fn append_event(base_dir: &Path, milestone_id: &MilestoneId, event: &MilestoneJournalEvent) {
    FsMilestoneJournalStore
        .append_event(
            base_dir,
            milestone_id,
            &event.to_ndjson_line().expect("serialize journal event"),
        )
        .expect("append journal event");
}

#[test]
fn create_milestone_writes_all_expected_artifacts() {
    let workspace = empty_workspace();
    let record = create_planning_milestone(&workspace, "ms-create");
    let (record_path, status_path, journal_path) = milestone_paths(workspace.path(), &record.id);

    assert!(
        record_path.is_file(),
        "expected milestone record at {}",
        record_path.display()
    );
    assert!(
        status_path.is_file(),
        "expected snapshot at {}",
        status_path.display()
    );
    assert!(
        journal_path.is_file(),
        "expected journal at {}",
        journal_path.display()
    );
}

#[test]
fn load_existing_milestone_round_trips_all_fields() {
    let workspace = empty_workspace();
    let record = create_planning_milestone(&workspace, "ms-roundtrip");

    let loaded = load_milestone(&FsMilestoneStore, workspace.path(), &record.id)
        .expect("load persisted milestone");
    let snapshot = load_snapshot(&FsMilestoneSnapshotStore, workspace.path(), &record.id)
        .expect("load persisted snapshot");

    assert_eq!(
        loaded, record,
        "loaded milestone should match the record written to disk"
    );
    assert_eq!(
        snapshot.status,
        MilestoneStatus::Planning,
        "new milestones should start in planning"
    );
    assert_eq!(
        snapshot.updated_at, record.created_at,
        "initial snapshot timestamp should match creation time"
    );
}

#[test]
fn update_milestone_fields_persists_to_disk() {
    let workspace = empty_workspace();
    let record = create_planning_milestone(&workspace, "ms-update");
    let mut updated = record.clone();
    updated.name = "Updated milestone name".to_owned();
    updated.description = "Updated milestone description".to_owned();

    FsMilestoneStore
        .write_milestone_record(workspace.path(), &record.id, &updated)
        .expect("write updated milestone record");

    let reloaded = load_milestone(&FsMilestoneStore, workspace.path(), &record.id)
        .expect("reload updated milestone");
    assert_eq!(
        reloaded.name, "Updated milestone name",
        "updated name should persist to disk"
    );
    assert_eq!(
        reloaded.description, "Updated milestone description",
        "updated description should persist to disk"
    );
}

#[test]
fn list_milestones_returns_all_workspace_milestones() {
    let workspace = empty_workspace();
    for slug in ["ms-alpha", "ms-beta", "ms-gamma"] {
        create_planning_milestone(&workspace, slug);
    }

    let ids = list_milestones(&FsMilestoneStore, workspace.path()).expect("list milestones");
    let id_values: Vec<_> = ids.iter().map(|id| id.as_str()).collect();

    assert_eq!(
        id_values,
        vec!["ms-alpha", "ms-beta", "ms-gamma"],
        "list_milestones should return every created milestone in sorted order"
    );
}

#[test]
fn list_milestones_empty_workspace_returns_empty() {
    let workspace = empty_workspace();

    let ids = list_milestones(&FsMilestoneStore, workspace.path())
        .expect("list empty workspace milestones");
    assert!(
        ids.is_empty(),
        "expected no milestones in a fresh workspace"
    );
}

#[test]
fn load_missing_milestone_returns_clear_error() {
    let workspace = empty_workspace();
    let milestone_id = MilestoneId::new("missing-milestone").expect("valid milestone id");

    let error = load_milestone(&FsMilestoneStore, workspace.path(), &milestone_id)
        .expect_err("missing milestone should fail clearly");

    match error {
        AppError::CorruptRecord { file, details } => {
            assert!(
                file.ends_with("milestones/missing-milestone/milestone.toml"),
                "unexpected error file path: {file}"
            );
            assert!(
                details.contains("not found"),
                "expected not-found details, got: {details}"
            );
        }
        other => panic!("expected corrupt-record error for missing milestone, got {other:?}"),
    }
}

#[test]
fn load_corrupted_milestone_toml_fails_gracefully() {
    let workspace = empty_workspace();
    let record = create_planning_milestone(&workspace, "ms-corrupt");
    let (record_path, _, _) = milestone_paths(workspace.path(), &record.id);
    fs::write(&record_path, "schema_version = [").expect("write corrupt milestone toml");

    let error = load_milestone(&FsMilestoneStore, workspace.path(), &record.id)
        .expect_err("corrupt milestone.toml should fail clearly");

    match error {
        AppError::CorruptRecord { file, details } => {
            assert!(
                file.ends_with("milestones/ms-corrupt/milestone.toml"),
                "unexpected error file path: {file}"
            );
            assert!(
                !details.is_empty(),
                "expected a concrete parse error message"
            );
        }
        other => {
            panic!("expected corrupt-record error for malformed milestone.toml, got {other:?}")
        }
    }
}

#[test]
fn milestone_id_validation_rejects_invalid_chars() {
    let workspace = empty_workspace();

    for invalid in ["", "has/slash", "has\\backslash", ".hidden"] {
        let error = create_milestone(
            &FsMilestoneStore,
            workspace.path(),
            CreateMilestoneInput {
                id: invalid.to_owned(),
                name: "Invalid milestone".to_owned(),
                description: "should fail".to_owned(),
            },
            ts(0),
        )
        .expect_err("invalid milestone id should be rejected");
        assert!(
            matches!(error, AppError::InvalidIdentifier { .. }),
            "expected InvalidIdentifier for '{invalid}', got {error:?}"
        );
    }
}

#[test]
fn append_journal_event_writes_ndjson_line() {
    let workspace = empty_workspace();
    let record = create_planning_milestone(&workspace, "ms-append-journal");
    let (_, _, journal_path) = milestone_paths(workspace.path(), &record.id);
    let event = MilestoneJournalEvent::new(MilestoneEventType::PlanUpdated, ts(1))
        .with_details("plan refreshed");

    append_event(workspace.path(), &record.id, &event);

    let last_line = latest_json_line(&journal_path);
    assert_eq!(
        last_line["event_type"], "plan_updated",
        "appended event type should be serialized to NDJSON"
    );
    assert_eq!(
        last_line["details"], "plan refreshed",
        "appended event details should round-trip through the journal file"
    );
}

#[test]
fn read_journal_returns_events_in_chronological_order() {
    let workspace = empty_workspace();
    let record = create_planning_milestone(&workspace, "ms-chronological");
    append_event(
        workspace.path(),
        &record.id,
        &MilestoneJournalEvent::new(MilestoneEventType::PlanDrafted, ts(1)),
    );
    append_event(
        workspace.path(),
        &record.id,
        &MilestoneJournalEvent::new(MilestoneEventType::PlanUpdated, ts(2)),
    );

    let journal = read_journal(&FsMilestoneJournalStore, workspace.path(), &record.id)
        .expect("read milestone journal");
    let timestamps: Vec<_> = journal.iter().map(|event| event.timestamp).collect();

    assert!(
        timestamps.windows(2).all(|pair| pair[0] <= pair[1]),
        "journal timestamps should be returned in append order: {timestamps:?}"
    );
}

#[test]
fn journal_event_schema_has_required_fields() {
    let workspace = ready_workspace("ms-schema");
    let milestone = &workspace.milestones[0];
    let (_, _, journal_path) = milestone_paths(workspace.path(), &milestone.milestone_id);

    let updated_snapshot = update_status(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        workspace.path(),
        &milestone.milestone_id,
        MilestoneStatus::Running,
        ts(5),
    )
    .expect("transition ready milestone to running");
    assert_eq!(updated_snapshot.status, MilestoneStatus::Running);

    let json = latest_json_line(&journal_path);
    for field in [
        "timestamp",
        "event_type",
        "actor",
        "from_state",
        "to_state",
        "reason",
        "metadata",
    ] {
        assert!(
            json.get(field).is_some(),
            "expected lifecycle journal event to include '{field}': {json}"
        );
    }
}

#[test]
fn truncated_journal_file_loads_partial_events() {
    let workspace = empty_workspace();
    let record = create_planning_milestone(&workspace, "ms-truncated-journal");
    let (_, _, journal_path) = milestone_paths(workspace.path(), &record.id);
    let event = MilestoneJournalEvent::new(MilestoneEventType::Created, ts(0))
        .with_details("Milestone created")
        .to_ndjson_line()
        .expect("serialize valid event");
    let truncated = "{\"timestamp\":\"2026-04-18T12:01:00Z\",\"event_type\":\"plan_updated";
    fs::write(&journal_path, format!("{event}\n{truncated}\n")).expect("write truncated journal");

    let journal = read_journal(&FsMilestoneJournalStore, workspace.path(), &record.id)
        .expect("truncated trailing line should be ignored");
    assert_eq!(
        journal.len(),
        1,
        "expected only the valid prefix of the journal to load"
    );
    assert_eq!(
        journal[0].event_type,
        MilestoneEventType::Created,
        "expected the valid leading event to survive journal recovery"
    );
}

#[test]
fn corrupted_journal_line_is_skipped_or_errors_clearly() {
    let workspace = empty_workspace();
    let record = create_planning_milestone(&workspace, "ms-corrupt-journal");
    let (_, _, journal_path) = milestone_paths(workspace.path(), &record.id);
    let first = MilestoneJournalEvent::new(MilestoneEventType::Created, ts(0))
        .to_ndjson_line()
        .expect("serialize first event");
    let third = MilestoneJournalEvent::new(MilestoneEventType::PlanUpdated, ts(2))
        .to_ndjson_line()
        .expect("serialize third event");
    fs::write(&journal_path, format!("{first}\n{{not-json}}\n{third}\n"))
        .expect("write malformed journal");

    let error = read_journal(&FsMilestoneJournalStore, workspace.path(), &record.id)
        .expect_err("non-trailing journal corruption should fail clearly");
    match error {
        AppError::CorruptRecord { file, details } => {
            assert!(
                file.ends_with("journal.ndjson"),
                "unexpected journal error file: {file}"
            );
            assert!(
                details.contains("line 2"),
                "expected offending line number in error details: {details}"
            );
        }
        other => panic!("expected corrupt-record error for malformed journal line, got {other:?}"),
    }
}

#[test]
fn transition_planning_to_ready_succeeds() {
    let workspace = empty_workspace();
    let record = create_planning_milestone(&workspace, "ms-plan-ready");
    let bundle = MilestoneFixtureBuilder::new("ms-plan-ready").bundle();

    let snapshot = persist_plan(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        &FsMilestonePlanStore,
        workspace.path(),
        &record.id,
        &bundle,
        ts(1),
    )
    .expect("persist plan into planning milestone");
    let journal = read_journal(&FsMilestoneJournalStore, workspace.path(), &record.id)
        .expect("read ready-transition journal");

    assert_eq!(
        snapshot.status,
        MilestoneStatus::Ready,
        "persisting a plan should transition the milestone to ready"
    );
    assert!(
        journal.iter().any(|event| {
            event.event_type == MilestoneEventType::StatusChanged
                && event.from_state == Some(MilestoneStatus::Planning)
                && event.to_state == Some(MilestoneStatus::Ready)
        }),
        "expected a planning -> ready lifecycle event in the journal"
    );
}

#[test]
fn transition_ready_to_running_succeeds() {
    let workspace = ready_workspace("ms-ready-running");
    let milestone = &workspace.milestones[0];

    let snapshot = update_status(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        workspace.path(),
        &milestone.milestone_id,
        MilestoneStatus::Running,
        ts(2),
    )
    .expect("transition ready milestone to running");

    assert_eq!(
        snapshot.status,
        MilestoneStatus::Running,
        "ready milestones should transition to running"
    );
}

#[test]
fn transition_completed_to_running_rejected() {
    let workspace = completed_workspace("ms-completed-running");
    let milestone = &workspace.milestones[0];

    let error = update_status(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        workspace.path(),
        &milestone.milestone_id,
        MilestoneStatus::Running,
        ts(10),
    )
    .expect_err("completed milestone should reject running transition");

    match error {
        AppError::InvalidConfigValue { value, reason, .. } => {
            assert_eq!(value, "completed -> running", "unexpected transition value");
            assert!(
                reason.contains("only to: none"),
                "expected allowed-targets guidance, got: {reason}"
            );
        }
        other => panic!("expected InvalidConfigValue for completed -> running, got {other:?}"),
    }
}

#[test]
fn transition_planning_to_completed_rejected() {
    let workspace = empty_workspace();
    let record = create_planning_milestone(&workspace, "ms-plan-complete");

    let error = update_status(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        workspace.path(),
        &record.id,
        MilestoneStatus::Completed,
        ts(2),
    )
    .expect_err("planning milestone should reject completed transition");

    match error {
        AppError::InvalidConfigValue { value, reason, .. } => {
            assert_eq!(
                value, "planning -> completed",
                "unexpected transition value"
            );
            assert!(
                reason.contains("only to: ready"),
                "expected planning transition guidance, got: {reason}"
            );
        }
        other => panic!("expected InvalidConfigValue for planning -> completed, got {other:?}"),
    }
}

#[test]
fn lifecycle_transition_emits_journal_event() {
    let workspace = ready_workspace("ms-transition-event");
    let milestone = &workspace.milestones[0];

    update_status(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        workspace.path(),
        &milestone.milestone_id,
        MilestoneStatus::Running,
        ts(3),
    )
    .expect("transition ready milestone to running");
    let journal = read_journal(
        &FsMilestoneJournalStore,
        workspace.path(),
        &milestone.milestone_id,
    )
    .expect("read lifecycle journal");

    let transition = journal
        .iter()
        .rev()
        .find(|event| event.event_type == MilestoneEventType::StatusChanged)
        .expect("expected a status-changed journal event");
    assert_eq!(
        transition.from_state,
        Some(MilestoneStatus::Ready),
        "transition should record the previous state"
    );
    assert_eq!(
        transition.to_state,
        Some(MilestoneStatus::Running),
        "transition should record the next state"
    );
    assert_eq!(
        transition.actor.as_deref(),
        Some("system"),
        "update_status should identify the system actor"
    );
    assert_eq!(
        transition.reason.as_deref(),
        Some("execution started"),
        "update_status should record why the transition happened"
    );
}

#[test]
fn invalid_transition_returns_clear_error_message() {
    let workspace = ready_workspace("ms-invalid-transition");
    let milestone = &workspace.milestones[0];

    update_status(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        workspace.path(),
        &milestone.milestone_id,
        MilestoneStatus::Running,
        ts(3),
    )
    .expect("transition ready milestone to running first");

    let error = update_status(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        workspace.path(),
        &milestone.milestone_id,
        MilestoneStatus::Completed,
        ts(4),
    )
    .expect_err("running milestone with open beads should not complete");

    match error {
        AppError::InvalidConfigValue { value, reason, .. } => {
            assert_eq!(value, "running -> completed", "unexpected transition value");
            assert!(
                reason.contains("cannot move to 'completed' until all beads are closed"),
                "expected closure guidance in error message: {reason}"
            );
        }
        other => panic!("expected InvalidConfigValue for blocked completion, got {other:?}"),
    }
}

#[test]
fn record_task_run_for_bead_persists_linkage() {
    let workspace = ready_workspace("ms-linkage");
    let milestone = &workspace.milestones[0];
    let plan_hash = plan_hash(&milestone.snapshot);

    record_bead_start(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        &FsTaskRunLineageStore,
        workspace.path(),
        &milestone.milestone_id,
        "ms-linkage.bead-1",
        "project-linkage",
        "run-1",
        &plan_hash,
        ts(5),
    )
    .expect("record bead start linkage");

    let task_runs = read_task_runs(
        &FsTaskRunLineageStore,
        workspace.path(),
        &milestone.milestone_id,
    )
    .expect("read task-run lineage");
    assert_eq!(
        task_runs.len(),
        1,
        "expected exactly one persisted linkage row"
    );
    assert_eq!(
        task_runs[0].project_id, "project-linkage",
        "linkage should persist the project id"
    );
    assert_eq!(
        task_runs[0].run_id.as_deref(),
        Some("run-1"),
        "linkage should persist the run id"
    );
}

#[test]
fn query_task_runs_for_bead_returns_all_attempts() {
    let workspace = ready_workspace("ms-attempts");
    let milestone = &workspace.milestones[0];
    let plan_hash = plan_hash(&milestone.snapshot);

    record_bead_start(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        &FsTaskRunLineageStore,
        workspace.path(),
        &milestone.milestone_id,
        "ms-attempts.bead-1",
        "project-attempts",
        "run-1",
        &plan_hash,
        ts(5),
    )
    .expect("record first attempt start");
    record_bead_completion(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        &FsTaskRunLineageStore,
        workspace.path(),
        &milestone.milestone_id,
        "ms-attempts.bead-1",
        "project-attempts",
        "run-1",
        Some(&plan_hash),
        TaskRunOutcome::Failed,
        Some("first attempt failed"),
        ts(5),
        ts(6),
    )
    .expect("finalize first attempt");
    record_bead_start(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        &FsTaskRunLineageStore,
        workspace.path(),
        &milestone.milestone_id,
        "ms-attempts.bead-1",
        "project-attempts",
        "run-2",
        &plan_hash,
        ts(7),
    )
    .expect("record retry start");
    record_bead_completion(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        &FsTaskRunLineageStore,
        workspace.path(),
        &milestone.milestone_id,
        "ms-attempts.bead-1",
        "project-attempts",
        "run-2",
        Some(&plan_hash),
        TaskRunOutcome::Succeeded,
        Some("retry succeeded"),
        ts(7),
        ts(8),
    )
    .expect("finalize retry attempt");

    let attempts = find_runs_for_bead(
        &FsTaskRunLineageStore,
        workspace.path(),
        &milestone.milestone_id,
        "ms-attempts.bead-1",
    )
    .expect("query bead attempts");

    assert_eq!(
        attempts.len(),
        2,
        "expected both attempts to be queryable by bead id"
    );
    assert_eq!(
        attempts[0].run_id.as_deref(),
        Some("run-1"),
        "first attempt should be returned first"
    );
    assert_eq!(
        attempts[1].run_id.as_deref(),
        Some("run-2"),
        "retry attempt should be returned second"
    );
}

#[test]
fn multiple_attempts_preserve_retry_history() {
    let workspace = ready_workspace("ms-retry-history");
    let milestone = &workspace.milestones[0];
    let plan_hash = plan_hash(&milestone.snapshot);

    for (offset, run_id, outcome) in [
        (5, "run-1", TaskRunOutcome::Failed),
        (7, "run-2", TaskRunOutcome::Succeeded),
    ] {
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            workspace.path(),
            &milestone.milestone_id,
            "ms-retry-history.bead-1",
            "project-retry",
            run_id,
            &plan_hash,
            ts(offset),
        )
        .expect("record retry attempt start");
        record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            workspace.path(),
            &milestone.milestone_id,
            "ms-retry-history.bead-1",
            "project-retry",
            run_id,
            Some(&plan_hash),
            outcome,
            Some("attempt finished"),
            ts(offset),
            ts(offset + 1),
        )
        .expect("record retry attempt completion");
    }

    let attempts = read_task_runs(
        &FsTaskRunLineageStore,
        workspace.path(),
        &milestone.milestone_id,
    )
    .expect("read persisted retry history");
    let run_ids: Vec<_> = attempts
        .iter()
        .map(|entry| entry.run_id.as_deref().unwrap_or("<missing>"))
        .collect();

    assert_eq!(
        run_ids,
        vec!["run-1", "run-2"],
        "expected each retry attempt to remain in task-run history"
    );
    assert_eq!(
        attempts[0].outcome,
        TaskRunOutcome::Failed,
        "first attempt outcome should be preserved"
    );
    assert_eq!(
        attempts[1].outcome,
        TaskRunOutcome::Succeeded,
        "retry success should be preserved"
    );
}

#[test]
fn linkage_survives_milestone_reload() {
    let workspace = ready_workspace("ms-reload-linkage");
    let milestone = &workspace.milestones[0];
    let plan_hash = plan_hash(&milestone.snapshot);

    record_bead_start(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        &FsTaskRunLineageStore,
        workspace.path(),
        &milestone.milestone_id,
        "ms-reload-linkage.bead-1",
        "project-reload",
        "run-1",
        &plan_hash,
        ts(5),
    )
    .expect("record bead start for reload test");
    record_bead_completion(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        &FsTaskRunLineageStore,
        workspace.path(),
        &milestone.milestone_id,
        "ms-reload-linkage.bead-1",
        "project-reload",
        "run-1",
        Some(&plan_hash),
        TaskRunOutcome::Succeeded,
        Some("completed after reload"),
        ts(5),
        ts(6),
    )
    .expect("record bead completion for reload test");

    let reloaded_record =
        load_milestone(&FsMilestoneStore, workspace.path(), &milestone.milestone_id)
            .expect("reload milestone record");
    let reloaded_snapshot = load_snapshot(
        &FsMilestoneSnapshotStore,
        workspace.path(),
        &milestone.milestone_id,
    )
    .expect("reload milestone snapshot");
    let attempts = read_task_runs(
        &FsTaskRunLineageStore,
        workspace.path(),
        &milestone.milestone_id,
    )
    .expect("reload task-run lineage");

    assert_eq!(
        reloaded_record.id, milestone.milestone_id,
        "milestone id should survive a reload"
    );
    assert_eq!(
        reloaded_snapshot.progress.completed_beads, 1,
        "completed bead progress should survive a reload"
    );
    assert_eq!(
        attempts.len(),
        1,
        "task-run linkage should still be present after reloading milestone state"
    );
    assert_eq!(
        attempts[0].outcome,
        TaskRunOutcome::Succeeded,
        "reloaded task-run lineage should preserve the outcome"
    );
}

#[test]
fn atomic_write_prevents_partial_state() {
    let workspace = ready_workspace("ms-atomic");
    let milestone = &workspace.milestones[0];
    let root = milestone_root(workspace.path(), &milestone.milestone_id);
    let status_path = root.join("status.json");
    let journal_path = root.join("journal.ndjson");
    let pending_path = root.join(".state-commit.json");
    let previous_snapshot = load_snapshot(
        &FsMilestoneSnapshotStore,
        workspace.path(),
        &milestone.milestone_id,
    )
    .expect("read previous snapshot");
    let previous_journal = read_journal(
        &FsMilestoneJournalStore,
        workspace.path(),
        &milestone.milestone_id,
    )
    .expect("read previous journal");

    let mut next_snapshot = previous_snapshot.clone();
    next_snapshot.status = MilestoneStatus::Running;
    next_snapshot.updated_at = ts(9);
    let transition = MilestoneJournalEvent::lifecycle_transition(
        ts(9),
        MilestoneStatus::Ready,
        MilestoneStatus::Running,
        "system",
        "execution started",
        Default::default(),
    );
    let mut next_journal = previous_journal.clone();
    next_journal.push(transition.clone());

    fs::write(
        &pending_path,
        serde_json::to_string_pretty(&json!({
            "recovery_action": "publish",
            "previous_snapshot": previous_snapshot,
            "previous_journal": previous_journal,
            "next_snapshot": next_snapshot,
            "next_journal": next_journal,
        }))
        .expect("serialize pending state commit"),
    )
    .expect("write pending state commit sidecar");
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&json!({
            "status": "running",
            "plan_hash": plan_hash(&milestone.snapshot),
            "plan_version": milestone.snapshot.plan_version,
            "progress": milestone.snapshot.progress.clone(),
            "updated_at": ts(9),
        }))
        .expect("serialize partial snapshot"),
    )
    .expect("write partially published snapshot");
    assert!(
        journal_path.is_file(),
        "journal should still exist before recovery"
    );

    let recovered_snapshot = load_snapshot(
        &FsMilestoneSnapshotStore,
        workspace.path(),
        &milestone.milestone_id,
    )
    .expect("recover partially written snapshot+journal pair");
    let repaired_journal_lines = raw_lines(&journal_path);

    assert!(
        !pending_path.exists(),
        "load_snapshot should finalize the pending state sidecar before any journal read"
    );
    assert_eq!(
        repaired_journal_lines.len(),
        next_journal.len(),
        "load_snapshot should repair the on-disk journal before any subsequent reads"
    );
    assert_eq!(
        serde_json::from_str::<MilestoneJournalEvent>(
            repaired_journal_lines
                .last()
                .expect("repaired journal should contain the transition event"),
        )
        .expect("parse repaired journal event"),
        transition,
        "load_snapshot should durably publish the transition event instead of leaving the old journal visible"
    );
    let recovered_journal = read_journal(
        &FsMilestoneJournalStore,
        workspace.path(),
        &milestone.milestone_id,
    )
    .expect("read recovered journal");

    assert_eq!(
        recovered_snapshot.status,
        MilestoneStatus::Running,
        "recovery should publish the intended next snapshot instead of exposing partial state"
    );
    assert!(
        recovered_journal.iter().any(|event| event == &transition),
        "recovery should publish the intended journal event as well"
    );
}

#[test]
fn temp_directory_isolation_between_tests() {
    let first = TempWorkspaceBuilder::new()
        .with_milestone(
            MilestoneFixtureBuilder::new("shared-ms").with_name("First workspace milestone"),
        )
        .build()
        .expect("first isolated workspace");
    let second = TempWorkspaceBuilder::new()
        .with_milestone(
            MilestoneFixtureBuilder::new("shared-ms").with_name("Second workspace milestone"),
        )
        .build()
        .expect("second isolated workspace");

    let first_record = load_milestone(
        &FsMilestoneStore,
        first.path(),
        &MilestoneId::new("shared-ms").expect("valid milestone id"),
    )
    .expect("load first isolated milestone");
    let second_record = load_milestone(
        &FsMilestoneStore,
        second.path(),
        &MilestoneId::new("shared-ms").expect("valid milestone id"),
    )
    .expect("load second isolated milestone");

    assert_eq!(
        first_record.name, "First workspace milestone",
        "first temp workspace should keep its own milestone data"
    );
    assert_eq!(
        second_record.name, "Second workspace milestone",
        "second temp workspace should keep its own milestone data"
    );
    assert_ne!(
        first.path(),
        second.path(),
        "each test workspace should have a unique temp directory"
    );
}
