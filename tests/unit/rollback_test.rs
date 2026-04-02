use std::cell::RefCell;
use std::path::Path;

use chrono::{TimeZone, Utc};

use ralph_burning::contexts::project_run_record::journal;
use ralph_burning::contexts::project_run_record::model::{
    ActiveRun, AmendmentQueueState, JournalEvent, JournalEventType, RollbackPoint,
    RollbackPointMeta, RunSnapshot, RunStatus,
};
use ralph_burning::contexts::project_run_record::service::{
    get_rollback_point_for_stage, perform_rollback, JournalStorePort, RepositoryResetPort,
    RollbackPointStorePort, RunSnapshotPort, RunSnapshotWritePort,
};
use ralph_burning::shared::domain::{FlowPreset, ProjectId, StageCursor, StageId};
use ralph_burning::shared::error::{AppError, AppResult};

fn test_timestamp() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 3, 12, 22, 0, 0)
        .single()
        .expect("valid timestamp")
}

fn project_id() -> ProjectId {
    ProjectId::new("alpha").expect("project id")
}

fn running_snapshot(stage: StageId) -> RunSnapshot {
    RunSnapshot {
        active_run: Some(ActiveRun {
            run_id: "run-1".to_owned(),
            stage_cursor: StageCursor::initial(stage),
            started_at: test_timestamp(),
            prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
            prompt_hash_at_stage_start: "prompt-hash".to_owned(),
            qa_iterations_current_cycle: 0,
            review_iterations_current_cycle: 0,
            final_review_restart_count: 0,
            stage_resolution_snapshot: None,
        }),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: Vec::new(),
        completion_rounds: 1,
        max_completion_rounds: Some(0),
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: format!("running: {}", stage.display_name()),
        last_stage_resolution_snapshot: None,
    }
}

fn paused_snapshot(summary: &str) -> RunSnapshot {
    RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Paused,
        cycle_history: Vec::new(),
        completion_rounds: 1,
        max_completion_rounds: Some(0),
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: summary.to_owned(),
        last_stage_resolution_snapshot: None,
    }
}

fn rollback_point(rollback_id: &str, stage_id: StageId, snapshot: RunSnapshot) -> RollbackPoint {
    RollbackPoint {
        rollback_id: rollback_id.to_owned(),
        created_at: test_timestamp(),
        stage_id,
        cycle: 1,
        git_sha: Some("deadbeef".to_owned()),
        run_snapshot: snapshot,
    }
}

struct FakeRunSnapshotStore {
    snapshot: RunSnapshot,
}

impl RunSnapshotPort for FakeRunSnapshotStore {
    fn read_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<RunSnapshot> {
        Ok(self.snapshot.clone())
    }
}

#[derive(Default)]
struct TrackingRunSnapshotWriteStore {
    writes: RefCell<Vec<RunSnapshot>>,
}

impl RunSnapshotWritePort for TrackingRunSnapshotWriteStore {
    fn write_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        snapshot: &RunSnapshot,
    ) -> AppResult<()> {
        self.writes.borrow_mut().push(snapshot.clone());
        Ok(())
    }
}

struct FakeJournalStore {
    events: Vec<JournalEvent>,
    appended: RefCell<Vec<JournalEvent>>,
    fail_with: Option<String>,
}

impl JournalStorePort for FakeJournalStore {
    fn read_journal(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<Vec<JournalEvent>> {
        Ok(self.events.clone())
    }

    fn append_event(&self, _base_dir: &Path, _project_id: &ProjectId, line: &str) -> AppResult<()> {
        if let Some(message) = &self.fail_with {
            return Err(AppError::Io(std::io::Error::other(message.clone())));
        }
        self.appended
            .borrow_mut()
            .push(journal::deserialize_event(line)?);
        Ok(())
    }
}

struct FakeRollbackStore {
    points: Vec<RollbackPoint>,
}

impl RollbackPointStorePort for FakeRollbackStore {
    fn write_rollback_point(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        _rollback_point: &RollbackPoint,
    ) -> AppResult<()> {
        Ok(())
    }

    fn list_rollback_points(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<Vec<RollbackPoint>> {
        Ok(self.points.clone())
    }

    fn read_rollback_point_by_stage(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        stage_id: StageId,
    ) -> AppResult<Option<RollbackPoint>> {
        Ok(self
            .points
            .iter()
            .filter(|point| point.stage_id == stage_id)
            .cloned()
            .max_by_key(|point| point.created_at))
    }
}

#[derive(Default)]
struct FakeResetPort {
    shas: RefCell<Vec<String>>,
    fail_with: Option<String>,
}

impl RepositoryResetPort for FakeResetPort {
    fn reset_to_sha(&self, _repo_root: &Path, sha: &str) -> AppResult<()> {
        self.shas.borrow_mut().push(sha.to_owned());
        if let Some(message) = &self.fail_with {
            return Err(AppError::Io(std::io::Error::other(message.clone())));
        }
        Ok(())
    }
}

fn rollback_created_event(sequence: u64, rollback_id: &str, stage_id: StageId) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp: test_timestamp(),
        event_type: JournalEventType::RollbackCreated,
        details: serde_json::json!({
            "rollback_id": rollback_id,
            "stage_id": stage_id.as_str(),
            "cycle": 1
        }),
    }
}

#[test]
fn get_rollback_point_for_stage_excludes_hidden_points_after_logical_rollback() {
    let journal_store = FakeJournalStore {
        events: vec![
            rollback_created_event(2, "rb-planning", StageId::Planning),
            rollback_created_event(4, "rb-review", StageId::Review),
            JournalEvent {
                sequence: 5,
                timestamp: test_timestamp(),
                event_type: JournalEventType::RollbackPerformed,
                details: serde_json::json!({
                    "rollback_id": "rb-planning",
                    "stage_id": "planning",
                    "cycle": 1,
                    "visible_through_sequence": 2,
                    "hard": false,
                    "rollback_count": 1
                }),
            },
        ],
        appended: RefCell::new(Vec::new()),
        fail_with: None,
    };
    let rollback_store = FakeRollbackStore {
        points: vec![
            rollback_point(
                "rb-planning",
                StageId::Planning,
                running_snapshot(StageId::Implementation),
            ),
            rollback_point(
                "rb-review",
                StageId::Review,
                running_snapshot(StageId::CompletionPanel),
            ),
        ],
    };

    let point = get_rollback_point_for_stage(
        &rollback_store,
        &journal_store,
        Path::new("/tmp"),
        &project_id(),
        StageId::Review,
    )
    .expect("lookup succeeds");

    assert!(point.is_none(), "hidden checkpoint should not be visible");
}

#[test]
fn perform_rollback_rejects_non_resumable_statuses() {
    let run_store = FakeRunSnapshotStore {
        snapshot: running_snapshot(StageId::Planning),
    };

    let error = perform_rollback(
        &run_store,
        &TrackingRunSnapshotWriteStore::default(),
        &FakeJournalStore {
            events: vec![],
            appended: RefCell::new(Vec::new()),
            fail_with: None,
        },
        &FakeRollbackStore { points: vec![] },
        None,
        Path::new("/tmp"),
        &project_id(),
        FlowPreset::Standard,
        StageId::Planning,
        false,
    )
    .expect_err("running snapshot should reject rollback");

    assert!(matches!(error, AppError::RollbackInvalidStatus { .. }));
}

#[test]
fn perform_rollback_rejects_stage_outside_project_flow() {
    let run_store = FakeRunSnapshotStore {
        snapshot: RunSnapshot {
            status: RunStatus::Failed,
            last_stage_resolution_snapshot: None,
            ..paused_snapshot("failed")
        },
    };

    let error = perform_rollback(
        &run_store,
        &TrackingRunSnapshotWriteStore::default(),
        &FakeJournalStore {
            events: vec![],
            appended: RefCell::new(Vec::new()),
            fail_with: None,
        },
        &FakeRollbackStore { points: vec![] },
        None,
        Path::new("/tmp"),
        &project_id(),
        FlowPreset::Standard,
        StageId::CiPlan,
        false,
    )
    .expect_err("stage membership should be validated");

    assert!(matches!(error, AppError::RollbackStageNotInFlow { .. }));
}

#[test]
fn perform_rollback_restores_snapshot_and_updates_meta() {
    let current_snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Failed,
        cycle_history: Vec::new(),
        completion_rounds: 2,
        max_completion_rounds: Some(0),
        rollback_point_meta: RollbackPointMeta {
            last_rollback_id: Some("previous".to_owned()),
            rollback_count: 2,
        },
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "failed at review".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    let point_snapshot = running_snapshot(StageId::Implementation);
    let rollback_point = rollback_point("rb-planning", StageId::Planning, point_snapshot.clone());
    let run_store = FakeRunSnapshotStore {
        snapshot: current_snapshot,
    };
    let write_store = TrackingRunSnapshotWriteStore::default();
    let journal_store = FakeJournalStore {
        events: vec![
            JournalEvent {
                sequence: 1,
                timestamp: test_timestamp(),
                event_type: JournalEventType::ProjectCreated,
                details: serde_json::json!({}),
            },
            rollback_created_event(2, "rb-planning", StageId::Planning),
        ],
        appended: RefCell::new(Vec::new()),
        fail_with: None,
    };

    let restored = perform_rollback(
        &run_store,
        &write_store,
        &journal_store,
        &FakeRollbackStore {
            points: vec![rollback_point.clone()],
        },
        None,
        Path::new("/tmp"),
        &project_id(),
        FlowPreset::Standard,
        StageId::Planning,
        false,
    )
    .expect("soft rollback succeeds");

    assert_eq!(restored.rollback_id, rollback_point.rollback_id);
    let writes = write_store.writes.borrow();
    assert_eq!(writes.len(), 1);
    let snapshot = &writes[0];
    assert_eq!(snapshot.status, RunStatus::Paused);
    assert!(snapshot.active_run.is_none());
    assert_eq!(snapshot.rollback_point_meta.rollback_count, 3);
    assert_eq!(
        snapshot.rollback_point_meta.last_rollback_id.as_deref(),
        Some("rb-planning")
    );
    assert!(
        snapshot.status_summary.contains("rollback to Planning"),
        "status summary should mention rollback target"
    );

    let appended = journal_store.appended.borrow();
    assert_eq!(appended.len(), 1);
    assert_eq!(appended[0].event_type, JournalEventType::RollbackPerformed);
    assert_eq!(
        appended[0]
            .details
            .get("visible_through_sequence")
            .and_then(|value| value.as_u64()),
        Some(2)
    );
}

#[test]
fn perform_rollback_preserves_unknown_legacy_max_completion_rounds() {
    let current_snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Failed,
        cycle_history: Vec::new(),
        completion_rounds: 2,
        max_completion_rounds: Some(7),
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "failed at review".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    let mut point_snapshot = running_snapshot(StageId::Implementation);
    point_snapshot.max_completion_rounds = None;
    let rollback_point = rollback_point("rb-planning", StageId::Planning, point_snapshot);
    let run_store = FakeRunSnapshotStore {
        snapshot: current_snapshot,
    };
    let write_store = TrackingRunSnapshotWriteStore::default();
    let journal_store = FakeJournalStore {
        events: vec![
            JournalEvent {
                sequence: 1,
                timestamp: test_timestamp(),
                event_type: JournalEventType::ProjectCreated,
                details: serde_json::json!({}),
            },
            rollback_created_event(2, "rb-planning", StageId::Planning),
        ],
        appended: RefCell::new(Vec::new()),
        fail_with: None,
    };

    perform_rollback(
        &run_store,
        &write_store,
        &journal_store,
        &FakeRollbackStore {
            points: vec![rollback_point],
        },
        None,
        Path::new("/tmp"),
        &project_id(),
        FlowPreset::Standard,
        StageId::Planning,
        false,
    )
    .expect("soft rollback succeeds");

    let writes = write_store.writes.borrow();
    assert_eq!(writes.len(), 1);
    assert_eq!(
        writes[0].max_completion_rounds, None,
        "rollback should preserve unknown historical max_completion_rounds"
    );
}

#[test]
fn hard_rollback_failure_preserves_logical_rollback_state() {
    let run_store = FakeRunSnapshotStore {
        snapshot: paused_snapshot("paused before hard rollback"),
    };
    let write_store = TrackingRunSnapshotWriteStore::default();
    let journal_store = FakeJournalStore {
        events: vec![
            JournalEvent {
                sequence: 1,
                timestamp: test_timestamp(),
                event_type: JournalEventType::ProjectCreated,
                details: serde_json::json!({}),
            },
            rollback_created_event(2, "rb-impl", StageId::Implementation),
        ],
        appended: RefCell::new(Vec::new()),
        fail_with: None,
    };
    let reset_port = FakeResetPort {
        shas: RefCell::new(Vec::new()),
        fail_with: Some("unknown revision".to_owned()),
    };

    let error = perform_rollback(
        &run_store,
        &write_store,
        &journal_store,
        &FakeRollbackStore {
            points: vec![rollback_point(
                "rb-impl",
                StageId::Implementation,
                running_snapshot(StageId::Qa),
            )],
        },
        Some(&reset_port),
        Path::new("/tmp"),
        &project_id(),
        FlowPreset::Standard,
        StageId::Implementation,
        true,
    )
    .expect_err("git reset failure should surface");

    assert!(matches!(error, AppError::RollbackGitResetFailed { .. }));
    let writes = write_store.writes.borrow();
    assert_eq!(writes.len(), 1, "logical rollback must be persisted first");
    assert_eq!(writes[0].status, RunStatus::Paused);
    assert_eq!(journal_store.appended.borrow().len(), 1);
    assert_eq!(
        reset_port.shas.borrow().as_slice(),
        &["deadbeef".to_owned()]
    );
}

#[test]
fn perform_rollback_restores_previous_snapshot_when_journal_append_fails() {
    let original_snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Failed,
        cycle_history: Vec::new(),
        completion_rounds: 2,
        max_completion_rounds: Some(0),
        rollback_point_meta: RollbackPointMeta {
            last_rollback_id: Some("previous".to_owned()),
            rollback_count: 2,
        },
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "failed at review".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    let run_store = FakeRunSnapshotStore {
        snapshot: original_snapshot.clone(),
    };
    let write_store = TrackingRunSnapshotWriteStore::default();
    let journal_store = FakeJournalStore {
        events: vec![
            JournalEvent {
                sequence: 1,
                timestamp: test_timestamp(),
                event_type: JournalEventType::ProjectCreated,
                details: serde_json::json!({}),
            },
            rollback_created_event(2, "rb-planning", StageId::Planning),
        ],
        appended: RefCell::new(Vec::new()),
        fail_with: Some("append failed".to_owned()),
    };

    let error = perform_rollback(
        &run_store,
        &write_store,
        &journal_store,
        &FakeRollbackStore {
            points: vec![rollback_point(
                "rb-planning",
                StageId::Planning,
                running_snapshot(StageId::Implementation),
            )],
        },
        None,
        Path::new("/tmp"),
        &project_id(),
        FlowPreset::Standard,
        StageId::Planning,
        false,
    )
    .expect_err("journal append failure should abort rollback");

    assert!(matches!(error, AppError::Io(_)));
    assert!(journal_store.appended.borrow().is_empty());

    let writes = write_store.writes.borrow();
    assert_eq!(
        writes.len(),
        2,
        "rollback should restore the prior snapshot"
    );
    assert_eq!(writes[0].status, RunStatus::Paused);
    assert_eq!(writes[0].rollback_point_meta.rollback_count, 3);
    assert_eq!(
        writes[0].rollback_point_meta.last_rollback_id.as_deref(),
        Some("rb-planning")
    );
    assert_eq!(writes[1], original_snapshot);
}
