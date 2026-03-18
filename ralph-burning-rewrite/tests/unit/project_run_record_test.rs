use std::path::Path;

use chrono::{TimeZone, Utc};

use ralph_burning::contexts::project_run_record::model::*;
use ralph_burning::contexts::project_run_record::service::*;
use ralph_burning::shared::domain::{FlowPreset, ProjectId};
use ralph_burning::shared::error::{AppError, AppResult};

// ── Fake implementations of ports for service-level testing ──

struct FakeProjectStore {
    existing_ids: Vec<String>,
}

impl FakeProjectStore {
    fn empty() -> Self {
        Self {
            existing_ids: Vec::new(),
        }
    }

    fn with_existing(ids: &[&str]) -> Self {
        Self {
            existing_ids: ids.iter().map(|s| s.to_string()).collect(),
        }
    }
}

impl ProjectStorePort for FakeProjectStore {
    fn project_exists(&self, _base_dir: &Path, project_id: &ProjectId) -> AppResult<bool> {
        Ok(self.existing_ids.contains(&project_id.to_string()))
    }

    fn read_project_record(
        &self,
        _base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<ProjectRecord> {
        if !self.existing_ids.contains(&project_id.to_string()) {
            return Err(AppError::ProjectNotFound {
                project_id: project_id.to_string(),
            });
        }
        Ok(make_project_record(project_id.as_str()))
    }

    fn list_project_ids(&self, _base_dir: &Path) -> AppResult<Vec<ProjectId>> {
        self.existing_ids
            .iter()
            .map(|id| ProjectId::new(id.as_str()))
            .collect()
    }

    fn stage_delete(&self, _base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
        if !self.existing_ids.contains(&project_id.to_string()) {
            return Err(AppError::ProjectNotFound {
                project_id: project_id.to_string(),
            });
        }
        Ok(())
    }

    fn commit_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
        Ok(())
    }

    fn rollback_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
        Ok(())
    }

    fn create_project_atomic(
        &self,
        _base_dir: &Path,
        _record: &ProjectRecord,
        _prompt_contents: &str,
        _run_snapshot: &RunSnapshot,
        _initial_journal_line: &str,
        _sessions: &SessionStore,
    ) -> AppResult<()> {
        Ok(())
    }
}

struct FakeJournalStore;

impl JournalStorePort for FakeJournalStore {
    fn read_journal(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<Vec<JournalEvent>> {
        Ok(vec![make_project_created_event()])
    }

    fn append_event(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        _line: &str,
    ) -> AppResult<()> {
        Ok(())
    }
}

struct FakeRunSnapshotStore {
    has_active_run: bool,
}

impl FakeRunSnapshotStore {
    fn no_run() -> Self {
        Self {
            has_active_run: false,
        }
    }

    fn active_run() -> Self {
        Self {
            has_active_run: true,
        }
    }
}

impl RunSnapshotPort for FakeRunSnapshotStore {
    fn read_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<RunSnapshot> {
        if self.has_active_run {
            Ok(RunSnapshot {
                active_run: Some(ActiveRun {
                    run_id: "run-1".to_owned(),
                    stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
                        ralph_burning::shared::domain::StageId::Planning,
                    ),
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
                completion_rounds: 0,
                rollback_point_meta: RollbackPointMeta::default(),
                amendment_queue: AmendmentQueueState::default(),
                status_summary: "running".to_owned(),
                last_stage_resolution_snapshot: None,
            })
        } else {
            Ok(RunSnapshot::initial())
        }
    }
}

struct FakeActiveProjectStore {
    active_id: Option<String>,
}

impl FakeActiveProjectStore {
    fn none() -> Self {
        Self { active_id: None }
    }

    fn with_active(id: &str) -> Self {
        Self {
            active_id: Some(id.to_owned()),
        }
    }
}

impl ActiveProjectPort for FakeActiveProjectStore {
    fn read_active_project_id(&self, _base_dir: &Path) -> AppResult<Option<String>> {
        Ok(self.active_id.clone())
    }

    fn clear_active_project(&self, _base_dir: &Path) -> AppResult<()> {
        Ok(())
    }

    fn write_active_project(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
        Ok(())
    }
}

// ── Helpers ──

fn test_timestamp() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 3, 11, 19, 0, 0)
        .single()
        .expect("valid timestamp")
}

fn make_project_record(id: &str) -> ProjectRecord {
    ProjectRecord {
        id: ProjectId::new(id).unwrap(),
        name: format!("Project {id}"),
        flow: FlowPreset::Standard,
        prompt_reference: "prompt.md".to_owned(),
        prompt_hash: "abc123".to_owned(),
        created_at: test_timestamp(),
        status_summary: ProjectStatusSummary::Created,
    }
}

fn make_project_created_event() -> JournalEvent {
    JournalEvent {
        sequence: 1,
        timestamp: test_timestamp(),
        event_type: JournalEventType::ProjectCreated,
        details: serde_json::json!({"project_id": "alpha", "flow": "standard"}),
    }
}

fn dummy_base_dir() -> std::path::PathBuf {
    std::path::PathBuf::from("/tmp/test")
}

// ── Domain Tests ──

#[test]
fn run_snapshot_initial_has_no_active_run() {
    let snapshot = RunSnapshot::initial();
    assert!(!snapshot.has_active_run());
    assert_eq!(snapshot.status, RunStatus::NotStarted);
}

#[test]
fn session_store_empty_has_no_sessions() {
    let store = SessionStore::empty();
    assert!(store.sessions.is_empty());
}

#[test]
fn project_record_flow_is_immutable_after_creation() {
    let record = make_project_record("alpha");
    assert_eq!(record.flow, FlowPreset::Standard);
    // Flow is a plain field with no setter — immutability enforced by
    // not providing mutation methods on ProjectRecord.
}

#[test]
fn journal_event_types_serialize_to_snake_case() {
    let event = make_project_created_event();
    let json = serde_json::to_string(&event).expect("serialize");
    assert!(json.contains("\"project_created\""));
}

// ── Service Tests with Fake Ports ──

#[test]
fn create_project_succeeds_with_valid_input() {
    let store = FakeProjectStore::empty();
    let journal_store = FakeJournalStore;
    let base_dir = dummy_base_dir();

    let input = CreateProjectInput {
        id: ProjectId::new("alpha").unwrap(),
        name: "Alpha Project".to_owned(),
        flow: FlowPreset::Standard,
        prompt_path: "prompt.md".to_owned(),
        prompt_contents: "# My prompt\nDo something.".to_owned(),
        prompt_hash: "hash123".to_owned(),
        created_at: test_timestamp(),
    };

    let result = create_project(&store, &journal_store, &base_dir, input);
    assert!(result.is_ok());

    let record = result.unwrap();
    assert_eq!(record.id.as_str(), "alpha");
    assert_eq!(record.flow, FlowPreset::Standard);
    assert_eq!(record.status_summary, ProjectStatusSummary::Created);
}

#[test]
fn create_project_fails_on_duplicate_id() {
    let store = FakeProjectStore::with_existing(&["alpha"]);
    let journal_store = FakeJournalStore;
    let base_dir = dummy_base_dir();

    let input = CreateProjectInput {
        id: ProjectId::new("alpha").unwrap(),
        name: "Alpha Again".to_owned(),
        flow: FlowPreset::Standard,
        prompt_path: "prompt.md".to_owned(),
        prompt_contents: "content".to_owned(),
        prompt_hash: "hash".to_owned(),
        created_at: test_timestamp(),
    };

    let result = create_project(&store, &journal_store, &base_dir, input);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        AppError::DuplicateProject { .. }
    ));
}

#[test]
fn list_projects_returns_entries_with_active_flag() {
    let store = FakeProjectStore::with_existing(&["alpha", "beta"]);
    let active_store = FakeActiveProjectStore::with_active("alpha");
    let base_dir = dummy_base_dir();

    let entries = list_projects(&store, &active_store, &base_dir).unwrap();
    assert_eq!(entries.len(), 2);

    let alpha = entries.iter().find(|e| e.id.as_str() == "alpha").unwrap();
    assert!(alpha.is_active);

    let beta = entries.iter().find(|e| e.id.as_str() == "beta").unwrap();
    assert!(!beta.is_active);
}

#[test]
fn list_projects_with_no_active_project() {
    let store = FakeProjectStore::with_existing(&["alpha"]);
    let active_store = FakeActiveProjectStore::none();
    let base_dir = dummy_base_dir();

    let entries = list_projects(&store, &active_store, &base_dir).unwrap();
    assert_eq!(entries.len(), 1);
    assert!(!entries[0].is_active);
}

#[test]
fn show_project_returns_detail() {
    let store = FakeProjectStore::with_existing(&["alpha"]);
    let run_store = FakeRunSnapshotStore::no_run();
    let journal_store = FakeJournalStore;
    let active_store = FakeActiveProjectStore::with_active("alpha");
    let base_dir = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let detail = show_project(
        &store,
        &run_store,
        &journal_store,
        &active_store,
        &base_dir,
        &pid,
    )
    .unwrap();

    assert_eq!(detail.record.id.as_str(), "alpha");
    assert!(detail.is_active);
    assert_eq!(detail.journal_event_count, 1);
    assert!(!detail.run_snapshot.has_active_run());
}

#[test]
fn show_project_fails_for_missing_project() {
    let store = FakeProjectStore::empty();
    let run_store = FakeRunSnapshotStore::no_run();
    let journal_store = FakeJournalStore;
    let active_store = FakeActiveProjectStore::none();
    let base_dir = dummy_base_dir();
    let pid = ProjectId::new("missing").unwrap();

    let result = show_project(
        &store,
        &run_store,
        &journal_store,
        &active_store,
        &base_dir,
        &pid,
    );
    assert!(matches!(
        result.unwrap_err(),
        AppError::ProjectNotFound { .. }
    ));
}

#[test]
fn delete_project_succeeds_when_no_active_run() {
    let store = FakeProjectStore::with_existing(&["alpha"]);
    let run_store = FakeRunSnapshotStore::no_run();
    let active_store = FakeActiveProjectStore::none();
    let base_dir = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = delete_project(&store, &run_store, &active_store, &base_dir, &pid);
    assert!(result.is_ok());
}

#[test]
fn delete_project_fails_with_active_run() {
    let store = FakeProjectStore::with_existing(&["alpha"]);
    let run_store = FakeRunSnapshotStore::active_run();
    let active_store = FakeActiveProjectStore::none();
    let base_dir = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = delete_project(&store, &run_store, &active_store, &base_dir, &pid);
    assert!(matches!(
        result.unwrap_err(),
        AppError::ActiveRunDelete { .. }
    ));
}

#[test]
fn delete_project_fails_for_missing_project() {
    let store = FakeProjectStore::empty();
    let run_store = FakeRunSnapshotStore::no_run();
    let active_store = FakeActiveProjectStore::none();
    let base_dir = dummy_base_dir();
    let pid = ProjectId::new("missing").unwrap();

    let result = delete_project(&store, &run_store, &active_store, &base_dir, &pid);
    assert!(matches!(
        result.unwrap_err(),
        AppError::ProjectNotFound { .. }
    ));
}

#[test]
fn run_status_reports_not_started_when_no_active_run() {
    let run_store = FakeRunSnapshotStore::no_run();
    let base_dir = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let status = run_status(&run_store, &base_dir, &pid).unwrap();
    assert_eq!(status.status, "not started");
    assert!(status.stage.is_none());
    assert!(status.cycle.is_none());
}

#[test]
fn run_status_reports_running_with_stage_cursor() {
    let run_store = FakeRunSnapshotStore::active_run();
    let base_dir = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let status = run_status(&run_store, &base_dir, &pid).unwrap();
    assert_eq!(status.status, "running");
    assert_eq!(status.stage, Some("planning".to_owned()));
    assert_eq!(status.cycle, Some(1));
}

// ── Semantic Validation ──

#[test]
fn run_snapshot_validates_running_without_active_run_as_corrupt() {
    let snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: Vec::new(),
        completion_rounds: 0,
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "running".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    let result = snapshot.validate_semantics();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("running"));
}

#[test]
fn run_snapshot_validates_paused_without_active_run_as_valid() {
    let snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Paused,
        cycle_history: Vec::new(),
        completion_rounds: 0,
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "paused".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    assert!(snapshot.validate_semantics().is_ok());
}

#[test]
fn run_snapshot_validates_paused_with_active_run_as_corrupt() {
    let snapshot = RunSnapshot {
        active_run: Some(ActiveRun {
            run_id: "run-1".to_owned(),
            stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
                ralph_burning::shared::domain::StageId::Planning,
            ),
            started_at: test_timestamp(),
            prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
            prompt_hash_at_stage_start: "prompt-hash".to_owned(),
            qa_iterations_current_cycle: 0,
            review_iterations_current_cycle: 0,
            final_review_restart_count: 0,
            stage_resolution_snapshot: None,
        }),
        interrupted_run: None,
        status: RunStatus::Paused,
        cycle_history: Vec::new(),
        completion_rounds: 0,
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "paused".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    let result = snapshot.validate_semantics();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("paused"));
}

#[test]
fn run_snapshot_validates_not_started_with_active_run_as_corrupt() {
    let snapshot = RunSnapshot {
        active_run: Some(ActiveRun {
            run_id: "run-1".to_owned(),
            stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
                ralph_burning::shared::domain::StageId::Planning,
            ),
            started_at: test_timestamp(),
            prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
            prompt_hash_at_stage_start: "prompt-hash".to_owned(),
            qa_iterations_current_cycle: 0,
            review_iterations_current_cycle: 0,
            final_review_restart_count: 0,
            stage_resolution_snapshot: None,
        }),
        interrupted_run: None,
        status: RunStatus::NotStarted,
        cycle_history: Vec::new(),
        completion_rounds: 0,
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "not started".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    let result = snapshot.validate_semantics();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not_started"));
}

#[test]
fn run_snapshot_validates_completed_without_active_run_as_valid() {
    let snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Completed,
        cycle_history: Vec::new(),
        completion_rounds: 3,
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "completed".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    assert!(snapshot.validate_semantics().is_ok());
}

#[test]
fn run_snapshot_validates_failed_without_active_run_as_valid() {
    let snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Failed,
        cycle_history: Vec::new(),
        completion_rounds: 0,
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "failed".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    assert!(snapshot.validate_semantics().is_ok());
}

#[test]
fn run_snapshot_validates_failed_with_active_run_as_corrupt() {
    let snapshot = RunSnapshot {
        active_run: Some(ActiveRun {
            run_id: "run-1".to_owned(),
            stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
                ralph_burning::shared::domain::StageId::Planning,
            ),
            started_at: test_timestamp(),
            prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
            prompt_hash_at_stage_start: "prompt-hash".to_owned(),
            qa_iterations_current_cycle: 0,
            review_iterations_current_cycle: 0,
            final_review_restart_count: 0,
            stage_resolution_snapshot: None,
        }),
        interrupted_run: None,
        status: RunStatus::Failed,
        cycle_history: Vec::new(),
        completion_rounds: 0,
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "failed".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    let result = snapshot.validate_semantics();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("failed"));
}

// ── Terminal State Run Status Reporting ──

struct FakeTerminalRunSnapshotStore {
    status: RunStatus,
    summary: String,
}

impl RunSnapshotPort for FakeTerminalRunSnapshotStore {
    fn read_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<RunSnapshot> {
        Ok(RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: self.status,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            rollback_point_meta: RollbackPointMeta::default(),
            amendment_queue: AmendmentQueueState::default(),
            status_summary: self.summary.clone(),
            last_stage_resolution_snapshot: None,
        })
    }
}

#[test]
fn run_status_reports_completed_for_terminal_snapshot() {
    let run_store = FakeTerminalRunSnapshotStore {
        status: RunStatus::Completed,
        summary: "done".to_owned(),
    };
    let base_dir = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let status = run_status(&run_store, &base_dir, &pid).unwrap();
    assert_eq!(status.status, "completed");
}

#[test]
fn run_status_reports_failed_for_terminal_snapshot() {
    let run_store = FakeTerminalRunSnapshotStore {
        status: RunStatus::Failed,
        summary: "error".to_owned(),
    };
    let base_dir = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let status = run_status(&run_store, &base_dir, &pid).unwrap();
    assert_eq!(status.status, "failed");
}

#[test]
fn delete_project_does_not_touch_pointer_on_stage_failure() {
    use std::cell::Cell;

    struct FailingStageStore {
        existing_ids: Vec<String>,
    }

    impl ProjectStorePort for FailingStageStore {
        fn project_exists(&self, _base_dir: &Path, project_id: &ProjectId) -> AppResult<bool> {
            Ok(self.existing_ids.contains(&project_id.to_string()))
        }

        fn read_project_record(
            &self,
            _base_dir: &Path,
            project_id: &ProjectId,
        ) -> AppResult<ProjectRecord> {
            Ok(make_project_record(project_id.as_str()))
        }

        fn list_project_ids(&self, _base_dir: &Path) -> AppResult<Vec<ProjectId>> {
            self.existing_ids.iter().map(ProjectId::new).collect()
        }

        fn stage_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
            Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "simulated stage failure",
            )))
        }

        fn commit_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
            Ok(())
        }

        fn rollback_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
            Ok(())
        }

        fn create_project_atomic(
            &self,
            _base_dir: &Path,
            _record: &ProjectRecord,
            _prompt_contents: &str,
            _run_snapshot: &RunSnapshot,
            _initial_journal_line: &str,
            _sessions: &SessionStore,
        ) -> AppResult<()> {
            Ok(())
        }
    }

    struct TrackingActiveStore {
        active_id: Option<String>,
        clear_called: Cell<bool>,
        write_called: Cell<bool>,
    }

    impl ActiveProjectPort for TrackingActiveStore {
        fn read_active_project_id(&self, _base_dir: &Path) -> AppResult<Option<String>> {
            Ok(self.active_id.clone())
        }

        fn clear_active_project(&self, _base_dir: &Path) -> AppResult<()> {
            self.clear_called.set(true);
            Ok(())
        }

        fn write_active_project(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
            self.write_called.set(true);
            Ok(())
        }
    }

    let store = FailingStageStore {
        existing_ids: vec!["alpha".to_owned()],
    };
    let run_store = FakeRunSnapshotStore::no_run();
    let active_store = TrackingActiveStore {
        active_id: Some("alpha".to_owned()),
        clear_called: Cell::new(false),
        write_called: Cell::new(false),
    };
    let base_dir = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = delete_project(&store, &run_store, &active_store, &base_dir, &pid);
    assert!(result.is_err(), "delete should fail");
    // The pointer must never be cleared or written when stage fails —
    // the project remains fully addressable with the original pointer.
    assert!(
        !active_store.clear_called.get(),
        "clear_active_project must not be called when stage fails"
    );
    assert!(
        !active_store.write_called.get(),
        "write_active_project must not be called when stage fails"
    );
}

#[test]
fn delete_project_rolls_back_on_clear_pointer_failure() {
    use std::cell::Cell;

    struct TrackingDeleteStore {
        existing_ids: Vec<String>,
        rollback_called: Cell<bool>,
    }

    impl ProjectStorePort for TrackingDeleteStore {
        fn project_exists(&self, _base_dir: &Path, project_id: &ProjectId) -> AppResult<bool> {
            Ok(self.existing_ids.contains(&project_id.to_string()))
        }

        fn read_project_record(
            &self,
            _base_dir: &Path,
            project_id: &ProjectId,
        ) -> AppResult<ProjectRecord> {
            Ok(make_project_record(project_id.as_str()))
        }

        fn list_project_ids(&self, _base_dir: &Path) -> AppResult<Vec<ProjectId>> {
            self.existing_ids.iter().map(ProjectId::new).collect()
        }

        fn stage_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
            Ok(())
        }

        fn commit_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
            Ok(())
        }

        fn rollback_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
            self.rollback_called.set(true);
            Ok(())
        }

        fn create_project_atomic(
            &self,
            _base_dir: &Path,
            _record: &ProjectRecord,
            _prompt_contents: &str,
            _run_snapshot: &RunSnapshot,
            _initial_journal_line: &str,
            _sessions: &SessionStore,
        ) -> AppResult<()> {
            Ok(())
        }
    }

    struct FailingClearActiveStore {
        active_id: Option<String>,
    }

    impl ActiveProjectPort for FailingClearActiveStore {
        fn read_active_project_id(&self, _base_dir: &Path) -> AppResult<Option<String>> {
            Ok(self.active_id.clone())
        }

        fn clear_active_project(&self, _base_dir: &Path) -> AppResult<()> {
            Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "simulated clear failure",
            )))
        }

        fn write_active_project(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
            Ok(())
        }
    }

    let store = TrackingDeleteStore {
        existing_ids: vec!["alpha".to_owned()],
        rollback_called: Cell::new(false),
    };
    let run_store = FakeRunSnapshotStore::no_run();
    let active_store = FailingClearActiveStore {
        active_id: Some("alpha".to_owned()),
    };
    let base_dir = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = delete_project(&store, &run_store, &active_store, &base_dir, &pid);
    // Stage succeeded but pointer clear failed — the project must be
    // rolled back so it remains addressable.
    assert!(result.is_err(), "should propagate clear-pointer failure");
    assert!(
        store.rollback_called.get(),
        "rollback_delete must be called when clear_active_project fails"
    );
}

#[test]
fn delete_project_succeeds_for_completed_terminal_state() {
    let store = FakeProjectStore::with_existing(&["alpha"]);
    let run_store = FakeTerminalRunSnapshotStore {
        status: RunStatus::Completed,
        summary: "done".to_owned(),
    };
    let active_store = FakeActiveProjectStore::none();
    let base_dir = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = delete_project(&store, &run_store, &active_store, &base_dir, &pid);
    assert!(result.is_ok());
}

// ── Failed Stage Summary model tests ──

#[test]
fn failed_stage_summary_serializes_correctly() {
    let summary = FailedStageSummary {
        stage_id: ralph_burning::shared::domain::StageId::Qa,
        cycle: 1,
        attempt: 1,
        failure_class: "QaReviewOutcomeFailure".to_owned(),
        message: "non-passing outcome".to_owned(),
        failed_at: test_timestamp(),
    };
    let json = serde_json::to_string(&summary).unwrap();
    assert!(json.contains("qa"));
    assert!(json.contains("QaReviewOutcomeFailure"));

    let roundtrip: FailedStageSummary = serde_json::from_str(&json).unwrap();
    assert_eq!(
        roundtrip.stage_id,
        ralph_burning::shared::domain::StageId::Qa
    );
}

#[test]
fn run_status_display_matches_display_str() {
    assert_eq!(format!("{}", RunStatus::NotStarted), "not started");
    assert_eq!(format!("{}", RunStatus::Running), "running");
    assert_eq!(format!("{}", RunStatus::Completed), "completed");
    assert_eq!(format!("{}", RunStatus::Failed), "failed");
    assert_eq!(format!("{}", RunStatus::Paused), "paused");
}

#[test]
fn run_snapshot_completed_has_no_active_run() {
    let snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Completed,
        cycle_history: Vec::new(),
        completion_rounds: 1,
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "completed".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    assert!(snapshot.validate_semantics().is_ok());
    assert!(!snapshot.has_active_run());
}

#[test]
fn run_snapshot_failed_has_no_active_run() {
    let snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Failed,
        cycle_history: Vec::new(),
        completion_rounds: 0,
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "failed at QA".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    assert!(snapshot.validate_semantics().is_ok());
    assert!(!snapshot.has_active_run());
}

// ── StageResolutionSnapshot serialization ─────────────────────────────────

#[test]
fn stage_resolution_snapshot_single_target_round_trip() {
    let snapshot = StageResolutionSnapshot {
        stage_id: ralph_burning::shared::domain::StageId::Planning,
        resolved_at: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        primary_target: Some(ResolvedTargetRecord {
            backend_family: "claude".to_owned(),
            model_id: "claude-3-sonnet".to_owned(),
        }),
        prompt_review_validators: Vec::new(),
        prompt_review_refiner: None,
        completion_completers: Vec::new(),
        final_review_reviewers: Vec::new(),
        final_review_arbiter: None,
    };

    let json = serde_json::to_string(&snapshot).unwrap();
    let deserialized: StageResolutionSnapshot = serde_json::from_str(&json).unwrap();
    assert_eq!(snapshot, deserialized);
    // Verify empty vecs and None are omitted from serialization
    assert!(!json.contains("prompt_review_validators"));
    assert!(!json.contains("completion_completers"));
}

#[test]
fn stage_resolution_snapshot_panel_target_round_trip() {
    let snapshot = StageResolutionSnapshot {
        stage_id: ralph_burning::shared::domain::StageId::CompletionPanel,
        resolved_at: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        primary_target: None,
        prompt_review_validators: Vec::new(),
        prompt_review_refiner: None,
        completion_completers: vec![
            ResolvedTargetRecord {
                backend_family: "claude".to_owned(),
                model_id: "claude-3-opus".to_owned(),
            },
            ResolvedTargetRecord {
                backend_family: "codex".to_owned(),
                model_id: "gpt-4o".to_owned(),
            },
        ],
        final_review_reviewers: Vec::new(),
        final_review_arbiter: None,
    };

    let json = serde_json::to_string(&snapshot).unwrap();
    let deserialized: StageResolutionSnapshot = serde_json::from_str(&json).unwrap();
    assert_eq!(snapshot, deserialized);
    assert!(json.contains("completion_completers"));
}

#[test]
fn active_run_with_snapshot_round_trip() {
    let active = ActiveRun {
        run_id: "run-001".to_owned(),
        stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
            ralph_burning::shared::domain::StageId::Planning,
        ),
        started_at: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
        prompt_hash_at_stage_start: "prompt-hash".to_owned(),
        qa_iterations_current_cycle: 1,
        review_iterations_current_cycle: 2,
        final_review_restart_count: 3,
        stage_resolution_snapshot: Some(StageResolutionSnapshot {
            stage_id: ralph_burning::shared::domain::StageId::Planning,
            resolved_at: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
            primary_target: Some(ResolvedTargetRecord {
                backend_family: "claude".to_owned(),
                model_id: "sonnet".to_owned(),
            }),
            prompt_review_validators: Vec::new(),
            prompt_review_refiner: None,
            completion_completers: Vec::new(),
            final_review_reviewers: Vec::new(),
            final_review_arbiter: None,
        }),
    };

    let json = serde_json::to_string(&active).unwrap();
    let deserialized: ActiveRun = serde_json::from_str(&json).unwrap();
    assert_eq!(active, deserialized);
}

#[test]
fn active_run_without_snapshot_omits_field() {
    let active = ActiveRun {
        run_id: "run-002".to_owned(),
        stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
            ralph_burning::shared::domain::StageId::Planning,
        ),
        started_at: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
        prompt_hash_at_stage_start: "prompt-hash".to_owned(),
        qa_iterations_current_cycle: 0,
        review_iterations_current_cycle: 0,
        final_review_restart_count: 0,
        stage_resolution_snapshot: None,
    };

    let json = serde_json::to_string(&active).unwrap();
    assert!(!json.contains("stage_resolution_snapshot"));
    let deserialized: ActiveRun = serde_json::from_str(&json).unwrap();
    assert_eq!(active, deserialized);
}

#[test]
fn payload_record_with_record_kind_and_producer_round_trip() {
    use ralph_burning::contexts::workflow_composition::panel_contracts::{
        RecordKind, RecordProducer,
    };

    let record = PayloadRecord {
        payload_id: "test-payload-1".to_owned(),
        stage_id: ralph_burning::shared::domain::StageId::CompletionPanel,
        cycle: 1,
        attempt: 1,
        created_at: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
        payload: serde_json::json!({"vote_complete": true}),
        record_kind: RecordKind::StageSupporting,
        producer: Some(RecordProducer::Agent {
            backend_family: "claude".to_owned(),
            model_id: "sonnet".to_owned(),
        }),
        completion_round: 2,
    };

    let json = serde_json::to_string(&record).unwrap();
    let deserialized: PayloadRecord = serde_json::from_str(&json).unwrap();
    assert_eq!(record, deserialized);
    assert!(json.contains("\"stage_supporting\""));
    assert!(json.contains("\"completion_round\":2"));
}

#[test]
fn payload_record_defaults_from_legacy_json() {
    use ralph_burning::contexts::workflow_composition::panel_contracts::RecordKind;

    // Simulate a legacy JSON record that lacks the new fields
    let json = r#"{
        "payload_id": "legacy-1",
        "stage_id": "planning",
        "cycle": 1,
        "attempt": 1,
        "created_at": "2025-01-01T00:00:00Z",
        "payload": {}
    }"#;

    let record: PayloadRecord = serde_json::from_str(json).unwrap();
    assert_eq!(record.record_kind, RecordKind::StagePrimary);
    assert!(record.producer.is_none());
    assert_eq!(record.completion_round, 1); // default_completion_round returns 1
}
