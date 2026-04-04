use std::cell::RefCell;
use std::path::Path;

use chrono::{TimeZone, Utc};
use tempfile::tempdir;

use ralph_burning::contexts::project_run_record::model::*;
use ralph_burning::contexts::project_run_record::service;
use ralph_burning::contexts::project_run_record::service::*;
use ralph_burning::contexts::requirements_drafting::service::SeedHandoff;
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

#[derive(Clone)]
struct CapturedProjectCreate {
    record: ProjectRecord,
    prompt_contents: String,
    initial_journal_line: String,
    run_snapshot: RunSnapshot,
}

struct RecordingProjectStore {
    existing_ids: Vec<String>,
    captured: RefCell<Option<CapturedProjectCreate>>,
}

impl RecordingProjectStore {
    fn empty() -> Self {
        Self {
            existing_ids: Vec::new(),
            captured: RefCell::new(None),
        }
    }

    fn with_existing(ids: &[&str]) -> Self {
        Self {
            existing_ids: ids.iter().map(|id| id.to_string()).collect(),
            captured: RefCell::new(None),
        }
    }

    fn captured(&self) -> CapturedProjectCreate {
        self.captured
            .borrow()
            .clone()
            .expect("project creation should be captured")
    }
}

impl ProjectStorePort for RecordingProjectStore {
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
        record: &ProjectRecord,
        prompt_contents: &str,
        run_snapshot: &RunSnapshot,
        initial_journal_line: &str,
        _sessions: &SessionStore,
    ) -> AppResult<()> {
        self.captured.replace(Some(CapturedProjectCreate {
            record: record.clone(),
            prompt_contents: prompt_contents.to_owned(),
            initial_journal_line: initial_journal_line.to_owned(),
            run_snapshot: run_snapshot.clone(),
        }));
        Ok(())
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
    custom_snapshot: Option<RunSnapshot>,
}

impl FakeRunSnapshotStore {
    fn no_run() -> Self {
        Self {
            has_active_run: false,
            custom_snapshot: None,
        }
    }

    fn active_run() -> Self {
        Self {
            has_active_run: true,
            custom_snapshot: None,
        }
    }

    fn with_snapshot(snapshot: RunSnapshot) -> Self {
        Self {
            has_active_run: false,
            custom_snapshot: Some(snapshot),
        }
    }
}

impl RunSnapshotPort for FakeRunSnapshotStore {
    fn read_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<RunSnapshot> {
        if let Some(ref snap) = self.custom_snapshot {
            return Ok(snap.clone());
        }
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
                max_completion_rounds: Some(0),
                rollback_point_meta: RollbackPointMeta::default(),
                amendment_queue: AmendmentQueueState::default(),
                status_summary: "running".to_owned(),
                last_stage_resolution_snapshot: None,
            })
        } else {
            Ok(RunSnapshot::initial(20))
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
        task_source: None,
    }
}

fn make_manual_amendment(amendment_id: &str, body: &str) -> QueuedAmendment {
    QueuedAmendment {
        amendment_id: amendment_id.to_owned(),
        source_stage: StageId::Planning,
        source_cycle: 1,
        source_completion_round: 1,
        body: body.to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 0,
        source: AmendmentSource::Manual,
        dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, body),
    }
}

fn reopened_completed_snapshot_with_round(
    pre_reopen_completion_round: u32,
    pending: Vec<QueuedAmendment>,
) -> RunSnapshot {
    let reopened_completion_round = pre_reopen_completion_round
        .checked_add(1)
        .expect("test completion round should not overflow");

    RunSnapshot {
        active_run: None,
        interrupted_run: Some(ActiveRun {
            run_id: "reopen-alpha".to_owned(),
            stage_cursor: ralph_burning::shared::domain::StageCursor::new(
                StageId::Planning,
                1,
                1,
                reopened_completion_round,
            )
            .unwrap(),
            started_at: test_timestamp(),
            prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
            prompt_hash_at_stage_start: "prompt-hash".to_owned(),
            qa_iterations_current_cycle: 0,
            review_iterations_current_cycle: 0,
            final_review_restart_count: 0,
            stage_resolution_snapshot: None,
        }),
        status: RunStatus::Paused,
        cycle_history: vec![CycleHistoryEntry {
            cycle: 1,
            stage_id: StageId::Planning,
            started_at: test_timestamp(),
            completed_at: Some(test_timestamp()),
        }],
        completion_rounds: reopened_completion_round,
        max_completion_rounds: Some(0),
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState {
            pending,
            ..AmendmentQueueState::default()
        },
        status_summary: "paused: amendments staged".to_owned(),
        last_stage_resolution_snapshot: None,
    }
}

fn reopened_completed_snapshot(pending: Vec<QueuedAmendment>) -> RunSnapshot {
    reopened_completed_snapshot_with_round(1, pending)
}

fn reopened_legacy_completed_snapshot_with_round(
    pre_reopen_completion_round: u32,
    pending: Vec<QueuedAmendment>,
) -> RunSnapshot {
    let mut snapshot = reopened_completed_snapshot_with_round(pre_reopen_completion_round, pending);
    snapshot.max_completion_rounds = None;
    snapshot
}

fn paused_snapshot(pending: Vec<QueuedAmendment>) -> RunSnapshot {
    RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Paused,
        cycle_history: vec![CycleHistoryEntry {
            cycle: 1,
            stage_id: StageId::Planning,
            started_at: test_timestamp(),
            completed_at: Some(test_timestamp()),
        }],
        completion_rounds: 1,
        max_completion_rounds: Some(0),
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState {
            pending,
            ..AmendmentQueueState::default()
        },
        status_summary: "paused".to_owned(),
        last_stage_resolution_snapshot: None,
    }
}

fn completed_snapshot_with_max_completion_rounds(
    max_completion_rounds: Option<u32>,
) -> RunSnapshot {
    RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Completed,
        cycle_history: vec![CycleHistoryEntry {
            cycle: 1,
            stage_id: StageId::FinalReview,
            started_at: test_timestamp(),
            completed_at: Some(test_timestamp()),
        }],
        completion_rounds: 1,
        max_completion_rounds,
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "completed".to_owned(),
        last_stage_resolution_snapshot: None,
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

fn make_seed_handoff(
    project_id: &str,
    flow: FlowPreset,
    recommended_flow: Option<FlowPreset>,
) -> SeedHandoff {
    SeedHandoff {
        requirements_run_id: "req-20260318-220000".to_owned(),
        project_id: project_id.to_owned(),
        project_name: format!("Project {project_id}"),
        flow,
        prompt_body: "# Seed prompt\nUse the payload body.".to_owned(),
        prompt_path: std::path::PathBuf::from("/tmp/requirements/seed/prompt.md"),
        recommended_flow,
    }
}

fn sample_bead_context() -> BeadProjectContext {
    BeadProjectContext {
        milestone_id: "ms-alpha".to_owned(),
        milestone_name: "Alpha Milestone".to_owned(),
        milestone_description: "Deliver the alpha milestone.".to_owned(),
        milestone_summary: Some("Ship milestone-aware task execution.".to_owned()),
        milestone_goals: vec![
            "Create bead-backed tasks without manual setup".to_owned(),
            "Keep run start compatibility intact".to_owned(),
        ],
        milestone_non_goals: vec!["Do not absorb unrelated future-bead work".to_owned()],
        milestone_constraints: vec!["Reuse the current project substrate".to_owned()],
        agents_guidance: Some("Follow AGENTS.md and keep changes inspectable.".to_owned()),
        bead_id: "ms-alpha.bead-2".to_owned(),
        bead_title: "Bootstrap bead-backed task creation".to_owned(),
        bead_description: Some(
            "Create a project directly from milestone and bead context.".to_owned(),
        ),
        bead_acceptance_criteria: vec![
            "Controller can create the project without manual setup".to_owned(),
            "Created task remains compatible with run start".to_owned(),
        ],
        bead_dependencies: vec![
            "ms-alpha.bead-1 (Define task-source metadata)".to_owned(),
            "ms-alpha.epic-1 (Task substrate epic)".to_owned(),
        ],
        already_planned_elsewhere: vec![
            "ms-alpha.bead-4 handles dependency-driven follow-up work.".to_owned(),
        ],
        review_policy:
            ralph_burning::contexts::project_run_record::task_prompt_contract::default_review_policy(
            ),
        parent_epic_id: Some("ms-alpha.epic-1".to_owned()),
        flow: FlowPreset::QuickDev,
        plan_hash: Some("plan-hash-123".to_owned()),
        plan_version: Some(3),
    }
}

// ── Domain Tests ──

#[test]
fn run_snapshot_initial_has_no_active_run() {
    let snapshot = RunSnapshot::initial(20);
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
        task_source: None,
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
        task_source: None,
    };

    let result = create_project(&store, &journal_store, &base_dir, input);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        AppError::DuplicateProject { .. }
    ));
}

#[test]
fn create_project_from_seed_uses_seed_flow_without_override() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;
    let handoff = make_seed_handoff(
        "seed-alpha",
        FlowPreset::Standard,
        Some(FlowPreset::QuickDev),
    );

    let record = create_project_from_seed(
        &store,
        &journal_store,
        &dummy_base_dir(),
        handoff,
        None,
        test_timestamp(),
    )
    .expect("create project from seed");

    let captured = store.captured();
    assert_eq!(record.id.as_str(), "seed-alpha");
    assert_eq!(record.flow, FlowPreset::Standard);
    assert_eq!(captured.record.flow, FlowPreset::Standard);
    assert_eq!(captured.record.prompt_reference, "prompt.md");
    assert_eq!(captured.run_snapshot.status, RunStatus::NotStarted);
}

#[test]
fn create_project_from_seed_applies_flow_override() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;
    let handoff = make_seed_handoff(
        "seed-override",
        FlowPreset::Standard,
        Some(FlowPreset::QuickDev),
    );

    let record = create_project_from_seed(
        &store,
        &journal_store,
        &dummy_base_dir(),
        handoff,
        Some(FlowPreset::QuickDev),
        test_timestamp(),
    )
    .expect("create project from seed with override");

    let captured = store.captured();
    assert_eq!(record.flow, FlowPreset::QuickDev);
    assert_eq!(captured.record.flow, FlowPreset::QuickDev);

    let event: JournalEvent =
        serde_json::from_str(&captured.initial_journal_line).expect("parse journal line");
    assert_eq!(event.details["flow"], "quick_dev");
    assert_eq!(event.details["seed_flow"], "standard");
    assert_eq!(event.details["recommended_flow"], "quick_dev");
}

#[test]
fn create_project_from_seed_writes_prompt_body_not_prompt_path_contents() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;
    let mut handoff = make_seed_handoff("seed-prompt", FlowPreset::Standard, None);
    handoff.prompt_body = "Prompt body from seed payload".to_owned();
    handoff.prompt_path = std::path::PathBuf::from("/tmp/requirements/seed/other-prompt.md");

    create_project_from_seed(
        &store,
        &journal_store,
        &dummy_base_dir(),
        handoff,
        None,
        test_timestamp(),
    )
    .expect("create project from seed");

    let captured = store.captured();
    assert_eq!(captured.prompt_contents, "Prompt body from seed payload");
    assert_eq!(
        captured.record.prompt_hash,
        ralph_burning::adapters::fs::FileSystem::prompt_hash("Prompt body from seed payload")
    );
}

#[test]
fn create_project_from_seed_journal_records_requirements_metadata() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;
    let handoff = make_seed_handoff(
        "seed-journal",
        FlowPreset::DocsChange,
        Some(FlowPreset::Standard),
    );

    create_project_from_seed(
        &store,
        &journal_store,
        &dummy_base_dir(),
        handoff,
        None,
        test_timestamp(),
    )
    .expect("create project from seed");

    let captured = store.captured();
    let event: JournalEvent =
        serde_json::from_str(&captured.initial_journal_line).expect("parse journal line");
    assert_eq!(event.event_type, JournalEventType::ProjectCreated);
    assert_eq!(event.details["source"], "requirements");
    assert_eq!(event.details["requirements_run_id"], "req-20260318-220000");
    assert_eq!(event.details["flow"], "docs_change");
}

#[test]
fn create_project_from_seed_rejects_duplicate_project_id() {
    let store = RecordingProjectStore::with_existing(&["dup-seed"]);
    let journal_store = FakeJournalStore;
    let handoff = make_seed_handoff("dup-seed", FlowPreset::Standard, None);

    let error = create_project_from_seed(
        &store,
        &journal_store,
        &dummy_base_dir(),
        handoff,
        None,
        test_timestamp(),
    )
    .expect_err("duplicate seed project should fail");

    assert!(matches!(error, AppError::DuplicateProject { .. }));
}

#[test]
fn render_bead_task_prompt_includes_milestone_scope_and_agents_guidance() {
    let prompt = render_bead_task_prompt(&sample_bead_context());

    assert!(prompt.contains("This project executes bead `ms-alpha.bead-2`"));
    assert!(prompt.contains("bead_execution_prompt"));
    assert!(prompt.contains("## Milestone Summary"));
    assert!(prompt.contains("## Current Bead Details"));
    assert!(prompt.contains("## Must-Do Scope"));
    assert!(prompt.contains("## Explicit Non-Goals"));
    assert!(prompt.contains("## Acceptance Criteria"));
    assert!(prompt.contains("## Already Planned Elsewhere"));
    assert!(prompt.contains("## Review Policy"));
    assert!(prompt.contains("## AGENTS / Repo Guidance"));
    assert!(prompt.contains("Follow AGENTS.md and keep changes inspectable."));
}

#[test]
fn render_bead_task_prompt_is_deterministic_for_hashing_and_drift_checks() {
    let context = sample_bead_context();
    let first = render_bead_task_prompt(&context);
    let second = render_bead_task_prompt(&context);

    assert_eq!(first, second);
    assert!(first.contains("<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->"));
    let milestone_index = first.find("## Milestone Summary").unwrap();
    let bead_index = first.find("## Current Bead Details").unwrap();
    let scope_index = first.find("## Must-Do Scope").unwrap();
    let review_index = first.find("## Review Policy").unwrap();
    assert!(milestone_index < bead_index);
    assert!(bead_index < scope_index);
    assert!(scope_index < review_index);
}

#[test]
fn render_bead_task_prompt_preserves_multiline_agents_guidance_verbatim() {
    let mut context = sample_bead_context();
    context.agents_guidance = Some(
        "### Repo Notes\n\n- Preserve existing markdown blocks.\n\n```bash\nbr ready\n```"
            .to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    assert!(prompt.contains("### Repo Notes"));
    assert!(prompt.contains("```bash\nbr ready\n```"));
    assert!(!prompt.contains("- ### Repo Notes"));
}

#[test]
fn render_bead_task_prompt_escapes_canonical_headings_inside_agents_guidance() {
    let mut context = sample_bead_context();
    context.agents_guidance = Some(
        "## Acceptance Criteria\nKeep this as embedded guidance, not a new contract section."
            .to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    assert!(prompt.contains("    ## Acceptance Criteria"));
    assert!(!prompt.contains("\n\n## Acceptance Criteria\n\nKeep this as embedded guidance"));
    assert!(
        ralph_burning::contexts::project_run_record::task_prompt_contract::validate_canonical_prompt_shape(&prompt)
            .is_ok()
    );
}

#[test]
fn render_bead_task_prompt_preserves_fenced_canonical_headings_inside_agents_guidance_verbatim() {
    let mut context = sample_bead_context();
    context.agents_guidance = Some(
        "```md\n## Review Policy\nKeep this fenced example verbatim.\n\n## Acceptance Criteria\nStill example content.\n```"
            .to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    assert!(prompt.contains(
        "```md\n## Review Policy\nKeep this fenced example verbatim.\n\n## Acceptance Criteria\nStill example content.\n```"
    ));
    assert!(
        !prompt.contains("```md\n    ## Review Policy")
            && !prompt.contains("```md\n    ## Acceptance Criteria")
    );
    assert!(
        ralph_burning::contexts::project_run_record::task_prompt_contract::validate_canonical_prompt_shape(&prompt)
            .is_ok()
    );
}

#[test]
fn render_bead_task_prompt_extracts_bead_local_non_goals_and_strips_embedded_contract_sections() {
    let mut context = sample_bead_context();
    context.bead_description = Some(
        "Goal:\nKeep the task prompt contract explicit.\n\nScope:\n- update the canonical prompt generator\n- keep consumer behavior aligned\n\nNon-goals:\n- do not redesign unrelated workflow stages\n- do not broaden the bead scope\n\n## Acceptance Criteria\n\n- this embedded section should not stay in must-do scope\n".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    let must_do_start = prompt
        .find("## Must-Do Scope")
        .expect("must-do section should exist");
    let non_goals_start = prompt
        .find("## Explicit Non-Goals")
        .expect("non-goals section should exist");
    let must_do_section = &prompt[must_do_start..non_goals_start];

    assert!(must_do_section.contains("Goal:"));
    assert!(must_do_section.contains("Scope:"));
    assert!(!must_do_section.contains("Non-goals:"));
    assert!(!must_do_section.contains("## Acceptance Criteria"));
    assert!(prompt.contains("- do not redesign unrelated workflow stages"));
    assert!(prompt.contains("- do not broaden the bead scope"));
    assert!(prompt.contains("- Do not absorb unrelated future-bead work"));
}

#[test]
fn render_bead_task_prompt_keeps_plain_subsections_inside_non_goals() {
    let mut context = sample_bead_context();
    context.bead_description = Some(
        "Scope:\n- update the canonical prompt generator\n\nNon-goals:\nExamples:\n- do not change CLI\n- do not rewrite the workflow engine\n".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    let must_do_start = prompt
        .find("## Must-Do Scope")
        .expect("must-do section should exist");
    let non_goals_start = prompt
        .find("## Explicit Non-Goals")
        .expect("non-goals section should exist");
    let must_do_section = &prompt[must_do_start..non_goals_start];

    assert!(must_do_section.contains("Scope:"));
    assert!(!must_do_section.contains("Examples:"));
    assert!(!must_do_section.contains("do not change CLI"));
    assert!(prompt.contains("- do not change CLI"));
    assert!(prompt.contains("- do not rewrite the workflow engine"));
}

#[test]
fn render_bead_task_prompt_keeps_markdown_subsections_inside_non_goals() {
    let mut context = sample_bead_context();
    context.bead_description = Some(
        "Scope:\n- update the canonical prompt generator\n\nNon-goals:\n### Examples\n- do not change CLI\n- do not rewrite the workflow engine\n".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    let must_do_start = prompt
        .find("## Must-Do Scope")
        .expect("must-do section should exist");
    let non_goals_start = prompt
        .find("## Explicit Non-Goals")
        .expect("non-goals section should exist");
    let must_do_section = &prompt[must_do_start..non_goals_start];

    assert!(must_do_section.contains("Scope:"));
    assert!(!must_do_section.contains("### Examples"));
    assert!(!must_do_section.contains("do not change CLI"));
    assert!(prompt.contains("- do not change CLI"));
    assert!(prompt.contains("- do not rewrite the workflow engine"));
}

#[test]
fn render_bead_task_prompt_resets_non_goals_when_a_new_scope_subsection_starts() {
    let mut context = sample_bead_context();
    context.bead_description = Some(
        "Scope:\n- update the canonical prompt generator\n\nNon-goals:\n- do not change CLI\n\nContract hardening notes:\n- preserve fix-now boundaries\n- keep planned-elsewhere work separate\n".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    let must_do_start = prompt
        .find("## Must-Do Scope")
        .expect("must-do section should exist");
    let non_goals_start = prompt
        .find("## Explicit Non-Goals")
        .expect("non-goals section should exist");
    let acceptance_start = prompt
        .find("## Acceptance Criteria")
        .expect("acceptance section should exist");
    let must_do_section = &prompt[must_do_start..non_goals_start];
    let non_goals_section = &prompt[non_goals_start..acceptance_start];

    assert!(must_do_section.contains("Contract hardening notes:"));
    assert!(must_do_section.contains("preserve fix-now boundaries"));
    assert!(must_do_section.contains("keep planned-elsewhere work separate"));
    assert!(non_goals_section.contains("- do not change CLI"));
    assert!(!non_goals_section.contains("Contract hardening notes:"));
}

#[test]
fn render_bead_task_prompt_keeps_single_word_labels_inside_non_goals_after_blank_lines() {
    let mut context = sample_bead_context();
    context.bead_description = Some(
        "Scope:\n- update the canonical prompt generator\n\nNon-goals:\n- do not change CLI\n\nDetails:\n- preserve the nested rationale inside explicit non-goals\n".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    let must_do_start = prompt
        .find("## Must-Do Scope")
        .expect("must-do section should exist");
    let non_goals_start = prompt
        .find("## Explicit Non-Goals")
        .expect("non-goals section should exist");
    let acceptance_start = prompt
        .find("## Acceptance Criteria")
        .expect("acceptance section should exist");
    let must_do_section = &prompt[must_do_start..non_goals_start];
    let non_goals_section = &prompt[non_goals_start..acceptance_start];

    assert!(!must_do_section.contains("Details:"));
    assert!(non_goals_section.contains("Details:"));
    assert!(non_goals_section.contains("preserve the nested rationale inside explicit non-goals"));
}

#[test]
fn render_bead_task_prompt_keeps_plain_subsections_inside_embedded_acceptance_criteria() {
    let mut context = sample_bead_context();
    context.bead_description = Some(
        "Scope:\n- keep the contract explicit\n\n## Acceptance Criteria\nNotes:\n- keep the rendered contract stable\n- keep downstream consumers aligned\n".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    let must_do_start = prompt
        .find("## Must-Do Scope")
        .expect("must-do section should exist");
    let non_goals_start = prompt
        .find("## Explicit Non-Goals")
        .expect("non-goals section should exist");
    let must_do_section = &prompt[must_do_start..non_goals_start];

    assert!(must_do_section.contains("Scope:"));
    assert!(!must_do_section.contains("Notes:"));
    assert!(!must_do_section.contains("keep the rendered contract stable"));
}

#[test]
fn render_bead_task_prompt_keeps_markdown_subsections_inside_embedded_acceptance_criteria() {
    let mut context = sample_bead_context();
    context.bead_description = Some(
        "Scope:\n- keep the contract explicit\n\n## Acceptance Criteria\n### Notes\n- keep the rendered contract stable\n- keep downstream consumers aligned\n".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    let must_do_start = prompt
        .find("## Must-Do Scope")
        .expect("must-do section should exist");
    let non_goals_start = prompt
        .find("## Explicit Non-Goals")
        .expect("non-goals section should exist");
    let must_do_section = &prompt[must_do_start..non_goals_start];

    assert!(must_do_section.contains("Scope:"));
    assert!(!must_do_section.contains("### Notes"));
    assert!(!must_do_section.contains("keep the rendered contract stable"));
}

#[test]
fn render_bead_task_prompt_uses_description_acceptance_criteria_when_structured_field_is_empty() {
    let mut context = sample_bead_context();
    context.bead_acceptance_criteria.clear();
    context.bead_description = Some(
        "Scope:\n- keep the contract explicit\n\n## Acceptance Criteria\n- preserve the canonical section order\n- keep prompt generation deterministic\n".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    let acceptance_start = prompt
        .find("## Acceptance Criteria")
        .expect("acceptance section should exist");
    let planned_start = prompt
        .find("## Already Planned Elsewhere")
        .expect("planned elsewhere section should exist");
    let acceptance_section = &prompt[acceptance_start..planned_start];

    assert!(acceptance_section.contains("- preserve the canonical section order"));
    assert!(acceptance_section.contains("- keep prompt generation deterministic"));
    assert!(!acceptance_section.contains("No explicit acceptance criteria were supplied."));
}

#[test]
fn render_bead_task_prompt_extracts_numbered_lists_from_colon_suffixed_markdown_sections() {
    let mut context = sample_bead_context();
    context.bead_acceptance_criteria.clear();
    context.bead_description = Some(
        "Scope:\n- keep the contract explicit\n\n## Non-goals:\n1. do not redesign unrelated workflow stages\n2. do not broaden the bead scope\n\n## Acceptance Criteria:\n1. preserve numbered acceptance criteria\n2. avoid the fallback acceptance message\n".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    let must_do_start = prompt
        .find("## Must-Do Scope")
        .expect("must-do section should exist");
    let non_goals_start = prompt
        .find("## Explicit Non-Goals")
        .expect("non-goals section should exist");
    let acceptance_start = prompt
        .find("## Acceptance Criteria")
        .expect("acceptance section should exist");
    let planned_start = prompt
        .find("## Already Planned Elsewhere")
        .expect("planned elsewhere section should exist");
    let must_do_section = &prompt[must_do_start..non_goals_start];
    let non_goals_section = &prompt[non_goals_start..acceptance_start];
    let acceptance_section = &prompt[acceptance_start..planned_start];

    assert!(!must_do_section.contains("## Non-goals:"));
    assert!(!must_do_section.contains("## Acceptance Criteria:"));
    assert!(non_goals_section.contains("- do not redesign unrelated workflow stages"));
    assert!(non_goals_section.contains("- do not broaden the bead scope"));
    assert!(acceptance_section.contains("- preserve numbered acceptance criteria"));
    assert!(acceptance_section.contains("- avoid the fallback acceptance message"));
    assert!(!acceptance_section.contains("No explicit acceptance criteria were supplied."));
}

#[test]
fn render_bead_task_prompt_preserves_nested_list_indentation_in_extracted_sections() {
    let mut context = sample_bead_context();
    context.bead_acceptance_criteria.clear();
    context.bead_description = Some(
        "Scope:\n- keep the contract explicit\n\nNon-goals:\n- do not broaden the bead scope\n - keep follow-up docs in a sibling bead\n\n## Acceptance Criteria\n- preserve the canonical contract shape\n - keep supporting rationale nested under the primary criterion\n".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    let non_goals_start = prompt
        .find("## Explicit Non-Goals")
        .expect("non-goals section should exist");
    let acceptance_start = prompt
        .find("## Acceptance Criteria")
        .expect("acceptance section should exist");
    let planned_start = prompt
        .find("## Already Planned Elsewhere")
        .expect("planned elsewhere section should exist");
    let non_goals_section = &prompt[non_goals_start..acceptance_start];
    let acceptance_section = &prompt[acceptance_start..planned_start];

    assert!(non_goals_section
        .contains("- do not broaden the bead scope\n     - keep follow-up docs in a sibling bead"));
    assert!(!non_goals_section.contains("\n- keep follow-up docs in a sibling bead"));
    assert!(acceptance_section.contains("- preserve the canonical contract shape\n     - keep supporting rationale nested under the primary criterion"));
    assert!(!acceptance_section
        .contains("\n- keep supporting rationale nested under the primary criterion"));
}

#[test]
fn render_bead_task_prompt_ignores_embedded_section_labels_inside_fenced_code_blocks() {
    let mut context = sample_bead_context();
    context.bead_description = Some(
        "Here is an example:\n```md\nNon-goals:\n- this is code, not a real section\n```\n\nScope:\n- keep the contract explicit\n".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    let must_do_start = prompt
        .find("## Must-Do Scope")
        .expect("must-do section should exist");
    let non_goals_start = prompt
        .find("## Explicit Non-Goals")
        .expect("non-goals section should exist");
    let acceptance_start = prompt
        .find("## Acceptance Criteria")
        .expect("acceptance section should exist");
    let must_do_section = &prompt[must_do_start..non_goals_start];
    let non_goals_section = &prompt[non_goals_start..acceptance_start];

    assert!(must_do_section.contains("```md"));
    assert!(must_do_section.contains("Non-goals:"));
    assert!(must_do_section.contains("- this is code, not a real section"));
    assert!(must_do_section.contains("Scope:"));
    assert!(!non_goals_section.contains("this is code, not a real section"));
}

#[test]
fn render_bead_task_prompt_preserves_fenced_non_goal_blocks_without_mangling_delimiters() {
    let mut context = sample_bead_context();
    context.bead_description = Some(
        "Scope:\n- keep the contract explicit\n\nNon-goals:\nExample config:\n````md\n## Acceptance Criteria\n```\nstill inside the block\n````\n".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    let non_goals_start = prompt
        .find("## Explicit Non-Goals")
        .expect("non-goals section should exist");
    let acceptance_start = prompt[non_goals_start..]
        .rfind("\n\n## Acceptance Criteria\n\n")
        .map(|offset| non_goals_start + offset + 2)
        .expect("acceptance section should exist");
    let non_goals_section = &prompt[non_goals_start..acceptance_start];

    assert!(non_goals_section.contains("````md"));
    assert!(non_goals_section.contains("```"));
    assert!(non_goals_section.contains("still inside the block"));
    assert!(!non_goals_section.contains("- ````md"));
    assert!(
        ralph_burning::contexts::project_run_record::task_prompt_contract::validate_canonical_prompt_shape(&prompt)
            .is_ok()
    );
}

#[test]
fn render_bead_task_prompt_puts_fence_first_bullet_items_on_indented_lines() {
    let mut context = sample_bead_context();
    context.already_planned_elsewhere = vec!["```md\n## Review Policy\n```".to_owned()];

    let prompt = render_bead_task_prompt(&context);

    let planned_start = prompt
        .find("## Already Planned Elsewhere")
        .expect("planned elsewhere section should exist");
    let review_start = prompt[planned_start..]
        .rfind("\n\n## Review Policy\n\n")
        .map(|offset| planned_start + offset + 2)
        .expect("review policy section should exist");
    let planned_section = &prompt[planned_start..review_start];

    assert!(planned_section.contains("-\n    ```md\n    ## Review Policy\n    ```"));
    assert!(!planned_section.contains("- ```md"));
    assert!(
        ralph_burning::contexts::project_run_record::task_prompt_contract::validate_canonical_prompt_shape(&prompt)
            .is_ok()
    );
}

#[test]
fn rendered_bead_task_prompt_with_embedded_sections_still_satisfies_contract_shape() {
    let mut context = sample_bead_context();
    context.bead_description = Some(
        "Goal:\nKeep the task prompt contract explicit.\n\nNon-goals:\n- leave unrelated flows untouched\n\n## Acceptance Criteria\n\n- embedded duplicate heading\n".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    assert!(
        ralph_burning::contexts::project_run_record::task_prompt_contract::validate_canonical_prompt_shape(&prompt)
            .is_ok()
    );
}

#[test]
fn rendered_bead_task_prompt_indents_multiline_milestone_fields_without_breaking_contract() {
    let mut context = sample_bead_context();
    context.milestone_summary = Some(
        "Ship the prompt contract update.\n## Acceptance Criteria\nKeep this as milestone prose."
            .to_owned(),
    );
    context.already_planned_elsewhere = vec![
        "ms-alpha.bead-9 handles follow-up validation.\n## Review Policy\nTrack it separately."
            .to_owned(),
    ];

    let prompt = render_bead_task_prompt(&context);

    assert!(prompt.contains("- Summary: Ship the prompt contract update."));
    assert!(prompt.contains("    ## Acceptance Criteria"));
    assert!(prompt.contains("- ms-alpha.bead-9 handles follow-up validation."));
    assert!(prompt.contains("    ## Review Policy"));
    assert!(
        ralph_burning::contexts::project_run_record::task_prompt_contract::validate_canonical_prompt_shape(&prompt)
            .is_ok()
    );
}

#[test]
fn rendered_bead_task_prompt_indents_heading_like_lines_inside_must_do_scope() {
    let mut context = sample_bead_context();
    context.bead_description = Some(
        "Ship the prompt contract update.\n\n## Review Policy\nKeep this example heading inside must-do scope.".to_owned(),
    );

    let prompt = render_bead_task_prompt(&context);

    assert!(prompt.contains("    ## Review Policy"));
    assert!(
        !prompt.contains("\n\n## Review Policy\n\nKeep this example heading inside must-do scope.")
    );
    assert!(
        ralph_burning::contexts::project_run_record::task_prompt_contract::validate_canonical_prompt_shape(&prompt)
            .is_ok()
    );
}

#[test]
fn create_project_from_bead_context_bootstraps_task_metadata_and_prompt() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;

    let record = create_project_from_bead_context(
        &store,
        &journal_store,
        &dummy_base_dir(),
        CreateProjectFromBeadContextInput {
            project_id: None,
            prompt_override: None,
            created_at: test_timestamp(),
            context: sample_bead_context(),
        },
    )
    .expect("create project from bead context");

    let captured = store.captured();
    assert_eq!(record.id.as_str(), "task-ms-alpha-bead-2");
    assert_eq!(record.flow, FlowPreset::QuickDev);
    assert_eq!(
        captured.record.name,
        "Alpha Milestone: Bootstrap bead-backed task creation"
    );
    let task_source = captured
        .record
        .task_source
        .as_ref()
        .expect("task source should be recorded");
    assert_eq!(task_source.milestone_id, "ms-alpha");
    assert_eq!(task_source.bead_id, "ms-alpha.bead-2");
    assert_eq!(
        task_source.parent_epic_id.as_deref(),
        Some("ms-alpha.epic-1")
    );
    assert_eq!(task_source.plan_hash.as_deref(), Some("plan-hash-123"));
    assert_eq!(task_source.plan_version, Some(3));
    assert!(captured.prompt_contents.contains("## Current Bead Details"));
    assert!(captured
        .prompt_contents
        .contains("Bootstrap bead-backed task creation"));

    let event: JournalEvent =
        serde_json::from_str(&captured.initial_journal_line).expect("parse journal line");
    assert_eq!(event.details["source"], "milestone_bead");
    assert_eq!(event.details["milestone_id"], "ms-alpha");
    assert_eq!(event.details["bead_id"], "ms-alpha.bead-2");
    assert_eq!(event.details["plan_hash"], "plan-hash-123");
    assert_eq!(event.details["plan_version"], 3);
}

#[test]
fn create_project_from_bead_context_respects_explicit_project_id_and_prompt_override() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;

    let record = create_project_from_bead_context(
        &store,
        &journal_store,
        &dummy_base_dir(),
        CreateProjectFromBeadContextInput {
            project_id: Some(ProjectId::new("custom-task").unwrap()),
            prompt_override: Some("# Custom Prompt\nUse the explicit prompt.".to_owned()),
            created_at: test_timestamp(),
            context: sample_bead_context(),
        },
    )
    .expect("create project from bead context with override");

    let captured = store.captured();
    assert_eq!(record.id.as_str(), "custom-task");
    assert_eq!(
        captured.prompt_contents,
        "# Custom Prompt\nUse the explicit prompt."
    );
    assert_eq!(
        captured.record.prompt_hash,
        ralph_burning::adapters::fs::FileSystem::prompt_hash(
            "# Custom Prompt\nUse the explicit prompt."
        )
    );
}

#[test]
fn create_project_from_bead_context_accepts_multiline_canonical_fields() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;
    let mut context = sample_bead_context();
    context.milestone_summary = Some(
        "Ship the prompt contract update.\n## Acceptance Criteria\nKeep this prose indented."
            .to_owned(),
    );

    let record = create_project_from_bead_context(
        &store,
        &journal_store,
        &dummy_base_dir(),
        CreateProjectFromBeadContextInput {
            project_id: None,
            prompt_override: None,
            created_at: test_timestamp(),
            context,
        },
    )
    .expect("multiline canonical fields should still validate");

    let captured = store.captured();
    assert_eq!(record.id.as_str(), "task-ms-alpha-bead-2");
    assert!(captured
        .prompt_contents
        .contains("    ## Acceptance Criteria"));
}

#[test]
fn create_project_from_bead_context_accepts_fence_first_bullet_fields() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;
    let mut context = sample_bead_context();
    context.already_planned_elsewhere = vec!["```md\n## Review Policy\n```".to_owned()];

    let record = create_project_from_bead_context(
        &store,
        &journal_store,
        &dummy_base_dir(),
        CreateProjectFromBeadContextInput {
            project_id: None,
            prompt_override: None,
            created_at: test_timestamp(),
            context,
        },
    )
    .expect("fence-first bullet fields should still validate");

    let captured = store.captured();
    assert_eq!(record.id.as_str(), "task-ms-alpha-bead-2");
    assert!(captured
        .prompt_contents
        .contains("-\n    ```md\n    ## Review Policy\n    ```"));
}

#[test]
fn create_project_from_bead_context_preserves_description_only_acceptance_criteria() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;
    let mut context = sample_bead_context();
    context.bead_acceptance_criteria.clear();
    context.bead_description = Some(
        "Scope:\n- keep the task scoped to the active bead\n\n## Acceptance Criteria\n- preserve description-only acceptance criteria\n- do not emit the empty acceptance fallback\n".to_owned(),
    );

    let record = create_project_from_bead_context(
        &store,
        &journal_store,
        &dummy_base_dir(),
        CreateProjectFromBeadContextInput {
            project_id: None,
            prompt_override: None,
            created_at: test_timestamp(),
            context,
        },
    )
    .expect("description-only acceptance criteria should bootstrap");

    let captured = store.captured();
    assert_eq!(record.id.as_str(), "task-ms-alpha-bead-2");
    assert!(captured
        .prompt_contents
        .contains("- preserve description-only acceptance criteria"));
    assert!(!captured
        .prompt_contents
        .contains("No explicit acceptance criteria were supplied."));
}

#[test]
fn create_project_from_bead_context_accepts_colon_suffixed_markdown_sections_and_numbered_lists() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;
    let mut context = sample_bead_context();
    context.bead_acceptance_criteria.clear();
    context.bead_description = Some(
        "Scope:\n- keep the task scoped to the active bead\n\n## Non-goals:\n1. do not broaden the active bead\n2. do not absorb later milestone work\n\n## Acceptance Criteria:\n1. preserve numbered acceptance criteria\n2. avoid the empty acceptance fallback\n".to_owned(),
    );

    let record = create_project_from_bead_context(
        &store,
        &journal_store,
        &dummy_base_dir(),
        CreateProjectFromBeadContextInput {
            project_id: None,
            prompt_override: None,
            created_at: test_timestamp(),
            context,
        },
    )
    .expect("colon-suffixed sections should bootstrap");

    let captured = store.captured();
    assert_eq!(record.id.as_str(), "task-ms-alpha-bead-2");
    assert!(captured
        .prompt_contents
        .contains("- do not broaden the active bead"));
    assert!(captured
        .prompt_contents
        .contains("- do not absorb later milestone work"));
    assert!(captured
        .prompt_contents
        .contains("- preserve numbered acceptance criteria"));
    assert!(captured
        .prompt_contents
        .contains("- avoid the empty acceptance fallback"));
    assert!(!captured
        .prompt_contents
        .contains("No explicit acceptance criteria were supplied."));
}

#[test]
fn create_project_from_bead_context_preserves_nested_list_indentation_in_extracted_sections() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;
    let mut context = sample_bead_context();
    context.bead_acceptance_criteria.clear();
    context.bead_description = Some(
        "Scope:\n- keep the task scoped to the active bead\n\n## Non-goals:\n- do not broaden the active bead\n - keep follow-up docs in a sibling bead\n\n## Acceptance Criteria:\n- preserve the canonical contract shape\n - keep supporting rationale nested under the primary criterion\n".to_owned(),
    );

    let record = create_project_from_bead_context(
        &store,
        &journal_store,
        &dummy_base_dir(),
        CreateProjectFromBeadContextInput {
            project_id: None,
            prompt_override: None,
            created_at: test_timestamp(),
            context,
        },
    )
    .expect("nested section lists should preserve indentation");

    let captured = store.captured();
    assert_eq!(record.id.as_str(), "task-ms-alpha-bead-2");
    assert!(captured.prompt_contents.contains(
        "- do not broaden the active bead\n     - keep follow-up docs in a sibling bead"
    ));
    assert!(!captured
        .prompt_contents
        .contains("\n- keep follow-up docs in a sibling bead"));
    assert!(captured.prompt_contents.contains(
        "- preserve the canonical contract shape\n     - keep supporting rationale nested under the primary criterion"
    ));
    assert!(!captured
        .prompt_contents
        .contains("\n- keep supporting rationale nested under the primary criterion"));
}

#[test]
fn create_project_from_bead_context_accepts_heading_like_lines_in_must_do_scope() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;
    let mut context = sample_bead_context();
    context.bead_description = Some(
        "Ship the prompt contract update.\n\n## Milestone Summary\nKeep this as bead prose."
            .to_owned(),
    );

    let record = create_project_from_bead_context(
        &store,
        &journal_store,
        &dummy_base_dir(),
        CreateProjectFromBeadContextInput {
            project_id: None,
            prompt_override: None,
            created_at: test_timestamp(),
            context,
        },
    )
    .expect("heading-like must-do lines should still validate");

    let captured = store.captured();
    assert_eq!(record.id.as_str(), "task-ms-alpha-bead-2");
    assert!(captured
        .prompt_contents
        .contains("    ## Milestone Summary"));
}

#[test]
fn create_project_from_bead_context_rejects_invalid_canonical_prompt_override() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;

    let error = create_project_from_bead_context(
        &store,
        &journal_store,
        &dummy_base_dir(),
        CreateProjectFromBeadContextInput {
            project_id: Some(ProjectId::new("custom-task").unwrap()),
            prompt_override: Some(format!(
                "{}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nOnly one section.",
                ralph_burning::contexts::project_run_record::task_prompt_contract::contract_marker(
                )
            )),
            created_at: test_timestamp(),
            context: sample_bead_context(),
        },
    )
    .expect_err("invalid canonical override should fail");

    assert!(matches!(
        error,
        AppError::InvalidPrompt { ref path, ref reason }
            if path == "<prompt override>"
                && reason.contains("canonical bead task contract violated")
                && reason.contains("## Current Bead Details")
    ));
}

#[test]
fn create_project_from_bead_context_allows_generic_override_that_quotes_marker() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;
    let quoted_marker =
        ralph_burning::contexts::project_run_record::task_prompt_contract::contract_marker();

    let record = create_project_from_bead_context(
        &store,
        &journal_store,
        &dummy_base_dir(),
        CreateProjectFromBeadContextInput {
            project_id: Some(ProjectId::new("quoted-marker").unwrap()),
            prompt_override: Some(format!(
                "# Generic Prompt\n\n## AGENTS / Repo Guidance\n\n```md\n{}\n```",
                quoted_marker
            )),
            created_at: test_timestamp(),
            context: sample_bead_context(),
        },
    )
    .expect("quoted marker in fenced generic prompt should not trigger canonical validation");

    let captured = store.captured();
    assert_eq!(record.id.as_str(), "quoted-marker");
    assert!(captured.prompt_contents.contains("# Generic Prompt"));
}

#[test]
fn create_project_from_bead_context_rejects_misplaced_top_level_contract_marker_override() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;
    let marker =
        ralph_burning::contexts::project_run_record::task_prompt_contract::contract_marker();

    let error = create_project_from_bead_context(
        &store,
        &journal_store,
        &dummy_base_dir(),
        CreateProjectFromBeadContextInput {
            project_id: Some(ProjectId::new("misplaced-marker").unwrap()),
            prompt_override: Some(format!(
                "# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH\n\n{}",
                marker
            )),
            created_at: test_timestamp(),
            context: sample_bead_context(),
        },
    )
    .expect_err("misplaced canonical marker should fail");

    assert!(matches!(
        error,
        AppError::InvalidPrompt { ref path, ref reason }
            if path == "<prompt override>"
                && reason.contains("canonical bead task contract violated")
                && reason.contains("must appear before the canonical section block")
    ));
}

#[test]
fn create_project_from_bead_context_rejects_extra_canonical_heading_after_agents_guidance() {
    let store = RecordingProjectStore::empty();
    let journal_store = FakeJournalStore;
    let marker =
        ralph_burning::contexts::project_run_record::task_prompt_contract::contract_marker();

    let error = create_project_from_bead_context(
        &store,
        &journal_store,
        &dummy_base_dir(),
        CreateProjectFromBeadContextInput {
            project_id: Some(ProjectId::new("duplicate-after-agents").unwrap()),
            prompt_override: Some(format!(
                "{marker}\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH\n\n## Acceptance Criteria\n\nduplicate drift"
            )),
            created_at: test_timestamp(),
            context: sample_bead_context(),
        },
    )
    .expect_err("duplicate canonical heading after AGENTS guidance should fail");

    assert!(matches!(
        error,
        AppError::InvalidPrompt { ref path, ref reason }
            if path == "<prompt override>"
                && reason.contains("canonical bead task contract violated")
                && reason.contains("unexpected extra canonical heading `## Acceptance Criteria`")
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
        max_completion_rounds: Some(0),
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
        max_completion_rounds: Some(0),
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
        max_completion_rounds: Some(0),
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
        max_completion_rounds: Some(0),
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
        max_completion_rounds: Some(0),
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
        max_completion_rounds: Some(0),
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
        max_completion_rounds: Some(0),
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
            max_completion_rounds: Some(0),
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
        max_completion_rounds: Some(0),
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
        max_completion_rounds: Some(0),
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
        final_review_planner: None,
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
        final_review_planner: None,
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
            final_review_planner: None,
            final_review_arbiter: None,
        }),
    };

    let json = serde_json::to_string(&active).unwrap();
    let deserialized: ActiveRun = serde_json::from_str(&json).unwrap();
    assert_eq!(active, deserialized);
}

#[test]
fn stage_resolution_snapshot_defaults_missing_final_review_planner_for_backwards_compat() {
    let json = r#"{
        "stage_id": "final_review",
        "resolved_at": "2025-01-01T00:00:00Z",
        "final_review_reviewers": [
            {"backend_family": "claude", "model_id": "claude-opus"}
        ],
        "final_review_arbiter": {"backend_family": "codex", "model_id": "codex-1"}
    }"#;

    let snapshot: StageResolutionSnapshot = serde_json::from_str(json).unwrap();
    assert_eq!(
        snapshot.stage_id,
        ralph_burning::shared::domain::StageId::FinalReview
    );
    assert_eq!(snapshot.final_review_reviewers.len(), 1);
    assert!(snapshot.final_review_planner.is_none());
    assert!(snapshot.final_review_arbiter.is_some());
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
            adapter_reported_backend_family: None,
            adapter_reported_model_id: None,
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

// ── Amendment Service Unit Tests ──────────────────────────────────────────

use ralph_burning::contexts::project_run_record::model::{AmendmentSource, QueuedAmendment};
use ralph_burning::shared::domain::StageId;

// -- Fake AmendmentQueuePort for service tests --

struct FakeAmendmentQueue {
    amendments: RefCell<Vec<QueuedAmendment>>,
}

impl FakeAmendmentQueue {
    fn empty() -> Self {
        Self {
            amendments: RefCell::new(Vec::new()),
        }
    }

    fn with(amendments: Vec<QueuedAmendment>) -> Self {
        Self {
            amendments: RefCell::new(amendments),
        }
    }
}

impl AmendmentQueuePort for FakeAmendmentQueue {
    fn write_amendment(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        amendment: &QueuedAmendment,
    ) -> AppResult<()> {
        self.amendments.borrow_mut().push(amendment.clone());
        Ok(())
    }

    fn list_pending_amendments(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<Vec<QueuedAmendment>> {
        Ok(self.amendments.borrow().clone())
    }

    fn remove_amendment(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        amendment_id: &str,
    ) -> AppResult<()> {
        let mut amendments = self.amendments.borrow_mut();
        let pos = amendments
            .iter()
            .position(|a| a.amendment_id == amendment_id);
        match pos {
            Some(idx) => {
                amendments.remove(idx);
                Ok(())
            }
            None => Err(AppError::AmendmentNotFound {
                amendment_id: amendment_id.to_owned(),
            }),
        }
    }

    fn drain_amendments(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<u32> {
        let mut amendments = self.amendments.borrow_mut();
        let count = amendments.len() as u32;
        amendments.clear();
        Ok(count)
    }

    fn has_pending_amendments(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<bool> {
        Ok(!self.amendments.borrow().is_empty())
    }
}

struct FailingRemoveAmendmentQueue {
    amendments: RefCell<Vec<QueuedAmendment>>,
}

impl FailingRemoveAmendmentQueue {
    fn with(amendments: Vec<QueuedAmendment>) -> Self {
        Self {
            amendments: RefCell::new(amendments),
        }
    }
}

impl AmendmentQueuePort for FailingRemoveAmendmentQueue {
    fn write_amendment(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        amendment: &QueuedAmendment,
    ) -> AppResult<()> {
        self.amendments.borrow_mut().push(amendment.clone());
        Ok(())
    }

    fn list_pending_amendments(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<Vec<QueuedAmendment>> {
        Ok(self.amendments.borrow().clone())
    }

    fn remove_amendment(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        _amendment_id: &str,
    ) -> AppResult<()> {
        Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "simulated remove failure",
        )))
    }

    fn drain_amendments(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<u32> {
        Ok(0)
    }

    fn has_pending_amendments(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<bool> {
        Ok(!self.amendments.borrow().is_empty())
    }
}

// -- Fake RunSnapshotWritePort --

struct FakeRunSnapshotWriteStore {
    written: RefCell<Option<RunSnapshot>>,
}

impl FakeRunSnapshotWriteStore {
    fn new() -> Self {
        Self {
            written: RefCell::new(None),
        }
    }

    fn written_snapshot(&self) -> Option<RunSnapshot> {
        self.written.borrow().clone()
    }
}

impl RunSnapshotWritePort for FakeRunSnapshotWriteStore {
    fn write_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        snapshot: &RunSnapshot,
    ) -> AppResult<()> {
        self.written.replace(Some(snapshot.clone()));
        Ok(())
    }
}

// -- SharedRunSnapshotStore: read+write store for tests that call service
// functions multiple times and need writes to be visible on subsequent reads.

struct SharedRunSnapshotStore {
    snapshot: RefCell<RunSnapshot>,
}

impl SharedRunSnapshotStore {
    fn new(initial: RunSnapshot) -> Self {
        Self {
            snapshot: RefCell::new(initial),
        }
    }

    fn initial() -> Self {
        Self::new(RunSnapshot::initial(20))
    }
}

impl RunSnapshotPort for SharedRunSnapshotStore {
    fn read_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<RunSnapshot> {
        Ok(self.snapshot.borrow().clone())
    }
}

impl RunSnapshotWritePort for SharedRunSnapshotStore {
    fn write_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        snapshot: &RunSnapshot,
    ) -> AppResult<()> {
        self.snapshot.replace(snapshot.clone());
        Ok(())
    }
}

// -- Dedup key determinism tests --

#[test]
fn dedup_key_is_deterministic_for_same_input() {
    let key1 = QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "fix the bug");
    let key2 = QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "fix the bug");
    assert_eq!(key1, key2);
}

#[test]
fn dedup_key_normalizes_whitespace() {
    let key1 = QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "fix  the\n bug");
    let key2 = QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "fix the bug");
    assert_eq!(key1, key2);
}

#[test]
fn dedup_key_differs_by_source() {
    let manual = QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "fix the bug");
    let pr = QueuedAmendment::compute_dedup_key(&AmendmentSource::PrReview, "fix the bug");
    assert_ne!(manual, pr);
}

#[test]
fn dedup_key_differs_by_body() {
    let key1 = QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "fix bug A");
    let key2 = QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "fix bug B");
    assert_ne!(key1, key2);
}

#[test]
fn dedup_key_is_sha256_hex() {
    let key = QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "test");
    assert_eq!(key.len(), 64); // SHA-256 produces 64 hex chars
    assert!(key.chars().all(|c| c.is_ascii_hexdigit()));
}

// -- AmendmentSource serialization tests --

#[test]
fn amendment_source_serializes_to_snake_case() {
    assert_eq!(
        serde_json::to_string(&AmendmentSource::Manual).unwrap(),
        "\"manual\""
    );
    assert_eq!(
        serde_json::to_string(&AmendmentSource::PrReview).unwrap(),
        "\"pr_review\""
    );
    assert_eq!(
        serde_json::to_string(&AmendmentSource::IssueCommand).unwrap(),
        "\"issue_command\""
    );
    assert_eq!(
        serde_json::to_string(&AmendmentSource::WorkflowStage).unwrap(),
        "\"workflow_stage\""
    );
}

#[test]
fn amendment_source_round_trips() {
    for source in &[
        AmendmentSource::Manual,
        AmendmentSource::PrReview,
        AmendmentSource::IssueCommand,
        AmendmentSource::WorkflowStage,
    ] {
        let json = serde_json::to_string(source).unwrap();
        let deserialized: AmendmentSource = serde_json::from_str(&json).unwrap();
        assert_eq!(source, &deserialized);
    }
}

#[test]
fn amendment_source_display_matches_as_str() {
    assert_eq!(format!("{}", AmendmentSource::Manual), "manual");
    assert_eq!(format!("{}", AmendmentSource::PrReview), "pr_review");
    assert_eq!(
        format!("{}", AmendmentSource::IssueCommand),
        "issue_command"
    );
    assert_eq!(
        format!("{}", AmendmentSource::WorkflowStage),
        "workflow_stage"
    );
}

// -- QueuedAmendment backwards-compat deserialization --

#[test]
fn queued_amendment_defaults_source_to_workflow_stage_on_missing() {
    let json = r#"{
        "amendment_id": "legacy-1",
        "source_stage": "qa",
        "source_cycle": 1,
        "source_completion_round": 1,
        "body": "fix the thing",
        "created_at": "2026-03-18T00:00:00Z"
    }"#;
    let amendment: QueuedAmendment = serde_json::from_str(json).unwrap();
    assert_eq!(amendment.source, AmendmentSource::WorkflowStage);
    assert_eq!(amendment.dedup_key, ""); // default empty string
}

// -- add_manual_amendment service tests --

#[test]
fn add_manual_amendment_creates_and_returns_id() {
    let queue = FakeAmendmentQueue::empty();
    let run_store = FakeRunSnapshotStore::no_run();
    let run_write = FakeRunSnapshotWriteStore::new();
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::add_manual_amendment(
        &queue,
        &run_store,
        &run_write,
        &journal,
        &project_store,
        &base,
        &pid,
        "fix the bug",
    );

    assert!(result.is_ok());
    match result.unwrap() {
        service::AmendmentAddResult::Created { amendment_id } => {
            assert!(amendment_id.starts_with("manual-"));
        }
        service::AmendmentAddResult::Duplicate { .. } => {
            panic!("expected Created, got Duplicate");
        }
    }

    // The amendment should be in the queue.
    let pending = queue.amendments.borrow();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].body, "fix the bug");
    assert_eq!(pending[0].source, AmendmentSource::Manual);
    assert!(!pending[0].dedup_key.is_empty());
}

#[test]
fn add_manual_amendment_rejects_running_project() {
    let queue = FakeAmendmentQueue::empty();
    let run_store = FakeRunSnapshotStore::active_run();
    let run_write = FakeRunSnapshotWriteStore::new();
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::add_manual_amendment(
        &queue,
        &run_store,
        &run_write,
        &journal,
        &project_store,
        &base,
        &pid,
        "fix the bug",
    );

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        AppError::AmendmentLeaseConflict { .. }
    ));
}

#[test]
fn add_manual_amendment_deduplicates() {
    let queue = FakeAmendmentQueue::empty();
    let shared_store = SharedRunSnapshotStore::initial();
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    // First add
    let first = service::add_manual_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        &base,
        &pid,
        "fix the bug",
    )
    .unwrap();
    assert!(matches!(first, service::AmendmentAddResult::Created { .. }));

    // Second add with same body
    let second = service::add_manual_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        &base,
        &pid,
        "fix the bug",
    )
    .unwrap();
    assert!(matches!(
        second,
        service::AmendmentAddResult::Duplicate { .. }
    ));

    // Only one amendment should be on disk
    assert_eq!(queue.amendments.borrow().len(), 1);
}

#[test]
fn add_manual_amendment_dedup_normalizes_whitespace() {
    let queue = FakeAmendmentQueue::empty();
    let shared_store = SharedRunSnapshotStore::initial();
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    service::add_manual_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        &base,
        &pid,
        "fix  the\nbug",
    )
    .unwrap();

    let second = service::add_manual_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        &base,
        &pid,
        "fix the bug",
    )
    .unwrap();

    assert!(matches!(
        second,
        service::AmendmentAddResult::Duplicate { .. }
    ));
}

#[test]
fn add_manual_amendment_reopens_legacy_completed_snapshot_without_backfilling_max_rounds() {
    let tmp = tempdir().unwrap();
    let project_root = tmp.path().join(".ralph-burning/projects/alpha");
    std::fs::create_dir_all(&project_root).unwrap();
    std::fs::write(project_root.join("prompt.md"), "# Prompt\n").unwrap();

    let queue = FakeAmendmentQueue::empty();
    let shared_store =
        SharedRunSnapshotStore::new(completed_snapshot_with_max_completion_rounds(None));
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let pid = ProjectId::new("alpha").unwrap();

    service::add_manual_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        tmp.path(),
        &pid,
        "fix the legacy bug",
    )
    .expect("add amendment");

    let snapshot = shared_store
        .read_run_snapshot(tmp.path(), &pid)
        .expect("read updated snapshot");
    assert_eq!(snapshot.status, RunStatus::Paused);
    assert_eq!(
        snapshot.max_completion_rounds, None,
        "reopening a legacy completed snapshot should preserve unknown historical max_completion_rounds"
    );
    assert_eq!(snapshot.amendment_queue.pending.len(), 1);
    assert_eq!(snapshot.completion_rounds, 2);
    assert_eq!(snapshot.status_summary, "paused: amendments staged");
}

// -- list_amendments service tests --

#[test]
fn list_amendments_empty_returns_empty() {
    let run_store = FakeRunSnapshotStore::no_run();
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::list_amendments(&run_store, &base, &pid).unwrap();
    assert!(result.is_empty());
}

#[test]
fn list_amendments_returns_all_pending() {
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    // Build a snapshot with two pending amendments in the canonical queue.
    let source = AmendmentSource::Manual;
    let mut snapshot = RunSnapshot::initial(20);
    for (id, body) in &[("manual-1", "fix bug A"), ("manual-2", "fix bug B")] {
        let dedup_key = QueuedAmendment::compute_dedup_key(&source, body);
        snapshot.amendment_queue.pending.push(QueuedAmendment {
            amendment_id: id.to_string(),
            source_stage: ralph_burning::shared::domain::StageId::Planning,
            source_cycle: 1,
            source_completion_round: 1,
            body: body.to_string(),
            created_at: test_timestamp(),
            batch_sequence: 0,
            source: source.clone(),
            dedup_key,
        });
    }

    let run_store = FakeRunSnapshotStore::with_snapshot(snapshot);
    let result = service::list_amendments(&run_store, &base, &pid).unwrap();
    assert_eq!(result.len(), 2);
}

// -- remove_amendment service tests --

#[test]
fn remove_amendment_succeeds_for_existing() {
    let queue = FakeAmendmentQueue::empty();
    let shared_store = SharedRunSnapshotStore::initial();
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::add_manual_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        &base,
        &pid,
        "fix bug",
    )
    .unwrap();
    let amendment_id = match result {
        service::AmendmentAddResult::Created { amendment_id } => amendment_id,
        _ => panic!("expected Created"),
    };

    let remove_result = service::remove_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &base,
        &pid,
        &amendment_id,
    );
    assert!(remove_result.is_ok());
}

#[test]
fn remove_amendment_restores_completed_status_when_reopen_queue_empties() {
    let pre_reopen_completion_round = 3;
    let amendment = make_manual_amendment("manual-1", "fix bug");
    let queue = FakeAmendmentQueue::with(vec![amendment.clone()]);
    let shared_store = SharedRunSnapshotStore::new(reopened_completed_snapshot_with_round(
        pre_reopen_completion_round,
        vec![amendment],
    ));
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    service::remove_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &base,
        &pid,
        "manual-1",
    )
    .expect("remove amendment");

    let snapshot = shared_store
        .read_run_snapshot(&base, &pid)
        .expect("read updated snapshot");
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert!(snapshot.amendment_queue.pending.is_empty());
    assert!(snapshot.interrupted_run.is_none());
    assert_eq!(snapshot.completion_rounds, pre_reopen_completion_round);
    assert_eq!(snapshot.status_summary, "completed");
}

#[test]
fn remove_amendment_restores_completed_status_without_backfilling_legacy_max_rounds() {
    let amendment = make_manual_amendment("manual-1", "fix bug");
    let queue = FakeAmendmentQueue::with(vec![amendment.clone()]);
    let shared_store = SharedRunSnapshotStore::new(reopened_legacy_completed_snapshot_with_round(
        3,
        vec![amendment],
    ));
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    service::remove_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &base,
        &pid,
        "manual-1",
    )
    .expect("remove amendment");

    let snapshot = shared_store
        .read_run_snapshot(&base, &pid)
        .expect("read updated snapshot");
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert!(
        snapshot.max_completion_rounds.is_none(),
        "restoring a reopened legacy snapshot should keep max_completion_rounds unknown"
    );
    assert!(snapshot.amendment_queue.pending.is_empty());
    assert_eq!(snapshot.completion_rounds, 3);
}

#[test]
fn remove_amendment_keeps_reopened_project_paused_when_other_amendments_remain() {
    let amendments = vec![
        make_manual_amendment("manual-1", "fix A"),
        make_manual_amendment("manual-2", "fix B"),
    ];
    let queue = FakeAmendmentQueue::with(amendments.clone());
    let shared_store = SharedRunSnapshotStore::new(reopened_completed_snapshot(amendments));
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    service::remove_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &base,
        &pid,
        "manual-1",
    )
    .expect("remove amendment");

    let snapshot = shared_store
        .read_run_snapshot(&base, &pid)
        .expect("read updated snapshot");
    assert_eq!(snapshot.status, RunStatus::Paused);
    assert_eq!(snapshot.amendment_queue.pending.len(), 1);
    assert_eq!(
        snapshot
            .interrupted_run
            .as_ref()
            .expect("reopen marker retained")
            .run_id,
        "reopen-alpha"
    );
    assert_eq!(snapshot.status_summary, "paused: amendments staged");
}

#[test]
fn remove_amendment_keeps_normally_paused_project_paused_when_queue_empties() {
    let amendment = make_manual_amendment("manual-1", "fix bug");
    let queue = FakeAmendmentQueue::with(vec![amendment.clone()]);
    let shared_store = SharedRunSnapshotStore::new(paused_snapshot(vec![amendment]));
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    service::remove_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &base,
        &pid,
        "manual-1",
    )
    .expect("remove amendment");

    let snapshot = shared_store
        .read_run_snapshot(&base, &pid)
        .expect("read updated snapshot");
    assert_eq!(snapshot.status, RunStatus::Paused);
    assert!(snapshot.amendment_queue.pending.is_empty());
    assert!(snapshot.interrupted_run.is_none());
    assert_eq!(snapshot.status_summary, "paused");
}

#[test]
fn remove_amendment_fails_for_missing() {
    let queue = FakeAmendmentQueue::empty();
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let run_store = FakeRunSnapshotStore::no_run();
    let run_write = FakeRunSnapshotWriteStore::new();
    let result =
        service::remove_amendment(&queue, &run_store, &run_write, &base, &pid, "nonexistent");
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        AppError::AmendmentNotFound { .. }
    ));
}

// -- clear_amendments service tests --

#[test]
fn clear_amendments_empty_returns_empty() {
    let queue = FakeAmendmentQueue::empty();
    let run_store = FakeRunSnapshotStore::no_run();
    let run_write = FakeRunSnapshotWriteStore::new();
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::clear_amendments(&queue, &run_store, &run_write, &base, &pid).unwrap();
    assert!(result.is_empty());
}

#[test]
fn clear_amendments_removes_all() {
    let queue = FakeAmendmentQueue::empty();
    let shared_store = SharedRunSnapshotStore::initial();
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    service::add_manual_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        &base,
        &pid,
        "fix A",
    )
    .unwrap();
    service::add_manual_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        &base,
        &pid,
        "fix B",
    )
    .unwrap();

    let removed =
        service::clear_amendments(&queue, &shared_store, &shared_store, &base, &pid).unwrap();
    assert_eq!(removed.len(), 2);
}

#[test]
fn clear_amendments_restores_completed_status_when_reopen_queue_empties() {
    let pre_reopen_completion_round = 4;
    let amendments = vec![
        make_manual_amendment("manual-1", "fix A"),
        make_manual_amendment("manual-2", "fix B"),
    ];
    let queue = FakeAmendmentQueue::with(amendments.clone());
    let shared_store = SharedRunSnapshotStore::new(reopened_completed_snapshot_with_round(
        pre_reopen_completion_round,
        amendments,
    ));
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let removed =
        service::clear_amendments(&queue, &shared_store, &shared_store, &base, &pid).unwrap();

    assert_eq!(removed.len(), 2);
    let snapshot = shared_store
        .read_run_snapshot(&base, &pid)
        .expect("read updated snapshot");
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert!(snapshot.amendment_queue.pending.is_empty());
    assert!(snapshot.interrupted_run.is_none());
    assert_eq!(snapshot.completion_rounds, pre_reopen_completion_round);
    assert_eq!(snapshot.status_summary, "completed");
}

#[test]
fn clear_amendments_restores_completed_status_for_reopened_empty_queue_fast_path() {
    let pre_reopen_completion_round = 2;
    let queue = FakeAmendmentQueue::empty();
    let shared_store = SharedRunSnapshotStore::new(reopened_completed_snapshot_with_round(
        pre_reopen_completion_round,
        Vec::new(),
    ));
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let removed =
        service::clear_amendments(&queue, &shared_store, &shared_store, &base, &pid).unwrap();

    assert!(removed.is_empty());
    let snapshot = shared_store
        .read_run_snapshot(&base, &pid)
        .expect("read updated snapshot");
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert!(snapshot.amendment_queue.pending.is_empty());
    assert!(snapshot.interrupted_run.is_none());
    assert_eq!(snapshot.completion_rounds, pre_reopen_completion_round);
    assert_eq!(snapshot.status_summary, "completed");
}

#[test]
fn clear_amendments_keeps_normally_paused_project_paused_when_queue_empties() {
    let amendments = vec![
        make_manual_amendment("manual-1", "fix A"),
        make_manual_amendment("manual-2", "fix B"),
    ];
    let queue = FakeAmendmentQueue::with(amendments.clone());
    let shared_store = SharedRunSnapshotStore::new(paused_snapshot(amendments));
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let removed =
        service::clear_amendments(&queue, &shared_store, &shared_store, &base, &pid).unwrap();

    assert_eq!(removed.len(), 2);
    let snapshot = shared_store
        .read_run_snapshot(&base, &pid)
        .expect("read updated snapshot");
    assert_eq!(snapshot.status, RunStatus::Paused);
    assert!(snapshot.amendment_queue.pending.is_empty());
    assert!(snapshot.interrupted_run.is_none());
    assert_eq!(snapshot.status_summary, "paused");
}

#[test]
fn clear_amendments_partial_failure_reports_remaining() {
    let amendments = vec![
        QueuedAmendment {
            amendment_id: "amend-1".to_owned(),
            source_stage: StageId::Planning,
            source_cycle: 1,
            source_completion_round: 1,
            body: "fix A".to_owned(),
            created_at: test_timestamp(),
            batch_sequence: 0,
            source: AmendmentSource::Manual,
            dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "fix A"),
        },
        QueuedAmendment {
            amendment_id: "amend-2".to_owned(),
            source_stage: StageId::Planning,
            source_cycle: 1,
            source_completion_round: 1,
            body: "fix B".to_owned(),
            created_at: test_timestamp(),
            batch_sequence: 0,
            source: AmendmentSource::Manual,
            dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "fix B"),
        },
    ];

    // Populate snapshot with the amendments so clear reads them from canonical state.
    let mut snapshot = RunSnapshot::initial(20);
    snapshot.amendment_queue.pending = amendments.clone();
    let shared_store = SharedRunSnapshotStore::new(snapshot);

    let queue = FailingRemoveAmendmentQueue::with(amendments);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::clear_amendments(&queue, &shared_store, &shared_store, &base, &pid);
    assert!(result.is_err());
    match result.unwrap_err() {
        AppError::AmendmentClearPartial {
            removed_count,
            total,
            remaining,
            ..
        } => {
            assert_eq!(removed_count, 0);
            assert_eq!(total, 2);
            assert_eq!(remaining.len(), 2);
        }
        other => panic!("expected AmendmentClearPartial, got: {other}"),
    }
}

// -- FailingRunSnapshotWriteStore for snapshot-write failure tests --

struct FailingRunSnapshotWriteStore;

impl RunSnapshotWritePort for FailingRunSnapshotWriteStore {
    fn write_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        _snapshot: &RunSnapshot,
    ) -> AppResult<()> {
        Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "simulated snapshot write failure",
        )))
    }
}

struct FailNTimesRunSnapshotStore {
    snapshot: RefCell<RunSnapshot>,
    remaining_failures: RefCell<u32>,
}

impl FailNTimesRunSnapshotStore {
    fn new(snapshot: RunSnapshot, failures: u32) -> Self {
        Self {
            snapshot: RefCell::new(snapshot),
            remaining_failures: RefCell::new(failures),
        }
    }
}

impl RunSnapshotPort for FailNTimesRunSnapshotStore {
    fn read_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<RunSnapshot> {
        Ok(self.snapshot.borrow().clone())
    }
}

impl RunSnapshotWritePort for FailNTimesRunSnapshotStore {
    fn write_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        snapshot: &RunSnapshot,
    ) -> AppResult<()> {
        let mut remaining_failures = self.remaining_failures.borrow_mut();
        if *remaining_failures > 0 {
            *remaining_failures -= 1;
            return Err(AppError::Io(std::io::Error::other(
                "simulated snapshot write failure",
            )));
        }

        self.snapshot.replace(snapshot.clone());
        Ok(())
    }
}

#[test]
fn add_manual_amendment_rolls_back_file_on_snapshot_write_failure() {
    let queue = FakeAmendmentQueue::empty();
    let run_store = FakeRunSnapshotStore::no_run();
    let run_write = FailingRunSnapshotWriteStore;
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::add_manual_amendment(
        &queue,
        &run_store,
        &run_write,
        &journal,
        &project_store,
        &base,
        &pid,
        "fix the bug",
    );

    // The add must fail.
    assert!(result.is_err());

    // The amendment file must be rolled back — the queue should be empty.
    assert!(
        queue.amendments.borrow().is_empty(),
        "amendment file should be rolled back on snapshot write failure"
    );
}

#[test]
fn add_manual_amendment_retry_reuses_preserved_file_after_completed_reopen_failure() {
    let tmp = tempdir().unwrap();
    let project_root = tmp.path().join(".ralph-burning/projects/alpha");
    std::fs::create_dir_all(&project_root).unwrap();
    std::fs::write(project_root.join("prompt.md"), "# Prompt\n").unwrap();

    let completed_snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Completed,
        cycle_history: vec![CycleHistoryEntry {
            cycle: 1,
            stage_id: StageId::FinalReview,
            started_at: test_timestamp(),
            completed_at: Some(test_timestamp()),
        }],
        completion_rounds: 1,
        max_completion_rounds: Some(0),
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "completed".to_owned(),
        last_stage_resolution_snapshot: None,
    };

    let queue = ralph_burning::adapters::fs::FsAmendmentQueueStore;
    let run_store = FailNTimesRunSnapshotStore::new(completed_snapshot, 1);
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let pid = ProjectId::new("alpha").unwrap();

    let first = service::add_manual_amendment(
        &queue,
        &run_store,
        &run_store,
        &journal,
        &project_store,
        tmp.path(),
        &pid,
        "fix the bug",
    );
    assert!(
        first.is_err(),
        "first completed-project add should fail reopen"
    );

    let preserved = queue.list_pending_amendments(tmp.path(), &pid).unwrap();
    assert_eq!(preserved.len(), 1, "failed reopen should preserve one file");
    let preserved_id = preserved[0].amendment_id.clone();
    let dedup_key = preserved[0].dedup_key.clone();
    let orphan_ids = ["manual-prepatch-orphan-a", "manual-prepatch-orphan-b"];
    for (index, orphan_id) in orphan_ids.iter().enumerate() {
        queue
            .write_amendment(
                tmp.path(),
                &pid,
                &QueuedAmendment {
                    amendment_id: (*orphan_id).to_owned(),
                    source_stage: StageId::Planning,
                    source_cycle: 1,
                    source_completion_round: 1,
                    body: "fix the bug".to_owned(),
                    created_at: preserved[0].created_at
                        + chrono::Duration::seconds((index + 1) as i64),
                    batch_sequence: 0,
                    source: AmendmentSource::Manual,
                    dedup_key: dedup_key.clone(),
                },
            )
            .unwrap();
    }
    assert_eq!(
        queue
            .list_pending_amendments(tmp.path(), &pid)
            .unwrap()
            .len(),
        3,
        "historical failed retries should leave multiple matching files on disk"
    );

    let second = service::add_manual_amendment(
        &queue,
        &run_store,
        &run_store,
        &journal,
        &project_store,
        tmp.path(),
        &pid,
        "fix the bug",
    )
    .unwrap();
    let created_id = match second {
        service::AmendmentAddResult::Created { amendment_id } => amendment_id,
        service::AmendmentAddResult::Duplicate { .. } => panic!("expected Created on retry"),
    };
    assert_eq!(
        created_id, preserved_id,
        "retry should reuse the preserved amendment file instead of creating a new one"
    );

    let pending = queue.list_pending_amendments(tmp.path(), &pid).unwrap();
    assert_eq!(pending.len(), 1, "retry should not leave an orphaned file");
    assert_eq!(pending[0].amendment_id, preserved_id);
    assert!(
        orphan_ids
            .iter()
            .all(|orphan_id| pending.iter().all(|a| a.amendment_id != *orphan_id)),
        "retry should collapse all historical duplicate files before reopening"
    );

    let reopened_snapshot = run_store.read_run_snapshot(tmp.path(), &pid).unwrap();
    assert_eq!(reopened_snapshot.status, RunStatus::Paused);
    assert_eq!(reopened_snapshot.amendment_queue.pending.len(), 1);
    assert_eq!(
        reopened_snapshot.amendment_queue.pending[0].amendment_id,
        preserved_id
    );

    service::remove_amendment(
        &queue,
        &run_store,
        &run_store,
        tmp.path(),
        &pid,
        &preserved_id,
    )
    .unwrap();
    assert!(
        queue
            .list_pending_amendments(tmp.path(), &pid)
            .unwrap()
            .is_empty(),
        "removing the reused amendment should not leave an orphan behind"
    );

    let restored_snapshot = run_store.read_run_snapshot(tmp.path(), &pid).unwrap();
    assert_eq!(restored_snapshot.status, RunStatus::Completed);
    assert!(restored_snapshot.amendment_queue.pending.is_empty());
}

#[test]
fn remove_amendment_preserves_amendment_on_snapshot_write_failure() {
    // Pre-populate the queue with one amendment.
    let queue = FakeAmendmentQueue::empty();
    let run_store_add = FakeRunSnapshotStore::no_run();
    let run_write_add = FakeRunSnapshotWriteStore::new();
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::add_manual_amendment(
        &queue,
        &run_store_add,
        &run_write_add,
        &journal,
        &project_store,
        &base,
        &pid,
        "keep me",
    )
    .unwrap();
    let amendment_id = match result {
        service::AmendmentAddResult::Created { amendment_id } => amendment_id,
        _ => panic!("expected Created"),
    };

    // Build a snapshot with the amendment in it (as canonical state).
    let source = AmendmentSource::Manual;
    let mut snapshot = RunSnapshot::initial(20);
    let dedup_key = QueuedAmendment::compute_dedup_key(&source, "keep me");
    snapshot.amendment_queue.pending.push(QueuedAmendment {
        amendment_id: amendment_id.clone(),
        source_stage: ralph_burning::shared::domain::StageId::Planning,
        source_cycle: 1,
        source_completion_round: 1,
        body: "keep me".to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 0,
        source,
        dedup_key,
    });
    let run_store_rm = FakeRunSnapshotStore::with_snapshot(snapshot);
    let run_write_rm = FailingRunSnapshotWriteStore;

    let remove_result = service::remove_amendment(
        &queue,
        &run_store_rm,
        &run_write_rm,
        &base,
        &pid,
        &amendment_id,
    );

    // Remove must fail.
    assert!(remove_result.is_err());

    // The amendment must still be in the queue — snapshot write failed so
    // no mutation should be visible.
    assert_eq!(
        queue.amendments.borrow().len(),
        1,
        "amendment must not be removed when snapshot write fails"
    );
}

#[test]
fn remove_amendment_fails_when_file_deletion_fails() {
    // Build a snapshot with one amendment.
    let source = AmendmentSource::Manual;
    let mut snapshot = RunSnapshot::initial(20);
    let dedup_key = QueuedAmendment::compute_dedup_key(&source, "keep me");
    let amendment = QueuedAmendment {
        amendment_id: "manual-test-123".to_owned(),
        source_stage: ralph_burning::shared::domain::StageId::Planning,
        source_cycle: 1,
        source_completion_round: 1,
        body: "keep me".to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 0,
        source,
        dedup_key,
    };
    snapshot.amendment_queue.pending.push(amendment.clone());
    let run_store = FakeRunSnapshotStore::with_snapshot(snapshot);
    let run_write = FakeRunSnapshotWriteStore::new();

    // Use FailingRemoveAmendmentQueue so file deletion fails.
    let queue = FailingRemoveAmendmentQueue::with(vec![amendment]);

    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::remove_amendment(
        &queue,
        &run_store,
        &run_write,
        &base,
        &pid,
        "manual-test-123",
    );

    // Remove must fail because the file couldn't be deleted.
    assert!(result.is_err());

    // Snapshot should NOT have been updated — no mutation visible.
    assert!(
        run_write.written_snapshot().is_none(),
        "snapshot must not be updated when file deletion fails"
    );
}

#[test]
fn clear_amendments_preserves_all_on_snapshot_write_failure() {
    let amendments = vec![
        QueuedAmendment {
            amendment_id: "amend-1".to_owned(),
            source_stage: StageId::Planning,
            source_cycle: 1,
            source_completion_round: 1,
            body: "fix A".to_owned(),
            created_at: test_timestamp(),
            batch_sequence: 0,
            source: AmendmentSource::Manual,
            dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "fix A"),
        },
        QueuedAmendment {
            amendment_id: "amend-2".to_owned(),
            source_stage: StageId::Planning,
            source_cycle: 1,
            source_completion_round: 1,
            body: "fix B".to_owned(),
            created_at: test_timestamp(),
            batch_sequence: 0,
            source: AmendmentSource::Manual,
            dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "fix B"),
        },
    ];

    // Build a snapshot with the amendments in canonical state.
    let mut snapshot = RunSnapshot::initial(20);
    snapshot.amendment_queue.pending = amendments.clone();
    let run_store = FakeRunSnapshotStore::with_snapshot(snapshot);

    let queue = FakeAmendmentQueue::with(amendments);
    let run_write = FailingRunSnapshotWriteStore;
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::clear_amendments(&queue, &run_store, &run_write, &base, &pid);

    // Clear must fail because snapshot write fails.
    assert!(result.is_err());

    // Files are deleted first, but since snapshot write fails the service
    // restores them. Both amendments must be back in the queue.
    assert_eq!(
        queue.amendments.borrow().len(),
        2,
        "amendment files must be restored when snapshot write fails"
    );
}

// -- FailingJournalStore: read_journal always fails --

struct FailingJournalStore;

impl JournalStorePort for FailingJournalStore {
    fn read_journal(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<Vec<JournalEvent>> {
        Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "simulated journal read failure",
        )))
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

#[test]
fn add_manual_amendment_fails_cleanly_on_journal_read_failure() {
    // With the journal preparation happening before mutations, a journal
    // read failure should prevent any mutation from occurring.
    let queue = FakeAmendmentQueue::empty();
    let run_store = FakeRunSnapshotStore::no_run();
    let run_write = FakeRunSnapshotWriteStore::new();
    let journal = FailingJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::add_manual_amendment(
        &queue,
        &run_store,
        &run_write,
        &journal,
        &project_store,
        &base,
        &pid,
        "should not persist",
    );

    // Must fail (journal read fails before any mutation).
    assert!(result.is_err());

    // No amendment file should have been written.
    assert!(
        queue.amendments.borrow().is_empty(),
        "no amendment file should be written when journal read fails"
    );

    // No snapshot should have been written.
    assert!(
        run_write.written_snapshot().is_none(),
        "no snapshot should be written when journal read fails"
    );
}

// -- FailAfterNWritesAmendmentQueue: write_amendment fails after N successes --

struct FailAfterNWritesAmendmentQueue {
    amendments: RefCell<Vec<QueuedAmendment>>,
    writes_before_failure: usize,
    write_count: RefCell<usize>,
}

impl FailAfterNWritesAmendmentQueue {
    fn new(writes_before_failure: usize) -> Self {
        Self {
            amendments: RefCell::new(Vec::new()),
            writes_before_failure,
            write_count: RefCell::new(0),
        }
    }
}

impl AmendmentQueuePort for FailAfterNWritesAmendmentQueue {
    fn write_amendment(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        amendment: &QueuedAmendment,
    ) -> AppResult<()> {
        let mut count = self.write_count.borrow_mut();
        if *count >= self.writes_before_failure {
            return Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "simulated write failure",
            )));
        }
        *count += 1;
        self.amendments.borrow_mut().push(amendment.clone());
        Ok(())
    }

    fn list_pending_amendments(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<Vec<QueuedAmendment>> {
        Ok(self.amendments.borrow().clone())
    }

    fn remove_amendment(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        amendment_id: &str,
    ) -> AppResult<()> {
        let mut amendments = self.amendments.borrow_mut();
        let pos = amendments
            .iter()
            .position(|a| a.amendment_id == amendment_id);
        match pos {
            Some(idx) => {
                amendments.remove(idx);
                Ok(())
            }
            None => Err(AppError::AmendmentNotFound {
                amendment_id: amendment_id.to_owned(),
            }),
        }
    }

    fn drain_amendments(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<u32> {
        let mut amendments = self.amendments.borrow_mut();
        let count = amendments.len() as u32;
        amendments.clear();
        Ok(count)
    }

    fn has_pending_amendments(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<bool> {
        Ok(!self.amendments.borrow().is_empty())
    }
}

#[test]
fn stage_amendment_batch_rolls_back_earlier_files_on_mid_batch_write_failure() {
    // The second write will fail, so the first file must be rolled back.
    let queue = FailAfterNWritesAmendmentQueue::new(1);
    let shared_store = SharedRunSnapshotStore::initial();
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let amendments = vec![
        QueuedAmendment {
            amendment_id: "batch-1".to_owned(),
            source_stage: StageId::Planning,
            source_cycle: 1,
            source_completion_round: 1,
            body: "first".to_owned(),
            created_at: test_timestamp(),
            batch_sequence: 0,
            source: AmendmentSource::PrReview,
            dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::PrReview, "first"),
        },
        QueuedAmendment {
            amendment_id: "batch-2".to_owned(),
            source_stage: StageId::Planning,
            source_cycle: 1,
            source_completion_round: 1,
            body: "second".to_owned(),
            created_at: test_timestamp(),
            batch_sequence: 1,
            source: AmendmentSource::PrReview,
            dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::PrReview, "second"),
        },
    ];

    let result = service::stage_amendment_batch(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        &base,
        &pid,
        &amendments,
    );

    // Must fail because the second write fails.
    assert!(result.is_err());

    // The first file must be rolled back — no amendment files should remain.
    assert!(
        queue.amendments.borrow().is_empty(),
        "earlier files must be rolled back when a later write fails in the batch"
    );

    // Canonical snapshot must not have been updated.
    let snap = shared_store.read_run_snapshot(&base, &pid).unwrap();
    assert!(
        snap.amendment_queue.pending.is_empty(),
        "snapshot must not be updated when batch staging fails"
    );
}

// -- FailingRepairWriteStore: first write succeeds, second (repair) fails --

struct FailingRepairWriteStore {
    snapshot: RefCell<RunSnapshot>,
}

impl FailingRepairWriteStore {
    fn new(initial: RunSnapshot) -> Self {
        Self {
            snapshot: RefCell::new(initial),
        }
    }
}

impl RunSnapshotPort for FailingRepairWriteStore {
    fn read_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<RunSnapshot> {
        Ok(self.snapshot.borrow().clone())
    }
}

impl RunSnapshotWritePort for FailingRepairWriteStore {
    fn write_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        _snapshot: &RunSnapshot,
    ) -> AppResult<()> {
        // All writes fail — simulates the repair write failing during
        // partial clear.
        Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "simulated repair write failure",
        )))
    }
}

#[test]
fn clear_partial_failure_restores_files_when_repair_write_fails() {
    // Build a scenario where one remove succeeds and one fails, then the
    // repair snapshot write also fails.
    let source = AmendmentSource::Manual;
    let amendment_a = QueuedAmendment {
        amendment_id: "amend-a".to_owned(),
        source_stage: StageId::Planning,
        source_cycle: 1,
        source_completion_round: 1,
        body: "fix A".to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 0,
        source: source.clone(),
        dedup_key: QueuedAmendment::compute_dedup_key(&source, "fix A"),
    };
    let amendment_b = QueuedAmendment {
        amendment_id: "amend-b".to_owned(),
        source_stage: StageId::Planning,
        source_cycle: 1,
        source_completion_round: 1,
        body: "fix B".to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 1,
        source: source.clone(),
        dedup_key: QueuedAmendment::compute_dedup_key(&source, "fix B"),
    };

    // Use FailAfterNRemovesAmendmentQueue: first remove succeeds, second fails.
    let queue =
        FailAfterNRemovesAmendmentQueue::new(vec![amendment_a.clone(), amendment_b.clone()], 1);

    let mut snapshot = RunSnapshot::initial(20);
    snapshot.amendment_queue.pending = vec![amendment_a.clone(), amendment_b.clone()];

    // Use FailingRepairWriteStore so the repair snapshot write also fails.
    let store = FailingRepairWriteStore::new(snapshot);

    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::clear_amendments(&queue, &store, &store, &base, &pid);

    // Must fail with an I/O error (not AmendmentClearPartial), because the
    // repair write failed.
    assert!(result.is_err());
    match result.unwrap_err() {
        AppError::AmendmentClearPartial { .. } => {
            panic!("should return I/O error, not AmendmentClearPartial, when repair write fails");
        }
        AppError::Io(_) => {} // expected
        other => panic!("unexpected error type: {:?}", other),
    }

    // The deleted file must be restored — both amendments should be on disk.
    assert_eq!(
        queue.amendments.borrow().len(),
        2,
        "deleted files must be restored when repair write fails"
    );
}

// -- FailAfterNRemovesAmendmentQueue: remove fails after N successes --

struct FailAfterNRemovesAmendmentQueue {
    amendments: RefCell<Vec<QueuedAmendment>>,
    removes_before_failure: usize,
    remove_count: RefCell<usize>,
}

impl FailAfterNRemovesAmendmentQueue {
    fn new(amendments: Vec<QueuedAmendment>, removes_before_failure: usize) -> Self {
        Self {
            amendments: RefCell::new(amendments),
            removes_before_failure,
            remove_count: RefCell::new(0),
        }
    }
}

impl AmendmentQueuePort for FailAfterNRemovesAmendmentQueue {
    fn write_amendment(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        amendment: &QueuedAmendment,
    ) -> AppResult<()> {
        self.amendments.borrow_mut().push(amendment.clone());
        Ok(())
    }

    fn list_pending_amendments(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<Vec<QueuedAmendment>> {
        Ok(self.amendments.borrow().clone())
    }

    fn remove_amendment(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        amendment_id: &str,
    ) -> AppResult<()> {
        let mut count = self.remove_count.borrow_mut();
        if *count >= self.removes_before_failure {
            return Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "simulated remove failure",
            )));
        }
        *count += 1;
        let mut amendments = self.amendments.borrow_mut();
        let pos = amendments
            .iter()
            .position(|a| a.amendment_id == amendment_id);
        match pos {
            Some(idx) => {
                amendments.remove(idx);
                Ok(())
            }
            None => Err(AppError::AmendmentNotFound {
                amendment_id: amendment_id.to_owned(),
            }),
        }
    }

    fn drain_amendments(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<u32> {
        let mut amendments = self.amendments.borrow_mut();
        let count = amendments.len() as u32;
        amendments.clear();
        Ok(count)
    }

    fn has_pending_amendments(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<bool> {
        Ok(!self.amendments.borrow().is_empty())
    }
}

// -- FailingAppendJournalStore: read_journal succeeds but append_event always fails --

struct FailingAppendJournalStore;

impl JournalStorePort for FailingAppendJournalStore {
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
        Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "simulated journal append failure",
        )))
    }
}

#[test]
fn add_manual_amendment_fails_when_journal_append_fails() {
    // Journal read/serialize succeeds, but append_event fails. The amendment
    // must be rolled back so no amendment is visible without its history event.
    let queue = FakeAmendmentQueue::empty();
    let shared_store = SharedRunSnapshotStore::initial();
    let journal = FailingAppendJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::add_manual_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        &base,
        &pid,
        "should not persist",
    );

    // Must fail because the journal append failed.
    assert!(result.is_err());

    // No amendment file should remain — rolled back.
    assert!(
        queue.amendments.borrow().is_empty(),
        "amendment file must be rolled back when journal append fails"
    );

    // Snapshot must be restored to pre-mutation state (no pending amendments).
    let snap = shared_store.read_run_snapshot(&base, &pid).unwrap();
    assert!(
        snap.amendment_queue.pending.is_empty(),
        "snapshot must be restored when journal append fails"
    );
}

#[test]
fn stage_amendment_batch_fails_when_journal_append_fails() {
    // Batch staging where journal append fails after snapshot commit.
    // All amendments and the snapshot must be rolled back.
    let queue = FakeAmendmentQueue::empty();
    let shared_store = SharedRunSnapshotStore::initial();
    let journal = FailingAppendJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let amendments = vec![
        QueuedAmendment {
            amendment_id: "batch-1".to_owned(),
            source_stage: StageId::Planning,
            source_cycle: 1,
            source_completion_round: 1,
            body: "first".to_owned(),
            created_at: test_timestamp(),
            batch_sequence: 0,
            source: AmendmentSource::PrReview,
            dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::PrReview, "first"),
        },
        QueuedAmendment {
            amendment_id: "batch-2".to_owned(),
            source_stage: StageId::Planning,
            source_cycle: 1,
            source_completion_round: 1,
            body: "second".to_owned(),
            created_at: test_timestamp(),
            batch_sequence: 1,
            source: AmendmentSource::PrReview,
            dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::PrReview, "second"),
        },
    ];

    let result = service::stage_amendment_batch(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        &base,
        &pid,
        &amendments,
    );

    // Must fail because the journal append failed.
    assert!(result.is_err());

    // All amendment files must be rolled back.
    assert!(
        queue.amendments.borrow().is_empty(),
        "all amendment files must be rolled back when journal append fails"
    );

    // Snapshot must be restored to pre-mutation state.
    let snap = shared_store.read_run_snapshot(&base, &pid).unwrap();
    assert!(
        snap.amendment_queue.pending.is_empty(),
        "snapshot must be restored when journal append fails during batch staging"
    );
}

// -- FailAfterNAppendsJournalStore: first N appends succeed, then fail --

struct FailAfterNAppendsJournalStore {
    appends_before_failure: usize,
    append_count: RefCell<usize>,
}

impl FailAfterNAppendsJournalStore {
    fn new(appends_before_failure: usize) -> Self {
        Self {
            appends_before_failure,
            append_count: RefCell::new(0),
        }
    }
}

impl JournalStorePort for FailAfterNAppendsJournalStore {
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
        let mut count = self.append_count.borrow_mut();
        if *count >= self.appends_before_failure {
            return Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "simulated journal append failure after N successes",
            )));
        }
        *count += 1;
        Ok(())
    }
}

// -- FailingRollbackSnapshotStore: first write succeeds, subsequent writes fail --

struct FailingRollbackSnapshotStore {
    snapshot: RefCell<RunSnapshot>,
    write_count: RefCell<usize>,
}

impl FailingRollbackSnapshotStore {
    fn new(initial: RunSnapshot) -> Self {
        Self {
            snapshot: RefCell::new(initial),
            write_count: RefCell::new(0),
        }
    }

    fn initial() -> Self {
        Self::new(RunSnapshot::initial(20))
    }
}

impl RunSnapshotPort for FailingRollbackSnapshotStore {
    fn read_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<RunSnapshot> {
        Ok(self.snapshot.borrow().clone())
    }
}

impl RunSnapshotWritePort for FailingRollbackSnapshotStore {
    fn write_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        snapshot: &RunSnapshot,
    ) -> AppResult<()> {
        let mut count = self.write_count.borrow_mut();
        if *count == 0 {
            // First write succeeds (canonical commit).
            *count += 1;
            self.snapshot.replace(snapshot.clone());
            Ok(())
        } else {
            // Subsequent writes fail (rollback attempt).
            Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "simulated rollback snapshot write failure",
            )))
        }
    }
}

#[test]
fn stage_amendment_batch_surfaces_partial_journal_as_corrupt_record() {
    // Two amendments: first journal append succeeds, second fails.
    // The first journal line is permanent — canonical state (snapshot + files)
    // is rolled back but the journal has orphaned entries.
    // Must return CorruptRecord, not a plain I/O error.
    let queue = FakeAmendmentQueue::empty();
    let shared_store = SharedRunSnapshotStore::initial();
    let journal = FailAfterNAppendsJournalStore::new(1); // succeed once, then fail
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let amendments = vec![
        QueuedAmendment {
            amendment_id: "batch-1".to_owned(),
            source_stage: StageId::Planning,
            source_cycle: 1,
            source_completion_round: 1,
            body: "first".to_owned(),
            created_at: test_timestamp(),
            batch_sequence: 0,
            source: AmendmentSource::PrReview,
            dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::PrReview, "first"),
        },
        QueuedAmendment {
            amendment_id: "batch-2".to_owned(),
            source_stage: StageId::Planning,
            source_cycle: 1,
            source_completion_round: 1,
            body: "second".to_owned(),
            created_at: test_timestamp(),
            batch_sequence: 1,
            source: AmendmentSource::PrReview,
            dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::PrReview, "second"),
        },
    ];

    let result = service::stage_amendment_batch(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        &base,
        &pid,
        &amendments,
    );

    // Must fail with CorruptRecord because the first journal line persisted
    // but canonical state was rolled back.
    assert!(result.is_err());
    match result.unwrap_err() {
        AppError::CorruptRecord { details, .. } => {
            assert!(
                details.contains("batch journal append failed after 1 of 2 events"),
                "CorruptRecord should describe partial journal state, got: {details}"
            );
        }
        other => panic!("expected CorruptRecord for partial journal persistence, got: {other:?}"),
    }

    // Amendment files must still be rolled back.
    assert!(
        queue.amendments.borrow().is_empty(),
        "amendment files must be rolled back even with partial journal"
    );

    // Snapshot must be restored to pre-mutation state.
    let snap = shared_store.read_run_snapshot(&base, &pid).unwrap();
    assert!(
        snap.amendment_queue.pending.is_empty(),
        "snapshot must be restored after partial journal rollback"
    );
}

#[test]
fn add_manual_amendment_returns_corrupt_record_when_rollback_fails() {
    // Journal append fails, then the rollback snapshot write also fails.
    // Must return CorruptRecord with both error details.
    let queue = FakeAmendmentQueue::empty();
    let store = FailingRollbackSnapshotStore::initial();
    let journal = FailingAppendJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::add_manual_amendment(
        &queue,
        &store,
        &store,
        &journal,
        &project_store,
        &base,
        &pid,
        "should trigger rollback failure",
    );

    assert!(result.is_err());
    match result.unwrap_err() {
        AppError::CorruptRecord { details, .. } => {
            assert!(
                details.contains("amendment journal append failed"),
                "CorruptRecord should mention journal append failure, got: {details}"
            );
            assert!(
                details.contains("rollback also failed"),
                "CorruptRecord should mention rollback failure, got: {details}"
            );
        }
        other => {
            panic!("expected CorruptRecord when rollback fails after journal error, got: {other:?}")
        }
    }
}

#[test]
fn stage_amendment_batch_returns_corrupt_record_when_rollback_fails() {
    // Journal append fails on the first event, but rollback snapshot write
    // also fails. Must return CorruptRecord even though no partial journal
    // entries exist.
    let queue = FakeAmendmentQueue::empty();
    let store = FailingRollbackSnapshotStore::initial();
    let journal = FailingAppendJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let amendments = vec![QueuedAmendment {
        amendment_id: "batch-1".to_owned(),
        source_stage: StageId::Planning,
        source_cycle: 1,
        source_completion_round: 1,
        body: "first".to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 0,
        source: AmendmentSource::PrReview,
        dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::PrReview, "first"),
    }];

    let result = service::stage_amendment_batch(
        &queue,
        &store,
        &store,
        &journal,
        &project_store,
        &base,
        &pid,
        &amendments,
    );

    assert!(result.is_err());
    match result.unwrap_err() {
        AppError::CorruptRecord { details, .. } => {
            assert!(
                details.contains("batch journal append failed"),
                "CorruptRecord should describe the journal failure, got: {details}"
            );
            assert!(
                details.contains("snapshot restore:") && !details.contains("snapshot restore: ok"),
                "CorruptRecord should indicate snapshot restore failure, got: {details}"
            );
        }
        other => {
            panic!("expected CorruptRecord when rollback fails after journal error, got: {other:?}")
        }
    }
}

#[test]
fn add_manual_amendment_returns_corrupt_record_when_file_rollback_fails() {
    // Journal append fails, snapshot restore succeeds, but amendment file
    // removal fails. Must still return CorruptRecord with file cleanup detail.
    let queue = FailingRemoveAmendmentQueue::with(vec![]);
    let shared_store = SharedRunSnapshotStore::initial();
    let journal = FailingAppendJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::add_manual_amendment(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        &base,
        &pid,
        "should trigger file rollback failure",
    );

    assert!(result.is_err());
    match result.unwrap_err() {
        AppError::CorruptRecord { details, .. } => {
            assert!(
                details.contains("amendment journal append failed"),
                "CorruptRecord should mention journal append failure, got: {details}"
            );
            assert!(
                details.contains("file cleanup:") && !details.contains("file cleanup: ok"),
                "CorruptRecord should indicate file cleanup failure, got: {details}"
            );
        }
        other => panic!(
            "expected CorruptRecord when file cleanup fails after journal error, got: {other:?}"
        ),
    }
}

// -- AlwaysFailingSnapshotStore: reads succeed, every write fails --

struct AlwaysFailingSnapshotStore {
    snapshot: RunSnapshot,
}

impl AlwaysFailingSnapshotStore {
    fn initial() -> Self {
        Self {
            snapshot: RunSnapshot::initial(20),
        }
    }
}

impl RunSnapshotPort for AlwaysFailingSnapshotStore {
    fn read_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<RunSnapshot> {
        Ok(self.snapshot.clone())
    }
}

impl RunSnapshotWritePort for AlwaysFailingSnapshotStore {
    fn write_run_snapshot(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        _snapshot: &RunSnapshot,
    ) -> AppResult<()> {
        Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "simulated snapshot write failure",
        )))
    }
}

// -- FailingWriteAmendmentQueue: remove succeeds, write always fails --
// Used for remove/clear tests where the file was successfully deleted
// but cannot be restored.

struct FailingWriteAmendmentQueue {
    amendments: RefCell<Vec<QueuedAmendment>>,
}

impl FailingWriteAmendmentQueue {
    fn with(amendments: Vec<QueuedAmendment>) -> Self {
        Self {
            amendments: RefCell::new(amendments),
        }
    }
}

impl AmendmentQueuePort for FailingWriteAmendmentQueue {
    fn write_amendment(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        _amendment: &QueuedAmendment,
    ) -> AppResult<()> {
        Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "simulated write failure",
        )))
    }

    fn list_pending_amendments(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<Vec<QueuedAmendment>> {
        Ok(self.amendments.borrow().clone())
    }

    fn remove_amendment(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        amendment_id: &str,
    ) -> AppResult<()> {
        let mut amendments = self.amendments.borrow_mut();
        let pos = amendments
            .iter()
            .position(|a| a.amendment_id == amendment_id);
        match pos {
            Some(idx) => {
                amendments.remove(idx);
                Ok(())
            }
            None => Err(AppError::AmendmentNotFound {
                amendment_id: amendment_id.to_owned(),
            }),
        }
    }

    fn drain_amendments(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<u32> {
        let mut amendments = self.amendments.borrow_mut();
        let count = amendments.len() as u32;
        amendments.clear();
        Ok(count)
    }

    fn has_pending_amendments(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<bool> {
        Ok(!self.amendments.borrow().is_empty())
    }
}

// ── Pre-commit rollback failure tests (Required Change 1) ──

#[test]
fn add_manual_amendment_returns_corrupt_when_snapshot_and_cleanup_both_fail() {
    // Snapshot/reopen write fails, and amendment file cleanup also fails.
    // Must return CorruptRecord with both failures, not just the snapshot error.
    let queue = FailingRemoveAmendmentQueue::with(vec![]);
    let store = AlwaysFailingSnapshotStore::initial();
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::add_manual_amendment(
        &queue,
        &store,
        &store,
        &journal,
        &project_store,
        &base,
        &pid,
        "should fail on snapshot then fail on cleanup",
    );

    assert!(result.is_err());
    match result.unwrap_err() {
        AppError::CorruptRecord { details, .. } => {
            assert!(
                details.contains("snapshot/reopen write failed"),
                "CorruptRecord should mention snapshot failure, got: {details}"
            );
            assert!(
                details.contains("amendment file cleanup also failed"),
                "CorruptRecord should mention cleanup failure, got: {details}"
            );
        }
        other => {
            panic!("expected CorruptRecord when both snapshot and cleanup fail, got: {other:?}")
        }
    }
}

#[test]
fn stage_amendment_batch_returns_corrupt_when_snapshot_and_cleanup_both_fail() {
    // Snapshot/reopen write fails, and file cleanup also fails.
    let queue = FailingRemoveAmendmentQueue::with(vec![]);
    let store = AlwaysFailingSnapshotStore::initial();
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let amendments = vec![QueuedAmendment {
        amendment_id: "batch-1".to_owned(),
        source_stage: StageId::Planning,
        source_cycle: 1,
        source_completion_round: 1,
        body: "first".to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 0,
        source: AmendmentSource::PrReview,
        dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::PrReview, "first"),
    }];

    let result = service::stage_amendment_batch(
        &queue,
        &store,
        &store,
        &journal,
        &project_store,
        &base,
        &pid,
        &amendments,
    );

    assert!(result.is_err());
    match result.unwrap_err() {
        AppError::CorruptRecord { details, .. } => {
            assert!(
                details.contains("snapshot/reopen write failed"),
                "CorruptRecord should mention snapshot failure, got: {details}"
            );
            assert!(
                details.contains("amendment file cleanup also failed"),
                "CorruptRecord should mention cleanup failure, got: {details}"
            );
        }
        other => {
            panic!("expected CorruptRecord when both snapshot and cleanup fail, got: {other:?}")
        }
    }
}

// ── Remove/clear restore failure tests (Required Change 2) ──

#[test]
fn remove_amendment_returns_corrupt_when_snapshot_and_restore_both_fail() {
    // File deletion succeeds, snapshot write fails, file restore also fails.
    let amendment = QueuedAmendment {
        amendment_id: "manual-test-123".to_owned(),
        source_stage: StageId::Planning,
        source_cycle: 1,
        source_completion_round: 1,
        body: "test body".to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 0,
        source: AmendmentSource::Manual,
        dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "test body"),
    };

    // FailingWriteAmendmentQueue: remove succeeds, write (restore) fails.
    let queue = FailingWriteAmendmentQueue::with(vec![amendment.clone()]);

    // Use a shared store seeded with the amendment in run.json, then make
    // snapshot write always fail by wrapping in AlwaysFailingSnapshotStore.
    let mut snap = RunSnapshot::initial(20);
    snap.amendment_queue.pending.push(amendment);
    let store = AlwaysFailingSnapshotStore { snapshot: snap };

    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::remove_amendment(&queue, &store, &store, &base, &pid, "manual-test-123");

    assert!(result.is_err());
    match result.unwrap_err() {
        AppError::CorruptRecord { details, .. } => {
            assert!(
                details.contains("snapshot write failed after amendment file deletion"),
                "CorruptRecord should mention snapshot failure, got: {details}"
            );
            assert!(
                details.contains("amendment file restore also failed"),
                "CorruptRecord should mention restore failure, got: {details}"
            );
        }
        other => {
            panic!("expected CorruptRecord when both snapshot and restore fail, got: {other:?}")
        }
    }
}

#[test]
fn clear_amendments_returns_corrupt_when_snapshot_and_restore_both_fail() {
    // All file deletions succeed, snapshot write fails, file restores also fail.
    let amendment = QueuedAmendment {
        amendment_id: "clear-test-1".to_owned(),
        source_stage: StageId::Planning,
        source_cycle: 1,
        source_completion_round: 1,
        body: "test body".to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 0,
        source: AmendmentSource::Manual,
        dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "test body"),
    };

    // FailingWriteAmendmentQueue: remove succeeds, write (restore) fails.
    let queue = FailingWriteAmendmentQueue::with(vec![amendment.clone()]);

    let mut snap = RunSnapshot::initial(20);
    snap.amendment_queue.pending.push(amendment);
    let store = AlwaysFailingSnapshotStore { snapshot: snap };

    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::clear_amendments(&queue, &store, &store, &base, &pid);

    assert!(result.is_err());
    match result.unwrap_err() {
        AppError::CorruptRecord { details, .. } => {
            assert!(
                details.contains("snapshot write failed after clearing amendments"),
                "CorruptRecord should mention snapshot failure, got: {details}"
            );
            assert!(
                details.contains("amendment file restore also failed"),
                "CorruptRecord should mention restore failure, got: {details}"
            );
        }
        other => {
            panic!("expected CorruptRecord when both snapshot and restore fail, got: {other:?}")
        }
    }
}

// -- PartialRemoveFailingWriteQueue: first N removes succeed, rest fail;
// write always fails. Used for partial-clear + restore-failure tests. --

struct PartialRemoveFailingWriteQueue {
    amendments: RefCell<Vec<QueuedAmendment>>,
    removes_before_failure: usize,
    remove_count: RefCell<usize>,
}

impl PartialRemoveFailingWriteQueue {
    fn new(amendments: Vec<QueuedAmendment>, removes_before_failure: usize) -> Self {
        Self {
            amendments: RefCell::new(amendments),
            removes_before_failure,
            remove_count: RefCell::new(0),
        }
    }
}

impl AmendmentQueuePort for PartialRemoveFailingWriteQueue {
    fn write_amendment(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        _amendment: &QueuedAmendment,
    ) -> AppResult<()> {
        Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "simulated write failure",
        )))
    }

    fn list_pending_amendments(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<Vec<QueuedAmendment>> {
        Ok(self.amendments.borrow().clone())
    }

    fn remove_amendment(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        amendment_id: &str,
    ) -> AppResult<()> {
        let mut count = self.remove_count.borrow_mut();
        if *count < self.removes_before_failure {
            *count += 1;
            let mut amendments = self.amendments.borrow_mut();
            amendments.retain(|a| a.amendment_id != amendment_id);
            Ok(())
        } else {
            Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "simulated remove failure",
            )))
        }
    }

    fn drain_amendments(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<u32> {
        Ok(0)
    }

    fn has_pending_amendments(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<bool> {
        Ok(!self.amendments.borrow().is_empty())
    }
}

#[test]
fn clear_amendments_partial_returns_corrupt_when_repair_and_restore_both_fail() {
    // Partial file deletion: first remove succeeds, second fails. Then repair
    // snapshot write also fails, and restoring the deleted file also fails.
    let a1 = QueuedAmendment {
        amendment_id: "clear-p-1".to_owned(),
        source_stage: StageId::Planning,
        source_cycle: 1,
        source_completion_round: 1,
        body: "first".to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 0,
        source: AmendmentSource::Manual,
        dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "first"),
    };
    let a2 = QueuedAmendment {
        amendment_id: "clear-p-2".to_owned(),
        source_stage: StageId::Planning,
        source_cycle: 1,
        source_completion_round: 1,
        body: "second".to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 1,
        source: AmendmentSource::Manual,
        dedup_key: QueuedAmendment::compute_dedup_key(&AmendmentSource::Manual, "second"),
    };

    // First remove succeeds (a1 deleted), second fails (a2 remains).
    // Write always fails, so restoring deleted a1 is impossible.
    let queue = PartialRemoveFailingWriteQueue::new(vec![a1.clone(), a2.clone()], 1);

    let mut snap = RunSnapshot::initial(20);
    snap.amendment_queue.pending.push(a1);
    snap.amendment_queue.pending.push(a2);
    let store = AlwaysFailingSnapshotStore { snapshot: snap };

    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let result = service::clear_amendments(&queue, &store, &store, &base, &pid);

    assert!(result.is_err());
    match result.unwrap_err() {
        AppError::CorruptRecord { details, .. } => {
            assert!(
                details.contains("snapshot repair write failed after partial clear"),
                "CorruptRecord should mention repair failure, got: {details}"
            );
            assert!(
                details.contains("amendment file restore also failed"),
                "CorruptRecord should mention restore failure, got: {details}"
            );
        }
        other => panic!("expected CorruptRecord when both repair and restore fail, got: {other:?}"),
    }
}

// ── Completed-project reopen failure: staged amendments must persist ─────

#[test]
fn stage_amendment_batch_reopens_legacy_completed_snapshot_without_backfilling_max_rounds() {
    let tmp = tempdir().unwrap();
    let project_root = tmp.path().join(".ralph-burning/projects/alpha");
    std::fs::create_dir_all(&project_root).unwrap();
    std::fs::write(project_root.join("prompt.md"), "# Prompt\n").unwrap();

    let queue = FakeAmendmentQueue::empty();
    let shared_store =
        SharedRunSnapshotStore::new(completed_snapshot_with_max_completion_rounds(None));
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let pid = ProjectId::new("alpha").unwrap();

    let amendments = vec![QueuedAmendment {
        amendment_id: "pr-review-legacy".to_owned(),
        source_stage: StageId::Review,
        source_cycle: 1,
        source_completion_round: 1,
        body: "keep historical limit unknown".to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 1,
        source: AmendmentSource::PrReview,
        dedup_key: QueuedAmendment::compute_dedup_key(
            &AmendmentSource::PrReview,
            "keep historical limit unknown",
        ),
    }];

    service::stage_amendment_batch(
        &queue,
        &shared_store,
        &shared_store,
        &journal,
        &project_store,
        tmp.path(),
        &pid,
        &amendments,
    )
    .expect("stage amendment batch");

    let snapshot = shared_store
        .read_run_snapshot(tmp.path(), &pid)
        .expect("read updated snapshot");
    assert_eq!(snapshot.status, RunStatus::Paused);
    assert_eq!(
        snapshot.max_completion_rounds, None,
        "staging amendments onto a legacy completed snapshot should preserve unknown historical max_completion_rounds"
    );
    assert_eq!(snapshot.amendment_queue.pending.len(), 1);
    assert_eq!(snapshot.completion_rounds, 2);
}

#[test]
fn stage_amendment_batch_preserves_files_on_completed_project_reopen_failure() {
    // When the project is completed and the reopen/snapshot write fails,
    // amendment files already written must remain on disk. The project
    // snapshot stays at its last committed state and no journal events
    // are written.
    let queue = FakeAmendmentQueue::empty();
    let completed_snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Completed,
        cycle_history: vec![CycleHistoryEntry {
            cycle: 1,
            stage_id: StageId::FinalReview,
            started_at: test_timestamp(),
            completed_at: Some(test_timestamp()),
        }],
        completion_rounds: 1,
        max_completion_rounds: Some(0),
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "completed".to_owned(),
        last_stage_resolution_snapshot: None,
    };

    // The read store returns the completed snapshot; the write store always fails.
    let shared_store = SharedRunSnapshotStore::new(completed_snapshot);
    let failing_write = FailingRunSnapshotWriteStore;
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let amendments = vec![QueuedAmendment {
        amendment_id: "pr-review-persist-me".to_owned(),
        source_stage: StageId::Review,
        source_cycle: 1,
        source_completion_round: 1,
        body: "persist me before failure".to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 1,
        source: AmendmentSource::PrReview,
        dedup_key: QueuedAmendment::compute_dedup_key(
            &AmendmentSource::PrReview,
            "persist me before failure",
        ),
    }];

    let result = service::stage_amendment_batch(
        &queue,
        &shared_store,
        &failing_write,
        &journal,
        &project_store,
        &base,
        &pid,
        &amendments,
    );

    // Must fail because the reopen/snapshot write fails.
    assert!(result.is_err());

    // Amendment files must remain on disk — they must NOT be rolled back.
    assert_eq!(
        queue.amendments.borrow().len(),
        1,
        "staged amendment files must persist when reopen/snapshot write fails for a completed project"
    );

    // Verify the correct amendment survived.
    assert_eq!(
        queue.amendments.borrow()[0].amendment_id,
        "pr-review-persist-me"
    );
}

#[test]
fn stage_amendment_batch_rolls_back_files_on_non_completed_snapshot_write_failure() {
    // When the project is NOT completed and the snapshot write fails,
    // amendment files must be rolled back (no pre-commit files leak).
    let queue = FakeAmendmentQueue::empty();
    let shared_store = SharedRunSnapshotStore::initial(); // status: NotStarted
    let failing_write = FailingRunSnapshotWriteStore;
    let journal = FakeJournalStore;
    let project_store = FakeProjectStore::with_existing(&["alpha"]);
    let base = dummy_base_dir();
    let pid = ProjectId::new("alpha").unwrap();

    let amendments = vec![QueuedAmendment {
        amendment_id: "batch-rollback-1".to_owned(),
        source_stage: StageId::Planning,
        source_cycle: 1,
        source_completion_round: 1,
        body: "should be rolled back".to_owned(),
        created_at: test_timestamp(),
        batch_sequence: 1,
        source: AmendmentSource::PrReview,
        dedup_key: QueuedAmendment::compute_dedup_key(
            &AmendmentSource::PrReview,
            "should be rolled back",
        ),
    }];

    let result = service::stage_amendment_batch(
        &queue,
        &shared_store,
        &failing_write,
        &journal,
        &project_store,
        &base,
        &pid,
        &amendments,
    );

    assert!(result.is_err());

    // Amendment files must be rolled back for non-completed projects.
    assert!(
        queue.amendments.borrow().is_empty(),
        "amendment files must be rolled back when snapshot write fails for non-completed project"
    );
}
