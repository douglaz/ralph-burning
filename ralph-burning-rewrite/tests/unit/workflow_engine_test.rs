use std::fs;
use std::path::Path;

use chrono::Utc;
use tempfile::tempdir;

use ralph_burning::adapters::fs::{
    FsJournalStore, FsPayloadArtifactWriteStore, FsProjectStore, FsRawOutputStore,
    FsRunSnapshotStore, FsRunSnapshotWriteStore, FsRuntimeLogWriteStore, FsSessionStore,
};
use ralph_burning::adapters::stub_backend::StubBackendAdapter;
use ralph_burning::contexts::agent_execution::service::AgentExecutionService;
use ralph_burning::contexts::project_run_record::model::{
    JournalEvent, JournalEventType, RunSnapshot, RunStatus,
};
use ralph_burning::contexts::project_run_record::service::{
    self, CreateProjectInput, JournalStorePort, RunSnapshotPort, RunSnapshotWritePort,
};
use ralph_burning::shared::error::{AppError, AppResult};
use ralph_burning::contexts::workflow_composition::engine;
use ralph_burning::contexts::workspace_governance;
use ralph_burning::contexts::workspace_governance::config::EffectiveConfig;
use ralph_burning::shared::domain::{FlowPreset, ProjectId, StageId};

fn setup_workspace(base_dir: &Path) {
    workspace_governance::initialize_workspace(base_dir, Utc::now()).unwrap();
}

fn create_standard_project(base_dir: &Path, project_id: &str) -> ProjectId {
    let pid = ProjectId::new(project_id).unwrap();
    let store = FsProjectStore;
    let journal_store = FsJournalStore;
    service::create_project(
        &store,
        &journal_store,
        base_dir,
        CreateProjectInput {
            id: pid.clone(),
            name: format!("Test {}", project_id),
            flow: FlowPreset::Standard,
            prompt_path: "prompt.md".to_owned(),
            prompt_contents: "# Test prompt".to_owned(),
            prompt_hash: "testhash123".to_owned(),
            created_at: Utc::now(),
        },
    )
    .unwrap();

    // Select as active
    workspace_governance::set_active_project(base_dir, &pid).unwrap();
    pid
}

fn build_agent_service(
) -> AgentExecutionService<StubBackendAdapter, FsRawOutputStore, FsSessionStore> {
    AgentExecutionService::new(
        StubBackendAdapter::default(),
        FsRawOutputStore,
        FsSessionStore,
    )
}

// ── Stage plan tests ────────────────────────────────────────────────────────

#[test]
fn stage_plan_with_prompt_review_enabled() {
    let plan = engine::standard_stage_plan(true);
    assert_eq!(plan.len(), 8);
    assert_eq!(plan[0], StageId::PromptReview);
    assert_eq!(plan[1], StageId::Planning);
    assert_eq!(plan[7], StageId::FinalReview);
}

#[test]
fn stage_plan_with_prompt_review_disabled() {
    let plan = engine::standard_stage_plan(false);
    assert_eq!(plan.len(), 7);
    assert_eq!(plan[0], StageId::Planning);
    assert!(!plan.contains(&StageId::PromptReview));
}

#[test]
fn role_mapping_is_deterministic() {
    use ralph_burning::shared::domain::BackendRole;
    assert_eq!(engine::role_for_stage(StageId::PromptReview), BackendRole::Planner);
    assert_eq!(engine::role_for_stage(StageId::Planning), BackendRole::Planner);
    assert_eq!(engine::role_for_stage(StageId::Implementation), BackendRole::Implementer);
    assert_eq!(engine::role_for_stage(StageId::Qa), BackendRole::QaValidator);
    assert_eq!(engine::role_for_stage(StageId::Review), BackendRole::Reviewer);
    assert_eq!(engine::role_for_stage(StageId::CompletionPanel), BackendRole::CompletionJudge);
    assert_eq!(engine::role_for_stage(StageId::AcceptanceQa), BackendRole::QaValidator);
    assert_eq!(engine::role_for_stage(StageId::FinalReview), BackendRole::CompletionJudge);
}

// ── Happy path tests ────────────────────────────────────────────────────────

#[tokio::test]
async fn happy_path_standard_run_completes() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "happy-test");

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_ok(), "execute_standard_run failed: {:?}", result.err());

    // Verify final run snapshot
    let snapshot = FsRunSnapshotStore.read_run_snapshot(base_dir, &pid).unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert!(snapshot.active_run.is_none());
    assert_eq!(snapshot.completion_rounds, 1);
    assert_eq!(snapshot.status_summary, "completed");

    // Verify journal events
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    // project_created + run_started + (8 * (stage_entered + stage_completed)) + run_completed
    // = 1 + 1 + 16 + 1 = 19 events (with prompt_review enabled by default)
    assert!(events.len() >= 19, "expected >= 19 events, got {}", events.len());

    // First event should be project_created, second run_started
    assert_eq!(events[0].event_type, JournalEventType::ProjectCreated);
    assert_eq!(events[1].event_type, JournalEventType::RunStarted);

    // Last event should be run_completed
    assert_eq!(events.last().unwrap().event_type, JournalEventType::RunCompleted);

    // Verify payloads and artifacts were written
    let payloads_dir = base_dir
        .join(".ralph-burning/projects/happy-test/history/payloads");
    let artifacts_dir = base_dir
        .join(".ralph-burning/projects/happy-test/history/artifacts");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    let artifact_count = fs::read_dir(&artifacts_dir).unwrap().count();
    assert_eq!(payload_count, 8, "expected 8 payloads");
    assert_eq!(artifact_count, 8, "expected 8 artifacts");
}

#[tokio::test]
async fn happy_path_prompt_review_disabled() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);

    // Disable prompt review in config
    workspace_governance::config::EffectiveConfig::set(base_dir, "prompt_review.enabled", "false")
        .unwrap();

    let pid = create_standard_project(base_dir, "no-pr-test");

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();
    assert!(!config.prompt_review_enabled());

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_ok());

    // Verify 7 stages completed (no prompt_review)
    let payloads_dir = base_dir
        .join(".ralph-burning/projects/no-pr-test/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(payload_count, 7, "expected 7 payloads without prompt_review");

    // Verify no prompt_review stage_entered in journal
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let pr_events: Vec<_> = events
        .iter()
        .filter(|e| {
            if e.event_type == JournalEventType::StageEntered {
                e.details.get("stage_id").and_then(|v| v.as_str()) == Some("prompt_review")
            } else {
                false
            }
        })
        .collect();
    assert!(pr_events.is_empty(), "prompt_review stage should not appear");
}

// ── Precondition failure tests ──────────────────────────────────────────────

#[tokio::test]
async fn run_start_rejects_already_running() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "running-test");

    // Manually set status to running
    let snapshot = RunSnapshot {
        active_run: Some(ralph_burning::contexts::project_run_record::model::ActiveRun {
            run_id: "run-fake".to_owned(),
            stage_cursor: ralph_burning::shared::domain::StageCursor::initial(StageId::Planning),
            started_at: Utc::now(),
        }),
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 0,
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "running".to_owned(),
    };
    FsRunSnapshotWriteStore
        .write_run_snapshot(base_dir, &pid, &snapshot)
        .unwrap();

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("not_started"), "unexpected error: {}", err);
}

#[tokio::test]
async fn run_start_rejects_completed_project() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "completed-test");

    let snapshot = RunSnapshot {
        active_run: None,
        status: RunStatus::Completed,
        cycle_history: vec![],
        completion_rounds: 1,
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "completed".to_owned(),
    };
    FsRunSnapshotWriteStore
        .write_run_snapshot(base_dir, &pid, &snapshot)
        .unwrap();

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_err());
}

// ── Preflight failure tests ─────────────────────────────────────────────────

#[tokio::test]
async fn preflight_failure_leaves_state_unchanged() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "preflight-test");

    // Use an unavailable backend to trigger preflight failure
    let agent_service = AgentExecutionService::new(
        StubBackendAdapter::default().unavailable(),
        FsRawOutputStore,
        FsSessionStore,
    );
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_err());

    // Verify run.json is still not_started
    let snapshot = FsRunSnapshotStore.read_run_snapshot(base_dir, &pid).unwrap();
    assert_eq!(snapshot.status, RunStatus::NotStarted);
    assert!(snapshot.active_run.is_none());

    // Verify journal only has project_created
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, JournalEventType::ProjectCreated);

    // Verify no payloads or artifacts created
    let payloads_dir = base_dir
        .join(".ralph-burning/projects/preflight-test/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(payload_count, 0);
}

// ── Stage plan resolution tests ─────────────────────────────────────────────

#[test]
fn resolve_stage_plan_produces_correct_targets() {
    let resolver = ralph_burning::contexts::agent_execution::service::BackendResolver::new();
    let stages = engine::standard_stage_plan(true);
    let plan = engine::resolve_stage_plan(&stages, &resolver, None).unwrap();

    assert_eq!(plan.len(), 8);
    assert_eq!(plan[0].stage_id, StageId::PromptReview);
    assert_eq!(
        plan[0].role,
        ralph_burning::shared::domain::BackendRole::Planner
    );
    assert_eq!(plan[2].stage_id, StageId::Implementation);
    assert_eq!(
        plan[2].role,
        ralph_burning::shared::domain::BackendRole::Implementer
    );
}

// ── Preflight check unit test ───────────────────────────────────────────────

#[tokio::test]
async fn preflight_check_succeeds_with_default_stub() {
    let resolver = ralph_burning::contexts::agent_execution::service::BackendResolver::new();
    let stages = engine::standard_stage_plan(true);
    let plan = engine::resolve_stage_plan(&stages, &resolver, None).unwrap();

    let adapter = StubBackendAdapter::default();
    let result = engine::preflight_check(&adapter, &plan).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn preflight_check_fails_with_unavailable_backend() {
    let resolver = ralph_burning::contexts::agent_execution::service::BackendResolver::new();
    let stages = engine::standard_stage_plan(true);
    let plan = engine::resolve_stage_plan(&stages, &resolver, None).unwrap();

    let adapter = StubBackendAdapter::default().unavailable();
    let result = engine::preflight_check(&adapter, &plan).await;
    assert!(result.is_err());
}

// ── Failing-port tests: journal-append and snapshot-write errors ─────────

use std::sync::atomic::{AtomicU32, Ordering};

/// A journal store that delegates to `FsJournalStore` but fails on the Nth
/// append_event call (1-indexed). This lets us test failure at specific
/// stage-commit boundaries.
struct FailingJournalStore {
    call_count: AtomicU32,
    fail_on_call: u32,
}

impl FailingJournalStore {
    fn new(fail_on_call: u32) -> Self {
        Self {
            call_count: AtomicU32::new(0),
            fail_on_call,
        }
    }
}

impl JournalStorePort for FailingJournalStore {
    fn read_journal(
        &self,
        base_dir: &Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
    ) -> AppResult<Vec<JournalEvent>> {
        FsJournalStore.read_journal(base_dir, project_id)
    }

    fn append_event(
        &self,
        base_dir: &Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        line: &str,
    ) -> AppResult<()> {
        let n = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.fail_on_call {
            return Err(AppError::Io(std::io::Error::other(
                "simulated journal append failure",
            )));
        }
        FsJournalStore.append_event(base_dir, project_id, line)
    }
}

/// A snapshot write store that delegates to `FsRunSnapshotWriteStore` but fails
/// on the Nth write call (1-indexed).
struct FailingSnapshotWriteStore {
    call_count: AtomicU32,
    fail_on_call: u32,
}

impl FailingSnapshotWriteStore {
    fn new(fail_on_call: u32) -> Self {
        Self {
            call_count: AtomicU32::new(0),
            fail_on_call,
        }
    }
}

impl RunSnapshotWritePort for FailingSnapshotWriteStore {
    fn write_run_snapshot(
        &self,
        base_dir: &Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        snapshot: &RunSnapshot,
    ) -> AppResult<()> {
        let n = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.fail_on_call {
            return Err(AppError::Io(std::io::Error::other(
                "simulated snapshot write failure",
            )));
        }
        FsRunSnapshotWriteStore.write_run_snapshot(base_dir, project_id, snapshot)
    }
}

/// Journal append failure after payload/artifact write must roll back the
/// payload/artifact pair so no partial durable history is visible, and the
/// run must end in failed state.
#[tokio::test]
async fn journal_failure_after_payload_rolls_back_and_fails_run() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "journal-fail");

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    // The engine calls append_event for:
    //   1: run_started
    //   2: stage_entered (stage 1)
    //   3: stage_completed (stage 1) — fail here
    let failing_journal = FailingJournalStore::new(3);

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &failing_journal,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_err(), "run should fail on journal failure");

    // Run must be in failed state
    let snapshot = FsRunSnapshotStore.read_run_snapshot(base_dir, &pid).unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);
    assert!(snapshot.active_run.is_none());

    // No payload/artifact should be visible for the first stage since it was
    // rolled back after journal failure
    let payloads_dir = base_dir
        .join(".ralph-burning/projects/journal-fail/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(payload_count, 0, "payload should have been rolled back");

    let artifacts_dir = base_dir
        .join(".ralph-burning/projects/journal-fail/history/artifacts");
    let artifact_count = fs::read_dir(&artifacts_dir).unwrap().count();
    assert_eq!(artifact_count, 0, "artifact should have been rolled back");

    // No stage_completed event should exist since journal append failed
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let stage_completed_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::StageCompleted)
        .collect();
    assert!(
        stage_completed_events.is_empty(),
        "no stage_completed event should exist after journal failure, found {}",
        stage_completed_events.len()
    );
}

/// Snapshot write failure during stage commit must roll back the
/// payload/artifact pair, leave no stage_completed journal event, and mark
/// the run as failed. Because the engine writes the snapshot BEFORE
/// stage_completed, a snapshot failure means stage_completed is never appended.
#[tokio::test]
async fn snapshot_failure_during_stage_commit_rolls_back_without_journal_leak() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "snap-fail");

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    // The engine calls write_run_snapshot for:
    //   1: initial Running snapshot (run start)
    //   2: stage_entered cursor update (stage 1)
    //   3: stage commit cursor update (stage 1) — fail here
    let failing_snapshot = FailingSnapshotWriteStore::new(3);

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &failing_snapshot,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_err(), "run should fail on snapshot failure");

    // The run must end in failed state. Since the failing store only fails
    // on call 3, the fail_run recovery can still write the failed snapshot
    // (calls 4+).
    let snapshot = FsRunSnapshotStore.read_run_snapshot(base_dir, &pid).unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);
    assert!(snapshot.active_run.is_none());

    // No payload/artifact should be visible for the first stage
    let payloads_dir = base_dir
        .join(".ralph-burning/projects/snap-fail/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(payload_count, 0, "payload should have been rolled back");

    let artifacts_dir = base_dir
        .join(".ralph-burning/projects/snap-fail/history/artifacts");
    let artifact_count = fs::read_dir(&artifacts_dir).unwrap().count();
    assert_eq!(artifact_count, 0, "artifact should have been rolled back");

    // CRITICAL: No stage_completed event must exist in the journal.
    // Because the engine writes snapshot before stage_completed, snapshot
    // failure prevents the journal event from ever being appended.
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let stage_completed_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::StageCompleted)
        .collect();
    assert!(
        stage_completed_events.is_empty(),
        "no stage_completed event should exist after snapshot failure, found {}",
        stage_completed_events.len()
    );
}
