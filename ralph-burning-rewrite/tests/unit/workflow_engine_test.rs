use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use serde_json::{json, Value};
use tempfile::tempdir;

use ralph_burning::adapters::fs::{
    FsArtifactStore, FsJournalStore, FsPayloadArtifactWriteStore, FsProjectStore, FsRawOutputStore,
    FsRunSnapshotStore, FsRunSnapshotWriteStore, FsRuntimeLogWriteStore, FsSessionStore,
};
use ralph_burning::adapters::stub_backend::StubBackendAdapter;
use ralph_burning::contexts::agent_execution::model::{
    CancellationToken, InvocationEnvelope, InvocationRequest,
};
use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
use ralph_burning::contexts::agent_execution::service::AgentExecutionService;
use ralph_burning::contexts::project_run_record::model::{
    JournalEvent, JournalEventType, RunSnapshot, RunStatus,
};
use ralph_burning::contexts::project_run_record::service::{
    self, CreateProjectInput, JournalStorePort, RunSnapshotPort, RunSnapshotWritePort,
};
use ralph_burning::contexts::workflow_composition::engine;
use ralph_burning::contexts::workspace_governance;
use ralph_burning::contexts::workspace_governance::config::EffectiveConfig;
use ralph_burning::shared::domain::{FailureClass, FlowPreset, ProjectId, StageId};
use ralph_burning::shared::error::{AppError, AppResult};

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

fn build_agent_service_with_adapter<A: AgentExecutionPort>(
    adapter: A,
) -> AgentExecutionService<A, FsRawOutputStore, FsSessionStore> {
    AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore)
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
    assert_eq!(
        engine::role_for_stage(StageId::PromptReview),
        BackendRole::Planner
    );
    assert_eq!(
        engine::role_for_stage(StageId::Planning),
        BackendRole::Planner
    );
    assert_eq!(
        engine::role_for_stage(StageId::Implementation),
        BackendRole::Implementer
    );
    assert_eq!(
        engine::role_for_stage(StageId::Qa),
        BackendRole::QaValidator
    );
    assert_eq!(
        engine::role_for_stage(StageId::Review),
        BackendRole::Reviewer
    );
    assert_eq!(
        engine::role_for_stage(StageId::CompletionPanel),
        BackendRole::CompletionJudge
    );
    assert_eq!(
        engine::role_for_stage(StageId::AcceptanceQa),
        BackendRole::QaValidator
    );
    assert_eq!(
        engine::role_for_stage(StageId::FinalReview),
        BackendRole::CompletionJudge
    );
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

    assert!(
        result.is_ok(),
        "execute_standard_run failed: {:?}",
        result.err()
    );

    // Verify final run snapshot
    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert!(snapshot.active_run.is_none());
    assert_eq!(snapshot.completion_rounds, 1);
    assert_eq!(snapshot.status_summary, "completed");

    // Verify journal events
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    // project_created + run_started + (8 * (stage_entered + stage_completed)) + run_completed
    // = 1 + 1 + 16 + 1 = 19 events (with prompt_review enabled by default)
    assert!(
        events.len() >= 19,
        "expected >= 19 events, got {}",
        events.len()
    );

    // First event should be project_created, second run_started
    assert_eq!(events[0].event_type, JournalEventType::ProjectCreated);
    assert_eq!(events[1].event_type, JournalEventType::RunStarted);

    // Last event should be run_completed
    assert_eq!(
        events.last().unwrap().event_type,
        JournalEventType::RunCompleted
    );

    // Verify payloads and artifacts were written
    let payloads_dir = base_dir.join(".ralph-burning/projects/happy-test/history/payloads");
    let artifacts_dir = base_dir.join(".ralph-burning/projects/happy-test/history/artifacts");
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
    let payloads_dir = base_dir.join(".ralph-burning/projects/no-pr-test/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(
        payload_count, 7,
        "expected 7 payloads without prompt_review"
    );

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
    assert!(
        pr_events.is_empty(),
        "prompt_review stage should not appear"
    );
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
        active_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: "run-fake".to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
                    StageId::Planning,
                ),
                started_at: Utc::now(),
            },
        ),
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
    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::NotStarted);
    assert!(snapshot.active_run.is_none());

    // Verify journal only has project_created
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, JournalEventType::ProjectCreated);

    // Verify no payloads or artifacts created
    let payloads_dir = base_dir.join(".ralph-burning/projects/preflight-test/history/payloads");
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

/// Stage-entered journal append failure must persist failed state.
/// The run must never be left in an ambiguous running state when the
/// stage_entered event cannot be persisted.
#[tokio::test]
async fn stage_entered_journal_failure_persists_failed_state() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "stage-entry-fail");

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    // The engine calls append_event for:
    //   1: run_started
    //   2: stage_entered (stage 1) — fail here
    let failing_journal = FailingJournalStore::new(2);

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

    assert!(
        result.is_err(),
        "run should fail on stage_entered journal failure"
    );

    // Run must be in failed state, not left in running
    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);
    assert!(snapshot.active_run.is_none());

    // No stage_entered event should exist for the first stage since the append failed
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let stage_entered_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::StageEntered)
        .collect();
    assert!(
        stage_entered_events.is_empty(),
        "no stage_entered event should exist after journal failure, found {}",
        stage_entered_events.len()
    );

    // No payload/artifact should exist
    let payloads_dir = base_dir.join(".ralph-burning/projects/stage-entry-fail/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(payload_count, 0, "no payloads should exist");
}

/// Run-started journal append failure must persist failed state.
/// The run must never be left in running state when run_started cannot be
/// appended to the journal.
#[tokio::test]
async fn run_started_journal_failure_persists_failed_state() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "run-started-fail");

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    // The engine calls append_event for:
    //   1: run_started — fail here
    let failing_journal = FailingJournalStore::new(1);

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

    assert!(
        result.is_err(),
        "run should fail on run_started journal failure"
    );

    // Run must be in failed state, not left in running
    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);
    assert!(snapshot.active_run.is_none());

    // No payloads or artifacts should exist
    let payloads_dir = base_dir.join(".ralph-burning/projects/run-started-fail/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(payload_count, 0, "no payloads should exist");
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
    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);
    assert!(snapshot.active_run.is_none());

    // No payload/artifact should be visible for the first stage since it was
    // rolled back after journal failure
    let payloads_dir = base_dir.join(".ralph-burning/projects/journal-fail/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(payload_count, 0, "payload should have been rolled back");

    let artifacts_dir = base_dir.join(".ralph-burning/projects/journal-fail/history/artifacts");
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

/// Snapshot write failure after a completed stage must still leave the run in a
/// failed state. The stage itself remains durable so resume can restart from
/// the next incomplete boundary.
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
    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);
    assert!(snapshot.active_run.is_none());

    // The completed first stage remains durable so resume can skip it.
    let payloads_dir = base_dir.join(".ralph-burning/projects/snap-fail/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(
        payload_count, 1,
        "completed stage payload should remain durable"
    );

    let artifacts_dir = base_dir.join(".ralph-burning/projects/snap-fail/history/artifacts");
    let artifact_count = fs::read_dir(&artifacts_dir).unwrap().count();
    assert_eq!(
        artifact_count, 1,
        "completed stage artifact should remain durable"
    );

    // The completed stage must remain visible in the journal.
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let stage_completed_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::StageCompleted)
        .collect();
    assert_eq!(stage_completed_events.len(), 1);
}

// ── Failing-port tests: payload/artifact write errors ────────────────────

use ralph_burning::contexts::project_run_record::model::ArtifactRecord;
use ralph_burning::contexts::project_run_record::model::PayloadRecord;
use ralph_burning::contexts::project_run_record::service::PayloadArtifactWritePort;

/// A payload/artifact write store that delegates to `FsPayloadArtifactWriteStore`
/// but fails `write_payload_artifact_pair` on the Nth call (1-indexed).
struct FailingPayloadArtifactWriteStore {
    call_count: AtomicU32,
    fail_on_call: u32,
}

impl FailingPayloadArtifactWriteStore {
    fn new(fail_on_call: u32) -> Self {
        Self {
            call_count: AtomicU32::new(0),
            fail_on_call,
        }
    }
}

impl PayloadArtifactWritePort for FailingPayloadArtifactWriteStore {
    fn write_payload_artifact_pair(
        &self,
        base_dir: &Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        payload: &PayloadRecord,
        artifact: &ArtifactRecord,
    ) -> AppResult<()> {
        let n = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.fail_on_call {
            return Err(AppError::Io(std::io::Error::other(
                "simulated payload/artifact write failure",
            )));
        }
        FsPayloadArtifactWriteStore
            .write_payload_artifact_pair(base_dir, project_id, payload, artifact)
    }

    fn remove_payload_artifact_pair(
        &self,
        base_dir: &Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        payload_id: &str,
        artifact_id: &str,
    ) -> AppResult<()> {
        FsPayloadArtifactWriteStore.remove_payload_artifact_pair(
            base_dir,
            project_id,
            payload_id,
            artifact_id,
        )
    }
}

/// A payload/artifact write store that simulates a leaked canonical payload:
/// on the Nth write call, it writes the payload to the canonical location
/// but then fails (simulating artifact write failure + cleanup failure).
/// This verifies the engine's defense-in-depth cleanup.
struct LeakingPayloadArtifactWriteStore {
    call_count: AtomicU32,
    fail_on_call: u32,
}

impl LeakingPayloadArtifactWriteStore {
    fn new(fail_on_call: u32) -> Self {
        Self {
            call_count: AtomicU32::new(0),
            fail_on_call,
        }
    }
}

impl PayloadArtifactWritePort for LeakingPayloadArtifactWriteStore {
    fn write_payload_artifact_pair(
        &self,
        base_dir: &Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        payload: &PayloadRecord,
        artifact: &ArtifactRecord,
    ) -> AppResult<()> {
        let n = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.fail_on_call {
            // Deliberately write the payload to the canonical location to simulate a leak
            let project_root = base_dir
                .join(".ralph-burning")
                .join("projects")
                .join(project_id.as_str());
            let payload_path = project_root
                .join("history/payloads")
                .join(format!("{}.json", payload.payload_id));
            let payload_json = serde_json::to_string_pretty(payload).unwrap();
            fs::write(&payload_path, payload_json).unwrap();

            return Err(AppError::Io(std::io::Error::other(
                "simulated artifact write failure with leaked payload",
            )));
        }
        FsPayloadArtifactWriteStore
            .write_payload_artifact_pair(base_dir, project_id, payload, artifact)
    }

    fn remove_payload_artifact_pair(
        &self,
        base_dir: &Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        payload_id: &str,
        artifact_id: &str,
    ) -> AppResult<()> {
        FsPayloadArtifactWriteStore.remove_payload_artifact_pair(
            base_dir,
            project_id,
            payload_id,
            artifact_id,
        )
    }
}

/// When write_payload_artifact_pair fails but leaks a canonical payload file,
/// the engine's defense-in-depth cleanup must remove it so no orphaned durable
/// history is visible after the run is failed.
#[tokio::test]
async fn leaked_payload_cleanup_on_write_failure() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "leak-cleanup");

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    // Fail on first write with a leaked payload in canonical location
    let leaking_store = LeakingPayloadArtifactWriteStore::new(1);

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &leaking_store,
        &FsRuntimeLogWriteStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_err(), "run should fail on write failure");

    // Run must be in failed state
    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);
    assert!(snapshot.active_run.is_none());

    // CRITICAL: No leaked payload should remain in canonical location.
    // The engine's defense-in-depth cleanup should have removed it.
    let payloads_dir = base_dir.join(".ralph-burning/projects/leak-cleanup/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(
        payload_count, 0,
        "leaked payload should have been cleaned up by engine, found {} files",
        payload_count
    );

    let artifacts_dir = base_dir.join(".ralph-burning/projects/leak-cleanup/history/artifacts");
    let artifact_count = fs::read_dir(&artifacts_dir).unwrap().count();
    assert_eq!(artifact_count, 0, "no artifacts should exist");

    // No stage_completed event should exist
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let stage_completed_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::StageCompleted)
        .collect();
    assert!(
        stage_completed_events.is_empty(),
        "no stage_completed event should exist after write failure"
    );
}

/// Payload/artifact write failure after stage_entered must persist failed
/// state. The run must never be left in running state when the payload/artifact
/// pair cannot be written.
#[tokio::test]
async fn payload_artifact_write_failure_persists_failed_state() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "pa-write-fail");

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    // Fail on the first write_payload_artifact_pair call (first stage commit)
    let failing_artifact = FailingPayloadArtifactWriteStore::new(1);

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &failing_artifact,
        &FsRuntimeLogWriteStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(
        result.is_err(),
        "run should fail on payload/artifact write failure"
    );

    // Run must be in failed state, not left in running
    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);
    assert!(snapshot.active_run.is_none());

    // No payload/artifact should exist since the write failed
    let payloads_dir = base_dir.join(".ralph-burning/projects/pa-write-fail/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(
        payload_count, 0,
        "no payloads should exist after write failure"
    );

    let artifacts_dir = base_dir.join(".ralph-burning/projects/pa-write-fail/history/artifacts");
    let artifact_count = fs::read_dir(&artifacts_dir).unwrap().count();
    assert_eq!(
        artifact_count, 0,
        "no artifacts should exist after write failure"
    );

    // No stage_completed event should exist
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let stage_completed_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::StageCompleted)
        .collect();
    assert!(
        stage_completed_events.is_empty(),
        "no stage_completed event should exist after payload/artifact write failure"
    );

    // A run_failed event should exist
    let run_failed_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::RunFailed)
        .collect();
    assert!(
        !run_failed_events.is_empty(),
        "run_failed event should exist after payload/artifact write failure"
    );
}

#[derive(Clone)]
struct RecordingAdapter {
    inner: StubBackendAdapter,
    requests: Arc<Mutex<Vec<(StageId, Value)>>>,
}

impl RecordingAdapter {
    fn new(inner: StubBackendAdapter) -> Self {
        Self {
            inner,
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn contexts_for(&self, stage_id: StageId) -> Vec<Value> {
        self.requests
            .lock()
            .expect("recording adapter lock poisoned")
            .iter()
            .filter(|(request_stage_id, _)| *request_stage_id == stage_id)
            .map(|(_, context)| context.clone())
            .collect()
    }
}

impl AgentExecutionPort for RecordingAdapter {
    async fn check_capability(
        &self,
        backend: &ralph_burning::shared::domain::ResolvedBackendTarget,
        stage_contract: &ralph_burning::contexts::workflow_composition::contracts::StageContract,
    ) -> AppResult<()> {
        self.inner.check_capability(backend, stage_contract).await
    }

    async fn check_availability(
        &self,
        backend: &ralph_burning::shared::domain::ResolvedBackendTarget,
    ) -> AppResult<()> {
        self.inner.check_availability(backend).await
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        self.requests
            .lock()
            .expect("recording adapter lock poisoned")
            .push((
                request.stage_contract.stage_id,
                request.payload.context.clone(),
            ));
        self.inner.invoke(request).await
    }

    async fn cancel(&self, invocation_id: &str) -> AppResult<()> {
        self.inner.cancel(invocation_id).await
    }
}

#[derive(Clone)]
struct CancelDuringRetryAdapter {
    inner: StubBackendAdapter,
    cancellation: CancellationToken,
    implementation_attempts: Arc<AtomicU32>,
}

impl CancelDuringRetryAdapter {
    fn new(cancellation: CancellationToken) -> Self {
        Self {
            inner: StubBackendAdapter::default(),
            cancellation,
            implementation_attempts: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl AgentExecutionPort for CancelDuringRetryAdapter {
    async fn check_capability(
        &self,
        backend: &ralph_burning::shared::domain::ResolvedBackendTarget,
        stage_contract: &ralph_burning::contexts::workflow_composition::contracts::StageContract,
    ) -> AppResult<()> {
        self.inner.check_capability(backend, stage_contract).await
    }

    async fn check_availability(
        &self,
        backend: &ralph_burning::shared::domain::ResolvedBackendTarget,
    ) -> AppResult<()> {
        self.inner.check_availability(backend).await
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        if request.stage_contract.stage_id == StageId::Implementation {
            let attempt = self.implementation_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == 1 {
                self.cancellation.cancel();
                return Err(AppError::InvocationFailed {
                    backend: request.resolved_target.backend.family.to_string(),
                    stage_id: StageId::Implementation,
                    failure_class: FailureClass::TransportFailure,
                    details: "cancelled between implementation retries".to_owned(),
                });
            }
        }

        self.inner.invoke(request).await
    }

    async fn cancel(&self, invocation_id: &str) -> AppResult<()> {
        self.inner.cancel(invocation_id).await
    }
}

fn request_changes_payload(follow_ups: &[&str]) -> Value {
    json!({
        "outcome": "request_changes",
        "evidence": ["needs follow-up"],
        "findings_or_gaps": ["gap found"],
        "follow_up_or_amendments": follow_ups,
    })
}

fn approved_validation_payload() -> Value {
    json!({
        "outcome": "approved",
        "evidence": ["looks good"],
        "findings_or_gaps": [],
        "follow_up_or_amendments": [],
    })
}

fn conditionally_approved_payload(follow_ups: &[&str]) -> Value {
    json!({
        "outcome": "conditionally_approved",
        "evidence": ["conditionally good"],
        "findings_or_gaps": ["minor fix"],
        "follow_up_or_amendments": follow_ups,
    })
}

fn prompt_review_payload(ready: bool) -> Value {
    json!({
        "problem_framing": "Prompt review outcome",
        "assumptions_or_open_questions": ["captured"],
        "proposed_work": [
            {
                "order": 1,
                "summary": "Continue workflow",
                "details": "Deterministic prompt-review payload"
            }
        ],
        "readiness": {
            "ready": ready,
            "risks": ["prompt needs refinement"]
        }
    })
}

fn stage_events<'a>(
    events: &'a [JournalEvent],
    event_type: JournalEventType,
    stage_id: &str,
) -> Vec<&'a JournalEvent> {
    events
        .iter()
        .filter(|event| {
            event.event_type == event_type
                && event
                    .details
                    .get("stage_id")
                    .and_then(|value| value.as_str())
                    == Some(stage_id)
        })
        .collect()
}

#[tokio::test]
async fn retry_exhaustion_transitions_run_to_failed_state() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "retry-exhaustion");

    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::Implementation, 3),
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

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);
    assert!(snapshot.active_run.is_none());

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let implementation_entered =
        stage_events(&events, JournalEventType::StageEntered, "implementation");
    assert_eq!(implementation_entered.len(), 3);

    let implementation_failed =
        stage_events(&events, JournalEventType::StageFailed, "implementation");
    assert_eq!(implementation_failed.len(), 3);
    assert_eq!(implementation_failed[0].details["will_retry"], true);
    assert_eq!(implementation_failed[1].details["will_retry"], true);
    assert_eq!(implementation_failed[2].details["will_retry"], false);
}

#[tokio::test]
async fn retry_success_on_second_attempt_completes_run() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "retry-success");

    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::Implementation, 1),
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

    assert!(result.is_ok(), "{result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let implementation_entered =
        stage_events(&events, JournalEventType::StageEntered, "implementation");
    assert_eq!(implementation_entered.len(), 2);

    let implementation_failed =
        stage_events(&events, JournalEventType::StageFailed, "implementation");
    assert_eq!(implementation_failed.len(), 1);
    assert_eq!(implementation_failed[0].details["will_retry"], true);

    let implementation_completed =
        stage_events(&events, JournalEventType::StageCompleted, "implementation");
    assert_eq!(implementation_completed.len(), 1);
    assert_eq!(implementation_completed[0].details["attempt"], 2);
}

#[tokio::test]
async fn remediation_cycle_is_triggered_by_qa_request_changes() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "remediation-cycle");

    let adapter = RecordingAdapter::new(StubBackendAdapter::default().with_stage_payload_sequence(
        StageId::Qa,
        vec![
            request_changes_payload(&["add missing regression test"]),
            approved_validation_payload(),
        ],
    ));
    let adapter_handle = adapter.clone();
    let agent_service = build_agent_service_with_adapter(adapter);
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

    assert!(result.is_ok(), "{result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let implementation_entered =
        stage_events(&events, JournalEventType::StageEntered, "implementation");
    assert_eq!(implementation_entered.len(), 2);

    let cycle_advanced: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::CycleAdvanced)
        .collect();
    assert_eq!(cycle_advanced.len(), 1);
    assert_eq!(cycle_advanced[0].details["to_cycle"], 2);

    let implementation_contexts = adapter_handle.contexts_for(StageId::Implementation);
    assert_eq!(implementation_contexts.len(), 2);
    assert_eq!(
        implementation_contexts[1]["remediation"]["follow_up_or_amendments"][0],
        "add missing regression test"
    );
}

#[tokio::test]
async fn remediation_limit_exceeded_fails_the_run() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "remediation-limit");

    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_stage_payload_sequence(
            StageId::Qa,
            vec![
                request_changes_payload(&["cycle-1"]),
                request_changes_payload(&["cycle-2"]),
                request_changes_payload(&["cycle-3"]),
            ],
        ),
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
    let error = result.unwrap_err().to_string();
    assert!(error.contains("remediation exhausted"), "{error}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);
    assert!(snapshot.active_run.is_none());

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let cycle_advanced: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::CycleAdvanced)
        .collect();
    assert_eq!(cycle_advanced.len(), 2);
}

#[tokio::test]
async fn resume_after_cycle_advanced_append_failure_restarts_at_implementation() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "resume-remediation-boundary");
    let config = EffectiveConfig::load(base_dir).unwrap();

    let adapter = RecordingAdapter::new(StubBackendAdapter::default().with_stage_payload_sequence(
        StageId::Qa,
        vec![
            request_changes_payload(&["carry remediation into cycle two"]),
            approved_validation_payload(),
        ],
    ));
    let adapter_handle = adapter.clone();
    let agent_service = build_agent_service_with_adapter(adapter);

    // Append order before the remediation handoff:
    //   1 run_started
    //   2 stage_entered(prompt_review)
    //   3 stage_completed(prompt_review)
    //   4 stage_entered(planning)
    //   5 stage_completed(planning)
    //   6 stage_entered(implementation)
    //   7 stage_completed(implementation)
    //   8 stage_entered(qa)
    //   9 stage_completed(qa)
    //  10 cycle_advanced -> fail here
    let failing_journal = FailingJournalStore::new(10);

    let first_result = engine::execute_standard_run(
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
    assert!(first_result.is_err());

    let failed_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(failed_snapshot.status, RunStatus::Failed);
    assert!(failed_snapshot.active_run.is_none());
    assert_eq!(failed_snapshot.cycle_history.last().unwrap().cycle, 2);

    let resume_result = engine::resume_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        base_dir,
        &pid,
        &config,
    )
    .await;
    assert!(resume_result.is_ok(), "{resume_result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let implementation_entered =
        stage_events(&events, JournalEventType::StageEntered, "implementation");
    assert_eq!(
        implementation_entered.len(),
        2,
        "resume must loop back through implementation for remediation"
    );

    let run_resumed = events
        .iter()
        .find(|event| event.event_type == JournalEventType::RunResumed)
        .expect("run_resumed");
    assert_eq!(run_resumed.details["resume_stage"], "implementation");
    assert_eq!(run_resumed.details["cycle"], 2);

    let implementation_contexts = adapter_handle.contexts_for(StageId::Implementation);
    assert_eq!(implementation_contexts.len(), 2);
    assert_eq!(
        implementation_contexts[1]["remediation"]["follow_up_or_amendments"][0],
        "carry remediation into cycle two"
    );
    assert_eq!(
        implementation_contexts[1]["remediation"]["source_stage"],
        "qa"
    );
}

#[tokio::test]
async fn resume_from_failed_run_skips_completed_stages() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "resume-failed");
    let config = EffectiveConfig::load(base_dir).unwrap();

    let failing_agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::Implementation, 3),
    );
    let first_result = engine::execute_standard_run(
        &failing_agent_service,
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
    assert!(first_result.is_err());

    let resume_agent_service = build_agent_service();
    let resume_result = engine::resume_standard_run(
        &resume_agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        base_dir,
        &pid,
        &config,
    )
    .await;
    assert!(resume_result.is_ok(), "{resume_result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let planning_entered = stage_events(&events, JournalEventType::StageEntered, "planning");
    assert_eq!(
        planning_entered.len(),
        1,
        "planning should not rerun on resume"
    );

    let implementation_entered =
        stage_events(&events, JournalEventType::StageEntered, "implementation");
    assert_eq!(implementation_entered.len(), 4);

    let run_started = events
        .iter()
        .find(|event| event.event_type == JournalEventType::RunStarted)
        .expect("run_started");
    let run_resumed = events
        .iter()
        .find(|event| event.event_type == JournalEventType::RunResumed)
        .expect("run_resumed");
    assert_eq!(run_started.details["run_id"], run_resumed.details["run_id"]);
    assert_eq!(run_resumed.details["resume_stage"], "implementation");
}

#[tokio::test]
async fn resume_from_paused_prompt_review_run_continues_from_planning() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "resume-paused");
    let config = EffectiveConfig::load(base_dir).unwrap();

    let paused_agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default()
            .with_stage_payload(StageId::PromptReview, prompt_review_payload(false)),
    );
    let first_result = engine::execute_standard_run(
        &paused_agent_service,
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
    assert!(first_result.is_ok(), "{first_result:?}");

    let paused_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(paused_snapshot.status, RunStatus::Paused);
    assert!(paused_snapshot.active_run.is_none());
    assert!(paused_snapshot.status_summary.contains("run resume"));

    let resume_agent_service = build_agent_service();
    let resume_result = engine::resume_standard_run(
        &resume_agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        base_dir,
        &pid,
        &config,
    )
    .await;
    assert!(resume_result.is_ok(), "{resume_result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert_eq!(
        stage_events(&events, JournalEventType::StageEntered, "prompt_review").len(),
        1
    );
    assert_eq!(
        stage_events(&events, JournalEventType::StageEntered, "planning").len(),
        1
    );

    let run_resumed = events
        .iter()
        .find(|event| event.event_type == JournalEventType::RunResumed)
        .expect("run_resumed");
    assert_eq!(run_resumed.details["resume_stage"], "planning");
}

#[tokio::test]
async fn cancellation_halts_retry_loop() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cancel-retry");

    let cancellation = CancellationToken::new();
    let agent_service =
        build_agent_service_with_adapter(CancelDuringRetryAdapter::new(cancellation.clone()));
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_standard_run_with_retry(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        base_dir,
        &pid,
        &config,
        &ralph_burning::contexts::workflow_composition::retry_policy::RetryPolicy::default_policy(),
        cancellation,
    )
    .await;

    assert!(result.is_err());

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);
    assert!(snapshot.active_run.is_none());

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let implementation_entered =
        stage_events(&events, JournalEventType::StageEntered, "implementation");
    assert_eq!(implementation_entered.len(), 1);

    let implementation_failed =
        stage_events(&events, JournalEventType::StageFailed, "implementation");
    assert_eq!(implementation_failed.len(), 1);
    assert_eq!(implementation_failed[0].details["will_retry"], false);
}

#[tokio::test]
async fn conditionally_approved_queues_amendments_and_proceeds() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "conditional-review");

    let agent_service =
        build_agent_service_with_adapter(StubBackendAdapter::default().with_stage_payload(
            StageId::Review,
            conditionally_approved_payload(&["tighten the acceptance note", "add one QA case"]),
        ));
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

    assert!(result.is_ok(), "{result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert_eq!(
        snapshot.amendment_queue.pending,
        vec![
            json!("tighten the acceptance note"),
            json!("add one QA case")
        ]
    );
}
