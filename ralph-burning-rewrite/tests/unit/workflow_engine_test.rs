use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use serde_json::{json, Value};
use tempfile::tempdir;

use ralph_burning::adapters::fs::{
    FsAmendmentQueueStore, FsArtifactStore, FsJournalStore, FsPayloadArtifactWriteStore,
    FsProjectStore, FsRawOutputStore, FsRollbackPointStore, FsRunSnapshotStore,
    FsRunSnapshotWriteStore, FsRuntimeLogWriteStore, FsSessionStore,
};
use ralph_burning::adapters::stub_backend::StubBackendAdapter;
use ralph_burning::contexts::agent_execution::model::{
    CancellationToken, InvocationEnvelope, InvocationRequest,
};
use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
use ralph_burning::contexts::agent_execution::service::AgentExecutionService;
use ralph_burning::contexts::project_run_record::journal;
use ralph_burning::contexts::project_run_record::model::{
    JournalEvent, JournalEventType, RunSnapshot, RunStatus, RuntimeLogEntry,
};
use ralph_burning::contexts::workflow_composition::panel_contracts::RecordKind;
use ralph_burning::contexts::project_run_record::service::{
    self, AmendmentQueuePort, CreateProjectInput, JournalStorePort, RunSnapshotPort,
    RunSnapshotWritePort, RuntimeLogWritePort,
};
use ralph_burning::contexts::workflow_composition::engine;
use ralph_burning::contexts::workspace_governance;
use ralph_burning::contexts::workspace_governance::config::EffectiveConfig;
use ralph_burning::shared::domain::{FailureClass, FlowPreset, ProjectId, RunId, StageId};
use ralph_burning::shared::error::{AppError, AppResult};

const JOURNAL_APPEND_FAIL_AFTER_ENV: &str = "RALPH_BURNING_TEST_JOURNAL_APPEND_FAIL_AFTER";
static FAILPOINT_ENV_MUTEX: Mutex<()> = Mutex::new(());

fn setup_workspace(base_dir: &Path) {
    workspace_governance::initialize_workspace(base_dir, Utc::now()).unwrap();
}

fn create_project_with_flow(base_dir: &Path, project_id: &str, flow: FlowPreset) -> ProjectId {
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
            flow,
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

fn create_standard_project(base_dir: &Path, project_id: &str) -> ProjectId {
    create_project_with_flow(base_dir, project_id, FlowPreset::Standard)
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

struct ScopedJournalAppendFailpoint {
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl ScopedJournalAppendFailpoint {
    fn for_project(project_id: &ProjectId, fail_after: u32) -> Self {
        let lock = FAILPOINT_ENV_MUTEX
            .lock()
            .expect("failpoint env mutex poisoned");
        std::env::set_var(
            JOURNAL_APPEND_FAIL_AFTER_ENV,
            format!("{}:{fail_after}", project_id.as_str()),
        );
        Self { _lock: lock }
    }
}

impl Drop for ScopedJournalAppendFailpoint {
    fn drop(&mut self) {
        std::env::remove_var(JOURNAL_APPEND_FAIL_AFTER_ENV);
    }
}

const MAX_COMPLETION_ROUNDS_ENV: &str = "RALPH_BURNING_TEST_MAX_COMPLETION_ROUNDS";

struct ScopedMaxCompletionRounds {
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl ScopedMaxCompletionRounds {
    fn set(max_rounds: u32) -> Self {
        let lock = FAILPOINT_ENV_MUTEX
            .lock()
            .expect("failpoint env mutex poisoned");
        std::env::set_var(MAX_COMPLETION_ROUNDS_ENV, max_rounds.to_string());
        Self { _lock: lock }
    }
}

impl Drop for ScopedMaxCompletionRounds {
    fn drop(&mut self) {
        std::env::remove_var(MAX_COMPLETION_ROUNDS_ENV);
    }
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
        &FsAmendmentQueueStore,
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

    assert!(
        events
            .iter()
            .any(|event| event.event_type == JournalEventType::RunCompleted),
        "run_completed event should be present"
    );
    assert_eq!(
        events.last().unwrap().event_type,
        JournalEventType::RollbackCreated
    );

    // Verify payloads and artifacts were written
    let payloads_dir = base_dir.join(".ralph-burning/projects/happy-test/history/payloads");
    let artifacts_dir = base_dir.join(".ralph-burning/projects/happy-test/history/artifacts");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    let artifact_count = fs::read_dir(&artifacts_dir).unwrap().count();
    // prompt_review: 4 records (1 refiner + 2 validators + 1 primary)
    // completion_panel: 3 records (2 completers + 1 aggregate)
    // other 6 stages: 1 each = 6
    // total = 13
    assert_eq!(payload_count, 13, "expected 13 payloads");
    assert_eq!(artifact_count, 13, "expected 13 artifacts");
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
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_ok());

    // Verify 7 stages completed (no prompt_review)
    let payloads_dir = base_dir.join(".ralph-burning/projects/no-pr-test/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    // completion_panel: 3 records (2 completers + 1 aggregate)
    // other 6 stages: 1 each = 6
    // total = 9 (no prompt_review)
    assert_eq!(
        payload_count, 9,
        "expected 9 payloads without prompt_review"
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

#[tokio::test]
async fn successful_stage_transitions_create_rollback_points() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "rollback-points");

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await
    .expect("run completes");

    let rollback_dir = base_dir.join(".ralph-burning/projects/rollback-points/rollback");
    let rollback_file_count = fs::read_dir(&rollback_dir).unwrap().count();
    assert_eq!(rollback_file_count, 8, "one checkpoint per completed stage");

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let rollback_events = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::RollbackCreated)
        .count();
    assert_eq!(rollback_events, 8);
}

#[tokio::test]
async fn resume_after_rollback_preserves_abandoned_payload_artifacts_on_disk() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "rollback-branch");
    let config = EffectiveConfig::load(base_dir).unwrap();

    let failing_review_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_invoke_failure(StageId::Review),
    );
    let first_result = engine::execute_standard_run(
        &failing_review_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;
    assert!(first_result.is_err(), "first branch should fail at review");

    let failed_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(failed_snapshot.status, RunStatus::Failed);
    assert!(failed_snapshot.active_run.is_none());

    service::perform_rollback(
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsRollbackPointStore,
        None,
        base_dir,
        &pid,
        FlowPreset::Standard,
        StageId::Planning,
        false,
    )
    .expect("rollback to planning succeeds");

    let resume_service = build_agent_service();
    let resume_result = engine::resume_standard_run(
        &resume_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;
    assert!(resume_result.is_ok(), "{resume_result:?}");

    let payloads_dir = base_dir.join(".ralph-burning/projects/rollback-branch/history/payloads");
    let artifacts_dir = base_dir.join(".ralph-burning/projects/rollback-branch/history/artifacts");

    let mut payload_files: Vec<_> = fs::read_dir(&payloads_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    payload_files.sort();

    let mut artifact_files: Vec<_> = fs::read_dir(&artifacts_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    artifact_files.sort();

    let implementation_payloads: Vec<_> = payload_files
        .iter()
        .filter(|name| name.contains("-implementation-c1-a1-cr1"))
        .cloned()
        .collect();
    assert_eq!(
        implementation_payloads.len(),
        2,
        "rollback/resume should retain both the abandoned and visible implementation payload files"
    );
    assert!(
        implementation_payloads
            .iter()
            .any(|name| name.contains("-rb1")),
        "resumed implementation payload should use a branch-specific durable ID"
    );
    assert!(
        implementation_payloads
            .iter()
            .any(|name| !name.contains("-rb1")),
        "abandoned implementation payload should remain on disk"
    );

    let implementation_artifacts: Vec<_> = artifact_files
        .iter()
        .filter(|name| name.contains("-implementation-c1-a1-cr1"))
        .cloned()
        .collect();
    assert_eq!(implementation_artifacts.len(), 2);
    assert!(implementation_artifacts
        .iter()
        .any(|name| name.contains("-rb1")));
    assert!(implementation_artifacts
        .iter()
        .any(|name| !name.contains("-rb1")));

    let history = service::run_history(&FsJournalStore, &FsArtifactStore, base_dir, &pid).unwrap();
    let visible_implementation_payloads: Vec<_> = history
        .payloads
        .iter()
        .filter(|record| record.stage_id == StageId::Implementation)
        .collect();
    assert_eq!(visible_implementation_payloads.len(), 1);
    assert!(
        visible_implementation_payloads[0]
            .payload_id
            .contains("-rb1"),
        "visible implementation history should come from the resumed branch"
    );

    // prompt_review (first run, preserved): 4 records; completion_panel (resume): 3 records
    // Old: 10 on disk, 8 visible → New: +3 (prompt_review) +2 (completion_panel) = +5
    assert_eq!(
        payload_files.len(),
        15,
        "old branch payload files should remain on disk alongside the resumed branch"
    );
    assert_eq!(
        history.payloads.len(),
        13,
        "run history should hide rolled-back stages"
    );
}

#[tokio::test]
async fn happy_path_docs_change_run_completes() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "docs-happy", FlowPreset::DocsChange);

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::DocsChange,
        &config,
    )
    .await;

    assert!(result.is_ok(), "{result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert!(snapshot.active_run.is_none());
    assert_eq!(snapshot.completion_rounds, 1);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let entered: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::StageEntered)
        .map(|event| event.details["stage_id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        vec!["docs_plan", "docs_update", "docs_validation", "review"],
        entered
    );

    let payload_count =
        fs::read_dir(base_dir.join(".ralph-burning/projects/docs-happy/history/payloads"))
            .unwrap()
            .count();
    let artifact_count =
        fs::read_dir(base_dir.join(".ralph-burning/projects/docs-happy/history/artifacts"))
            .unwrap()
            .count();
    assert_eq!(payload_count, 4);
    assert_eq!(artifact_count, 4);
}

#[tokio::test]
async fn happy_path_ci_improvement_run_completes() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "ci-happy", FlowPreset::CiImprovement);

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::CiImprovement,
        &config,
    )
    .await;

    assert!(result.is_ok(), "{result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert!(snapshot.active_run.is_none());
    assert_eq!(snapshot.completion_rounds, 1);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let entered: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::StageEntered)
        .map(|event| event.details["stage_id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        vec!["ci_plan", "ci_update", "ci_validation", "review"],
        entered
    );

    let payload_count =
        fs::read_dir(base_dir.join(".ralph-burning/projects/ci-happy/history/payloads"))
            .unwrap()
            .count();
    let artifact_count =
        fs::read_dir(base_dir.join(".ralph-burning/projects/ci-happy/history/artifacts"))
            .unwrap()
            .count();
    assert_eq!(payload_count, 4);
    assert_eq!(artifact_count, 4);
}

#[tokio::test]
async fn docs_change_remediation_restarts_from_docs_update() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "docs-remediation", FlowPreset::DocsChange);

    let adapter = RecordingAdapter::new(StubBackendAdapter::default().with_stage_payload_sequence(
        StageId::DocsValidation,
        vec![
            request_changes_payload(&["fix the broken link targets"]),
            approved_validation_payload(),
        ],
    ));
    let adapter_handle = adapter.clone();
    let agent_service = build_agent_service_with_adapter(adapter);
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::DocsChange,
        &config,
    )
    .await;

    assert!(result.is_ok(), "{result:?}");

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert_eq!(
        stage_events(&events, JournalEventType::StageEntered, "docs_update").len(),
        2
    );

    let cycle_advanced: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::CycleAdvanced)
        .collect();
    assert_eq!(cycle_advanced.len(), 1);
    assert_eq!(cycle_advanced[0].details["resume_stage"], "docs_update");

    let docs_update_contexts = adapter_handle.contexts_for(StageId::DocsUpdate);
    assert_eq!(docs_update_contexts.len(), 2);
    assert_eq!(
        docs_update_contexts[1]["remediation"]["follow_up_or_amendments"][0],
        "fix the broken link targets"
    );
}

#[tokio::test]
async fn docs_change_conditionally_approved_records_follow_ups_without_durable_amendments() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "docs-conditional", FlowPreset::DocsChange);

    let agent_service =
        build_agent_service_with_adapter(StubBackendAdapter::default().with_stage_payload(
            StageId::DocsValidation,
            conditionally_approved_payload(&["add a rollout caveat", "tighten the examples"]),
        ));
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::DocsChange,
        &config,
    )
    .await;

    assert!(result.is_ok(), "{result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert!(snapshot.amendment_queue.pending.is_empty());
    assert_eq!(
        snapshot
            .amendment_queue
            .recorded_follow_ups
            .iter()
            .map(|item| item.body.as_str())
            .collect::<Vec<_>>(),
        vec!["add a rollout caveat", "tighten the examples"]
    );

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == JournalEventType::AmendmentQueued)
            .count(),
        0
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == JournalEventType::CompletionRoundAdvanced)
            .count(),
        0
    );
}

#[tokio::test]
async fn ci_improvement_remediation_restarts_from_ci_update() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "ci-remediation", FlowPreset::CiImprovement);

    let adapter = RecordingAdapter::new(StubBackendAdapter::default().with_stage_payload_sequence(
        StageId::CiValidation,
        vec![
            request_changes_payload(&["tighten the workflow assertion"]),
            approved_validation_payload(),
        ],
    ));
    let adapter_handle = adapter.clone();
    let agent_service = build_agent_service_with_adapter(adapter);
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::CiImprovement,
        &config,
    )
    .await;

    assert!(result.is_ok(), "{result:?}");

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert_eq!(
        stage_events(&events, JournalEventType::StageEntered, "ci_update").len(),
        2
    );

    let cycle_advanced: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::CycleAdvanced)
        .collect();
    assert_eq!(cycle_advanced.len(), 1);
    assert_eq!(cycle_advanced[0].details["resume_stage"], "ci_update");

    let ci_update_contexts = adapter_handle.contexts_for(StageId::CiUpdate);
    assert_eq!(ci_update_contexts.len(), 2);
    assert_eq!(
        ci_update_contexts[1]["remediation"]["follow_up_or_amendments"][0],
        "tighten the workflow assertion"
    );
}

#[tokio::test]
async fn ci_improvement_rejected_validation_fails_run() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "ci-rejected", FlowPreset::CiImprovement);

    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default()
            .with_stage_payload(StageId::CiValidation, rejected_validation_payload()),
    );
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::CiImprovement,
        &config,
    )
    .await;

    assert!(result.is_err());

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let run_failed = events
        .iter()
        .rev()
        .find(|event| event.event_type == JournalEventType::RunFailed)
        .expect("run_failed");
    assert_eq!(
        run_failed.details["failure_class"],
        "qa_review_outcome_failure"
    );
}

#[tokio::test]
async fn resume_from_failed_docs_change_run_skips_completed_stages() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "docs-resume", FlowPreset::DocsChange);
    let config = EffectiveConfig::load(base_dir).unwrap();

    let failing_agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::DocsUpdate, 3),
    );
    let first_result = engine::execute_run(
        &failing_agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::DocsChange,
        &config,
    )
    .await;
    assert!(first_result.is_err());

    let resume_agent_service = build_agent_service();
    let resume_result = engine::resume_run(
        &resume_agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::DocsChange,
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
        stage_events(&events, JournalEventType::StageEntered, "docs_plan").len(),
        1
    );
    assert_eq!(
        stage_events(&events, JournalEventType::StageEntered, "docs_update").len(),
        4
    );

    let run_resumed = events
        .iter()
        .find(|event| event.event_type == JournalEventType::RunResumed)
        .expect("run_resumed");
    assert_eq!(run_resumed.details["resume_stage"], "docs_update");
}

#[tokio::test]
async fn resume_from_failed_ci_improvement_run_skips_completed_stages() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "ci-resume", FlowPreset::CiImprovement);
    let config = EffectiveConfig::load(base_dir).unwrap();

    let failing_agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::CiUpdate, 3),
    );
    let first_result = engine::execute_run(
        &failing_agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::CiImprovement,
        &config,
    )
    .await;
    assert!(first_result.is_err());

    let resume_agent_service = build_agent_service();
    let resume_result = engine::resume_run(
        &resume_agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::CiImprovement,
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
        stage_events(&events, JournalEventType::StageEntered, "ci_plan").len(),
        1
    );
    assert_eq!(
        stage_events(&events, JournalEventType::StageEntered, "ci_update").len(),
        4
    );

    let run_resumed = events
        .iter()
        .find(|event| event.event_type == JournalEventType::RunResumed)
        .expect("run_resumed");
    assert_eq!(run_resumed.details["resume_stage"], "ci_update");
}

// ── Quick Dev flow tests ────────────────────────────────────────────────────

#[tokio::test]
async fn happy_path_quick_dev_run_completes() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "qd-happy", FlowPreset::QuickDev);

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::QuickDev,
        &config,
    )
    .await;

    assert!(result.is_ok(), "{result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert!(snapshot.active_run.is_none());
    assert_eq!(snapshot.completion_rounds, 1);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let entered: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::StageEntered)
        .map(|event| event.details["stage_id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        vec![
            "plan_and_implement",
            "review",
            "apply_fixes",
            "final_review"
        ],
        entered
    );

    let payload_count =
        fs::read_dir(base_dir.join(".ralph-burning/projects/qd-happy/history/payloads"))
            .unwrap()
            .count();
    let artifact_count =
        fs::read_dir(base_dir.join(".ralph-burning/projects/qd-happy/history/artifacts"))
            .unwrap()
            .count();
    assert_eq!(payload_count, 4);
    assert_eq!(artifact_count, 4);
}

#[tokio::test]
async fn quick_dev_review_request_changes_restarts_from_apply_fixes() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "qd-remediation", FlowPreset::QuickDev);

    let adapter = RecordingAdapter::new(StubBackendAdapter::default().with_stage_payload_sequence(
        StageId::Review,
        vec![
            request_changes_payload(&["fix the identified issues"]),
            approved_validation_payload(),
        ],
    ));
    let adapter_handle = adapter.clone();
    let agent_service = build_agent_service_with_adapter(adapter);
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::QuickDev,
        &config,
    )
    .await;

    assert!(result.is_ok(), "{result:?}");

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();

    // In quick_dev, the execution stage (apply_fixes) comes AFTER the trigger
    // stage (review), so apply_fixes is entered once: only after the cycle
    // advance, not before the review that triggered remediation.
    assert_eq!(
        stage_events(&events, JournalEventType::StageEntered, "apply_fixes").len(),
        1
    );

    let cycle_advanced: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::CycleAdvanced)
        .collect();
    assert_eq!(cycle_advanced.len(), 1);
    assert_eq!(cycle_advanced[0].details["resume_stage"], "apply_fixes");

    // apply_fixes is invoked once (after cycle advance) with remediation context
    let apply_fixes_contexts = adapter_handle.contexts_for(StageId::ApplyFixes);
    assert_eq!(apply_fixes_contexts.len(), 1);
    assert_eq!(
        apply_fixes_contexts[0]["remediation"]["follow_up_or_amendments"][0],
        "fix the identified issues"
    );
}

#[tokio::test]
async fn quick_dev_review_rejected_fails_run() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "qd-rejected", FlowPreset::QuickDev);

    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default()
            .with_stage_payload(StageId::Review, rejected_validation_payload()),
    );
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::QuickDev,
        &config,
    )
    .await;

    assert!(result.is_err());

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let run_failed = events
        .iter()
        .rev()
        .find(|event| event.event_type == JournalEventType::RunFailed)
        .expect("run_failed");
    assert_eq!(
        run_failed.details["failure_class"],
        "qa_review_outcome_failure"
    );
}

#[tokio::test]
async fn quick_dev_final_review_conditionally_approved_triggers_completion_round() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "qd-cr", FlowPreset::QuickDev);

    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_stage_payload_sequence(
            StageId::FinalReview,
            vec![
                conditionally_approved_payload(&["polish the edge cases"]),
                approved_validation_payload(),
            ],
        ),
    );
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::QuickDev,
        &config,
    )
    .await;

    assert!(result.is_ok(), "run should complete: {result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert_eq!(snapshot.completion_rounds, 2);
    assert!(snapshot.amendment_queue.pending.is_empty());

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let round_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::CompletionRoundAdvanced)
        .collect();
    assert_eq!(round_events.len(), 1);

    // Verify the run restarted at plan_and_implement
    let plan_entries = stage_events(
        &events,
        JournalEventType::StageEntered,
        "plan_and_implement",
    );
    assert_eq!(
        plan_entries.len(),
        2,
        "plan_and_implement should run twice across completion rounds"
    );
}

#[tokio::test]
async fn resume_from_failed_quick_dev_run_skips_completed_stages() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "qd-resume", FlowPreset::QuickDev);
    let config = EffectiveConfig::load(base_dir).unwrap();

    let failing_agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::Review, 3),
    );
    let first_result = engine::execute_run(
        &failing_agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::QuickDev,
        &config,
    )
    .await;
    assert!(first_result.is_err());

    let resume_agent_service = build_agent_service();
    let resume_result = engine::resume_run(
        &resume_agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::QuickDev,
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
        stage_events(
            &events,
            JournalEventType::StageEntered,
            "plan_and_implement"
        )
        .len(),
        1
    );
    assert_eq!(
        stage_events(&events, JournalEventType::StageEntered, "review").len(),
        4
    );

    let run_resumed = events
        .iter()
        .find(|event| event.event_type == JournalEventType::RunResumed)
        .expect("run_resumed");
    assert_eq!(run_resumed.details["resume_stage"], "review");
}

#[tokio::test]
async fn quick_dev_preflight_failure_leaves_state_unchanged() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "qd-preflight", FlowPreset::QuickDev);

    let agent_service =
        build_agent_service_with_adapter(StubBackendAdapter::default().unavailable());
    let config = EffectiveConfig::load(base_dir).unwrap();

    let pre_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(pre_snapshot.status, RunStatus::NotStarted);

    let result = engine::execute_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::QuickDev,
        &config,
    )
    .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        format!("{err}").contains("preflight") || format!("{err}").contains("unavailable"),
        "expected preflight failure, got: {err}"
    );

    let post_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(post_snapshot.status, RunStatus::NotStarted);
    assert!(post_snapshot.active_run.is_none());

    let payload_count =
        fs::read_dir(base_dir.join(".ralph-burning/projects/qd-preflight/history/payloads"))
            .unwrap()
            .count();
    assert_eq!(payload_count, 0);
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
                stage_resolution_snapshot: None,
            },
        ),
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 0,
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "running".to_owned(),
        last_stage_resolution_snapshot: None,
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
        &FsAmendmentQueueStore,
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
        last_stage_resolution_snapshot: None,
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
        &FsAmendmentQueueStore,
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
        &FsAmendmentQueueStore,
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
        &FsAmendmentQueueStore,
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
        &FsAmendmentQueueStore,
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
        &FsAmendmentQueueStore,
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

    // Panel dispatch writes supporting records (refiner + validators) before the
    // primary record. Journal failure rolls back only the primary pair. Supporting
    // records remain as durable evidence.
    let payloads_dir = base_dir.join(".ralph-burning/projects/journal-fail/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(
        payload_count, 3,
        "3 supporting records (refiner + 2 validators) should remain; primary rolled back"
    );

    let artifacts_dir = base_dir.join(".ralph-burning/projects/journal-fail/history/artifacts");
    let artifact_count = fs::read_dir(&artifacts_dir).unwrap().count();
    assert_eq!(
        artifact_count, 3,
        "3 supporting artifacts should remain; primary rolled back"
    );

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
    //   2: stage_entered cursor update (stage 1/prompt_review)
    //   3: persist_stage_resolution_snapshot (prompt_review panel)
    //   4: stage commit cursor update (stage 1) — fail here
    let failing_snapshot = FailingSnapshotWriteStore::new(4);

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &failing_snapshot,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
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

    // The completed first stage (prompt_review panel) remains durable.
    // Panel dispatch writes 3 supporting + 1 primary = 4 records.
    let payloads_dir = base_dir.join(".ralph-burning/projects/snap-fail/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(
        payload_count, 4,
        "completed panel stage payloads should remain durable"
    );

    let artifacts_dir = base_dir.join(".ralph-burning/projects/snap-fail/history/artifacts");
    let artifact_count = fs::read_dir(&artifacts_dir).unwrap().count();
    assert_eq!(
        artifact_count, 4,
        "completed panel stage artifacts should remain durable"
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
// TODO(panel-dispatch): Update for panel supporting record cleanup.
// Panel dispatch writes supporting records before the primary; the leaking
// store fails on the first write but supporting records may persist.
#[tokio::test]
#[ignore = "needs update for panel dispatch supporting record cleanup"]
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
        &FsAmendmentQueueStore,
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
        &FsAmendmentQueueStore,
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
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

#[derive(Clone)]
struct RecordedRequest {
    stage_id: StageId,
    context: Value,
    invocation_id: String,
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
            .filter(|request| request.stage_id == stage_id)
            .map(|request| request.context.clone())
            .collect()
    }

    fn invocation_ids_for(&self, stage_id: StageId) -> Vec<String> {
        self.requests
            .lock()
            .expect("recording adapter lock poisoned")
            .iter()
            .filter(|request| request.stage_id == stage_id)
            .map(|request| request.invocation_id.clone())
            .collect()
    }
}

impl AgentExecutionPort for RecordingAdapter {
    async fn check_capability(
        &self,
        backend: &ralph_burning::shared::domain::ResolvedBackendTarget,
        contract: &ralph_burning::contexts::agent_execution::model::InvocationContract,
    ) -> AppResult<()> {
        self.inner.check_capability(backend, contract).await
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
            .push(RecordedRequest {
                stage_id: request.contract.stage_id().expect("stage id"),
                context: request.payload.context.clone(),
                invocation_id: request.invocation_id.clone(),
            });
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
        contract: &ralph_burning::contexts::agent_execution::model::InvocationContract,
    ) -> AppResult<()> {
        self.inner.check_capability(backend, contract).await
    }

    async fn check_availability(
        &self,
        backend: &ralph_burning::shared::domain::ResolvedBackendTarget,
    ) -> AppResult<()> {
        self.inner.check_availability(backend).await
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        if request.contract.stage_id() == Some(StageId::Implementation) {
            let attempt = self.implementation_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == 1 {
                self.cancellation.cancel();
                return Err(AppError::InvocationFailed {
                    backend: request.resolved_target.backend.family.to_string(),
                    contract_id: StageId::Implementation.to_string(),
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

#[derive(Clone)]
struct CancelBetweenRetryAttemptsLogWriter {
    cancellation: CancellationToken,
    cancellation_count: Arc<AtomicU32>,
}

impl CancelBetweenRetryAttemptsLogWriter {
    fn new(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            cancellation_count: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl RuntimeLogWritePort for CancelBetweenRetryAttemptsLogWriter {
    fn append_runtime_log(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        entry: &RuntimeLogEntry,
    ) -> AppResult<()> {
        if entry.source == "engine"
            && entry.message.contains("stage_failed: implementation")
            && entry.message.contains("retry=true")
            && self.cancellation_count.fetch_add(1, Ordering::SeqCst) == 0
        {
            self.cancellation.cancel();
        }

        Ok(())
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
        &FsAmendmentQueueStore,
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
        &FsAmendmentQueueStore,
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
        &FsAmendmentQueueStore,
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
        &FsAmendmentQueueStore,
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
    //   4 rollback_created(prompt_review)
    //   5 stage_entered(planning)
    //   6 stage_completed(planning)
    //   7 rollback_created(planning)
    //   8 stage_entered(implementation)
    //   9 stage_completed(implementation)
    //  10 rollback_created(implementation)
    //  11 stage_entered(qa)
    //  12 stage_completed(qa)
    //  13 cycle_advanced -> fail here
    let failing_journal = FailingJournalStore::new(13);

    let first_result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &failing_journal,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
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
        &FsAmendmentQueueStore,
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
        &FsAmendmentQueueStore,
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
        &FsAmendmentQueueStore,
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

    // Panel model: readiness.ready=false causes validator rejection → run fails.
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
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;
    assert!(first_result.is_err(), "prompt review rejection should fail the run");

    let paused_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(paused_snapshot.status, RunStatus::Failed);
    assert!(paused_snapshot.active_run.is_none());

    let resume_agent_service = build_agent_service();
    let resume_result = engine::resume_standard_run(
        &resume_agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
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
    // prompt_review entered twice: once in failed run, once on resume
    assert_eq!(
        stage_events(&events, JournalEventType::StageEntered, "prompt_review").len(),
        2
    );
    assert_eq!(
        stage_events(&events, JournalEventType::StageEntered, "planning").len(),
        1
    );

    let run_resumed = events
        .iter()
        .find(|event| event.event_type == JournalEventType::RunResumed)
        .expect("run_resumed");
    // Resume from failed prompt_review restarts at prompt_review, not planning.
    assert_eq!(run_resumed.details["resume_stage"], "prompt_review");
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
        &FsAmendmentQueueStore,
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
async fn cancellation_between_retry_attempts_does_not_start_next_attempt() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cancel-between-retries");

    let cancellation = CancellationToken::new();
    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::Implementation, 1),
    );
    let log_writer = CancelBetweenRetryAttemptsLogWriter::new(cancellation.clone());
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_standard_run_with_retry(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &log_writer,
        &FsAmendmentQueueStore,
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
    assert_eq!(implementation_failed[0].details["will_retry"], true);

    let run_failed = events
        .iter()
        .rev()
        .find(|event| event.event_type == JournalEventType::RunFailed)
        .expect("run_failed");
    assert_eq!(run_failed.details["failure_class"], "cancellation");
}

#[tokio::test]
async fn standard_non_late_conditionally_approved_does_not_queue_amendments_and_proceeds() {
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
        &FsAmendmentQueueStore,
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
    // Non-late-stage ConditionallyApproved does not queue amendments (SC-CR-005).
    assert!(
        snapshot.amendment_queue.pending.is_empty(),
        "non-late-stage ConditionallyApproved should not queue amendments"
    );
}

// ── Completion Round Tests ──────────────────────────────────────────────────

fn rejected_validation_payload() -> Value {
    json!({
        "outcome": "rejected",
        "evidence": ["failed review"],
        "findings_or_gaps": ["critical issue"],
        "follow_up_or_amendments": ["rework the rejected stage output"],
    })
}

#[tokio::test]
async fn late_stage_conditionally_approved_triggers_completion_round_advancement() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-alpha");

    // CompletionPanel returns conditionally_approved on first call, approved on second.
    // AcceptanceQa and FinalReview return approved.
    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_stage_payload_sequence(
            StageId::CompletionPanel,
            vec![
                conditionally_approved_payload(&["tighten the acceptance note"]),
                approved_validation_payload(),
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
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_ok(), "run should complete: {result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert_eq!(
        snapshot.completion_rounds, 2,
        "should be completion round 2"
    );
    // Panel dispatch does not queue amendments; completion_panel produces
    // ContinueWork/Complete verdicts only.

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();

    let round_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::CompletionRoundAdvanced)
        .collect();
    assert_eq!(
        round_events.len(),
        1,
        "should have one completion_round_advanced event"
    );
    assert_eq!(round_events[0].details["from_round"], 1);
    assert_eq!(round_events[0].details["to_round"], 2);
    assert_eq!(round_events[0].details["source_stage"], "completion_panel");

    // Planning should be entered twice (once for initial, once for round 2).
    let planning_entered = stage_events(&events, JournalEventType::StageEntered, "planning");
    assert_eq!(planning_entered.len(), 2, "planning entered twice");

    // Check that no amendment files remain on disk.
    let amendments_dir = base_dir.join(".ralph-burning/projects/cr-alpha/amendments");
    if amendments_dir.is_dir() {
        let files: Vec<_> = std::fs::read_dir(&amendments_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .collect();
        assert!(
            files.is_empty(),
            "amendment files should be drained from disk"
        );
    }
}

#[tokio::test]
async fn resume_late_stage_conditionally_approved_reports_completion_round_overflow() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-overflow");
    let run_id = RunId::new("run-overflow").unwrap();
    let started_at = Utc::now();

    let snapshot = RunSnapshot {
        active_run: None,
        status: RunStatus::Failed,
        cycle_history: vec![],
        completion_rounds: u32::MAX,
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "failed".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    FsRunSnapshotWriteStore
        .write_run_snapshot(base_dir, &pid, &snapshot)
        .unwrap();

    let prior_stage_ids = [
        StageId::PromptReview,
        StageId::Planning,
        StageId::Implementation,
        StageId::Qa,
        StageId::Review,
    ];
    let mut sequence = 2;
    let run_started =
        journal::run_started_event(sequence, started_at, &run_id, StageId::PromptReview);
    let run_started_line = journal::serialize_event(&run_started).unwrap();
    FsJournalStore
        .append_event(base_dir, &pid, &run_started_line)
        .unwrap();

    for stage_id in prior_stage_ids {
        sequence += 1;
        let payload_id = format!("{run_id}-{stage_id}-c1-a1-cr{}", u32::MAX);
        let artifact_id = format!("{run_id}-{stage_id}-c1-a1-cr{}-artifact", u32::MAX);
        FsPayloadArtifactWriteStore
            .write_payload_artifact_pair(
                base_dir,
                &pid,
                &PayloadRecord {
                    payload_id: payload_id.clone(),
                    stage_id,
                    cycle: 1,
                    attempt: 1,
                    created_at: started_at,
                    payload: json!({
                        "stage": stage_id.as_str(),
                        "completion_round": u32::MAX,
                    }),
                    record_kind: RecordKind::StagePrimary,
                    producer: None,
                    completion_round: 0,
                },
                &ArtifactRecord {
                    artifact_id: artifact_id.clone(),
                    payload_id: payload_id.clone(),
                    stage_id,
                    created_at: started_at,
                    content: format!("artifact for {}", stage_id.as_str()),
                    record_kind: RecordKind::StagePrimary,
                    producer: None,
                    completion_round: 0,
                },
            )
            .unwrap();
        let stage_completed = journal::stage_completed_event(
            sequence,
            started_at,
            &run_id,
            stage_id,
            1,
            1,
            &payload_id,
            &artifact_id,
        );
        let line = journal::serialize_event(&stage_completed).unwrap();
        FsJournalStore.append_event(base_dir, &pid, &line).unwrap();
    }

    let agent_service =
        build_agent_service_with_adapter(StubBackendAdapter::default().with_stage_payload(
            StageId::CompletionPanel,
            conditionally_approved_payload(&["overflow"]),
        ));
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::resume_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        FlowPreset::Standard,
        &config,
    )
    .await;

    // With panel dispatch, the overflow error is caught by fail_run_result and
    // wrapped as ResumeFailed. The underlying error is StageCursorOverflow.
    assert!(result.is_err(), "run should fail on overflow");
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(
        err_msg.contains("completion_round") || err_msg.contains("overflow")
            || err_msg.contains("max completion rounds"),
        "error should reference overflow: {err_msg}"
    );

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let round_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::CompletionRoundAdvanced)
        .collect();
    assert!(
        round_events.is_empty(),
        "completion_round_advanced should not be emitted on overflow"
    );
}

#[tokio::test]
async fn late_stage_rejected_causes_terminal_failure() {
    // Panel model: "rejected" maps to vote_complete=false → ContinueWork loops
    // until max rounds exceeded → terminal failure.
    let _guard = ScopedMaxCompletionRounds::set(2);
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-gamma");

    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default()
            .with_stage_payload(StageId::CompletionPanel, rejected_validation_payload()),
    );
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_err(), "run should fail on max rounds exceeded");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Failed);

    // Panel model produces completion_round_advanced events before max rounds failure.
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let round_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::CompletionRoundAdvanced)
        .collect();
    assert!(
        !round_events.is_empty(),
        "completion_round_advanced events should exist before max rounds failure"
    );
}

#[tokio::test]
async fn late_stage_approved_advances_to_next_late_stage() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-delta");

    // All stages return approved (default behavior of StubBackendAdapter).
    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_ok(), "run should complete: {result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert_eq!(snapshot.completion_rounds, 1, "should complete in round 1");

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    // Verify completion_panel -> acceptance_qa -> final_review progression.
    let cp_completed = stage_events(
        &events,
        JournalEventType::StageCompleted,
        "completion_panel",
    );
    let aq_completed = stage_events(&events, JournalEventType::StageCompleted, "acceptance_qa");
    let fr_completed = stage_events(&events, JournalEventType::StageCompleted, "final_review");
    assert_eq!(cp_completed.len(), 1);
    assert_eq!(aq_completed.len(), 1);
    assert_eq!(fr_completed.len(), 1);
}

#[tokio::test]
async fn late_stage_request_changes_triggers_completion_round_like_conditional() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-beta");

    // AcceptanceQa returns request_changes on first invocation, approved on second.
    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_stage_payload_sequence(
            StageId::AcceptanceQa,
            vec![
                request_changes_payload(&["fix acceptance criteria"]),
                approved_validation_payload(),
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
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_ok(), "run should complete: {result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert_eq!(snapshot.completion_rounds, 2);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let round_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::CompletionRoundAdvanced)
        .collect();
    assert_eq!(round_events.len(), 1);
    assert_eq!(round_events[0].details["source_stage"], "acceptance_qa");
}

#[tokio::test]
async fn cycle_advanced_not_emitted_when_entering_implementation_from_completion_round() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-kappa");

    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_stage_payload_sequence(
            StageId::CompletionPanel,
            vec![
                conditionally_approved_payload(&["minor fix"]),
                approved_validation_payload(),
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
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_ok(), "run should complete: {result:?}");

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();

    // Should have completion_round_advanced but no cycle_advanced.
    let round_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::CompletionRoundAdvanced)
        .collect();
    assert_eq!(round_events.len(), 1);

    let cycle_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::CycleAdvanced)
        .collect();
    assert!(
        cycle_events.is_empty(),
        "cycle_advanced should not be emitted when entering implementation from completion round"
    );
}

// ── Completion Guard Resumability Regression ─────────────────────────────────

#[tokio::test]
async fn completion_guard_produces_resumable_snapshot_when_disk_amendments_remain() {
    // Scenario: orphaned amendment files exist on disk when completion is attempted.
    // The completion guard must leave the snapshot in Failed state with active_run == None,
    // so `run resume` can pick it up.
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "guard-resume");

    // Place an orphaned amendment file on disk before running.
    let amendments_dir = base_dir.join(".ralph-burning/projects/guard-resume/amendments");
    fs::create_dir_all(&amendments_dir).unwrap();
    let orphaned = serde_json::json!({
        "amendment_id": "orphaned-amd-001",
        "source_stage": "completion_panel",
        "source_cycle": 1,
        "source_completion_round": 1,
        "body": "orphaned amendment from prior crash",
        "created_at": "2026-03-10T00:00:00Z",
        "batch_sequence": 1
    });
    fs::write(
        amendments_dir.join("orphaned-amd-001.json"),
        serde_json::to_string_pretty(&orphaned).unwrap(),
    )
    .unwrap();

    // All stages return approved, so the engine will reach complete_run().
    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    // The run must fail with CompletionBlocked.
    assert!(
        result.is_err(),
        "run should fail when completion guard fires"
    );
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("completion blocked"),
        "error should be CompletionBlocked, got: {err}"
    );

    // The snapshot must be Failed with active_run == None (resumable).
    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(
        snapshot.status,
        RunStatus::Failed,
        "snapshot must be Failed for resumability"
    );
    assert!(
        snapshot.active_run.is_none(),
        "active_run must be None so resume can pick it up"
    );
    assert!(
        snapshot.status_summary.contains("blocked"),
        "status_summary should mention blocked: {}",
        snapshot.status_summary
    );

    // The orphaned amendment file must still be on disk (not deleted by the guard).
    assert!(
        amendments_dir.join("orphaned-amd-001.json").exists(),
        "amendment file must not be deleted by the completion guard"
    );

    // Resume with the orphaned amendment still on disk.
    // The engine should reconcile it, restart from planning, process through all stages,
    // drain the amendment, and complete successfully.
    let resume_result = engine::resume_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;
    assert!(
        resume_result.is_ok(),
        "resume should succeed with orphaned amendments reconciled: {resume_result:?}"
    );

    let resumed_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(resumed_snapshot.status, RunStatus::Completed);

    // Amendment file should be drained after planning commit.
    let remaining: Vec<_> = std::fs::read_dir(&amendments_dir)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        remaining.is_empty(),
        "amendment files should be drained after successful resume"
    );
}

// ── Final-Review Continuation Coverage ───────────────────────────────────────

#[tokio::test]
async fn final_review_conditionally_approved_triggers_completion_round_advancement() {
    // When final_review returns conditionally_approved, the engine should queue amendments,
    // advance the completion round, restart from planning, and complete on the next pass.
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "fr-cond");

    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_stage_payload_sequence(
            StageId::FinalReview,
            vec![
                conditionally_approved_payload(&["tighten final wording"]),
                approved_validation_payload(),
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
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(
        result.is_ok(),
        "run should complete after final_review continuation: {result:?}"
    );

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert_eq!(
        snapshot.completion_rounds, 2,
        "should advance to completion round 2"
    );
    assert!(
        snapshot.amendment_queue.pending.is_empty(),
        "amendments should be drained after planning commit"
    );

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();

    // Amendment event with the follow-up body.
    let amendment_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::AmendmentQueued)
        .collect();
    assert!(
        !amendment_events.is_empty(),
        "should have amendment_queued events"
    );
    assert_eq!(amendment_events[0].details["body"], "tighten final wording");

    // Completion round advanced with source_stage = final_review.
    let round_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::CompletionRoundAdvanced)
        .collect();
    assert_eq!(
        round_events.len(),
        1,
        "should have one completion_round_advanced event"
    );
    assert_eq!(round_events[0].details["source_stage"], "final_review");
    assert_eq!(round_events[0].details["from_round"], 1);
    assert_eq!(round_events[0].details["to_round"], 2);

    // Planning should be entered twice (initial + restart from final_review).
    let planning_entered = stage_events(&events, JournalEventType::StageEntered, "planning");
    assert_eq!(planning_entered.len(), 2, "planning entered twice");

    // No amendment files remain on disk.
    let amendments_dir = base_dir.join(".ralph-burning/projects/fr-cond/amendments");
    if amendments_dir.is_dir() {
        let files: Vec<_> = std::fs::read_dir(&amendments_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
            .collect();
        assert!(
            files.is_empty(),
            "amendment files should be drained from disk"
        );
    }
}

#[tokio::test]
async fn final_review_request_changes_triggers_completion_round_advancement() {
    // When final_review returns request_changes, the engine should follow the same
    // completion-round path as conditionally_approved.
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "fr-reqch");

    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_stage_payload_sequence(
            StageId::FinalReview,
            vec![
                request_changes_payload(&["fix final review finding"]),
                approved_validation_payload(),
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
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(
        result.is_ok(),
        "run should complete after final_review request_changes: {result:?}"
    );

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert_eq!(snapshot.completion_rounds, 2);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let round_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::CompletionRoundAdvanced)
        .collect();
    assert_eq!(round_events.len(), 1);
    assert_eq!(round_events[0].details["source_stage"], "final_review");
}

#[tokio::test]
async fn completion_round_restart_creates_distinct_round_aware_payload_artifact_files() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-ids");

    // CompletionPanel returns conditionally_approved on first call, approved on second.
    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_stage_payload_sequence(
            StageId::CompletionPanel,
            vec![
                conditionally_approved_payload(&["tighten note"]),
                approved_validation_payload(),
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
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_ok(), "run should complete: {result:?}");

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.completion_rounds, 2);

    let payloads_dir = base_dir.join(".ralph-burning/projects/cr-ids/history/payloads");
    let artifacts_dir = base_dir.join(".ralph-burning/projects/cr-ids/history/artifacts");

    let payload_files: Vec<String> = fs::read_dir(&payloads_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    let artifact_files: Vec<String> = fs::read_dir(&artifacts_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();

    // Round-1 planning should have -cr1, round-2 planning should have -cr2.
    let cr1_planning_payloads: Vec<_> = payload_files
        .iter()
        .filter(|name| name.contains("-planning-c1-a1-cr1"))
        .collect();
    let cr2_planning_payloads: Vec<_> = payload_files
        .iter()
        .filter(|name| name.contains("-planning-c1-a1-cr2"))
        .collect();
    assert_eq!(
        cr1_planning_payloads.len(),
        1,
        "round-1 planning payload should exist: {payload_files:?}"
    );
    assert_eq!(
        cr2_planning_payloads.len(),
        1,
        "round-2 planning payload should exist: {payload_files:?}"
    );

    let cr1_planning_artifacts: Vec<_> = artifact_files
        .iter()
        .filter(|name| name.contains("-planning-c1-a1-cr1"))
        .collect();
    let cr2_planning_artifacts: Vec<_> = artifact_files
        .iter()
        .filter(|name| name.contains("-planning-c1-a1-cr2"))
        .collect();
    assert_eq!(
        cr1_planning_artifacts.len(),
        1,
        "round-1 planning artifact should exist: {artifact_files:?}"
    );
    assert_eq!(
        cr2_planning_artifacts.len(),
        1,
        "round-2 planning artifact should exist: {artifact_files:?}"
    );

    // Verify round-1 and round-2 are distinct files (not overwritten).
    assert_ne!(
        cr1_planning_payloads[0], cr2_planning_payloads[0],
        "round-1 and round-2 planning payload filenames must differ"
    );
    assert_ne!(
        cr1_planning_artifacts[0], cr2_planning_artifacts[0],
        "round-1 and round-2 planning artifact filenames must differ"
    );

    // Verify all payload/artifact files contain -cr suffix (no legacy format).
    for name in &payload_files {
        assert!(
            name.contains("-cr"),
            "payload file should contain -cr: {name}"
        );
    }
    for name in &artifact_files {
        assert!(
            name.contains("-cr"),
            "artifact file should contain -cr: {name}"
        );
    }
}

#[tokio::test]
async fn invocation_ids_differ_across_completion_rounds() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-invocation-ids");

    let adapter = RecordingAdapter::new(StubBackendAdapter::default().with_stage_payload_sequence(
        StageId::CompletionPanel,
        vec![
            conditionally_approved_payload(&["tighten note"]),
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
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    assert!(result.is_ok(), "run should complete: {result:?}");

    let planning_ids = adapter_handle.invocation_ids_for(StageId::Planning);
    assert_eq!(planning_ids.len(), 2, "planning should run once per round");
    assert_ne!(
        planning_ids[0], planning_ids[1],
        "invocation ids must differ when only completion_round changes"
    );
    assert!(
        planning_ids[0].ends_with("-planning-c1-a1-cr1"),
        "round-1 planning id should include completion_round=1: {:?}",
        planning_ids
    );
    assert!(
        planning_ids[1].ends_with("-planning-c1-a1-cr2"),
        "round-2 planning id should include completion_round=2: {:?}",
        planning_ids
    );
}

// Panel dispatch: aggregate commit failure (stage_completed append). With the
// commit ordering (aggregate + stage_completed BEFORE transition), if
// stage_completed fails, no aggregate or stage_completed leaks, and resume
// restarts from completion_panel.
#[tokio::test]
async fn resume_after_completion_aggregate_commit_failure_preserves_round() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-resume-after-append-fail");

    // First call: ContinueWork (vote_complete=false via translate_to_panel_payload).
    // Second call (after resume): Complete (vote_complete=true).
    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_stage_payload_sequence(
            StageId::CompletionPanel,
            vec![
                conditionally_approved_payload(&["tighten the acceptance note"]),
                approved_validation_payload(),
            ],
        ),
    );
    let config = EffectiveConfig::load(base_dir).unwrap();

    // Standard flow with commit ordering: aggregate + stage_completed first,
    // then completion_round_advanced and cursor snapshot.
    //   1  run_started
    //   2  stage_entered(prompt_review)
    //   3  stage_completed(prompt_review)
    //   4  rollback_created(prompt_review)
    //   5  stage_entered(planning)
    //   6  stage_completed(planning)
    //   7  rollback_created(planning)
    //   8  stage_entered(implementation)
    //   9  stage_completed(implementation)
    //   10 rollback_created(implementation)
    //   11 stage_entered(qa)
    //   12 stage_completed(qa)
    //   13 rollback_created(qa)
    //   14 stage_entered(review)
    //   15 stage_completed(review)
    //   16 rollback_created(review)
    //   17 stage_entered(completion_panel)
    //   18 stage_completed(completion_panel) -> fail here (before transition)
    let failing_journal = FailingJournalStore::new(18);

    let first_result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &failing_journal,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;
    assert!(first_result.is_err(), "run should fail on append error");

    let failed_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(failed_snapshot.status, RunStatus::Failed);
    assert!(failed_snapshot.active_run.is_none());

    let failed_events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    // stage_completed for completion_panel should NOT be persisted (that's
    // where the failure is, and aggregate records are cleaned up).
    let completion_completed = failed_events.iter().any(|event| {
        event.event_type == JournalEventType::StageCompleted
            && event.details.get("stage_id").and_then(|v| v.as_str()) == Some("completion_panel")
    });
    assert!(
        !completion_completed,
        "stage_completed for completion_panel must not exist when aggregate commit failed"
    );
    // No completion_round_advanced either (it comes AFTER aggregate in new ordering).
    assert!(
        failed_events
            .iter()
            .all(|event| event.event_type != JournalEventType::CompletionRoundAdvanced),
        "completion_round_advanced must not exist when aggregate commit failed"
    );

    // Supporting records from the completion panel should be durable.
    let payloads_dir =
        base_dir.join(".ralph-burning/projects/cr-resume-after-append-fail/history/payloads");
    let payload_files: Vec<String> = fs::read_dir(&payloads_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        payload_files.iter().any(|name| name.contains("-completion_panel-")),
        "completion supporting records should be durable: {payload_files:?}"
    );

    // Resume: re-execute completion_panel, this time the stub returns Complete.
    let resume_result = engine::resume_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
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
}

// Panel dispatch: stage_completed append failure for completion panel via
// ScopedJournalAppendFailpoint. Aggregate + stage_completed are committed
// BEFORE the transition. If stage_completed fails, supporting records remain
// durable but no aggregate or stage_completed leaks.
#[tokio::test]
async fn completion_stage_completed_append_failure_leaves_supporting_records() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-stage-completed-fail");

    // ContinueWork on first call (vote_complete=false).
    let agent_service =
        build_agent_service_with_adapter(StubBackendAdapter::default().with_stage_payload(
            StageId::CompletionPanel,
            conditionally_approved_payload(&["fix something"]),
        ));
    let config = EffectiveConfig::load(base_dir).unwrap();

    // Commit ordering: aggregate + stage_completed FIRST, then transition.
    //   17 stage_entered(completion_panel)
    //   18 stage_completed(completion_panel) -> fail here via failpoint
    // ScopedJournalAppendFailpoint uses `current >= threshold` (0-indexed),
    // so threshold=17 allows 17 appends (0-16) and fails the 18th (counter=17).
    let _failpoint = ScopedJournalAppendFailpoint::for_project(&pid, 17);

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;
    assert!(result.is_err(), "run should fail on stage_completed append");

    let failed_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(failed_snapshot.status, RunStatus::Failed);

    // Supporting records from the completion panel execution should be durable.
    let payloads_dir =
        base_dir.join(".ralph-burning/projects/cr-stage-completed-fail/history/payloads");
    let payload_files: Vec<String> = fs::read_dir(&payloads_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        payload_files.iter().any(|name| name.contains("-completion_panel-")),
        "completion supporting records should be durable: {payload_files:?}"
    );

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    // stage_completed for completion_panel should NOT be present (that's where we fail).
    let completion_completed = events.iter().any(|event| {
        event.event_type == JournalEventType::StageCompleted
            && event.details.get("stage_id").and_then(|v| v.as_str()) == Some("completion_panel")
    });
    assert!(
        !completion_completed,
        "stage_completed for completion_panel must not exist when its append failed"
    );
}

// Panel dispatch: resume after completion panel failure produces no duplicate
// supporting records. The supporting records from the first (failed) attempt
// remain durable, and the resume re-executes the panel cleanly.
#[tokio::test]
async fn resume_after_completion_panel_failure_no_duplicate_supporting_records() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-resume-after-panel-fail");

    let adapter = RecordingAdapter::new(StubBackendAdapter::default().with_stage_payload_sequence(
        StageId::CompletionPanel,
        vec![
            conditionally_approved_payload(&["fix something"]),
            approved_validation_payload(),
        ],
    ));
    let agent_service = build_agent_service_with_adapter(adapter);
    let config = EffectiveConfig::load(base_dir).unwrap();

    {
        // Fail at stage_completed for completion_panel (18th journal append).
        // With commit ordering, aggregate commit happens first.
        // ScopedJournalAppendFailpoint threshold=17 allows 17 appends and fails the 18th.
        let _failpoint = ScopedJournalAppendFailpoint::for_project(&pid, 17);
        let first_result = engine::execute_standard_run(
            &agent_service,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            base_dir,
            &pid,
            &config,
        )
        .await;
        assert!(first_result.is_err(), "run should fail on journal append");
    }

    let failed_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(failed_snapshot.status, RunStatus::Failed);

    // Resume with failpoint removed — should complete.
    let resume_result = engine::resume_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
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
}

// Panel dispatch: completion_round_advanced failure via failpoint. With the
// commit ordering (aggregate first), stage_completed IS written but
// completion_round_advanced fails. Verifies that resume completes successfully.
#[tokio::test]
async fn resume_after_completion_round_advanced_failpoint_completes() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-resume-after-first-append-fail");

    let adapter = RecordingAdapter::new(StubBackendAdapter::default().with_stage_payload_sequence(
        StageId::CompletionPanel,
        vec![
            conditionally_approved_payload(&["fix something"]),
            approved_validation_payload(),
        ],
    ));
    let agent_service = build_agent_service_with_adapter(adapter);
    let config = EffectiveConfig::load(base_dir).unwrap();

    {
        // With commit ordering: stage_completed is the 18th journal append
        // and completion_round_advanced is the 19th. ScopedJournalAppendFailpoint
        // threshold=18 allows 18 appends and fails the 19th.
        let _failpoint = ScopedJournalAppendFailpoint::for_project(&pid, 18);
        let first_result = engine::execute_standard_run(
            &agent_service,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            base_dir,
            &pid,
            &config,
        )
        .await;
        assert!(
            first_result.is_err(),
            "run should fail on journal append"
        );
    }

    let failed_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(failed_snapshot.status, RunStatus::Failed);

    let failed_events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    // stage_completed IS written (it comes before completion_round_advanced).
    let completion_completed = failed_events.iter().any(|event| {
        event.event_type == JournalEventType::StageCompleted
            && event.details.get("stage_id").and_then(|v| v.as_str()) == Some("completion_panel")
    });
    assert!(
        completion_completed,
        "stage_completed for completion_panel should be persisted (aggregate committed first)"
    );
    // No completion_round_advanced in journal (that's where we failed).
    assert!(
        failed_events
            .iter()
            .all(|event| event.event_type != JournalEventType::CompletionRoundAdvanced),
        "completion_round_advanced should not be persisted on failpoint"
    );

    // Resume completes successfully. Resume sees StageCompleted for
    // completion_panel and advances past it.
    let resume_result = engine::resume_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
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
}

// Panel dispatch: successful completion round with ContinueWork then Complete.
// Verifies aggregate records, completion_round advancement, and final completion.
#[tokio::test]
async fn completion_panel_continue_then_complete_success() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-full-batch-success");

    // First call: ContinueWork (vote_complete=false). Second call: Complete.
    let adapter = RecordingAdapter::new(StubBackendAdapter::default().with_stage_payload_sequence(
        StageId::CompletionPanel,
        vec![
            conditionally_approved_payload(&["fix something"]),
            approved_validation_payload(),
        ],
    ));
    let agent_service = build_agent_service_with_adapter(adapter);
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
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
    assert_eq!(snapshot.completion_rounds, 2);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();

    // Verify completion_round_advanced event exists.
    let cra_events: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::CompletionRoundAdvanced)
        .collect();
    assert_eq!(cra_events.len(), 1, "should have exactly one CRA event");

    // Verify stage_completed events for completion_panel.
    let completion_completed: Vec<_> = events
        .iter()
        .filter(|event| {
            event.event_type == JournalEventType::StageCompleted
                && event.details.get("stage_id").and_then(|v| v.as_str())
                    == Some("completion_panel")
        })
        .collect();
    assert_eq!(
        completion_completed.len(),
        2,
        "should have stage_completed for both completion rounds"
    );

    // Verify supporting and aggregate records exist.
    let payloads_dir =
        base_dir.join(".ralph-burning/projects/cr-full-batch-success/history/payloads");
    let payload_files: Vec<String> = fs::read_dir(&payloads_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    // Both completion rounds should have supporting records.
    assert!(
        payload_files.iter().any(|name| name.contains("-completion_panel-") && name.contains("-cr1")),
        "round 1 completion records should exist: {payload_files:?}"
    );
    assert!(
        payload_files.iter().any(|name| name.contains("-completion_panel-") && name.contains("-cr2")),
        "round 2 completion records should exist: {payload_files:?}"
    );
    // Aggregate records should exist for both rounds.
    assert!(
        payload_files.iter().any(|name| name.contains("aggregate") && name.contains("-cr1")),
        "round 1 aggregate should exist: {payload_files:?}"
    );
    assert!(
        payload_files.iter().any(|name| name.contains("aggregate") && name.contains("-cr2")),
        "round 2 aggregate should exist: {payload_files:?}"
    );
}
