use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use serde_json::{json, Value};
use tempfile::tempdir;

use ralph_burning::adapters::fs::{
    FileSystem, FsAmendmentQueueStore, FsArtifactStore, FsJournalStore,
    FsPayloadArtifactWriteStore, FsProjectStore, FsRawOutputStore, FsRollbackPointStore,
    FsRunSnapshotStore, FsRunSnapshotWriteStore, FsRuntimeLogWriteStore, FsSessionStore,
};
use ralph_burning::adapters::stub_backend::StubBackendAdapter;
use ralph_burning::contexts::agent_execution::model::{
    CancellationToken, InvocationEnvelope, InvocationRequest,
};
use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
use ralph_burning::contexts::agent_execution::service::AgentExecutionService;
use ralph_burning::contexts::milestone_record::bundle::{
    AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
};
use ralph_burning::contexts::milestone_record::service::{
    create_milestone, persist_plan, read_task_runs, CreateMilestoneInput,
};
use ralph_burning::contexts::project_run_record::journal;
use ralph_burning::contexts::project_run_record::model::{
    JournalEvent, JournalEventType, RunSnapshot, RunStatus, RuntimeLogEntry, TaskOrigin, TaskSource,
};
use ralph_burning::contexts::project_run_record::service::{
    self, CreateProjectInput, JournalStorePort, ProjectStorePort, RunSnapshotPort,
    RunSnapshotWritePort, RuntimeLogWritePort,
};
use ralph_burning::contexts::project_run_record::ArtifactStorePort;
use ralph_burning::contexts::workflow_composition::engine;
use ralph_burning::contexts::workflow_composition::panel_contracts::RecordKind;
use ralph_burning::contexts::workspace_governance;
use ralph_burning::contexts::workspace_governance::config::EffectiveConfig;
use ralph_burning::shared::domain::{
    BackendFamily, FailureClass, FlowPreset, ProjectId, RunId, StageId,
};
use ralph_burning::shared::error::{AppError, AppResult};

const JOURNAL_APPEND_FAIL_AFTER_ENV: &str = "RALPH_BURNING_TEST_JOURNAL_APPEND_FAIL_AFTER";
static FAILPOINT_ENV_MUTEX: Mutex<()> = Mutex::new(());

fn setup_workspace(base_dir: &Path) {
    workspace_governance::initialize_workspace(base_dir, Utc::now()).unwrap();
}

fn live_workspace_root(base_dir: &Path) -> PathBuf {
    FileSystem::live_workspace_root_path(base_dir)
}

fn workspace_config_path(base_dir: &Path) -> PathBuf {
    live_workspace_root(base_dir).join("workspace.toml")
}

fn project_root(base_dir: &Path, project_id: &str) -> PathBuf {
    live_workspace_root(base_dir)
        .join("projects")
        .join(project_id)
}

fn create_project_with_flow(base_dir: &Path, project_id: &str, flow: FlowPreset) -> ProjectId {
    let pid = ProjectId::new(project_id).unwrap();
    let store = FsProjectStore;
    let journal_store = FsJournalStore;
    let prompt_contents = "# Test prompt";
    service::create_project(
        &store,
        &journal_store,
        base_dir,
        CreateProjectInput {
            id: pid.clone(),
            name: format!("Test {}", project_id),
            flow,
            prompt_path: "prompt.md".to_owned(),
            prompt_contents: prompt_contents.to_owned(),
            prompt_hash: ralph_burning::adapters::fs::FileSystem::prompt_hash(prompt_contents),
            created_at: Utc::now(),
            task_source: None,
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

fn create_milestone_task_project(
    base_dir: &Path,
    project_id: &str,
    task_source: TaskSource,
) -> ProjectId {
    let pid = ProjectId::new(project_id).unwrap();
    let store = FsProjectStore;
    let journal_store = FsJournalStore;
    let prompt_contents = "# Test prompt";
    service::create_project(
        &store,
        &journal_store,
        base_dir,
        CreateProjectInput {
            id: pid.clone(),
            name: format!("Test {}", project_id),
            flow: FlowPreset::Standard,
            prompt_path: "prompt.md".to_owned(),
            prompt_contents: prompt_contents.to_owned(),
            prompt_hash: ralph_burning::adapters::fs::FileSystem::prompt_hash(prompt_contents),
            created_at: Utc::now(),
            task_source: Some(task_source),
        },
    )
    .unwrap();

    workspace_governance::set_active_project(base_dir, &pid).unwrap();
    pid
}

fn sample_milestone_bundle(milestone_id: &str) -> MilestoneBundle {
    MilestoneBundle {
        schema_version: 1,
        identity: MilestoneIdentity {
            id: milestone_id.to_owned(),
            name: "Alpha Milestone".to_owned(),
        },
        executive_summary: "Ship bead-backed task creation.".to_owned(),
        goals: vec!["Create the bead-backed task path.".to_owned()],
        non_goals: vec![],
        constraints: vec!["Keep the run substrate compatible.".to_owned()],
        acceptance_map: vec![AcceptanceCriterion {
            id: "AC-1".to_owned(),
            description: "Task creation works".to_owned(),
            covered_by: vec!["bead-2".to_owned()],
        }],
        workstreams: vec![Workstream {
            name: "Creation".to_owned(),
            description: Some("Project bootstrap flow.".to_owned()),
            beads: vec![BeadProposal {
                bead_id: Some(format!("{milestone_id}.bead-2")),
                explicit_id: None,
                title: "Bootstrap bead-backed task creation".to_owned(),
                description: Some("Create a project from milestone context.".to_owned()),
                bead_type: Some("feature".to_owned()),
                priority: Some(1),
                labels: vec!["creation".to_owned()],
                depends_on: vec![],
                acceptance_criteria: vec!["AC-1".to_owned()],
                flow_override: Some(FlowPreset::Standard),
            }],
        }],
        default_flow: FlowPreset::Minimal,
        agents_guidance: Some("Keep it deterministic.".to_owned()),
    }
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

fn run_git(repo_root: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo_root)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git {:?} failed: stdout={} stderr={}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn init_git_repo(repo_root: &Path) {
    run_git(repo_root, &["init"]);
    run_git(repo_root, &["config", "user.name", "Test User"]);
    run_git(repo_root, &["config", "user.email", "test@example.com"]);
    fs::write(repo_root.join("tracked.txt"), "baseline\n").expect("write tracked file");
    run_git(repo_root, &["add", "tracked.txt"]);
    run_git(repo_root, &["commit", "-m", "initial"]);
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

#[test]
fn final_review_arbiter_drift_is_detected() {
    use ralph_burning::contexts::agent_execution::policy::ResolvedPanelMember;
    use ralph_burning::shared::domain::ResolvedBackendTarget;

    let reviewers = [ResolvedPanelMember {
        target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-model"),
        required: true,
        configured_index: 0,
    }];
    let arbiter_a = ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-a");
    let arbiter_b = ResolvedBackendTarget::new(BackendFamily::Codex, "arbiter-b");

    let original =
        engine::build_final_review_snapshot(StageId::FinalReview, &reviewers, &arbiter_a);
    let drifted = engine::build_final_review_snapshot(StageId::FinalReview, &reviewers, &arbiter_b);
    assert!(
        engine::resolution_has_drifted(&original, &drifted),
        "arbiter changes must trigger final-review drift"
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
    let payloads_dir = project_root(base_dir, "happy-test").join("history/payloads");
    let artifacts_dir = project_root(base_dir, "happy-test").join("history/artifacts");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    let artifact_count = fs::read_dir(&artifacts_dir).unwrap().count();
    // prompt_review: 4 records (1 refiner + 2 validators + 1 primary)
    // completion_panel: 3 records (2 completers + 1 aggregate)
    // final_review: 4 records (3 reviewer proposals + 1 aggregate)
    // other 5 stages: 1 each = 5
    // total = 16
    assert_eq!(payload_count, 16, "expected 16 payloads");
    assert_eq!(artifact_count, 16, "expected 16 artifacts");
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
    let payloads_dir = project_root(base_dir, "no-pr-test").join("history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    // completion_panel: 3 records (2 completers + 1 aggregate)
    // final_review: 4 records (3 reviewers + 1 aggregate)
    // other 5 stages: 1 each = 5
    // total = 12 (no prompt_review)
    assert_eq!(
        payload_count, 12,
        "expected 12 payloads without prompt_review"
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

    let rollback_dir = project_root(base_dir, "rollback-points").join("rollback");
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
async fn checkpoint_creation_failure_is_tolerated_and_logged() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "checkpoint-warn");

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
    .expect("run completes without git repo");

    let rollback_dir = project_root(base_dir, "checkpoint-warn").join("rollback");
    let rollback_files: Vec<_> = fs::read_dir(&rollback_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(
        rollback_files.len(),
        8,
        "one rollback point per completed stage"
    );
    for path in rollback_files {
        let rollback_json: Value =
            serde_json::from_str(&fs::read_to_string(path).unwrap()).expect("parse rollback");
        assert!(
            rollback_json
                .get("git_sha")
                .map_or(true, serde_json::Value::is_null),
            "non-git runs should persist rollback points without git_sha"
        );
    }

    let runtime_logs = fs::read_to_string(
        project_root(base_dir, "checkpoint-warn").join("runtime/logs/run.ndjson"),
    )
    .expect("read runtime logs");
    let warning_count = runtime_logs
        .lines()
        .filter(|line| line.contains("checkpoint creation failed"))
        .count();
    assert_eq!(
        warning_count, 8,
        "every checkpoint failure should be warned"
    );
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

    let payloads_dir = project_root(base_dir, "rollback-branch").join("history/payloads");
    let artifacts_dir = project_root(base_dir, "rollback-branch").join("history/artifacts");

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

    let history = service::run_history(
        &FsProjectStore,
        &FsJournalStore,
        &FsArtifactStore,
        base_dir,
        &pid,
    )
    .unwrap();
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

    // The resumed visible branch now includes the final-review panel records
    // and the abandoned implementation payload remains on disk.
    assert_eq!(
        payload_files.len(),
        18,
        "old branch payload files should remain on disk alongside the resumed branch"
    );
    assert_eq!(
        history.payloads.len(),
        16,
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

    let payload_count = fs::read_dir(project_root(base_dir, "docs-happy").join("history/payloads"))
        .unwrap()
        .count();
    let artifact_count =
        fs::read_dir(project_root(base_dir, "docs-happy").join("history/artifacts"))
            .unwrap()
            .count();
    // 4 primary stage records + 1 local validation supporting record
    assert_eq!(payload_count, 5);
    assert_eq!(artifact_count, 5);
}

#[tokio::test]
async fn primary_stage_artifacts_persist_agent_producer_metadata() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "docs-producer", FlowPreset::DocsChange);

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

    let artifacts = FsArtifactStore.list_artifacts(base_dir, &pid).unwrap();
    let docs_plan_artifact = artifacts
        .iter()
        .find(|record| {
            record.stage_id == StageId::DocsPlan && record.record_kind == RecordKind::StagePrimary
        })
        .expect("docs_plan primary artifact should exist");

    assert!(
        matches!(
            &docs_plan_artifact.producer,
            Some(
                ralph_burning::contexts::workflow_composition::panel_contracts::RecordProducer::Agent {
                    requested_backend_family: backend_family,
                    requested_model_id: model_id,
                    ..
                }
            ) if backend_family == "claude" && model_id == "claude-opus-4-7"
        ),
        "docs_plan primary artifact should persist agent producer metadata: {:?}",
        docs_plan_artifact.producer
    );
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

    let payload_count = fs::read_dir(project_root(base_dir, "ci-happy").join("history/payloads"))
        .unwrap()
        .count();
    let artifact_count = fs::read_dir(project_root(base_dir, "ci-happy").join("history/artifacts"))
        .unwrap()
        .count();
    // 4 primary stage records + 1 local validation supporting record
    assert_eq!(payload_count, 5);
    assert_eq!(artifact_count, 5);
}

#[tokio::test]
async fn docs_change_remediation_restarts_from_docs_update() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "docs-remediation", FlowPreset::DocsChange);

    // Create a marker-based command that fails on first invocation and
    // succeeds on subsequent invocations, simulating a fix cycle.
    let marker = base_dir.join("docs-validation-marker");
    let cmd = format!(
        "if [ -f '{}' ]; then exit 0; else touch '{}' && exit 1; fi",
        marker.display(),
        marker.display()
    );

    // Append docs_commands to workspace config so EffectiveConfig::load picks them up.
    let ws_config_path = workspace_config_path(base_dir);
    let mut ws_config = fs::read_to_string(&ws_config_path).unwrap();
    ws_config.push_str(&format!("\n[validation]\ndocs_commands = [{:?}]\n", cmd));
    fs::write(&ws_config_path, ws_config).unwrap();

    let adapter = RecordingAdapter::new(StubBackendAdapter::default());
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

    // Verify the remediation context contains follow-up items from the local validation failure.
    let docs_update_contexts = adapter_handle.contexts_for(StageId::DocsUpdate);
    assert_eq!(docs_update_contexts.len(), 2);
    assert!(
        docs_update_contexts[1].get("remediation").is_some(),
        "second docs_update invocation should have remediation context"
    );
}

#[tokio::test]
async fn docs_change_local_validation_pass_completes_without_amendments() {
    // DocsValidation now runs locally. Passing commands complete the run
    // without follow-ups or amendments (local validation is binary pass/fail).
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "docs-conditional", FlowPreset::DocsChange);

    // Append docs_commands to workspace config.
    let ws_config_path = workspace_config_path(base_dir);
    let mut ws_config = fs::read_to_string(&ws_config_path).unwrap();
    ws_config.push_str("\n[validation]\ndocs_commands = [\"true\"]\n");
    fs::write(&ws_config_path, ws_config).unwrap();

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
    assert!(snapshot.amendment_queue.pending.is_empty());
    // Local validation does not produce follow-ups.
    assert!(snapshot.amendment_queue.recorded_follow_ups.is_empty());

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

    // Create a marker-based command that fails on first invocation and
    // succeeds on subsequent invocations, simulating a fix cycle.
    let marker = base_dir.join("ci-validation-marker");
    let cmd = format!(
        "if [ -f '{}' ]; then exit 0; else touch '{}' && exit 1; fi",
        marker.display(),
        marker.display()
    );

    // Append ci_commands to workspace config.
    let ws_config_path = workspace_config_path(base_dir);
    let mut ws_config = fs::read_to_string(&ws_config_path).unwrap();
    ws_config.push_str(&format!("\n[validation]\nci_commands = [{:?}]\n", cmd));
    fs::write(&ws_config_path, ws_config).unwrap();

    let adapter = RecordingAdapter::new(StubBackendAdapter::default());
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

    // Verify the remediation context contains follow-up items from the local validation failure.
    let ci_update_contexts = adapter_handle.contexts_for(StageId::CiUpdate);
    assert_eq!(ci_update_contexts.len(), 2);
    assert!(
        ci_update_contexts[1].get("remediation").is_some(),
        "second ci_update invocation should have remediation context"
    );
}

#[tokio::test]
async fn ci_improvement_always_failing_validation_fails_run() {
    // CiValidation now runs locally. A command that always fails exhausts
    // remediation cycles and fails the run.
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "ci-rejected", FlowPreset::CiImprovement);

    // Append ci_commands to workspace config.
    let ws_config_path = workspace_config_path(base_dir);
    let mut ws_config = fs::read_to_string(&ws_config_path).unwrap();
    ws_config.push_str("\n[validation]\nci_commands = [\"false\"]\n");
    fs::write(&ws_config_path, ws_config).unwrap();

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
    // Remediation exhaustion or qa iteration cap failure
    let failure_class = run_failed.details["failure_class"].as_str().unwrap_or("");
    assert!(
        failure_class == "remediation_exhausted"
            || failure_class == "stage_commit_failed"
            || failure_class == "qa_review_outcome_failure",
        "unexpected failure_class: {failure_class}"
    );
}

#[tokio::test(start_paused = true)]
async fn resume_from_failed_docs_change_run_skips_completed_stages() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "docs-resume", FlowPreset::DocsChange);
    let config = EffectiveConfig::load(base_dir).unwrap();

    let failing_agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::DocsUpdate, 5),
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
        6
    );

    let run_resumed = events
        .iter()
        .find(|event| event.event_type == JournalEventType::RunResumed)
        .expect("run_resumed");
    assert_eq!(run_resumed.details["resume_stage"], "docs_update");
}

#[tokio::test(start_paused = true)]
async fn resume_from_failed_ci_improvement_run_skips_completed_stages() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "ci-resume", FlowPreset::CiImprovement);
    let config = EffectiveConfig::load(base_dir).unwrap();

    let failing_agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::CiUpdate, 5),
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
        6
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
    // ApplyFixes is skipped when Review approves with no findings.
    assert_eq!(
        vec!["plan_and_implement", "review", "final_review"],
        entered
    );

    // Verify the StageSkipped journal event was recorded for apply_fixes.
    let skipped: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::StageSkipped)
        .collect();
    assert_eq!(skipped.len(), 1);
    assert_eq!(skipped[0].details["stage_id"], "apply_fixes");
    assert_eq!(
        skipped[0].details["reason"],
        "review approved with no findings"
    );

    let payload_count = fs::read_dir(project_root(base_dir, "qd-happy").join("history/payloads"))
        .unwrap()
        .count();
    let artifact_count = fs::read_dir(project_root(base_dir, "qd-happy").join("history/artifacts"))
        .unwrap()
        .count();
    // One fewer payload/artifact pair since ApplyFixes was skipped.
    assert_eq!(payload_count, 6);
    assert_eq!(artifact_count, 6);
}

#[tokio::test]
async fn quick_dev_review_approved_with_findings_runs_apply_fixes() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "qd-findings", FlowPreset::QuickDev);

    // Review returns Approved but with non-empty findings_or_gaps.
    let agent_service =
        build_agent_service_with_adapter(StubBackendAdapter::default().with_stage_payload(
            StageId::Review,
            json!({
                "outcome": "approved",
                "evidence": ["looks good overall"],
                "findings_or_gaps": ["minor style issue detected"],
                "follow_up_or_amendments": [],
            }),
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
        FlowPreset::QuickDev,
        &config,
    )
    .await;

    assert!(result.is_ok(), "{result:?}");

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let entered: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::StageEntered)
        .map(|event| event.details["stage_id"].as_str().unwrap().to_owned())
        .collect();
    // ApplyFixes should NOT be skipped when Review has findings.
    assert_eq!(
        vec![
            "plan_and_implement",
            "review",
            "apply_fixes",
            "final_review"
        ],
        entered,
        "apply_fixes must run when review has findings"
    );

    // No StageSkipped events should be present.
    let skipped: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::StageSkipped)
        .collect();
    assert!(
        skipped.is_empty(),
        "no stages should be skipped when review has findings"
    );
}

#[tokio::test]
async fn quick_dev_review_approved_with_amendments_runs_apply_fixes() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "qd-amendments", FlowPreset::QuickDev);

    // Review returns Approved but with non-empty follow_up_or_amendments.
    let agent_service =
        build_agent_service_with_adapter(StubBackendAdapter::default().with_stage_payload(
            StageId::Review,
            json!({
                "outcome": "approved",
                "evidence": ["looks good overall"],
                "findings_or_gaps": [],
                "follow_up_or_amendments": ["add a test for edge case"],
            }),
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
        FlowPreset::QuickDev,
        &config,
    )
    .await;

    assert!(result.is_ok(), "{result:?}");

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let entered: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::StageEntered)
        .map(|event| event.details["stage_id"].as_str().unwrap().to_owned())
        .collect();
    // ApplyFixes should NOT be skipped when Review has amendments.
    assert_eq!(
        vec![
            "plan_and_implement",
            "review",
            "apply_fixes",
            "final_review"
        ],
        entered,
        "apply_fixes must run when review has amendments"
    );
}

#[tokio::test]
async fn resume_after_apply_fixes_skipped_does_not_rerun_apply_fixes() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "qd-skip-resume", FlowPreset::QuickDev);
    let config = EffectiveConfig::load(base_dir).unwrap();

    // FinalReview fails once (simulates crash after ApplyFixes was skipped),
    // then succeeds on resume.
    let failing_agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::FinalReview, 1),
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

    // Verify that the StageSkipped event was committed before the failure.
    let events_before = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let skipped: Vec<_> = events_before
        .iter()
        .filter(|event| event.event_type == JournalEventType::StageSkipped)
        .collect();
    assert_eq!(skipped.len(), 1, "apply_fixes skip event must be committed");
    assert_eq!(skipped[0].details["stage_id"], "apply_fixes");

    // Resume — should pick up at FinalReview, not re-run ApplyFixes.
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

    // Resume should start at final_review, not apply_fixes.
    let run_resumed = events
        .iter()
        .find(|event| event.event_type == JournalEventType::RunResumed)
        .expect("run_resumed");
    assert_eq!(
        run_resumed.details["resume_stage"], "final_review",
        "resume must skip past apply_fixes which was already skipped"
    );

    // ApplyFixes must never have been entered.
    let apply_fixes_entered = stage_events(&events, JournalEventType::StageEntered, "apply_fixes");
    assert!(
        apply_fixes_entered.is_empty(),
        "apply_fixes must not be entered after being skipped"
    );
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
async fn iterative_minimal_loops_plan_and_implement_until_stable_then_runs_final_review() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    init_git_repo(base_dir);
    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "iter-min", FlowPreset::IterativeMinimal);

    let adapter = IterativePlanAdapter::new();
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
        FlowPreset::IterativeMinimal,
        &config,
    )
    .await;

    assert!(
        result.is_ok(),
        "iterative_minimal run should complete: {result:?}"
    );
    assert_eq!(adapter_handle.plan_calls(), 3);

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .expect("read final snapshot");
    assert_eq!(snapshot.status, RunStatus::Completed);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert_eq!(
        stage_events(
            &events,
            JournalEventType::StageEntered,
            "plan_and_implement"
        )
        .len(),
        1,
        "plan_and_implement stage should still be entered once"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == JournalEventType::ImplementerIterationStarted)
            .count(),
        3
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == JournalEventType::ImplementerIterationCompleted)
            .count(),
        3
    );
    let exited = events
        .iter()
        .find(|event| event.event_type == JournalEventType::ImplementerLoopExited)
        .expect("implementer_loop_exited event");
    assert_eq!(exited.details["reason"], "stable");
    assert_eq!(exited.details["total_iterations"], 3);
    assert_eq!(
        stage_events(&events, JournalEventType::StageEntered, "final_review").len(),
        1,
        "final_review should still run after the loop exits"
    );
}

#[tokio::test]
async fn iterative_minimal_falls_back_to_workspace_diff_when_no_git_repo_exists() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "iter-no-git", FlowPreset::IterativeMinimal);

    let adapter = IterativePlanAdapter::new();
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
        FlowPreset::IterativeMinimal,
        &config,
    )
    .await;

    assert!(
        result.is_ok(),
        "iterative_minimal run should complete outside git repos: {result:?}"
    );
    assert_eq!(adapter_handle.plan_calls(), 3);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let exited = events
        .iter()
        .find(|event| event.event_type == JournalEventType::ImplementerLoopExited)
        .expect("implementer_loop_exited event");
    assert_eq!(exited.details["reason"], "stable");
    assert_eq!(
        stage_events(&events, JournalEventType::StageEntered, "final_review").len(),
        1,
        "final_review should still run after the filesystem fallback loop exits"
    );
}

#[tokio::test]
async fn iterative_minimal_detects_changes_to_already_dirty_files() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    init_git_repo(base_dir);
    let pid = create_project_with_flow(base_dir, "iter-dirty", FlowPreset::IterativeMinimal);

    let adapter = DirtyFileIterativePlanAdapter::new();
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
        FlowPreset::IterativeMinimal,
        &config,
    )
    .await;

    assert!(result.is_ok(), "{result:?}");

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let completed: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == JournalEventType::ImplementerIterationCompleted)
        .collect();
    assert!(
        completed.len() >= 2,
        "expected at least two completed implementer iterations"
    );
    assert_eq!(completed[0].details["diff_changed"], true);
    assert_eq!(
        completed[1].details["diff_changed"], true,
        "rewriting an already-dirty file must still count as a diff change"
    );
}

#[tokio::test]
async fn iterative_minimal_resume_after_terminal_loop_does_not_reinvoke_implementer() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    init_git_repo(base_dir);
    setup_workspace(base_dir);
    let pid = create_project_with_flow(
        base_dir,
        "iter-resume-terminal",
        FlowPreset::IterativeMinimal,
    );

    let adapter = IterativePlanAdapter::new();
    let adapter_handle = adapter.clone();
    let agent_service = build_agent_service_with_adapter(adapter);
    let config = EffectiveConfig::load(base_dir).unwrap();

    {
        let _failpoint = ScopedJournalAppendFailpoint::for_project(&pid, 9);
        let first_result = engine::execute_run(
            &agent_service,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            base_dir,
            &pid,
            FlowPreset::IterativeMinimal,
            &config,
        )
        .await;
        assert!(
            first_result.is_err(),
            "failpoint should interrupt stage commit"
        );
    }

    assert_eq!(
        adapter_handle.plan_calls(),
        3,
        "initial run should stop after the stable loop finishes"
    );

    let failed_events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert_eq!(
        failed_events
            .iter()
            .filter(|event| event.event_type == JournalEventType::ImplementerLoopExited)
            .count(),
        1,
        "terminal loop exit must already be durable before resume"
    );
    assert_eq!(
        stage_events(
            &failed_events,
            JournalEventType::StageCompleted,
            "plan_and_implement"
        )
        .len(),
        0,
        "stage commit should not have completed under the failpoint"
    );

    let resume_agent_service = build_agent_service_with_adapter(adapter_handle.clone());
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
        FlowPreset::IterativeMinimal,
        &config,
    )
    .await;
    assert!(resume_result.is_ok(), "{resume_result:?}");
    assert_eq!(
        adapter_handle.plan_calls(),
        3,
        "resume should reuse the terminal iteration output instead of invoking the implementer again"
    );

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(snapshot.status, RunStatus::Completed);

    let resumed_events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert_eq!(
        resumed_events
            .iter()
            .filter(|event| event.event_type == JournalEventType::ImplementerLoopExited)
            .count(),
        1,
        "resume should not append a duplicate loop exit event when one is already durable"
    );
}

#[tokio::test]
async fn minimal_flow_remains_single_pass_without_iteration_events() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "minimal-plain", FlowPreset::Minimal);
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
        FlowPreset::Minimal,
        &config,
    )
    .await;

    assert!(result.is_ok(), "{result:?}");

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert!(
        events.iter().all(|event| {
            !matches!(
                event.event_type,
                JournalEventType::ImplementerIterationStarted
                    | JournalEventType::ImplementerIterationCompleted
                    | JournalEventType::ImplementerLoopExited
            )
        }),
        "legacy minimal flow must not emit iterative_minimal journal events"
    );
    assert_eq!(
        stage_events(
            &events,
            JournalEventType::StageEntered,
            "plan_and_implement"
        )
        .len(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn resume_from_failed_quick_dev_run_skips_completed_stages() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_project_with_flow(base_dir, "qd-resume", FlowPreset::QuickDev);
    let config = EffectiveConfig::load(base_dir).unwrap();

    let failing_agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::Review, 5),
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
        6
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
        fs::read_dir(project_root(base_dir, "qd-preflight").join("history/payloads"))
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
                prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
                prompt_hash_at_stage_start: "prompt-hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 0,
        max_completion_rounds: Some(0),
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
        interrupted_run: None,
        status: RunStatus::Completed,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(0),
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
    let payloads_dir = project_root(base_dir, "preflight-test").join("history/payloads");
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
    let temp = tempdir().unwrap();
    setup_workspace(temp.path());
    let config = EffectiveConfig::load(temp.path()).unwrap();
    let resolver = ralph_burning::contexts::agent_execution::service::BackendResolver::new();
    let stages = engine::standard_stage_plan(true);
    let plan = engine::resolve_stage_plan(&stages, &resolver, None).unwrap();

    let adapter = StubBackendAdapter::default();
    let result = engine::preflight_check(&adapter, &config, 1, &plan).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn preflight_check_fails_with_unavailable_backend() {
    let temp = tempdir().unwrap();
    setup_workspace(temp.path());
    let config = EffectiveConfig::load(temp.path()).unwrap();
    let resolver = ralph_burning::contexts::agent_execution::service::BackendResolver::new();
    let stages = engine::standard_stage_plan(true);
    let plan = engine::resolve_stage_plan(&stages, &resolver, None).unwrap();

    let adapter = StubBackendAdapter::default().unavailable();
    let result = engine::preflight_check(&adapter, &config, 1, &plan).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn preflight_check_validates_final_review_arbiter_member() {
    let temp = tempdir().unwrap();
    setup_workspace(temp.path());

    let workspace_toml = workspace_config_path(temp.path());
    let content = fs::read_to_string(&workspace_toml).unwrap();
    let patched = if content.contains("[workflow]") {
        format!("{content}\n[final_review]\narbiter_backend = \"openrouter\"\n")
    } else {
        format!("{content}\n[final_review]\narbiter_backend = \"openrouter\"\n")
    };
    fs::write(&workspace_toml, patched).unwrap();

    let config = EffectiveConfig::load(temp.path()).unwrap();
    let resolver = ralph_burning::contexts::agent_execution::service::BackendResolver::new();
    let plan = engine::resolve_stage_plan(&[StageId::FinalReview], &resolver, None).unwrap();

    let adapter = StubBackendAdapter::default();
    let result = engine::preflight_check(&adapter, &config, 1, &plan).await;
    match result {
        Err(AppError::PreflightFailed { stage_id, details }) => {
            assert_eq!(stage_id, StageId::FinalReview);
            assert!(
                details.contains("arbiter"),
                "expected arbiter-specific preflight failure, got: {details}"
            );
        }
        other => panic!("expected arbiter preflight failure, got: {other:?}"),
    }
}

#[tokio::test]
async fn preflight_check_honours_final_review_arbiter_override() {
    let temp = tempdir().unwrap();
    setup_workspace(temp.path());

    // Set the default planner family to an unavailable backend, but override
    // the final-review arbiter to "claude" which is available by default.
    // If preflight correctly uses the dedicated arbiter override, it should succeed.
    let workspace_toml = workspace_config_path(temp.path());
    let content = fs::read_to_string(&workspace_toml).unwrap();
    let patched = format!(
        "{content}\n[workflow]\nplanner_backend = \"openrouter\"\n\n[final_review]\narbiter_backend = \"claude\"\n"
    );
    fs::write(&workspace_toml, patched).unwrap();

    let config = EffectiveConfig::load(temp.path()).unwrap();
    let resolver = ralph_burning::contexts::agent_execution::service::BackendResolver::new();
    let plan = engine::resolve_stage_plan(&[StageId::FinalReview], &resolver, None).unwrap();

    let adapter = StubBackendAdapter::default();
    let result = engine::preflight_check(&adapter, &config, 1, &plan).await;
    assert!(
        result.is_ok(),
        "preflight should succeed when final_review.arbiter_backend overrides \
         an unavailable workflow.planner_backend, got: {result:?}"
    );
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

/// A journal store that fails specifically when final_review tries to commit a
/// completion_round_advanced event. This exercises the gap where the run
/// snapshot is already advanced but the journal has not caught up yet.
struct FinalReviewRoundAdvanceFailingJournalStore;

impl JournalStorePort for FinalReviewRoundAdvanceFailingJournalStore {
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
        let event = journal::deserialize_event(line)?;
        if event.event_type == JournalEventType::CompletionRoundAdvanced
            && event
                .details
                .get("source_stage")
                .and_then(|value| value.as_str())
                == Some(StageId::FinalReview.as_str())
        {
            return Err(AppError::Io(std::io::Error::other(
                "simulated final-review completion_round_advanced failure",
            )));
        }
        FsJournalStore.append_event(base_dir, project_id, line)
    }
}

/// A journal store that fails specifically when completion_panel tries to
/// commit a completion_round_advanced event. This lets tests inspect the
/// persisted round-restart snapshot before the journal catches up.
struct CompletionPanelRoundAdvanceFailingJournalStore;

impl JournalStorePort for CompletionPanelRoundAdvanceFailingJournalStore {
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
        let event = journal::deserialize_event(line)?;
        if event.event_type == JournalEventType::CompletionRoundAdvanced
            && event
                .details
                .get("source_stage")
                .and_then(|value| value.as_str())
                == Some(StageId::CompletionPanel.as_str())
        {
            return Err(AppError::Io(std::io::Error::other(
                "simulated completion-panel completion_round_advanced failure",
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

#[derive(Clone)]
struct RecordingSnapshotWriteStore {
    writes: Arc<Mutex<Vec<RunSnapshot>>>,
}

impl RecordingSnapshotWriteStore {
    fn new() -> Self {
        Self {
            writes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn writes(&self) -> Vec<RunSnapshot> {
        self.writes
            .lock()
            .expect("recording snapshot write lock poisoned")
            .clone()
    }
}

impl RunSnapshotWritePort for RecordingSnapshotWriteStore {
    fn write_run_snapshot(
        &self,
        base_dir: &Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        snapshot: &RunSnapshot,
    ) -> AppResult<()> {
        self.writes
            .lock()
            .expect("recording snapshot write lock poisoned")
            .push(snapshot.clone());
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
    let payloads_dir = project_root(base_dir, "stage-entry-fail").join("history/payloads");
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
    let payloads_dir = project_root(base_dir, "run-started-fail").join("history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(payload_count, 0, "no payloads should exist");
}

#[tokio::test]
async fn bead_backed_run_started_failure_does_not_open_milestone_lineage() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();
    let now = Utc::now();

    setup_workspace(base_dir);
    let milestone = create_milestone(
        &ralph_burning::adapters::fs::FsMilestoneStore,
        base_dir,
        CreateMilestoneInput {
            id: "ms-alpha".to_owned(),
            name: "Alpha".to_owned(),
            description: "Test milestone".to_owned(),
        },
        now,
    )
    .unwrap();
    persist_plan(
        &ralph_burning::adapters::fs::FsMilestoneSnapshotStore,
        &ralph_burning::adapters::fs::FsMilestoneJournalStore,
        &ralph_burning::adapters::fs::FsMilestonePlanStore,
        base_dir,
        &milestone.id,
        &sample_milestone_bundle("ms-alpha"),
        now,
    )
    .unwrap();

    let pid = create_milestone_task_project(
        base_dir,
        "bead-run-started-fail",
        TaskSource {
            milestone_id: milestone.id.to_string(),
            bead_id: "ms-alpha.bead-2".to_owned(),
            parent_epic_id: None,
            origin: TaskOrigin::Milestone,
            plan_hash: Some("plan-v1".to_owned()),
            plan_version: Some(1),
        },
    );

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();
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

    let task_runs = read_task_runs(
        &ralph_burning::adapters::fs::FsTaskRunLineageStore,
        base_dir,
        &milestone.id,
    )
    .unwrap();
    assert!(
        task_runs.is_empty(),
        "milestone lineage must remain untouched when run_started is not durable"
    );
}

#[tokio::test]
async fn bead_backed_run_resumed_failure_does_not_open_milestone_lineage() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();
    let now = Utc::now();

    setup_workspace(base_dir);
    let milestone = create_milestone(
        &ralph_burning::adapters::fs::FsMilestoneStore,
        base_dir,
        CreateMilestoneInput {
            id: "ms-alpha".to_owned(),
            name: "Alpha".to_owned(),
            description: "Test milestone".to_owned(),
        },
        now,
    )
    .unwrap();

    let pid = create_milestone_task_project(
        base_dir,
        "bead-run-resumed-fail",
        TaskSource {
            milestone_id: milestone.id.to_string(),
            bead_id: "ms-alpha.bead-2".to_owned(),
            parent_epic_id: None,
            origin: TaskOrigin::Milestone,
            plan_hash: Some("plan-v1".to_owned()),
            plan_version: Some(1),
        },
    );

    let run_id = RunId::new("run-resume-failure").unwrap();
    let snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::new(
                    StageId::Implementation,
                    1,
                    1,
                    1,
                )
                .unwrap(),
                started_at: now,
                prompt_hash_at_cycle_start: FsProjectStore
                    .read_project_record(base_dir, &pid)
                    .unwrap()
                    .prompt_hash,
                prompt_hash_at_stage_start: FsProjectStore
                    .read_project_record(base_dir, &pid)
                    .unwrap()
                    .prompt_hash,
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        status: RunStatus::Failed,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(20),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "failed at implementation".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    FsRunSnapshotWriteStore
        .write_run_snapshot(base_dir, &pid, &snapshot)
        .unwrap();

    for event in [
        journal::run_started_event(2, now, &run_id, StageId::PromptReview, 20),
        journal::stage_completed_event(
            3,
            now,
            &run_id,
            StageId::PromptReview,
            1,
            1,
            "prompt-review-payload",
            "prompt-review-artifact",
        ),
        journal::stage_completed_event(
            4,
            now,
            &run_id,
            StageId::Planning,
            1,
            1,
            "planning-payload",
            "planning-artifact",
        ),
    ] {
        FsJournalStore
            .append_event(base_dir, &pid, &journal::serialize_event(&event).unwrap())
            .unwrap();
    }

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();
    let failing_journal = FailingJournalStore::new(1);

    let result = engine::resume_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &failing_journal,
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
        result.is_err(),
        "resume should fail when run_resumed is not durable"
    );

    let task_runs = read_task_runs(
        &ralph_burning::adapters::fs::FsTaskRunLineageStore,
        base_dir,
        &milestone.id,
    )
    .unwrap();
    assert!(
        task_runs.is_empty(),
        "milestone lineage must remain untouched when run_resumed is not durable"
    );
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
    let payloads_dir = project_root(base_dir, "journal-fail").join("history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(
        payload_count, 3,
        "3 supporting records (refiner + 2 validators) should remain; primary rolled back"
    );

    let artifacts_dir = project_root(base_dir, "journal-fail").join("history/artifacts");
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
    let payloads_dir = project_root(base_dir, "snap-fail").join("history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(
        payload_count, 4,
        "completed panel stage payloads should remain durable"
    );

    let artifacts_dir = project_root(base_dir, "snap-fail").join("history/artifacts");
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
            let project_root = project_root(base_dir, project_id.as_str());
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
    let payloads_dir = project_root(base_dir, "leak-cleanup").join("history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(
        payload_count, 0,
        "leaked payload should have been cleaned up by engine, found {} files",
        payload_count
    );

    let artifacts_dir = project_root(base_dir, "leak-cleanup").join("history/artifacts");
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
    let payloads_dir = project_root(base_dir, "pa-write-fail").join("history/payloads");
    let payload_count = fs::read_dir(&payloads_dir).unwrap().count();
    assert_eq!(
        payload_count, 0,
        "no payloads should exist after write failure"
    );

    let artifacts_dir = project_root(base_dir, "pa-write-fail").join("history/artifacts");
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
struct IterativePlanAdapter {
    inner: StubBackendAdapter,
    plan_calls: Arc<AtomicU32>,
}

impl IterativePlanAdapter {
    fn new() -> Self {
        Self {
            inner: StubBackendAdapter::default(),
            plan_calls: Arc::new(AtomicU32::new(0)),
        }
    }

    fn plan_calls(&self) -> u32 {
        self.plan_calls.load(Ordering::SeqCst)
    }
}

impl AgentExecutionPort for IterativePlanAdapter {
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
        if request.contract.stage_id() == Some(StageId::PlanAndImplement) {
            let call = self.plan_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call == 1 {
                fs::write(
                    request.working_dir.join("iterative-output.txt"),
                    "changed once\n",
                )
                .expect("write iterative output");
            }
        }

        self.inner.invoke(request).await
    }

    async fn cancel(&self, invocation_id: &str) -> AppResult<()> {
        self.inner.cancel(invocation_id).await
    }
}

#[derive(Clone)]
struct DirtyFileIterativePlanAdapter {
    inner: StubBackendAdapter,
    plan_calls: Arc<AtomicU32>,
}

impl DirtyFileIterativePlanAdapter {
    fn new() -> Self {
        Self {
            inner: StubBackendAdapter::default(),
            plan_calls: Arc::new(AtomicU32::new(0)),
        }
    }
}

impl AgentExecutionPort for DirtyFileIterativePlanAdapter {
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
        if request.contract.stage_id() == Some(StageId::PlanAndImplement) {
            let call = self.plan_calls.fetch_add(1, Ordering::SeqCst) + 1;
            let path = request.working_dir.join("iterative-output.txt");
            match call {
                1 => fs::write(&path, "changed once\n").expect("write first iterative output"),
                2 => fs::write(&path, "changed twice\n").expect("write second iterative output"),
                _ => {}
            }
        }

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

#[tokio::test(start_paused = true)]
async fn retry_exhaustion_transitions_run_to_failed_state() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "retry-exhaustion");

    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::Implementation, 5),
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
    assert_eq!(implementation_entered.len(), 5);

    let implementation_failed =
        stage_events(&events, JournalEventType::StageFailed, "implementation");
    assert_eq!(implementation_failed.len(), 5);
    assert_eq!(implementation_failed[0].details["will_retry"], true);
    assert_eq!(implementation_failed[1].details["will_retry"], true);
    assert_eq!(implementation_failed[2].details["will_retry"], true);
    assert_eq!(implementation_failed[3].details["will_retry"], true);
    assert_eq!(implementation_failed[4].details["will_retry"], false);
}

#[tokio::test(start_paused = true)]
async fn stage_failed_runtime_log_includes_failure_class_and_error_details() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "runtime-log-diag");

    // Fail implementation once then succeed — the first failure should
    // produce a stage_failed entry in the runtime log with failure_class
    // and the propagated error details.
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

    // The run should succeed (transient failure retried successfully).
    assert!(result.is_ok(), "{result:?}");

    // Read the runtime log and find stage_failed entries.
    let runtime_log_path =
        project_root(base_dir, "runtime-log-diag").join("runtime/logs/run.ndjson");
    let runtime_logs = fs::read_to_string(&runtime_log_path)
        .expect("runtime log should exist after a run with failures");

    let stage_failed_lines: Vec<&str> = runtime_logs
        .lines()
        .filter(|line| line.contains("stage_failed:"))
        .collect();
    assert!(
        !stage_failed_lines.is_empty(),
        "runtime log should contain at least one stage_failed entry"
    );

    let first_failed = stage_failed_lines[0];
    assert!(
        first_failed.contains("failure_class="),
        "stage_failed entry should include failure_class=: {first_failed}"
    );
    assert!(
        first_failed.contains("transport_failure"),
        "failure_class should be transport_failure for stub transient failures: {first_failed}"
    );
    assert!(
        first_failed.contains("error="),
        "stage_failed entry should include error=: {first_failed}"
    );
    assert!(
        first_failed.contains("stub adapter configured to fail"),
        "error should contain the stub adapter's failure detail: {first_failed}"
    );
    assert!(
        first_failed.contains("invocation_id="),
        "stage_failed entry should include invocation_id=: {first_failed}"
    );
}

#[tokio::test(start_paused = true)]
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

/// Snapshot write store that fails specifically during the pre-backoff write.
/// Delegates to `FsRunSnapshotWriteStore` for all writes except when the
/// snapshot is in the "retrying" state (status=Failed, status_summary starts
/// with "retrying"), simulating a disk error during the backoff window.
struct BackoffFailingSnapshotWriteStore;

impl RunSnapshotWritePort for BackoffFailingSnapshotWriteStore {
    fn write_run_snapshot(
        &self,
        base_dir: &Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        snapshot: &RunSnapshot,
    ) -> AppResult<()> {
        if snapshot.status == RunStatus::Failed && snapshot.status_summary.starts_with("retrying") {
            return Err(AppError::Io(std::io::Error::other(
                "simulated disk error during pre-backoff snapshot write",
            )));
        }
        FsRunSnapshotWriteStore.write_run_snapshot(base_dir, project_id, snapshot)
    }
}

/// When the pre-backoff snapshot write fails, the engine must route the error
/// through fail_run_result (not bare `?`) so the run ends in a recoverable
/// Failed state with a run_failed journal event.
#[tokio::test(start_paused = true)]
async fn snapshot_write_failure_during_backoff_routes_through_fail_run() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "snap-backoff-fail");

    // 1 transient failure triggers the backoff path; the snapshot write
    // during backoff will fail via BackoffFailingSnapshotWriteStore.
    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::Implementation, 1),
    );
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &BackoffFailingSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    // (a) The engine must return an error.
    assert!(
        result.is_err(),
        "run should fail when pre-backoff snapshot write fails"
    );

    // (b) The snapshot on disk must be in Failed state (fail_run_result
    // re-attempts the write, which succeeds because the snapshot is no
    // longer in the "retrying" state — it has the fail_run summary).
    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(
        snapshot.status,
        RunStatus::Failed,
        "run must end in Failed state, not stranded in Running"
    );
    assert!(snapshot.active_run.is_none());

    // (c) The journal must contain the stage_failed event (durable before
    // the snapshot write was attempted).
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let impl_failed = stage_events(&events, JournalEventType::StageFailed, "implementation");
    assert!(
        !impl_failed.is_empty(),
        "stage_failed event must be present in journal"
    );
    assert_eq!(impl_failed[0].details["will_retry"], true);

    // (d) A run_failed journal event should be present (from fail_run_result).
    let run_failed: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::RunFailed)
        .collect();
    assert_eq!(
        run_failed.len(),
        1,
        "fail_run_result should emit a run_failed journal event"
    );
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

#[tokio::test(start_paused = true)]
async fn resume_from_failed_run_skips_completed_stages() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "resume-failed");
    let config = EffectiveConfig::load(base_dir).unwrap();

    let failing_agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::Implementation, 5),
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
    assert_eq!(implementation_entered.len(), 6);

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
async fn resume_uses_interrupted_cycle_prompt_baseline_instead_of_project_record_hash() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "resume-prompt-baseline");
    let project_root = project_root(base_dir, pid.as_str());
    fs::write(
        project_root.join("config.toml"),
        "[workflow]\nprompt_change_action = \"abort\"\n",
    )
    .unwrap();
    let config =
        EffectiveConfig::load_for_project(base_dir, Some(&pid), Default::default()).unwrap();
    assert_eq!(
        config.run_policy().prompt_change_action,
        ralph_burning::shared::domain::PromptChangeAction::Abort
    );

    let original_cycle_hash = FsProjectStore
        .read_project_record(base_dir, &pid)
        .unwrap()
        .prompt_hash;
    let run_id = RunId::new("run-prompt-baseline").unwrap();
    let started_at = Utc::now();
    let snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::new(
                    StageId::Implementation,
                    1,
                    1,
                    1,
                )
                .unwrap(),
                started_at,
                prompt_hash_at_cycle_start: original_cycle_hash.clone(),
                prompt_hash_at_stage_start: original_cycle_hash.clone(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        status: RunStatus::Failed,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(0),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "failed at implementation".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    FsRunSnapshotWriteStore
        .write_run_snapshot(base_dir, &pid, &snapshot)
        .unwrap();

    let run_started = journal::run_started_event(2, started_at, &run_id, StageId::PromptReview, 20);
    FsJournalStore
        .append_event(
            base_dir,
            &pid,
            &journal::serialize_event(&run_started).unwrap(),
        )
        .unwrap();
    let prompt_review_completed = journal::stage_completed_event(
        3,
        started_at,
        &run_id,
        StageId::PromptReview,
        1,
        1,
        "prompt-review-payload",
        "prompt-review-artifact",
    );
    FsJournalStore
        .append_event(
            base_dir,
            &pid,
            &journal::serialize_event(&prompt_review_completed).unwrap(),
        )
        .unwrap();
    let planning_completed = journal::stage_completed_event(
        4,
        started_at,
        &run_id,
        StageId::Planning,
        1,
        1,
        "planning-payload",
        "planning-artifact",
    );
    FsJournalStore
        .append_event(
            base_dir,
            &pid,
            &journal::serialize_event(&planning_completed).unwrap(),
        )
        .unwrap();

    let changed_prompt = "# Test prompt\n\nChanged after failure.\n";
    fs::write(project_root.join("prompt.md"), changed_prompt).unwrap();
    let changed_prompt_hash = ralph_burning::adapters::fs::FileSystem::prompt_hash(changed_prompt);
    assert_ne!(original_cycle_hash, changed_prompt_hash);

    let mut project_record = FsProjectStore.read_project_record(base_dir, &pid).unwrap();
    project_record.prompt_hash = changed_prompt_hash;
    fs::write(
        project_root.join("project.toml"),
        toml::to_string_pretty(&project_record).unwrap(),
    )
    .unwrap();

    let resume_result = engine::resume_standard_run(
        &build_agent_service(),
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
    assert!(resume_result.is_err(), "resume should fail on prompt drift");
    let resume_error = resume_result.unwrap_err().to_string();
    assert!(
        resume_error.contains("prompt hash mismatch on resume"),
        "unexpected resume error: {resume_error}"
    );
    assert!(
        resume_error.contains(&original_cycle_hash),
        "resume error should reference the interrupted cycle baseline: {resume_error}"
    );

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert!(
        events
            .iter()
            .all(|event| event.event_type != JournalEventType::RunResumed),
        "resume should fail before persisting run_resumed"
    );
}

#[tokio::test]
async fn continue_resume_keeps_original_cycle_prompt_baseline_for_later_resumes() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "resume-prompt-continue-baseline");
    let project_root = project_root(base_dir, pid.as_str());
    fs::write(
        project_root.join("config.toml"),
        "[workflow]\nprompt_change_action = \"continue\"\n",
    )
    .unwrap();
    let continue_config =
        EffectiveConfig::load_for_project(base_dir, Some(&pid), Default::default()).unwrap();

    let original_cycle_hash = FsProjectStore
        .read_project_record(base_dir, &pid)
        .unwrap()
        .prompt_hash;
    let run_id = RunId::new("run-prompt-continue-baseline").unwrap();
    let started_at = Utc::now();
    let snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::new(
                    StageId::Implementation,
                    1,
                    1,
                    1,
                )
                .unwrap(),
                started_at,
                prompt_hash_at_cycle_start: original_cycle_hash.clone(),
                prompt_hash_at_stage_start: original_cycle_hash.clone(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        status: RunStatus::Failed,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(0),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "failed at implementation".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    FsRunSnapshotWriteStore
        .write_run_snapshot(base_dir, &pid, &snapshot)
        .unwrap();

    for event in [
        journal::run_started_event(2, started_at, &run_id, StageId::PromptReview, 20),
        journal::stage_completed_event(
            3,
            started_at,
            &run_id,
            StageId::PromptReview,
            1,
            1,
            "prompt-review-payload",
            "prompt-review-artifact",
        ),
        journal::stage_completed_event(
            4,
            started_at,
            &run_id,
            StageId::Planning,
            1,
            1,
            "planning-payload",
            "planning-artifact",
        ),
    ] {
        FsJournalStore
            .append_event(base_dir, &pid, &journal::serialize_event(&event).unwrap())
            .unwrap();
    }
    for (stage_id, payload_id, artifact_id) in [
        (
            StageId::PromptReview,
            "prompt-review-payload",
            "prompt-review-artifact",
        ),
        (StageId::Planning, "planning-payload", "planning-artifact"),
    ] {
        FsPayloadArtifactWriteStore
            .write_payload_artifact_pair(
                base_dir,
                &pid,
                &PayloadRecord {
                    payload_id: payload_id.to_owned(),
                    stage_id,
                    cycle: 1,
                    attempt: 1,
                    created_at: started_at,
                    payload: json!({
                        "stage": stage_id.as_str(),
                        "cycle": 1,
                    }),
                    record_kind: RecordKind::StagePrimary,
                    producer: None,
                    completion_round: 1,
                },
                &ArtifactRecord {
                    artifact_id: artifact_id.to_owned(),
                    payload_id: payload_id.to_owned(),
                    stage_id,
                    created_at: started_at,
                    content: format!("artifact for {}", stage_id.as_str()),
                    record_kind: RecordKind::StagePrimary,
                    producer: None,
                    completion_round: 1,
                },
            )
            .unwrap();
    }

    let first_changed_prompt = "# Test prompt\n\nFirst changed prompt.\n";
    fs::write(project_root.join("prompt.md"), first_changed_prompt).unwrap();
    let first_changed_hash =
        ralph_burning::adapters::fs::FileSystem::prompt_hash(first_changed_prompt);

    let first_resume_result = engine::resume_standard_run(
        &build_agent_service_with_adapter(
            StubBackendAdapter::default().with_invoke_failure(StageId::Implementation),
        ),
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &continue_config,
    )
    .await;
    assert!(
        first_resume_result.is_err(),
        "resume should fail at implementation after continuing past prompt drift"
    );

    let after_continue_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    let interrupted_run = after_continue_snapshot
        .interrupted_run
        .as_ref()
        .expect("failed resume should preserve interrupted run metadata");
    assert_eq!(
        interrupted_run.prompt_hash_at_cycle_start,
        original_cycle_hash
    );
    assert_eq!(
        interrupted_run.prompt_hash_at_stage_start,
        first_changed_hash
    );

    fs::write(
        project_root.join("config.toml"),
        "[workflow]\nprompt_change_action = \"abort\"\n",
    )
    .unwrap();
    let abort_config =
        EffectiveConfig::load_for_project(base_dir, Some(&pid), Default::default()).unwrap();

    let second_changed_prompt = "# Test prompt\n\nSecond changed prompt.\n";
    fs::write(project_root.join("prompt.md"), second_changed_prompt).unwrap();
    let second_changed_hash =
        ralph_burning::adapters::fs::FileSystem::prompt_hash(second_changed_prompt);

    let second_resume_result = engine::resume_standard_run(
        &build_agent_service(),
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &abort_config,
    )
    .await;
    assert!(
        second_resume_result.is_err(),
        "second resume should fail on prompt drift under abort"
    );
    let resume_error = second_resume_result.unwrap_err().to_string();
    assert!(
        resume_error.contains(&original_cycle_hash),
        "resume error should keep the original cycle baseline: {resume_error}"
    );
    assert!(
        resume_error.contains(&second_changed_hash),
        "resume error should reference the current prompt hash: {resume_error}"
    );
    assert!(
        !resume_error.contains(&first_changed_hash),
        "resume error should not treat the continued prompt hash as the cycle baseline: {resume_error}"
    );
}

#[tokio::test]
async fn continue_resume_keeps_original_cycle_prompt_baseline_after_completion_round_restart() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "resume-prompt-continue-round-baseline");
    let project_root = project_root(base_dir, pid.as_str());
    fs::write(
        project_root.join("config.toml"),
        "[workflow]\nprompt_change_action = \"continue\"\n",
    )
    .unwrap();
    let continue_config =
        EffectiveConfig::load_for_project(base_dir, Some(&pid), Default::default()).unwrap();

    let original_cycle_hash = FsProjectStore
        .read_project_record(base_dir, &pid)
        .unwrap()
        .prompt_hash;
    let run_id = RunId::new("run-prompt-continue-round-baseline").unwrap();
    let started_at = Utc::now();
    let snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::new(
                    StageId::Implementation,
                    1,
                    1,
                    1,
                )
                .unwrap(),
                started_at,
                prompt_hash_at_cycle_start: original_cycle_hash.clone(),
                prompt_hash_at_stage_start: original_cycle_hash.clone(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        status: RunStatus::Failed,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(0),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "failed at implementation".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    FsRunSnapshotWriteStore
        .write_run_snapshot(base_dir, &pid, &snapshot)
        .unwrap();

    for event in [
        journal::run_started_event(2, started_at, &run_id, StageId::PromptReview, 20),
        journal::stage_completed_event(
            3,
            started_at,
            &run_id,
            StageId::PromptReview,
            1,
            1,
            "prompt-review-payload",
            "prompt-review-artifact",
        ),
        journal::stage_completed_event(
            4,
            started_at,
            &run_id,
            StageId::Planning,
            1,
            1,
            "planning-payload",
            "planning-artifact",
        ),
    ] {
        FsJournalStore
            .append_event(base_dir, &pid, &journal::serialize_event(&event).unwrap())
            .unwrap();
    }
    for (stage_id, payload_id, artifact_id) in [
        (
            StageId::PromptReview,
            "prompt-review-payload",
            "prompt-review-artifact",
        ),
        (StageId::Planning, "planning-payload", "planning-artifact"),
    ] {
        FsPayloadArtifactWriteStore
            .write_payload_artifact_pair(
                base_dir,
                &pid,
                &PayloadRecord {
                    payload_id: payload_id.to_owned(),
                    stage_id,
                    cycle: 1,
                    attempt: 1,
                    created_at: started_at,
                    payload: json!({
                        "stage": stage_id.as_str(),
                        "cycle": 1,
                    }),
                    record_kind: RecordKind::StagePrimary,
                    producer: None,
                    completion_round: 1,
                },
                &ArtifactRecord {
                    artifact_id: artifact_id.to_owned(),
                    payload_id: payload_id.to_owned(),
                    stage_id,
                    created_at: started_at,
                    content: format!("artifact for {}", stage_id.as_str()),
                    record_kind: RecordKind::StagePrimary,
                    producer: None,
                    completion_round: 1,
                },
            )
            .unwrap();
    }

    let first_changed_prompt = "# Test prompt\n\nFirst changed prompt.\n";
    fs::write(project_root.join("prompt.md"), first_changed_prompt).unwrap();
    let first_changed_hash =
        ralph_burning::adapters::fs::FileSystem::prompt_hash(first_changed_prompt);
    let failing_journal = CompletionPanelRoundAdvanceFailingJournalStore;

    let first_resume_result = engine::resume_standard_run(
        &build_agent_service_with_adapter(
            StubBackendAdapter::default().with_stage_payload_sequence(
                StageId::CompletionPanel,
                vec![conditionally_approved_payload(&["restart from completion"])],
            ),
        ),
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &failing_journal,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &continue_config,
    )
    .await;
    assert!(
        first_resume_result.is_err(),
        "resume should fail after persisting the completion round restart snapshot"
    );
    let first_resume_error = first_resume_result.unwrap_err().to_string();

    let after_continue_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    let interrupted_run = after_continue_snapshot
        .interrupted_run
        .as_ref()
        .expect("failed round-two planning should preserve interrupted run metadata");
    assert_eq!(
        interrupted_run.stage_cursor.stage,
        StageId::Planning,
        "first resume failed before the completion round restart snapshot was preserved: {first_resume_error}"
    );
    assert_eq!(interrupted_run.stage_cursor.completion_round, 2);
    assert_eq!(
        interrupted_run.prompt_hash_at_cycle_start,
        original_cycle_hash
    );
    assert_eq!(
        interrupted_run.prompt_hash_at_stage_start,
        first_changed_hash
    );

    fs::write(
        project_root.join("config.toml"),
        "[workflow]\nprompt_change_action = \"abort\"\n",
    )
    .unwrap();
    let abort_config =
        EffectiveConfig::load_for_project(base_dir, Some(&pid), Default::default()).unwrap();

    let second_changed_prompt = "# Test prompt\n\nSecond changed prompt.\n";
    fs::write(project_root.join("prompt.md"), second_changed_prompt).unwrap();
    let second_changed_hash =
        ralph_burning::adapters::fs::FileSystem::prompt_hash(second_changed_prompt);

    let second_resume_result = engine::resume_standard_run(
        &build_agent_service(),
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &abort_config,
    )
    .await;
    assert!(
        second_resume_result.is_err(),
        "second resume should fail on prompt drift under abort"
    );
    let resume_error = second_resume_result.unwrap_err().to_string();
    assert!(
        resume_error.contains(&original_cycle_hash),
        "resume error should keep the original cycle baseline after the completion round restart: {resume_error}"
    );
    assert!(
        resume_error.contains(&second_changed_hash),
        "resume error should reference the current prompt hash: {resume_error}"
    );
    assert!(
        !resume_error.contains(&first_changed_hash),
        "resume error should not treat the round-restart prompt hash as the cycle baseline: {resume_error}"
    );
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
    assert!(
        first_result.is_err(),
        "prompt review rejection should fail the run"
    );

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

#[tokio::test(start_paused = true)]
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

#[tokio::test(start_paused = true)]
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
    let amendments_dir = project_root(base_dir, "cr-alpha").join("amendments");
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
        interrupted_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::new(
                    StageId::CompletionPanel,
                    1,
                    1,
                    u32::MAX,
                )
                .unwrap(),
                started_at,
                prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
                prompt_hash_at_stage_start: "prompt-hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        status: RunStatus::Failed,
        cycle_history: vec![],
        completion_rounds: u32::MAX,
        max_completion_rounds: Some(0),
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
        journal::run_started_event(sequence, started_at, &run_id, StageId::PromptReview, 20);
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
        err_msg.contains("completion_round")
            || err_msg.contains("overflow")
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
    assert_eq!(
        snapshot.completion_rounds, 2,
        "terminal failure should not advance the canonical completion round"
    );
    assert_eq!(
        snapshot
            .interrupted_run
            .as_ref()
            .map(|run| run.stage_cursor.completion_round),
        Some(2),
        "interrupted cursor should remain aligned with the canonical completion round"
    );

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
    let run_failed = events
        .iter()
        .find(|e| e.event_type == JournalEventType::RunFailed)
        .expect("run_failed event");
    assert_eq!(run_failed.details["completion_rounds"], 2);
    assert_eq!(run_failed.details["completion_rounds_display"], "3/2");
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
    let amendments_dir = project_root(base_dir, "guard-resume").join("amendments");
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
    assert!(
        FileSystem::read_pid_file(base_dir, &pid).unwrap().is_none(),
        "pid file should be removed when completion is blocked"
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
    assert_eq!(
        amendment_events[0].details["reviewer_sources"][0]["reviewer_id"],
        "reviewer-1"
    );
    assert_eq!(
        amendment_events[0].details["reviewer_sources"][0]["backend_family"],
        "codex"
    );

    let reviewer_completed_events: Vec<_> = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::ReviewerCompleted)
        .collect();
    assert!(
        reviewer_completed_events.iter().any(|event| {
            event.details["reviewer_id"] == "reviewer-1"
                && event.details["phase"] == "proposal"
                && event.details["duration_ms"].as_u64().is_some()
        }),
        "final_review should journal per-reviewer completion timing"
    );

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
    let amendments_dir = project_root(base_dir, "fr-cond").join("amendments");
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
async fn resume_uses_interrupted_final_review_restart_count_when_journal_lags() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "fr-restart-counter-resume");
    EffectiveConfig::set(base_dir, "final_review.max_restarts", "1").unwrap();
    let config = EffectiveConfig::load(base_dir).unwrap();

    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_stage_payload_sequence(
            StageId::FinalReview,
            vec![
                conditionally_approved_payload(&["tighten final wording"]),
                conditionally_approved_payload(&["tighten final wording again"]),
            ],
        ),
    );
    let failing_journal = FinalReviewRoundAdvanceFailingJournalStore;

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
    assert!(
        first_result.is_err(),
        "run should fail after persisting the final-review restart snapshot"
    );

    let failed_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(failed_snapshot.status, RunStatus::Failed);
    assert_eq!(failed_snapshot.completion_rounds, 2);
    let interrupted = failed_snapshot
        .interrupted_run
        .as_ref()
        .expect("failed run should preserve interrupted active_run");
    assert_eq!(interrupted.stage_cursor.stage, StageId::Planning);
    assert_eq!(interrupted.stage_cursor.completion_round, 2);
    assert_eq!(
        interrupted.final_review_restart_count, 1,
        "interrupted snapshot should retain the consumed final-review restart"
    );

    let failed_events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert_eq!(
        failed_events
            .iter()
            .filter(|event| {
                event.event_type == JournalEventType::CompletionRoundAdvanced
                    && event
                        .details
                        .get("source_stage")
                        .and_then(|value| value.as_str())
                        == Some(StageId::FinalReview.as_str())
            })
            .count(),
        0,
        "journal should still lag the snapshot after the injected append failure"
    );

    let resume_result = engine::resume_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &failing_journal,
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
        "resume should honor the interrupted final-review restart count and force-complete instead of attempting another restart: {resume_result:?}"
    );

    let completed_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(completed_snapshot.status, RunStatus::Completed);
    assert_eq!(
        completed_snapshot.completion_rounds, 2,
        "resume should not allow a third completion round when the restart cap is already consumed"
    );

    let resumed_events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert_eq!(
        resumed_events
            .iter()
            .filter(|event| {
                event.event_type == JournalEventType::CompletionRoundAdvanced
                    && event.details.get("source_stage").and_then(|value| value.as_str())
                        == Some(StageId::FinalReview.as_str())
            })
            .count(),
        0,
        "resume should not attempt another final-review completion_round_advanced append once the persisted restart count is at the cap"
    );
    assert_eq!(
        stage_events(&resumed_events, JournalEventType::StageEntered, "planning").len(),
        2,
        "the run should resume at round-two planning only once"
    );
}

#[tokio::test]
async fn resume_warns_when_final_review_arbiter_drifts() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "fr-arbiter-drift-resume");
    let initial_config = EffectiveConfig::load(base_dir).unwrap();

    let initial_result = engine::execute_standard_run(
        &build_agent_service_with_adapter(
            StubBackendAdapter::default().with_invoke_failure(StageId::FinalReview),
        ),
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &initial_config,
    )
    .await;
    assert!(
        initial_result.is_err(),
        "initial run should fail in final_review so resume can exercise drift detection"
    );

    let baseline_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    let baseline_resolution = baseline_snapshot
        .last_stage_resolution_snapshot
        .as_ref()
        .expect("failed final_review run should preserve a resolution snapshot");
    assert_eq!(baseline_resolution.stage_id, StageId::FinalReview);
    let persisted_arbiter = baseline_resolution
        .final_review_arbiter
        .clone()
        .expect("baseline failing run should capture the final-review arbiter");

    EffectiveConfig::set(base_dir, "final_review.arbiter_backend", "claude").unwrap();
    let drift_config = EffectiveConfig::load(base_dir).unwrap();

    let resume_result = engine::resume_standard_run(
        &build_agent_service(),
        &FsRunSnapshotStore,
        &FsRunSnapshotWriteStore,
        &FsJournalStore,
        &FsArtifactStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &drift_config,
    )
    .await;
    assert!(
        resume_result.is_ok(),
        "resume should complete after detecting arbiter drift: {resume_result:?}"
    );

    let completed_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(completed_snapshot.status, RunStatus::Completed);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let warning = events
        .iter()
        .find(|event| {
            event.event_type == JournalEventType::DurableWarning
                && event.details.get("warning_kind").and_then(Value::as_str) == Some("resume_drift")
                && event.details.get("stage_id").and_then(Value::as_str)
                    == Some(StageId::FinalReview.as_str())
        })
        .expect("resume should emit a durable final_review drift warning");
    let old_arbiter = warning
        .details
        .get("details")
        .and_then(|details| details.get("old_resolution"))
        .and_then(|resolution| resolution.get("final_review_arbiter"))
        .expect("durable warning should include the persisted old arbiter");
    let new_arbiter = warning
        .details
        .get("details")
        .and_then(|details| details.get("new_resolution"))
        .and_then(|resolution| resolution.get("final_review_arbiter"))
        .expect("durable warning should include the re-resolved new arbiter");
    assert_eq!(
        old_arbiter.get("backend_family").and_then(Value::as_str),
        Some(persisted_arbiter.backend_family.as_str())
    );
    assert_eq!(
        old_arbiter.get("model_id").and_then(Value::as_str),
        Some(persisted_arbiter.model_id.as_str())
    );
    assert_eq!(
        new_arbiter.get("backend_family").and_then(Value::as_str),
        Some(BackendFamily::Claude.as_str())
    );
    assert_ne!(
        old_arbiter, new_arbiter,
        "resume should compare against the persisted arbiter baseline"
    );
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

    let payloads_dir = project_root(base_dir, "cr-ids").join("history/payloads");
    let artifacts_dir = project_root(base_dir, "cr-ids").join("history/artifacts");

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

    // First call: Complete.
    // Second call (after resume): Complete again.
    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_stage_payload_sequence(
            StageId::CompletionPanel,
            vec![approved_validation_payload(), approved_validation_payload()],
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
        project_root(base_dir, "cr-resume-after-append-fail").join("history/payloads");
    let payload_files: Vec<String> = fs::read_dir(&payloads_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        payload_files
            .iter()
            .any(|name| name.contains("-completion_panel-")),
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
    let payloads_dir = project_root(base_dir, "cr-stage-completed-fail").join("history/payloads");
    let payload_files: Vec<String> = fs::read_dir(&payloads_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert!(
        payload_files
            .iter()
            .any(|name| name.contains("-completion_panel-")),
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
// new commit ordering (ContinueWork writes no stage_completed), a failpoint on
// the completion_round_advanced journal append means no stage_completed or
// completion_round_advanced is persisted, aggregate records are cleaned up,
// and resume restarts from completion_panel.
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
        // New commit ordering: ContinueWork does NOT write stage_completed.
        // completion_round_advanced is the journal commit point. With the
        // failpoint set to allow 17 appends (stages before completion_panel
        // produce 17 events), the 18th append (completion_round_advanced) fails.
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

    let failed_events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    // No stage_completed for completion_panel (ContinueWork path does not
    // write stage_completed; completion_round_advanced is the commit point).
    let completion_completed = failed_events.iter().any(|event| {
        event.event_type == JournalEventType::StageCompleted
            && event.details.get("stage_id").and_then(|v| v.as_str()) == Some("completion_panel")
    });
    assert!(
        !completion_completed,
        "stage_completed for completion_panel should NOT be persisted in ContinueWork path"
    );
    // No completion_round_advanced in journal (that's where we failed).
    assert!(
        failed_events
            .iter()
            .all(|event| event.event_type != JournalEventType::CompletionRoundAdvanced),
        "completion_round_advanced should not be persisted on failpoint"
    );

    // Resume restarts from completion_panel (since no stage_completed or
    // completion_round_advanced is durable). Re-executes the panel.
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

#[tokio::test]
async fn qa_iteration_counter_resets_on_new_cycle_before_completion_round_resume() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "qa-cap-round-restart");
    EffectiveConfig::set(base_dir, "workflow.max_qa_iterations", "1").unwrap();
    let config = EffectiveConfig::load(base_dir).unwrap();

    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default()
            .with_stage_payload_sequence(
                StageId::Qa,
                vec![
                    request_changes_payload(&["cycle-one-fix"]),
                    approved_validation_payload(),
                    request_changes_payload(&["round-two-fix"]),
                    approved_validation_payload(),
                ],
            )
            .with_stage_payload_sequence(
                StageId::CompletionPanel,
                vec![
                    conditionally_approved_payload(&["restart from completion"]),
                    approved_validation_payload(),
                ],
            ),
    );
    let snapshot_writes = RecordingSnapshotWriteStore::new();
    let failing_journal = FailingJournalStore::new(26);

    let first_result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &snapshot_writes,
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
        first_result.is_err(),
        "run should fail after persisting completion_round_advanced"
    );

    let round_restart_snapshot = snapshot_writes
        .writes()
        .into_iter()
        .find(|snapshot| {
            snapshot.status == RunStatus::Running
                && snapshot.active_run.as_ref().is_some_and(|active_run| {
                    active_run.stage_cursor.stage == StageId::Planning
                        && active_run.stage_cursor.completion_round == 2
                        && active_run.qa_iterations_current_cycle == 0
                        && active_run.review_iterations_current_cycle == 0
                })
        })
        .expect("round restart snapshot should reset counters for the new cycle");
    let active_run = round_restart_snapshot
        .active_run
        .expect("round restart snapshot should include active run metadata");
    assert_eq!(active_run.qa_iterations_current_cycle, 0);
    assert_eq!(active_run.review_iterations_current_cycle, 0);

    let failed_events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    assert_eq!(
        failed_events
            .iter()
            .filter(|event| event.event_type == JournalEventType::CompletionRoundAdvanced)
            .count(),
        1,
        "completion round advance should be committed before the forced failure"
    );
    assert_eq!(
        failed_events
            .iter()
            .filter(|event| event.event_type == JournalEventType::CycleAdvanced)
            .count(),
        1,
        "only the original QA remediation should have advanced the cycle"
    );

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
        "resume should allow a fresh QA cap budget in the new cycle: {resume_result:?}"
    );

    let resumed_events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let run_resumed = resumed_events
        .iter()
        .find(|event| event.event_type == JournalEventType::RunResumed)
        .expect("run_resumed");
    assert_eq!(run_resumed.details["resume_stage"], "planning");
    assert_eq!(run_resumed.details["completion_round"], 2);
    assert_eq!(
        resumed_events
            .iter()
            .filter(|event| event.event_type == JournalEventType::CycleAdvanced)
            .count(),
        2,
        "resume should be able to advance into a new remediation cycle after the round-two QA failure"
    );

    let completed_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(completed_snapshot.status, RunStatus::Completed);
}

#[tokio::test]
async fn resume_uses_current_cycle_review_counter_instead_of_prior_cycles() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "resume-review-counter-reset");
    EffectiveConfig::set(base_dir, "workflow.max_review_iterations", "3").unwrap();
    let config = EffectiveConfig::load(base_dir).unwrap();

    let run_id = RunId::new("run-review-counter-reset").unwrap();
    let started_at = Utc::now();
    let prompt_hash = FsProjectStore
        .read_project_record(base_dir, &pid)
        .unwrap()
        .prompt_hash;
    let snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::new(
                    StageId::Planning,
                    2,
                    1,
                    2,
                )
                .unwrap(),
                started_at,
                prompt_hash_at_cycle_start: prompt_hash.clone(),
                prompt_hash_at_stage_start: prompt_hash,
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        status: RunStatus::Failed,
        cycle_history: vec![
            ralph_burning::contexts::project_run_record::model::CycleHistoryEntry {
                cycle: 2,
                stage_id: StageId::Implementation,
                started_at,
                completed_at: None,
            },
        ],
        completion_rounds: 2,
        max_completion_rounds: Some(0),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "failed at planning".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    FsRunSnapshotWriteStore
        .write_run_snapshot(base_dir, &pid, &snapshot)
        .unwrap();

    let run_started = journal::run_started_event(2, started_at, &run_id, StageId::PromptReview, 20);
    FsJournalStore
        .append_event(
            base_dir,
            &pid,
            &journal::serialize_event(&run_started).unwrap(),
        )
        .unwrap();

    let write_stage_completed =
        |sequence: u64, stage_id: StageId, cycle: u32, payload_id: &str, artifact_id: &str| {
            FsPayloadArtifactWriteStore
                .write_payload_artifact_pair(
                    base_dir,
                    &pid,
                    &PayloadRecord {
                        payload_id: payload_id.to_owned(),
                        stage_id,
                        cycle,
                        attempt: 1,
                        created_at: started_at,
                        payload: json!({
                            "stage": stage_id.as_str(),
                            "cycle": cycle,
                        }),
                        record_kind: RecordKind::StagePrimary,
                        producer: None,
                        completion_round: if cycle == 2 { 2 } else { 1 },
                    },
                    &ArtifactRecord {
                        artifact_id: artifact_id.to_owned(),
                        payload_id: payload_id.to_owned(),
                        stage_id,
                        created_at: started_at,
                        content: format!("artifact for {}", stage_id.as_str()),
                        record_kind: RecordKind::StagePrimary,
                        producer: None,
                        completion_round: if cycle == 2 { 2 } else { 1 },
                    },
                )
                .unwrap();

            let event = journal::stage_completed_event(
                sequence,
                started_at,
                &run_id,
                stage_id,
                cycle,
                1,
                payload_id,
                artifact_id,
            );
            FsJournalStore
                .append_event(base_dir, &pid, &journal::serialize_event(&event).unwrap())
                .unwrap();
        };

    write_stage_completed(
        3,
        StageId::PromptReview,
        1,
        "prompt-review-payload",
        "prompt-review-artifact",
    );
    write_stage_completed(
        4,
        StageId::Planning,
        1,
        "planning-1-payload",
        "planning-1-artifact",
    );
    write_stage_completed(
        5,
        StageId::Implementation,
        1,
        "implementation-1-payload",
        "implementation-1-artifact",
    );
    write_stage_completed(6, StageId::Qa, 1, "qa-1-payload", "qa-1-artifact");
    write_stage_completed(
        7,
        StageId::Review,
        1,
        "review-1-payload",
        "review-1-artifact",
    );

    let cycle_advanced = journal::cycle_advanced_event(
        8,
        started_at,
        &run_id,
        StageId::Review,
        1,
        2,
        StageId::Implementation,
    );
    FsJournalStore
        .append_event(
            base_dir,
            &pid,
            &journal::serialize_event(&cycle_advanced).unwrap(),
        )
        .unwrap();

    write_stage_completed(
        9,
        StageId::Implementation,
        2,
        "implementation-2-payload",
        "implementation-2-artifact",
    );
    write_stage_completed(10, StageId::Qa, 2, "qa-2-payload", "qa-2-artifact");
    write_stage_completed(
        11,
        StageId::Review,
        2,
        "review-2-payload",
        "review-2-artifact",
    );

    let completion_round_advanced = journal::completion_round_advanced_event(
        12,
        started_at,
        &run_id,
        StageId::CompletionPanel,
        1,
        2,
        1,
        20,
    );
    FsJournalStore
        .append_event(
            base_dir,
            &pid,
            &journal::serialize_event(&completion_round_advanced).unwrap(),
        )
        .unwrap();

    let resume_result = engine::resume_standard_run(
        &build_agent_service_with_adapter(
            StubBackendAdapter::default().with_stage_payload_sequence(
                StageId::Review,
                vec![
                    request_changes_payload(&["round-two-review-fix"]),
                    approved_validation_payload(),
                ],
            ),
        ),
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
        "resume should not inherit review cap usage from a prior cycle: {resume_result:?}"
    );

    let completed_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(completed_snapshot.status, RunStatus::Completed);

    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let run_resumed = events
        .iter()
        .find(|event| event.event_type == JournalEventType::RunResumed)
        .expect("run_resumed");
    assert_eq!(run_resumed.details["resume_stage"], "planning");
    assert_eq!(run_resumed.details["cycle"], 2);
    assert_eq!(run_resumed.details["completion_round"], 2);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == JournalEventType::CycleAdvanced)
            .count(),
        2,
        "the round-two review request should still be allowed to open a fresh remediation cycle"
    );
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
        1,
        "ContinueWork path no longer writes stage_completed; only the final Complete round does"
    );

    // Verify supporting and aggregate records exist.
    let payloads_dir = project_root(base_dir, "cr-full-batch-success").join("history/payloads");
    let payload_files: Vec<String> = fs::read_dir(&payloads_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    // Both completion rounds should have supporting records.
    assert!(
        payload_files
            .iter()
            .any(|name| name.contains("-completion_panel-") && name.contains("-cr1")),
        "round 1 completion records should exist: {payload_files:?}"
    );
    assert!(
        payload_files
            .iter()
            .any(|name| name.contains("-completion_panel-") && name.contains("-cr2")),
        "round 2 completion records should exist: {payload_files:?}"
    );
    // Aggregate records should exist for both rounds.
    assert!(
        payload_files
            .iter()
            .any(|name| name.contains("aggregate") && name.contains("-cr1")),
        "round 1 aggregate should exist: {payload_files:?}"
    );
    assert!(
        payload_files
            .iter()
            .any(|name| name.contains("aggregate") && name.contains("-cr2")),
        "round 2 aggregate should exist: {payload_files:?}"
    );
}

// Regression: completion panel commit failure after cursor advance must
// retain last_stage_resolution_snapshot so resume drift detection works.
// Previously, the Complete/ContinueWork paths cleared active_run's
// stage_resolution_snapshot before the journal commit point, and fail_run
// would copy that None into last_stage_resolution_snapshot.
#[tokio::test]
async fn completion_panel_commit_failure_retains_resolution_snapshot() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "cr-snapshot-retain");

    // ContinueWork on first call (vote_complete=false via conditionally_approved).
    let agent_service =
        build_agent_service_with_adapter(StubBackendAdapter::default().with_stage_payload(
            StageId::CompletionPanel,
            conditionally_approved_payload(&["fix something"]),
        ));
    let config = EffectiveConfig::load(base_dir).unwrap();

    // Standard flow event numbering:
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
    //   18 completion_round_advanced -> fail here (ContinueWork commit point)
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
    assert!(result.is_err(), "run should fail on commit point append");

    let failed_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(failed_snapshot.status, RunStatus::Failed);
    assert!(failed_snapshot.active_run.is_none());

    // The critical assertion: last_stage_resolution_snapshot must still
    // contain the completion panel's resolution, even though active_run
    // was overwritten with stage_resolution_snapshot: None before the
    // commit point failed.
    assert!(
        failed_snapshot.last_stage_resolution_snapshot.is_some(),
        "last_stage_resolution_snapshot must be retained after completion panel commit failure"
    );
    let snapshot = failed_snapshot.last_stage_resolution_snapshot.unwrap();
    assert_eq!(
        snapshot.stage_id,
        StageId::CompletionPanel,
        "retained snapshot must be for completion_panel"
    );
    assert!(
        !snapshot.completion_completers.is_empty(),
        "retained snapshot must include completion panel members"
    );
}

#[tokio::test]
async fn standard_flow_review_invocation_context_contains_local_validation() {
    // When standard_commands are configured, the Review stage's invocation
    // context must contain a top-level "local_validation" key with evidence
    // from the validation runner.
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "review-ctx-validation");

    let ws_config_path = workspace_config_path(base_dir);
    let mut ws_config = fs::read_to_string(&ws_config_path).unwrap();
    ws_config.push_str(
        "\n[validation]\nstandard_commands = [\"echo validation-evidence-marker\"]\npre_commit_fmt = false\npre_commit_clippy = false\n",
    );
    fs::write(&ws_config_path, ws_config).unwrap();

    let adapter = RecordingAdapter::new(StubBackendAdapter::default());
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
        FlowPreset::Standard,
        &config,
    )
    .await;

    assert!(result.is_ok(), "{result:?}");

    // The Review stage invocation context must contain local_validation at the
    // top level (not nested under "remediation").
    let review_contexts = adapter_handle.contexts_for(StageId::Review);
    assert!(
        !review_contexts.is_empty(),
        "review stage should have been invoked"
    );
    let review_ctx = &review_contexts[0];
    assert!(
        review_ctx.get("local_validation").is_some(),
        "review invocation context must contain top-level local_validation key, got: {review_ctx}"
    );
    let local_val = &review_ctx["local_validation"];
    assert_eq!(
        local_val.get("group").and_then(|v| v.as_str()),
        Some("standard_validation"),
        "local_validation.group must be standard_validation"
    );
    assert!(
        local_val.get("passed").is_some(),
        "local_validation must include passed field"
    );
    // It must NOT be under "remediation".
    assert!(
        review_ctx.get("remediation").is_none(),
        "local_validation evidence should not be nested under remediation"
    );
}

/// Regression test: pre-commit failure triggers remediation, the run is
/// interrupted during that remediation, and resume reconstructs the
/// pre-commit remediation context from durable supporting evidence.
#[tokio::test]
async fn pre_commit_failure_remediation_survives_resume() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "precommit-resume");

    // Enable pre_commit_fmt (and disable others) in workspace config.
    let ws_config_path = workspace_config_path(base_dir);
    let mut ws_config = fs::read_to_string(&ws_config_path).unwrap();
    ws_config.push_str(
        "\n[validation]\npre_commit_fmt = true\npre_commit_clippy = false\npre_commit_nix_build = false\n",
    );
    fs::write(&ws_config_path, ws_config).unwrap();

    // Create a minimal Cargo project in the project root with intentionally
    // bad formatting so `cargo fmt --check` fails.
    let project_root = project_root(base_dir, pid.as_str());
    fs::write(
        project_root.join("Cargo.toml"),
        "[package]\nname = \"test-fmt\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .unwrap();
    fs::create_dir_all(project_root.join("src")).unwrap();
    // Intentionally bad formatting: no spaces, single line.
    fs::write(
        project_root.join("src/main.rs"),
        "fn main(){println!(\"hello\");let x=1;let y=2;let z=x+y;println!(\"{z}\");}",
    )
    .unwrap();

    let config = EffectiveConfig::load(base_dir).unwrap();

    // Use an adapter that succeeds the first Implementation invocation
    // (cycle 1) but fails the second (remediation cycle 2 after pre-commit
    // failure), simulating an interrupted remediation.
    let failing_adapter =
        StubBackendAdapter::default().with_delayed_failure(StageId::Implementation, 1);
    let failing_agent_service = build_agent_service_with_adapter(failing_adapter);

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
        FlowPreset::Standard,
        &config,
    )
    .await;
    // The run should fail because the second Implementation invocation fails.
    assert!(
        first_result.is_err(),
        "expected run to fail during remediation cycle, got: {first_result:?}"
    );

    let snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(
        snapshot.status,
        RunStatus::Failed,
        "run should be in Failed status after interrupted remediation"
    );

    // Verify pre-commit evidence was persisted (supporting record).
    let payloads = FsArtifactStore.list_payloads(base_dir, &pid).unwrap();
    let pre_commit_evidence = payloads.iter().find(|record| {
        record.record_kind == RecordKind::StageSupporting
            && matches!(
                &record.producer,
                Some(ralph_burning::contexts::workflow_composition::panel_contracts::RecordProducer::LocalValidation { command })
                    if command == "pre_commit"
            )
    });
    assert!(
        pre_commit_evidence.is_some(),
        "durable pre-commit evidence must exist for resume to work"
    );

    // Now resume. Fix the formatting so pre-commit passes on the next attempt.
    fs::write(
        project_root.join("src/main.rs"),
        "fn main() {\n    println!(\"hello\");\n    let x = 1;\n    let y = 2;\n    let z = x + y;\n    println!(\"{z}\");\n}\n",
    )
    .unwrap();

    let resume_agent_service = build_agent_service();
    let resume_config = EffectiveConfig::load(base_dir).unwrap();
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
        FlowPreset::Standard,
        &resume_config,
    )
    .await;
    assert!(
        resume_result.is_ok(),
        "resume after pre-commit failure should succeed: {resume_result:?}"
    );

    let final_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(
        final_snapshot.status,
        RunStatus::Completed,
        "run should complete after successful resume"
    );
}

// ── Snapshot/journal reconciliation tests ──────────────────────────────────

/// When `fail_run()` snapshot write fails but the journal has run_failed,
/// `resume` should reconcile the stale Running snapshot to Failed and proceed.
#[tokio::test]
async fn resume_reconciles_stale_running_snapshot_with_journal_run_failed() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "stale-snap-resume");

    let run_id = RunId::new("run-stale-snap").unwrap();
    let now = Utc::now();

    // Simulate: snapshot still says Running (as if fail_run snapshot write failed),
    // but journal has run_started + stage_entered + run_failed.
    let snapshot = RunSnapshot {
        active_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
                    StageId::Planning,
                ),
                started_at: now,
                prompt_hash_at_cycle_start: FsProjectStore
                    .read_project_record(base_dir, &pid)
                    .unwrap()
                    .prompt_hash
                    .clone(),
                prompt_hash_at_stage_start: FsProjectStore
                    .read_project_record(base_dir, &pid)
                    .unwrap()
                    .prompt_hash,
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(20),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "running: planning".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    FsRunSnapshotWriteStore
        .write_run_snapshot(base_dir, &pid, &snapshot)
        .unwrap();

    // Append journal events: run_started, then run_failed (simulating the
    // scenario where fail_run logged to journal but snapshot write failed).
    for event in [
        journal::run_started_event(2, now, &run_id, StageId::PromptReview, 20),
        journal::run_failed_event(
            3,
            now,
            &run_id,
            StageId::Planning,
            "unknown",
            "simulated failure",
            1,
            20,
            None,
        ),
    ] {
        FsJournalStore
            .append_event(base_dir, &pid, &journal::serialize_event(&event).unwrap())
            .unwrap();
    }

    // Resume should detect the mismatch and reconcile.
    // It will still fail during execution (stub backend), but the point is
    // it should NOT reject with "already has a running run".
    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::resume_standard_run(
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

    // The resume attempt itself may fail (stub backend), but it should NOT
    // fail with "already has a running run" — the reconciliation should have
    // treated it as Failed and allowed the resume path to proceed.
    if let Err(ref e) = result {
        let msg = e.to_string();
        assert!(
            !msg.contains("already has a running run"),
            "resume should reconcile stale Running snapshot via journal, got: {msg}"
        );
    }

    // The snapshot should now reflect Failed (or later state), not Running.
    let reconciled_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_ne!(
        reconciled_snapshot.status,
        RunStatus::Running,
        "snapshot should no longer be Running after reconciliation"
    );
}

/// When the snapshot is Running but the orchestrator pid file is missing,
/// resume should treat the run as stale, recover it, and continue.
#[tokio::test]
async fn resume_recovers_stale_running_snapshot_when_pid_file_is_missing() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "stale-pid-missing");
    let run_id = RunId::new("run-stale-pid-missing").unwrap();
    let now = Utc::now();
    let prompt_hash = FsProjectStore
        .read_project_record(base_dir, &pid)
        .unwrap()
        .prompt_hash;

    let snapshot = RunSnapshot {
        active_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
                    StageId::Planning,
                ),
                started_at: now,
                prompt_hash_at_cycle_start: prompt_hash.clone(),
                prompt_hash_at_stage_start: prompt_hash,
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(20),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "running: planning".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    FsRunSnapshotWriteStore
        .write_run_snapshot(base_dir, &pid, &snapshot)
        .unwrap();
    FsJournalStore
        .append_event(
            base_dir,
            &pid,
            &journal::serialize_event(&journal::run_started_event(
                2,
                now,
                &run_id,
                StageId::Planning,
                20,
            ))
            .unwrap(),
        )
        .unwrap();
    FileSystem::remove_pid_file(base_dir, &pid).unwrap();

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();
    let result = engine::resume_standard_run(
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
        result.is_ok(),
        "resume should recover stale running run: {result:?}"
    );

    let final_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(final_snapshot.status, RunStatus::Completed);
    assert!(
        final_snapshot.interrupted_run.is_none(),
        "completed run should clear interrupted_run after successful resume"
    );
    assert!(
        FileSystem::read_pid_file(base_dir, &pid).unwrap().is_none(),
        "pid file should be removed after resumed run completes"
    );
}

/// When a run dies after publishing `run.json` but before `run_started`
/// reaches the journal, resume should fall back to the interrupted snapshot
/// metadata instead of aborting on the missing event.
#[tokio::test]
async fn resume_recovers_stale_running_snapshot_without_run_started_event() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "stale-without-run-started");
    let run_id = RunId::new("run-stale-without-run-started").unwrap();
    let now = Utc::now();
    let prompt_hash = FsProjectStore
        .read_project_record(base_dir, &pid)
        .unwrap()
        .prompt_hash;

    let snapshot = RunSnapshot {
        active_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
                    StageId::Planning,
                ),
                started_at: now,
                prompt_hash_at_cycle_start: prompt_hash.clone(),
                prompt_hash_at_stage_start: prompt_hash,
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(20),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "running: planning".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    FsRunSnapshotWriteStore
        .write_run_snapshot(base_dir, &pid, &snapshot)
        .unwrap();
    FileSystem::remove_pid_file(base_dir, &pid).unwrap();

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();
    let result = engine::resume_standard_run(
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
        result.is_ok(),
        "resume should recover a stale run even without run_started: {result:?}"
    );

    let final_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(final_snapshot.status, RunStatus::Completed);
    assert!(
        final_snapshot.interrupted_run.is_none(),
        "completed run should clear interrupted_run after successful resume"
    );
}

/// When a resumed attempt crashes after writing `run.json` but before
/// `run_resumed` reaches the journal, engine-level stale recovery must ignore
/// terminal events from the previous attempt and reconcile based on pid
/// liveness instead.
#[tokio::test]
async fn resume_recovers_unjournaled_resumed_attempt_without_inheriting_previous_attempt_failure() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "stale-unjournaled-resume-engine");
    let run_id = RunId::new("run-stale-unjournaled-resume-engine").unwrap();
    let initial_started_at = Utc::now();
    let resumed_at = initial_started_at + chrono::Duration::minutes(10);
    let prompt_hash = FsProjectStore
        .read_project_record(base_dir, &pid)
        .unwrap()
        .prompt_hash;

    let snapshot = RunSnapshot {
        active_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
                    StageId::Planning,
                ),
                started_at: resumed_at,
                prompt_hash_at_cycle_start: prompt_hash.clone(),
                prompt_hash_at_stage_start: prompt_hash,
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(20),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "running: planning".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    FsRunSnapshotWriteStore
        .write_run_snapshot(base_dir, &pid, &snapshot)
        .unwrap();
    for event in [
        journal::run_started_event(2, initial_started_at, &run_id, StageId::Planning, 20),
        journal::run_failed_event(
            3,
            initial_started_at + chrono::Duration::minutes(5),
            &run_id,
            StageId::Planning,
            "stage_failure",
            "older attempt failed",
            1,
            20,
            None,
        ),
    ] {
        FsJournalStore
            .append_event(base_dir, &pid, &journal::serialize_event(&event).unwrap())
            .unwrap();
    }
    FileSystem::remove_pid_file(base_dir, &pid).unwrap();

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();
    let result = engine::resume_standard_run(
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
        result.is_ok(),
        "resume should recover the unjournaled resumed attempt: {result:?}"
    );

    let final_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(final_snapshot.status, RunStatus::Completed);

    let runtime_logs = fs::read_to_string(
        project_root(base_dir, "stale-unjournaled-resume-engine").join("runtime/logs/run.ndjson"),
    )
    .expect("read runtime logs");
    assert!(
        runtime_logs.contains("orchestrator process was not alive"),
        "stale recovery should use pid liveness for the active attempt: {runtime_logs}"
    );
    assert!(
        !runtime_logs.contains("durable run_failed journal event"),
        "previous-attempt run_failed should not be treated as the active attempt's terminal state: {runtime_logs}"
    );
}

/// When the journal still contains an older completed run, resume must keep
/// using the recovered snapshot's run id instead of binding to the last
/// visible historical run_started event.
#[tokio::test]
async fn resume_recovers_stale_running_snapshot_without_binding_to_old_run_history() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "stale-old-run-history");
    let old_run_id = RunId::new("run-old-history").unwrap();
    let current_run_id = RunId::new("run-current-history").unwrap();
    let now = Utc::now();
    let prompt_hash = FsProjectStore
        .read_project_record(base_dir, &pid)
        .unwrap()
        .prompt_hash;

    let snapshot = RunSnapshot {
        active_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: current_run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
                    StageId::Planning,
                ),
                started_at: now,
                prompt_hash_at_cycle_start: prompt_hash.clone(),
                prompt_hash_at_stage_start: prompt_hash,
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(20),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "running: planning".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    FsRunSnapshotWriteStore
        .write_run_snapshot(base_dir, &pid, &snapshot)
        .unwrap();

    for event in [
        journal::run_started_event(2, now, &old_run_id, StageId::Planning, 20),
        journal::run_completed_event(3, now, &old_run_id, 1, 20),
    ] {
        FsJournalStore
            .append_event(base_dir, &pid, &journal::serialize_event(&event).unwrap())
            .unwrap();
    }
    FileSystem::remove_pid_file(base_dir, &pid).unwrap();

    let agent_service = build_agent_service();
    let config = EffectiveConfig::load(base_dir).unwrap();
    let result = engine::resume_standard_run(
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
        result.is_ok(),
        "resume should ignore historical run_started events from older runs: {result:?}"
    );

    let final_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(final_snapshot.status, RunStatus::Completed);

    let journal_text =
        fs::read_to_string(project_root(base_dir, "stale-old-run-history").join("journal.ndjson"))
            .expect("read journal");
    assert!(
        journal_text.contains("\"run_id\":\"run-current-history\""),
        "resume should continue the recovered run id, got journal: {journal_text}"
    );
}

/// Status reporting correctly shows Failed when snapshot is stale Running
/// but journal has run_failed.
#[test]
fn status_reconciles_stale_running_snapshot_with_journal_run_failed() {
    use ralph_burning::contexts::project_run_record::queries;

    let run_id = RunId::new("run-status-reconcile").unwrap();
    let now = Utc::now();

    let mut snapshot = RunSnapshot {
        active_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
                    StageId::Planning,
                ),
                started_at: now,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(20),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "running: planning".to_owned(),
        last_stage_resolution_snapshot: None,
    };

    // Journal has project_created + run_started + run_failed.
    let events = vec![
        journal::run_started_event(1, now, &run_id, StageId::Planning, 20),
        journal::run_failed_event(
            2,
            now,
            &run_id,
            StageId::Planning,
            "unknown",
            "simulated failure",
            1,
            20,
            None,
        ),
    ];

    // Before reconciliation: snapshot says Running.
    assert_eq!(snapshot.status, RunStatus::Running);

    // Reconcile.
    let patched = queries::reconcile_snapshot_status(&mut snapshot, &events);
    assert!(patched, "reconciliation should detect mismatch and patch");
    assert_eq!(snapshot.status, RunStatus::Failed);
    assert!(snapshot.active_run.is_none());
}

/// Status reconciliation is a no-op when snapshot and journal agree.
#[test]
fn status_reconciliation_is_noop_when_snapshot_already_failed() {
    use ralph_burning::contexts::project_run_record::queries;

    let run_id = RunId::new("run-noop-reconcile").unwrap();
    let now = Utc::now();

    let mut snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Failed,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(20),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "failed at planning".to_owned(),
        last_stage_resolution_snapshot: None,
    };

    let events = vec![
        journal::run_started_event(1, now, &run_id, StageId::Planning, 20),
        journal::run_failed_event(
            2,
            now,
            &run_id,
            StageId::Planning,
            "unknown",
            "failure",
            1,
            20,
            None,
        ),
    ];

    let patched = queries::reconcile_snapshot_status(&mut snapshot, &events);
    assert!(
        !patched,
        "no reconciliation needed when snapshot is already Failed"
    );
    assert_eq!(snapshot.status, RunStatus::Failed);
}

/// Status reconciliation detects run_completed in journal when snapshot
/// is stale Running.
#[test]
fn status_reconciles_stale_running_snapshot_with_journal_run_completed() {
    use ralph_burning::contexts::project_run_record::queries;

    let run_id = RunId::new("run-complete-reconcile").unwrap();
    let now = Utc::now();

    let mut snapshot = RunSnapshot {
        active_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
                    StageId::Planning,
                ),
                started_at: now,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(20),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "running: planning".to_owned(),
        last_stage_resolution_snapshot: None,
    };

    let events = vec![
        journal::run_started_event(1, now, &run_id, StageId::Planning, 20),
        journal::run_completed_event(2, now, &run_id, 1, 20),
    ];

    let patched = queries::reconcile_snapshot_status(&mut snapshot, &events);
    assert!(patched, "reconciliation should detect mismatch and patch");
    assert_eq!(snapshot.status, RunStatus::Completed);
    assert!(snapshot.active_run.is_none());
}

/// Status reconciliation must ignore terminal events from older attempts when
/// a resumed attempt with the same run_id is still active.
#[test]
fn status_reconciliation_ignores_terminal_events_before_latest_resume_boundary() {
    use ralph_burning::contexts::project_run_record::queries;

    let run_id = RunId::new("run-resumed-status").unwrap();
    let started_at = Utc::now();
    let resumed_at = started_at + chrono::Duration::minutes(10);

    let mut snapshot = RunSnapshot {
        active_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::new(
                    StageId::Implementation,
                    2,
                    1,
                    1,
                )
                .unwrap(),
                started_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(20),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "running: implementation".to_owned(),
        last_stage_resolution_snapshot: None,
    };

    let events = vec![
        journal::run_started_event(1, started_at, &run_id, StageId::Planning, 20),
        journal::run_failed_event(
            2,
            started_at + chrono::Duration::minutes(5),
            &run_id,
            StageId::Review,
            "stage_failure",
            "older attempt failed",
            1,
            20,
            None,
        ),
        journal::run_resumed_event(3, resumed_at, &run_id, StageId::Implementation, 2, 1, 20),
        journal::stage_entered_event(4, resumed_at, &run_id, StageId::Implementation, 2, 1),
    ];

    let patched = queries::reconcile_snapshot_status(&mut snapshot, &events);
    assert!(
        !patched,
        "older terminal events from the same run_id must not override a newer resumed attempt"
    );
    assert_eq!(snapshot.status, RunStatus::Running);
    assert!(snapshot.active_run.is_some());
}

/// Status reconciliation must also ignore terminal events from the previous
/// attempt when a resumed snapshot was persisted before `run_resumed` became
/// durable in the journal.
#[test]
fn status_reconciliation_ignores_terminal_events_before_unjournaled_resume_snapshot_started_at() {
    use ralph_burning::contexts::project_run_record::queries;

    let run_id = RunId::new("run-resume-window-status").unwrap();
    let started_at = Utc::now();
    let resumed_at = started_at + chrono::Duration::minutes(10);

    let mut snapshot = RunSnapshot {
        active_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::new(
                    StageId::Implementation,
                    2,
                    1,
                    1,
                )
                .unwrap(),
                started_at: resumed_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(20),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "running: implementation".to_owned(),
        last_stage_resolution_snapshot: None,
    };

    let events = vec![
        journal::run_started_event(1, started_at, &run_id, StageId::Planning, 20),
        journal::run_failed_event(
            2,
            started_at + chrono::Duration::minutes(5),
            &run_id,
            StageId::Review,
            "stage_failure",
            "older attempt failed",
            1,
            20,
            None,
        ),
    ];

    let patched = queries::reconcile_snapshot_status(&mut snapshot, &events);
    assert!(
        !patched,
        "an unjournaled resumed attempt must not inherit terminal state from the previous attempt"
    );
    assert_eq!(snapshot.status, RunStatus::Running);
    assert!(snapshot.active_run.is_some());
}

/// Status reconciliation must still trust a durable terminal event when the
/// active snapshot was written before `run_started` reached the journal.
#[test]
fn status_reconciliation_repairs_terminal_snapshot_without_run_started_event() {
    use ralph_burning::contexts::project_run_record::queries;

    let run_id = RunId::new("run-missing-run-started-status").unwrap();
    let started_at = Utc::now();

    let mut snapshot = RunSnapshot {
        active_run: Some(
            ralph_burning::contexts::project_run_record::model::ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: ralph_burning::shared::domain::StageCursor::initial(
                    StageId::Planning,
                ),
                started_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            },
        ),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 1,
        max_completion_rounds: Some(20),
        rollback_point_meta: Default::default(),
        amendment_queue: Default::default(),
        status_summary: "running: planning".to_owned(),
        last_stage_resolution_snapshot: None,
    };

    let events = vec![journal::run_failed_event(
        1,
        started_at,
        &run_id,
        StageId::Planning,
        "stage_failure",
        "failed before run_started persisted",
        1,
        20,
        None,
    )];

    let patched = queries::reconcile_snapshot_status(&mut snapshot, &events);
    assert!(
        patched,
        "terminal reconciliation should still patch a stale running snapshot without a durable run_started event"
    );
    assert_eq!(snapshot.status, RunStatus::Failed);
    assert!(snapshot.active_run.is_none());
}

/// `last_terminal_event_type` returns the correct terminal event.
#[test]
fn last_terminal_event_type_returns_run_failed() {
    let run_id = RunId::new("run-terminal").unwrap();
    let now = Utc::now();

    let events = vec![
        journal::run_started_event(1, now, &run_id, StageId::Planning, 20),
        journal::run_failed_event(
            2,
            now,
            &run_id,
            StageId::Planning,
            "unknown",
            "failure",
            1,
            20,
            None,
        ),
    ];

    assert_eq!(
        journal::last_terminal_event_type(&events),
        Some(JournalEventType::RunFailed)
    );
}

/// `last_terminal_event_type` returns None when no terminal event exists.
#[test]
fn last_terminal_event_type_returns_none_without_terminal() {
    let run_id = RunId::new("run-no-terminal").unwrap();
    let now = Utc::now();

    let events = vec![journal::run_started_event(
        1,
        now,
        &run_id,
        StageId::Planning,
        20,
    )];

    assert_eq!(journal::last_terminal_event_type(&events), None);
}

/// When the fail_run snapshot write fails (simulated via FailingSnapshotWriteStore),
/// the engine still records run_failed in the journal. The stale snapshot left on
/// disk can then be recovered via journal-based reconciliation on resume.
///
/// This is the end-to-end test: start a run, let the backend fail at a stage,
/// have the fail_run snapshot write also fail, then verify:
/// 1. The journal has a run_failed event despite the snapshot write failure
/// 2. The snapshot on disk is stale (still Running)
/// 3. Resume reconciles and proceeds
#[tokio::test]
async fn fail_run_snapshot_failure_leaves_journal_recoverable_state() {
    let tmp = tempdir().unwrap();
    let base_dir = tmp.path();

    setup_workspace(base_dir);
    let pid = create_standard_project(base_dir, "snap-fail-journal");

    // The engine writes the snapshot multiple times:
    //   1: initial Running snapshot
    //   2: status_summary update for prompt_review stage
    //   ... (more during stage execution)
    // We want the fail_run write to fail. The fail_run writes happen after
    // the stub backend's stage failure. Empirically, fail_on_call=3 targets
    // the fail_run path (the exact count depends on the stage execution flow,
    // but any failure after the initial writes will exercise the fallback).
    //
    // Instead of guessing the exact call count, we use a snapshot write store
    // that fails specifically on Failed-status writes (like BackoffFailingSnapshotWriteStore).
    struct FailRunSnapshotWriteStore;

    impl RunSnapshotWritePort for FailRunSnapshotWriteStore {
        fn write_run_snapshot(
            &self,
            base_dir: &Path,
            project_id: &ralph_burning::shared::domain::ProjectId,
            snapshot: &RunSnapshot,
        ) -> AppResult<()> {
            // Fail all writes where the status is Failed (i.e. fail_run writes).
            if snapshot.status == RunStatus::Failed {
                return Err(AppError::Io(std::io::Error::other(
                    "simulated disk error during fail_run snapshot write",
                )));
            }
            FsRunSnapshotWriteStore.write_run_snapshot(base_dir, project_id, snapshot)
        }
    }

    // Make the stub backend fail permanently at prompt_review (first stage).
    // transient_failure with count=100 ensures all retries are exhausted.
    let agent_service = build_agent_service_with_adapter(
        StubBackendAdapter::default().with_transient_failure(StageId::PromptReview, 100),
    );
    let config = EffectiveConfig::load(base_dir).unwrap();

    let result = engine::execute_standard_run(
        &agent_service,
        &FsRunSnapshotStore,
        &FailRunSnapshotWriteStore,
        &FsJournalStore,
        &FsPayloadArtifactWriteStore,
        &FsRuntimeLogWriteStore,
        &FsAmendmentQueueStore,
        base_dir,
        &pid,
        &config,
    )
    .await;

    // The run should fail (backend fails at prompt_review).
    assert!(result.is_err(), "run should fail due to backend failure");

    // The journal should have run_failed despite the snapshot write failure.
    let events = FsJournalStore.read_journal(base_dir, &pid).unwrap();
    let has_run_failed = events
        .iter()
        .any(|e| e.event_type == JournalEventType::RunFailed);
    assert!(
        has_run_failed,
        "journal should have run_failed even when fail_run snapshot write fails"
    );

    // The snapshot on disk should still be Running (since fail_run write failed).
    let disk_snapshot = FsRunSnapshotStore
        .read_run_snapshot(base_dir, &pid)
        .unwrap();
    assert_eq!(
        disk_snapshot.status,
        RunStatus::Running,
        "snapshot should still be Running since fail_run write was blocked"
    );

    // Verify that status reconciliation fixes the stale snapshot.
    use ralph_burning::contexts::project_run_record::queries;
    let mut reconciled = disk_snapshot;
    let patched = queries::reconcile_snapshot_status(&mut reconciled, &events);
    assert!(
        patched,
        "reconciliation should detect and patch the mismatch"
    );
    assert_eq!(
        reconciled.status,
        RunStatus::Failed,
        "reconciled status should be Failed per journal"
    );
}
