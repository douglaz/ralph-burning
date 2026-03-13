use chrono::{Duration, Utc};
use tempfile::tempdir;

use ralph_burning::adapters::fs::FsDaemonStore;
use ralph_burning::adapters::worktree::WorktreeAdapter;
use ralph_burning::contexts::automation_runtime::lease_service::LeaseService;
use ralph_burning::contexts::automation_runtime::model::{
    DaemonTask, DispatchMode, TaskStatus, WatchedIssueMeta, WorktreeLease,
};
use ralph_burning::contexts::automation_runtime::routing::RoutingEngine;
use ralph_burning::contexts::automation_runtime::task_service::{
    CreateTaskInput, DaemonTaskService,
};
use ralph_burning::contexts::automation_runtime::watcher::parse_requirements_command;
use ralph_burning::contexts::automation_runtime::{DaemonStorePort, WorktreePort};
use ralph_burning::shared::domain::FlowPreset;
use ralph_burning::shared::error::AppError;

fn sample_task() -> DaemonTask {
    let now = Utc::now();
    DaemonTask {
        task_id: "task-1".to_owned(),
        issue_ref: "repo#1".to_owned(),
        project_id: "demo".to_owned(),
        project_name: Some("Demo".to_owned()),
        prompt: Some("Prompt".to_owned()),
        routing_command: None,
        routing_labels: vec![],
        resolved_flow: Some(FlowPreset::Standard),
        routing_source: None,
        routing_warnings: vec![],
        status: TaskStatus::Pending,
        created_at: now,
        updated_at: now,
        attempt_count: 0,
        lease_id: None,
        failure_class: None,
        failure_message: None,
        dispatch_mode: DispatchMode::Workflow,
        source_revision: None,
        requirements_run_id: None,
    }
}

#[test]
fn routing_resolution_prefers_command_over_label_and_default() {
    let engine = RoutingEngine::new();
    let resolution = engine
        .resolve_flow(
            Some("/rb flow quick_dev"),
            &[String::from("rb:flow:docs_change")],
            FlowPreset::CiImprovement,
        )
        .expect("resolve flow");

    assert_eq!(FlowPreset::QuickDev, resolution.flow);
}

#[test]
fn routing_labels_ignore_malformed_values_with_warning() {
    let engine = RoutingEngine::new();
    let resolution = engine
        .resolve_flow(
            None,
            &[String::from("rb:flow"), String::from("rb:flow:docs_change")],
            FlowPreset::Standard,
        )
        .expect("resolve labels");

    assert_eq!(FlowPreset::DocsChange, resolution.flow);
    assert_eq!(1, resolution.warnings.len());
}

#[test]
fn conflicting_routing_labels_fail_resolution() {
    let engine = RoutingEngine::new();
    let error = engine
        .resolve_flow(
            None,
            &[
                String::from("rb:flow:standard"),
                String::from("rb:flow:quick_dev"),
            ],
            FlowPreset::Standard,
        )
        .expect_err("conflicting labels should fail");

    assert!(matches!(error, AppError::AmbiguousRouting { .. }));
}

#[test]
fn task_state_machine_accepts_expected_transitions() {
    let mut task = sample_task();
    task.transition_to(TaskStatus::Claimed, Utc::now())
        .expect("pending -> claimed");
    task.transition_to(TaskStatus::Active, Utc::now())
        .expect("claimed -> active");
    task.transition_to(TaskStatus::Completed, Utc::now())
        .expect("active -> completed");
}

#[test]
fn task_state_machine_rejects_invalid_transition() {
    let mut task = sample_task();
    let error = task
        .transition_to(TaskStatus::Completed, Utc::now())
        .expect_err("pending -> completed should fail");

    assert!(matches!(error, AppError::TaskStateTransitionInvalid { .. }));
}

#[test]
fn task_state_machine_active_to_waiting() {
    let mut task = sample_task();
    task.transition_to(TaskStatus::Claimed, Utc::now())
        .expect("pending -> claimed");
    task.transition_to(TaskStatus::Active, Utc::now())
        .expect("claimed -> active");
    task.transition_to(TaskStatus::WaitingForRequirements, Utc::now())
        .expect("active -> waiting_for_requirements");
    assert_eq!(TaskStatus::WaitingForRequirements, task.status);
    assert!(!task.is_terminal());
}

#[test]
fn task_state_machine_waiting_to_pending() {
    let mut task = sample_task();
    task.status = TaskStatus::WaitingForRequirements;
    task.transition_to(TaskStatus::Pending, Utc::now())
        .expect("waiting -> pending");
    assert_eq!(TaskStatus::Pending, task.status);
}

#[test]
fn task_state_machine_waiting_to_failed() {
    let mut task = sample_task();
    task.status = TaskStatus::WaitingForRequirements;
    task.transition_to(TaskStatus::Failed, Utc::now())
        .expect("waiting -> failed");
    assert_eq!(TaskStatus::Failed, task.status);
}

#[test]
fn task_state_machine_waiting_to_aborted() {
    let mut task = sample_task();
    task.status = TaskStatus::WaitingForRequirements;
    task.transition_to(TaskStatus::Aborted, Utc::now())
        .expect("waiting -> aborted");
    assert_eq!(TaskStatus::Aborted, task.status);
}

#[test]
fn lease_ttl_detects_staleness() {
    let now = Utc::now();
    let lease = WorktreeLease {
        lease_id: "lease-1".to_owned(),
        task_id: "task-1".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: "/tmp/demo".into(),
        branch_name: "rb/task/task-1".to_owned(),
        acquired_at: now,
        ttl_seconds: 300,
        last_heartbeat: now,
    };

    assert!(!lease.is_stale_at(now + Duration::seconds(299)));
    assert!(lease.is_stale_at(now + Duration::seconds(301)));
}

#[test]
fn worktree_path_derivation_is_deterministic() {
    let adapter = WorktreeAdapter;
    let temp = tempdir().expect("tempdir");
    let path = adapter.worktree_path(temp.path(), "task-99");

    assert_eq!(temp.path().join(".ralph-burning/worktrees/task-99"), path);
    assert_eq!("rb/task/task-99", adapter.branch_name("task-99"));
}

#[test]
fn create_task_rejects_duplicate_active_issue() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let routing = RoutingEngine::new();

    DaemonTaskService::create_task(
        &store,
        temp.path(),
        &routing,
        FlowPreset::Standard,
        CreateTaskInput {
            task_id: "task-1".to_owned(),
            issue_ref: "repo#1".to_owned(),
            project_id: "demo".to_owned(),
            project_name: Some("Demo".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
        },
    )
    .expect("create first task");

    let error = DaemonTaskService::create_task(
        &store,
        temp.path(),
        &routing,
        FlowPreset::Standard,
        CreateTaskInput {
            task_id: "task-2".to_owned(),
            issue_ref: "repo#1".to_owned(),
            project_id: "demo-2".to_owned(),
            project_name: Some("Demo Two".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            dispatch_mode: DispatchMode::Workflow,
            source_revision: None,
        },
    )
    .expect_err("duplicate issue should fail");

    assert!(matches!(error, AppError::DuplicateTaskForIssue { .. }));
}

#[test]
fn retry_resets_failed_task_to_pending_and_increments_attempt_count() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let mut task = sample_task();
    task.status = TaskStatus::Failed;
    task.set_failure("daemon_dispatch_failed", "boom");
    store.create_task(temp.path(), &task).expect("persist task");

    let retried = DaemonTaskService::retry_task(&store, temp.path(), &task.task_id)
        .expect("retry failed task");

    assert_eq!(TaskStatus::Pending, retried.status);
    assert_eq!(1, retried.attempt_count);
    assert!(retried.failure_class.is_none());
    assert!(retried.failure_message.is_none());
}

// ── Watched-issue ingestion tests ───────────────────────────────────────────

#[test]
fn watched_issue_ingestion_creates_task_idempotently() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let routing = RoutingEngine::new();

    let issue = WatchedIssueMeta {
        issue_ref: "org/repo#42".to_owned(),
        source_revision: "deadbeef".to_owned(),
        title: "Fix the thing".to_owned(),
        body: "It is broken".to_owned(),
        labels: vec![],
        routing_command: None,
    };

    // First ingestion creates a task
    let result = DaemonTaskService::create_task_from_watched_issue(
        &store,
        temp.path(),
        &routing,
        FlowPreset::Standard,
        &issue,
        DispatchMode::Workflow,
    )
    .expect("create from watched issue");
    assert!(result.is_some());
    let task = result.unwrap();
    assert_eq!("org/repo#42", task.issue_ref);
    assert_eq!(Some("deadbeef".to_owned()), task.source_revision);
    assert_eq!(DispatchMode::Workflow, task.dispatch_mode);

    // Second ingestion with same (issue_ref, source_revision) is a no-op
    let result2 = DaemonTaskService::create_task_from_watched_issue(
        &store,
        temp.path(),
        &routing,
        FlowPreset::Standard,
        &issue,
        DispatchMode::Workflow,
    )
    .expect("idempotent re-ingestion");
    assert!(result2.is_none());
}

#[test]
fn watched_issue_newer_revision_after_terminal_creates_fresh_task() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let routing = RoutingEngine::new();

    let issue1 = WatchedIssueMeta {
        issue_ref: "org/repo#50".to_owned(),
        source_revision: "rev1aaaa".to_owned(),
        title: "First".to_owned(),
        body: "Body".to_owned(),
        labels: vec![],
        routing_command: None,
    };

    // Create and complete the first task
    let result = DaemonTaskService::create_task_from_watched_issue(
        &store,
        temp.path(),
        &routing,
        FlowPreset::Standard,
        &issue1,
        DispatchMode::Workflow,
    )
    .expect("create first");
    let task = result.unwrap();

    // Directly set the task to Completed (bypassing state machine for test setup)
    let mut raw = store.read_task(temp.path(), &task.task_id).unwrap();
    raw.status = TaskStatus::Completed;
    raw.updated_at = Utc::now();
    store.write_task(temp.path(), &raw).unwrap();

    // New revision should create a fresh task
    let issue2 = WatchedIssueMeta {
        issue_ref: "org/repo#50".to_owned(),
        source_revision: "rev2bbbb".to_owned(),
        title: "Second".to_owned(),
        body: "Updated body".to_owned(),
        labels: vec![],
        routing_command: None,
    };

    let result2 = DaemonTaskService::create_task_from_watched_issue(
        &store,
        temp.path(),
        &routing,
        FlowPreset::Standard,
        &issue2,
        DispatchMode::Workflow,
    )
    .expect("create second for new revision");
    assert!(result2.is_some());
}

#[test]
fn watched_issue_different_revision_while_non_terminal_fails() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let routing = RoutingEngine::new();

    let issue1 = WatchedIssueMeta {
        issue_ref: "org/repo#60".to_owned(),
        source_revision: "aaa11111".to_owned(),
        title: "First".to_owned(),
        body: "Body".to_owned(),
        labels: vec![],
        routing_command: None,
    };

    DaemonTaskService::create_task_from_watched_issue(
        &store,
        temp.path(),
        &routing,
        FlowPreset::Standard,
        &issue1,
        DispatchMode::Workflow,
    )
    .expect("create first");

    let issue2 = WatchedIssueMeta {
        issue_ref: "org/repo#60".to_owned(),
        source_revision: "bbb22222".to_owned(),
        title: "Second".to_owned(),
        body: "Body".to_owned(),
        labels: vec![],
        routing_command: None,
    };

    let err = DaemonTaskService::create_task_from_watched_issue(
        &store,
        temp.path(),
        &routing,
        FlowPreset::Standard,
        &issue2,
        DispatchMode::Workflow,
    )
    .expect_err("should reject different revision while non-terminal");

    assert!(matches!(err, AppError::DuplicateWatchedIssue { .. }));
}

#[test]
fn waiting_for_requirements_resume_transitions() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    let mut task = sample_task();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("persist task");

    // Transition to waiting
    let waiting = DaemonTaskService::mark_waiting_for_requirements(
        &store,
        temp.path(),
        &task.task_id,
        "req-20260313",
    )
    .expect("mark waiting");
    assert_eq!(TaskStatus::WaitingForRequirements, waiting.status);
    assert_eq!(Some("req-20260313".to_owned()), waiting.requirements_run_id);
    assert!(waiting.lease_id.is_none());

    // Resume from waiting
    let resumed = DaemonTaskService::resume_from_waiting(&store, temp.path(), &task.task_id)
        .expect("resume from waiting");
    assert_eq!(TaskStatus::Pending, resumed.status);
    assert_eq!(DispatchMode::Workflow, resumed.dispatch_mode);
}

// ── Requirements-link failure invariant tests ────────────────────────────────

#[test]
fn link_failure_on_pending_task_transitions_to_failed() {
    // When requirements_quick succeeds but the first task-link write fails,
    // mark_failed must still work on a pending task (Pending → Failed).
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    let task = sample_task();
    store.create_task(temp.path(), &task).expect("persist task");

    // Simulate a link failure: the task is still Pending because we never
    // transitioned it. mark_failed should transition Pending → Failed.
    let failed = DaemonTaskService::mark_failed(
        &store,
        temp.path(),
        &task.task_id,
        "requirements_linking_failed",
        "simulated write_task failure during link",
    )
    .expect("mark_failed should succeed");
    assert_eq!(TaskStatus::Failed, failed.status);
    assert_eq!(
        Some("requirements_linking_failed".to_owned()),
        failed.failure_class
    );
}

#[test]
fn link_failure_on_waiting_task_transitions_to_failed() {
    // When mark_waiting_for_requirements succeeds but a subsequent operation
    // fails (e.g. metadata write in check_waiting_tasks), mark_failed must
    // work: WaitingForRequirements → Failed.
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    let mut task = sample_task();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("persist task");

    // Successfully transition to waiting
    let waiting = DaemonTaskService::mark_waiting_for_requirements(
        &store,
        temp.path(),
        &task.task_id,
        "req-link-fail-test",
    )
    .expect("mark waiting");
    assert_eq!(TaskStatus::WaitingForRequirements, waiting.status);
    assert_eq!(
        Some("req-link-fail-test".to_owned()),
        waiting.requirements_run_id
    );

    // Simulate a post-link failure: mark_failed should transition
    // WaitingForRequirements → Failed while preserving the requirements_run_id.
    let failed = DaemonTaskService::mark_failed(
        &store,
        temp.path(),
        &task.task_id,
        "requirements_linking_failed",
        "simulated post-link metadata write failure",
    )
    .expect("mark_failed should succeed from WaitingForRequirements");
    assert_eq!(TaskStatus::Failed, failed.status);
    assert_eq!(
        Some("requirements_linking_failed".to_owned()),
        failed.failure_class
    );
    // The requirements_run_id must remain addressable even after failure
    assert_eq!(
        Some("req-link-fail-test".to_owned()),
        failed.requirements_run_id,
        "requirements_run_id must be preserved after link failure"
    );
}

#[test]
fn link_failure_on_active_task_transitions_to_failed() {
    // When requirements_draft transitions Active → Active but the subsequent
    // link write fails, mark_failed must work: Active → Failed.
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    let mut task = sample_task();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("persist task");

    // Simulate: draft() succeeds and returns a run_id, but the first
    // write_task to set requirements_run_id fails. mark_failed should
    // still transition Active → Failed with explicit failure class.
    let failed = DaemonTaskService::mark_failed(
        &store,
        temp.path(),
        &task.task_id,
        "requirements_linking_failed",
        "simulated write_task failure during draft link",
    )
    .expect("mark_failed should succeed from Active");
    assert_eq!(TaskStatus::Failed, failed.status);
    assert_eq!(
        Some("requirements_linking_failed".to_owned()),
        failed.failure_class
    );
    // No requirements_run_id should be set since linking failed before persist
    assert!(
        failed.requirements_run_id.is_none(),
        "requirements_run_id should be None when link write fails before persist"
    );
}

#[test]
fn resume_from_non_waiting_state_fails() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    let task = sample_task();
    store.create_task(temp.path(), &task).expect("persist task");

    let err = DaemonTaskService::resume_from_waiting(&store, temp.path(), &task.task_id)
        .expect_err("should fail for pending task");
    assert!(matches!(err, AppError::TaskStateTransitionInvalid { .. }));
}

// ── Requirements command parsing ────────────────────────────────────────────

#[test]
fn parse_requirements_command_draft() {
    let result = parse_requirements_command("/rb requirements draft").unwrap();
    assert_eq!(Some(DispatchMode::RequirementsDraft), result);
}

#[test]
fn parse_requirements_command_quick() {
    let result = parse_requirements_command("/rb requirements quick").unwrap();
    assert_eq!(Some(DispatchMode::RequirementsQuick), result);
}

#[test]
fn parse_requirements_command_unknown_fails() {
    let result = parse_requirements_command("/rb requirements bogus");
    assert!(result.is_err());
}

#[test]
fn parse_requirements_command_no_match() {
    let result = parse_requirements_command("/rb flow standard").unwrap();
    assert_eq!(None, result);
}

#[test]
fn parse_requirements_command_multiline_body() {
    let body = "Please help.\n\n/rb requirements quick\n\nThanks!";
    let result = parse_requirements_command(body).unwrap();
    assert_eq!(Some(DispatchMode::RequirementsQuick), result);
}

#[test]
fn parse_requirements_command_bare_requirements_fails() {
    // "/rb requirements" without a subcommand is malformed
    let result = parse_requirements_command("/rb requirements");
    assert!(result.is_err(), "bare '/rb requirements' should fail");
}

#[test]
fn parse_requirements_command_extra_tokens_fails() {
    // "/rb requirements draft extra" has too many tokens
    let result = parse_requirements_command("/rb requirements draft extra");
    assert!(result.is_err(), "extra tokens should fail");
}

#[test]
fn is_requirements_command_identifies_requirements_commands() {
    use ralph_burning::contexts::automation_runtime::watcher::is_requirements_command;
    assert!(is_requirements_command("/rb requirements draft"));
    assert!(is_requirements_command("/rb requirements quick"));
    assert!(is_requirements_command("/rb requirements"));
    assert!(is_requirements_command("rb requirements unknown"));
    assert!(!is_requirements_command("/rb flow standard"));
    assert!(!is_requirements_command(""));
    assert!(!is_requirements_command("some random text"));
}

#[test]
fn watched_issue_with_requirements_command_routes_flow_from_labels() {
    // When routing_command is a requirements command, flow should come from labels/default
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let routing = RoutingEngine::new();

    let issue = WatchedIssueMeta {
        issue_ref: "org/repo#70".to_owned(),
        source_revision: "ccc33333".to_owned(),
        title: "Req with label flow".to_owned(),
        body: "Body".to_owned(),
        labels: vec!["rb:flow:quick_dev".to_owned()],
        routing_command: Some("/rb requirements quick".to_owned()),
    };

    let result = DaemonTaskService::create_task_from_watched_issue(
        &store,
        temp.path(),
        &routing,
        FlowPreset::Standard,
        &issue,
        DispatchMode::RequirementsQuick,
    )
    .expect("should succeed with label-based flow routing");
    let task = result.expect("task should be created");
    // Flow should come from the label, not from parsing the requirements command
    assert_eq!(Some(FlowPreset::QuickDev), task.resolved_flow);
}

// ── Dispatch mode serialization ─────────────────────────────────────────────

#[test]
fn dispatch_mode_display() {
    assert_eq!("workflow", DispatchMode::Workflow.as_str());
    assert_eq!("requirements_draft", DispatchMode::RequirementsDraft.as_str());
    assert_eq!("requirements_quick", DispatchMode::RequirementsQuick.as_str());
}

#[test]
fn task_with_dispatch_mode_roundtrips_through_json() {
    let mut task = sample_task();
    task.dispatch_mode = DispatchMode::RequirementsQuick;
    task.source_revision = Some("abc123".to_owned());
    task.requirements_run_id = Some("req-123".to_owned());

    let json = serde_json::to_string(&task).expect("serialize");
    let deserialized: DaemonTask = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(DispatchMode::RequirementsQuick, deserialized.dispatch_mode);
    assert_eq!(Some("abc123".to_owned()), deserialized.source_revision);
    assert_eq!(Some("req-123".to_owned()), deserialized.requirements_run_id);
}

#[test]
fn task_without_dispatch_mode_defaults_to_workflow() {
    // Backward compat: older JSON without dispatch_mode should default to workflow
    let json = r#"{
        "task_id": "task-old",
        "issue_ref": "repo#1",
        "project_id": "demo",
        "status": "pending",
        "created_at": "2026-03-13T00:00:00Z",
        "updated_at": "2026-03-13T00:00:00Z",
        "attempt_count": 0
    }"#;
    let task: DaemonTask = serde_json::from_str(json).expect("deserialize legacy task");
    assert_eq!(DispatchMode::Workflow, task.dispatch_mode);
    assert!(task.source_revision.is_none());
    assert!(task.requirements_run_id.is_none());
}

// ── Failure injection: requirements-link write failures ─────────────────────

#[test]
fn mark_waiting_write_failure_leaves_task_in_recoverable_state() {
    // Verifies the invariant: if mark_waiting_for_requirements fails (e.g. a
    // write_task error), the task stays in Active and the caller can still
    // transition it to Failed with an explicit linking failure class.
    //
    // We simulate this by calling mark_waiting on a task whose ID doesn't
    // exist on disk, triggering a read_task failure.
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    // Create task directories but don't write the task file —
    // mark_waiting_for_requirements will fail on read_task.
    let tasks_dir = temp.path().join(".ralph-burning/daemon/tasks");
    std::fs::create_dir_all(&tasks_dir).expect("create tasks dir");

    // Try marking a nonexistent task as waiting — must fail
    let err = DaemonTaskService::mark_waiting_for_requirements(
        &store,
        temp.path(),
        "nonexistent-task",
        "req-fail-test",
    );
    assert!(err.is_err(), "mark_waiting should fail for missing task");

    // Now create a real Active task and verify it can still be marked failed
    // (simulates the daemon_loop recovery path after a link failure).
    let mut task = sample_task();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("persist task");

    let failed_task = DaemonTaskService::mark_failed(
        &store,
        temp.path(),
        &task.task_id,
        "requirements_linking_failed",
        "simulated write failure during link",
    )
    .expect("mark_failed should succeed from Active");
    assert_eq!(TaskStatus::Failed, failed_task.status);
    assert_eq!(
        Some("requirements_linking_failed".to_owned()),
        failed_task.failure_class
    );
}

#[test]
fn link_result_write_failure_transitions_task_to_failed() {
    // Tests the quick-path invariant: if the link_result closure fails (write_task
    // or journal append), the task transitions to Failed with explicit failure class.
    // This verifies the state machine permits Active -> Failed.
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    let mut task = sample_task();
    task.status = TaskStatus::Active;
    task.dispatch_mode = DispatchMode::RequirementsQuick;
    store.create_task(temp.path(), &task).expect("persist task");

    // Simulate the scenario: requirements run created, then linking fails.
    // mark_failed with "requirements_linking_failed" should work from Active.
    let failed = DaemonTaskService::mark_failed(
        &store,
        temp.path(),
        &task.task_id,
        "requirements_linking_failed",
        "write_task failed during link",
    )
    .expect("mark_failed from Active");
    assert_eq!(TaskStatus::Failed, failed.status);
    assert_eq!(
        Some("requirements_linking_failed".to_owned()),
        failed.failure_class
    );
    assert!(failed
        .failure_message
        .as_ref()
        .unwrap()
        .contains("write_task failed"));
    // Requirements run ID should NOT be set — linking never completed
    assert!(failed.requirements_run_id.is_none());
}

#[test]
fn post_link_metadata_failure_transitions_waiting_task_to_failed() {
    // Tests the resume-path invariant: if post-seed metadata update fails after
    // a task is in WaitingForRequirements, the task can transition to Failed.
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    let mut task = sample_task();
    task.status = TaskStatus::WaitingForRequirements;
    task.requirements_run_id = Some("req-linked-ok".to_owned());
    store.create_task(temp.path(), &task).expect("persist task");

    // Simulate the scenario: requirements run completed, seed extracted,
    // but post-seed metadata write fails. The daemon should mark failed.
    let failed = DaemonTaskService::mark_failed(
        &store,
        temp.path(),
        &task.task_id,
        "requirements_linking_failed",
        "post-seed metadata update failed",
    )
    .expect("mark_failed from WaitingForRequirements");
    assert_eq!(TaskStatus::Failed, failed.status);
    assert_eq!(
        Some("requirements_linking_failed".to_owned()),
        failed.failure_class
    );
    // The requirements_run_id should still be set — the run itself succeeded
    assert_eq!(
        Some("req-linked-ok".to_owned()),
        failed.requirements_run_id
    );
}

#[test]
fn active_task_can_transition_to_pending_for_requeue() {
    // Tests the state transition needed when an empty-question requirements_draft
    // completes directly and the task needs to be requeued for workflow dispatch.
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    let mut task = sample_task();
    task.status = TaskStatus::Active;
    task.dispatch_mode = DispatchMode::RequirementsDraft;
    task.requirements_run_id = Some("req-empty-draft".to_owned());
    store.create_task(temp.path(), &task).expect("persist task");

    // Simulate the empty-question draft requeue: Active → Pending with Workflow mode
    let mut t = store.read_task(temp.path(), &task.task_id).expect("read");
    t.dispatch_mode = DispatchMode::Workflow;
    t.transition_to(TaskStatus::Pending, Utc::now())
        .expect("Active → Pending transition should succeed");
    store.write_task(temp.path(), &t).expect("write");

    let requeued = store.read_task(temp.path(), &task.task_id).expect("read");
    assert_eq!(TaskStatus::Pending, requeued.status);
    assert_eq!(DispatchMode::Workflow, requeued.dispatch_mode);
    assert_eq!(
        Some("req-empty-draft".to_owned()),
        requeued.requirements_run_id
    );
}

// ---------------------------------------------------------------------------
// Writer lock contention (CLI-level)
// ---------------------------------------------------------------------------

#[test]
fn writer_lock_acquire_release_roundtrip() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let project_id = ralph_burning::shared::domain::ProjectId::new("lock-test".to_owned())
        .expect("valid id");

    store
        .acquire_writer_lock(temp.path(), &project_id, "cli")
        .expect("acquire lock");

    // Second acquire should fail with ProjectWriterLockHeld
    let err = store
        .acquire_writer_lock(temp.path(), &project_id, "cli-2")
        .expect_err("second acquire should fail");
    assert!(
        matches!(err, AppError::ProjectWriterLockHeld { .. }),
        "expected ProjectWriterLockHeld, got: {err:?}"
    );

    // Release and re-acquire should succeed
    store
        .release_writer_lock(temp.path(), &project_id)
        .expect("release lock");
    store
        .acquire_writer_lock(temp.path(), &project_id, "cli-3")
        .expect("re-acquire after release");
    store
        .release_writer_lock(temp.path(), &project_id)
        .expect("final release");
}

#[test]
fn writer_lock_release_is_idempotent() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let project_id = ralph_burning::shared::domain::ProjectId::new("idem-test".to_owned())
        .expect("valid id");

    // Release without acquire should not fail
    store
        .release_writer_lock(temp.path(), &project_id)
        .expect("release without acquire should succeed");
}

// ---------------------------------------------------------------------------
// Reconcile partial-failure accounting
// ---------------------------------------------------------------------------

#[test]
fn reconcile_reports_only_successful_releases() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    // Create a task and a stale lease, but no actual worktree — release will
    // still succeed because remove_worktree errors are deferred.
    let mut task = sample_task();
    task.task_id = "reconcile-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    let lease = WorktreeLease {
        lease_id: "lease-reconcile-test".to_owned(),
        task_id: "reconcile-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: temp.path().join("nonexistent-wt"),
        branch_name: "rb/reconcile-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    // Create the writer lock so release can succeed
    let project_id = ralph_burning::shared::domain::ProjectId::new("demo".to_owned())
        .expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-reconcile-test")
        .expect("acquire lock");

    let worktree_adapter = WorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0), // force all leases stale
        Utc::now(),
    )
    .expect("reconcile");

    assert_eq!(1, report.stale_lease_ids.len());
    assert_eq!("lease-reconcile-test", report.stale_lease_ids[0]);
    assert_eq!(1, report.failed_task_ids.len());
    assert_eq!("reconcile-test", report.failed_task_ids[0]);
}

#[test]
fn reconcile_report_has_cleanup_failures_is_false_when_empty() {
    let report = ralph_burning::contexts::automation_runtime::ReconcileReport::default();
    assert!(!report.has_cleanup_failures());
}
