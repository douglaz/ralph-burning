use chrono::{Duration, Utc};
use tempfile::tempdir;

use ralph_burning::adapters::fs::FsDaemonStore;
use ralph_burning::adapters::worktree::WorktreeAdapter;
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
