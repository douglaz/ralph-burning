use chrono::{Duration, Utc};
use tempfile::tempdir;

use ralph_burning::adapters::fs::FsDaemonStore;
use ralph_burning::adapters::worktree::WorktreeAdapter;
use ralph_burning::contexts::automation_runtime::model::{DaemonTask, TaskStatus, WorktreeLease};
use ralph_burning::contexts::automation_runtime::routing::RoutingEngine;
use ralph_burning::contexts::automation_runtime::task_service::{
    CreateTaskInput, DaemonTaskService,
};
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
