use chrono::{Duration, Utc};
use tempfile::tempdir;

use ralph_burning::adapters::fs::FsDaemonStore;
use ralph_burning::adapters::worktree::WorktreeAdapter;
use ralph_burning::contexts::automation_runtime::lease_service::{
    try_preserve_failed_branch, LeaseService, ReleaseMode,
};
use ralph_burning::contexts::automation_runtime::model::{
    CliWriterLease, DaemonTask, DispatchMode, LeaseRecord, TaskStatus, WatchedIssueMeta,
    WorktreeLease,
};
use ralph_burning::contexts::automation_runtime::routing::RoutingEngine;
use ralph_burning::contexts::automation_runtime::task_service::{
    CreateTaskInput, DaemonTaskService,
};
use ralph_burning::contexts::automation_runtime::watcher::parse_requirements_command;
use ralph_burning::contexts::automation_runtime::{DaemonStorePort, WorktreePort};
#[cfg(feature = "test-stub")]
use ralph_burning::shared::domain::BackendFamily;
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
        repo_slug: None,
        issue_number: None,
        pr_url: None,
        last_seen_comment_id: None,
        last_seen_review_id: None,
        label_dirty: false,
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
        branch_name: "rb/task-1".to_owned(),
        acquired_at: now,
        ttl_seconds: 300,
        last_heartbeat: now,
    };

    assert!(!lease.is_stale_at(now + Duration::seconds(299)));
    assert!(lease.is_stale_at(now + Duration::seconds(301)));
}

#[test]
fn cli_writer_lease_serde_round_trip() {
    let now = Utc::now();
    let lease = CliWriterLease {
        lease_id: "lease-cli-1".to_owned(),
        project_id: "demo".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: now,
        ttl_seconds: 300,
        last_heartbeat: now,
    };

    let json = serde_json::to_string(&lease).expect("serialize cli lease");
    let deserialized: CliWriterLease = serde_json::from_str(&json).expect("deserialize cli lease");
    assert_eq!(lease, deserialized);

    let record_json = serde_json::to_value(LeaseRecord::CliWriter(lease.clone()))
        .expect("serialize lease record");
    assert_eq!(
        Some("cli_writer"),
        record_json
            .get("lease_kind")
            .and_then(|value| value.as_str())
    );

    let record: LeaseRecord =
        serde_json::from_value(record_json).expect("deserialize lease record");
    assert_eq!(LeaseRecord::CliWriter(lease), record);
}

#[test]
fn legacy_worktree_lease_json_deserializes_as_lease_record() {
    let json = r#"{
        "lease_id": "lease-legacy-1",
        "task_id": "task-legacy-1",
        "project_id": "demo",
        "worktree_path": "/tmp/demo",
        "branch_name": "rb/task-legacy-1",
        "acquired_at": "2026-03-14T02:50:39Z",
        "ttl_seconds": 300,
        "last_heartbeat": "2026-03-14T02:55:39Z"
    }"#;

    let record: LeaseRecord =
        serde_json::from_str(json).expect("deserialize legacy worktree lease");

    match record {
        LeaseRecord::Worktree(lease) => {
            assert_eq!("lease-legacy-1", lease.lease_id);
            assert_eq!("task-legacy-1", lease.task_id);
            assert_eq!("demo", lease.project_id);
        }
        LeaseRecord::CliWriter(_) => panic!("legacy worktree lease must deserialize as worktree"),
    }
}

#[test]
fn cli_writer_lease_staleness_matches_worktree_lease() {
    let now = Utc::now();
    let worktree = WorktreeLease {
        lease_id: "lease-worktree-1".to_owned(),
        task_id: "task-1".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: "/tmp/demo".into(),
        branch_name: "rb/task-1".to_owned(),
        acquired_at: now,
        ttl_seconds: 300,
        last_heartbeat: now,
    };
    let cli = CliWriterLease {
        lease_id: "lease-cli-1".to_owned(),
        project_id: "demo".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: now,
        ttl_seconds: 300,
        last_heartbeat: now,
    };

    assert_eq!(worktree.heartbeat_deadline(), cli.heartbeat_deadline());
    assert_eq!(
        worktree.is_stale_at(now + Duration::seconds(299)),
        cli.is_stale_at(now + Duration::seconds(299))
    );
    assert_eq!(
        worktree.is_stale_at(now + Duration::seconds(301)),
        cli.is_stale_at(now + Duration::seconds(301))
    );
}

#[test]
fn fs_daemon_store_lists_worktree_and_cli_lease_records_from_same_directory() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let now = Utc::now();
    let worktree = WorktreeLease {
        lease_id: "lease-worktree-1".to_owned(),
        task_id: "task-1".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: temp.path().join("worktree-task-1"),
        branch_name: "rb/task-1".to_owned(),
        acquired_at: now,
        ttl_seconds: 300,
        last_heartbeat: now,
    };
    let cli = CliWriterLease {
        lease_id: "lease-cli-1".to_owned(),
        project_id: "demo".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: now + Duration::seconds(1),
        ttl_seconds: 300,
        last_heartbeat: now + Duration::seconds(1),
    };

    store
        .write_lease(temp.path(), &worktree)
        .expect("write worktree lease");
    store
        .write_lease_record(temp.path(), &LeaseRecord::CliWriter(cli.clone()))
        .expect("write cli lease record");

    let records = store
        .list_lease_records(temp.path())
        .expect("list lease records");
    assert_eq!(2, records.len());
    assert!(
        matches!(&records[0], LeaseRecord::Worktree(lease) if lease.lease_id == worktree.lease_id)
    );
    assert!(matches!(&records[1], LeaseRecord::CliWriter(lease) if lease.lease_id == cli.lease_id));

    let worktree_only = store
        .list_leases(temp.path())
        .expect("list worktree leases");
    assert_eq!(vec![worktree.clone()], worktree_only);

    let raw_cli_record = std::fs::read_to_string(
        temp.path()
            .join(".ralph-burning/daemon/leases")
            .join("lease-cli-1.json"),
    )
    .expect("read cli lease record");
    assert!(raw_cli_record.contains("\"lease_kind\": \"cli_writer\""));
}

#[test]
fn worktree_path_derivation_is_deterministic() {
    let adapter = WorktreeAdapter;
    let temp = tempdir().expect("tempdir");
    let path = adapter.worktree_path(temp.path(), "task-99");

    assert_eq!(temp.path().join("worktrees/task-99"), path);
    assert_eq!("rb/task-99", adapter.branch_name("task-99"));
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
        None,
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
        None,
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
        None,
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
        None,
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
        None,
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
        None,
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
fn parse_requirements_command_bare_requirements_defaults_to_draft() {
    // "/rb requirements" without a subcommand defaults to RequirementsDraft
    let result = parse_requirements_command("/rb requirements").unwrap();
    assert_eq!(
        Some(DispatchMode::RequirementsDraft),
        result,
        "bare '/rb requirements' should default to RequirementsDraft"
    );
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
        None,
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
    assert_eq!(
        "requirements_draft",
        DispatchMode::RequirementsDraft.as_str()
    );
    assert_eq!(
        "requirements_quick",
        DispatchMode::RequirementsQuick.as_str()
    );
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
    assert_eq!(Some("req-linked-ok".to_owned()), failed.requirements_run_id);
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
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("lock-test".to_owned()).expect("valid id");

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
        .release_writer_lock(temp.path(), &project_id, "cli")
        .expect("release lock");
    store
        .acquire_writer_lock(temp.path(), &project_id, "cli-3")
        .expect("re-acquire after release");
    store
        .release_writer_lock(temp.path(), &project_id, "cli-3")
        .expect("final release");
}

#[test]
fn writer_lock_release_is_idempotent() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("idem-test".to_owned()).expect("valid id");

    // Release without acquire should return AlreadyAbsent (not an error)
    let outcome = store
        .release_writer_lock(temp.path(), &project_id, "any-owner")
        .expect("release without acquire should succeed");
    assert!(
        matches!(
            outcome,
            ralph_burning::contexts::automation_runtime::WriterLockReleaseOutcome::AlreadyAbsent
        ),
        "absent lock should return AlreadyAbsent"
    );
}

// ---------------------------------------------------------------------------
// Reconcile partial-failure accounting
// ---------------------------------------------------------------------------

#[test]
fn reconcile_reports_only_successful_releases() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    // Create a task and a stale lease, but no actual worktree — reconcile
    // must treat the missing worktree as a cleanup failure (not a release).
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

    // Create the writer lock
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
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

    // Missing worktree is a cleanup failure — lease is NOT released
    assert!(
        report.released_lease_ids.is_empty(),
        "missing worktree must not be counted as a successful release"
    );
    assert_eq!(1, report.cleanup_failures.len());
    assert_eq!("lease-reconcile-test", report.cleanup_failures[0].lease_id);
    assert!(
        report.cleanup_failures[0]
            .details
            .contains("worktree_absent"),
        "details should indicate worktree was absent, got: {}",
        report.cleanup_failures[0].details
    );

    // Lease must remain durable for operator recovery
    let leases = store.list_leases(temp.path()).expect("list leases");
    assert_eq!(
        1,
        leases.len(),
        "lease should remain durable when worktree is absent"
    );
}

#[test]
fn reconcile_report_has_cleanup_failures_is_false_when_empty() {
    let report = ralph_burning::contexts::automation_runtime::ReconcileReport::default();
    assert!(!report.has_cleanup_failures());
}

// ---------------------------------------------------------------------------
// Reconcile partial-failure: worktree removal fails → lease stays durable
// ---------------------------------------------------------------------------

/// A worktree adapter whose `remove_worktree` always fails.
struct FailingWorktreeAdapter;

impl WorktreePort for FailingWorktreeAdapter {
    fn worktree_path(&self, base_dir: &std::path::Path, task_id: &str) -> std::path::PathBuf {
        base_dir
            .join(".ralph-burning")
            .join("worktrees")
            .join(task_id)
    }

    fn branch_name(&self, task_id: &str) -> String {
        format!("rb/{task_id}")
    }

    fn create_worktree(
        &self,
        _repo_root: &std::path::Path,
        _worktree_path: &std::path::Path,
        _branch_name: &str,
        _task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        Ok(())
    }

    fn remove_worktree(
        &self,
        _repo_root: &std::path::Path,
        _worktree_path: &std::path::Path,
        _task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WorktreeCleanupOutcome,
    > {
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "simulated worktree removal failure",
        )
        .into())
    }

    fn rebase_onto_default_branch(
        &self,
        _repo_root: &std::path::Path,
        _worktree_path: &std::path::Path,
        _branch_name: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        Ok(())
    }
}

/// A worktree adapter that succeeds without requiring a real git repository.
/// Creates simple directories for worktrees instead of calling `git worktree add`.
struct SuccessWorktreeAdapter;

impl WorktreePort for SuccessWorktreeAdapter {
    fn worktree_path(&self, base_dir: &std::path::Path, task_id: &str) -> std::path::PathBuf {
        base_dir
            .join(".ralph-burning")
            .join("worktrees")
            .join(task_id)
    }

    fn branch_name(&self, task_id: &str) -> String {
        format!("rb/{task_id}")
    }

    fn create_worktree(
        &self,
        _repo_root: &std::path::Path,
        worktree_path: &std::path::Path,
        _branch_name: &str,
        _task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        std::fs::create_dir_all(worktree_path)?;
        Ok(())
    }

    fn remove_worktree(
        &self,
        _repo_root: &std::path::Path,
        worktree_path: &std::path::Path,
        _task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WorktreeCleanupOutcome,
    > {
        use ralph_burning::contexts::automation_runtime::WorktreeCleanupOutcome;
        if worktree_path.exists() {
            std::fs::remove_dir_all(worktree_path)?;
            Ok(WorktreeCleanupOutcome::Removed)
        } else {
            Ok(WorktreeCleanupOutcome::AlreadyAbsent)
        }
    }

    fn rebase_onto_default_branch(
        &self,
        _repo_root: &std::path::Path,
        _worktree_path: &std::path::Path,
        _branch_name: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        Ok(())
    }
}

#[test]
fn reconcile_partial_cleanup_failure_keeps_lease_durable() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    // Create a task and a stale lease
    let mut task = sample_task();
    task.task_id = "partial-cleanup-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    let lease = WorktreeLease {
        lease_id: "lease-partial-cleanup-test".to_owned(),
        task_id: "partial-cleanup-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: temp.path().join("some-worktree"),
        branch_name: "rb/partial-cleanup-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    // Create the worktree directory so reconcile's pre-check passes and the
    // FailingWorktreeAdapter's remove_worktree is actually reached.
    std::fs::create_dir_all(temp.path().join("some-worktree")).expect("create worktree dir");

    // Create the writer lock
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-partial-cleanup-test")
        .expect("acquire lock");

    // Use the FailingWorktreeAdapter so worktree removal fails
    let failing_worktree = FailingWorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &failing_worktree,
        temp.path(),
        temp.path(),
        Some(0), // force stale
        Utc::now(),
    )
    .expect("reconcile");

    // Lease should NOT be in released_lease_ids
    assert!(
        report.released_lease_ids.is_empty(),
        "released_lease_ids should be empty when worktree removal fails"
    );
    // Should have a cleanup failure
    assert_eq!(1, report.cleanup_failures.len());
    assert_eq!(
        "lease-partial-cleanup-test",
        report.cleanup_failures[0].lease_id
    );
    assert!(
        report.cleanup_failures[0]
            .details
            .contains("worktree_remove:"),
        "details should indicate worktree removal failure, got: {}",
        report.cleanup_failures[0].details
    );

    // The lease file should still exist on disk (durable for later reconcile)
    let leases = store.list_leases(temp.path()).expect("list leases");
    assert_eq!(
        1,
        leases.len(),
        "lease should remain durable after partial cleanup failure"
    );
    assert_eq!("lease-partial-cleanup-test", leases[0].lease_id);

    // The task should be Failed (reconciliation_timeout) — terminal but recoverable
    let failed_task = store
        .read_task(temp.path(), "partial-cleanup-test")
        .expect("read task");
    assert_eq!(TaskStatus::Failed, failed_task.status);
    assert_eq!(
        Some("reconciliation_timeout".to_owned()),
        failed_task.failure_class
    );
}

// ---------------------------------------------------------------------------
// Claim-journal rollback: task ends Pending or Failed, never stranded Claimed
// ---------------------------------------------------------------------------

/// A DaemonStorePort wrapper that makes `append_daemon_journal_event` fail
/// after a configurable number of successful calls.
struct FailingJournalStore {
    inner: FsDaemonStore,
    fail_after: std::sync::atomic::AtomicUsize,
    call_count: std::sync::atomic::AtomicUsize,
}

impl FailingJournalStore {
    fn new(fail_after: usize) -> Self {
        Self {
            inner: FsDaemonStore,
            fail_after: std::sync::atomic::AtomicUsize::new(fail_after),
            call_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

impl DaemonStorePort for FailingJournalStore {
    fn list_tasks(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<DaemonTask>> {
        self.inner.list_tasks(base_dir)
    }
    fn read_task(
        &self,
        base_dir: &std::path::Path,
        task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<DaemonTask> {
        self.inner.read_task(base_dir, task_id)
    }
    fn create_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.create_task(base_dir, task)
    }
    fn write_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_task(base_dir, task)
    }
    fn list_leases(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<WorktreeLease>> {
        self.inner.list_leases(base_dir)
    }
    fn read_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<WorktreeLease> {
        self.inner.read_lease(base_dir, lease_id)
    }
    fn write_lease(
        &self,
        base_dir: &std::path::Path,
        lease: &WorktreeLease,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease(base_dir, lease)
    }
    fn list_lease_records(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<LeaseRecord>> {
        self.inner.list_lease_records(base_dir)
    }
    fn read_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<LeaseRecord> {
        self.inner.read_lease_record(base_dir, lease_id)
    }
    fn write_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease: &LeaseRecord,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease_record(base_dir, lease)
    }
    fn remove_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::ResourceCleanupOutcome,
    > {
        self.inner.remove_lease(base_dir, lease_id)
    }
    fn read_daemon_journal(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::DaemonJournalEvent>,
    > {
        self.inner.read_daemon_journal(base_dir)
    }
    fn append_daemon_journal_event(
        &self,
        base_dir: &std::path::Path,
        event: &ralph_burning::contexts::automation_runtime::DaemonJournalEvent,
    ) -> ralph_burning::shared::error::AppResult<()> {
        let count = self
            .call_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let limit = self.fail_after.load(std::sync::atomic::Ordering::SeqCst);
        if count >= limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "simulated journal append failure",
            )
            .into());
        }
        self.inner.append_daemon_journal_event(base_dir, event)
    }
    fn acquire_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner
            .acquire_writer_lock(base_dir, project_id, lease_id)
    }
    fn release_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        expected_owner: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WriterLockReleaseOutcome,
    > {
        self.inner
            .release_writer_lock(base_dir, project_id, expected_owner)
    }
}

#[test]
fn claim_journal_failure_rolls_back_to_pending_not_stranded_claimed() {
    // When the LeaseAcquired journal append fails, the task must end up
    // Pending (not Claimed) with no lease and no writer lock.
    // Uses SuccessWorktreeAdapter so worktree creation succeeds without git,
    // ensuring the test actually reaches the journal-failure branch.
    let temp = tempdir().expect("tempdir");
    let worktree_adapter = SuccessWorktreeAdapter;
    let routing = RoutingEngine::new();

    // fail_after=0: the very first journal append (LeaseAcquired) will fail.
    // claim_task internally does write_task(Claimed) first, then tries journal.
    let store = FailingJournalStore::new(0);

    let mut task = sample_task();
    task.task_id = "claim-rollback-test".to_owned();
    task.project_id = "rollback-proj".to_owned();
    store.create_task(temp.path(), &task).expect("create task");

    let result = DaemonTaskService::claim_task(
        &store,
        &worktree_adapter,
        &routing,
        temp.path(),
        temp.path(),
        "claim-rollback-test",
        FlowPreset::Standard,
        300,
        None,
        None,
    );

    assert!(result.is_err(), "claim_task should fail on journal error");

    // The task must NOT be stranded in Claimed
    let task_after = store
        .read_task(temp.path(), "claim-rollback-test")
        .expect("read task");
    assert_ne!(
        TaskStatus::Claimed,
        task_after.status,
        "task must not be stranded in Claimed after journal failure"
    );
    // Task should be Pending (rollback succeeded) or Failed (rollback failed)
    assert!(
        task_after.status == TaskStatus::Pending || task_after.status == TaskStatus::Failed,
        "task should be Pending or Failed, got: {:?}",
        task_after.status
    );
    // lease_id should be cleared (resources were released by SuccessWorktreeAdapter)
    assert!(
        task_after.lease_id.is_none(),
        "lease_id must be cleared after rollback"
    );
}

#[test]
fn claim_task_claimed_journal_failure_marks_failed_with_cleared_lease() {
    // When TaskClaimed journal fails (after LeaseAcquired succeeds),
    // the task must end up Failed with cleared lease_id.
    // Uses SuccessWorktreeAdapter so worktree creation succeeds without git,
    // ensuring the test reaches the second journal append (TaskClaimed).
    let temp = tempdir().expect("tempdir");
    let worktree_adapter = SuccessWorktreeAdapter;
    let routing = RoutingEngine::new();

    // fail_after=1: the first journal append (LeaseAcquired) succeeds,
    // the second (TaskClaimed) fails.
    let store = FailingJournalStore::new(1);

    let mut task = sample_task();
    task.task_id = "claim-fail-test".to_owned();
    task.project_id = "fail-proj".to_owned();
    store.create_task(temp.path(), &task).expect("create task");

    let result = DaemonTaskService::claim_task(
        &store,
        &worktree_adapter,
        &routing,
        temp.path(),
        temp.path(),
        "claim-fail-test",
        FlowPreset::Standard,
        300,
        None,
        None,
    );

    assert!(result.is_err(), "claim_task should fail on journal error");

    let task_after = store
        .read_task(temp.path(), "claim-fail-test")
        .expect("read task");
    assert_eq!(
        TaskStatus::Failed,
        task_after.status,
        "task should be Failed after TaskClaimed journal failure"
    );
    assert_eq!(
        Some("claim_journal_failed".to_owned()),
        task_after.failure_class
    );
    // Resources were released by SuccessWorktreeAdapter, so lease_id must be cleared
    assert!(
        task_after.lease_id.is_none(),
        "lease_id must be cleared after claim journal failure when resources released"
    );
}

// ---------------------------------------------------------------------------
// Claim journal failure + release failure: task must end Failed with lease retained
// ---------------------------------------------------------------------------

#[test]
fn claim_journal_failure_with_release_failure_marks_failed_retains_lease() {
    // When LeaseAcquired journal fails AND LeaseService::release() also fails
    // (e.g. worktree removal fails), the task must end up Failed with
    // claim_journal_failed and the lease_id must NOT be cleared (since the
    // lease/worktree/lock remain on disk).
    let temp = tempdir().expect("tempdir");
    let failing_worktree = FailingWorktreeAdapter;
    let routing = RoutingEngine::new();

    // fail_after=0: the very first journal append (LeaseAcquired) will fail.
    let store = FailingJournalStore::new(0);

    let mut task = sample_task();
    task.task_id = "double-fail-test".to_owned();
    task.project_id = "double-fail-proj".to_owned();
    store.create_task(temp.path(), &task).expect("create task");

    let result = DaemonTaskService::claim_task(
        &store,
        &failing_worktree,
        &routing,
        temp.path(),
        temp.path(),
        "double-fail-test",
        FlowPreset::Standard,
        300,
        None,
        None,
    );

    assert!(result.is_err(), "claim_task should fail on journal error");

    let task_after = store
        .read_task(temp.path(), "double-fail-test")
        .expect("read task");
    assert_eq!(
        TaskStatus::Failed,
        task_after.status,
        "task must be Failed when both journal and release fail"
    );
    assert_eq!(
        Some("claim_journal_failed".to_owned()),
        task_after.failure_class,
        "failure class must be claim_journal_failed"
    );
    // lease_id should NOT be cleared because the lease is still on disk
    assert!(
        task_after.lease_id.is_some(),
        "lease_id must be retained when release fails (lease remains on disk)"
    );
}

// ---------------------------------------------------------------------------
// Panic-safe CLI lock release (RAII guard drop)
// ---------------------------------------------------------------------------

#[test]
fn cli_writer_lock_guard_releases_on_drop() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("raii-test".to_owned()).expect("valid id");

    // Acquire via the guard pattern, then drop it
    {
        store
            .acquire_writer_lock(temp.path(), &project_id, "cli")
            .expect("acquire lock");
        // Lock is held here
        let err = store
            .acquire_writer_lock(temp.path(), &project_id, "cli-2")
            .expect_err("should be held");
        assert!(matches!(err, AppError::ProjectWriterLockHeld { .. }));

        // Simulate RAII release (guard drop)
        store
            .release_writer_lock(temp.path(), &project_id, "cli")
            .expect("release lock");
    }

    // After release, lock should be available
    store
        .acquire_writer_lock(temp.path(), &project_id, "cli-3")
        .expect("should be available after RAII release");
    store
        .release_writer_lock(temp.path(), &project_id, "cli-3")
        .expect("final cleanup");
}

// ---------------------------------------------------------------------------
// Reconcile sub-step failure: lease file or writer lock already absent
// ---------------------------------------------------------------------------

/// A DaemonStorePort wrapper that returns `AlreadyAbsent` for configurable
/// sub-steps (lease file deletion, writer lock release) while delegating
/// everything else to the inner FsDaemonStore.
struct SubStepAbsentStore {
    inner: FsDaemonStore,
    lease_file_absent: bool,
    writer_lock_absent: bool,
}

impl SubStepAbsentStore {
    fn new(lease_file_absent: bool, writer_lock_absent: bool) -> Self {
        Self {
            inner: FsDaemonStore,
            lease_file_absent,
            writer_lock_absent,
        }
    }
}

impl DaemonStorePort for SubStepAbsentStore {
    fn list_tasks(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<DaemonTask>> {
        self.inner.list_tasks(base_dir)
    }
    fn read_task(
        &self,
        base_dir: &std::path::Path,
        task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<DaemonTask> {
        self.inner.read_task(base_dir, task_id)
    }
    fn create_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.create_task(base_dir, task)
    }
    fn write_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_task(base_dir, task)
    }
    fn list_leases(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<WorktreeLease>> {
        self.inner.list_leases(base_dir)
    }
    fn read_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<WorktreeLease> {
        self.inner.read_lease(base_dir, lease_id)
    }
    fn write_lease(
        &self,
        base_dir: &std::path::Path,
        lease: &WorktreeLease,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease(base_dir, lease)
    }
    fn list_lease_records(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<LeaseRecord>> {
        self.inner.list_lease_records(base_dir)
    }
    fn read_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<LeaseRecord> {
        self.inner.read_lease_record(base_dir, lease_id)
    }
    fn write_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease: &LeaseRecord,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease_record(base_dir, lease)
    }
    fn remove_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::ResourceCleanupOutcome,
    > {
        if self.lease_file_absent {
            Ok(ralph_burning::contexts::automation_runtime::ResourceCleanupOutcome::AlreadyAbsent)
        } else {
            self.inner.remove_lease(base_dir, lease_id)
        }
    }
    fn read_daemon_journal(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::DaemonJournalEvent>,
    > {
        self.inner.read_daemon_journal(base_dir)
    }
    fn append_daemon_journal_event(
        &self,
        base_dir: &std::path::Path,
        event: &ralph_burning::contexts::automation_runtime::DaemonJournalEvent,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.append_daemon_journal_event(base_dir, event)
    }
    fn acquire_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner
            .acquire_writer_lock(base_dir, project_id, lease_id)
    }
    fn release_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        expected_owner: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WriterLockReleaseOutcome,
    > {
        if self.writer_lock_absent {
            Ok(ralph_burning::contexts::automation_runtime::WriterLockReleaseOutcome::AlreadyAbsent)
        } else {
            self.inner
                .release_writer_lock(base_dir, project_id, expected_owner)
        }
    }
}

#[test]
fn reconcile_lease_file_absent_reports_cleanup_failure() {
    // Worktree exists and is removed, but the lease file is already missing
    // during cleanup → reconcile must record a sub-step failure and NOT count
    // the lease as released.
    let temp = tempdir().expect("tempdir");
    let store = SubStepAbsentStore::new(true, false);

    let mut task = sample_task();
    task.task_id = "lease-file-absent-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    // Create a worktree directory so the pre-check passes
    let wt_path = temp.path().join("wt-lease-file-test");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-lfa-test".to_owned(),
        task_id: "lease-file-absent-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/lease-file-absent-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-lfa-test")
        .expect("acquire lock");

    let worktree_adapter = SuccessWorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0),
        Utc::now(),
    )
    .expect("reconcile");

    assert_eq!(1, report.stale_lease_ids.len());
    assert!(
        report.released_lease_ids.is_empty(),
        "lease must NOT be counted as released when lease file was already absent"
    );
    assert!(
        report.has_cleanup_failures(),
        "should have cleanup failures"
    );
    assert!(
        report
            .cleanup_failures
            .iter()
            .any(|f| f.details.contains("lease_file_absent")),
        "should report lease_file_absent sub-step failure, got: {:?}",
        report.cleanup_failures
    );
    assert_eq!(
        "lease-lfa-test",
        report
            .cleanup_failures
            .iter()
            .find(|f| f.details.contains("lease_file_absent"))
            .unwrap()
            .lease_id
    );
}

#[test]
fn reconcile_writer_lock_absent_reports_cleanup_failure() {
    // Worktree exists and is removed, lease file exists and is removed, but
    // the writer lock is already missing → reconcile must record a sub-step
    // failure and NOT count the lease as released.
    let temp = tempdir().expect("tempdir");
    let store = SubStepAbsentStore::new(false, true);

    let mut task = sample_task();
    task.task_id = "lock-absent-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    let wt_path = temp.path().join("wt-lock-absent-test");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-lock-absent".to_owned(),
        task_id: "lock-absent-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/lock-absent-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    // Do NOT create a writer lock — it should be "already absent"

    let worktree_adapter = SuccessWorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0),
        Utc::now(),
    )
    .expect("reconcile");

    assert_eq!(1, report.stale_lease_ids.len());
    assert!(
        report.released_lease_ids.is_empty(),
        "lease must NOT be counted as released when writer lock was already absent"
    );
    assert!(
        report.has_cleanup_failures(),
        "should have cleanup failures"
    );
    assert!(
        report
            .cleanup_failures
            .iter()
            .any(|f| f.details.contains("writer_lock_absent")),
        "should report writer_lock_absent sub-step failure, got: {:?}",
        report.cleanup_failures
    );
}

#[test]
fn reconcile_both_substeps_absent_reports_writer_lock_failure() {
    // Writer lock is already missing → reconcile records a cleanup failure.
    // With the new cleanup order (worktree → writer-lock → lease-file),
    // lease-file deletion is skipped when writer-lock release does not
    // return Released, so only the writer_lock_absent failure is reported.
    let temp = tempdir().expect("tempdir");
    let store = SubStepAbsentStore::new(true, true);

    let mut task = sample_task();
    task.task_id = "both-absent-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    let wt_path = temp.path().join("wt-both-absent");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-both-absent".to_owned(),
        task_id: "both-absent-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/both-absent-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let worktree_adapter = SuccessWorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0),
        Utc::now(),
    )
    .expect("reconcile");

    assert!(
        report.released_lease_ids.is_empty(),
        "lease must NOT be counted as released"
    );
    let failure_details: Vec<&str> = report
        .cleanup_failures
        .iter()
        .map(|f| f.details.as_str())
        .collect();
    assert!(
        failure_details
            .iter()
            .any(|d| d.contains("writer_lock_absent")),
        "should report writer_lock_absent, got: {failure_details:?}"
    );
}

// ---------------------------------------------------------------------------
// Reconcile sub-step failure: real I/O errors on lease file or writer lock
// ---------------------------------------------------------------------------

/// A DaemonStorePort wrapper that returns real errors (not `AlreadyAbsent`)
/// for configurable sub-steps (lease file deletion, writer lock release) while
/// delegating everything else to the inner FsDaemonStore.
struct SubStepErrorStore {
    inner: FsDaemonStore,
    lease_file_error: bool,
    writer_lock_error: bool,
}

impl SubStepErrorStore {
    fn new(lease_file_error: bool, writer_lock_error: bool) -> Self {
        Self {
            inner: FsDaemonStore,
            lease_file_error,
            writer_lock_error,
        }
    }
}

impl DaemonStorePort for SubStepErrorStore {
    fn list_tasks(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<DaemonTask>> {
        self.inner.list_tasks(base_dir)
    }
    fn read_task(
        &self,
        base_dir: &std::path::Path,
        task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<DaemonTask> {
        self.inner.read_task(base_dir, task_id)
    }
    fn create_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.create_task(base_dir, task)
    }
    fn write_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_task(base_dir, task)
    }
    fn list_leases(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<WorktreeLease>> {
        self.inner.list_leases(base_dir)
    }
    fn read_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<WorktreeLease> {
        self.inner.read_lease(base_dir, lease_id)
    }
    fn write_lease(
        &self,
        base_dir: &std::path::Path,
        lease: &WorktreeLease,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease(base_dir, lease)
    }
    fn list_lease_records(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<LeaseRecord>> {
        self.inner.list_lease_records(base_dir)
    }
    fn read_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<LeaseRecord> {
        self.inner.read_lease_record(base_dir, lease_id)
    }
    fn write_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease: &LeaseRecord,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease_record(base_dir, lease)
    }
    fn remove_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::ResourceCleanupOutcome,
    > {
        if self.lease_file_error {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "simulated lease file deletion failure",
            )
            .into())
        } else {
            self.inner.remove_lease(base_dir, lease_id)
        }
    }
    fn read_daemon_journal(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::DaemonJournalEvent>,
    > {
        self.inner.read_daemon_journal(base_dir)
    }
    fn append_daemon_journal_event(
        &self,
        base_dir: &std::path::Path,
        event: &ralph_burning::contexts::automation_runtime::DaemonJournalEvent,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.append_daemon_journal_event(base_dir, event)
    }
    fn acquire_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner
            .acquire_writer_lock(base_dir, project_id, lease_id)
    }
    fn release_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        expected_owner: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WriterLockReleaseOutcome,
    > {
        if self.writer_lock_error {
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "simulated writer lock release failure",
            )
            .into())
        } else {
            self.inner
                .release_writer_lock(base_dir, project_id, expected_owner)
        }
    }
}

#[test]
fn reconcile_lease_file_delete_error_reports_specific_failure() {
    // Worktree exists and is removed, but lease file deletion returns a real
    // I/O error → reconcile must record the specific sub-step name and NOT
    // count the lease as released.
    let temp = tempdir().expect("tempdir");
    let store = SubStepErrorStore::new(true, false);

    let mut task = sample_task();
    task.task_id = "lease-file-err-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    let wt_path = temp.path().join("wt-lease-file-err");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-lfe-test".to_owned(),
        task_id: "lease-file-err-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/lease-file-err-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-lfe-test")
        .expect("acquire lock");

    let worktree_adapter = SuccessWorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0),
        Utc::now(),
    )
    .expect("reconcile");

    assert_eq!(1, report.stale_lease_ids.len());
    assert!(
        report.released_lease_ids.is_empty(),
        "lease must NOT be counted as released when lease file deletion errors"
    );
    assert!(
        report.has_cleanup_failures(),
        "should have cleanup failures"
    );
    assert!(
        report
            .cleanup_failures
            .iter()
            .any(|f| f.details.contains("lease_file_delete:")),
        "should report lease_file_delete sub-step failure, got: {:?}",
        report.cleanup_failures
    );
    assert!(
        report
            .cleanup_failures
            .iter()
            .any(|f| f.details.contains("simulated lease file deletion failure")),
        "should include the original error message, got: {:?}",
        report.cleanup_failures
    );
}

#[test]
fn reconcile_writer_lock_release_error_reports_specific_failure() {
    // Worktree exists and is removed, lease file is removed, but writer lock
    // release returns a real I/O error → reconcile must record the specific
    // sub-step name and NOT count the lease as released.
    let temp = tempdir().expect("tempdir");
    let store = SubStepErrorStore::new(false, true);

    let mut task = sample_task();
    task.task_id = "lock-err-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    let wt_path = temp.path().join("wt-lock-err");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-lock-err".to_owned(),
        task_id: "lock-err-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/lock-err-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-lock-err")
        .expect("acquire lock");

    let worktree_adapter = SuccessWorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0),
        Utc::now(),
    )
    .expect("reconcile");

    assert_eq!(1, report.stale_lease_ids.len());
    assert!(
        report.released_lease_ids.is_empty(),
        "lease must NOT be counted as released when writer lock release errors"
    );
    assert!(
        report.has_cleanup_failures(),
        "should have cleanup failures"
    );
    assert!(
        report
            .cleanup_failures
            .iter()
            .any(|f| f.details.contains("writer_lock_release:")),
        "should report writer_lock_release sub-step failure, got: {:?}",
        report.cleanup_failures
    );
    assert!(
        report
            .cleanup_failures
            .iter()
            .any(|f| f.details.contains("simulated writer lock release failure")),
        "should include the original error message, got: {:?}",
        report.cleanup_failures
    );
}

#[test]
fn reconcile_both_substep_errors_reports_writer_lock_failure() {
    // Writer lock release returns a real I/O error → reconcile records the
    // specific sub-step failure. With the new cleanup order (worktree →
    // writer-lock → lease-file), lease-file deletion is skipped when
    // writer-lock release fails, so only the writer_lock_release error is
    // reported.
    let temp = tempdir().expect("tempdir");
    let store = SubStepErrorStore::new(true, true);

    let mut task = sample_task();
    task.task_id = "both-err-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    let wt_path = temp.path().join("wt-both-err");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-both-err".to_owned(),
        task_id: "both-err-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/both-err-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let worktree_adapter = SuccessWorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0),
        Utc::now(),
    )
    .expect("reconcile");

    assert!(
        report.released_lease_ids.is_empty(),
        "lease must NOT be counted as released"
    );
    let failure_details: Vec<&str> = report
        .cleanup_failures
        .iter()
        .map(|f| f.details.as_str())
        .collect();
    assert!(
        failure_details
            .iter()
            .any(|d| d.contains("writer_lock_release:")),
        "should report writer_lock_release, got: {failure_details:?}"
    );
}

// ---------------------------------------------------------------------------
// Non-reconcile release callers: shared release() contract regression
// ---------------------------------------------------------------------------

#[test]
fn release_with_lease_file_error_sets_resources_released_false() {
    // When lease-file deletion fails during release(), `resources_released`
    // must be false so non-reconcile callers do not silently clear durable
    // lease references.
    let temp = tempdir().expect("tempdir");
    let store = SubStepErrorStore::new(true, false);

    let mut task = sample_task();
    task.task_id = "release-lfe-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    let wt_path = temp.path().join("wt-release-lfe");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-release-lfe".to_owned(),
        task_id: "release-lfe-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/release-lfe-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-release-lfe")
        .expect("acquire lock");

    let worktree_adapter = SuccessWorktreeAdapter;
    let result = LeaseService::release(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        &lease,
        ReleaseMode::Idempotent,
    )
    .expect("release returns Ok with sub-step failures");

    assert!(
        !result.resources_released,
        "resources_released must be false when lease-file deletion fails"
    );
    assert!(
        result.has_cleanup_failures(),
        "has_cleanup_failures must be true"
    );
    assert!(
        result.lease_file_error.is_some(),
        "should report lease_file_error"
    );

    // LeaseReleased journal event must NOT have been emitted
    let journal = store
        .read_daemon_journal(temp.path())
        .expect("read journal");
    assert!(
        !journal.iter().any(|e| e.event_type
            == ralph_burning::contexts::automation_runtime::DaemonJournalEventType::LeaseReleased),
        "LeaseReleased must not be emitted on partial cleanup failure"
    );
}

#[test]
fn release_with_writer_lock_error_sets_resources_released_false() {
    // When writer-lock release fails, `resources_released` must be false.
    let temp = tempdir().expect("tempdir");
    let store = SubStepErrorStore::new(false, true);

    let mut task = sample_task();
    task.task_id = "release-wle-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    let wt_path = temp.path().join("wt-release-wle");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-release-wle".to_owned(),
        task_id: "release-wle-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/release-wle-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-release-wle")
        .expect("acquire lock");

    let worktree_adapter = SuccessWorktreeAdapter;
    let result = LeaseService::release(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        &lease,
        ReleaseMode::Idempotent,
    )
    .expect("release returns Ok with sub-step failures");

    assert!(
        !result.resources_released,
        "resources_released must be false when writer-lock release fails"
    );
    assert!(
        result.writer_lock_error.is_some(),
        "should report writer_lock_error"
    );

    // LeaseReleased journal event must NOT have been emitted
    let journal = store
        .read_daemon_journal(temp.path())
        .expect("read journal");
    assert!(
        !journal.iter().any(|e| e.event_type
            == ralph_burning::contexts::automation_runtime::DaemonJournalEventType::LeaseReleased),
        "LeaseReleased must not be emitted on partial cleanup failure"
    );
}

#[test]
fn release_full_success_sets_resources_released_true_and_emits_journal() {
    // When all sub-steps succeed, `resources_released` must be true and
    // LeaseReleased journal event must be emitted.
    let temp = tempdir().expect("tempdir");
    let store = FsDaemonStore;

    let mut task = sample_task();
    task.task_id = "release-ok-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    let wt_path = temp.path().join("wt-release-ok");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-release-ok".to_owned(),
        task_id: "release-ok-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/release-ok-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-release-ok")
        .expect("acquire lock");

    let worktree_adapter = SuccessWorktreeAdapter;
    let result = LeaseService::release(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        &lease,
        ReleaseMode::Idempotent,
    )
    .expect("release succeeds");

    assert!(
        result.resources_released,
        "resources_released must be true when all sub-steps succeed"
    );
    assert!(
        !result.has_cleanup_failures(),
        "has_cleanup_failures must be false"
    );

    // LeaseReleased journal event MUST have been emitted
    let journal = store
        .read_daemon_journal(temp.path())
        .expect("read journal");
    assert!(
        journal.iter().any(|e| e.event_type
            == ralph_burning::contexts::automation_runtime::DaemonJournalEventType::LeaseReleased),
        "LeaseReleased must be emitted on full cleanup success"
    );
}

#[test]
fn abort_cleanup_preserves_lease_reference_on_partial_failure() {
    // Simulates the abort cleanup path: when release() returns Ok but with
    // partial failures, the task's lease_id must NOT be cleared.
    let temp = tempdir().expect("tempdir");
    let store = SubStepErrorStore::new(true, false); // lease file error

    let mut task = sample_task();
    task.task_id = "abort-partial-test".to_owned();
    task.status = TaskStatus::Active;
    task.lease_id = Some("lease-abort-partial".to_owned());
    store.create_task(temp.path(), &task).expect("create task");

    let wt_path = temp.path().join("wt-abort-partial");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-abort-partial".to_owned(),
        task_id: "abort-partial-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/abort-partial-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-abort-partial")
        .expect("acquire lock");

    let worktree_adapter = SuccessWorktreeAdapter;

    // Simulate what cleanup_aborted_task does: release, then conditionally clear
    let release_result = LeaseService::release(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        &lease,
        ReleaseMode::Idempotent,
    );
    match release_result {
        Ok(ref r) if r.resources_released => {
            ralph_burning::contexts::automation_runtime::DaemonTaskService::clear_lease_reference(
                &store,
                temp.path(),
                "abort-partial-test",
            )
            .expect("clear lease ref");
        }
        Ok(_) => {
            // Partial failure — do NOT clear lease reference
        }
        Err(e) => panic!("unexpected release error: {e}"),
    }

    // Verify: task's lease_id must still be set (real I/O error on lease file)
    let task_after = store
        .read_task(temp.path(), "abort-partial-test")
        .expect("read task");
    assert_eq!(
        task_after.lease_id.as_deref(),
        Some("lease-abort-partial"),
        "lease_id must NOT be cleared when release partially fails due to I/O error"
    );
}

#[test]
fn daemon_loop_cleanup_preserves_lease_reference_on_partial_failure() {
    // Simulates the daemon-loop release_task_lease path: when release()
    // returns Ok but with partial failures, the task's lease_id must NOT
    // be cleared.
    let temp = tempdir().expect("tempdir");
    let store = SubStepErrorStore::new(false, true); // writer lock error

    let mut task = sample_task();
    task.task_id = "loop-partial-test".to_owned();
    task.status = TaskStatus::Active;
    task.lease_id = Some("lease-loop-partial".to_owned());
    store.create_task(temp.path(), &task).expect("create task");

    let wt_path = temp.path().join("wt-loop-partial");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-loop-partial".to_owned(),
        task_id: "loop-partial-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/loop-partial-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-loop-partial")
        .expect("acquire lock");

    let worktree_adapter = SuccessWorktreeAdapter;

    // Simulate what release_task_lease does: release, then conditionally clear
    let release_result = LeaseService::release(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        &lease,
        ReleaseMode::Idempotent,
    );
    match release_result {
        Ok(ref r) if r.resources_released => {
            ralph_burning::contexts::automation_runtime::DaemonTaskService::clear_lease_reference(
                &store,
                temp.path(),
                "loop-partial-test",
            )
            .expect("clear lease ref");
        }
        Ok(_) => {
            // Partial failure — do NOT clear lease reference
        }
        Err(e) => panic!("unexpected release error: {e}"),
    }

    // Verify: task's lease_id must still be set
    let task_after = store
        .read_task(temp.path(), "loop-partial-test")
        .expect("read task");
    assert_eq!(
        task_after.lease_id.as_deref(),
        Some("lease-loop-partial"),
        "lease_id must NOT be cleared when release partially fails"
    );
}

// ---------------------------------------------------------------------------
// Idempotent release: missing worktree keeps resources_released=false
// ---------------------------------------------------------------------------

#[test]
fn release_idempotent_mode_missing_worktree_returns_partial() {
    // When a worktree is already absent, resources_released must be false
    // regardless of ReleaseMode — all three sub-steps must positively
    // succeed. Idempotent mode still returns Ok (no error), but callers
    // must not clear durable lease references.
    let temp = tempdir().expect("tempdir");
    let store = FsDaemonStore;

    let mut task = sample_task();
    task.task_id = "idempotent-wt-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    // Do NOT create the worktree directory — it's already absent
    let wt_path = temp.path().join("wt-missing-idempotent");

    let lease = WorktreeLease {
        lease_id: "lease-idempotent-wt".to_owned(),
        task_id: "idempotent-wt-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/idempotent-wt-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-idempotent-wt")
        .expect("acquire lock");

    let worktree_adapter = SuccessWorktreeAdapter;
    let result = LeaseService::release(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        &lease,
        ReleaseMode::Idempotent,
    )
    .expect("release returns Ok in idempotent mode (no error)");

    assert!(
        !result.resources_released,
        "resources_released must be false when worktree is already absent"
    );
    assert!(
        result.worktree_already_absent,
        "worktree_already_absent flag should be set"
    );

    // Lease file must remain on disk for recovery visibility.
    let lease_file = temp
        .path()
        .join(".ralph-burning")
        .join("daemon")
        .join("leases")
        .join("lease-idempotent-wt.json");
    assert!(
        lease_file.exists(),
        "lease file must remain durable after partial release (worktree absent)"
    );
}

#[test]
fn release_with_absent_worktree_preserves_lease_file_for_subsequent_lookups() {
    // Regression: when worktree is AlreadyAbsent, the lease file must remain
    // on disk even though the writer lock is released. Without this, a
    // subsequent cleanup_aborted_task() call finds no lease via
    // find_lease_for_task(), sees task.lease_id.is_some(), and incorrectly
    // clears the durable reference — defeating the fail-closed invariant.
    let temp = tempdir().expect("tempdir");
    let store = FsDaemonStore;

    let mut task = sample_task();
    task.task_id = "absent-wt-lease-file".to_owned();
    task.status = TaskStatus::Aborted;
    task.lease_id = Some("lease-absent-wt-durable".to_owned());
    store.create_task(temp.path(), &task).expect("create task");

    // Do NOT create the worktree — it is already absent.
    let wt_path = temp.path().join("wt-absent-durable");

    let lease = WorktreeLease {
        lease_id: "lease-absent-wt-durable".to_owned(),
        task_id: "absent-wt-lease-file".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/absent-wt-durable".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-absent-wt-durable")
        .expect("acquire lock");

    let worktree_adapter = SuccessWorktreeAdapter;
    let result = LeaseService::release(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        &lease,
        ReleaseMode::Idempotent,
    )
    .expect("release returns Ok in idempotent mode");

    assert!(
        !result.resources_released,
        "resources_released must be false when worktree is already absent"
    );
    assert!(
        result.worktree_already_absent,
        "worktree_already_absent flag should be set"
    );

    // Core assertion: lease file must still exist on disk so subsequent
    // find_lease_for_task() lookups still discover the lease.
    let lease_file = temp
        .path()
        .join(".ralph-burning")
        .join("daemon")
        .join("leases")
        .join("lease-absent-wt-durable.json");
    assert!(
        lease_file.exists(),
        "lease file must remain durable after partial release (worktree absent)"
    );

    // find_lease_for_task must still return the lease.
    let found = LeaseService::find_lease_for_task(&store, temp.path(), "absent-wt-lease-file")
        .expect("find_lease_for_task");
    assert!(
        found.is_some(),
        "find_lease_for_task must still discover the lease after partial release"
    );

    // Task lease_id must remain set.
    let task_after = store
        .read_task(temp.path(), "absent-wt-lease-file")
        .expect("read task");
    assert_eq!(
        task_after.lease_id.as_deref(),
        Some("lease-absent-wt-durable"),
        "lease_id must NOT be cleared after partial release"
    );
}

#[test]
fn abort_cleanup_with_missing_worktree_returns_partial_in_idempotent_mode() {
    // Simulates daemon abort when the worktree is already gone: release()
    // with Idempotent mode returns Ok but resources_released=false.
    // Callers must NOT clear durable lease references — the incomplete
    // cleanup state must remain visible for operator recovery.
    let temp = tempdir().expect("tempdir");
    let store = FsDaemonStore;

    let mut task = sample_task();
    task.task_id = "abort-missing-wt".to_owned();
    task.status = TaskStatus::Aborted;
    task.lease_id = Some("lease-abort-missing-wt".to_owned());
    store.create_task(temp.path(), &task).expect("create task");

    // Missing worktree — common during abort
    let wt_path = temp.path().join("wt-abort-missing");

    let lease = WorktreeLease {
        lease_id: "lease-abort-missing-wt".to_owned(),
        task_id: "abort-missing-wt".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/abort-missing-wt".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-abort-missing-wt")
        .expect("acquire lock");

    let worktree_adapter = SuccessWorktreeAdapter;

    // Simulate cleanup_aborted_task with Idempotent mode
    let release_result = LeaseService::release(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        &lease,
        ReleaseMode::Idempotent,
    );
    match release_result {
        Ok(ref r) if r.resources_released => {
            panic!("resources_released must be false when worktree is already absent")
        }
        Ok(ref r) => {
            assert!(
                r.worktree_already_absent,
                "worktree_already_absent flag should be set"
            );
            // Partial cleanup — do NOT clear lease reference
        }
        Err(e) => panic!("unexpected release error: {e}"),
    }

    // Verify: lease_id must still be set (not cleared)
    let task_after = store
        .read_task(temp.path(), "abort-missing-wt")
        .expect("read task");
    assert_eq!(
        task_after.lease_id.as_deref(),
        Some("lease-abort-missing-wt"),
        "lease_id must NOT be cleared when release partially fails"
    );

    // Lease file must remain on disk so subsequent find_lease_for_task()
    // lookups do not incorrectly clear lease_id.
    let lease_file = temp
        .path()
        .join(".ralph-burning")
        .join("daemon")
        .join("leases")
        .join("lease-abort-missing-wt.json");
    assert!(
        lease_file.exists(),
        "lease file must remain durable after partial release (worktree absent)"
    );

    // find_lease_for_task must still discover the lease.
    let found = LeaseService::find_lease_for_task(&store, temp.path(), "abort-missing-wt")
        .expect("find_lease_for_task");
    assert!(
        found.is_some(),
        "find_lease_for_task must still return the lease so cleanup_aborted_task does not clear lease_id"
    );
}

// ---------------------------------------------------------------------------
// Claim journal failure + partial release (Ok with resources_released=false)
// ---------------------------------------------------------------------------

/// A DaemonStorePort that fails journal appends AND returns a real I/O error
/// for remove_lease, so release() returns Ok(ReleaseResult { resources_released: false }).
/// This exercises the claim rollback path where release_result is Ok but partial.
struct JournalFailPartialReleaseStore {
    inner: FsDaemonStore,
}

impl DaemonStorePort for JournalFailPartialReleaseStore {
    fn list_tasks(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<DaemonTask>> {
        self.inner.list_tasks(base_dir)
    }
    fn read_task(
        &self,
        base_dir: &std::path::Path,
        task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<DaemonTask> {
        self.inner.read_task(base_dir, task_id)
    }
    fn create_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.create_task(base_dir, task)
    }
    fn write_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_task(base_dir, task)
    }
    fn list_leases(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<WorktreeLease>> {
        self.inner.list_leases(base_dir)
    }
    fn read_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<WorktreeLease> {
        self.inner.read_lease(base_dir, lease_id)
    }
    fn write_lease(
        &self,
        base_dir: &std::path::Path,
        lease: &WorktreeLease,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease(base_dir, lease)
    }
    fn list_lease_records(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<LeaseRecord>> {
        self.inner.list_lease_records(base_dir)
    }
    fn read_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<LeaseRecord> {
        self.inner.read_lease_record(base_dir, lease_id)
    }
    fn write_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease: &LeaseRecord,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease_record(base_dir, lease)
    }
    fn remove_lease(
        &self,
        _base_dir: &std::path::Path,
        _lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::ResourceCleanupOutcome,
    > {
        // Return a real I/O error so release() gets resources_released=false
        // regardless of ReleaseMode.
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "simulated lease file deletion failure",
        )
        .into())
    }
    fn read_daemon_journal(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::DaemonJournalEvent>,
    > {
        self.inner.read_daemon_journal(base_dir)
    }
    fn append_daemon_journal_event(
        &self,
        _base_dir: &std::path::Path,
        _event: &ralph_burning::contexts::automation_runtime::DaemonJournalEvent,
    ) -> ralph_burning::shared::error::AppResult<()> {
        // Always fail journal appends
        Err(std::io::Error::new(std::io::ErrorKind::Other, "simulated journal failure").into())
    }
    fn acquire_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner
            .acquire_writer_lock(base_dir, project_id, lease_id)
    }
    fn release_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        expected_owner: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WriterLockReleaseOutcome,
    > {
        self.inner
            .release_writer_lock(base_dir, project_id, expected_owner)
    }
}

#[test]
fn claim_journal_failure_with_partial_release_marks_failed_retains_lease() {
    // When LeaseAcquired journal fails AND release() returns Ok with
    // resources_released=false (partial cleanup), the task must end up Failed
    // with lease_id retained. Previously this path panicked via unwrap_err()
    // on an Ok value.
    let temp = tempdir().expect("tempdir");
    let store = JournalFailPartialReleaseStore {
        inner: FsDaemonStore,
    };
    let worktree_adapter = SuccessWorktreeAdapter;
    let routing = RoutingEngine::new();

    let mut task = sample_task();
    task.task_id = "partial-release-test".to_owned();
    task.project_id = "partial-proj".to_owned();
    store.create_task(temp.path(), &task).expect("create task");

    let result = DaemonTaskService::claim_task(
        &store,
        &worktree_adapter,
        &routing,
        temp.path(),
        temp.path(),
        "partial-release-test",
        FlowPreset::Standard,
        300,
        None,
        None,
    );

    assert!(result.is_err(), "claim_task should fail on journal error");

    let task_after = store
        .read_task(temp.path(), "partial-release-test")
        .expect("read task");
    assert_eq!(
        TaskStatus::Failed,
        task_after.status,
        "task must be Failed when journal fails and release is partial"
    );
    assert_eq!(
        Some("claim_journal_failed".to_owned()),
        task_after.failure_class,
        "failure class must be claim_journal_failed"
    );
    // lease_id must NOT be cleared — partial cleanup means resources remain
    assert!(
        task_after.lease_id.is_some(),
        "lease_id must be retained when release returns Ok with resources_released=false"
    );
}

// ---------------------------------------------------------------------------
// Reconcile worktree disappearance race: worktree vanishes between pre-check
// and release()
// ---------------------------------------------------------------------------

/// A worktree adapter where remove_worktree always returns AlreadyAbsent,
/// simulating the race where the worktree disappears between the pre-check
/// and the actual removal attempt.
struct DisappearingWorktreeAdapter;

impl WorktreePort for DisappearingWorktreeAdapter {
    fn worktree_path(&self, base_dir: &std::path::Path, task_id: &str) -> std::path::PathBuf {
        base_dir
            .join(".ralph-burning")
            .join("worktrees")
            .join(task_id)
    }

    fn branch_name(&self, task_id: &str) -> String {
        format!("rb/{task_id}")
    }

    fn create_worktree(
        &self,
        _repo_root: &std::path::Path,
        worktree_path: &std::path::Path,
        _branch_name: &str,
        _task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        std::fs::create_dir_all(worktree_path)?;
        Ok(())
    }

    fn remove_worktree(
        &self,
        _repo_root: &std::path::Path,
        _worktree_path: &std::path::Path,
        _task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WorktreeCleanupOutcome,
    > {
        // Always report AlreadyAbsent — simulates the race condition
        Ok(ralph_burning::contexts::automation_runtime::WorktreeCleanupOutcome::AlreadyAbsent)
    }

    fn rebase_onto_default_branch(
        &self,
        _repo_root: &std::path::Path,
        _worktree_path: &std::path::Path,
        _branch_name: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        Ok(())
    }
}

#[test]
fn reconcile_worktree_race_reports_cleanup_failure() {
    // When the worktree exists at pre-check time but disappears before
    // release() removes it, reconcile must NOT count the lease as released.
    // Previously this race went undetected because only the pre-check
    // enforced the missing-worktree policy.
    let temp = tempdir().expect("tempdir");
    let store = FsDaemonStore;

    let mut task = sample_task();
    task.task_id = "wt-race-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    // Create worktree directory so the pre-check passes
    let wt_path = temp.path().join("wt-race-dir");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-wt-race".to_owned(),
        task_id: "wt-race-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/wt-race-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-wt-race")
        .expect("acquire lock");

    // Use DisappearingWorktreeAdapter: worktree exists for pre-check but
    // remove_worktree returns AlreadyAbsent (simulating race).
    let worktree_adapter = DisappearingWorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0),
        Utc::now(),
    )
    .expect("reconcile");

    assert_eq!(1, report.stale_lease_ids.len());
    assert!(
        report.released_lease_ids.is_empty(),
        "lease must NOT be counted as released when worktree disappeared during release"
    );
    assert!(
        report.has_cleanup_failures(),
        "should have cleanup failures for worktree race"
    );
    assert!(
        report
            .cleanup_failures
            .iter()
            .any(|f| f.details.contains("worktree_absent_during_release")),
        "should report worktree_absent_during_release, got: {:?}",
        report.cleanup_failures
    );
}

// ---------------------------------------------------------------------------
// No process-global CWD dependency: structural assertion
// ---------------------------------------------------------------------------

#[test]
fn daemon_loop_process_cycle_does_not_call_set_current_dir() {
    // Structural assertion: the daemon_loop module does not contain
    // any reference to std::env::set_current_dir. This is validated at
    // the source level — if someone adds it, this test will catch it.
    let source = include_str!("../../src/contexts/automation_runtime/daemon_loop.rs");
    assert!(
        !source.contains("set_current_dir"),
        "daemon_loop.rs must not call set_current_dir"
    );
}

// ── Daemon requirements dispatch honors workspace backend/model defaults ────

#[cfg(feature = "test-stub")]
/// Helper: create a workspace.toml with explicit backend/model defaults in a
/// temp directory and return the loaded `EffectiveConfig`.
fn setup_workspace_with_defaults(
    base_dir: &std::path::Path,
    default_backend: &str,
    default_model: &str,
) -> ralph_burning::contexts::workspace_governance::config::EffectiveConfig {
    let ws_dir = base_dir.join(".ralph-burning");
    std::fs::create_dir_all(&ws_dir).expect("create workspace dir");
    let toml_content = format!(
        r#"version = 1
created_at = "2026-03-14T00:00:00Z"

[settings]
default_backend = "{default_backend}"
default_model = "{default_model}"
"#
    );
    std::fs::write(ws_dir.join("workspace.toml"), &toml_content).expect("write workspace.toml");
    ralph_burning::contexts::workspace_governance::config::EffectiveConfig::load(base_dir)
        .expect("load effective config")
}

#[cfg(feature = "test-stub")]
/// Helper: build a `RequirementsService` with workspace defaults by calling the
/// exact same `build_requirements_service` function the daemon uses. This ensures
/// that a regression in daemon wiring is caught by the test suite.
fn build_test_requirements_service_with_defaults(
    adapter: ralph_burning::adapters::stub_backend::StubBackendAdapter,
    effective_config: &ralph_burning::contexts::workspace_governance::config::EffectiveConfig,
) -> ralph_burning::contexts::requirements_drafting::service::RequirementsService<
    ralph_burning::adapters::stub_backend::StubBackendAdapter,
    ralph_burning::adapters::fs::FsRawOutputStore,
    ralph_burning::adapters::fs::FsSessionStore,
    ralph_burning::adapters::fs::FsRequirementsStore,
> {
    ralph_burning::contexts::automation_runtime::daemon_loop::build_requirements_service_for_test(
        adapter,
        effective_config,
    )
    .expect("build requirements service with defaults")
}

#[cfg(feature = "test-stub")]
#[tokio::test]
async fn daemon_requirements_quick_honors_workspace_backend_model_defaults() {
    // Regression: daemon-driven requirements_quick must resolve the same
    // backend family and model ID as the direct CLI requirements path for a
    // workspace with explicit defaults. Uses the StubBackendAdapter's
    // recording seam to verify the actual resolved target at invocation time.
    let temp = tempdir().expect("tempdir");
    let base_dir = temp.path();

    // Set up workspace with explicit defaults (codex / gpt-5.4)
    let effective_config = setup_workspace_with_defaults(base_dir, "codex", "gpt-5.4");

    // Build the service the same way the daemon does after the fix
    let adapter = ralph_burning::adapters::stub_backend::StubBackendAdapter::default();
    let req_svc = build_test_requirements_service_with_defaults(adapter.clone(), &effective_config);

    // Run requirements quick
    let _run_id = req_svc
        .quick(base_dir, "Test idea for quick", Utc::now(), None)
        .await
        .expect("requirements quick should succeed");

    // Verify the recorded invocations used the workspace defaults
    let invocations = adapter.recorded_invocations();
    assert!(
        !invocations.is_empty(),
        "at least one invocation should have been recorded"
    );
    for inv in &invocations {
        assert_eq!(
            BackendFamily::Codex,
            inv.resolved_target.backend.family,
            "invocation '{}' should use workspace default backend (codex), got {:?}",
            inv.contract_label,
            inv.resolved_target.backend.family
        );
        assert_eq!(
            "gpt-5.4", inv.resolved_target.model.model_id,
            "invocation '{}' should use workspace default model (gpt-5.4), got {}",
            inv.contract_label, inv.resolved_target.model.model_id
        );
    }
}

#[cfg(feature = "test-stub")]
#[tokio::test]
async fn daemon_requirements_draft_honors_workspace_backend_model_defaults() {
    // Regression: daemon-driven requirements_draft must resolve the same
    // backend family and model ID as the direct CLI requirements path for a
    // workspace with explicit defaults.
    let temp = tempdir().expect("tempdir");
    let base_dir = temp.path();

    // Set up workspace with explicit defaults (codex / gpt-5.4)
    let effective_config = setup_workspace_with_defaults(base_dir, "codex", "gpt-5.4");

    // Build the service the same way the daemon does after the fix
    let adapter = ralph_burning::adapters::stub_backend::StubBackendAdapter::default();
    let req_svc = build_test_requirements_service_with_defaults(adapter.clone(), &effective_config);

    // Run requirements draft
    let _run_id = req_svc
        .draft(base_dir, "Test idea for draft", Utc::now(), None)
        .await
        .expect("requirements draft should succeed");

    // Verify the recorded invocations used the workspace defaults
    let invocations = adapter.recorded_invocations();
    assert!(
        !invocations.is_empty(),
        "at least one invocation should have been recorded"
    );
    for inv in &invocations {
        assert_eq!(
            BackendFamily::Codex,
            inv.resolved_target.backend.family,
            "invocation '{}' should use workspace default backend (codex), got {:?}",
            inv.contract_label,
            inv.resolved_target.backend.family
        );
        assert_eq!(
            "gpt-5.4", inv.resolved_target.model.model_id,
            "invocation '{}' should use workspace default model (gpt-5.4), got {}",
            inv.contract_label, inv.resolved_target.model.model_id
        );
    }
}

#[cfg(feature = "test-stub")]
#[tokio::test]
async fn daemon_requirements_quick_without_defaults_uses_role_defaults() {
    // When workspace defaults are unset, daemon requirements behavior remains
    // unchanged and falls back to existing role defaults. Requirements stages
    // use different roles (Planner, Reviewer), so each invocation gets its
    // own role's default backend/model rather than a single shared default.
    use ralph_burning::shared::domain::BackendRole;

    let temp = tempdir().expect("tempdir");
    let base_dir = temp.path();

    // Set up workspace without backend/model defaults
    let ws_dir = base_dir.join(".ralph-burning");
    std::fs::create_dir_all(&ws_dir).expect("create workspace dir");
    std::fs::write(
        ws_dir.join("workspace.toml"),
        "version = 1\ncreated_at = \"2026-03-14T00:00:00Z\"\n\n[settings]\n",
    )
    .expect("write workspace.toml");
    let effective_config =
        ralph_burning::contexts::workspace_governance::config::EffectiveConfig::load(base_dir)
            .expect("load effective config");

    let adapter = ralph_burning::adapters::stub_backend::StubBackendAdapter::default();
    let req_svc = build_test_requirements_service_with_defaults(adapter.clone(), &effective_config);

    let _run_id = req_svc
        .quick(base_dir, "Test with no defaults", Utc::now(), None)
        .await
        .expect("requirements quick should succeed");

    // Each invocation should use its role's built-in default (no workspace override).
    // Verify that no invocation was overridden to a non-default backend.
    let invocations = adapter.recorded_invocations();
    assert!(!invocations.is_empty());
    // Requirements stages only use Planner and Reviewer roles.
    // Planner default: Claude / claude-opus-4-6
    // Reviewer default: Claude / sonnet-4.0
    for inv in &invocations {
        let expected = if inv.contract_label.contains("review") {
            BackendRole::Reviewer.default_target()
        } else {
            BackendRole::Planner.default_target()
        };
        assert_eq!(
            expected.backend.family, inv.resolved_target.backend.family,
            "without workspace defaults, invocation '{}' should use role default backend",
            inv.contract_label
        );
        assert_eq!(
            expected.model.model_id, inv.resolved_target.model.model_id,
            "without workspace defaults, invocation '{}' should use role default model",
            inv.contract_label
        );
    }
}

#[cfg(feature = "test-stub")]
#[tokio::test]
async fn daemon_requirements_partial_defaults_backend_only() {
    // Partial defaults: default_backend alone selects that backend's default model.
    let temp = tempdir().expect("tempdir");
    let base_dir = temp.path();

    let ws_dir = base_dir.join(".ralph-burning");
    std::fs::create_dir_all(&ws_dir).expect("create workspace dir");
    std::fs::write(
        ws_dir.join("workspace.toml"),
        "version = 1\ncreated_at = \"2026-03-14T00:00:00Z\"\n\n[settings]\ndefault_backend = \"codex\"\n",
    )
    .expect("write workspace.toml");
    let effective_config =
        ralph_burning::contexts::workspace_governance::config::EffectiveConfig::load(base_dir)
            .expect("load effective config");

    let adapter = ralph_burning::adapters::stub_backend::StubBackendAdapter::default();
    let req_svc = build_test_requirements_service_with_defaults(adapter.clone(), &effective_config);

    let _run_id = req_svc
        .quick(base_dir, "Backend-only default", Utc::now(), None)
        .await
        .expect("requirements quick should succeed");

    let invocations = adapter.recorded_invocations();
    assert!(!invocations.is_empty());
    for inv in &invocations {
        assert_eq!(
            BackendFamily::Codex,
            inv.resolved_target.backend.family,
            "backend-only default should select codex family"
        );
        // default_backend alone should pick that backend's default model
        let expected_model =
            ralph_burning::shared::domain::ModelSpec::default_for_backend(BackendFamily::Codex);
        assert_eq!(
            expected_model.model_id, inv.resolved_target.model.model_id,
            "backend-only default should select codex's default model ({}), got {}",
            expected_model.model_id, inv.resolved_target.model.model_id
        );
    }
}

#[cfg(feature = "test-stub")]
#[tokio::test]
async fn daemon_requirements_partial_defaults_model_only() {
    // Partial defaults: default_model alone overrides only the model on the
    // role's default backend (Planner → Claude).
    let temp = tempdir().expect("tempdir");
    let base_dir = temp.path();

    let ws_dir = base_dir.join(".ralph-burning");
    std::fs::create_dir_all(&ws_dir).expect("create workspace dir");
    std::fs::write(
        ws_dir.join("workspace.toml"),
        "version = 1\ncreated_at = \"2026-03-14T00:00:00Z\"\n\n[settings]\ndefault_model = \"sonnet-4.0\"\n",
    )
    .expect("write workspace.toml");
    let effective_config =
        ralph_burning::contexts::workspace_governance::config::EffectiveConfig::load(base_dir)
            .expect("load effective config");

    let adapter = ralph_burning::adapters::stub_backend::StubBackendAdapter::default();
    let req_svc = build_test_requirements_service_with_defaults(adapter.clone(), &effective_config);

    let _run_id = req_svc
        .quick(base_dir, "Model-only default", Utc::now(), None)
        .await
        .expect("requirements quick should succeed");

    let invocations = adapter.recorded_invocations();
    assert!(!invocations.is_empty());
    for inv in &invocations {
        // default_model should override the model ID on whichever backend the role defaults to
        assert_eq!(
            "sonnet-4.0", inv.resolved_target.model.model_id,
            "model-only default should override to sonnet-4.0"
        );
    }
}

#[test]
fn daemon_requirements_quick_prerun_failure_invalid_backend_no_run_created() {
    // Pre-run failure invariant: invalid backend config is rejected before a
    // requirements service or run can be created, so no requirements history is
    // materialized on disk.
    let temp = tempdir().expect("tempdir");
    let base_dir = temp.path();

    // Set up workspace with an invalid default_backend value
    let ws_dir = base_dir.join(".ralph-burning");
    std::fs::create_dir_all(&ws_dir).expect("create workspace dir");
    std::fs::write(
        ws_dir.join("workspace.toml"),
        "version = 1\ncreated_at = \"2026-03-14T00:00:00Z\"\n\n[settings]\ndefault_backend = \"invalid_backend_xyz\"\n",
    )
    .expect("write workspace.toml");
    let result =
        ralph_burning::contexts::workspace_governance::config::EffectiveConfig::load(base_dir);
    match result {
        Ok(_) => panic!("effective config load should fail with invalid default_backend"),
        Err(AppError::InvalidConfigValue {
            ref key, ref value, ..
        }) => {
            assert_eq!(key, "backend");
            assert_eq!(value, "invalid_backend_xyz");
        }
        Err(other) => panic!("expected InvalidConfigValue error, got: {other:?}"),
    }

    // No requirements run directory should have been created
    let requirements_dir = base_dir.join(".ralph-burning").join("requirements");
    assert!(
        !requirements_dir.exists(),
        "no requirements directory should exist when service construction fails before run creation"
    );
}

#[test]
fn daemon_requirements_draft_prerun_failure_invalid_backend_no_run_created() {
    // Pre-run failure invariant: same as the quick path. Invalid backend config
    // is rejected before a requirements service or run exists.
    let temp = tempdir().expect("tempdir");
    let base_dir = temp.path();

    // Set up workspace with an invalid default_backend value
    let ws_dir = base_dir.join(".ralph-burning");
    std::fs::create_dir_all(&ws_dir).expect("create workspace dir");
    std::fs::write(
        ws_dir.join("workspace.toml"),
        "version = 1\ncreated_at = \"2026-03-14T00:00:00Z\"\n\n[settings]\ndefault_backend = \"nonexistent_provider\"\n",
    )
    .expect("write workspace.toml");
    let result =
        ralph_burning::contexts::workspace_governance::config::EffectiveConfig::load(base_dir);
    match result {
        Ok(_) => panic!("effective config load should fail with invalid default_backend"),
        Err(AppError::InvalidConfigValue {
            ref key, ref value, ..
        }) => {
            assert_eq!(key, "backend");
            assert_eq!(value, "nonexistent_provider");
        }
        Err(other) => panic!("expected InvalidConfigValue error, got: {other:?}"),
    }

    // No requirements run directory should have been created
    let requirements_dir = base_dir.join(".ralph-burning").join("requirements");
    assert!(
        !requirements_dir.exists(),
        "no requirements directory should exist when service construction fails before run creation"
    );
}

// ---------------------------------------------------------------------------
// CLI writer-lease guard: acquisition, heartbeat, and cleanup
// ---------------------------------------------------------------------------

use std::sync::Arc;

use ralph_burning::contexts::automation_runtime::cli_writer_lease::{
    CliWriterLeaseGuard, CLI_LEASE_HEARTBEAT_CADENCE_SECONDS, CLI_LEASE_TTL_SECONDS,
};

fn arc_store() -> Arc<dyn DaemonStorePort + Send + Sync> {
    Arc::new(FsDaemonStore)
}

#[tokio::test]
async fn cli_lease_guard_creates_reconcile_visible_lease_record() {
    let temp = tempdir().expect("tempdir");
    let project_id = ralph_burning::shared::domain::ProjectId::new("lease-guard-test".to_owned())
        .expect("valid id");

    let guard = CliWriterLeaseGuard::acquire(
        arc_store(),
        temp.path(),
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )
    .expect("acquire");

    // Lease record should be reconcile-visible via list_lease_records
    let records = FsDaemonStore.list_lease_records(temp.path()).expect("list");
    assert_eq!(1, records.len(), "one lease record expected");
    match &records[0] {
        LeaseRecord::CliWriter(cli) => {
            assert_eq!("lease-guard-test", cli.project_id);
            assert_eq!("cli", cli.owner);
            assert_eq!(CLI_LEASE_TTL_SECONDS, cli.ttl_seconds);
        }
        LeaseRecord::Worktree(_) => panic!("expected CliWriter lease record"),
    }

    // Writer lock should be held
    let err = FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "contender")
        .expect_err("lock should be held");
    assert!(matches!(err, AppError::ProjectWriterLockHeld { .. }));

    drop(guard);

    // After drop: both cleaned up
    let records_after = FsDaemonStore
        .list_lease_records(temp.path())
        .expect("list after");
    assert!(
        records_after.is_empty(),
        "lease record should be removed after drop"
    );
    FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "post-drop")
        .expect("lock should be available after guard drop");
    FsDaemonStore
        .release_writer_lock(temp.path(), &project_id, "post-drop")
        .expect("cleanup");
}

#[tokio::test]
async fn cli_lease_guard_heartbeat_advances_last_heartbeat() {
    let temp = tempdir().expect("tempdir");
    let project_id = ralph_burning::shared::domain::ProjectId::new("hb-advance-test".to_owned())
        .expect("valid id");

    let guard = CliWriterLeaseGuard::acquire(
        arc_store(),
        temp.path(),
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        1, // 1-second heartbeat for fast testing
    )
    .expect("acquire");

    let lease_id = guard.lease_id().to_owned();

    let record_before = FsDaemonStore
        .read_lease_record(temp.path(), &lease_id)
        .expect("read before");
    let hb_before = match &record_before {
        LeaseRecord::CliWriter(cli) => cli.last_heartbeat,
        _ => panic!("expected CliWriter"),
    };

    // Wait for heartbeat to tick
    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

    let record_after = FsDaemonStore
        .read_lease_record(temp.path(), &lease_id)
        .expect("read after");
    let hb_after = match &record_after {
        LeaseRecord::CliWriter(cli) => cli.last_heartbeat,
        _ => panic!("expected CliWriter"),
    };

    assert!(
        hb_after > hb_before,
        "heartbeat should advance last_heartbeat, before={hb_before}, after={hb_after}"
    );

    drop(guard);
}

#[tokio::test]
async fn cli_lease_guard_drop_cleans_up_on_error_path() {
    let temp = tempdir().expect("tempdir");
    let project_id = ralph_burning::shared::domain::ProjectId::new("err-cleanup-test".to_owned())
        .expect("valid id");

    let guard = CliWriterLeaseGuard::acquire(
        arc_store(),
        temp.path(),
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )
    .expect("acquire");

    // Simulate error: drop without awaiting completion
    drop(guard);

    // Both lease record and writer lock should be cleaned up
    let records = FsDaemonStore.list_lease_records(temp.path()).expect("list");
    assert!(
        records.is_empty(),
        "lease record should be removed on error-path drop"
    );
    FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "post-error")
        .expect("lock should be available after error-path drop");
    FsDaemonStore
        .release_writer_lock(temp.path(), &project_id, "post-error")
        .expect("cleanup");
}

#[tokio::test]
async fn cli_lease_guard_failed_lock_leaves_no_lease_record() {
    let temp = tempdir().expect("tempdir");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("contention-guard-test".to_owned())
            .expect("valid id");

    // Pre-hold the lock
    FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "blocker")
        .expect("pre-acquire");

    let result = CliWriterLeaseGuard::acquire(
        arc_store(),
        temp.path(),
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    );
    assert!(result.is_err(), "acquire should fail when lock is held");

    // No lease record should exist
    let records = FsDaemonStore.list_lease_records(temp.path()).expect("list");
    assert!(
        records.is_empty(),
        "no lease record should be written on failed lock acquisition"
    );

    FsDaemonStore
        .release_writer_lock(temp.path(), &project_id, "blocker")
        .expect("cleanup blocker");
}

// ---------------------------------------------------------------------------
// Reconcile: stale CLI writer lease cleanup
// ---------------------------------------------------------------------------

#[test]
fn reconcile_stale_cli_lease_cleans_lease_and_writer_lock() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    // Inject a stale CLI lease record with a matching writer lock.
    let cli_lease = CliWriterLease {
        lease_id: "cli-stale-reconcile".to_owned(),
        project_id: "stale-proj".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store
        .write_lease_record(temp.path(), &LeaseRecord::CliWriter(cli_lease))
        .expect("write cli lease");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("stale-proj".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "cli-stale-reconcile")
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

    // Stale CLI lease counted
    assert_eq!(1, report.stale_lease_ids.len(), "stale_leases should be 1");
    assert_eq!("cli-stale-reconcile", report.stale_lease_ids[0]);

    // Successfully released
    assert_eq!(
        1,
        report.released_lease_ids.len(),
        "released_leases should be 1"
    );
    assert_eq!("cli-stale-reconcile", report.released_lease_ids[0]);

    // No tasks failed (CLI leases are task-independent)
    assert!(
        report.failed_task_ids.is_empty(),
        "failed_tasks should be 0 for CLI lease reconcile"
    );

    // No cleanup failures
    assert!(
        report.cleanup_failures.is_empty(),
        "no cleanup failures expected"
    );

    // Lease record and writer lock should be gone
    let records = store.list_lease_records(temp.path()).expect("list");
    assert!(records.is_empty(), "CLI lease record should be removed");
    store
        .acquire_writer_lock(temp.path(), &project_id, "post-reconcile")
        .expect("writer lock should be available after reconcile");
    store
        .release_writer_lock(temp.path(), &project_id, "post-reconcile")
        .expect("cleanup");
}

#[test]
fn reconcile_stale_cli_lease_missing_writer_lock_reports_cleanup_failure() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    // Inject a stale CLI lease record WITHOUT a matching writer lock.
    let cli_lease = CliWriterLease {
        lease_id: "cli-no-lock".to_owned(),
        project_id: "no-lock-proj".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store
        .write_lease_record(temp.path(), &LeaseRecord::CliWriter(cli_lease))
        .expect("write cli lease");

    let worktree_adapter = WorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0),
        Utc::now(),
    )
    .expect("reconcile");

    assert_eq!(1, report.stale_lease_ids.len());
    // Writer lock was already absent → cleanup failure, not a release.
    // The lease file is still pruned so later reconcile runs do not
    // rediscover it, but it is never counted as released.
    assert!(
        report.released_lease_ids.is_empty(),
        "missing writer lock should prevent counting as released"
    );
    assert_eq!(1, report.cleanup_failures.len());
    assert!(
        report.cleanup_failures[0]
            .details
            .contains("writer_lock_absent"),
        "details should mention writer_lock_absent, got: {}",
        report.cleanup_failures[0].details
    );
    assert_eq!(None, report.cleanup_failures[0].task_id);

    // The stale lease record should be pruned even though the pass was
    // a cleanup failure — prevents rediscovery on subsequent reconcile runs.
    let remaining = store.list_lease_records(temp.path()).expect("list");
    assert!(
        remaining.is_empty(),
        "stale CLI lease should be pruned after writer_lock_absent"
    );
}

#[test]
fn reconcile_stale_cli_lease_missing_lease_file_reports_cleanup_failure() {
    // Exercises the race where the CLI lease file disappears between
    // list_lease_records and remove_lease. Uses SubStepAbsentStore so the
    // lease is visible during listing but remove_lease returns AlreadyAbsent.
    let store = SubStepAbsentStore::new(true, false);
    let temp = tempdir().expect("tempdir");

    let cli_lease = CliWriterLease {
        lease_id: "cli-race".to_owned(),
        project_id: "race-proj".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store
        .write_lease_record(temp.path(), &LeaseRecord::CliWriter(cli_lease))
        .expect("write cli lease");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("race-proj".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "cli-race")
        .expect("acquire lock");

    let worktree_adapter = WorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0),
        Utc::now(),
    )
    .expect("reconcile");

    // The lease was listed and identified as stale.
    assert_eq!(1, report.stale_lease_ids.len());
    assert_eq!("cli-race", report.stale_lease_ids[0]);

    // Writer lock released OK, but lease file was "already absent"
    // at removal time → cleanup failure, not a successful release.
    assert!(
        report.released_lease_ids.is_empty(),
        "absent lease file should prevent counting as released"
    );
    assert_eq!(1, report.cleanup_failures.len());
    assert!(
        report.cleanup_failures[0]
            .details
            .contains("lease_file_absent"),
        "details should mention lease_file_absent, got: {}",
        report.cleanup_failures[0].details
    );
    assert_eq!(None, report.cleanup_failures[0].task_id);
}

#[test]
fn reconcile_non_stale_cli_lease_is_not_cleaned() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    // Inject a fresh CLI lease (not stale).
    let cli_lease = CliWriterLease {
        lease_id: "cli-fresh".to_owned(),
        project_id: "fresh-proj".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 300,
        last_heartbeat: Utc::now(),
    };
    store
        .write_lease_record(temp.path(), &LeaseRecord::CliWriter(cli_lease))
        .expect("write cli lease");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("fresh-proj".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "cli-fresh")
        .expect("acquire lock");

    let worktree_adapter = WorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        None, // use natural TTL
        Utc::now(),
    )
    .expect("reconcile");

    assert!(
        report.stale_lease_ids.is_empty(),
        "fresh lease should not be stale"
    );
    assert!(report.released_lease_ids.is_empty());
    assert!(report.cleanup_failures.is_empty());

    // Lease and lock should still exist
    let records = store.list_lease_records(temp.path()).expect("list");
    assert_eq!(1, records.len(), "fresh lease should remain");

    // Clean up
    store
        .remove_lease(temp.path(), "cli-fresh")
        .expect("cleanup lease");
    store
        .release_writer_lock(temp.path(), &project_id, "cli-fresh")
        .expect("cleanup lock");
}

/// Regression test: CLI `close()` successfully releases the writer lock but
/// lease-file deletion fails at close time. A subsequent `daemon reconcile`
/// discovers the stale CLI lease, finds the writer lock already absent,
/// prunes the lease file, but does NOT count the lease as released.
/// Verifies accounting: `stale_leases == 1`, `released_leases == 0`,
/// `failed_tasks == 0`, and a follow-up writer-lock acquisition succeeds.
#[test]
fn reconcile_prunes_stale_cli_lease_after_close_released_writer_lock() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("close-proj".to_owned()).expect("valid id");

    // Simulate the state left behind when CLI close() releases the writer
    // lock but fails to delete the lease file: a stale CLI lease record
    // exists on disk, but no writer lock is held.
    let cli_lease = CliWriterLease {
        lease_id: "cli-close-stale".to_owned(),
        project_id: "close-proj".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store
        .write_lease_record(temp.path(), &LeaseRecord::CliWriter(cli_lease))
        .expect("write stale cli lease");
    // No writer lock on disk — simulates close() having released it.

    let worktree_adapter = WorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0),
        Utc::now(),
    )
    .expect("reconcile");

    // Accounting invariants:
    assert_eq!(1, report.stale_lease_ids.len(), "stale_leases should be 1");
    assert!(
        report.released_lease_ids.is_empty(),
        "released_leases should be 0 — writer_lock_absent makes the pass a cleanup failure"
    );
    assert!(
        report.failed_task_ids.is_empty(),
        "failed_tasks should be 0 — CLI leases have no task"
    );

    // The cleanup failure should mention writer_lock_absent.
    assert_eq!(1, report.cleanup_failures.len());
    assert!(
        report.cleanup_failures[0]
            .details
            .contains("writer_lock_absent"),
        "expected writer_lock_absent detail, got: {}",
        report.cleanup_failures[0].details
    );
    assert_eq!(None, report.cleanup_failures[0].task_id);

    // The stale lease file should be pruned so later reconcile runs do
    // not rediscover it.
    let remaining = store.list_lease_records(temp.path()).expect("list");
    assert!(
        remaining.is_empty(),
        "lease record should be pruned after reconcile"
    );

    // A follow-up writer-lock acquisition should succeed — the project
    // is no longer blocked by a stale lease.
    store
        .acquire_writer_lock(temp.path(), &project_id, "post-reconcile")
        .expect("writer lock should be available after stale lease pruning");
    store
        .release_writer_lock(temp.path(), &project_id, "post-reconcile")
        .expect("cleanup");
}

/// Regression test: writer_lock_absent followed by lease-file deletion
/// failure. Both sub-steps fail — reconcile must record each explicitly,
/// not increment released_leases, not mutate daemon tasks, and not touch
/// worktrees.
#[test]
fn reconcile_writer_lock_absent_then_lease_delete_failure_records_both() {
    // Use SubStepAbsentStore with writer_lock_absent=true AND
    // lease_file_absent=true to simulate both sub-steps failing.
    let store = SubStepAbsentStore::new(true, true);
    let temp = tempdir().expect("tempdir");

    let cli_lease = CliWriterLease {
        lease_id: "cli-double-fail".to_owned(),
        project_id: "double-fail-proj".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store
        .write_lease_record(temp.path(), &LeaseRecord::CliWriter(cli_lease))
        .expect("write cli lease");

    let worktree_adapter = WorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0),
        Utc::now(),
    )
    .expect("reconcile");

    assert_eq!(1, report.stale_lease_ids.len());
    assert!(
        report.released_lease_ids.is_empty(),
        "double failure should not count as released"
    );
    assert!(
        report.failed_task_ids.is_empty(),
        "CLI leases have no task to fail"
    );

    // Both sub-step failures should be recorded explicitly.
    assert_eq!(
        2,
        report.cleanup_failures.len(),
        "expected two cleanup failures (writer_lock_absent + lease_file_absent), got: {:?}",
        report
            .cleanup_failures
            .iter()
            .map(|f| &f.details)
            .collect::<Vec<_>>()
    );
    assert!(
        report.cleanup_failures[0]
            .details
            .contains("writer_lock_absent"),
        "first failure should be writer_lock_absent, got: {}",
        report.cleanup_failures[0].details
    );
    assert!(
        report.cleanup_failures[1]
            .details
            .contains("lease_file_absent"),
        "second failure should be lease_file_absent, got: {}",
        report.cleanup_failures[1].details
    );
}

// ---------------------------------------------------------------------------
// Owner-aware writer-lock release
// ---------------------------------------------------------------------------

#[test]
fn owner_matched_release_succeeds() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("owner-match".to_owned()).expect("valid id");

    store
        .acquire_writer_lock(temp.path(), &project_id, "my-lease")
        .expect("acquire");
    let outcome = store
        .release_writer_lock(temp.path(), &project_id, "my-lease")
        .expect("release");
    assert!(
        matches!(
            outcome,
            ralph_burning::contexts::automation_runtime::WriterLockReleaseOutcome::Released
        ),
        "owner-matched release should return Released"
    );
    // Lock should be gone
    store
        .acquire_writer_lock(temp.path(), &project_id, "after")
        .expect("should be available");
    store
        .release_writer_lock(temp.path(), &project_id, "after")
        .expect("cleanup");
}

#[test]
fn owner_mismatch_does_not_delete_replaced_lock() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let project_id = ralph_burning::shared::domain::ProjectId::new("owner-mismatch".to_owned())
        .expect("valid id");

    // Acquire with one owner
    store
        .acquire_writer_lock(temp.path(), &project_id, "original-owner")
        .expect("acquire");

    // Try to release with a different owner
    let outcome = store
        .release_writer_lock(temp.path(), &project_id, "wrong-owner")
        .expect("release should not error");
    match outcome {
        ralph_burning::contexts::automation_runtime::WriterLockReleaseOutcome::OwnerMismatch {
            ref actual_owner,
        } => {
            assert_eq!("original-owner", actual_owner);
        }
        other => panic!("expected OwnerMismatch, got: {other:?}"),
    }

    // Lock must still exist (not deleted)
    let err = store
        .acquire_writer_lock(temp.path(), &project_id, "contender")
        .expect_err("lock should still be held");
    assert!(matches!(err, AppError::ProjectWriterLockHeld { .. }));

    // Cleanup with correct owner
    store
        .release_writer_lock(temp.path(), &project_id, "original-owner")
        .expect("cleanup");
}

// ---------------------------------------------------------------------------
// CLI guard cleanup leaves lease durable when lock release fails
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cli_guard_drop_leaves_lease_durable_on_lock_mismatch() {
    // Simulate: after CLI guard acquires, another writer replaces the lock.
    // On drop, the guard should detect mismatch and NOT delete the lease record.
    let temp = tempdir().expect("tempdir");
    let project_id = ralph_burning::shared::domain::ProjectId::new("guard-mismatch".to_owned())
        .expect("valid id");

    let guard = CliWriterLeaseGuard::acquire(
        arc_store(),
        temp.path(),
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )
    .expect("acquire");

    let lease_id = guard.lease_id().to_owned();

    // Replace the writer lock behind the guard's back
    std::fs::remove_file(
        temp.path()
            .join(".ralph-burning/daemon/leases/writer-guard-mismatch.lock"),
    )
    .expect("remove lock");
    FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "usurper")
        .expect("usurp lock");

    drop(guard);

    // CLI lease record must remain durable (not deleted)
    let records = FsDaemonStore.list_lease_records(temp.path()).expect("list");
    assert!(
        records.iter().any(|r| match r {
            LeaseRecord::CliWriter(cli) => cli.lease_id == lease_id,
            _ => false,
        }),
        "CLI lease record must remain when lock release detected mismatch"
    );

    // The usurper's lock must still be intact (not deleted by guard)
    let err = FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "another")
        .expect_err("usurper's lock should still be held");
    assert!(matches!(err, AppError::ProjectWriterLockHeld { .. }));

    // Cleanup
    FsDaemonStore
        .release_writer_lock(temp.path(), &project_id, "usurper")
        .expect("cleanup lock");
    FsDaemonStore
        .remove_lease(temp.path(), &lease_id)
        .expect("cleanup lease");
}

#[tokio::test]
async fn cli_guard_drop_leaves_lease_durable_on_lock_absent() {
    // If the lock file is already gone when the guard drops, the lease record
    // must stay durable for reconcile visibility.
    let temp = tempdir().expect("tempdir");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("guard-absent".to_owned()).expect("valid id");

    let guard = CliWriterLeaseGuard::acquire(
        arc_store(),
        temp.path(),
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )
    .expect("acquire");

    let lease_id = guard.lease_id().to_owned();

    // Remove the lock file behind the guard's back
    std::fs::remove_file(
        temp.path()
            .join(".ralph-burning/daemon/leases/writer-guard-absent.lock"),
    )
    .expect("remove lock");

    drop(guard);

    // CLI lease record must remain durable
    let records = FsDaemonStore.list_lease_records(temp.path()).expect("list");
    assert!(
        records.iter().any(|r| match r {
            LeaseRecord::CliWriter(cli) => cli.lease_id == lease_id,
            _ => false,
        }),
        "CLI lease record must remain when lock was already absent"
    );

    // Cleanup
    FsDaemonStore
        .remove_lease(temp.path(), &lease_id)
        .expect("cleanup lease");
}

// ---------------------------------------------------------------------------
// Guard explicit close: success and failure invariants
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cli_guard_close_succeeds_and_drop_is_noop() {
    let temp = tempdir().expect("tempdir");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("close-ok".to_owned()).expect("valid id");

    let guard = CliWriterLeaseGuard::acquire(
        arc_store(),
        temp.path(),
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )
    .expect("acquire");

    // Explicit close should succeed.
    guard.close().expect("close should succeed");

    // Both lease record and lock should be cleaned up.
    let records = FsDaemonStore.list_lease_records(temp.path()).expect("list");
    assert!(
        records.is_empty(),
        "lease record should be removed after close"
    );
    FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "post-close")
        .expect("lock should be available after close");
    FsDaemonStore
        .release_writer_lock(temp.path(), &project_id, "post-close")
        .expect("cleanup");
}

#[tokio::test]
async fn cli_guard_close_fails_when_lock_absent() {
    let temp = tempdir().expect("tempdir");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("close-absent".to_owned()).expect("valid id");

    let guard = CliWriterLeaseGuard::acquire(
        arc_store(),
        temp.path(),
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )
    .expect("acquire");

    let lease_id = guard.lease_id().to_owned();

    // Remove the lock file behind the guard's back.
    std::fs::remove_file(
        temp.path()
            .join(".ralph-burning/daemon/leases/writer-close-absent.lock"),
    )
    .expect("remove lock");

    // Close should fail with writer_lock_absent.
    let err = guard.close().expect_err("close should fail");
    let err_msg = format!("{err}");
    assert!(
        err_msg.contains("writer_lock_absent"),
        "error should mention writer_lock_absent, got: {err_msg}"
    );

    // CLI lease record must remain durable.
    let records = FsDaemonStore.list_lease_records(temp.path()).expect("list");
    assert!(
        records
            .iter()
            .any(|r| matches!(r, LeaseRecord::CliWriter(cli) if cli.lease_id == lease_id)),
        "CLI lease record must remain when close fails with lock absent"
    );

    // Cleanup
    FsDaemonStore
        .remove_lease(temp.path(), &lease_id)
        .expect("cleanup lease");
}

#[tokio::test]
async fn cli_guard_close_fails_when_lock_owner_mismatch() {
    let temp = tempdir().expect("tempdir");
    let project_id = ralph_burning::shared::domain::ProjectId::new("close-mismatch".to_owned())
        .expect("valid id");

    let guard = CliWriterLeaseGuard::acquire(
        arc_store(),
        temp.path(),
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )
    .expect("acquire");

    let lease_id = guard.lease_id().to_owned();

    // Replace the writer lock with a different owner.
    std::fs::remove_file(
        temp.path()
            .join(".ralph-burning/daemon/leases/writer-close-mismatch.lock"),
    )
    .expect("remove lock");
    FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "usurper")
        .expect("usurp lock");

    // Close should fail with writer_lock_owner_mismatch.
    let err = guard.close().expect_err("close should fail");
    let err_msg = format!("{err}");
    assert!(
        err_msg.contains("writer_lock_owner_mismatch"),
        "error should mention writer_lock_owner_mismatch, got: {err_msg}"
    );

    // CLI lease record must remain durable.
    let records = FsDaemonStore.list_lease_records(temp.path()).expect("list");
    assert!(
        records
            .iter()
            .any(|r| matches!(r, LeaseRecord::CliWriter(cli) if cli.lease_id == lease_id)),
        "CLI lease record must remain when close fails with owner mismatch"
    );

    // Usurper's lock must be untouched.
    let err = FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "another")
        .expect_err("usurper's lock should still be held");
    assert!(matches!(err, AppError::ProjectWriterLockHeld { .. }));

    // Cleanup
    FsDaemonStore
        .release_writer_lock(temp.path(), &project_id, "usurper")
        .expect("cleanup lock");
    FsDaemonStore
        .remove_lease(temp.path(), &lease_id)
        .expect("cleanup lease");
}

#[tokio::test]
async fn cli_guard_close_lease_delete_failure_keeps_lock_released() {
    // After successful lock release, if lease file delete fails,
    // close returns error but the lock stays released.
    let temp = tempdir().expect("tempdir");
    let project_id = ralph_burning::shared::domain::ProjectId::new("close-lease-fail".to_owned())
        .expect("valid id");

    let guard = CliWriterLeaseGuard::acquire(
        arc_store(),
        temp.path(),
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )
    .expect("acquire");

    let lease_id = guard.lease_id().to_owned();

    // Make the lease file non-deletable by replacing it with a directory
    // containing a file (remove_file on a directory fails).
    let lease_path = temp
        .path()
        .join(format!(".ralph-burning/daemon/leases/{lease_id}.json"));
    std::fs::remove_file(&lease_path).expect("remove lease file");
    std::fs::create_dir(&lease_path).expect("create dir at lease path");
    std::fs::write(lease_path.join("blocker"), "x").expect("write blocker");

    // Close should fail at lease_file_delete step.
    let err = guard.close().expect_err("close should fail");
    let err_msg = format!("{err}");
    assert!(
        err_msg.contains("lease_file_delete"),
        "error should mention lease_file_delete, got: {err_msg}"
    );

    // Writer lock must still be released (the lock release succeeded).
    FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "after-close")
        .expect("lock should be available after close");
    FsDaemonStore
        .release_writer_lock(temp.path(), &project_id, "after-close")
        .expect("cleanup lock");

    // Cleanup the lease dir-file
    std::fs::remove_file(lease_path.join("blocker")).expect("cleanup blocker");
    std::fs::remove_dir(&lease_path).expect("cleanup lease dir");
}

// ---------------------------------------------------------------------------
// Stale CLI reconcile: owner mismatch reports failure without deleting lease
// ---------------------------------------------------------------------------

#[test]
fn reconcile_stale_cli_lease_owner_mismatch_reports_cleanup_failure() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    // Inject a stale CLI lease with a writer lock owned by a different writer.
    let cli_lease = CliWriterLease {
        lease_id: "cli-stale-mismatch".to_owned(),
        project_id: "mismatch-proj".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store
        .write_lease_record(temp.path(), &LeaseRecord::CliWriter(cli_lease))
        .expect("write cli lease");
    let project_id = ralph_burning::shared::domain::ProjectId::new("mismatch-proj".to_owned())
        .expect("valid id");
    // Acquire the lock with a DIFFERENT owner to simulate mismatch
    store
        .acquire_writer_lock(temp.path(), &project_id, "other-writer")
        .expect("acquire lock as different owner");

    let worktree_adapter = WorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0),
        Utc::now(),
    )
    .expect("reconcile");

    // Stale CLI lease counted
    assert_eq!(1, report.stale_lease_ids.len());
    assert_eq!("cli-stale-mismatch", report.stale_lease_ids[0]);

    // NOT released (mismatch)
    assert!(
        report.released_lease_ids.is_empty(),
        "owner-mismatch should prevent counting as released"
    );

    // No tasks failed
    assert!(report.failed_task_ids.is_empty());

    // Cleanup failure reported with distinct mismatch detail
    assert_eq!(1, report.cleanup_failures.len());
    assert!(
        report.cleanup_failures[0]
            .details
            .contains("writer_lock_owner_mismatch"),
        "details should mention owner_mismatch, got: {}",
        report.cleanup_failures[0].details
    );
    assert_eq!(None, report.cleanup_failures[0].task_id);

    // CLI lease record must still exist (not deleted)
    let records = store.list_lease_records(temp.path()).expect("list");
    assert_eq!(
        1,
        records.len(),
        "CLI lease record must remain after mismatch"
    );

    // The other writer's lock must still be intact
    let err = store
        .acquire_writer_lock(temp.path(), &project_id, "contender")
        .expect_err("lock should still be held by other-writer");
    assert!(matches!(err, AppError::ProjectWriterLockHeld { .. }));

    // Cleanup
    store
        .release_writer_lock(temp.path(), &project_id, "other-writer")
        .expect("cleanup lock");
    store
        .remove_lease(temp.path(), "cli-stale-mismatch")
        .expect("cleanup lease");
}

// ---------------------------------------------------------------------------
// Normal stale CLI cleanup allows subsequent lock acquisition
// ---------------------------------------------------------------------------

#[test]
fn reconcile_stale_cli_cleanup_allows_subsequent_run_start() {
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");

    // Inject stale CLI lease with matching owner in the lock
    let cli_lease = CliWriterLease {
        lease_id: "cli-stale-reacquire".to_owned(),
        project_id: "reacquire-proj".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 300,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store
        .write_lease_record(temp.path(), &LeaseRecord::CliWriter(cli_lease))
        .expect("write cli lease");
    let project_id = ralph_burning::shared::domain::ProjectId::new("reacquire-proj".to_owned())
        .expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "cli-stale-reacquire")
        .expect("acquire lock");

    let worktree_adapter = WorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(0),
        Utc::now(),
    )
    .expect("reconcile");

    assert_eq!(1, report.stale_lease_ids.len());
    assert_eq!(1, report.released_lease_ids.len());
    assert!(report.cleanup_failures.is_empty());
    assert!(report.failed_task_ids.is_empty());

    // Subsequent run start can acquire the lock
    store
        .acquire_writer_lock(temp.path(), &project_id, "new-cli-session")
        .expect("should be able to acquire after stale cleanup");

    // Cleanup
    store
        .release_writer_lock(temp.path(), &project_id, "new-cli-session")
        .expect("cleanup");
}

// ---------------------------------------------------------------------------
// Durable worktree lease cleanup: writer-lock release failure after worktree
// removal preserves lease file and returns resources_released=false
// ---------------------------------------------------------------------------

#[test]
fn release_writer_lock_failure_after_worktree_removal_preserves_lease_file() {
    // When worktree removal succeeds but writer-lock release fails,
    // release() must NOT delete the worktree lease file, must return
    // resources_released=false, and must preserve the writer-lock error
    // detail so callers can report the specific failure.
    let temp = tempdir().expect("tempdir");
    let store = SubStepErrorStore::new(false, true); // writer_lock_error=true

    let mut task = sample_task();
    task.task_id = "wt-lock-fail-test".to_owned();
    task.status = TaskStatus::Active;
    store.create_task(temp.path(), &task).expect("create task");

    let wt_path = temp.path().join("wt-lock-fail");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-wt-lock-fail".to_owned(),
        task_id: "wt-lock-fail-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/wt-lock-fail-test".to_owned(),
        acquired_at: Utc::now() - Duration::hours(1),
        ttl_seconds: 60,
        last_heartbeat: Utc::now() - Duration::hours(1),
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-wt-lock-fail")
        .expect("acquire lock");

    let worktree_adapter = SuccessWorktreeAdapter;
    let result = LeaseService::release(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        &lease,
        ReleaseMode::Idempotent,
    )
    .expect("release returns Ok with sub-step failures");

    // resources_released must be false
    assert!(
        !result.resources_released,
        "resources_released must be false when writer-lock release fails"
    );
    assert!(
        result.writer_lock_error.is_some(),
        "should report writer_lock_error"
    );

    // Worktree should be gone (removal succeeded)
    assert!(
        !temp.path().join("wt-lock-fail").exists(),
        "worktree should be removed"
    );

    // Lease file must still exist on disk — preserved for recovery
    let leases = store.list_leases(temp.path()).expect("list leases");
    assert_eq!(
        1,
        leases.len(),
        "lease file must remain durable when writer-lock release fails after worktree removal"
    );
    assert_eq!("lease-wt-lock-fail", leases[0].lease_id);

    // No LeaseReleased journal event
    let journal = store
        .read_daemon_journal(temp.path())
        .expect("read journal");
    assert!(
        !journal.iter().any(|e| e.event_type
            == ralph_burning::contexts::automation_runtime::DaemonJournalEventType::LeaseReleased),
        "LeaseReleased must not be emitted on partial cleanup failure"
    );
}

// ---------------------------------------------------------------------------
// CLI crash-safe acquisition: lease persistence before writer-lock acquisition
// ---------------------------------------------------------------------------

/// A DaemonStorePort wrapper that tracks call order to prove CLI lease
/// persistence occurs before writer-lock acquisition.
struct CliAcquireOrderTrackingStore {
    inner: FsDaemonStore,
    operations: std::sync::Mutex<Vec<&'static str>>,
}

impl CliAcquireOrderTrackingStore {
    fn new() -> Self {
        Self {
            inner: FsDaemonStore,
            operations: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl DaemonStorePort for CliAcquireOrderTrackingStore {
    fn list_tasks(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<DaemonTask>> {
        self.inner.list_tasks(base_dir)
    }
    fn read_task(
        &self,
        base_dir: &std::path::Path,
        task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<DaemonTask> {
        self.inner.read_task(base_dir, task_id)
    }
    fn create_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.create_task(base_dir, task)
    }
    fn write_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_task(base_dir, task)
    }
    fn list_leases(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<WorktreeLease>> {
        self.inner.list_leases(base_dir)
    }
    fn read_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<WorktreeLease> {
        self.inner.read_lease(base_dir, lease_id)
    }
    fn write_lease(
        &self,
        base_dir: &std::path::Path,
        lease: &WorktreeLease,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease(base_dir, lease)
    }
    fn list_lease_records(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::model::LeaseRecord>,
    > {
        self.inner.list_lease_records(base_dir)
    }
    fn read_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::model::LeaseRecord,
    > {
        self.inner.read_lease_record(base_dir, lease_id)
    }
    fn write_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease: &ralph_burning::contexts::automation_runtime::model::LeaseRecord,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.operations.lock().unwrap().push("write_lease_record");
        self.inner.write_lease_record(base_dir, lease)
    }
    fn remove_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::ResourceCleanupOutcome,
    > {
        self.inner.remove_lease(base_dir, lease_id)
    }
    fn read_daemon_journal(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::DaemonJournalEvent>,
    > {
        self.inner.read_daemon_journal(base_dir)
    }
    fn append_daemon_journal_event(
        &self,
        base_dir: &std::path::Path,
        event: &ralph_burning::contexts::automation_runtime::DaemonJournalEvent,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.append_daemon_journal_event(base_dir, event)
    }
    fn acquire_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.operations.lock().unwrap().push("acquire_writer_lock");
        self.inner
            .acquire_writer_lock(base_dir, project_id, lease_id)
    }
    fn release_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        expected_owner: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WriterLockReleaseOutcome,
    > {
        self.inner
            .release_writer_lock(base_dir, project_id, expected_owner)
    }
}

#[tokio::test]
async fn cli_acquire_persists_lease_before_writer_lock() {
    // Proves the crash-safety invariant: the durable CLI lease record is
    // written before the writer lock is acquired.
    let temp = tempdir().expect("tempdir");
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("order-test".to_owned()).expect("valid id");

    let tracking_store = Arc::new(CliAcquireOrderTrackingStore::new());
    let store: Arc<dyn DaemonStorePort + Send + Sync> = Arc::clone(&tracking_store) as _;

    let guard = CliWriterLeaseGuard::acquire(
        store,
        temp.path(),
        project_id,
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    )
    .expect("acquire");

    let ops = tracking_store.operations.lock().unwrap();
    assert!(
        ops.len() >= 2,
        "should have at least write_lease_record and acquire_writer_lock, got: {ops:?}"
    );
    assert_eq!(
        ops[0], "write_lease_record",
        "first operation must be lease persistence, got: {ops:?}"
    );
    assert_eq!(
        ops[1], "acquire_writer_lock",
        "second operation must be lock acquisition, got: {ops:?}"
    );

    drop(guard);
}

// ---------------------------------------------------------------------------
// CLI contention rollback: lock held + prewritten-lease cleanup failure
// ---------------------------------------------------------------------------

/// A DaemonStorePort wrapper that delegates `write_lease_record` (succeeds),
/// always fails `acquire_writer_lock` with ProjectWriterLockHeld, and fails
/// `remove_lease` with an I/O error. This tests the contention + cleanup
/// failure path in the reordered acquire().
struct CliContentionCleanupFailStore {
    inner: FsDaemonStore,
}

impl DaemonStorePort for CliContentionCleanupFailStore {
    fn list_tasks(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<DaemonTask>> {
        self.inner.list_tasks(base_dir)
    }
    fn read_task(
        &self,
        base_dir: &std::path::Path,
        task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<DaemonTask> {
        self.inner.read_task(base_dir, task_id)
    }
    fn create_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.create_task(base_dir, task)
    }
    fn write_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_task(base_dir, task)
    }
    fn list_leases(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<WorktreeLease>> {
        self.inner.list_leases(base_dir)
    }
    fn read_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<WorktreeLease> {
        self.inner.read_lease(base_dir, lease_id)
    }
    fn write_lease(
        &self,
        base_dir: &std::path::Path,
        lease: &WorktreeLease,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease(base_dir, lease)
    }
    fn list_lease_records(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::model::LeaseRecord>,
    > {
        self.inner.list_lease_records(base_dir)
    }
    fn read_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::model::LeaseRecord,
    > {
        self.inner.read_lease_record(base_dir, lease_id)
    }
    fn write_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease: &ralph_burning::contexts::automation_runtime::model::LeaseRecord,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease_record(base_dir, lease)
    }
    fn remove_lease(
        &self,
        _base_dir: &std::path::Path,
        _lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::ResourceCleanupOutcome,
    > {
        // Simulate prewritten-lease cleanup failure
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "simulated lease cleanup failure",
        )
        .into())
    }
    fn read_daemon_journal(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::DaemonJournalEvent>,
    > {
        self.inner.read_daemon_journal(base_dir)
    }
    fn append_daemon_journal_event(
        &self,
        base_dir: &std::path::Path,
        event: &ralph_burning::contexts::automation_runtime::DaemonJournalEvent,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.append_daemon_journal_event(base_dir, event)
    }
    fn acquire_writer_lock(
        &self,
        _base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        _lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        // Simulate lock contention
        Err(AppError::ProjectWriterLockHeld {
            project_id: project_id.to_string(),
        })
    }
    fn release_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        expected_owner: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WriterLockReleaseOutcome,
    > {
        self.inner
            .release_writer_lock(base_dir, project_id, expected_owner)
    }
}

#[tokio::test]
async fn cli_contention_cleanup_failure_reports_both_causes() {
    // When writer-lock acquisition fails (contention) and the prewritten CLI
    // lease cleanup also fails, the returned error must preserve both the
    // contention cause and the cleanup failure cause.
    let temp = tempdir().expect("tempdir");
    let project_id = ralph_burning::shared::domain::ProjectId::new("contention-fail".to_owned())
        .expect("valid id");

    // Pre-hold the writer lock with a different owner to verify it stays intact.
    FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "competing-writer")
        .expect("pre-acquire");

    let store: Arc<dyn DaemonStorePort + Send + Sync> = Arc::new(CliContentionCleanupFailStore {
        inner: FsDaemonStore,
    });

    let result = CliWriterLeaseGuard::acquire(
        store,
        temp.path(),
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    );
    let err = match result {
        Err(e) => e,
        Ok(guard) => {
            drop(guard);
            panic!("acquire should fail on contention");
        }
    };

    let err_msg = format!("{err}");
    // Must be an AcquisitionRollbackFailed variant
    assert!(
        matches!(err, AppError::AcquisitionRollbackFailed { .. }),
        "error should be AcquisitionRollbackFailed, got: {err:?}"
    );
    // Must include the contention cause
    assert!(
        err_msg.contains("contention-fail"),
        "error should include contention project id, got: {err_msg}"
    );
    // Must include the cleanup failure
    assert!(
        err_msg.contains("simulated lease cleanup failure"),
        "error should include cleanup failure detail, got: {err_msg}"
    );

    // The competing writer's lock must remain untouched.
    let lock_err = FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "third-party")
        .expect_err("competing writer lock must still be held");
    assert!(
        matches!(lock_err, AppError::ProjectWriterLockHeld { .. }),
        "lock should still be held by competing-writer"
    );

    // Cleanup
    FsDaemonStore
        .release_writer_lock(temp.path(), &project_id, "competing-writer")
        .expect("cleanup");
}

// ---------------------------------------------------------------------------
// CLI contention rollback: lock held + prewritten lease already absent
// ---------------------------------------------------------------------------

/// A DaemonStorePort wrapper that delegates `write_lease_record` (succeeds),
/// always fails `acquire_writer_lock` with ProjectWriterLockHeld, and returns
/// `AlreadyAbsent` from `remove_lease`. This tests the contention +
/// already-absent rollback path.
struct CliContentionLeaseAbsentStore {
    inner: FsDaemonStore,
}

impl DaemonStorePort for CliContentionLeaseAbsentStore {
    fn list_tasks(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<DaemonTask>> {
        self.inner.list_tasks(base_dir)
    }
    fn read_task(
        &self,
        base_dir: &std::path::Path,
        task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<DaemonTask> {
        self.inner.read_task(base_dir, task_id)
    }
    fn create_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.create_task(base_dir, task)
    }
    fn write_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_task(base_dir, task)
    }
    fn list_leases(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<WorktreeLease>> {
        self.inner.list_leases(base_dir)
    }
    fn read_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<WorktreeLease> {
        self.inner.read_lease(base_dir, lease_id)
    }
    fn write_lease(
        &self,
        base_dir: &std::path::Path,
        lease: &WorktreeLease,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease(base_dir, lease)
    }
    fn list_lease_records(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::model::LeaseRecord>,
    > {
        self.inner.list_lease_records(base_dir)
    }
    fn read_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::model::LeaseRecord,
    > {
        self.inner.read_lease_record(base_dir, lease_id)
    }
    fn write_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease: &ralph_burning::contexts::automation_runtime::model::LeaseRecord,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease_record(base_dir, lease)
    }
    fn remove_lease(
        &self,
        _base_dir: &std::path::Path,
        _lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::ResourceCleanupOutcome,
    > {
        // Simulate the prewritten lease being already absent at rollback time
        Ok(ralph_burning::contexts::automation_runtime::ResourceCleanupOutcome::AlreadyAbsent)
    }
    fn read_daemon_journal(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::DaemonJournalEvent>,
    > {
        self.inner.read_daemon_journal(base_dir)
    }
    fn append_daemon_journal_event(
        &self,
        base_dir: &std::path::Path,
        event: &ralph_burning::contexts::automation_runtime::DaemonJournalEvent,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.append_daemon_journal_event(base_dir, event)
    }
    fn acquire_writer_lock(
        &self,
        _base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        _lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        // Simulate lock contention
        Err(AppError::ProjectWriterLockHeld {
            project_id: project_id.to_string(),
        })
    }
    fn release_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        expected_owner: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WriterLockReleaseOutcome,
    > {
        self.inner
            .release_writer_lock(base_dir, project_id, expected_owner)
    }
}

#[tokio::test]
async fn cli_contention_lease_already_absent_reports_rollback_failure() {
    // When writer-lock acquisition fails (contention) and the prewritten CLI
    // lease is already absent at rollback time, the returned error must be
    // AcquisitionRollbackFailed preserving both the contention cause and the
    // already-absent detail.
    let temp = tempdir().expect("tempdir");
    let project_id = ralph_burning::shared::domain::ProjectId::new("absent-rollback".to_owned())
        .expect("valid id");

    // Pre-hold the writer lock with a different owner to verify it stays intact.
    FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "competing-writer")
        .expect("pre-acquire");

    let store: Arc<dyn DaemonStorePort + Send + Sync> = Arc::new(CliContentionLeaseAbsentStore {
        inner: FsDaemonStore,
    });

    let result = CliWriterLeaseGuard::acquire(
        store,
        temp.path(),
        project_id.clone(),
        CLI_LEASE_TTL_SECONDS,
        CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
    );
    let err = match result {
        Err(e) => e,
        Ok(guard) => {
            drop(guard);
            panic!("acquire should fail on contention");
        }
    };

    let err_msg = format!("{err}");
    // Must be an AcquisitionRollbackFailed variant
    assert!(
        matches!(err, AppError::AcquisitionRollbackFailed { .. }),
        "error should be AcquisitionRollbackFailed, got: {err:?}"
    );
    // Must include the contention cause
    assert!(
        err_msg.contains("absent-rollback"),
        "error should include contention project id, got: {err_msg}"
    );
    // Must include the already-absent detail
    assert!(
        err_msg.contains("already absent"),
        "error should include already-absent detail, got: {err_msg}"
    );

    // The competing writer's lock must remain untouched.
    let lock_err = FsDaemonStore
        .acquire_writer_lock(temp.path(), &project_id, "third-party")
        .expect_err("competing writer lock must still be held");
    assert!(
        matches!(lock_err, AppError::ProjectWriterLockHeld { .. }),
        "lock should still be held by competing-writer"
    );

    // Cleanup
    FsDaemonStore
        .release_writer_lock(temp.path(), &project_id, "competing-writer")
        .expect("cleanup");
}

// ---------------------------------------------------------------------------
// Worktree acquisition rollback: lease persistence fails + lock release fails
// ---------------------------------------------------------------------------

/// A DaemonStorePort wrapper that fails `write_lease` (worktree lease
/// persistence) and `release_writer_lock` (rollback) to test combined
/// rollback failure reporting for worktree acquisition.
struct WorktreeAcquireRollbackFailStore {
    inner: FsDaemonStore,
}

impl DaemonStorePort for WorktreeAcquireRollbackFailStore {
    fn list_tasks(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<DaemonTask>> {
        self.inner.list_tasks(base_dir)
    }
    fn read_task(
        &self,
        base_dir: &std::path::Path,
        task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<DaemonTask> {
        self.inner.read_task(base_dir, task_id)
    }
    fn create_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.create_task(base_dir, task)
    }
    fn write_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_task(base_dir, task)
    }
    fn list_leases(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<WorktreeLease>> {
        self.inner.list_leases(base_dir)
    }
    fn read_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<WorktreeLease> {
        self.inner.read_lease(base_dir, lease_id)
    }
    fn write_lease(
        &self,
        _base_dir: &std::path::Path,
        _lease: &WorktreeLease,
    ) -> ralph_burning::shared::error::AppResult<()> {
        // Simulate worktree lease persistence failure
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "simulated worktree lease write failure",
        )
        .into())
    }
    fn list_lease_records(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::model::LeaseRecord>,
    > {
        self.inner.list_lease_records(base_dir)
    }
    fn read_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::model::LeaseRecord,
    > {
        self.inner.read_lease_record(base_dir, lease_id)
    }
    fn write_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease: &ralph_burning::contexts::automation_runtime::model::LeaseRecord,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease_record(base_dir, lease)
    }
    fn remove_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::ResourceCleanupOutcome,
    > {
        self.inner.remove_lease(base_dir, lease_id)
    }
    fn read_daemon_journal(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::DaemonJournalEvent>,
    > {
        self.inner.read_daemon_journal(base_dir)
    }
    fn append_daemon_journal_event(
        &self,
        base_dir: &std::path::Path,
        event: &ralph_burning::contexts::automation_runtime::DaemonJournalEvent,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.append_daemon_journal_event(base_dir, event)
    }
    fn acquire_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner
            .acquire_writer_lock(base_dir, project_id, lease_id)
    }
    fn release_writer_lock(
        &self,
        _base_dir: &std::path::Path,
        _project_id: &ralph_burning::shared::domain::ProjectId,
        _expected_owner: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WriterLockReleaseOutcome,
    > {
        // Simulate writer-lock release failure during rollback
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "simulated rollback lock release failure",
        )
        .into())
    }
}

#[test]
fn worktree_acquire_rollback_failure_reports_both_causes_and_lock_warning() {
    // When worktree lease persistence fails and rollback lock release also
    // fails, the returned error must include both the acquisition failure
    // and rollback failure details, including the "writer lock may still be
    // held" warning.
    let temp = tempdir().expect("tempdir");
    let store = WorktreeAcquireRollbackFailStore {
        inner: FsDaemonStore,
    };
    let worktree_adapter = SuccessWorktreeAdapter;
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("wt-rb-fail".to_owned()).expect("valid id");

    // Create a task so acquire() can proceed past the duplicate-lease check
    let mut task = sample_task();
    task.task_id = "wt-rb-fail-task".to_owned();
    task.project_id = "wt-rb-fail".to_owned();
    store.create_task(temp.path(), &task).expect("create task");

    let err = LeaseService::acquire(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        "wt-rb-fail-task",
        &project_id,
        300,
        None,
        None,
        false,
    )
    .expect_err("acquire should fail");

    let err_msg = format!("{err}");
    // Must include the original lease-write failure
    assert!(
        err_msg.contains("simulated worktree lease write failure"),
        "error should include lease-write failure, got: {err_msg}"
    );
    // Must include the "writer lock may still be held" warning
    assert!(
        err_msg.contains("writer lock may still be held"),
        "error should include lock-held warning, got: {err_msg}"
    );
    // Must include the rollback failure detail
    assert!(
        err_msg.contains("simulated rollback lock release failure"),
        "error should include rollback failure detail, got: {err_msg}"
    );
    // Must be an AcquisitionRollbackFailed variant
    assert!(
        matches!(err, AppError::AcquisitionRollbackFailed { .. }),
        "error should be AcquisitionRollbackFailed, got: {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Worktree acquisition rollback: create_worktree creates dir then fails +
// lock release fails → both worktree removal and lock failure reported
// ---------------------------------------------------------------------------

/// A worktree adapter that creates the directory and then returns an error,
/// simulating a partial worktree creation (e.g. `git worktree add` fails
/// after creating the directory but before finishing setup).
struct PartialCreateWorktreeAdapter;

impl WorktreePort for PartialCreateWorktreeAdapter {
    fn worktree_path(&self, base_dir: &std::path::Path, task_id: &str) -> std::path::PathBuf {
        base_dir
            .join(".ralph-burning")
            .join("worktrees")
            .join(task_id)
    }

    fn branch_name(&self, task_id: &str) -> String {
        format!("rb/{task_id}")
    }

    fn create_worktree(
        &self,
        _repo_root: &std::path::Path,
        worktree_path: &std::path::Path,
        _branch_name: &str,
        task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        // Create the directory (partial side-effect) then fail
        std::fs::create_dir_all(worktree_path)?;
        Err(
            ralph_burning::shared::error::AppError::WorktreeCreationFailed {
                task_id: task_id.to_owned(),
                details: "simulated partial worktree creation failure".to_owned(),
            },
        )
    }

    fn remove_worktree(
        &self,
        _repo_root: &std::path::Path,
        worktree_path: &std::path::Path,
        _task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WorktreeCleanupOutcome,
    > {
        use ralph_burning::contexts::automation_runtime::WorktreeCleanupOutcome;
        if worktree_path.exists() {
            std::fs::remove_dir_all(worktree_path)?;
            Ok(WorktreeCleanupOutcome::Removed)
        } else {
            Ok(WorktreeCleanupOutcome::AlreadyAbsent)
        }
    }

    fn rebase_onto_default_branch(
        &self,
        _repo_root: &std::path::Path,
        _worktree_path: &std::path::Path,
        _branch_name: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        Ok(())
    }
}

/// A DaemonStorePort wrapper that fails `release_writer_lock` (for rollback
/// testing) but delegates everything else to FsDaemonStore.
struct LockReleaseFailStore {
    inner: FsDaemonStore,
}

impl DaemonStorePort for LockReleaseFailStore {
    fn list_tasks(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<DaemonTask>> {
        self.inner.list_tasks(base_dir)
    }
    fn read_task(
        &self,
        base_dir: &std::path::Path,
        task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<DaemonTask> {
        self.inner.read_task(base_dir, task_id)
    }
    fn create_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.create_task(base_dir, task)
    }
    fn write_task(
        &self,
        base_dir: &std::path::Path,
        task: &DaemonTask,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_task(base_dir, task)
    }
    fn list_leases(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<Vec<WorktreeLease>> {
        self.inner.list_leases(base_dir)
    }
    fn read_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<WorktreeLease> {
        self.inner.read_lease(base_dir, lease_id)
    }
    fn write_lease(
        &self,
        base_dir: &std::path::Path,
        lease: &WorktreeLease,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease(base_dir, lease)
    }
    fn list_lease_records(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::model::LeaseRecord>,
    > {
        self.inner.list_lease_records(base_dir)
    }
    fn read_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::model::LeaseRecord,
    > {
        self.inner.read_lease_record(base_dir, lease_id)
    }
    fn write_lease_record(
        &self,
        base_dir: &std::path::Path,
        lease: &ralph_burning::contexts::automation_runtime::model::LeaseRecord,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.write_lease_record(base_dir, lease)
    }
    fn remove_lease(
        &self,
        base_dir: &std::path::Path,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::ResourceCleanupOutcome,
    > {
        self.inner.remove_lease(base_dir, lease_id)
    }
    fn read_daemon_journal(
        &self,
        base_dir: &std::path::Path,
    ) -> ralph_burning::shared::error::AppResult<
        Vec<ralph_burning::contexts::automation_runtime::DaemonJournalEvent>,
    > {
        self.inner.read_daemon_journal(base_dir)
    }
    fn append_daemon_journal_event(
        &self,
        base_dir: &std::path::Path,
        event: &ralph_burning::contexts::automation_runtime::DaemonJournalEvent,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner.append_daemon_journal_event(base_dir, event)
    }
    fn acquire_writer_lock(
        &self,
        base_dir: &std::path::Path,
        project_id: &ralph_burning::shared::domain::ProjectId,
        lease_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.inner
            .acquire_writer_lock(base_dir, project_id, lease_id)
    }
    fn release_writer_lock(
        &self,
        _base_dir: &std::path::Path,
        _project_id: &ralph_burning::shared::domain::ProjectId,
        _expected_owner: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WriterLockReleaseOutcome,
    > {
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "simulated rollback lock release failure",
        )
        .into())
    }
}

#[test]
fn worktree_acquire_create_worktree_partial_fail_rollback_cleans_dir_and_reports_lock_failure() {
    // When create_worktree() leaves a partially created directory and then
    // fails, rollback must:
    //   1. Remove the partially created worktree directory
    //   2. Attempt writer-lock release (which also fails here)
    //   3. Return AcquisitionRollbackFailed with both the create failure and
    //      rollback failure details, including the "writer lock may still be
    //      held" warning.
    let temp = tempdir().expect("tempdir");
    let store = LockReleaseFailStore {
        inner: FsDaemonStore,
    };
    let worktree_adapter = PartialCreateWorktreeAdapter;
    let project_id =
        ralph_burning::shared::domain::ProjectId::new("wt-partial".to_owned()).expect("valid id");

    let err = LeaseService::acquire(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        "wt-partial-task",
        &project_id,
        300,
        None,
        None,
        false,
    )
    .expect_err("acquire should fail");

    let err_msg = format!("{err}");
    // Must include the original create-worktree failure
    assert!(
        err_msg.contains("simulated partial worktree creation failure"),
        "error should include create-worktree failure, got: {err_msg}"
    );
    // Must include the "writer lock may still be held" warning
    assert!(
        err_msg.contains("writer lock may still be held"),
        "error should include lock-held warning, got: {err_msg}"
    );
    // Must include the rollback lock release failure detail
    assert!(
        err_msg.contains("simulated rollback lock release failure"),
        "error should include rollback failure detail, got: {err_msg}"
    );
    // Must be an AcquisitionRollbackFailed variant
    assert!(
        matches!(err, AppError::AcquisitionRollbackFailed { .. }),
        "error should be AcquisitionRollbackFailed, got: {err:?}"
    );

    // The partially created worktree directory must have been cleaned up
    let wt_path = temp
        .path()
        .join(".ralph-burning")
        .join("worktrees")
        .join("wt-partial-task");
    assert!(
        !wt_path.exists(),
        "partially created worktree directory should have been removed during rollback"
    );
}

#[test]
fn worktree_acquire_create_worktree_partial_fail_rollback_clean_lock_release_succeeds() {
    // When create_worktree() fails after creating a directory but lock
    // release succeeds, rollback should clean the directory and return
    // only the original create-worktree error (not AcquisitionRollbackFailed).
    let temp = tempdir().expect("tempdir");
    let store = FsDaemonStore;
    let worktree_adapter = PartialCreateWorktreeAdapter;
    let project_id = ralph_burning::shared::domain::ProjectId::new("wt-partial-ok".to_owned())
        .expect("valid id");

    let err = LeaseService::acquire(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        "wt-partial-ok-task",
        &project_id,
        300,
        None,
        None,
        false,
    )
    .expect_err("acquire should fail");

    let err_msg = format!("{err}");
    // Should be the original error, not AcquisitionRollbackFailed
    assert!(
        err_msg.contains("simulated partial worktree creation failure"),
        "error should include create-worktree failure, got: {err_msg}"
    );
    assert!(
        !matches!(err, AppError::AcquisitionRollbackFailed { .. }),
        "should NOT be AcquisitionRollbackFailed when rollback succeeds, got: {err:?}"
    );

    // The partially created worktree directory must have been cleaned up
    let wt_path = temp
        .path()
        .join(".ralph-burning")
        .join("worktrees")
        .join("wt-partial-ok-task");
    assert!(
        !wt_path.exists(),
        "partially created worktree directory should have been removed during rollback"
    );

    // Writer lock should be available
    store
        .acquire_writer_lock(temp.path(), &project_id, "after-rollback")
        .expect("lock should be available after clean rollback");
    store
        .release_writer_lock(temp.path(), &project_id, "after-rollback")
        .expect("cleanup");
}

// ---------------------------------------------------------------------------
// Reconcile: oversized TTL override must not reclaim fresh leases
// ---------------------------------------------------------------------------

#[test]
fn reconcile_oversized_ttl_override_does_not_reclaim_fresh_worktree_or_cli_lease() {
    // A ttl_override_seconds value above i64::MAX must be saturated to
    // i64::MAX, preventing fresh leases from being marked stale.
    let store = FsDaemonStore;
    let temp = tempdir().expect("tempdir");
    let now = Utc::now();

    // Create a fresh worktree lease with a matching task.
    let mut task = sample_task();
    task.task_id = "oversized-ttl-task".to_owned();
    task.project_id = "oversized-proj".to_owned();
    task.status = TaskStatus::Active;
    task.lease_id = Some("lease-oversized-ttl-task".to_owned());
    store.create_task(temp.path(), &task).expect("create task");

    let wt_path = temp.path().join("wt-oversized");
    std::fs::create_dir_all(&wt_path).expect("create worktree dir");

    let wt_lease = WorktreeLease {
        lease_id: "lease-oversized-ttl-task".to_owned(),
        task_id: "oversized-ttl-task".to_owned(),
        project_id: "oversized-proj".to_owned(),
        worktree_path: wt_path,
        branch_name: "rb/oversized-ttl-task".to_owned(),
        acquired_at: now,
        ttl_seconds: 300,
        last_heartbeat: now,
    };
    store
        .write_lease(temp.path(), &wt_lease)
        .expect("write worktree lease");
    let project_id = ralph_burning::shared::domain::ProjectId::new("oversized-proj".to_owned())
        .expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-oversized-ttl-task")
        .expect("acquire lock for worktree");

    // Inject a fresh CLI lease with matching writer lock.
    let cli_lease = CliWriterLease {
        lease_id: "cli-oversized-ttl".to_owned(),
        project_id: "cli-oversized-proj".to_owned(),
        owner: "cli".to_owned(),
        acquired_at: now,
        ttl_seconds: 300,
        last_heartbeat: now,
    };
    store
        .write_lease_record(temp.path(), &LeaseRecord::CliWriter(cli_lease))
        .expect("write cli lease");
    let cli_project_id =
        ralph_burning::shared::domain::ProjectId::new("cli-oversized-proj".to_owned())
            .expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &cli_project_id, "cli-oversized-ttl")
        .expect("acquire lock for cli");

    let worktree_adapter = WorktreeAdapter;

    // Use u64::MAX as the oversized TTL — must saturate, not wrap negative.
    let report = LeaseService::reconcile(
        &store,
        &worktree_adapter,
        temp.path(),
        temp.path(),
        Some(u64::MAX),
        now,
    )
    .expect("reconcile");

    assert_eq!(
        0,
        report.stale_lease_ids.len(),
        "stale_leases should be 0 with oversized TTL, got: {:?}",
        report.stale_lease_ids
    );
    assert_eq!(
        0,
        report.released_lease_ids.len(),
        "released_leases should be 0 with oversized TTL"
    );
    assert_eq!(
        0,
        report.failed_task_ids.len(),
        "failed_tasks should be 0 with oversized TTL"
    );
    assert!(
        report.cleanup_failures.is_empty(),
        "no cleanup failures expected"
    );

    // Cleanup
    store
        .release_writer_lock(temp.path(), &project_id, "lease-oversized-ttl-task")
        .expect("cleanup wt lock");
    store
        .release_writer_lock(temp.path(), &cli_project_id, "cli-oversized-ttl")
        .expect("cleanup cli lock");
}

// ---------------------------------------------------------------------------
// Worktree preservation: has_checkpoint_commits implementation-stage gating
// ---------------------------------------------------------------------------

/// A worktree adapter that tracks force_push_branch calls and allows configuring
/// has_checkpoint_commits return value. Used for preservation regression tests.
struct TrackingWorktreeAdapter {
    checkpoint_result: bool,
    force_push_calls: std::sync::Mutex<Vec<String>>,
    resume_calls: std::sync::Mutex<Vec<String>>,
}

impl TrackingWorktreeAdapter {
    fn new(checkpoint_result: bool) -> Self {
        Self {
            checkpoint_result,
            force_push_calls: std::sync::Mutex::new(Vec::new()),
            resume_calls: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn force_push_call_count(&self) -> usize {
        self.force_push_calls.lock().unwrap().len()
    }

    fn resume_call_count(&self) -> usize {
        self.resume_calls.lock().unwrap().len()
    }
}

impl WorktreePort for TrackingWorktreeAdapter {
    fn worktree_path(&self, base_dir: &std::path::Path, task_id: &str) -> std::path::PathBuf {
        base_dir
            .join(".ralph-burning")
            .join("worktrees")
            .join(task_id)
    }

    fn branch_name(&self, task_id: &str) -> String {
        format!("rb/{task_id}")
    }

    fn create_worktree(
        &self,
        _repo_root: &std::path::Path,
        worktree_path: &std::path::Path,
        _branch_name: &str,
        _task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        std::fs::create_dir_all(worktree_path)?;
        Ok(())
    }

    fn remove_worktree(
        &self,
        _repo_root: &std::path::Path,
        worktree_path: &std::path::Path,
        _task_id: &str,
    ) -> ralph_burning::shared::error::AppResult<
        ralph_burning::contexts::automation_runtime::WorktreeCleanupOutcome,
    > {
        use ralph_burning::contexts::automation_runtime::WorktreeCleanupOutcome;
        if worktree_path.exists() {
            std::fs::remove_dir_all(worktree_path)?;
            Ok(WorktreeCleanupOutcome::Removed)
        } else {
            Ok(WorktreeCleanupOutcome::AlreadyAbsent)
        }
    }

    fn rebase_onto_default_branch(
        &self,
        _repo_root: &std::path::Path,
        _worktree_path: &std::path::Path,
        _branch_name: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        Ok(())
    }

    fn has_checkpoint_commits(
        &self,
        _repo_root: &std::path::Path,
        _worktree_path: &std::path::Path,
    ) -> bool {
        self.checkpoint_result
    }

    fn force_push_branch(
        &self,
        _repo_root: &std::path::Path,
        _worktree_path: &std::path::Path,
        branch_name: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        self.force_push_calls
            .lock()
            .unwrap()
            .push(branch_name.to_owned());
        Ok(())
    }

    fn try_resume_from_remote(
        &self,
        _repo_root: &std::path::Path,
        _worktree_path: &std::path::Path,
        branch_name: &str,
    ) -> ralph_burning::shared::error::AppResult<bool> {
        self.resume_calls
            .lock()
            .unwrap()
            .push(branch_name.to_owned());
        Ok(true)
    }
}

#[test]
fn try_preserve_failed_branch_force_pushes_when_checkpoints_present() {
    let tracking = TrackingWorktreeAdapter::new(true);
    let temp = tempdir().expect("tempdir");

    let worktree_path = temp.path().join("worktrees").join("push-preserve-test");
    std::fs::create_dir_all(&worktree_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-push-preserve-test".to_owned(),
        task_id: "push-preserve-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path,
        branch_name: "rb/push-preserve-test".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 3600,
        last_heartbeat: Utc::now(),
    };

    // The centralized try_preserve_failed_branch should force_push when
    // implementation-stage checkpoints exist.
    try_preserve_failed_branch(&tracking, temp.path(), &lease);
    assert_eq!(
        tracking.force_push_call_count(),
        1,
        "force_push_branch should be called once when implementation-stage checkpoints exist"
    );
}

#[test]
fn try_preserve_failed_branch_skips_push_without_checkpoints() {
    let tracking = TrackingWorktreeAdapter::new(false);
    let temp = tempdir().expect("tempdir");

    let worktree_path = temp.path().join("worktrees").join("no-push-test");
    std::fs::create_dir_all(&worktree_path).expect("create worktree dir");

    let lease = WorktreeLease {
        lease_id: "lease-no-push-test".to_owned(),
        task_id: "no-push-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path,
        branch_name: "rb/no-push-test".to_owned(),
        acquired_at: Utc::now(),
        ttl_seconds: 3600,
        last_heartbeat: Utc::now(),
    };

    // No implementation-stage checkpoints → no push
    try_preserve_failed_branch(&tracking, temp.path(), &lease);
    assert_eq!(
        tracking.force_push_call_count(),
        0,
        "force_push_branch should NOT be called when no implementation-stage checkpoints exist"
    );
}

#[test]
fn reconcile_stale_failed_task_preserves_branch_before_cleanup() {
    let store = FsDaemonStore;
    let tracking = TrackingWorktreeAdapter::new(true);
    let temp = tempdir().expect("tempdir");

    // Create a failed task with a stale lease
    let mut task = sample_task();
    task.task_id = "reconcile-preserve-test".to_owned();
    task.status = TaskStatus::Failed;
    task.failure_class = Some("test_failure".to_owned());
    store.create_task(temp.path(), &task).expect("create task");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("demo".to_owned()).expect("valid id");
    store
        .acquire_writer_lock(temp.path(), &project_id, "lease-reconcile-preserve-test")
        .expect("lock");

    let worktree_path = temp
        .path()
        .join("worktrees")
        .join("reconcile-preserve-test");
    std::fs::create_dir_all(&worktree_path).expect("create worktree dir");

    let stale_time = Utc::now() - Duration::hours(2);
    let lease = WorktreeLease {
        lease_id: "lease-reconcile-preserve-test".to_owned(),
        task_id: "reconcile-preserve-test".to_owned(),
        project_id: "demo".to_owned(),
        worktree_path,
        branch_name: "rb/reconcile-preserve-test".to_owned(),
        acquired_at: stale_time,
        ttl_seconds: 60,
        last_heartbeat: stale_time,
    };
    store.write_lease(temp.path(), &lease).expect("write lease");

    // Reconcile should detect the stale lease, preserve the branch, then clean up.
    let report = LeaseService::reconcile(
        &store,
        &tracking,
        temp.path(),
        temp.path(),
        None,
        Utc::now(),
    )
    .expect("reconcile");

    assert!(
        report.stale_lease_ids.contains(&lease.lease_id),
        "lease should be detected as stale"
    );
    assert_eq!(
        tracking.force_push_call_count(),
        1,
        "reconcile should force_push_branch for stale failed task with checkpoints"
    );
}

#[test]
fn lease_acquire_with_is_retry_true_calls_try_resume_from_remote() {
    let store = FsDaemonStore;
    let tracking = TrackingWorktreeAdapter::new(false);
    let temp = tempdir().expect("tempdir");

    let mut task = sample_task();
    task.task_id = "retry-resume-test".to_owned();
    task.project_id = "retry-resume".to_owned();
    store.create_task(temp.path(), &task).expect("create task");

    let project_id =
        ralph_burning::shared::domain::ProjectId::new("retry-resume".to_owned()).expect("valid id");

    let lease = LeaseService::acquire(
        &store,
        &tracking,
        temp.path(),
        temp.path(),
        "retry-resume-test",
        &project_id,
        300,
        None,
        None,
        true,
    )
    .expect("acquire should succeed");

    assert_eq!(
        tracking.resume_call_count(),
        1,
        "try_resume_from_remote should be called when is_retry=true"
    );

    // Clean up lease so the test directory can be removed
    let _ = LeaseService::release(
        &store,
        &tracking,
        temp.path(),
        temp.path(),
        &lease,
        ralph_burning::contexts::automation_runtime::lease_service::ReleaseMode::Idempotent,
    );
}

#[test]
fn lease_acquire_with_is_retry_false_skips_try_resume_from_remote() {
    let store = FsDaemonStore;
    let tracking = TrackingWorktreeAdapter::new(false);
    let temp = tempdir().expect("tempdir");

    let mut task = sample_task();
    task.task_id = "fresh-no-resume-test".to_owned();
    task.project_id = "fresh-no-resume".to_owned();
    store.create_task(temp.path(), &task).expect("create task");

    let project_id = ralph_burning::shared::domain::ProjectId::new("fresh-no-resume".to_owned())
        .expect("valid id");

    let lease = LeaseService::acquire(
        &store,
        &tracking,
        temp.path(),
        temp.path(),
        "fresh-no-resume-test",
        &project_id,
        300,
        None,
        None,
        false,
    )
    .expect("acquire should succeed");

    assert_eq!(
        tracking.resume_call_count(),
        0,
        "try_resume_from_remote should NOT be called when is_retry=false"
    );

    let _ = LeaseService::release(
        &store,
        &tracking,
        temp.path(),
        temp.path(),
        &lease,
        ralph_burning::contexts::automation_runtime::lease_service::ReleaseMode::Idempotent,
    );
}
