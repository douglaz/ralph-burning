use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use chrono::{Duration, Utc};
use ralph_burning::contexts::automation_runtime::model::{
    DaemonTask, RoutingSource, TaskStatus, WorktreeLease,
};
use ralph_burning::shared::domain::FlowPreset;
use tempfile::tempdir;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_ralph-burning")
}

fn initialize_workspace_fixture() -> tempfile::TempDir {
    let temp_dir = tempdir().expect("create temp dir");
    let output = Command::new(binary())
        .arg("init")
        .current_dir(temp_dir.path())
        .output()
        .expect("run init");
    assert!(output.status.success());
    temp_dir
}

fn create_project_fixture(base_dir: &std::path::Path, project_id: &str) {
    let project_root = base_dir.join(".ralph-burning/projects").join(project_id);
    fs::create_dir_all(&project_root).expect("create project directory");
    // Write a complete canonical ProjectRecord so validation passes
    let project_toml = format!(
        r#"id = "{project_id}"
name = "Fixture {project_id}"
flow = "standard"
prompt_reference = "prompt.md"
prompt_hash = "0000000000000000"
created_at = "2026-03-11T19:00:00Z"
status_summary = "created"
"#
    );
    fs::write(project_root.join("project.toml"), project_toml).expect("write project");
    // Write required canonical files so run queries don't fail on missing files
    fs::write(project_root.join("prompt.md"), "# Fixture prompt\n").expect("write prompt");
    fs::write(
        project_root.join("run.json"),
        r#"{"active_run":null,"status":"not_started","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"not started"}"#,
    ).expect("write run.json");
    fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-03-11T19:00:00Z","event_type":"project_created","details":{{"project_id":"{}","flow":"standard"}}}}"#,
            project_id
        ),
    ).expect("write journal");
    fs::write(project_root.join("sessions.json"), r#"{"sessions":[]}"#).expect("write sessions");
    for subdir in &[
        "history/payloads",
        "history/artifacts",
        "runtime/logs",
        "runtime/backend",
        "runtime/temp",
        "amendments",
        "rollback",
    ] {
        fs::create_dir_all(project_root.join(subdir)).expect("create project subdirectory");
    }
}

fn write_editor_script(
    base_dir: &std::path::Path,
    name: &str,
    contents: &str,
) -> std::path::PathBuf {
    let script_path = base_dir.join(name);
    fs::write(&script_path, contents).expect("write editor script");
    let mut permissions = fs::metadata(&script_path)
        .expect("script metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script_path, permissions).expect("set script permissions");
    script_path
}

fn write_daemon_task(base_dir: &std::path::Path, task: &DaemonTask) {
    let path = base_dir
        .join(".ralph-burning/daemon/tasks")
        .join(format!("{}.json", task.task_id));
    fs::create_dir_all(path.parent().expect("task parent")).expect("create task dir");
    fs::write(
        path,
        serde_json::to_string_pretty(task).expect("serialize daemon task"),
    )
    .expect("write daemon task");
}

fn write_worktree_lease(base_dir: &std::path::Path, lease: &WorktreeLease) {
    let path = base_dir
        .join(".ralph-burning/daemon/leases")
        .join(format!("{}.json", lease.lease_id));
    fs::create_dir_all(path.parent().expect("lease parent")).expect("create lease dir");
    fs::write(
        path,
        serde_json::to_string_pretty(lease).expect("serialize daemon lease"),
    )
    .expect("write daemon lease");
}

fn init_git_repo(base_dir: &std::path::Path) {
    let init = Command::new("git")
        .args(["init", "-b", "main"])
        .current_dir(base_dir)
        .output()
        .expect("git init");
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );

    let name = Command::new("git")
        .args(["config", "user.name", "Test User"])
        .current_dir(base_dir)
        .output()
        .expect("git config user.name");
    assert!(name.status.success());

    let email = Command::new("git")
        .args(["config", "user.email", "test@example.com"])
        .current_dir(base_dir)
        .output()
        .expect("git config user.email");
    assert!(email.status.success());

    fs::write(base_dir.join("README.md"), "# fixture\n").expect("write readme");
    let add = Command::new("git")
        .args(["add", "README.md"])
        .current_dir(base_dir)
        .output()
        .expect("git add");
    assert!(add.status.success());

    let commit = Command::new("git")
        .args(["commit", "-m", "initial"])
        .current_dir(base_dir)
        .output()
        .expect("git commit");
    assert!(
        commit.status.success(),
        "{}",
        String::from_utf8_lossy(&commit.stderr)
    );
}

#[test]
fn flow_list_prints_all_presets() {
    let output = Command::new(binary())
        .args(["flow", "list"])
        .output()
        .expect("run flow list");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("standard"));
    assert!(stdout.contains("quick_dev"));
    assert!(stdout.contains("docs_change"));
    assert!(stdout.contains("ci_improvement"));
}

#[test]
fn flow_show_standard_prints_stage_sequence() {
    let output = Command::new(binary())
        .args(["flow", "show", "standard"])
        .output()
        .expect("run flow show");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Stage count: 8"));
    assert!(stdout.contains("1. prompt_review"));
    assert!(stdout.contains("8. final_review"));
    assert!(stdout.contains("Final review enabled: yes"));
}

#[test]
fn flow_show_invalid_preset_exits_non_zero_with_clear_error() {
    let output = Command::new(binary())
        .args(["flow", "show", "unknown_flow"])
        .output()
        .expect("run flow show invalid");

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown flow preset 'unknown_flow'"));
}

#[test]
fn init_creates_workspace_layout() {
    let temp_dir = tempdir().expect("create temp dir");
    let output = Command::new(binary())
        .arg("init")
        .current_dir(temp_dir.path())
        .output()
        .expect("run init");

    assert!(output.status.success());
    assert!(temp_dir
        .path()
        .join(".ralph-burning/workspace.toml")
        .is_file());
    assert!(temp_dir.path().join(".ralph-burning/projects").is_dir());
    assert!(temp_dir.path().join(".ralph-burning/requirements").is_dir());
    assert!(temp_dir.path().join(".ralph-burning/daemon/tasks").is_dir());
    assert!(temp_dir
        .path()
        .join(".ralph-burning/daemon/leases")
        .is_dir());
}

#[test]
fn init_fails_when_workspace_already_exists() {
    let temp_dir = tempdir().expect("create temp dir");

    let first = Command::new(binary())
        .arg("init")
        .current_dir(temp_dir.path())
        .output()
        .expect("run first init");
    assert!(first.status.success());

    let second = Command::new(binary())
        .arg("init")
        .current_dir(temp_dir.path())
        .output()
        .expect("run second init");

    assert!(!second.status.success());

    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(stderr.contains("workspace already initialized"));
}

#[test]
fn config_show_prints_effective_values_and_sources() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["config", "show"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run config show");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("[settings]"));
    assert!(stdout.contains("prompt_review.enabled = true # source: default"));
    assert!(stdout.contains("default_flow = \"standard\" # source: default"));
    assert!(stdout.contains("default_backend = \"<unset>\" # source: default"));
}

#[test]
fn daemon_status_lists_non_terminal_tasks_first() {
    let temp_dir = initialize_workspace_fixture();
    let now = Utc::now();

    write_daemon_task(
        temp_dir.path(),
        &DaemonTask {
            task_id: "task-active".to_owned(),
            issue_ref: "repo#2".to_owned(),
            project_id: "demo-active".to_owned(),
            project_name: Some("Active".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Active,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: Some("lease-active".to_owned()),
            failure_class: None,
            failure_message: None,
        },
    );
    write_worktree_lease(
        temp_dir.path(),
        &WorktreeLease {
            lease_id: "lease-active".to_owned(),
            task_id: "task-active".to_owned(),
            project_id: "demo-active".to_owned(),
            worktree_path: temp_dir.path().join(".ralph-burning/worktrees/task-active"),
            branch_name: "rb/task/task-active".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
        },
    );
    write_daemon_task(
        temp_dir.path(),
        &DaemonTask {
            task_id: "task-completed".to_owned(),
            issue_ref: "repo#3".to_owned(),
            project_id: "demo-completed".to_owned(),
            project_name: Some("Completed".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Completed,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
        },
    );

    let output = Command::new(binary())
        .args(["daemon", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run daemon status");
    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    let active_idx = stdout.find("task-active").expect("active task");
    let completed_idx = stdout.find("task-completed").expect("completed task");
    assert!(active_idx < completed_idx);
    assert!(stdout.contains("lease=lease-active"));
}

#[test]
fn daemon_retry_transitions_failed_task_to_pending() {
    let temp_dir = initialize_workspace_fixture();
    let now = Utc::now();
    write_daemon_task(
        temp_dir.path(),
        &DaemonTask {
            task_id: "task-failed".to_owned(),
            issue_ref: "repo#4".to_owned(),
            project_id: "demo-failed".to_owned(),
            project_name: Some("Failed".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Failed,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: Some("daemon_dispatch_failed".to_owned()),
            failure_message: Some("boom".to_owned()),
        },
    );

    let output = Command::new(binary())
        .args(["daemon", "retry", "task-failed"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run daemon retry");
    assert!(output.status.success());

    let task_path = temp_dir
        .path()
        .join(".ralph-burning/daemon/tasks/task-failed.json");
    let task: DaemonTask =
        serde_json::from_str(&fs::read_to_string(task_path).expect("read task")).expect("task");
    assert_eq!(TaskStatus::Pending, task.status);
    assert_eq!(1, task.attempt_count);
}

#[test]
fn daemon_abort_claimed_task_releases_lease() {
    let temp_dir = initialize_workspace_fixture();
    let now = Utc::now();
    let missing_worktree = temp_dir.path().join(".ralph-burning/worktrees/missing");
    write_daemon_task(
        temp_dir.path(),
        &DaemonTask {
            task_id: "task-claimed".to_owned(),
            issue_ref: "repo#5".to_owned(),
            project_id: "demo-claimed".to_owned(),
            project_name: Some("Claimed".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Claimed,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: Some("lease-claimed".to_owned()),
            failure_class: None,
            failure_message: None,
        },
    );
    write_worktree_lease(
        temp_dir.path(),
        &WorktreeLease {
            lease_id: "lease-claimed".to_owned(),
            task_id: "task-claimed".to_owned(),
            project_id: "demo-claimed".to_owned(),
            worktree_path: missing_worktree,
            branch_name: "rb/task/task-claimed".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
        },
    );

    let output = Command::new(binary())
        .args(["daemon", "abort", "task-claimed"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run daemon abort");
    assert!(output.status.success());

    let task_path = temp_dir
        .path()
        .join(".ralph-burning/daemon/tasks/task-claimed.json");
    let task: DaemonTask =
        serde_json::from_str(&fs::read_to_string(task_path).expect("read task")).expect("task");
    assert_eq!(TaskStatus::Aborted, task.status);
    assert!(task.lease_id.is_none());
    assert!(!temp_dir
        .path()
        .join(".ralph-burning/daemon/leases/lease-claimed.json")
        .exists());
}

#[test]
fn daemon_abort_active_task_releases_lease() {
    let temp_dir = initialize_workspace_fixture();
    let now = Utc::now();
    let missing_worktree = temp_dir
        .path()
        .join(".ralph-burning/worktrees/missing-active");
    write_daemon_task(
        temp_dir.path(),
        &DaemonTask {
            task_id: "task-active-abort".to_owned(),
            issue_ref: "repo#5a".to_owned(),
            project_id: "demo-active-abort".to_owned(),
            project_name: Some("Active Abort".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Active,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: Some("lease-active-abort".to_owned()),
            failure_class: None,
            failure_message: None,
        },
    );
    write_worktree_lease(
        temp_dir.path(),
        &WorktreeLease {
            lease_id: "lease-active-abort".to_owned(),
            task_id: "task-active-abort".to_owned(),
            project_id: "demo-active-abort".to_owned(),
            worktree_path: missing_worktree,
            branch_name: "rb/task/task-active-abort".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
        },
    );

    let output = Command::new(binary())
        .args(["daemon", "abort", "task-active-abort"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run daemon abort");
    assert!(output.status.success());

    let task_path = temp_dir
        .path()
        .join(".ralph-burning/daemon/tasks/task-active-abort.json");
    let task: DaemonTask =
        serde_json::from_str(&fs::read_to_string(task_path).expect("read task")).expect("task");
    assert_eq!(TaskStatus::Aborted, task.status);
    assert!(task.lease_id.is_none());
    assert!(!temp_dir
        .path()
        .join(".ralph-burning/daemon/leases/lease-active-abort.json")
        .exists());
}

#[test]
fn daemon_reconcile_fails_stale_claimed_task() {
    let temp_dir = initialize_workspace_fixture();
    let now = Utc::now();
    write_daemon_task(
        temp_dir.path(),
        &DaemonTask {
            task_id: "task-stale".to_owned(),
            issue_ref: "repo#6".to_owned(),
            project_id: "demo-stale".to_owned(),
            project_name: Some("Stale".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: Some(RoutingSource::DefaultFlow),
            routing_warnings: vec![],
            status: TaskStatus::Claimed,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: Some("lease-stale".to_owned()),
            failure_class: None,
            failure_message: None,
        },
    );
    write_worktree_lease(
        temp_dir.path(),
        &WorktreeLease {
            lease_id: "lease-stale".to_owned(),
            task_id: "task-stale".to_owned(),
            project_id: "demo-stale".to_owned(),
            worktree_path: temp_dir.path().join(".ralph-burning/worktrees/task-stale"),
            branch_name: "rb/task/task-stale".to_owned(),
            acquired_at: now - Duration::minutes(10),
            ttl_seconds: 300,
            last_heartbeat: now - Duration::minutes(10),
        },
    );

    let output = Command::new(binary())
        .args(["daemon", "reconcile", "--ttl-seconds", "1"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run daemon reconcile");
    assert!(output.status.success());

    let task_path = temp_dir
        .path()
        .join(".ralph-burning/daemon/tasks/task-stale.json");
    let task: DaemonTask =
        serde_json::from_str(&fs::read_to_string(task_path).expect("read task")).expect("task");
    assert_eq!(TaskStatus::Failed, task.status);
    assert_eq!(
        Some("reconciliation_timeout"),
        task.failure_class.as_deref()
    );
}

#[test]
fn daemon_start_single_iteration_fails_and_cleans_up_on_post_claim_error() {
    let temp_dir = initialize_workspace_fixture();
    init_git_repo(temp_dir.path());
    create_project_fixture(temp_dir.path(), "demo-conflict");

    let now = Utc::now();
    write_daemon_task(
        temp_dir.path(),
        &DaemonTask {
            task_id: "task-conflict".to_owned(),
            issue_ref: "repo#6a".to_owned(),
            project_id: "demo-conflict".to_owned(),
            project_name: Some("Conflict".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: Some("/rb flow docs_change".to_owned()),
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::DocsChange),
            routing_source: Some(RoutingSource::Command),
            routing_warnings: vec![],
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
        },
    );

    let output = Command::new(binary())
        .args(["daemon", "start", "--single-iteration"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run daemon start");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("claimed task task-conflict"));
    assert!(stdout.contains("failed task task-conflict"));

    let task_path = temp_dir
        .path()
        .join(".ralph-burning/daemon/tasks/task-conflict.json");
    let task: DaemonTask =
        serde_json::from_str(&fs::read_to_string(task_path).expect("read task")).expect("task");
    assert_eq!(TaskStatus::Failed, task.status);
    assert_eq!(
        Some("daemon_dispatch_failed"),
        task.failure_class.as_deref()
    );
    assert!(task.lease_id.is_none());
    assert!(!temp_dir
        .path()
        .join(".ralph-burning/daemon/leases/lease-task-conflict.json")
        .exists());
    assert!(!temp_dir
        .path()
        .join(".ralph-burning/daemon/leases/writer-demo-conflict.lock")
        .exists());
    assert!(!temp_dir
        .path()
        .join(".ralph-burning/worktrees/task-conflict")
        .exists());
}

#[test]
fn daemon_start_single_iteration_processes_pending_task() {
    let temp_dir = initialize_workspace_fixture();
    init_git_repo(temp_dir.path());

    let now = Utc::now();
    write_daemon_task(
        temp_dir.path(),
        &DaemonTask {
            task_id: "task-run".to_owned(),
            issue_ref: "repo#7".to_owned(),
            project_id: "demo-run".to_owned(),
            project_name: Some("Daemon Run".to_owned()),
            prompt: Some("Implement the daemon task".to_owned()),
            routing_command: Some("/rb flow docs_change".to_owned()),
            routing_labels: vec![String::from("rb:flow:standard")],
            resolved_flow: Some(FlowPreset::DocsChange),
            routing_source: Some(RoutingSource::Command),
            routing_warnings: vec![],
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
        },
    );

    let output = Command::new(binary())
        .args(["daemon", "start", "--single-iteration"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run daemon start");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("claimed task task-run"));
    assert!(stdout.contains("completed task task-run"));

    let task_path = temp_dir
        .path()
        .join(".ralph-burning/daemon/tasks/task-run.json");
    let task: DaemonTask =
        serde_json::from_str(&fs::read_to_string(task_path).expect("read task")).expect("task");
    assert_eq!(TaskStatus::Completed, task.status);
    assert!(task.lease_id.is_none());
    assert!(!temp_dir
        .path()
        .join(".ralph-burning/daemon/leases/lease-task-run.json")
        .exists());
}

#[test]
fn config_get_prints_known_values_and_rejects_unknown_keys() {
    let temp_dir = initialize_workspace_fixture();

    let known = Command::new(binary())
        .args(["config", "get", "default_flow"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run config get");
    assert!(known.status.success());
    assert_eq!("standard\n", String::from_utf8_lossy(&known.stdout));

    let unknown = Command::new(binary())
        .args(["config", "get", "unknown.key"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run config get invalid");
    assert!(!unknown.status.success());
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("unknown config key"));
}

#[test]
fn config_set_updates_valid_keys_and_rejects_invalid_values() {
    let temp_dir = initialize_workspace_fixture();

    let valid = Command::new(binary())
        .args(["config", "set", "default_flow", "quick_dev"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run config set");
    assert!(valid.status.success());
    assert!(String::from_utf8_lossy(&valid.stdout).contains("Updated default_flow = quick_dev"));

    let workspace_config =
        fs::read_to_string(temp_dir.path().join(".ralph-burning/workspace.toml"))
            .expect("read workspace config");
    assert!(workspace_config.contains("default_flow = \"quick_dev\""));

    let invalid_value = Command::new(binary())
        .args(["config", "set", "default_flow", "unknown_flow"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run invalid config set");
    assert!(!invalid_value.status.success());
    assert!(String::from_utf8_lossy(&invalid_value.stderr).contains("invalid value"));

    let invalid_key = Command::new(binary())
        .args(["config", "set", "unknown.key", "value"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run invalid key config set");
    assert!(!invalid_key.status.success());
    assert!(String::from_utf8_lossy(&invalid_key.stderr).contains("unknown config key"));
}

#[test]
fn config_edit_revalidates_workspace_file() {
    let temp_dir = initialize_workspace_fixture();
    let editor = write_editor_script(
        temp_dir.path(),
        "editor-valid.sh",
        "#!/bin/sh\ncat <<'EOF' > \"$1\"\nversion = 1\ncreated_at = \"2026-03-11T17:50:55Z\"\n\n[settings]\ndefault_backend = \"claude\"\nEOF\n",
    );

    let output = Command::new(binary())
        .args(["config", "edit"])
        .env("EDITOR", &editor)
        .env_remove("VISUAL")
        .current_dir(temp_dir.path())
        .output()
        .expect("run config edit");

    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("Validated workspace.toml"));
}

#[test]
fn config_edit_prefers_editor_over_visual() {
    let temp_dir = initialize_workspace_fixture();
    let editor = write_editor_script(
        temp_dir.path(),
        "editor-wins.sh",
        "#!/bin/sh\ncat <<'EOF' > \"$1\"\nversion = 1\ncreated_at = \"2026-03-11T17:50:55Z\"\n\n[settings]\ndefault_backend = \"editor\"\nEOF\n",
    );
    let visual = write_editor_script(
        temp_dir.path(),
        "visual-loses.sh",
        "#!/bin/sh\ncat <<'EOF' > \"$1\"\nversion = 1\ncreated_at = \"2026-03-11T17:50:55Z\"\n\n[settings]\ndefault_backend = \"visual\"\nEOF\n",
    );

    let output = Command::new(binary())
        .args(["config", "edit"])
        .env("EDITOR", &editor)
        .env("VISUAL", &visual)
        .current_dir(temp_dir.path())
        .output()
        .expect("run config edit");

    assert!(output.status.success());

    let workspace_config =
        fs::read_to_string(temp_dir.path().join(".ralph-burning/workspace.toml"))
            .expect("read workspace config");
    assert!(workspace_config.contains("default_backend = \"editor\""));
    assert!(!workspace_config.contains("default_backend = \"visual\""));
}

#[test]
fn config_edit_fails_when_editor_leaves_invalid_file() {
    let temp_dir = initialize_workspace_fixture();
    let editor = write_editor_script(
        temp_dir.path(),
        "editor-invalid.sh",
        "#!/bin/sh\nprintf '%s\n' 'version = 999' 'created_at = \"2026-03-11T17:50:55Z\"' > \"$1\"\n",
    );

    let output = Command::new(binary())
        .args(["config", "edit"])
        .env("EDITOR", &editor)
        .env_remove("VISUAL")
        .current_dir(temp_dir.path())
        .output()
        .expect("run invalid config edit");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("workspace.toml is invalid after editing"));
    assert!(stderr.contains("unsupported workspace version 999"));
}

#[test]
fn project_select_sets_active_project_and_rejects_missing_projects() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    let existing = Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project select");
    assert!(existing.status.success());
    assert!(String::from_utf8_lossy(&existing.stdout).contains("Selected project alpha"));
    assert_eq!(
        "alpha",
        fs::read_to_string(temp_dir.path().join(".ralph-burning/active-project"))
            .expect("read active project")
    );

    let missing = Command::new(binary())
        .args(["project", "select", "missing"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run missing project select");
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("project 'missing' was not found"));
}

#[test]
fn project_select_rejects_path_like_ids_before_writing_active_project() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["project", "select", "../escape"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run path-like project select");

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("invalid identifier"));
    assert!(!temp_dir
        .path()
        .join(".ralph-burning/active-project")
        .exists());
}

// ── Project Create ──

fn write_prompt_fixture(base_dir: &std::path::Path) -> std::path::PathBuf {
    let prompt_path = base_dir.join("test-prompt.md");
    fs::write(&prompt_path, "# Test Prompt\nImplement the feature.\n").expect("write prompt");
    prompt_path
}

#[test]
fn project_create_succeeds_and_writes_all_canonical_files() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "alpha",
            "--name",
            "Alpha Project",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Created project 'alpha'"));
    assert!(stdout.contains("standard"));

    let project_root = temp_dir.path().join(".ralph-burning/projects/alpha");
    assert!(project_root.join("project.toml").is_file());
    assert!(project_root.join("prompt.md").is_file());
    assert!(project_root.join("run.json").is_file());
    assert!(project_root.join("journal.ndjson").is_file());
    assert!(project_root.join("sessions.json").is_file());
    assert!(project_root.join("history/payloads").is_dir());
    assert!(project_root.join("history/artifacts").is_dir());
    assert!(project_root.join("runtime/logs").is_dir());
    assert!(project_root.join("runtime/backend").is_dir());
    assert!(project_root.join("runtime/temp").is_dir());
    assert!(project_root.join("amendments").is_dir());
    assert!(project_root.join("rollback").is_dir());
}

#[test]
fn project_create_initializes_journal_with_project_created_event() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "beta",
            "--name",
            "Beta",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "quick_dev",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");
    assert!(output.status.success());

    let journal = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/beta/journal.ndjson"),
    )
    .expect("read journal");

    assert!(journal.contains("\"project_created\""));
    assert!(journal.contains("\"sequence\":1"));
}

#[test]
fn project_create_run_json_shows_not_started() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "gamma",
            "--name",
            "Gamma",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    let run_json = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/gamma/run.json"),
    )
    .expect("read run.json");

    assert!(run_json.contains("\"not_started\""));
    assert!(run_json.contains("\"active_run\": null"));
}

#[test]
fn project_create_records_canonical_prompt_reference_not_source_path() {
    let temp_dir = initialize_workspace_fixture();
    // Use a non-standard filename to verify the recorded reference is canonical
    let external_prompt = temp_dir.path().join("my-external-prompt.md");
    fs::write(&external_prompt, "# External Prompt\nContent.").expect("write prompt");

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "reftest",
            "--name",
            "Ref Test",
            "--prompt",
            external_prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");
    assert!(output.status.success());

    let project_toml = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/reftest/project.toml"),
    )
    .expect("read project.toml");

    // prompt_reference should be the canonical copied path, not the source path
    assert!(
        project_toml.contains("prompt_reference = \"prompt.md\""),
        "project.toml should record canonical prompt.md, got:\n{project_toml}"
    );
    assert!(
        !project_toml.contains("my-external-prompt"),
        "project.toml should not contain the source path"
    );
}

#[test]
fn project_create_fails_on_duplicate_id() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    let first = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "dup",
            "--name",
            "First",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("first create");
    assert!(first.status.success());

    let second = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "dup",
            "--name",
            "Second",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("second create");

    assert!(!second.status.success());
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(stderr.contains("already exists"));
}

#[test]
fn project_create_fails_on_invalid_flow() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "bad-flow",
            "--name",
            "Bad Flow",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "nonexistent",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown flow preset"));
}

#[test]
fn project_create_fails_on_missing_prompt() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "no-prompt",
            "--name",
            "No Prompt",
            "--prompt",
            "/nonexistent/prompt.md",
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid prompt file"));
}

#[test]
fn project_create_does_not_set_active_project() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "noactive",
            "--name",
            "No Active",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    // active-project should not exist (create does not set it)
    assert!(!temp_dir
        .path()
        .join(".ralph-burning/active-project")
        .exists());
}

// ── Project List ──

#[test]
fn project_list_shows_no_projects_when_empty() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["project", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project list");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("No projects found"));
}

#[test]
fn project_list_shows_created_projects_with_active_marker() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    // Create two projects
    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "alpha",
            "--name",
            "Alpha",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create alpha");

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "beta",
            "--name",
            "Beta",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "quick_dev",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create beta");

    // Select alpha as active
    Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select alpha");

    let output = Command::new(binary())
        .args(["project", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project list");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("alpha *"));
    assert!(stdout.contains("beta"));
    assert!(stdout.contains("standard"));
    assert!(stdout.contains("quick_dev"));
}

// ── Project Show ──

#[test]
fn project_show_displays_project_details() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "showme",
            "--name",
            "Show Me",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "docs_change",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    let output = Command::new(binary())
        .args(["project", "show", "showme"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Project: showme"));
    assert!(stdout.contains("Name: Show Me"));
    assert!(stdout.contains("Flow: docs_change"));
    assert!(stdout.contains("Prompt hash:"));
    assert!(stdout.contains("Run status: not started"));
    assert!(stdout.contains("Journal events: 1"));
}

#[test]
fn project_show_resolves_active_project_when_no_id_given() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "active-show",
            "--name",
            "Active Show",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "active-show"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let output = Command::new(binary())
        .args(["project", "show"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show without id");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Project: active-show (active)"));
}

// ── Project Delete ──

#[test]
fn project_delete_removes_project_directory() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "deleteme",
            "--name",
            "Delete Me",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    let output = Command::new(binary())
        .args(["project", "delete", "deleteme"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project delete");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Deleted project 'deleteme'"));
    assert!(!temp_dir
        .path()
        .join(".ralph-burning/projects/deleteme")
        .exists());
}

#[test]
fn project_delete_clears_active_project_if_selected() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "active-del",
            "--name",
            "Active Del",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "active-del"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    assert!(temp_dir
        .path()
        .join(".ralph-burning/active-project")
        .exists());

    let output = Command::new(binary())
        .args(["project", "delete", "active-del"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project delete");

    assert!(output.status.success());
    assert!(!temp_dir
        .path()
        .join(".ralph-burning/active-project")
        .exists());
}

#[test]
fn project_delete_fails_for_missing_project() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["project", "delete", "nonexistent"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project delete");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("not found"));
}

// ── Run Status ──

#[test]
fn run_status_shows_not_started_for_new_project() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "status-test",
            "--name",
            "Status Test",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "status-test"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: not started"));
}

// ── Run History ──

#[test]
fn run_history_shows_journal_events_for_new_project() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "hist-test",
            "--name",
            "History Test",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "hist-test"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let output = Command::new(binary())
        .args(["run", "history"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Journal Events"));
    assert!(stdout.contains("ProjectCreated"));
}

// ── Run Tail ──

#[test]
fn run_tail_shows_durable_history_only_by_default() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "tail-test",
            "--name",
            "Tail Test",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "tail-test"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let output = Command::new(binary())
        .args(["run", "tail"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run tail");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Durable History"));
    // No runtime logs section when --logs not passed
    assert!(!stdout.contains("Runtime Logs"));
}

#[test]
fn run_tail_with_logs_includes_runtime_logs_section() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "tail-logs",
            "--name",
            "Tail Logs",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "tail-logs"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let output = Command::new(binary())
        .args(["run", "tail", "--logs"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run tail --logs");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Durable History"));
    assert!(stdout.contains("Runtime Logs"));
}

#[test]
fn run_tail_with_logs_shows_only_newest_log_file() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "tail-multi",
            "--name",
            "Tail Multi",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "tail-multi"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Write two runtime log files: old and new
    let logs_dir = temp_dir
        .path()
        .join(".ralph-burning/projects/tail-multi/runtime/logs");
    fs::write(
        logs_dir.join("001.ndjson"),
        r#"{"timestamp":"2026-03-11T18:00:00Z","level":"info","source":"agent","message":"old log entry"}"#.to_owned() + "\n",
    ).expect("write old log");
    fs::write(
        logs_dir.join("002.ndjson"),
        r#"{"timestamp":"2026-03-11T19:00:00Z","level":"info","source":"agent","message":"new log entry"}"#.to_owned() + "\n",
    ).expect("write new log");

    let output = Command::new(binary())
        .args(["run", "tail", "--logs"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run tail --logs");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Runtime Logs"));
    // Only the newest log file entries should appear
    assert!(
        stdout.contains("new log entry"),
        "newest log should be shown"
    );
    assert!(
        !stdout.contains("old log entry"),
        "older log files should not be included"
    );
}

// ── Fail-fast on missing canonical files ──

#[test]
fn run_status_fails_fast_when_run_json_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "broken",
            "--name",
            "Broken",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "broken"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Delete run.json to simulate corruption
    fs::remove_file(
        temp_dir
            .path()
            .join(".ralph-burning/projects/broken/run.json"),
    )
    .expect("remove run.json");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("run.json"));
    assert!(stderr.contains("missing"));
}

#[test]
fn run_history_fails_fast_when_journal_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "nojrnl",
            "--name",
            "No Journal",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "nojrnl"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Delete journal.ndjson to simulate corruption
    fs::remove_file(
        temp_dir
            .path()
            .join(".ralph-burning/projects/nojrnl/journal.ndjson"),
    )
    .expect("remove journal");

    let output = Command::new(binary())
        .args(["run", "history"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("journal.ndjson"));
    assert!(stderr.contains("missing"));
}

#[test]
fn run_status_fails_fast_when_run_json_is_corrupt() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "corrupt",
            "--name",
            "Corrupt",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "corrupt"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Write corrupt JSON to run.json
    fs::write(
        temp_dir
            .path()
            .join(".ralph-burning/projects/corrupt/run.json"),
        "{invalid json}",
    )
    .expect("corrupt run.json");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("run.json"));
}

// ── Missing project.toml corruption detection ──

#[test]
fn project_show_fails_fast_when_project_toml_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "corrupt-proj",
            "--name",
            "Corrupt",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    // Delete project.toml to simulate corruption
    fs::remove_file(
        temp_dir
            .path()
            .join(".ralph-burning/projects/corrupt-proj/project.toml"),
    )
    .expect("remove project.toml");

    let output = Command::new(binary())
        .args(["project", "show", "corrupt-proj"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
    assert!(stderr.contains("missing"));
}

#[test]
fn project_list_fails_fast_when_project_toml_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "good-proj",
            "--name",
            "Good",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    // Delete project.toml to simulate corruption
    fs::remove_file(
        temp_dir
            .path()
            .join(".ralph-burning/projects/good-proj/project.toml"),
    )
    .expect("remove project.toml");

    let output = Command::new(binary())
        .args(["project", "list"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project list");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
    assert!(stderr.contains("missing"));
}

#[test]
fn project_delete_fails_fast_when_project_toml_is_missing() {
    let temp_dir = initialize_workspace_fixture();

    // Create a bare directory without project.toml (simulates corruption)
    let corrupt_dir = temp_dir.path().join(".ralph-burning/projects/bare-proj");
    fs::create_dir_all(&corrupt_dir).expect("create bare project dir");

    let output = Command::new(binary())
        .args(["project", "delete", "bare-proj"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project delete");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
    assert!(stderr.contains("missing"));
}

// ── Terminal snapshot status reporting ──

#[test]
fn run_status_reports_completed_for_terminal_run_snapshot() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "terminal",
            "--name",
            "Terminal",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "terminal"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Write a completed terminal snapshot (no active_run)
    let completed_snapshot = r#"{
  "active_run": null,
  "status": "completed",
  "cycle_history": [],
  "completion_rounds": 3,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "completed after 3 rounds"
}"#;
    fs::write(
        temp_dir
            .path()
            .join(".ralph-burning/projects/terminal/run.json"),
        completed_snapshot,
    )
    .expect("write completed snapshot");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Status: completed"));
    assert!(stdout.contains("completed after 3 rounds"));
}

#[test]
fn run_status_fails_for_semantically_inconsistent_snapshot() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "inconsist",
            "--name",
            "Inconsistent",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "inconsist"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Write a semantically inconsistent snapshot: running with no active_run
    let bad_snapshot = r#"{
  "active_run": null,
  "status": "running",
  "cycle_history": [],
  "completion_rounds": 0,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "running"
}"#;
    fs::write(
        temp_dir
            .path()
            .join(".ralph-burning/projects/inconsist/run.json"),
        bad_snapshot,
    )
    .expect("write inconsistent snapshot");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("run.json"));
    assert!(stderr.contains("inconsistent"));
}

#[test]
fn project_delete_fails_for_semantically_inconsistent_active_run() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "bad-state",
            "--name",
            "Bad State",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    // Write a semantically inconsistent snapshot: paused with an active_run
    let bad_snapshot = r#"{
  "active_run": {
    "run_id": "run-bad-state",
    "stage_cursor": {
      "stage": "planning",
      "cycle": 1,
      "attempt": 1,
      "completion_round": 1
    },
    "started_at": "2026-03-11T19:00:00Z"
  },
  "status": "paused",
  "cycle_history": [],
  "completion_rounds": 0,
  "rollback_point_meta": { "last_rollback_id": null, "rollback_count": 0 },
  "amendment_queue": { "pending": [], "processed_count": 0 },
  "status_summary": "paused"
}"#;
    fs::write(
        temp_dir
            .path()
            .join(".ralph-burning/projects/bad-state/run.json"),
        bad_snapshot,
    )
    .expect("write inconsistent snapshot");

    let output = Command::new(binary())
        .args(["project", "delete", "bad-state"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project delete");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("run.json"));
    assert!(stderr.contains("inconsistent"));
}

// ── Active-project canonical validation (corrupt project.toml) ──

#[test]
fn run_status_fails_fast_when_active_project_toml_is_corrupt() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "corrupt-active",
            "--name",
            "Corrupt Active",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "corrupt-active"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Corrupt project.toml content (file exists but is malformed)
    fs::write(
        temp_dir
            .path()
            .join(".ralph-burning/projects/corrupt-active/project.toml"),
        "this is {{ not valid toml",
    )
    .expect("corrupt project.toml");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
}

#[test]
fn run_history_fails_fast_when_active_project_toml_is_corrupt() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "corrupt-hist",
            "--name",
            "Corrupt Hist",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "corrupt-hist"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Corrupt project.toml content
    fs::write(
        temp_dir
            .path()
            .join(".ralph-burning/projects/corrupt-hist/project.toml"),
        "not valid toml {{{",
    )
    .expect("corrupt project.toml");

    let output = Command::new(binary())
        .args(["run", "history"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
}

#[test]
fn run_tail_fails_fast_when_active_project_toml_is_corrupt() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "corrupt-tail",
            "--name",
            "Corrupt Tail",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "corrupt-tail"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Corrupt project.toml content
    fs::write(
        temp_dir
            .path()
            .join(".ralph-burning/projects/corrupt-tail/project.toml"),
        "{invalid toml}",
    )
    .expect("corrupt project.toml");

    let output = Command::new(binary())
        .args(["run", "tail"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run tail");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
}

#[test]
fn project_show_no_id_fails_fast_when_active_project_toml_is_corrupt() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "corrupt-show",
            "--name",
            "Corrupt Show",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "corrupt-show"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Corrupt project.toml content
    fs::write(
        temp_dir
            .path()
            .join(".ralph-burning/projects/corrupt-show/project.toml"),
        "garbled content }{{}",
    )
    .expect("corrupt project.toml");

    let output = Command::new(binary())
        .args(["project", "show"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
}

#[test]
fn run_status_fails_fast_when_active_project_toml_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "missing-toml",
            "--name",
            "Missing Toml",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "missing-toml"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Delete project.toml
    fs::remove_file(
        temp_dir
            .path()
            .join(".ralph-burning/projects/missing-toml/project.toml"),
    )
    .expect("remove project.toml");

    let output = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("project.toml"));
    assert!(stderr.contains("missing"));
}

// ── Run.json schema completeness ──

#[test]
fn project_create_run_json_contains_all_canonical_fields() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "schema",
            "--name",
            "Schema Check",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    let run_json = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/schema/run.json"),
    )
    .expect("read run.json");

    assert!(run_json.contains("\"cycle_history\""));
    assert!(run_json.contains("\"completion_rounds\""));
    assert!(run_json.contains("\"rollback_point_meta\""));
    assert!(run_json.contains("\"amendment_queue\""));
    assert!(run_json.contains("\"active_run\""));
    assert!(run_json.contains("\"status\""));
    assert!(run_json.contains("\"status_summary\""));
}

// ── Regression: project select rejects schema-invalid project.toml ──

#[test]
fn project_select_rejects_syntactically_valid_but_schema_invalid_project_toml() {
    let temp_dir = initialize_workspace_fixture();

    // Create a project directory with a syntactically valid TOML that is missing
    // required canonical fields (only has 'id', no name/flow/prompt_reference/etc.)
    let project_root = temp_dir.path().join(".ralph-burning/projects/partial");
    fs::create_dir_all(&project_root).expect("create project directory");
    fs::write(project_root.join("project.toml"), "id = \"partial\"\n")
        .expect("write incomplete project.toml");

    let output = Command::new(binary())
        .args(["project", "select", "partial"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project select");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("project.toml"),
        "error should reference project.toml, got: {stderr}"
    );
    assert!(
        stderr.contains("invalid canonical structure") || stderr.contains("corrupt"),
        "error should indicate structural invalidity, got: {stderr}"
    );
}

// ── Regression: delete transactional with active-project pointer ──

#[test]
fn project_delete_clears_active_pointer_transactionally() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    // Create and select a project
    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "txn-del",
            "--name",
            "Txn Delete",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "txn-del"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Verify it's the active project
    let active = fs::read_to_string(temp_dir.path().join(".ralph-burning/active-project"))
        .expect("read active-project");
    assert_eq!(active, "txn-del");

    // Delete the project
    let output = Command::new(binary())
        .args(["project", "delete", "txn-del"])
        .current_dir(temp_dir.path())
        .output()
        .expect("delete project");
    assert!(output.status.success());

    // Active-project pointer should be cleared
    assert!(
        !temp_dir
            .path()
            .join(".ralph-burning/active-project")
            .exists(),
        "active-project pointer should be cleared after delete"
    );

    // Project directory should be gone
    assert!(
        !temp_dir
            .path()
            .join(".ralph-burning/projects/txn-del")
            .exists(),
        "project directory should be removed"
    );
}

#[test]
fn empty_journal_fails_fast_on_project_show() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    // Truncate journal to empty
    fs::write(
        temp_dir
            .path()
            .join(".ralph-burning/projects/alpha/journal.ndjson"),
        "",
    )
    .expect("truncate journal");

    let output = Command::new(binary())
        .args(["project", "show", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project show");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("journal.ndjson"),
        "error should reference journal.ndjson, got: {stderr}"
    );
    assert!(
        stderr.contains("empty"),
        "error should mention empty journal, got: {stderr}"
    );
}

#[test]
fn empty_journal_fails_fast_on_run_history() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    // Select and truncate journal
    Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    fs::write(
        temp_dir
            .path()
            .join(".ralph-burning/projects/alpha/journal.ndjson"),
        "",
    )
    .expect("truncate journal");

    let output = Command::new(binary())
        .args(["run", "history"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run history");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("journal.ndjson"),
        "error should reference journal.ndjson, got: {stderr}"
    );
}

#[test]
fn empty_journal_fails_fast_on_run_tail() {
    let temp_dir = initialize_workspace_fixture();
    create_project_fixture(temp_dir.path(), "alpha");

    Command::new(binary())
        .args(["project", "select", "alpha"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    fs::write(
        temp_dir
            .path()
            .join(".ralph-burning/projects/alpha/journal.ndjson"),
        "",
    )
    .expect("truncate journal");

    let output = Command::new(binary())
        .args(["run", "tail"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run tail");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("journal.ndjson"),
        "error should reference journal.ndjson, got: {stderr}"
    );
}

#[test]
fn delete_with_unremovable_active_pointer_restores_project() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    // Create and select a project
    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "restore-me",
            "--name",
            "Restore Me",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "restore-me"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    // Replace active-project file with a directory to make remove_file fail
    let ap_path = temp_dir.path().join(".ralph-burning/active-project");
    fs::remove_file(&ap_path).expect("remove active-project file");
    fs::create_dir_all(ap_path.join("blocker")).expect("create blocking dir");

    // Attempt delete — should fail because clearing the pointer fails
    let output = Command::new(binary())
        .args(["project", "delete", "restore-me"])
        .current_dir(temp_dir.path())
        .output()
        .expect("delete project");

    assert!(
        !output.status.success(),
        "delete should fail when pointer clear fails"
    );

    // Project must still be addressable at its canonical path
    assert!(
        temp_dir
            .path()
            .join(".ralph-burning/projects/restore-me/project.toml")
            .exists(),
        "project should be restored after failed pointer clear"
    );
}

// ── Run Start ──

fn setup_project(temp_dir: &tempfile::TempDir, project_id: &str, flow: &str) {
    let prompt = write_prompt_fixture(temp_dir.path());
    let create = Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            project_id,
            "--name",
            &format!("Test {project_id}"),
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            flow,
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");
    assert!(
        create.status.success(),
        "setup create failed: {}",
        String::from_utf8_lossy(&create.stderr)
    );

    let select = Command::new(binary())
        .args(["project", "select", project_id])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");
    assert!(select.status.success());
}

fn setup_standard_project(temp_dir: &tempfile::TempDir, project_id: &str) {
    setup_project(temp_dir, project_id, "standard");
}

#[test]
fn run_start_completes_standard_flow_end_to_end() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-e2e");

    let output = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        output.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Starting run for project"));
    assert!(stdout.contains("Run completed successfully"));
}

#[test]
fn run_start_completes_docs_change_flow_end_to_end() {
    let temp_dir = initialize_workspace_fixture();
    setup_project(&temp_dir, "docs-run", "docs_change");

    let output = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        output.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let payload_files: Vec<_> = fs::read_dir(
        temp_dir
            .path()
            .join(".ralph-burning/projects/docs-run/history/payloads"),
    )
    .expect("read payloads dir")
    .filter_map(|e| e.ok())
    .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
    .collect();
    assert_eq!(payload_files.len(), 4);

    let journal = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/docs-run/journal.ndjson"),
    )
    .expect("read journal");
    assert!(journal.contains("\"docs_plan\""));
    assert!(journal.contains("\"docs_update\""));
    assert!(journal.contains("\"docs_validation\""));
}

#[test]
fn run_start_completes_ci_improvement_flow_end_to_end() {
    let temp_dir = initialize_workspace_fixture();
    setup_project(&temp_dir, "ci-run", "ci_improvement");

    let output = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        output.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let payload_files: Vec<_> = fs::read_dir(
        temp_dir
            .path()
            .join(".ralph-burning/projects/ci-run/history/payloads"),
    )
    .expect("read payloads dir")
    .filter_map(|e| e.ok())
    .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
    .collect();
    assert_eq!(payload_files.len(), 4);

    let journal = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/ci-run/journal.ndjson"),
    )
    .expect("read journal");
    assert!(journal.contains("\"ci_plan\""));
    assert!(journal.contains("\"ci_update\""));
    assert!(journal.contains("\"ci_validation\""));
}

#[test]
fn run_start_produces_completed_snapshot() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-snap");

    let start = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(
        start.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );

    // Verify run.json shows completed
    let run_json = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/run-snap/run.json"),
    )
    .expect("read run.json");
    assert!(
        run_json.contains("\"completed\""),
        "run.json should contain completed status, got: {run_json}"
    );
    assert!(
        run_json.contains("\"active_run\":null") || run_json.contains("\"active_run\": null"),
        "active_run should be null after completion"
    );
}

#[test]
fn run_start_persists_journal_events() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-journal");

    let start = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(start.status.success());

    let journal = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/run-journal/journal.ndjson"),
    )
    .expect("read journal");

    // Should have project_created + run_started + stage events + run_completed
    assert!(
        journal.contains("\"run_started\""),
        "journal should contain run_started"
    );
    assert!(
        journal.contains("\"stage_entered\""),
        "journal should contain stage_entered"
    );
    assert!(
        journal.contains("\"stage_completed\""),
        "journal should contain stage_completed"
    );
    assert!(
        journal.contains("\"run_completed\""),
        "journal should contain run_completed"
    );
}

#[test]
fn run_start_persists_payload_and_artifact_records() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-artifacts");

    let start = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(start.status.success());

    let payloads_dir = temp_dir
        .path()
        .join(".ralph-burning/projects/run-artifacts/history/payloads");
    let artifacts_dir = temp_dir
        .path()
        .join(".ralph-burning/projects/run-artifacts/history/artifacts");

    // Standard flow has 8 stages, each producing a payload + artifact
    let payload_files: Vec<_> = fs::read_dir(&payloads_dir)
        .expect("read payloads dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();
    let artifact_files: Vec<_> = fs::read_dir(&artifacts_dir)
        .expect("read artifacts dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .collect();

    assert_eq!(
        payload_files.len(),
        8,
        "expected 8 payload files for standard flow, got {}",
        payload_files.len()
    );
    assert_eq!(
        artifact_files.len(),
        8,
        "expected 8 artifact files for standard flow, got {}",
        artifact_files.len()
    );
}

#[test]
fn run_start_status_shows_completed_after_run() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-status-after");

    let start = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(start.status.success());

    let status = Command::new(binary())
        .args(["run", "status"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run status");

    assert!(status.status.success());
    let stdout = String::from_utf8_lossy(&status.stdout);
    assert!(
        stdout.contains("Status: completed"),
        "run status should show completed after successful run, got: {stdout}"
    );
}

#[test]
fn run_start_rejects_quick_dev_flow() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project",
            "create",
            "--id",
            "quickdev",
            "--name",
            "Quick Dev",
            "--prompt",
            prompt.to_str().unwrap(),
            "--flow",
            "quick_dev",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    Command::new(binary())
        .args(["project", "select", "quickdev"])
        .current_dir(temp_dir.path())
        .output()
        .expect("select project");

    let output = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not yet supported"),
        "should reject quick_dev flow, got: {stderr}"
    );
}

#[test]
fn run_start_rejects_already_completed_project() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-dup");

    // First run should succeed
    let first = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("first run start");
    assert!(first.status.success());

    // Second run should fail because status is completed
    let second = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("second run start");

    assert!(!second.status.success());
    let stderr = String::from_utf8_lossy(&second.stderr);
    assert!(
        stderr.contains("not_started"),
        "should require not_started status, got: {stderr}"
    );
}

#[test]
fn run_start_rejects_already_running_project() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "run-running");

    // Write a running snapshot to simulate an active run
    let running_snapshot = r#"{"active_run":{"run_id":"run-test","stage_cursor":{"stage":"planning","cycle":1,"attempt":1,"completion_round":0},"started_at":"2026-03-11T19:00:00Z"},"status":"running","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"running: Planning"}"#;
    fs::write(
        temp_dir
            .path()
            .join(".ralph-burning/projects/run-running/run.json"),
        running_snapshot,
    )
    .expect("write running snapshot");

    let output = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("not_started"),
        "should require not_started status, got: {stderr}"
    );
}

#[test]
fn run_start_without_active_project_fails() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("active") || stderr.contains("no project"),
        "should require active project, got: {stderr}"
    );
}

#[test]
fn run_start_with_prompt_review_disabled_produces_seven_stages() {
    let temp_dir = initialize_workspace_fixture();

    // Disable prompt_review before creating the project
    let set_output = Command::new(binary())
        .args(["config", "set", "prompt_review.enabled", "false"])
        .current_dir(temp_dir.path())
        .output()
        .expect("config set");
    assert!(
        set_output.status.success(),
        "config set failed: {}",
        String::from_utf8_lossy(&set_output.stderr)
    );

    setup_standard_project(&temp_dir, "no-pr-cli");

    let start = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");
    assert!(
        start.status.success(),
        "run start failed: {}",
        String::from_utf8_lossy(&start.stderr)
    );

    // Verify 7 payloads (no prompt_review)
    let payloads_dir = temp_dir
        .path()
        .join(".ralph-burning/projects/no-pr-cli/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir)
        .expect("read payloads dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .count();
    assert_eq!(
        payload_count, 7,
        "expected 7 payloads without prompt_review, got {payload_count}"
    );

    // Verify no prompt_review stage in journal
    let journal = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/no-pr-cli/journal.ndjson"),
    )
    .expect("read journal");
    assert!(
        !journal.contains("\"prompt_review\""),
        "journal should not contain prompt_review stage when disabled"
    );

    // Verify completed status
    let run_json = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/no-pr-cli/run.json"),
    )
    .expect("read run.json");
    assert!(
        run_json.contains("\"completed\""),
        "run should be completed, got: {run_json}"
    );
}

#[test]
fn run_start_preflight_failure_leaves_state_unchanged() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "preflight-cli");

    // Corrupt the run.json to simulate a state that would fail validation
    // before the engine can proceed. We test that the CLI properly handles
    // preflight-like errors with no state mutation.
    //
    // The stub backend always passes preflight, so we verify the no-mutation
    // invariant via the workspace-version validation path: an unsupported
    // workspace version must leave all project state unchanged.
    let ws_toml_path = temp_dir.path().join(".ralph-burning/workspace.toml");
    let ws_toml = fs::read_to_string(&ws_toml_path).expect("read workspace.toml");
    let corrupted = ws_toml.replace("version = 1", "version = 999");
    fs::write(&ws_toml_path, corrupted).expect("write corrupted workspace.toml");

    // Capture pre-run state
    let pre_run_json = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/preflight-cli/run.json"),
    )
    .expect("read run.json before");
    let pre_journal = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/preflight-cli/journal.ndjson"),
    )
    .expect("read journal before");

    let output = Command::new(binary())
        .args(["run", "start"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        !output.status.success(),
        "run start should fail with bad workspace version"
    );

    // Verify NO state mutation occurred
    let post_run_json = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/preflight-cli/run.json"),
    )
    .expect("read run.json after");
    let post_journal = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/preflight-cli/journal.ndjson"),
    )
    .expect("read journal after");

    assert_eq!(
        pre_run_json, post_run_json,
        "run.json must not change on pre-engine failure"
    );
    assert_eq!(
        pre_journal, post_journal,
        "journal must not change on pre-engine failure"
    );

    let payloads_dir = temp_dir
        .path()
        .join(".ralph-burning/projects/preflight-cli/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir)
        .expect("read payloads dir")
        .filter_map(|e| e.ok())
        .count();
    assert_eq!(
        payload_count, 0,
        "no payloads should exist after preflight failure"
    );
}

#[test]
fn run_start_backend_preflight_failure_leaves_state_unchanged() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "preflight-backend");

    // Capture pre-run state
    let pre_run_json = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/preflight-backend/run.json"),
    )
    .expect("read run.json before");
    let pre_journal = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/preflight-backend/journal.ndjson"),
    )
    .expect("read journal before");

    // Use env var to make the backend unavailable at preflight
    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_TEST_BACKEND_UNAVAILABLE", "1")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        !output.status.success(),
        "run start should fail with unavailable backend"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("preflight") || stderr.contains("unavailable"),
        "error should reference preflight or unavailable, got: {stderr}"
    );

    // Verify NO state mutation occurred — byte-identical
    let post_run_json = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/preflight-backend/run.json"),
    )
    .expect("read run.json after");
    let post_journal = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/preflight-backend/journal.ndjson"),
    )
    .expect("read journal after");

    assert_eq!(
        pre_run_json, post_run_json,
        "run.json must be byte-identical after preflight failure"
    );
    assert_eq!(
        pre_journal, post_journal,
        "journal must be byte-identical after preflight failure"
    );

    let payloads_dir = temp_dir
        .path()
        .join(".ralph-burning/projects/preflight-backend/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir)
        .expect("read payloads dir")
        .filter_map(|e| e.ok())
        .count();
    assert_eq!(
        payload_count, 0,
        "no payloads should exist after preflight failure"
    );

    let artifacts_dir = temp_dir
        .path()
        .join(".ralph-burning/projects/preflight-backend/history/artifacts");
    let artifact_count = fs::read_dir(&artifacts_dir)
        .expect("read artifacts dir")
        .filter_map(|e| e.ok())
        .count();
    assert_eq!(
        artifact_count, 0,
        "no artifacts should exist after preflight failure"
    );
}

#[test]
fn run_start_mid_stage_failure_no_partial_durable_history() {
    let temp_dir = initialize_workspace_fixture();
    setup_standard_project(&temp_dir, "midstage-fail");

    // Use env var to fail the first stage's invocation (prompt_review is
    // enabled by default, so it's the first stage executed).
    let output = Command::new(binary())
        .args(["run", "start"])
        .env("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "prompt_review")
        .current_dir(temp_dir.path())
        .output()
        .expect("run start");

    assert!(
        !output.status.success(),
        "run start should fail on mid-stage invoke failure"
    );

    // Run snapshot must be failed, not running
    let run_json = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/midstage-fail/run.json"),
    )
    .expect("read run.json");
    assert!(
        run_json.contains("\"failed\""),
        "run.json should show failed status, got: {run_json}"
    );
    assert!(
        run_json.contains("\"active_run\":null") || run_json.contains("\"active_run\": null"),
        "active_run should be null after failure"
    );

    // No payload or artifact files should exist — no partial durable history
    let payloads_dir = temp_dir
        .path()
        .join(".ralph-burning/projects/midstage-fail/history/payloads");
    let payload_count = fs::read_dir(&payloads_dir)
        .expect("read payloads dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .count();
    assert_eq!(
        payload_count, 0,
        "no payloads should exist after mid-stage failure"
    );

    let artifacts_dir = temp_dir
        .path()
        .join(".ralph-burning/projects/midstage-fail/history/artifacts");
    let artifact_count = fs::read_dir(&artifacts_dir)
        .expect("read artifacts dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "json"))
        .count();
    assert_eq!(
        artifact_count, 0,
        "no artifacts should exist after mid-stage failure"
    );

    // No stage_completed event should exist in the journal
    let journal = fs::read_to_string(
        temp_dir
            .path()
            .join(".ralph-burning/projects/midstage-fail/journal.ndjson"),
    )
    .expect("read journal");
    assert!(
        !journal.contains("\"stage_completed\""),
        "no stage_completed event should exist after mid-stage failure"
    );

    // Journal should end with run_failed event
    let last_line = journal.lines().last().expect("journal should not be empty");
    assert!(
        last_line.contains("\"run_failed\""),
        "last journal event should be run_failed, got: {last_line}"
    );
}

// ── Requirements CLI tests ──────────────────────────────────────────────────

#[test]
fn requirements_quick_creates_completed_run() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["requirements", "quick", "--idea", "Build a REST API"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements quick");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "requirements quick should succeed.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("Requirements completed"),
        "stdout should contain completion message.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("ralph-burning project create"),
        "stdout should contain suggested create command.\nstdout: {stdout}"
    );

    // Verify run directory exists
    let req_dir = temp_dir.path().join(".ralph-burning/requirements");
    assert!(req_dir.is_dir(), "requirements directory should exist");
    let entries: Vec<_> = fs::read_dir(&req_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .collect();
    assert_eq!(entries.len(), 1, "should have exactly one requirements run");

    // Regression: verify the required file layout includes answers.toml and answers.json
    let run_dir = entries[0].path();
    assert!(
        run_dir.join("answers.toml").exists(),
        "quick run must have answers.toml"
    );
    assert!(
        run_dir.join("answers.json").exists(),
        "quick run must have answers.json"
    );
    assert!(
        run_dir.join("journal.ndjson").exists(),
        "quick run must have journal.ndjson"
    );
    assert!(
        run_dir.join("run.json").exists(),
        "quick run must have run.json"
    );
}

#[test]
fn requirements_show_displays_completed_run() {
    let temp_dir = initialize_workspace_fixture();

    // First create a quick run
    let output = Command::new(binary())
        .args(["requirements", "quick", "--idea", "Build a REST API"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements quick");
    assert!(output.status.success());

    // Find the run ID from the requirements directory
    let req_dir = temp_dir.path().join(".ralph-burning/requirements");
    let run_id = fs::read_dir(&req_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
        .map(|e| e.file_name().to_string_lossy().to_string())
        .next()
        .expect("should have one run");

    // Now show the run
    let output = Command::new(binary())
        .args(["requirements", "show", &run_id])
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements show");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "requirements show should succeed.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("Status:           completed"),
        "should show completed status.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("Mode:             quick"),
        "should show quick mode.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("Recommended Flow: standard"),
        "should show recommended flow.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("Seed Prompt:"),
        "should show seed prompt path.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("Suggested command:"),
        "should show suggested create command.\nstdout: {stdout}"
    );
}

#[test]
fn requirements_draft_with_empty_questions_completes() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["requirements", "draft", "--idea", "Simple refactoring"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements draft");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Default stub returns empty question set, so draft should complete
    assert!(
        output.status.success(),
        "requirements draft should succeed with empty questions.\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("Requirements completed"),
        "should complete through pipeline.\nstdout: {stdout}"
    );
}

#[test]
fn requirements_show_on_nonexistent_run_fails() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["requirements", "show", "nonexistent-run"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements show");

    assert!(
        !output.status.success(),
        "requirements show should fail for nonexistent run"
    );
}

#[test]
fn requirements_answer_happy_path_completes_run() {
    let temp_dir = initialize_workspace_fixture();
    let run_id = "req-20260312-120000";
    let run_dir = temp_dir
        .path()
        .join(".ralph-burning/requirements")
        .join(run_id);

    // Create required directory structure
    for subdir in &[
        "",
        "history/payloads",
        "history/artifacts",
        "seed",
        "runtime/logs",
        "runtime/backend",
        "runtime/temp",
    ] {
        fs::create_dir_all(run_dir.join(subdir)).expect("create subdir");
    }

    // Write sessions.json (PersistedSessions requires { sessions: [] })
    fs::write(run_dir.join("sessions.json"), r#"{"sessions":[]}"#).expect("write sessions");

    // Write run.json in awaiting_answers state
    let run_json = serde_json::json!({
        "run_id": run_id,
        "idea": "Build a REST API",
        "mode": "draft",
        "status": "awaiting_answers",
        "question_round": 1,
        "latest_question_set_id": format!("{run_id}-qs-1"),
        "latest_draft_id": null,
        "latest_review_id": null,
        "latest_seed_id": null,
        "pending_question_count": 1,
        "created_at": "2026-03-12T12:00:00Z",
        "updated_at": "2026-03-12T12:00:00Z",
        "status_summary": "awaiting answers: 1 question(s), round 1"
    });
    fs::write(
        run_dir.join("run.json"),
        serde_json::to_string_pretty(&run_json).unwrap(),
    )
    .expect("write run.json");

    // Write question set payload
    let qs_payload = serde_json::json!({
        "questions": [
            {
                "id": "q1",
                "prompt": "What framework?",
                "rationale": "Determines architecture",
                "required": true
            }
        ]
    });
    fs::write(
        run_dir.join(format!("history/payloads/{run_id}-qs-1.json")),
        serde_json::to_string(&qs_payload).unwrap(),
    )
    .expect("write question payload");

    // Write journal with RunCreated and QuestionsGenerated
    let journal = format!(
        "{}\n{}\n",
        serde_json::json!({
            "sequence": 1,
            "timestamp": "2026-03-12T12:00:00Z",
            "event_type": "run_created",
            "details": { "run_id": run_id, "status": "drafting", "status_summary": "drafting" }
        }),
        serde_json::json!({
            "sequence": 2,
            "timestamp": "2026-03-12T12:00:00Z",
            "event_type": "questions_generated",
            "details": { "run_id": run_id, "status": "awaiting_answers", "status_summary": "awaiting answers" }
        }),
    );
    fs::write(run_dir.join("journal.ndjson"), journal).expect("write journal");

    // Write valid answers.toml
    fs::write(run_dir.join("answers.toml"), "q1 = \"Use Actix Web\"\n")
        .expect("write answers.toml");

    // Run requirements answer with EDITOR=true (no-op editor)
    let output = Command::new(binary())
        .args(["requirements", "answer", run_id])
        .env("EDITOR", "true")
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements answer");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "requirements answer should succeed.\nstdout: {stdout}\nstderr: {stderr}"
    );

    // Verify run completed
    let run_data: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(run_dir.join("run.json")).expect("read run.json"))
            .expect("parse run.json");
    assert_eq!(
        run_data["status"], "completed",
        "run should be completed after answer"
    );

    // Verify seed files exist
    assert!(
        run_dir.join("seed/project.json").exists(),
        "seed/project.json should exist"
    );
    assert!(
        run_dir.join("seed/prompt.md").exists(),
        "seed/prompt.md should exist"
    );
}

#[test]
fn requirements_answer_on_nonexistent_run_fails() {
    let temp_dir = initialize_workspace_fixture();

    let output = Command::new(binary())
        .args(["requirements", "answer", "nonexistent-run"])
        .current_dir(temp_dir.path())
        .output()
        .expect("run requirements answer");

    assert!(
        !output.status.success(),
        "requirements answer should fail for nonexistent run"
    );
}
