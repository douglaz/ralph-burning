use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use ralph_burning::test_support::fixtures::TempWorkspaceBuilder;
use tempfile::tempdir;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_ralph-burning")
}

fn initialize_workspace_fixture() -> tempfile::TempDir {
    TempWorkspaceBuilder::new()
        .build()
        .expect("workspace fixture")
        .into_temp_dir()
}

fn live_workspace_root(base_dir: &std::path::Path) -> std::path::PathBuf {
    ralph_burning::adapters::fs::FileSystem::live_workspace_root_path(base_dir)
}

fn project_root(base_dir: &std::path::Path, project_id: &str) -> std::path::PathBuf {
    live_workspace_root(base_dir)
        .join("projects")
        .join(project_id)
}

fn create_project_fixture(base_dir: &std::path::Path, project_id: &str) {
    let project_root = project_root(base_dir, project_id);
    fs::create_dir_all(&project_root).expect("create project directory");
    let prompt_contents = "# Fixture prompt\n";
    let project_toml = format!(
        r#"id = "{project_id}"
name = "Fixture {project_id}"
flow = "standard"
prompt_reference = "prompt.md"
prompt_hash = "{}"
created_at = "2026-03-11T19:00:00Z"
status_summary = "created"
"#,
        ralph_burning::adapters::fs::FileSystem::prompt_hash(prompt_contents)
    );
    fs::write(project_root.join("project.toml"), project_toml).expect("write project");
    fs::write(project_root.join("prompt.md"), prompt_contents).expect("write prompt");
    fs::write(
        project_root.join("run.json"),
        r#"{"active_run":null,"status":"not_started","cycle_history":[],"completion_rounds":0,"rollback_point_meta":{"last_rollback_id":null,"rollback_count":0},"amendment_queue":{"pending":[],"processed_count":0},"status_summary":"not started"}"#,
    )
    .expect("write run.json");
    fs::write(
        project_root.join("journal.ndjson"),
        format!(
            r#"{{"sequence":1,"timestamp":"2026-03-11T19:00:00Z","event_type":"project_created","details":{{"project_id":"{}","flow":"standard"}}}}"#,
            project_id
        ),
    )
    .expect("write journal");
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

fn select_active_project_fixture(base_dir: &std::path::Path, project_id: &str) {
    fs::write(
        live_workspace_root(base_dir).join("active-project"),
        format!("{project_id}\n"),
    )
    .expect("write active project");
}

fn write_executable(path: &std::path::Path, contents: &str) {
    fs::write(path, contents).expect("write executable");
    let mut permissions = fs::metadata(path).expect("stat executable").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("chmod executable");
}

fn write_fake_tmux(bin_dir: &std::path::Path, expected_session: &str) {
    write_executable(
        &bin_dir.join("tmux"),
        &format!(
            r#"#!/usr/bin/env bash
set -eu
cmd="$1"
shift
case "$cmd" in
  has-session)
    if [ "$2" = "{expected_session}" ]; then
      exit 0
    fi
    exit 1
    ;;
  attach-session)
    if [ "$2" = "{expected_session}" ]; then
      printf 'attached:%s\n' "$2"
      exit 0
    fi
    exit 1
    ;;
  *)
    exit 1
    ;;
esac
"#
        ),
    );
}

#[test]
fn run_attach_uses_recorded_active_tmux_session_even_when_current_config_is_direct() {
    let workspace = initialize_workspace_fixture();
    create_project_fixture(workspace.path(), "alpha");
    select_active_project_fixture(workspace.path(), "alpha");

    let recorded_session = "rb-alpha-run-1-final_review-reviewer-member-a-c1-a1-cr1";
    let active_session = serde_json::json!({
        "invocation_id": "run-1-final_review-reviewer-member-a-c1-a1-cr1",
        "session_name": recorded_session,
        "recorded_at": "2026-03-19T09:00:00Z"
    });
    fs::write(
        project_root(workspace.path(), "alpha").join("runtime/active-tmux-session.json"),
        serde_json::to_string_pretty(&active_session).expect("serialize active session"),
    )
    .expect("write active tmux session");

    let bin_dir = tempdir().expect("create bin dir");
    write_fake_tmux(bin_dir.path(), recorded_session);
    let path = format!(
        "{}:{}",
        bin_dir.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let output = Command::new(binary())
        .args(["run", "attach"])
        .current_dir(workspace.path())
        .env("PATH", path)
        .output()
        .expect("run attach");

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains(recorded_session),
        "attach should use the recorded session name: {stdout}"
    );
    assert!(
        stdout.contains(&format!("attached:{recorded_session}")),
        "attach should reach the tmux attach command: {stdout}"
    );
}
