use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

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
    fs::write(project_root.join("project.toml"), "id = \"fixture\"\n").expect("write project");
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
            "project", "create",
            "--id", "alpha",
            "--name", "Alpha Project",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    assert!(output.status.success(), "stderr: {}", String::from_utf8_lossy(&output.stderr));

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
            "project", "create",
            "--id", "beta",
            "--name", "Beta",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "quick_dev",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");
    assert!(output.status.success());

    let journal = fs::read_to_string(
        temp_dir.path().join(".ralph-burning/projects/beta/journal.ndjson"),
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
            "project", "create",
            "--id", "gamma",
            "--name", "Gamma",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("run project create");

    let run_json = fs::read_to_string(
        temp_dir.path().join(".ralph-burning/projects/gamma/run.json"),
    )
    .expect("read run.json");

    assert!(run_json.contains("\"not_started\""));
    assert!(run_json.contains("\"active_run\": null"));
}

#[test]
fn project_create_fails_on_duplicate_id() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    let first = Command::new(binary())
        .args([
            "project", "create",
            "--id", "dup",
            "--name", "First",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("first create");
    assert!(first.status.success());

    let second = Command::new(binary())
        .args([
            "project", "create",
            "--id", "dup",
            "--name", "Second",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
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
            "project", "create",
            "--id", "bad-flow",
            "--name", "Bad Flow",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "nonexistent",
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
            "project", "create",
            "--id", "no-prompt",
            "--name", "No Prompt",
            "--prompt", "/nonexistent/prompt.md",
            "--flow", "standard",
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
            "project", "create",
            "--id", "noactive",
            "--name", "No Active",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
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
            "project", "create",
            "--id", "alpha",
            "--name", "Alpha",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create alpha");

    Command::new(binary())
        .args([
            "project", "create",
            "--id", "beta",
            "--name", "Beta",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "quick_dev",
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
            "project", "create",
            "--id", "showme",
            "--name", "Show Me",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "docs_change",
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
            "project", "create",
            "--id", "active-show",
            "--name", "Active Show",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
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
            "project", "create",
            "--id", "deleteme",
            "--name", "Delete Me",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
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
            "project", "create",
            "--id", "active-del",
            "--name", "Active Del",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
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
            "project", "create",
            "--id", "status-test",
            "--name", "Status Test",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
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
            "project", "create",
            "--id", "hist-test",
            "--name", "History Test",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
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
            "project", "create",
            "--id", "tail-test",
            "--name", "Tail Test",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
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
            "project", "create",
            "--id", "tail-logs",
            "--name", "Tail Logs",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
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

// ── Fail-fast on missing canonical files ──

#[test]
fn run_status_fails_fast_when_run_json_is_missing() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project", "create",
            "--id", "broken",
            "--name", "Broken",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
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
        temp_dir.path().join(".ralph-burning/projects/broken/run.json"),
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
            "project", "create",
            "--id", "nojrnl",
            "--name", "No Journal",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
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
        temp_dir.path().join(".ralph-burning/projects/nojrnl/journal.ndjson"),
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
            "project", "create",
            "--id", "corrupt",
            "--name", "Corrupt",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
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
        temp_dir.path().join(".ralph-burning/projects/corrupt/run.json"),
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

// ── Run.json schema completeness ──

#[test]
fn project_create_run_json_contains_all_canonical_fields() {
    let temp_dir = initialize_workspace_fixture();
    let prompt = write_prompt_fixture(temp_dir.path());

    Command::new(binary())
        .args([
            "project", "create",
            "--id", "schema",
            "--name", "Schema Check",
            "--prompt", prompt.to_str().unwrap(),
            "--flow", "standard",
        ])
        .current_dir(temp_dir.path())
        .output()
        .expect("create project");

    let run_json = fs::read_to_string(
        temp_dir.path().join(".ralph-burning/projects/schema/run.json"),
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
