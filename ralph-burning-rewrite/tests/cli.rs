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
