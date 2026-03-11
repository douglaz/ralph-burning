use std::process::Command;

use tempfile::tempdir;

fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_ralph-burning")
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
