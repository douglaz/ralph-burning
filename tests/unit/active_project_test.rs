use std::fs;

use ralph_burning::adapters::fs::FileSystem;
use ralph_burning::contexts::workspace_governance::{resolve_active_project, set_active_project};
use ralph_burning::shared::domain::ProjectId;
use ralph_burning::shared::error::AppError;
use tempfile::tempdir;

use super::workspace_test::{
    active_project_path, initialize_workspace_fixture, live_project_root, live_workspace_root,
};

#[test]
fn resolve_active_project_fails_when_pointer_is_missing() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let error = resolve_active_project(temp_dir.path()).expect_err("missing active project");

    assert!(matches!(error, AppError::NoActiveProject));
}

#[test]
fn set_active_project_writes_pointer_and_resolves_project() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());
    create_project_fixture(temp_dir.path(), "alpha");

    let project_id = ProjectId::new("alpha").expect("project id");
    set_active_project(temp_dir.path(), &project_id).expect("set active project");

    let active_project = FileSystem::read_active_project(&live_workspace_root(temp_dir.path()))
        .expect("read active project");
    assert_eq!(Some("alpha".to_owned()), active_project);

    let resolved = resolve_active_project(temp_dir.path()).expect("resolve active project");
    assert_eq!("alpha", resolved.as_str());
}

#[test]
fn resolve_active_project_trims_newlines() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());
    create_project_fixture(temp_dir.path(), "alpha");
    fs::write(active_project_path(temp_dir.path()), "alpha\n").expect("write active project");

    let resolved = resolve_active_project(temp_dir.path()).expect("resolve active project");

    assert_eq!("alpha", resolved.as_str());
}

#[test]
fn set_active_project_rejects_missing_projects() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());
    let project_id = ProjectId::new("missing").expect("project id");

    let error = set_active_project(temp_dir.path(), &project_id).expect_err("missing project");

    assert!(matches!(error, AppError::ProjectNotFound { .. }));
}

#[test]
fn resolve_active_project_rejects_path_like_pointer_values() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());
    fs::write(active_project_path(temp_dir.path()), "../escape\n").expect("write active project");

    let error =
        resolve_active_project(temp_dir.path()).expect_err("path-like active project should fail");

    assert!(matches!(error, AppError::InvalidIdentifier { .. }));
}

fn create_project_fixture(base_dir: &std::path::Path, project_id: &str) {
    let project_root = live_project_root(base_dir, project_id);
    fs::create_dir_all(&project_root).expect("create project directory");
    let prompt_contents = "# Fixture prompt\n";
    // Write a complete canonical ProjectRecord so validation passes
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
}
