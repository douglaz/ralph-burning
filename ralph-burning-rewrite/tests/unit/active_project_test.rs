use std::fs;

use ralph_burning::adapters::fs::FileSystem;
use ralph_burning::contexts::workspace_governance::{
    resolve_active_project, set_active_project, WORKSPACE_DIR,
};
use ralph_burning::shared::domain::ProjectId;
use ralph_burning::shared::error::AppError;
use tempfile::tempdir;

use super::workspace_test::initialize_workspace_fixture;

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

    let active_project = FileSystem::read_active_project(&temp_dir.path().join(WORKSPACE_DIR))
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
    fs::write(
        temp_dir.path().join(".ralph-burning/active-project"),
        "alpha\n",
    )
    .expect("write active project");

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

fn create_project_fixture(base_dir: &std::path::Path, project_id: &str) {
    let project_root = base_dir.join(".ralph-burning/projects").join(project_id);
    fs::create_dir_all(&project_root).expect("create project directory");
    fs::write(project_root.join("project.toml"), "id = \"fixture\"\n").expect("write project");
}
