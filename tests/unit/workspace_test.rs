use chrono::TimeZone;
use ralph_burning::adapters::fs::FileSystem;
use ralph_burning::contexts::workspace_governance::{
    initialize_workspace, load_workspace_config, REQUIRED_WORKSPACE_DIRECTORIES,
};
use ralph_burning::shared::domain::{WorkspaceConfig, CURRENT_WORKSPACE_VERSION};
use std::path::{Path, PathBuf};
use tempfile::tempdir;

pub(crate) fn audit_workspace_root(base_dir: &Path) -> PathBuf {
    FileSystem::audit_workspace_root_path(base_dir)
}

pub(crate) fn live_workspace_root(base_dir: &Path) -> PathBuf {
    FileSystem::live_workspace_root_path(base_dir)
}

pub(crate) fn active_project_path(base_dir: &Path) -> PathBuf {
    live_workspace_root(base_dir).join("active-project")
}

pub(crate) fn live_project_root(base_dir: &Path, project_id: &str) -> PathBuf {
    live_workspace_root(base_dir)
        .join("projects")
        .join(project_id)
}

#[test]
fn workspace_config_serialization_round_trip() {
    let created_at = chrono::Utc
        .with_ymd_and_hms(2026, 3, 11, 17, 50, 55)
        .single()
        .expect("valid timestamp");
    let config = WorkspaceConfig::new(created_at);

    let serialized = toml::to_string_pretty(&config).expect("serialize config");
    let parsed: WorkspaceConfig = toml::from_str(&serialized).expect("deserialize config");

    assert_eq!(config, parsed);
    assert_eq!(CURRENT_WORKSPACE_VERSION, parsed.version);
}

#[test]
fn initialize_workspace_creates_required_directory_structure_and_config() {
    let temp_dir = tempdir().expect("create temp dir");
    let created_at = chrono::Utc
        .with_ymd_and_hms(2026, 3, 11, 17, 50, 55)
        .single()
        .expect("valid timestamp");

    let result = initialize_workspace(temp_dir.path(), created_at).expect("initialize workspace");

    assert_eq!(live_workspace_root(temp_dir.path()), result.workspace_root);
    for required_directory in REQUIRED_WORKSPACE_DIRECTORIES {
        assert!(result.workspace_root.join(required_directory).is_dir());
        assert!(audit_workspace_root(temp_dir.path())
            .join(required_directory)
            .is_dir());
    }
    assert!(audit_workspace_root(temp_dir.path())
        .join("workspace.toml")
        .is_file());

    let config = load_workspace_config(temp_dir.path()).expect("load workspace config");
    assert_eq!(created_at, config.created_at);
    assert_eq!(CURRENT_WORKSPACE_VERSION, config.version);
}

#[test]
fn initialize_workspace_fails_when_workspace_already_exists() {
    let temp_dir = tempdir().expect("create temp dir");
    let created_at = chrono::Utc
        .with_ymd_and_hms(2026, 3, 11, 17, 50, 55)
        .single()
        .expect("valid timestamp");

    initialize_workspace(temp_dir.path(), created_at).expect("initial initialization succeeds");
    let second_attempt = initialize_workspace(temp_dir.path(), created_at);

    assert!(second_attempt.is_err());
}

#[test]
fn initialize_workspace_repairs_legacy_audit_only_workspace() {
    let temp_dir = tempdir().expect("create temp dir");
    let created_at = chrono::Utc
        .with_ymd_and_hms(2026, 3, 11, 17, 50, 55)
        .single()
        .expect("valid timestamp");
    let audit_root = audit_workspace_root(temp_dir.path());
    std::fs::create_dir_all(audit_root.join("projects/legacy-project"))
        .expect("create legacy project directory");
    std::fs::write(
        audit_root.join("projects/legacy-project/project.toml"),
        "id = \"legacy-project\"\nname = \"Legacy\"\nflow = \"minimal\"\nprompt_reference = \"prompt.md\"\nprompt_hash = \"hash\"\ncreated_at = \"2026-03-11T17:50:55Z\"\nstatus_summary = \"created\"\n",
    )
    .expect("write legacy project config");

    let result = initialize_workspace(temp_dir.path(), created_at).expect("repair workspace");

    assert_eq!(live_workspace_root(temp_dir.path()), result.workspace_root);
    assert!(audit_root.join("workspace.toml").is_file());
    assert!(live_workspace_root(temp_dir.path())
        .join("workspace.toml")
        .is_file());
    assert!(live_workspace_root(temp_dir.path())
        .join("projects/legacy-project/project.toml")
        .is_file());
}

pub(crate) fn initialize_workspace_fixture(base_dir: &Path) -> PathBuf {
    let created_at = chrono::Utc
        .with_ymd_and_hms(2026, 3, 11, 17, 50, 55)
        .single()
        .expect("valid timestamp");
    initialize_workspace(base_dir, created_at)
        .expect("initialize workspace")
        .workspace_root
}
