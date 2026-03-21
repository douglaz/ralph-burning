use chrono::TimeZone;
use ralph_burning::contexts::workspace_governance::{
    initialize_workspace, load_workspace_config, REQUIRED_WORKSPACE_DIRECTORIES, WORKSPACE_DIR,
};
use ralph_burning::shared::domain::{WorkspaceConfig, CURRENT_WORKSPACE_VERSION};
use std::path::{Path, PathBuf};
use tempfile::tempdir;

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

    assert_eq!(temp_dir.path().join(WORKSPACE_DIR), result.workspace_root);
    for required_directory in REQUIRED_WORKSPACE_DIRECTORIES {
        assert!(result.workspace_root.join(required_directory).is_dir());
    }

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

pub(crate) fn initialize_workspace_fixture(base_dir: &Path) -> PathBuf {
    let created_at = chrono::Utc
        .with_ymd_and_hms(2026, 3, 11, 17, 50, 55)
        .single()
        .expect("valid timestamp");
    initialize_workspace(base_dir, created_at)
        .expect("initialize workspace")
        .workspace_root
}
