use chrono::TimeZone;
use ralph_burning::adapters::fs::FileSystem;
use ralph_burning::contexts::workspace_governance::config::{
    CliBackendOverrides, ConfigValue, ConfigValueSource, EffectiveConfig, DEFAULT_EXECUTION_MODE,
    DEFAULT_STREAM_OUTPUT,
};
use ralph_burning::shared::domain::{ExecutionMode, ProjectConfig, ProjectId, WorkspaceConfig};
use tempfile::tempdir;

use super::workspace_test::initialize_workspace_fixture;

fn test_timestamp() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc
        .with_ymd_and_hms(2026, 3, 19, 8, 30, 28)
        .single()
        .expect("valid timestamp")
}

fn write_workspace_config(base_dir: &std::path::Path, config: &WorkspaceConfig) {
    let path = initialize_workspace_fixture(base_dir).join("workspace.toml");
    FileSystem::write_atomic(
        &path,
        &toml::to_string_pretty(config).expect("serialize workspace"),
    )
    .expect("write workspace config");
}

fn write_project_config(
    base_dir: &std::path::Path,
    project_id: &ProjectId,
    config: &ProjectConfig,
) {
    FileSystem::write_project_config(base_dir, project_id, config).expect("write project config");
}

#[test]
fn execution_settings_default_to_direct_without_streaming() {
    let temp_dir = tempdir().expect("create temp dir");
    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let effective = EffectiveConfig::load(temp_dir.path()).expect("load config");

    assert_eq!(DEFAULT_EXECUTION_MODE, effective.effective_execution_mode());
    assert_eq!(DEFAULT_STREAM_OUTPUT, effective.effective_stream_output());
    assert!(matches!(
        effective
            .get("execution.mode")
            .expect("execution.mode")
            .source,
        ConfigValueSource::Default
    ));
    assert!(matches!(
        effective
            .get("execution.stream_output")
            .expect("execution.stream_output")
            .source,
        ConfigValueSource::Default
    ));
}

#[test]
fn execution_settings_follow_workspace_project_and_cli_precedence() {
    let temp_dir = tempdir().expect("create temp dir");
    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.execution.mode = Some(ExecutionMode::Tmux);
    workspace.execution.stream_output = Some(false);
    write_workspace_config(temp_dir.path(), &workspace);

    let project_id = ProjectId::new("alpha").expect("project id");
    let mut project = ProjectConfig::default();
    project.execution.mode = Some(ExecutionMode::Direct);
    project.execution.stream_output = Some(true);
    write_project_config(temp_dir.path(), &project_id, &project);

    let effective = EffectiveConfig::load_for_project(
        temp_dir.path(),
        Some(&project_id),
        CliBackendOverrides {
            execution_mode: Some(ExecutionMode::Tmux),
            stream_output: Some(false),
            ..Default::default()
        },
    )
    .expect("load config");

    assert_eq!(ExecutionMode::Tmux, effective.effective_execution_mode());
    assert!(!effective.effective_stream_output());
    assert!(matches!(
        effective
            .get("execution.mode")
            .expect("execution.mode")
            .source,
        ConfigValueSource::CliOverride
    ));
    assert!(matches!(
        effective
            .get("execution.stream_output")
            .expect("execution.stream_output")
            .source,
        ConfigValueSource::CliOverride
    ));
}

#[test]
fn execution_settings_round_trip_in_workspace_and_project_toml() {
    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.execution.mode = Some(ExecutionMode::Tmux);
    workspace.execution.stream_output = Some(true);

    let serialized = toml::to_string_pretty(&workspace).expect("serialize workspace");
    let reparsed: WorkspaceConfig = toml::from_str(&serialized).expect("deserialize workspace");
    assert_eq!(workspace, reparsed);

    let mut project = ProjectConfig::default();
    project.execution.mode = Some(ExecutionMode::Direct);
    project.execution.stream_output = Some(false);

    let serialized = toml::to_string_pretty(&project).expect("serialize project");
    let reparsed: ProjectConfig = toml::from_str(&serialized).expect("deserialize project");
    assert_eq!(project, reparsed);
}

#[test]
fn execution_config_entries_render_expected_values() {
    let temp_dir = tempdir().expect("create temp dir");
    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.execution.mode = Some(ExecutionMode::Tmux);
    workspace.execution.stream_output = Some(true);
    write_workspace_config(temp_dir.path(), &workspace);

    let effective = EffectiveConfig::load(temp_dir.path()).expect("load config");
    assert_eq!(
        ConfigValue::String(Some("tmux".to_owned())),
        effective
            .get("execution.mode")
            .expect("execution.mode")
            .value
    );
    assert_eq!(
        ConfigValue::Bool(true),
        effective
            .get("execution.stream_output")
            .expect("execution.stream_output")
            .value
    );
}
