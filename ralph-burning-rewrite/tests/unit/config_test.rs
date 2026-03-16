use std::fs;

use chrono::TimeZone;
use ralph_burning::adapters::fs::FileSystem;
use ralph_burning::contexts::workspace_governance::{
    EffectiveConfig, DEFAULT_FLOW_PRESET, DEFAULT_PROMPT_REVIEW_ENABLED,
};
use ralph_burning::shared::domain::{
    FlowPreset, PromptReviewSettings, WorkspaceConfig, WorkspaceSettings,
};
use ralph_burning::shared::error::AppError;
use tempfile::tempdir;

use super::workspace_test::initialize_workspace_fixture;

#[test]
fn effective_config_loads_compiled_defaults() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let config = EffectiveConfig::load(temp_dir.path()).expect("load effective config");

    assert_eq!(
        DEFAULT_PROMPT_REVIEW_ENABLED,
        config.prompt_review_enabled()
    );
    assert_eq!(DEFAULT_FLOW_PRESET, config.default_flow());
    assert_eq!(None, config.default_backend());
    assert_eq!(None, config.default_model());
    assert_eq!(
        Some("claude".to_owned()),
        match config
            .get("default_backend")
            .expect("default backend")
            .value
        {
            ralph_burning::contexts::workspace_governance::ConfigValue::String(value) => value,
            other => panic!("expected string config value, got {other:?}"),
        }
    );
    assert!(matches!(
        config.get("default_flow").expect("default flow").source,
        ralph_burning::contexts::workspace_governance::ConfigValueSource::Default
    ));
}

#[test]
fn effective_config_merges_workspace_overrides() {
    let temp_dir = tempdir().expect("create temp dir");
    let workspace_root = initialize_workspace_fixture(temp_dir.path());
    let workspace_config_path = workspace_root.join("workspace.toml");

    let created_at = chrono::Utc
        .with_ymd_and_hms(2026, 3, 11, 17, 50, 55)
        .single()
        .expect("valid timestamp");
    let mut config = WorkspaceConfig::new(created_at);
    config.prompt_review = PromptReviewSettings {
        enabled: Some(false),
        refiner_backend: None,
        validator_backends: None,
        min_reviewers: None,
        extra: toml::map::Map::new(),
    };
    config.settings = WorkspaceSettings {
        default_flow: Some(FlowPreset::QuickDev),
        default_backend: Some("claude".to_owned()),
        default_model: Some("opus".to_owned()),
        extra: toml::map::Map::new(),
    };
    FileSystem::write_atomic(
        &workspace_config_path,
        &toml::to_string_pretty(&config).expect("serialize config"),
    )
    .expect("write config");

    let effective = EffectiveConfig::load(temp_dir.path()).expect("load effective config");

    assert!(!effective.prompt_review_enabled());
    assert_eq!(FlowPreset::QuickDev, effective.default_flow());
    assert_eq!(Some("claude"), effective.default_backend());
    assert_eq!(Some("opus"), effective.default_model());
    assert!(matches!(
        effective
            .get("default_model")
            .expect("default model")
            .source,
        ralph_burning::contexts::workspace_governance::ConfigValueSource::WorkspaceToml
    ));
}

#[test]
fn workspace_config_round_trips_extended_settings() {
    let created_at = chrono::Utc
        .with_ymd_and_hms(2026, 3, 11, 17, 50, 55)
        .single()
        .expect("valid timestamp");
    let mut config = WorkspaceConfig::new(created_at);
    config.prompt_review = PromptReviewSettings {
        enabled: Some(false),
        refiner_backend: None,
        validator_backends: None,
        min_reviewers: None,
        extra: toml::map::Map::new(),
    };
    config.settings = WorkspaceSettings {
        default_flow: Some(FlowPreset::DocsChange),
        default_backend: Some("openrouter".to_owned()),
        default_model: Some("gpt-5".to_owned()),
        extra: toml::map::Map::new(),
    };

    let serialized = toml::to_string_pretty(&config).expect("serialize config");
    let parsed: WorkspaceConfig = toml::from_str(&serialized).expect("deserialize config");

    assert_eq!(config, parsed);
}

#[test]
fn config_set_preserves_unknown_settings_keys() {
    let temp_dir = tempdir().expect("create temp dir");
    let workspace_root = initialize_workspace_fixture(temp_dir.path());
    let config_path = workspace_root.join("workspace.toml");
    let raw = r#"
version = 1
created_at = "2026-03-11T17:50:55Z"

[settings]
default_backend = "claude"
future_toggle = "enabled"

[prompt_review]
enabled = false
owner = "ops"

[routing]
mode = "repo_default"
"#;
    fs::write(&config_path, raw).expect("write config");

    let entry = EffectiveConfig::set(temp_dir.path(), "default_flow", "quick_dev")
        .expect("update default flow");

    assert_eq!("default_flow", entry.key);
    assert_eq!("quick_dev", entry.value.display_value());

    let updated = fs::read_to_string(&config_path).expect("read updated config");
    assert!(updated.contains("future_toggle = \"enabled\""));
    assert!(updated.contains("owner = \"ops\""));
    assert!(updated.contains("[routing]"));
}

#[test]
fn workspace_config_preserves_unknown_keys_through_round_trip() {
    let raw = r#"
version = 1
created_at = "2026-03-11T17:50:55Z"

[settings]
default_backend = "claude"
future_toggle = "enabled"

[prompt_review]
enabled = true
owner = "ops"

[routing]
mode = "repo_default"
"#;

    let parsed: WorkspaceConfig = toml::from_str(raw).expect("deserialize config");
    let serialized = toml::to_string_pretty(&parsed).expect("serialize config");
    let reparsed: WorkspaceConfig = toml::from_str(&serialized).expect("deserialize config");

    assert_eq!(
        Some(&toml::Value::String("enabled".to_owned())),
        reparsed.settings.extra.get("future_toggle")
    );
    assert_eq!(
        Some(&toml::Value::String("ops".to_owned())),
        reparsed.prompt_review.extra.get("owner")
    );
    assert_eq!(
        Some(&toml::Value::String("repo_default".to_owned())),
        reparsed
            .extra
            .get("routing")
            .and_then(|value| value.as_table())
            .and_then(|table| table.get("mode"))
    );
}

#[test]
fn effective_config_rejects_unknown_keys_and_invalid_values() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let unknown_key = EffectiveConfig::set(temp_dir.path(), "unknown.key", "value")
        .expect_err("unknown key should fail");
    assert!(matches!(unknown_key, AppError::UnknownConfigKey { .. }));

    let invalid_flow = EffectiveConfig::set(temp_dir.path(), "default_flow", "unknown")
        .expect_err("invalid flow should fail");
    assert!(matches!(invalid_flow, AppError::InvalidConfigValue { .. }));

    let invalid_bool = EffectiveConfig::set(temp_dir.path(), "prompt_review.enabled", "yes")
        .expect_err("invalid bool should fail");
    assert!(matches!(invalid_bool, AppError::InvalidConfigValue { .. }));
}
