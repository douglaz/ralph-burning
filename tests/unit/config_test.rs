use std::fs;

use chrono::TimeZone;
use ralph_burning::adapters::fs::FileSystem;
use ralph_burning::contexts::workspace_governance::config::{
    DEFAULT_EXISTING_BEAD_MATCH_THRESHOLD_SCORE,
    DEFAULT_ITERATIVE_MINIMAL_MAX_CONSECUTIVE_IMPLEMENTER_ROUNDS,
    DEFAULT_ITERATIVE_MINIMAL_STABLE_ROUNDS_REQUIRED, DEFAULT_PARSIMONIOUS_BEAD_CREATION_ENABLED,
};
use ralph_burning::contexts::workspace_governance::{
    ConfigValue, EffectiveConfig, DEFAULT_FLOW_PRESET, DEFAULT_PROMPT_REVIEW_ENABLED,
};
use ralph_burning::shared::domain::{
    FlowPreset, ProjectId, PromptReviewSettings, WorkspaceConfig, WorkspaceSettings,
};
use ralph_burning::shared::error::AppError;
use tempfile::tempdir;

use super::workspace_test::{initialize_workspace_fixture, live_project_root};

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
    assert_eq!(
        Some("codex".to_owned()),
        match config
            .get("workflow.implementer_backend")
            .expect("implementer backend")
            .value
        {
            ConfigValue::String(value) => value,
            other => panic!("expected string config value, got {other:?}"),
        }
    );
    assert_eq!(
        vec![
            "codex/gpt-5.5-xhigh".to_owned(),
            "?claude/claude-opus-4-7-max".to_owned(),
            "?codex/gpt-5.3-codex-spark-xhigh".to_owned(),
        ],
        match config
            .get("final_review.backends")
            .expect("final review backends")
            .value
        {
            ConfigValue::StringList(values) => values,
            other => panic!("expected string list config value, got {other:?}"),
        }
    );
    assert_eq!(
        Some("gpt-5.5-high".to_owned()),
        match config
            .get("backends.codex.role_models.implementer")
            .expect("codex implementer role model")
            .value
        {
            ConfigValue::String(value) => value,
            other => panic!("expected string config value, got {other:?}"),
        }
    );
    assert_eq!(
        Some("codex".to_owned()),
        match config
            .get("final_review.arbiter_backend")
            .expect("final review arbiter backend")
            .value
        {
            ConfigValue::String(value) => value,
            other => panic!("expected string config value, got {other:?}"),
        }
    );
    assert_eq!(
        Some("gpt-5.5-xhigh".to_owned()),
        match config
            .get("backends.codex.role_models.arbiter")
            .expect("codex arbiter role model")
            .value
        {
            ConfigValue::String(value) => value,
            other => panic!("expected string config value, got {other:?}"),
        }
    );
    assert_eq!(
        Some(1_u64),
        match config
            .get("final_review.min_reviewers")
            .expect("final review minimum reviewers")
            .value
        {
            ConfigValue::Integer(value) => Some(value),
            other => panic!("expected integer config value, got {other:?}"),
        }
    );
    assert_eq!(
        Some("gpt-5.5-xhigh".to_owned()),
        match config
            .get("backends.codex.role_models.final_reviewer")
            .expect("codex final reviewer role model")
            .value
        {
            ConfigValue::String(value) => value,
            other => panic!("expected string config value, got {other:?}"),
        }
    );
    assert!(matches!(
        config.get("default_flow").expect("default flow").source,
        ralph_burning::contexts::workspace_governance::ConfigValueSource::Default
    ));
    assert_eq!(
        ConfigValue::Integer(DEFAULT_ITERATIVE_MINIMAL_MAX_CONSECUTIVE_IMPLEMENTER_ROUNDS as u64),
        config
            .get("workflow.iterative_minimal.max_consecutive_implementer_rounds")
            .expect("iterative minimal max rounds")
            .value
    );
    assert_eq!(
        ConfigValue::Integer(DEFAULT_ITERATIVE_MINIMAL_STABLE_ROUNDS_REQUIRED as u64),
        config
            .get("workflow.iterative_minimal.stable_rounds_required")
            .expect("iterative minimal stable rounds")
            .value
    );
    assert_eq!(
        ConfigValue::Bool(DEFAULT_PARSIMONIOUS_BEAD_CREATION_ENABLED),
        config
            .get("workflow.parsimonious_bead_creation.enabled")
            .expect("parsimonious enabled")
            .value
    );
    assert_eq!(
        ConfigValue::Float(DEFAULT_EXISTING_BEAD_MATCH_THRESHOLD_SCORE),
        config
            .get("workflow.parsimonious_bead_creation.existing_bead_match_threshold_score")
            .expect("parsimonious existing match threshold")
            .value
    );
    assert_eq!(
        ConfigValue::Integer(2),
        config
            .get("workflow.parsimonious_bead_creation.proposal_threshold")
            .expect("parsimonious proposal threshold")
            .value
    );
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
        max_refinement_retries: None,
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
        max_refinement_retries: None,
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
fn config_set_updates_iterative_minimal_nested_values() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let max_entry = EffectiveConfig::set(
        temp_dir.path(),
        "workflow.iterative_minimal.max_consecutive_implementer_rounds",
        "7",
    )
    .expect("set iterative max rounds");
    assert_eq!(
        "workflow.iterative_minimal.max_consecutive_implementer_rounds",
        max_entry.key
    );
    assert_eq!("7", max_entry.value.display_value());

    let stable_entry = EffectiveConfig::set(
        temp_dir.path(),
        "workflow.iterative_minimal.stable_rounds_required",
        "3",
    )
    .expect("set iterative stable rounds");
    assert_eq!(
        "workflow.iterative_minimal.stable_rounds_required",
        stable_entry.key
    );
    assert_eq!("3", stable_entry.value.display_value());

    let reloaded = EffectiveConfig::load(temp_dir.path()).expect("reload effective config");
    assert_eq!(
        ConfigValue::Integer(7),
        reloaded
            .get("workflow.iterative_minimal.max_consecutive_implementer_rounds")
            .expect("reloaded iterative max rounds")
            .value
    );
    assert_eq!(
        ConfigValue::Integer(3),
        reloaded
            .get("workflow.iterative_minimal.stable_rounds_required")
            .expect("reloaded iterative stable rounds")
            .value
    );
}

#[test]
fn config_set_updates_new_bead_proposal_threshold() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let entry = EffectiveConfig::set(temp_dir.path(), "workflow.new_bead_proposal_threshold", "3")
        .expect("set proposal threshold");
    assert_eq!("workflow.new_bead_proposal_threshold", entry.key);
    assert_eq!("3", entry.value.display_value());

    let reloaded = EffectiveConfig::load(temp_dir.path()).expect("reload effective config");
    assert_eq!(
        ConfigValue::Integer(3),
        reloaded
            .get("workflow.new_bead_proposal_threshold")
            .expect("reloaded proposal threshold")
            .value
    );
}

#[test]
fn config_set_updates_parsimonious_bead_creation_settings() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let enabled = EffectiveConfig::set(
        temp_dir.path(),
        "workflow.parsimonious_bead_creation.enabled",
        "false",
    )
    .expect("set parsimonious enabled");
    assert_eq!("false", enabled.value.display_value());

    let score = EffectiveConfig::set(
        temp_dir.path(),
        "workflow.parsimonious_bead_creation.existing_bead_match_threshold_score",
        "0.7",
    )
    .expect("set parsimonious match threshold");
    assert_eq!("0.7", score.value.display_value());

    let threshold = EffectiveConfig::set(
        temp_dir.path(),
        "workflow.parsimonious_bead_creation.proposal_threshold",
        "3",
    )
    .expect("set parsimonious proposal threshold");
    assert_eq!("3", threshold.value.display_value());

    let reloaded = EffectiveConfig::load(temp_dir.path()).expect("reload effective config");
    assert_eq!(
        ConfigValue::Bool(false),
        reloaded
            .get("workflow.parsimonious_bead_creation.enabled")
            .expect("reloaded parsimonious enabled")
            .value
    );
    assert_eq!(
        ConfigValue::Float(0.7),
        reloaded
            .get("workflow.parsimonious_bead_creation.existing_bead_match_threshold_score")
            .expect("reloaded parsimonious match threshold")
            .value
    );
    assert_eq!(
        ConfigValue::Integer(3),
        reloaded
            .get("workflow.parsimonious_bead_creation.proposal_threshold")
            .expect("reloaded parsimonious proposal threshold")
            .value
    );
}

#[test]
fn config_set_rejects_zero_iterative_minimal_values() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let error = EffectiveConfig::set(
        temp_dir.path(),
        "workflow.iterative_minimal.max_consecutive_implementer_rounds",
        "0",
    )
    .expect_err("zero max rounds must be rejected");
    match error {
        AppError::InvalidConfigValue { key, value, reason } => {
            assert_eq!(
                key,
                "workflow.iterative_minimal.max_consecutive_implementer_rounds"
            );
            assert_eq!(value, "0");
            assert!(reason.contains("positive integer"));
        }
        other => panic!("expected InvalidConfigValue, got {other:?}"),
    }

    let error = EffectiveConfig::set(
        temp_dir.path(),
        "workflow.iterative_minimal.stable_rounds_required",
        "0",
    )
    .expect_err("zero stable rounds must be rejected");
    match error {
        AppError::InvalidConfigValue { key, value, reason } => {
            assert_eq!(key, "workflow.iterative_minimal.stable_rounds_required");
            assert_eq!(value, "0");
            assert!(reason.contains("positive integer"));
        }
        other => panic!("expected InvalidConfigValue, got {other:?}"),
    }
}

#[test]
fn config_set_rejects_iterative_minimal_stable_rounds_above_max_rounds() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    EffectiveConfig::set(
        temp_dir.path(),
        "workflow.iterative_minimal.max_consecutive_implementer_rounds",
        "5",
    )
    .expect("seed iterative max rounds");

    let error = EffectiveConfig::set(
        temp_dir.path(),
        "workflow.iterative_minimal.stable_rounds_required",
        "6",
    )
    .expect_err("stable rounds above max rounds must be rejected");
    match error {
        AppError::InvalidConfigValue { key, value, reason } => {
            assert_eq!(key, "workflow.iterative_minimal.stable_rounds_required");
            assert_eq!(value, "6");
            assert!(reason.contains("max_consecutive_implementer_rounds (5)"));
        }
        other => panic!("expected InvalidConfigValue, got {other:?}"),
    }
}

#[test]
fn config_set_project_rejects_cross_scope_iterative_minimal_policy() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());
    let project_id = ProjectId::new("cross-scope-project").expect("project id");

    EffectiveConfig::set(
        temp_dir.path(),
        "workflow.iterative_minimal.max_consecutive_implementer_rounds",
        "5",
    )
    .expect("seed workspace iterative max rounds");

    let error = EffectiveConfig::set_project(
        temp_dir.path(),
        &project_id,
        "workflow.iterative_minimal.stable_rounds_required",
        "6",
    )
    .expect_err("project override should be rejected when it exceeds the workspace max rounds");
    match error {
        AppError::InvalidConfigValue { key, value, reason } => {
            assert_eq!(key, "workflow.iterative_minimal.stable_rounds_required");
            assert_eq!(value, "6");
            assert!(reason.contains("max_consecutive_implementer_rounds (5)"));
        }
        other => panic!("expected InvalidConfigValue, got {other:?}"),
    }
}

#[test]
fn config_set_rejects_cross_scope_iterative_minimal_policy_against_existing_project_override() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());
    let project_id = ProjectId::new("cross-scope-project").expect("project id");

    EffectiveConfig::set_project(
        temp_dir.path(),
        &project_id,
        "workflow.iterative_minimal.stable_rounds_required",
        "6",
    )
    .expect("seed project iterative stable rounds");

    let error = EffectiveConfig::set(
        temp_dir.path(),
        "workflow.iterative_minimal.max_consecutive_implementer_rounds",
        "5",
    )
    .expect_err(
        "workspace edit should be rejected when it would invalidate an existing project override",
    );
    match error {
        AppError::InvalidConfigValue { key, value, reason } => {
            assert_eq!(key, "workflow.iterative_minimal.stable_rounds_required");
            assert_eq!(value, "6");
            assert!(reason.contains("max_consecutive_implementer_rounds (5)"));
        }
        other => panic!("expected InvalidConfigValue, got {other:?}"),
    }
}

#[test]
fn config_set_allows_unrelated_workspace_edits_when_project_config_is_malformed() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());
    let project_id = ProjectId::new("broken-project").expect("project id");

    FileSystem::write_project_config(temp_dir.path(), &project_id, &Default::default())
        .expect("seed project config");
    let project_config_path =
        live_project_root(temp_dir.path(), project_id.as_str()).join("config.toml");
    fs::write(
        &project_config_path,
        "[workflow.iterative_minimal\nstable_rounds_required = 2\n",
    )
    .expect("write malformed project config");

    let entry = EffectiveConfig::set(temp_dir.path(), "default_flow", "quick_dev")
        .expect("unrelated workspace config edit should ignore malformed project configs");
    assert_eq!("default_flow", entry.key);
    assert_eq!("quick_dev", entry.value.display_value());

    let reloaded = EffectiveConfig::load(temp_dir.path()).expect("reload workspace config");
    assert_eq!(FlowPreset::QuickDev, reloaded.default_flow());
}

#[test]
fn effective_config_load_rejects_zero_iterative_minimal_values_from_file() {
    let temp_dir = tempdir().expect("create temp dir");
    let workspace_root = initialize_workspace_fixture(temp_dir.path());
    EffectiveConfig::set(
        temp_dir.path(),
        "workflow.iterative_minimal.max_consecutive_implementer_rounds",
        "7",
    )
    .expect("seed iterative max rounds");
    let config_path = workspace_root.join("workspace.toml");
    let raw = fs::read_to_string(&config_path).expect("read workspace config");
    let updated = raw.replace(
        "max_consecutive_implementer_rounds = 7",
        "max_consecutive_implementer_rounds = 0",
    );
    fs::write(&config_path, updated).expect("write invalid workspace config");

    let error = EffectiveConfig::load(temp_dir.path()).expect_err("zero max rounds must fail");
    match error {
        AppError::InvalidConfigValue { key, value, reason } => {
            assert_eq!(
                key,
                "workflow.iterative_minimal.max_consecutive_implementer_rounds"
            );
            assert_eq!(value, "0");
            assert!(reason.contains("positive integer"));
        }
        other => panic!("expected InvalidConfigValue, got {other:?}"),
    }
}

#[test]
fn effective_config_load_rejects_iterative_minimal_stable_rounds_above_max_rounds() {
    let temp_dir = tempdir().expect("create temp dir");
    let workspace_root = initialize_workspace_fixture(temp_dir.path());
    EffectiveConfig::set(
        temp_dir.path(),
        "workflow.iterative_minimal.max_consecutive_implementer_rounds",
        "5",
    )
    .expect("seed iterative max rounds");
    EffectiveConfig::set(
        temp_dir.path(),
        "workflow.iterative_minimal.stable_rounds_required",
        "4",
    )
    .expect("seed iterative stable rounds");
    let config_path = workspace_root.join("workspace.toml");
    let raw = fs::read_to_string(&config_path).expect("read workspace config");
    let updated = raw.replace("stable_rounds_required = 4", "stable_rounds_required = 6");
    fs::write(&config_path, updated).expect("write invalid workspace config");

    let error = EffectiveConfig::load(temp_dir.path())
        .expect_err("stable rounds above max rounds must fail");
    match error {
        AppError::InvalidConfigValue { key, value, reason } => {
            assert_eq!(key, "workflow.iterative_minimal.stable_rounds_required");
            assert_eq!(value, "6");
            assert!(reason.contains("max_consecutive_implementer_rounds (5)"));
        }
        other => panic!("expected InvalidConfigValue, got {other:?}"),
    }
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

#[test]
fn config_set_accepts_panel_backend_model_overrides_and_displays_them() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let entry = EffectiveConfig::set(
        temp_dir.path(),
        "final_review.backends",
        r#"["codex/gpt-5.5-xhigh", "?codex/gpt-5.3-codex-spark-xhigh"]"#,
    )
    .expect("set final review backends");

    assert_eq!(
        r#"["codex/gpt-5.5-xhigh", "?codex/gpt-5.3-codex-spark-xhigh"]"#,
        entry.value.toml_like_value()
    );

    let loaded = EffectiveConfig::load(temp_dir.path()).expect("reload config");
    let fetched = loaded
        .get("final_review.backends")
        .expect("get final review backends");
    assert_eq!(
        r#"["codex/gpt-5.5-xhigh", "?codex/gpt-5.3-codex-spark-xhigh"]"#,
        fetched.value.toml_like_value()
    );
}

#[test]
fn config_set_accepts_legacy_parenthesized_panel_backend_model_overrides() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let entry = EffectiveConfig::set(
        temp_dir.path(),
        "final_review.backends",
        r#"["openrouter(openai/gpt-5.5)", "?openrouter(openai/gpt-5.5-mini)"]"#,
    )
    .expect("set final review backends");

    assert_eq!(
        r#"["openrouter/openai/gpt-5.5", "?openrouter/openai/gpt-5.5-mini"]"#,
        entry.value.toml_like_value()
    );

    let loaded = EffectiveConfig::load(temp_dir.path()).expect("reload config");
    let fetched = loaded
        .get("final_review.backends")
        .expect("get final review backends");
    assert_eq!(
        r#"["openrouter/openai/gpt-5.5", "?openrouter/openai/gpt-5.5-mini"]"#,
        fetched.value.toml_like_value()
    );
}

#[test]
fn config_set_rejects_invalid_panel_backend_model_overrides_before_writing() {
    let temp_dir = tempdir().expect("create temp dir");
    let workspace_root = initialize_workspace_fixture(temp_dir.path());
    let config_path = workspace_root.join("workspace.toml");
    let original = fs::read_to_string(&config_path).expect("read original config");

    let error = EffectiveConfig::set(temp_dir.path(), "final_review.backends", r#"["codex/"]"#)
        .expect_err("invalid panel backend override should fail");

    assert!(matches!(
        error,
        AppError::InvalidConfigValue { ref key, .. } if key == "final_review.backends"
    ));

    let after = fs::read_to_string(&config_path).expect("read config after failed set");
    assert_eq!(original, after);
}
