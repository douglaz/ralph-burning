use chrono::TimeZone;
use tempfile::tempdir;

use ralph_burning::adapters::fs::FileSystem;
use ralph_burning::contexts::agent_execution::policy::BackendPolicyService;
use ralph_burning::contexts::workspace_governance::config::{
    CliBackendOverrides, EffectiveConfig, DEFAULT_PROCESS_BACKEND_TIMEOUT_SECS,
};
use ralph_burning::shared::domain::{
    BackendFamily, BackendPolicyRole, BackendRoleTimeouts, BackendRuntimeSettings,
    BackendSelection, CompletionSettings, FlowPreset, PanelBackendSpec, ProjectConfig, ProjectId,
    WorkspaceConfig,
};
use ralph_burning::shared::error::AppError;

use super::workspace_test::initialize_workspace_fixture;

fn write_workspace_config(base_dir: &std::path::Path, config: &WorkspaceConfig) {
    let workspace_root = base_dir.join(".ralph-burning");
    FileSystem::write_atomic(
        &workspace_root.join("workspace.toml"),
        &toml::to_string_pretty(config).expect("serialize workspace config"),
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

fn test_timestamp() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc
        .with_ymd_and_hms(2026, 3, 16, 2, 10, 31)
        .single()
        .expect("valid timestamp")
}

fn empty_backend_settings(enabled: bool) -> BackendRuntimeSettings {
    BackendRuntimeSettings {
        enabled: Some(enabled),
        command: None,
        args: None,
        timeout_seconds: None,
        role_models: Default::default(),
        role_timeouts: Default::default(),
        extra: toml::Table::new(),
    }
}

#[test]
fn project_config_round_trips_role_timeouts_and_sections() {
    let mut config = ProjectConfig::default();
    config.settings.default_flow = Some(FlowPreset::Standard);
    config.prompt_review.enabled = Some(true);
    config.completion = CompletionSettings {
        backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            PanelBackendSpec::optional(BackendFamily::OpenRouter),
        ]),
        min_completers: Some(2),
        consensus_threshold: Some(0.66),
        extra: toml::Table::new(),
    };
    config.backends.insert(
        "claude".to_owned(),
        BackendRuntimeSettings {
            enabled: Some(true),
            command: Some("claude".to_owned()),
            args: Some(vec![]),
            timeout_seconds: Some(120),
            role_models: Default::default(),
            role_timeouts: BackendRoleTimeouts {
                planner: Some(90),
                implementer: None,
                reviewer: Some(60),
                qa: None,
                completer: None,
                final_reviewer: None,
                prompt_reviewer: None,
                prompt_validator: None,
                arbiter: None,
                acceptance_qa: None,
                extra: toml::Table::new(),
            },
            extra: toml::Table::new(),
        },
    );

    let serialized = toml::to_string_pretty(&config).expect("serialize project config");
    let parsed: ProjectConfig = toml::from_str(&serialized).expect("deserialize project config");

    assert_eq!(config, parsed);
}

#[test]
fn merge_precedence_cli_overrides_project_and_workspace() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.workflow.reviewer_backend = Some("claude".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let project_id = ProjectId::new("alpha").expect("project id");
    let mut project = ProjectConfig::default();
    project.workflow.reviewer_backend = Some("openrouter".to_owned());
    write_project_config(temp_dir.path(), &project_id, &project);

    let effective = EffectiveConfig::load_for_project(
        temp_dir.path(),
        Some(&project_id),
        CliBackendOverrides {
            reviewer_backend: Some(BackendSelection::new(BackendFamily::Codex, None)),
            ..Default::default()
        },
    )
    .expect("load config");

    let policy = BackendPolicyService::new(&effective);
    let target = policy
        .resolve_role_target(BackendPolicyRole::Reviewer, 1)
        .expect("resolve reviewer target");

    assert_eq!(BackendFamily::Codex, target.backend.family);
    assert!(matches!(
        effective
            .get("workflow.reviewer_backend")
            .expect("reviewer backend")
            .source,
        ralph_burning::contexts::workspace_governance::ConfigValueSource::CliOverride
    ));
}

#[test]
fn per_role_override_beats_default_backend_policy() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("claude".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let project_id = ProjectId::new("alpha").expect("project id");
    let mut project = ProjectConfig::default();
    project.workflow.reviewer_backend = Some("codex".to_owned());
    write_project_config(temp_dir.path(), &project_id, &project);

    let effective = EffectiveConfig::load_for_project(
        temp_dir.path(),
        Some(&project_id),
        CliBackendOverrides::default(),
    )
    .expect("load config");

    let target = BackendPolicyService::new(&effective)
        .resolve_role_target(BackendPolicyRole::Reviewer, 1)
        .expect("resolve reviewer");

    assert_eq!(BackendFamily::Codex, target.backend.family);
}

#[test]
fn completion_panel_includes_enabled_openrouter_and_rejects_required_disabled_backend() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(true));
    workspace.completion = CompletionSettings {
        backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            PanelBackendSpec::optional(BackendFamily::OpenRouter),
        ]),
        min_completers: Some(1),
        consensus_threshold: Some(0.66),
        extra: toml::Table::new(),
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let effective = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let panel = BackendPolicyService::new(&effective)
        .resolve_completion_panel(1)
        .expect("resolve completion panel");
    assert_eq!(2, panel.completers.len());
    assert_eq!(
        BackendFamily::Claude,
        panel.completers[0].target.backend.family
    );
    assert_eq!(
        BackendFamily::OpenRouter,
        panel.completers[1].target.backend.family
    );
    assert!(panel.completers[0].required);
    assert!(!panel.completers[1].required);

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    workspace.completion = CompletionSettings {
        backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            PanelBackendSpec::required(BackendFamily::OpenRouter),
        ]),
        min_completers: Some(2),
        consensus_threshold: Some(0.66),
        extra: toml::Table::new(),
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let effective = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let error = BackendPolicyService::new(&effective)
        .resolve_completion_panel(1)
        .expect_err("required disabled backend should fail");

    assert!(matches!(error, AppError::BackendUnavailable { .. }));
}

#[test]
fn disabled_default_backend_fails_role_resolution() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("openrouter".to_owned());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    write_workspace_config(temp_dir.path(), &workspace);

    let effective = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let error = BackendPolicyService::new(&effective)
        .resolve_role_target(BackendPolicyRole::Planner, 1)
        .expect_err("disabled default backend should fail");

    assert!(matches!(error, AppError::BackendUnavailable { .. }));
}

#[test]
fn completion_panel_defaults_to_opposite_family_when_backends_are_unset() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("claude".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let effective = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let policy = BackendPolicyService::new(&effective);

    let first_cycle = policy
        .resolve_completion_panel(1)
        .expect("resolve cycle one completion panel");
    assert_eq!(BackendFamily::Claude, first_cycle.planner.backend.family);
    assert_eq!(
        effective.completion_policy().min_completers,
        first_cycle.completers.len()
    );
    assert!(first_cycle
        .completers
        .iter()
        .all(|member| member.target.backend.family == BackendFamily::Codex));

    let second_cycle = policy
        .resolve_completion_panel(2)
        .expect("resolve cycle two completion panel");
    assert_eq!(BackendFamily::Codex, second_cycle.planner.backend.family);
    assert_eq!(
        effective.completion_policy().min_completers,
        second_cycle.completers.len()
    );
    assert!(second_cycle
        .completers
        .iter()
        .all(|member| member.target.backend.family == BackendFamily::Claude));
}

#[test]
fn final_review_panel_resolution_includes_planner_target() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("claude".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let effective = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let panel = BackendPolicyService::new(&effective)
        .resolve_final_review_panel(1)
        .expect("resolve final review panel");

    assert_eq!(BackendFamily::Claude, panel.planner.backend.family);
    assert!(
        !panel.reviewers.is_empty(),
        "final-review reviewers should resolve"
    );
    assert_eq!(BackendFamily::Claude, panel.arbiter.backend.family);
}

#[test]
fn opposite_family_uses_fallback_chain_and_cycle_alternates() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("claude".to_owned());
    workspace
        .backends
        .insert("codex".to_owned(), empty_backend_settings(false));
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(true));
    write_workspace_config(temp_dir.path(), &workspace);

    let effective = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let policy = BackendPolicyService::new(&effective);

    assert_eq!(
        BackendFamily::OpenRouter,
        policy
            .opposite_family(BackendFamily::Claude)
            .expect("fallback opposite")
    );
    assert_eq!(
        BackendFamily::Claude,
        policy
            .planner_family_for_cycle(1)
            .expect("odd cycle planner family")
    );
    assert_eq!(
        BackendFamily::OpenRouter,
        policy
            .planner_family_for_cycle(2)
            .expect("even cycle planner family")
    );

    let implementer = policy
        .resolve_role_target(BackendPolicyRole::Implementer, 1)
        .expect("implementer target");
    assert_eq!(BackendFamily::OpenRouter, implementer.backend.family);
}

#[test]
fn timeout_fallback_chain_prefers_role_timeout_then_backend_timeout_then_process_default() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.backends.insert(
        "claude".to_owned(),
        BackendRuntimeSettings {
            enabled: Some(true),
            command: Some("claude".to_owned()),
            args: Some(vec![]),
            timeout_seconds: Some(45),
            role_models: Default::default(),
            role_timeouts: BackendRoleTimeouts {
                planner: None,
                implementer: None,
                reviewer: Some(90),
                qa: None,
                completer: None,
                final_reviewer: None,
                prompt_reviewer: None,
                prompt_validator: None,
                arbiter: None,
                acceptance_qa: None,
                extra: toml::Table::new(),
            },
            extra: toml::Table::new(),
        },
    );
    write_workspace_config(temp_dir.path(), &workspace);

    let effective = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let policy = BackendPolicyService::new(&effective);

    assert_eq!(
        90,
        policy
            .timeout_for_role(BackendFamily::Claude, BackendPolicyRole::Reviewer)
            .as_secs()
    );
    assert_eq!(
        45,
        policy
            .timeout_for_role(BackendFamily::Claude, BackendPolicyRole::Planner)
            .as_secs()
    );
    assert_eq!(
        DEFAULT_PROCESS_BACKEND_TIMEOUT_SECS,
        policy
            .timeout_for_role(BackendFamily::Codex, BackendPolicyRole::Planner)
            .as_secs()
    );
}

// ── Panel probe semantics (Slice 5) ────────────────────────────────────────

#[test]
fn prompt_review_panel_resolution_includes_refiner_and_validators() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let effective = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let panel = BackendPolicyService::new(&effective)
        .resolve_prompt_review_panel(1)
        .expect("resolve prompt review panel");

    assert!(
        !panel.validators.is_empty(),
        "prompt review should resolve at least one validator"
    );
}

#[test]
fn backend_enabled_public_reflects_config() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(true));
    write_workspace_config(temp_dir.path(), &workspace);

    let effective = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let policy = BackendPolicyService::new(&effective);

    assert!(policy.backend_enabled_public(BackendFamily::Claude));
    assert!(policy.backend_enabled_public(BackendFamily::Codex));
    assert!(policy.backend_enabled_public(BackendFamily::OpenRouter));
    assert!(!policy.backend_enabled_public(BackendFamily::Stub));
}

#[test]
fn optional_panel_member_omission_does_not_fail_when_minimum_met() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    workspace.completion = CompletionSettings {
        backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            PanelBackendSpec::optional(BackendFamily::OpenRouter),
        ]),
        min_completers: Some(1),
        consensus_threshold: Some(0.66),
        extra: toml::Table::new(),
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let effective = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let panel = BackendPolicyService::new(&effective)
        .resolve_completion_panel(1)
        .expect("optional omission should not fail");

    assert_eq!(1, panel.completers.len());
    assert!(panel.completers[0].required);
}
