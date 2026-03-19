use chrono::TimeZone;
use tempfile::tempdir;

use ralph_burning::adapters::fs::FileSystem;
use ralph_burning::contexts::agent_execution::diagnostics::{
    BackendCheckFailureKind, BackendDiagnosticsService,
};
use ralph_burning::contexts::workspace_governance::config::{CliBackendOverrides, EffectiveConfig};
use ralph_burning::shared::domain::{
    BackendFamily, BackendRuntimeSettings, BackendSelection, CompletionSettings, FlowPreset,
    PanelBackendSpec, WorkspaceConfig,
};

use super::workspace_test::initialize_workspace_fixture;

fn write_workspace_config(base_dir: &std::path::Path, config: &WorkspaceConfig) {
    let workspace_root = base_dir.join(".ralph-burning");
    FileSystem::write_atomic(
        &workspace_root.join("workspace.toml"),
        &toml::to_string_pretty(config).expect("serialize workspace config"),
    )
    .expect("write workspace config");
}

fn test_timestamp() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc
        .with_ymd_and_hms(2026, 3, 19, 3, 28, 0)
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

// ── list tests ──────────────────────────────────────────────────────────────

#[test]
fn list_backends_returns_all_families() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let entries = service.list_backends();

    assert_eq!(4, entries.len());
    assert_eq!("claude", entries[0].family);
    assert!(entries[0].enabled);
    assert_eq!("codex", entries[1].family);
    assert!(entries[1].enabled);
    assert_eq!("openrouter", entries[2].family);
    assert!(!entries[2].enabled);
    assert_eq!("stub", entries[3].family);
    assert!(!entries[3].enabled);
    assert_eq!(Some(true), entries[3].compile_only);
}

#[test]
fn list_backends_reflects_enablement_config() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(true));
    workspace
        .backends
        .insert("codex".to_owned(), empty_backend_settings(false));
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let entries = service.list_backends();

    let codex = entries.iter().find(|e| e.family == "codex").unwrap();
    let openrouter = entries.iter().find(|e| e.family == "openrouter").unwrap();
    assert!(!codex.enabled);
    assert!(openrouter.enabled);
}

// ── check tests ─────────────────────────────────────────────────────────────

#[test]
fn check_passes_with_default_config() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service.check_backends(FlowPreset::Standard);

    assert!(result.passed, "default config should pass check: {:?}", result.failures);
}

#[test]
fn check_fails_when_base_backend_disabled() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("openrouter".to_owned());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service.check_backends(FlowPreset::Standard);

    assert!(!result.passed);
    assert!(
        result.failures.iter().any(|f| f.failure_kind == BackendCheckFailureKind::BackendDisabled),
        "expected at least one BackendDisabled failure: {:?}",
        result.failures
    );
}

#[test]
fn check_reports_required_panel_member_failure() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

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

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service.check_backends(FlowPreset::Standard);

    assert!(!result.passed);
    let panel_failure = result
        .failures
        .iter()
        .find(|f| f.role == "completion_panel")
        .expect("expected completion_panel failure");
    assert_eq!(
        BackendCheckFailureKind::RequiredMemberUnavailable,
        panel_failure.failure_kind
    );
}

// ── show-effective tests ────────────────────────────────────────────────────

#[test]
fn show_effective_contains_all_roles() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    assert!(!view.roles.is_empty());
    // Should have entries for all resolvable roles
    let role_names: Vec<&str> = view.roles.iter().map(|r| r.role.as_str()).collect();
    assert!(role_names.contains(&"planner"), "should contain planner");
    assert!(
        role_names.contains(&"implementer"),
        "should contain implementer"
    );
}

#[test]
fn show_effective_reports_source_precedence() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("codex".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    assert_eq!("workspace.toml", view.base_backend.source);
    assert!(view.base_backend.value.contains("codex"));
}

#[test]
fn show_effective_cli_override_source() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load_for_project(
        temp_dir.path(),
        None,
        CliBackendOverrides {
            backend: Some(BackendSelection::new(BackendFamily::Codex, None)),
            ..Default::default()
        },
    )
    .expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    assert_eq!("cli override", view.base_backend.source);
}

// ── probe tests ─────────────────────────────────────────────────────────────

#[test]
fn probe_singular_role() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service
        .probe("planner", FlowPreset::Standard, 1)
        .expect("probe planner");

    assert_eq!("planner", result.role);
    assert_eq!("standard", result.flow);
    assert_eq!(1, result.cycle);
    assert!(result.panel.is_none());
    assert!(!result.target.backend_family.is_empty());
}

#[test]
fn probe_completion_panel() {
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

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service
        .probe("completion_panel", FlowPreset::Standard, 1)
        .expect("probe completion panel");

    assert_eq!("completion_panel", result.role);
    let panel = result.panel.expect("should have panel view");
    assert_eq!("completion", panel.panel_type);
    assert_eq!(1, panel.minimum);
    assert_eq!(2, panel.resolved_count);
}

#[test]
fn probe_final_review_panel() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service
        .probe("final_review_panel", FlowPreset::Standard, 1)
        .expect("probe final review panel");

    assert_eq!("final_review_panel", result.role);
    let panel = result.panel.expect("should have panel view");
    assert_eq!("final_review", panel.panel_type);
    assert!(!panel.members.is_empty());
}

#[test]
fn probe_optional_member_omitted_when_disabled() {
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

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service
        .probe("completion_panel", FlowPreset::Standard, 1)
        .expect("probe completion panel with omitted");

    let panel = result.panel.expect("should have panel view");
    assert_eq!(1, panel.resolved_count);
    assert_eq!(1, panel.omitted.len());
    assert!(panel.omitted[0].was_optional);
    assert_eq!("openrouter", panel.omitted[0].backend_family);
}

#[test]
fn probe_required_member_failure_is_exact() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

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

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service.probe("completion_panel", FlowPreset::Standard, 1);

    assert!(result.is_err(), "required disabled member should fail probe");
    let error = result.unwrap_err();
    let msg = error.to_string();
    assert!(
        msg.contains("openrouter") || msg.contains("unavailable"),
        "error should identify the failing backend: {}",
        msg
    );
}

// ── aggregation tests ───────────────────────────────────────────────────────

#[test]
fn check_aggregates_all_failures_in_one_run() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Disable the base backend
    workspace.settings.default_backend = Some("openrouter".to_owned());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service.check_backends(FlowPreset::Standard);

    assert!(!result.passed);
    // Should have multiple failures aggregated (base + various roles)
    assert!(
        result.failures.len() > 1,
        "expected multiple aggregated failures, got {}",
        result.failures.len()
    );
}
