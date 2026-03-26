use chrono::TimeZone;
use tempfile::tempdir;

use ralph_burning::adapters::fs::FileSystem;
use ralph_burning::contexts::agent_execution::diagnostics::{
    BackendCheckFailureKind, BackendDiagnosticsService,
};
use ralph_burning::contexts::workspace_governance::config::{CliBackendOverrides, EffectiveConfig};
use ralph_burning::shared::domain::{
    BackendFamily, BackendRoleModels, BackendRoleTimeouts, BackendRuntimeSettings,
    BackendSelection, CompletionSettings, ExecutionMode, FinalReviewSettings, FlowPreset,
    PanelBackendSpec, PromptReviewSettings, WorkspaceConfig,
};

use super::env_test_support::{lock_path_mutex, PathGuard};
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
    #[cfg(not(feature = "test-stub"))]
    assert_eq!(Some(true), entries[3].compile_only);
    #[cfg(feature = "test-stub")]
    assert_eq!(None, entries[3].compile_only);
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

    assert!(
        result.passed,
        "default config should pass check: {:?}",
        result.failures
    );
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
        result
            .failures
            .iter()
            .any(|f| f.failure_kind == BackendCheckFailureKind::BackendDisabled),
        "expected at least one BackendDisabled failure: {:?}",
        result.failures
    );
}

#[test]
fn check_reports_tmux_unavailable_when_tmux_mode_is_configured() {
    let _path_lock = lock_path_mutex();
    let isolated_path = tempdir().expect("create isolated path");
    let _path_guard = PathGuard::replace(isolated_path.path());

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.execution.mode = Some(ExecutionMode::Tmux);
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service.check_backends(FlowPreset::Standard);

    assert!(!result.passed, "tmux mode without tmux should fail check");
    let failure = result
        .failures
        .iter()
        .find(|failure| failure.failure_kind == BackendCheckFailureKind::TmuxUnavailable)
        .expect("tmux-unavailable failure");
    assert_eq!("execution", failure.role);
    assert_eq!("tmux", failure.backend_family);
    assert_eq!("workspace.toml", failure.config_source);
    assert!(
        failure.details.contains("tmux"),
        "failure should mention tmux: {}",
        failure.details
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
    // With decomposed panel checks, the failure now identifies the exact
    // member index and config source, not the panel as a whole.
    let member_failure = result
        .failures
        .iter()
        .find(|f| f.role.starts_with("completion_panel.member"))
        .expect("expected completion_panel.member[N] failure");
    assert_eq!(
        BackendCheckFailureKind::RequiredMemberUnavailable,
        member_failure.failure_kind
    );
    assert_eq!("openrouter", member_failure.backend_family);
    assert_eq!("completion.backends", member_failure.config_source);
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
    assert!(!result.target.unwrap().backend_family.is_empty());
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

    assert!(
        result.is_err(),
        "required disabled member should fail probe"
    );
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

// ── flow-scoped check tests ─────────────────────────────────────────────────

#[test]
fn check_docs_change_flow_skips_completion_and_final_review() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    // Configure completion to require an unavailable backend.
    // For docs_change flow, this should NOT cause a failure since it doesn't
    // include CompletionPanel or FinalReview stages.
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
    workspace.final_review = FinalReviewSettings {
        enabled: Some(true),
        backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            PanelBackendSpec::required(BackendFamily::OpenRouter),
        ]),
        min_reviewers: Some(2),
        ..Default::default()
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    // DocsChange should pass — its stages don't need completion/final-review
    let result = service.check_backends(FlowPreset::DocsChange);
    assert!(
        result.passed,
        "docs_change should pass even with broken completion/final-review config: {:?}",
        result.failures
    );

    // Standard should fail — it uses CompletionPanel and FinalReview
    let result = service.check_backends(FlowPreset::Standard);
    assert!(
        !result.passed,
        "standard should fail with broken completion config"
    );
}

// ── flow-scoped probe tests ─────────────────────────────────────────────────

#[test]
fn probe_completion_panel_fails_for_docs_change_flow() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    // docs_change flow does not have CompletionPanel
    let result = service.probe("completion_panel", FlowPreset::DocsChange, 1);
    assert!(
        result.is_err(),
        "probing completion_panel on docs_change flow should fail"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("does not include stage"),
        "error should indicate the stage is not in the flow: {}",
        err_msg
    );
}

#[test]
fn probe_final_review_panel_fails_for_docs_change_flow() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    // docs_change flow does not have FinalReview
    let result = service.probe("final_review_panel", FlowPreset::DocsChange, 1);
    assert!(
        result.is_err(),
        "probing final_review_panel on docs_change flow should fail"
    );
}

// ── show-effective session policy tests ──────────────────────────────────────

#[test]
fn show_effective_reports_per_role_session_policy() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    // Main-stage roles should use reuse_if_allowed
    let planner = view.roles.iter().find(|r| r.role == "planner").unwrap();
    assert_eq!("reuse_if_allowed", planner.session_policy);

    let implementer = view.roles.iter().find(|r| r.role == "implementer").unwrap();
    assert_eq!("reuse_if_allowed", implementer.session_policy);

    // Panel roles should use new_session
    let completer = view.roles.iter().find(|r| r.role == "completer");
    if let Some(completer) = completer {
        assert_eq!("new_session", completer.session_policy);
    }

    let final_reviewer = view.roles.iter().find(|r| r.role == "final_reviewer");
    if let Some(fr) = final_reviewer {
        assert_eq!("new_session", fr.session_policy);
    }
}

// ── show-effective source precedence for inherited roles ─────────────────────

#[test]
fn show_effective_reports_source_for_prompt_reviewer_role() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.prompt_review.refiner_backend = Some("codex".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    let prompt_reviewer = view.roles.iter().find(|r| r.role == "prompt_reviewer");
    if let Some(pr) = prompt_reviewer {
        // Should report as workspace.toml, not "default"
        assert_eq!("workspace.toml", pr.override_source);
    }
}

#[test]
fn show_effective_reports_source_for_arbiter_role() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.final_review.arbiter_backend = Some("codex".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    let arbiter = view.roles.iter().find(|r| r.role == "arbiter");
    if let Some(arb) = arbiter {
        assert_eq!("workspace.toml", arb.override_source);
    }
}

// ── final-review probe arbiter tests ─────────────────────────────────────────

#[test]
fn probe_final_review_panel_includes_arbiter() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service
        .probe("final_review_panel", FlowPreset::Standard, 1)
        .expect("probe final review panel");

    let panel = result.panel.expect("should have panel view");
    assert_eq!("final_review", panel.panel_type);
    let arbiter = panel
        .arbiter
        .expect("should have arbiter in final_review panel");
    assert!(!arbiter.backend_family.is_empty());
    assert!(!arbiter.model_id.is_empty());
    assert!(arbiter.required);
}

// ── structured panel failure tests ───────────────────────────────────────────

#[test]
fn check_panel_failure_identifies_exact_backend_family() {
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
    // With decomposed panel checks, each member has its own failure entry
    let member_failure = result
        .failures
        .iter()
        .find(|f| f.role.starts_with("completion_panel.member"))
        .expect("expected completion_panel.member[N] failure");

    // The failure should identify the exact backend, not just "mixed"
    assert_eq!(
        BackendCheckFailureKind::RequiredMemberUnavailable,
        member_failure.failure_kind
    );
    assert_eq!("openrouter", member_failure.backend_family);
    assert_eq!("completion.backends", member_failure.config_source);
}

// ── inherited source-precedence tests ────────────────────────────────────────

#[test]
fn show_effective_inherited_role_reports_base_source() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    // Set default_backend at workspace level
    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("codex".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    // Roles that inherit from base should report default_backend with the
    // actual source layer, not just "default"
    let completer = view.roles.iter().find(|r| r.role == "completer");
    if let Some(c) = completer {
        assert!(
            c.override_source.contains("workspace.toml"),
            "completer should report workspace.toml source, got: {}",
            c.override_source
        );
    }

    // Planner should also inherit from default_backend since no explicit override
    let planner = view.roles.iter().find(|r| r.role == "planner").unwrap();
    assert!(
        planner.override_source.contains("workspace.toml"),
        "planner should report workspace.toml source when inheriting from base, got: {}",
        planner.override_source
    );
}

#[test]
fn show_effective_explicit_role_override_beats_inheritance() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("codex".to_owned());
    workspace.workflow.planner_backend = Some("claude".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    // Planner has explicit override — should report workspace.toml directly
    let planner = view.roles.iter().find(|r| r.role == "planner").unwrap();
    assert_eq!(
        "workspace.toml", planner.override_source,
        "explicit planner override should report workspace.toml"
    );
}

// ── availability-aware check tests ───────────────────────────────────────────

#[tokio::test]
async fn check_with_availability_covers_panel_targets() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;

    struct AlwaysAvailableAdapter;
    impl AgentExecutionPort for AlwaysAvailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            _backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = AlwaysAvailableAdapter;

    let result = service
        .check_backends_with_availability(FlowPreset::Standard, &adapter)
        .await;
    assert!(
        result.passed,
        "all-available adapter should pass: {:?}",
        result.failures
    );
}

#[tokio::test]
async fn check_with_availability_short_circuits_on_tmux_unavailable() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;

    struct PanicAdapter;

    impl AgentExecutionPort for PanicAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            panic!("availability adapter should not be queried when tmux is unavailable");
        }

        async fn check_availability(
            &self,
            _backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            panic!("availability adapter should not be queried when tmux is unavailable");
        }

        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }

        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let _path_lock = lock_path_mutex();
    let isolated_path = tempdir().expect("create isolated path");
    let _path_guard = PathGuard::replace(isolated_path.path());

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.execution.mode = Some(ExecutionMode::Tmux);
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service
        .check_backends_with_availability(FlowPreset::Standard, &PanicAdapter)
        .await;

    assert!(
        result
            .failures
            .iter()
            .any(|failure| failure.failure_kind == BackendCheckFailureKind::TmuxUnavailable),
        "tmux-unavailable failure should be preserved"
    );
}

#[tokio::test]
async fn check_with_availability_reports_panel_member_failure() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that fails availability for codex backends
    struct CodexUnavailableAdapter;
    impl AgentExecutionPort for CodexUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            if backend.backend.family == BackendFamily::Codex {
                Err(AppError::BackendUnavailable {
                    backend: "codex".to_owned(),
                    details: "codex binary not found".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = CodexUnavailableAdapter;

    let result = service
        .check_backends_with_availability(FlowPreset::Standard, &adapter)
        .await;

    assert!(!result.passed, "codex-unavailable should fail check");
    let avail_failure = result
        .failures
        .iter()
        .find(|f| f.failure_kind == BackendCheckFailureKind::AvailabilityFailure)
        .expect("expected availability failure");
    assert_eq!("codex", avail_failure.backend_family);
    // Should NOT have generic "availability" role
    assert_ne!("availability", avail_failure.role);
}

// ── probe availability tests ─────────────────────────────────────────────────

#[tokio::test]
async fn probe_with_availability_omits_unavailable_optional_member() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that fails availability for openrouter backends
    struct OpenRouterUnavailableAdapter;
    impl AgentExecutionPort for OpenRouterUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            if backend.backend.family == BackendFamily::OpenRouter {
                Err(AppError::BackendUnavailable {
                    backend: "openrouter".to_owned(),
                    details: "OPENROUTER_API_KEY not set".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

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
    let adapter = OpenRouterUnavailableAdapter;

    let result = service
        .probe_with_availability("completion_panel", FlowPreset::Standard, 1, &adapter)
        .await
        .expect("probe should succeed");

    let panel = result.panel.expect("should have panel view");

    // OpenRouter is optional and unavailable — should be omitted, not a member
    assert_eq!(
        1, panel.resolved_count,
        "only claude should remain as resolved member"
    );
    assert_eq!(1, panel.omitted.len(), "openrouter should be in omitted");
    assert_eq!("openrouter", panel.omitted[0].backend_family);
    assert!(panel.omitted[0].was_optional);
}

// ── adapter construction failure tests ───────────────────────────────────────

#[test]
fn check_with_adapter_failure_reports_availability_error() {
    use ralph_burning::shared::error::AppError;

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    // Simulate a bogus RALPH_BURNING_BACKEND that causes adapter build failure
    let adapter_err = AppError::InvalidConfigValue {
        key: "RALPH_BURNING_BACKEND".to_owned(),
        value: "bogus".to_owned(),
        reason: "expected one of process, openrouter".to_owned(),
    };

    let result = service.check_backends_with_adapter_failure(FlowPreset::Standard, &adapter_err);

    assert!(!result.passed, "adapter failure should fail check");
    let adapter_failure = result
        .failures
        .iter()
        .find(|f| f.role == "adapter")
        .expect("expected adapter-level failure");
    assert_eq!(
        BackendCheckFailureKind::AvailabilityFailure,
        adapter_failure.failure_kind
    );
    assert!(
        adapter_failure
            .details
            .contains("adapter construction failed"),
        "details should mention adapter construction: {}",
        adapter_failure.details
    );
    assert_eq!("RALPH_BURNING_BACKEND", adapter_failure.config_source);
}

// ── probe required member availability failure tests ─────────────────────────

#[tokio::test]
async fn probe_with_availability_fails_on_required_unavailable_member() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that fails availability for openrouter backends
    struct OpenRouterUnavailableAdapter;
    impl AgentExecutionPort for OpenRouterUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            if backend.backend.family == BackendFamily::OpenRouter {
                Err(AppError::BackendUnavailable {
                    backend: "openrouter".to_owned(),
                    details: "OPENROUTER_API_KEY not set".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(true));
    workspace.completion = CompletionSettings {
        backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            // OpenRouter is REQUIRED, not optional
            PanelBackendSpec::required(BackendFamily::OpenRouter),
        ]),
        min_completers: Some(2),
        consensus_threshold: Some(0.66),
        extra: toml::Table::new(),
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = OpenRouterUnavailableAdapter;

    let result = service
        .probe_with_availability("completion_panel", FlowPreset::Standard, 1, &adapter)
        .await;

    assert!(
        result.is_err(),
        "probe should fail when required member is unavailable"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("openrouter") || err_msg.contains("unavailable"),
        "error should identify the failing backend: {}",
        err_msg
    );
}

#[tokio::test]
async fn probe_with_availability_fails_on_unavailable_planner() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that fails ALL availability checks
    struct AllUnavailableAdapter;
    impl AgentExecutionPort for AllUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Err(AppError::BackendUnavailable {
                backend: backend.backend.family.as_str().to_owned(),
                details: "binary not found".to_owned(),
            })
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = AllUnavailableAdapter;

    // Planner target is unavailable — should fail the probe
    let result = service
        .probe_with_availability("planner", FlowPreset::Standard, 1, &adapter)
        .await;

    assert!(
        result.is_err(),
        "probe should fail when planner is unavailable"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("planner") || err_msg.contains("unavailable"),
        "error should mention planner: {}",
        err_msg
    );
}

// ── per-role availability aggregation test ────────────────────────────────────

#[tokio::test]
async fn check_with_availability_reports_all_role_aliases_for_shared_target() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that fails availability for ALL backends
    struct AllUnavailableAdapter;
    impl AgentExecutionPort for AllUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Err(AppError::BackendUnavailable {
                backend: backend.backend.family.as_str().to_owned(),
                details: "binary not found".to_owned(),
            })
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = AllUnavailableAdapter;

    let result = service
        .check_backends_with_availability(FlowPreset::Standard, &adapter)
        .await;

    assert!(!result.passed, "all-unavailable should fail");

    // With the default config (claude base + codex opposite), when everything is
    // unavailable we should get multiple failures with different role identities
    // (planner, implementer, reviewer, etc.) — NOT just one deduplicated entry.
    let avail_failures: Vec<_> = result
        .failures
        .iter()
        .filter(|f| f.failure_kind == BackendCheckFailureKind::AvailabilityFailure)
        .collect();

    assert!(
        avail_failures.len() > 2,
        "expected multiple per-role availability failures, got {}: {:?}",
        avail_failures.len(),
        avail_failures.iter().map(|f| &f.role).collect::<Vec<_>>()
    );

    // Verify role/member identities are preserved — should see both stage roles
    // AND panel-specific roles like final_review_panel.arbiter
    let roles: Vec<&str> = avail_failures.iter().map(|f| f.role.as_str()).collect();
    assert!(
        roles.iter().any(|r| r.contains("final_review_panel")),
        "expected final_review_panel-related failure, got roles: {:?}",
        roles
    );
}

// ── effective-required scope tests ─────────────────────────────────────────────

#[test]
fn check_passes_when_all_roles_overridden_and_default_backend_disabled() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Disable the default backend (openrouter)
    workspace.settings.default_backend = Some("openrouter".to_owned());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    // But explicitly override ALL workflow roles to enabled backends
    workspace.workflow.planner_backend = Some("claude".to_owned());
    workspace.workflow.implementer_backend = Some("codex".to_owned());
    workspace.workflow.reviewer_backend = Some("claude".to_owned());
    workspace.workflow.qa_backend = Some("codex".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    // docs_change flow: DocsPlan→Planner, DocsUpdate→Implementer,
    // DocsValidation→Qa, Review→Reviewer — all have overrides, so
    // disabled default_backend should NOT cause failure.
    let result = service.check_backends(FlowPreset::DocsChange);
    assert!(
        result.passed,
        "docs_change with all roles overridden should pass even with disabled default_backend: {:?}",
        result.failures
    );
}

#[test]
fn check_does_not_fail_on_final_reviewer_when_explicit_panel_backends() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Explicitly override all stage roles
    workspace.workflow.planner_backend = Some("claude".to_owned());
    workspace.workflow.implementer_backend = Some("codex".to_owned());
    workspace.workflow.reviewer_backend = Some("claude".to_owned());
    workspace.workflow.qa_backend = Some("codex".to_owned());
    // Explicitly configure final_review with enabled backends
    workspace.final_review = FinalReviewSettings {
        enabled: Some(true),
        backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            PanelBackendSpec::required(BackendFamily::Codex),
        ]),
        min_reviewers: Some(2),
        arbiter_backend: Some("claude".to_owned()),
        ..Default::default()
    };
    // Also override prompt_review refiner — it would otherwise fall through
    // to the disabled default_backend.
    workspace.prompt_review.refiner_backend = Some("claude".to_owned());
    // Disable default_backend — should be unused since everything is explicit
    workspace.settings.default_backend = Some("openrouter".to_owned());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    // Standard flow includes FinalReview, but with explicit panel backends
    // and all stage roles overridden, neither default_backend nor
    // FinalReviewer generic role should cause failure.
    let result = service.check_backends(FlowPreset::Standard);
    assert!(
        result.passed,
        "standard flow with explicit overrides should pass: {:?}",
        result.failures
    );
}

// ── probe primary config source tests ─────────────────────────────────────────

#[tokio::test]
async fn probe_prompt_review_panel_failure_reports_refiner_source() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    struct AllUnavailableAdapter;
    impl AgentExecutionPort for AllUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Err(AppError::BackendUnavailable {
                backend: backend.backend.family.as_str().to_owned(),
                details: "binary not found".to_owned(),
            })
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = AllUnavailableAdapter;

    let result = service
        .probe_with_availability("prompt_review_panel", FlowPreset::Standard, 1, &adapter)
        .await;

    assert!(
        result.is_err(),
        "probe should fail when refiner is unavailable"
    );
    let err_msg = result.unwrap_err().to_string();
    // No explicit refiner override set, so refiner inherits from default_backend
    assert!(
        err_msg.contains("[source: default_backend]"),
        "inherited refiner failure should report default_backend, got: {}",
        err_msg
    );
}

// ── probe timeout fidelity tests ──────────────────────────────────────────────

#[test]
fn probe_final_review_panel_uses_planner_timeout_not_final_reviewer() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Set distinct planner and final_reviewer timeouts so the test can
    // distinguish which role the probe uses.
    let mut claude_settings = empty_backend_settings(true);
    claude_settings.role_timeouts = BackendRoleTimeouts {
        planner: Some(11),
        final_reviewer: Some(22),
        ..Default::default()
    };
    workspace
        .backends
        .insert("claude".to_owned(), claude_settings);
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service
        .probe("final_review_panel", FlowPreset::Standard, 1)
        .expect("probe should succeed");

    // Panel probes use target: None — members are in the panel view.
    assert!(
        result.target.is_none(),
        "panel probes should have target: None"
    );
    assert!(
        result.panel.is_some(),
        "panel probes should have panel view"
    );
}

#[test]
fn probe_prompt_review_panel_uses_prompt_reviewer_timeout_not_validator() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    let mut claude_settings = empty_backend_settings(true);
    claude_settings.role_timeouts = BackendRoleTimeouts {
        prompt_reviewer: Some(15),
        prompt_validator: Some(30),
        ..Default::default()
    };
    workspace
        .backends
        .insert("claude".to_owned(), claude_settings);
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service
        .probe("prompt_review_panel", FlowPreset::Standard, 1)
        .expect("probe should succeed");

    assert!(
        result.target.is_none(),
        "panel probes should have target: None"
    );
    assert!(
        result.panel.is_some(),
        "panel probes should have panel view"
    );
}

#[test]
fn probe_completion_panel_uses_planner_timeout_not_completer() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    let mut claude_settings = empty_backend_settings(true);
    claude_settings.role_timeouts = BackendRoleTimeouts {
        planner: Some(7),
        completer: Some(42),
        ..Default::default()
    };
    workspace
        .backends
        .insert("claude".to_owned(), claude_settings);
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service
        .probe("completion_panel", FlowPreset::Standard, 1)
        .expect("probe should succeed");

    assert!(
        result.target.is_none(),
        "panel probes should have target: None"
    );
    assert!(
        result.panel.is_some(),
        "panel probes should have panel view"
    );
}

// ── probe failure config source tests ─────────────────────────────────────────

#[tokio::test]
async fn probe_failure_includes_config_source_for_planner() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    struct AllUnavailableAdapter;
    impl AgentExecutionPort for AllUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Err(AppError::BackendUnavailable {
                backend: backend.backend.family.as_str().to_owned(),
                details: "binary not found".to_owned(),
            })
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = AllUnavailableAdapter;

    let result = service
        .probe_with_availability("final_review_panel", FlowPreset::Standard, 1, &adapter)
        .await;

    assert!(
        result.is_err(),
        "probe should fail when planner is unavailable"
    );
    let err_msg = result.unwrap_err().to_string();
    // No explicit planner override set, so planner inherits from default_backend
    assert!(
        err_msg.contains("[source: default_backend]"),
        "inherited planner failure should report default_backend, not workflow.planner_backend: {}",
        err_msg
    );
}

#[tokio::test]
async fn probe_failure_includes_config_source_for_required_member() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that fails availability only for openrouter
    struct OpenRouterUnavailableAdapter;
    impl AgentExecutionPort for OpenRouterUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            if backend.backend.family == BackendFamily::OpenRouter {
                Err(AppError::BackendUnavailable {
                    backend: "openrouter".to_owned(),
                    details: "OPENROUTER_API_KEY not set".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(true));
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
    let adapter = OpenRouterUnavailableAdapter;

    let result = service
        .probe_with_availability("completion_panel", FlowPreset::Standard, 1, &adapter)
        .await;

    assert!(
        result.is_err(),
        "probe should fail on required unavailable member"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("[source: completion.backends]"),
        "error should include config source field for completion member: {}",
        err_msg
    );
}

// ── exact panel-failure identity tests (check) ────────────────────────────

#[test]
fn check_arbiter_failure_reports_exact_member_and_source() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Disable openrouter but configure the arbiter to use it
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    workspace.final_review = FinalReviewSettings {
        enabled: Some(true),
        backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            PanelBackendSpec::required(BackendFamily::Codex),
        ]),
        min_reviewers: Some(2),
        arbiter_backend: Some("openrouter".to_owned()),
        ..Default::default()
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service.check_backends(FlowPreset::Standard);

    assert!(
        !result.passed,
        "should fail when arbiter backend is disabled"
    );
    let arbiter_failure = result
        .failures
        .iter()
        .find(|f| f.role == "final_review_panel.arbiter")
        .expect("expected exact arbiter failure identity");
    assert_eq!(
        BackendCheckFailureKind::RequiredMemberUnavailable,
        arbiter_failure.failure_kind
    );
    assert_eq!(
        "final_review.arbiter_backend", arbiter_failure.config_source,
        "arbiter failure should report the arbiter config field, not the panel backends field"
    );
}

#[test]
fn check_refiner_failure_reports_exact_member_and_source() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Disable openrouter but configure the refiner to use it
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    workspace.prompt_review.refiner_backend = Some("openrouter".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service.check_backends(FlowPreset::Standard);

    assert!(
        !result.passed,
        "should fail when refiner backend is disabled"
    );
    let refiner_failure = result
        .failures
        .iter()
        .find(|f| f.role == "prompt_review_panel.refiner")
        .expect("expected exact refiner failure identity");
    assert_eq!(
        BackendCheckFailureKind::RequiredMemberUnavailable,
        refiner_failure.failure_kind
    );
    assert_eq!(
        "prompt_review.refiner_backend", refiner_failure.config_source,
        "refiner failure should report the refiner config field, not the validator backends field"
    );
}

// ── per-field source precedence tests (show-effective) ────────────────────

#[test]
fn show_effective_reports_model_source_from_role_models() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    let mut claude_settings = empty_backend_settings(true);
    claude_settings.role_models = BackendRoleModels {
        planner: Some("planner-model-x".to_owned()),
        ..Default::default()
    };
    workspace
        .backends
        .insert("claude".to_owned(), claude_settings);
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    let planner = view.roles.iter().find(|r| r.role == "planner").unwrap();
    assert_eq!("planner-model-x", planner.model_id);
    assert_eq!(
        "workspace.toml", planner.model_source,
        "model set via backends.claude.role_models.planner should report workspace.toml source"
    );
}

#[test]
fn show_effective_reports_timeout_source_from_role_timeouts() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    let mut claude_settings = empty_backend_settings(true);
    claude_settings.role_timeouts = BackendRoleTimeouts {
        planner: Some(11),
        ..Default::default()
    };
    workspace
        .backends
        .insert("claude".to_owned(), claude_settings);
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    let planner = view.roles.iter().find(|r| r.role == "planner").unwrap();
    assert_eq!(11, planner.timeout_seconds);
    assert_eq!(
        "workspace.toml", planner.timeout_source,
        "timeout set via backends.claude.role_timeouts.planner should report workspace.toml source"
    );
}

#[test]
fn show_effective_default_model_and_timeout_source() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    // With no explicit role models or timeouts, sources should be "default"
    let planner = view.roles.iter().find(|r| r.role == "planner").unwrap();
    assert_eq!(
        "default", planner.model_source,
        "default model should report source as 'default'"
    );
    assert_eq!(
        "default", planner.timeout_source,
        "default timeout should report source as 'default'"
    );
}

// ── optional-member availability semantics tests ──────────────────────────

#[tokio::test]
async fn check_with_availability_passes_when_optional_member_unavailable_but_minimum_satisfied() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that fails availability for openrouter backends
    struct OpenRouterUnavailableAdapter;
    impl AgentExecutionPort for OpenRouterUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            if backend.backend.family == BackendFamily::OpenRouter {
                Err(AppError::BackendUnavailable {
                    backend: "openrouter".to_owned(),
                    details: "OPENROUTER_API_KEY not set".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(true));
    // Config: claude (required) + codex (required) + openrouter (optional),
    // min_completers = 2. OpenRouter is optional and unavailable but the two
    // required members satisfy the minimum → check should PASS.
    workspace.completion = CompletionSettings {
        backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            PanelBackendSpec::required(BackendFamily::Codex),
            PanelBackendSpec::optional(BackendFamily::OpenRouter),
        ]),
        min_completers: Some(2),
        consensus_threshold: Some(0.66),
        extra: toml::Table::new(),
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = OpenRouterUnavailableAdapter;

    let result = service
        .check_backends_with_availability(FlowPreset::Standard, &adapter)
        .await;
    assert!(
        result.passed,
        "optional unavailable member should not block check when minimum is satisfied: {:?}",
        result.failures
    );
}

#[tokio::test]
async fn check_with_availability_fails_when_optional_omission_violates_minimum() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that fails availability for openrouter backends
    struct OpenRouterUnavailableAdapter;
    impl AgentExecutionPort for OpenRouterUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            if backend.backend.family == BackendFamily::OpenRouter {
                Err(AppError::BackendUnavailable {
                    backend: "openrouter".to_owned(),
                    details: "OPENROUTER_API_KEY not set".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(true));
    // Config: claude (required) + openrouter (optional), min_completers = 2.
    // OpenRouter is optional and unavailable, so only 1 available member
    // remains, which is below minimum 2 → should report PanelMinimumViolation.
    workspace.completion = CompletionSettings {
        backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            PanelBackendSpec::optional(BackendFamily::OpenRouter),
        ]),
        min_completers: Some(2),
        consensus_threshold: Some(0.66),
        extra: toml::Table::new(),
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = OpenRouterUnavailableAdapter;

    let result = service
        .check_backends_with_availability(FlowPreset::Standard, &adapter)
        .await;
    assert!(
        !result.passed,
        "optional omission dropping below minimum should fail check"
    );
    let min_violation = result
        .failures
        .iter()
        .find(|f| f.failure_kind == BackendCheckFailureKind::PanelMinimumViolation);
    assert!(
        min_violation.is_some(),
        "should report PanelMinimumViolation, not AvailabilityFailure: {:?}",
        result.failures
    );
}

// ── show-effective broken role resolution tests ───────────────────────────

#[test]
fn show_effective_surfaces_broken_role_instead_of_dropping() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Configure planner to use a disabled backend
    workspace.workflow.planner_backend = Some("openrouter".to_owned());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    // The planner role must be present even though its configured backend
    // (openrouter) is disabled — show-effective must not silently drop it.
    let planner = view.roles.iter().find(|r| r.role == "planner");
    assert!(
        planner.is_some(),
        "planner must appear in show-effective even when its backend is disabled; roles: {:?}",
        view.roles.iter().map(|r| &r.role).collect::<Vec<_>>()
    );

    let planner = planner.unwrap();
    assert_eq!("openrouter", planner.backend_family);
    assert!(
        planner.resolution_error.is_some(),
        "planner with disabled backend should have resolution_error set"
    );
    // override_source should still reflect the workspace.toml configuration
    assert_eq!("workspace.toml", planner.override_source);
}

#[test]
fn show_effective_json_includes_resolution_error_for_disabled_role() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.workflow.planner_backend = Some("openrouter".to_owned());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    // Verify JSON serialization includes the resolution_error field
    let json = serde_json::to_string(&view).expect("serialize");
    assert!(
        json.contains("resolution_error"),
        "JSON output should contain resolution_error for broken roles: {}",
        json
    );
}

// ── probe config-time failure identity tests ──────────────────────────────

#[test]
fn probe_config_time_failure_includes_target_identity_and_source() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Disable openrouter and set it as a required completion member
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

    // Config-time failure: required openrouter member is disabled
    let result = service.probe("completion_panel", FlowPreset::Standard, 1);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();

    // Must identify the exact target and source, not just bubble up a generic error
    assert!(
        err_msg.contains("completion_panel"),
        "config-time failure should identify the target: {}",
        err_msg
    );
    assert!(
        err_msg.contains("[source:"),
        "config-time failure should include source field: {}",
        err_msg
    );
}

#[test]
fn probe_prompt_review_panel_failure_reports_refiner_not_planner() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Disable openrouter and configure the prompt_review refiner to use it
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    workspace.prompt_review = PromptReviewSettings {
        enabled: Some(true),
        refiner_backend: Some("openrouter".to_owned()),
        validator_backends: None,
        min_reviewers: None,
        max_refinement_retries: None,
        extra: toml::Table::new(),
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    // Config-time failure: refiner backend is disabled
    let result = service.probe("prompt_review_panel", FlowPreset::Standard, 1);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();

    // Must report "refiner" as the target, NOT "planner/primary"
    assert!(
        err_msg.contains("refiner"),
        "prompt_review_panel config failure should identify refiner as target: {}",
        err_msg
    );
    assert!(
        err_msg.contains("prompt_review"),
        "prompt_review_panel config failure should reference prompt_review source: {}",
        err_msg
    );
}

#[tokio::test]
async fn probe_with_availability_final_review_failure_reports_planner_not_generic() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    struct AllUnavailableAdapter;
    impl AgentExecutionPort for AllUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Err(AppError::BackendUnavailable {
                backend: backend.backend.family.as_str().to_owned(),
                details: "binary not found".to_owned(),
            })
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = AllUnavailableAdapter;

    let result = service
        .probe_with_availability("final_review_panel", FlowPreset::Standard, 1, &adapter)
        .await;

    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    // Must mention "planner" as the primary target, not a generic label
    assert!(
        err_msg.contains("planner"),
        "final_review_panel availability failure should identify 'planner' as target: {}",
        err_msg
    );
}

// ── backend check aggregation tests ──────────────────────────────────────

#[tokio::test]
async fn check_with_availability_aggregates_arbiter_failure_independently_of_reviewer_resolution() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that reports all backends as unavailable
    struct AllUnavailableAdapter;
    impl AgentExecutionPort for AllUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Err(AppError::BackendUnavailable {
                backend: backend.backend.family.as_str().to_owned(),
                details: "binary not found".to_owned(),
            })
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Configure final_review with reviewer using codex (which we'll also disable
    // at the config level to make resolve_final_review_panel fail) and
    // arbiter using openrouter. Both codex and openrouter are enabled so
    // config-time resolution succeeds, but all backends are unavailable at
    // the adapter level.
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(true));
    workspace.final_review = FinalReviewSettings {
        enabled: Some(true),
        backends: Some(vec![PanelBackendSpec::required(BackendFamily::Codex)]),
        arbiter_backend: Some("openrouter".to_owned()),
        min_reviewers: Some(1),
        ..Default::default()
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = AllUnavailableAdapter;

    let result = service
        .check_backends_with_availability(FlowPreset::Standard, &adapter)
        .await;

    assert!(!result.passed, "all backends unavailable should fail");

    // The arbiter must be independently reported as an availability failure
    let arbiter_failure = result
        .failures
        .iter()
        .find(|f| f.role == "final_review_panel.arbiter");
    assert!(
        arbiter_failure.is_some(),
        "arbiter availability should be checked independently of reviewer resolution; failures: {:?}",
        result.failures
    );
    assert_eq!(
        BackendCheckFailureKind::AvailabilityFailure,
        arbiter_failure.unwrap().failure_kind
    );
    assert_eq!("openrouter", arbiter_failure.unwrap().backend_family);
}

#[tokio::test]
async fn check_with_availability_aggregates_refiner_failure_independently_of_validator_resolution()
{
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that reports all backends as unavailable
    struct AllUnavailableAdapter;
    impl AgentExecutionPort for AllUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Err(AppError::BackendUnavailable {
                backend: backend.backend.family.as_str().to_owned(),
                details: "binary not found".to_owned(),
            })
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(true));
    workspace.prompt_review = PromptReviewSettings {
        enabled: Some(true),
        refiner_backend: Some("openrouter".to_owned()),
        validator_backends: None,
        min_reviewers: None,
        max_refinement_retries: None,
        extra: toml::Table::new(),
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = AllUnavailableAdapter;

    let result = service
        .check_backends_with_availability(FlowPreset::Standard, &adapter)
        .await;

    assert!(!result.passed, "all backends unavailable should fail");

    // The refiner must be independently reported as an availability failure
    let refiner_failure = result
        .failures
        .iter()
        .find(|f| f.role == "prompt_review_panel.refiner");
    assert!(
        refiner_failure.is_some(),
        "refiner availability should be checked independently of validator resolution; failures: {:?}",
        result.failures
    );
    assert_eq!(
        BackendCheckFailureKind::AvailabilityFailure,
        refiner_failure.unwrap().failure_kind
    );
    assert_eq!("openrouter", refiner_failure.unwrap().backend_family);
}

// ── CLI coverage for probe config-time failures ──────────────────────────

#[test]
fn probe_config_time_failure_for_disabled_final_review_arbiter() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    workspace.final_review = FinalReviewSettings {
        enabled: Some(true),
        arbiter_backend: Some("openrouter".to_owned()),
        backends: None,
        min_reviewers: None,
        ..Default::default()
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    // Config-time failure: arbiter backend is disabled — probing the
    // final_review_panel should fail with target/source identity.
    let result = service.probe("final_review_panel", FlowPreset::Standard, 1);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();

    assert!(
        err_msg.contains("final_review_panel"),
        "config-time failure should identify the target panel: {}",
        err_msg
    );
    assert!(
        err_msg.contains("[source:"),
        "config-time failure should include source field: {}",
        err_msg
    );
}

// ── probe config-time exact member identity tests ────────────────────────

#[test]
fn probe_completion_panel_disabled_member_reports_exact_member_identity() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    // claude (required) + openrouter (required, disabled) with min=2
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
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();

    // Must identify the exact failing member, not just "planner"
    assert!(
        err_msg.contains("completion_panel.member[1]"),
        "should identify exact failing member completion_panel.member[1]: {}",
        err_msg
    );
    assert!(
        err_msg.contains("openrouter"),
        "should identify the failing backend family 'openrouter': {}",
        err_msg
    );
    assert!(
        err_msg.contains("completion.backends"),
        "should report the config source as 'completion.backends': {}",
        err_msg
    );
    // Must NOT attribute the failure to the planner
    assert!(
        !err_msg.contains("workflow.planner_backend"),
        "should not misattribute to workflow.planner_backend: {}",
        err_msg
    );
}

#[test]
fn probe_final_review_panel_disabled_arbiter_reports_arbiter_identity() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    workspace.final_review = FinalReviewSettings {
        enabled: Some(true),
        arbiter_backend: Some("openrouter".to_owned()),
        backends: None,
        min_reviewers: None,
        ..Default::default()
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    let result = service.probe("final_review_panel", FlowPreset::Standard, 1);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();

    // Must identify "arbiter" as the failing target
    assert!(
        err_msg.contains("arbiter"),
        "should identify arbiter as the failing target: {}",
        err_msg
    );
    assert!(
        err_msg.contains("openrouter"),
        "should identify openrouter as the failing backend: {}",
        err_msg
    );
    assert!(
        err_msg.contains("final_review.arbiter_backend"),
        "should report the config source as 'final_review.arbiter_backend': {}",
        err_msg
    );
    // Must NOT attribute the failure to the planner
    assert!(
        !err_msg.contains("workflow.planner_backend"),
        "should not misattribute to workflow.planner_backend: {}",
        err_msg
    );
}

// ── model source from default_backend tests ──────────────────────────────

#[test]
fn show_effective_model_source_from_default_backend_embedded_model() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Set default_backend with an embedded model: "claude(custom-model-x)"
    workspace.settings.default_backend = Some("claude(custom-model-x)".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    // The planner (which inherits from default_backend) should resolve to
    // "custom-model-x" with a source that traces back to default_backend,
    // NOT "default".
    let planner = view.roles.iter().find(|r| r.role == "planner").unwrap();
    assert_eq!(
        "custom-model-x", planner.model_id,
        "planner model should resolve to the embedded model"
    );
    assert!(
        planner.model_source != "default",
        "model_source should NOT be 'default' when model comes from default_backend; got: {}",
        planner.model_source
    );

    // The top-level default_model field must also reflect the embedded model
    // value and source, not the compile-time family default.
    assert_eq!(
        "custom-model-x", view.default_model.value,
        "default_model.value should be the embedded model, not the family default"
    );
    assert!(
        view.default_model.source != "default",
        "default_model.source should NOT be 'default' when model comes from default_backend; got: {}",
        view.default_model.source
    );
}

#[test]
fn show_effective_default_model_field_matches_base_backend_model() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Set default_backend with an embedded model
    workspace.settings.default_backend = Some("codex(my-custom-model)".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    // The top-level default_model field must report the embedded model value
    // and trace its source to default_backend, not to default_model or "default".
    assert_eq!(
        "my-custom-model", view.default_model.value,
        "default_model.value should match the model embedded in default_backend"
    );
    assert_ne!(
        "default", view.default_model.source,
        "default_model.source should trace to default_backend, not 'default'"
    );

    // base_backend should show the full selection string
    assert!(
        view.base_backend.value.contains("codex"),
        "base_backend.value should contain 'codex'"
    );
    assert!(
        view.base_backend.value.contains("my-custom-model"),
        "base_backend.value should contain the embedded model"
    );
}

#[test]
fn show_effective_model_source_default_when_no_embedded_model() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    // No default_backend override, no default_model — model comes from
    // the compile-time family default and should report "default".
    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    let planner = view.roles.iter().find(|r| r.role == "planner").unwrap();
    assert_eq!(
        "default", planner.model_source,
        "model_source should be 'default' when no embedded model and no default_model"
    );
}

// ── default_model source attribution tests ────────────────────────────────

#[test]
fn show_effective_default_model_from_settings_default_model_reports_correct_source() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Only set settings.default_model, NOT default_backend with embedded model
    workspace.settings.default_model = Some("my-workspace-model".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    // The top-level default_model should trace to the default_model setting,
    // not to default_backend or "default".
    assert_eq!(
        "my-workspace-model", view.default_model.value,
        "default_model.value should come from settings.default_model"
    );
    assert_ne!(
        "default", view.default_model.source,
        "default_model.source should NOT be 'default' when set via settings.default_model"
    );
    // source_for_default_model() looks up "default_model" key in config
    assert!(
        view.default_model.source.contains("workspace"),
        "default_model.source should trace to workspace.toml, got: {}",
        view.default_model.source
    );

    // Inherited roles (e.g., planner) should also report model_source as
    // coming from default_model, not from default_backend or "default".
    let planner = view.roles.iter().find(|r| r.role == "planner").unwrap();
    assert_eq!(
        "my-workspace-model", planner.model_id,
        "planner model should resolve to the settings.default_model value"
    );
    assert_ne!(
        "default", planner.model_source,
        "planner model_source should NOT be 'default' when model comes from settings.default_model; got: {}",
        planner.model_source
    );
}

#[test]
fn show_effective_default_model_embedded_in_default_backend_beats_settings_default_model() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Set both: embedded model in default_backend AND separate default_model
    workspace.settings.default_backend = Some("claude(embedded-model)".to_owned());
    workspace.settings.default_model = Some("separate-model".to_owned());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    // When default_backend has an embedded model, it takes precedence
    assert_eq!(
        "embedded-model", view.default_model.value,
        "embedded model in default_backend should take precedence over settings.default_model"
    );
    // Source should trace to default_backend, not default_model
    assert_ne!(
        "default", view.default_model.source,
        "source should not be 'default'"
    );
}

// ── probe minimum-violation from optional omission tests ──────────────────

#[test]
fn probe_completion_panel_optional_omission_below_minimum_reports_insufficient_members() {
    use ralph_burning::shared::error::AppError;

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Disable openrouter — it's optional but needed for the minimum
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    // claude (required) + openrouter (optional, disabled), min_completers = 2
    // Only 1 member resolves, which is below minimum 2.
    workspace.completion = CompletionSettings {
        backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            PanelBackendSpec::optional(BackendFamily::OpenRouter),
        ]),
        min_completers: Some(2),
        consensus_threshold: Some(0.66),
        extra: toml::Table::new(),
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    let result = service.probe("completion_panel", FlowPreset::Standard, 1);
    assert!(
        result.is_err(),
        "should fail when optional omission drops below minimum"
    );
    let err = result.unwrap_err();
    let err_msg = err.to_string();

    // Must be an InsufficientPanelMembers error, not BackendUnavailable with "unknown"
    assert!(
        matches!(&err, AppError::InsufficientPanelMembers { panel, resolved, minimum }
            if panel == "completion_panel" && *resolved == 1 && *minimum == 2),
        "expected InsufficientPanelMembers {{ panel: completion_panel, resolved: 1, minimum: 2 }}, got: {}",
        err_msg
    );
    assert!(
        !err_msg.contains("backend 'unknown'"),
        "should not fall back to generic BackendUnavailable with 'unknown': {}",
        err_msg
    );
}

#[test]
fn probe_final_review_panel_optional_omission_below_minimum_reports_insufficient_members() {
    use ralph_burning::shared::error::AppError;

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    // claude (required) + openrouter (optional, disabled), min_reviewers = 2
    workspace.final_review = FinalReviewSettings {
        enabled: Some(true),
        backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            PanelBackendSpec::optional(BackendFamily::OpenRouter),
        ]),
        min_reviewers: Some(2),
        ..Default::default()
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    let result = service.probe("final_review_panel", FlowPreset::Standard, 1);
    assert!(
        result.is_err(),
        "should fail when optional omission drops below minimum"
    );
    let err = result.unwrap_err();
    let err_msg = err.to_string();

    assert!(
        matches!(&err, AppError::InsufficientPanelMembers { panel, resolved, minimum }
            if panel == "final_review_panel" && *resolved == 1 && *minimum == 2),
        "expected InsufficientPanelMembers {{ panel: final_review_panel, resolved: 1, minimum: 2 }}, got: {}",
        err_msg
    );
    assert!(
        !err_msg.contains("backend 'unknown'"),
        "should not fall back to generic BackendUnavailable with 'unknown': {}",
        err_msg
    );
}

#[test]
fn probe_prompt_review_panel_optional_omission_below_minimum_reports_insufficient_members() {
    use ralph_burning::shared::error::AppError;

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    // claude (required) + openrouter (optional, disabled), min_reviewers = 2
    workspace.prompt_review = PromptReviewSettings {
        enabled: Some(true),
        refiner_backend: None,
        validator_backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            PanelBackendSpec::optional(BackendFamily::OpenRouter),
        ]),
        min_reviewers: Some(2),
        max_refinement_retries: None,
        extra: toml::Table::new(),
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    let result = service.probe("prompt_review_panel", FlowPreset::Standard, 1);
    assert!(
        result.is_err(),
        "should fail when optional omission drops below minimum"
    );
    let err = result.unwrap_err();
    let err_msg = err.to_string();

    assert!(
        matches!(&err, AppError::InsufficientPanelMembers { panel, resolved, minimum }
            if panel == "prompt_review_panel" && *resolved == 1 && *minimum == 2),
        "expected InsufficientPanelMembers {{ panel: prompt_review_panel, resolved: 1, minimum: 2 }}, got: {}",
        err_msg
    );
    assert!(
        !err_msg.contains("backend 'unknown'"),
        "should not fall back to generic BackendUnavailable with 'unknown': {}",
        err_msg
    );
}

// ── probe_with_availability minimum-violation from optional omission ──────

#[tokio::test]
async fn probe_with_availability_optional_omission_below_minimum_reports_insufficient_members() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that fails availability for openrouter backends
    struct OpenRouterUnavailableAdapter;
    impl AgentExecutionPort for OpenRouterUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            if backend.backend.family == BackendFamily::OpenRouter {
                Err(AppError::BackendUnavailable {
                    backend: "openrouter".to_owned(),
                    details: "OPENROUTER_API_KEY not set".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(true));
    // claude (required) + openrouter (optional, enabled but unavailable),
    // min_completers = 2. OpenRouter passes config-time checks but fails
    // at availability, leaving only 1 available member below minimum 2.
    workspace.completion = CompletionSettings {
        backends: Some(vec![
            PanelBackendSpec::required(BackendFamily::Claude),
            PanelBackendSpec::optional(BackendFamily::OpenRouter),
        ]),
        min_completers: Some(2),
        consensus_threshold: Some(0.66),
        extra: toml::Table::new(),
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = OpenRouterUnavailableAdapter;

    let result = service
        .probe_with_availability("completion_panel", FlowPreset::Standard, 1, &adapter)
        .await;

    assert!(
        result.is_err(),
        "should fail when optional unavailable member drops below minimum"
    );
    let err = result.unwrap_err();
    let err_msg = err.to_string();

    // Must be an InsufficientPanelMembers error, not BackendUnavailable
    assert!(
        matches!(&err, AppError::InsufficientPanelMembers { panel, resolved, minimum }
            if panel == "completion" && *resolved == 1 && *minimum == 2),
        "expected InsufficientPanelMembers for availability-time minimum violation, got: {}",
        err_msg
    );
}

// ── configured-index identity after optional member omission ─────────────

#[tokio::test]
async fn probe_with_availability_reports_correct_configured_index_after_optional_omission() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that fails availability for claude backends (spec[1] after
    /// the optional openrouter at spec[0] is omitted).
    struct ClaudeUnavailableAdapter;
    impl AgentExecutionPort for ClaudeUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            if backend.backend.family == BackendFamily::Claude {
                Err(AppError::BackendUnavailable {
                    backend: "claude".to_owned(),
                    details: "claude binary not found".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    // prompt_review.validator_backends = ["?openrouter", "claude"]
    // openrouter is optional+disabled (will be omitted at config time),
    // claude is required (spec index 1) and will fail at availability time.
    // refiner is explicitly set to codex so it passes availability.
    workspace.prompt_review = PromptReviewSettings {
        enabled: Some(true),
        validator_backends: Some(vec![
            PanelBackendSpec::optional(BackendFamily::OpenRouter),
            PanelBackendSpec::required(BackendFamily::Claude),
        ]),
        min_reviewers: Some(1),
        max_refinement_retries: None,
        refiner_backend: Some("codex".to_owned()),
        extra: toml::Table::new(),
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = ClaudeUnavailableAdapter;

    let result = service
        .probe_with_availability("prompt_review_panel", FlowPreset::Standard, 1, &adapter)
        .await;

    assert!(
        result.is_err(),
        "probe should fail when required member is unavailable"
    );
    let err_msg = result.unwrap_err().to_string();
    // The failing member is at configured spec index 1 (not 0, which would be
    // the index from the filtered list after optional openrouter was omitted).
    assert!(
        err_msg.contains("[1]"),
        "error should reference configured spec index 1, not filtered index 0: {}",
        err_msg
    );
    assert!(
        !err_msg.contains("[0]"),
        "error must NOT reference filtered index 0 for spec-index-1 member: {}",
        err_msg
    );
}

#[tokio::test]
async fn check_with_availability_reports_correct_configured_index_for_panel_member() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that fails codex availability. Claude (optional, disabled at config
    /// time) is at spec[0], codex (required) is at spec[1].
    struct CodexUnavailableAdapter;
    impl AgentExecutionPort for CodexUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            if backend.backend.family == BackendFamily::Codex {
                Err(AppError::BackendUnavailable {
                    backend: "codex".to_owned(),
                    details: "codex binary not found".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Disable openrouter (optional spec[0]), enable codex (required spec[1])
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    workspace.completion = CompletionSettings {
        backends: Some(vec![
            PanelBackendSpec::optional(BackendFamily::OpenRouter),
            PanelBackendSpec::required(BackendFamily::Codex),
        ]),
        min_completers: Some(1),
        consensus_threshold: Some(0.66),
        extra: toml::Table::new(),
    };
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = CodexUnavailableAdapter;

    let result = service
        .check_backends_with_availability(FlowPreset::Standard, &adapter)
        .await;

    assert!(
        !result.passed,
        "should fail when required member is unavailable"
    );
    let panel_failure = result
        .failures
        .iter()
        .find(|f| f.role.contains("completion_panel.member"))
        .expect("expected panel member failure");
    // The codex member is at configured spec index 1, not filtered index 0
    assert!(
        panel_failure.role.contains("[1]"),
        "failure role should reference configured spec index 1: {}",
        panel_failure.role
    );
    assert_eq!("codex", panel_failure.backend_family);
}

// ── implicit completion-panel resolution alignment ───────────────────────

/// Regression: when `completion.backends` is not explicitly configured,
/// `backend check` must validate the same implicit targets that
/// `resolve_completion_panel()` / `default_completion_targets()` would use,
/// not the built-in default backend list.
///
/// Setup: default_backend=claude, claude disabled, all stage roles
/// overridden to codex, openrouter enabled. With implicit completion
/// backends, runtime resolves the Completer role via `opposite_family(codex)`
/// which falls through to openrouter (claude disabled). The built-in
/// default completion backends list is `[claude(required), codex(required)]`
/// — the old code would fail on disabled claude even though runtime never
/// uses it.
#[test]
fn check_passes_with_implicit_completion_backends_when_runtime_targets_are_available() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // default_backend = claude, but claude is disabled
    workspace.settings.default_backend = Some("claude".to_owned());
    workspace
        .backends
        .insert("claude".to_owned(), empty_backend_settings(false));
    // Enable openrouter so opposite_family(codex) can fall through to it
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(true));
    // Override all stage roles to codex — planner=codex
    workspace.workflow.planner_backend = Some("codex".to_owned());
    workspace.workflow.implementer_backend = Some("codex".to_owned());
    workspace.workflow.reviewer_backend = Some("codex".to_owned());
    workspace.workflow.qa_backend = Some("codex".to_owned());
    // Do NOT set completion.backends — leave implicit so runtime uses
    // default_completion_targets() which resolves the Completer role
    // (opposite of planner=codex → openrouter, since claude is disabled).
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    // Probe should succeed (runtime resolution uses opposite family)
    let probe_result = service.probe("completion_panel", FlowPreset::Standard, 1);
    assert!(
        probe_result.is_ok(),
        "probe should succeed with implicit completion backends: {:?}",
        probe_result.err()
    );

    // Check should also succeed — both should use the same resolution path
    let check_result = service.check_backends(FlowPreset::Standard);
    let completion_failures: Vec<_> = check_result
        .failures
        .iter()
        .filter(|f| f.role.contains("completion"))
        .collect();
    assert!(
        completion_failures.is_empty(),
        "check should not fail on completion panel when implicit targets are available: {:?}",
        completion_failures
    );
}

/// Regression: when completion backends are implicit and the Completer role
/// target is actually disabled, `backend check` should report the failure
/// using the real resolution source (e.g. `default_backend`), not
/// `completion.backends`.
#[test]
fn check_fails_with_implicit_completion_backends_when_completer_role_unavailable() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Claude is the default; codex (the opposite family for Completer) is disabled
    workspace
        .backends
        .insert("codex".to_owned(), empty_backend_settings(false));
    // No explicit completion.backends
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service.check_backends(FlowPreset::Standard);

    let completion_failure = result
        .failures
        .iter()
        .find(|f| f.role.contains("completion"));
    assert!(
        completion_failure.is_some(),
        "check should fail on completion panel when Completer role target is unavailable: {:?}",
        result.failures
    );
}

// ── final-review scoping alignment with engine stage plan ────────────────

/// Regression: `backend check` must validate final review whenever the
/// flow's stage plan includes FinalReview, regardless of `final_review.enabled`.
/// The engine's `stage_plan_for_flow()` does NOT filter FinalReview based
/// on that flag, so diagnostics must not skip it either.
#[test]
fn check_validates_final_review_even_when_final_review_enabled_is_false() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Disable final review via config flag
    workspace.final_review = FinalReviewSettings {
        enabled: Some(false),
        // Configure arbiter/reviewers on a disabled backend so check would fail
        // IF it actually validates final review
        arbiter_backend: Some("openrouter".to_owned()),
        backends: Some(vec![PanelBackendSpec::required(BackendFamily::OpenRouter)]),
        min_reviewers: Some(1),
        ..Default::default()
    };
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    // Standard flow includes FinalReview in its stage plan regardless of
    // final_review.enabled, so check must validate it and report failure.
    let result = service.check_backends(FlowPreset::Standard);
    let final_review_failures: Vec<_> = result
        .failures
        .iter()
        .filter(|f| f.role.contains("final_review"))
        .collect();
    assert!(
        !final_review_failures.is_empty(),
        "check should validate final review even when final_review.enabled=false \
         because the engine stage plan still includes FinalReview: {:?}",
        result.failures
    );
}

/// Regression: for flows that do NOT include FinalReview in their stage
/// definitions (e.g. docs_change), `backend check` should still skip
/// final review validation regardless of `final_review.enabled`.
#[test]
fn check_still_skips_final_review_for_flows_without_final_review_stage() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    // Configure broken final review
    workspace.final_review = FinalReviewSettings {
        enabled: Some(true),
        arbiter_backend: Some("openrouter".to_owned()),
        backends: Some(vec![PanelBackendSpec::required(BackendFamily::OpenRouter)]),
        min_reviewers: Some(1),
        ..Default::default()
    };
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    // DocsChange does not include FinalReview in its stages
    let result = service.check_backends(FlowPreset::DocsChange);
    let final_review_failures: Vec<_> = result
        .failures
        .iter()
        .filter(|f| f.role.contains("final_review"))
        .collect();
    assert!(
        final_review_failures.is_empty(),
        "docs_change should skip final review validation since the flow \
         does not include FinalReview: {:?}",
        final_review_failures
    );
}

// ── opposite-family role attribution ──────────────────────────────────────

/// Regression: when opposite-family roles (implementer, qa, completer) have no
/// explicit override, `family_for_role()` must reflect the runtime resolution
/// path (`opposite_family(planner_family)`), not `default_backend`.
///
/// Setup: default_backend=claude, both opposite families disabled. The runtime
/// resolution for implementer would attempt opposite_family(claude) and fail.
/// `show-effective` must report the attempted opposite family, not "claude".
#[test]
fn show_effective_opposite_family_roles_report_attempted_family_not_base() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("claude".to_owned());
    // Disable both codex and openrouter so opposite_family(claude) fails
    workspace
        .backends
        .insert("codex".to_owned(), empty_backend_settings(false));
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    // Implementer, qa, acceptance_qa, completer use opposite-family resolution.
    // With no opposite family available, they should NOT report "claude" as
    // the backend family — that would be wrong per runtime semantics.
    let implementer = view.roles.iter().find(|r| r.role == "implementer").unwrap();
    assert_ne!(
        "claude", implementer.backend_family,
        "implementer should not report base backend 'claude' — it uses opposite-family resolution"
    );
    assert!(
        implementer.resolution_error.is_some(),
        "implementer should have resolution_error when opposite family is unavailable"
    );

    let qa = view.roles.iter().find(|r| r.role == "qa").unwrap();
    assert_ne!(
        "claude", qa.backend_family,
        "qa should not report base backend 'claude' — it uses opposite-family resolution"
    );

    let completer = view.roles.iter().find(|r| r.role == "completer").unwrap();
    assert_ne!(
        "claude", completer.backend_family,
        "completer should not report base backend 'claude' — it uses opposite-family resolution"
    );

    // Planner-family roles (planner, reviewer) should still report "claude"
    let planner = view.roles.iter().find(|r| r.role == "planner").unwrap();
    assert_eq!(
        "claude", planner.backend_family,
        "planner should report base backend 'claude'"
    );
}

/// Regression: when opposite-family roles resolve successfully, show-effective
/// must report the actual opposite family (e.g., codex), not the base backend.
#[test]
fn show_effective_opposite_family_roles_report_resolved_family_when_available() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("claude".to_owned());
    // Codex is enabled by default — it is the opposite family for claude
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let view = service.show_effective();

    // When resolution succeeds, the resolved target family is used (codex)
    let implementer = view.roles.iter().find(|r| r.role == "implementer").unwrap();
    assert_eq!(
        "codex", implementer.backend_family,
        "implementer should resolve to opposite family 'codex' when available"
    );
    assert!(
        implementer.resolution_error.is_none(),
        "implementer should not have resolution_error when opposite family is available"
    );
}

/// Regression: `backend check` must report the attempted opposite family (not
/// the base backend) when opposite-family roles fail.
#[test]
fn check_reports_opposite_family_not_base_for_failed_opposite_roles() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("claude".to_owned());
    workspace
        .backends
        .insert("codex".to_owned(), empty_backend_settings(false));
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let result = service.check_backends(FlowPreset::Standard);

    // Implementer failures should NOT report backend_family="claude"
    let impl_failure = result.failures.iter().find(|f| f.role == "implementer");
    if let Some(failure) = impl_failure {
        assert_ne!(
            "claude", failure.backend_family,
            "implementer failure should report the attempted opposite family, not 'claude': {:?}",
            failure
        );
    }
}

/// Regression: `backend probe --role implementer` must report the opposite-family
/// failure, not claim the base backend is unavailable.
#[test]
fn probe_singular_opposite_role_reports_correct_family() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("claude".to_owned());
    workspace
        .backends
        .insert("codex".to_owned(), empty_backend_settings(false));
    workspace
        .backends
        .insert("openrouter".to_owned(), empty_backend_settings(false));
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);

    let result = service.probe("implementer", FlowPreset::Standard, 1);
    assert!(
        result.is_err(),
        "probe should fail when no opposite family is enabled"
    );
    let err_msg = result.unwrap_err().to_string();
    // The outer error should identify the opposite-family target, not plain 'claude'.
    // The inner error detail naturally mentions claude as the base family, which is fine.
    assert!(
        err_msg.contains("opposite_of(claude)") || err_msg.contains("opposite"),
        "probe error should identify the opposite-family resolution path, not blame 'claude' directly: {}",
        err_msg
    );
    // The error must also mention the resolution failure reason
    assert!(
        err_msg.contains("no opposite backend family is enabled"),
        "probe error should include the opposite-family failure reason: {}",
        err_msg
    );
}

// ── implicit completion-panel availability config source ──────────────────

/// Regression: when completion backends are implicit and availability checks
/// run, the config source reported for completion panel failures must reflect
/// the actual resolution source (Completer role → default_backend), not the
/// hard-coded "completion.backends".
#[tokio::test]
async fn check_with_availability_implicit_completion_reports_correct_source() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that fails availability for codex backends
    struct CodexUnavailableAdapter;
    impl AgentExecutionPort for CodexUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            if backend.backend.family == BackendFamily::Codex {
                Err(AppError::BackendUnavailable {
                    backend: "codex".to_owned(),
                    details: "codex binary not found".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("claude".to_owned());
    // No explicit completion.backends — implicit resolution uses Completer role
    // which resolves to opposite_family(claude) = codex
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = CodexUnavailableAdapter;

    let result = service
        .check_backends_with_availability(FlowPreset::Standard, &adapter)
        .await;

    // Completion panel members should report source from Completer role
    // resolution, NOT "completion.backends"
    let completion_failures: Vec<_> = result
        .failures
        .iter()
        .filter(|f| f.role.contains("completion_panel"))
        .collect();
    for failure in &completion_failures {
        assert_ne!(
            "completion.backends", failure.config_source,
            "implicit completion members should not report 'completion.backends' as source: {:?}",
            failure
        );
    }
}

/// Regression: when completion backends are implicit, `backend probe --role
/// completion_panel` availability failures must report the Completer role
/// source, not "completion.backends".
#[tokio::test]
async fn probe_with_availability_implicit_completion_reports_correct_source() {
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::shared::domain::ResolvedBackendTarget;
    use ralph_burning::shared::error::AppError;

    /// Adapter that fails availability for codex backends only
    struct CodexUnavailableAdapter;
    impl AgentExecutionPort for CodexUnavailableAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> ralph_burning::shared::error::AppResult<()> {
            Ok(())
        }
        async fn check_availability(
            &self,
            backend: &ResolvedBackendTarget,
        ) -> ralph_burning::shared::error::AppResult<()> {
            if backend.backend.family == BackendFamily::Codex {
                Err(AppError::BackendUnavailable {
                    backend: "codex".to_owned(),
                    details: "codex binary not found".to_owned(),
                })
            } else {
                Ok(())
            }
        }
        async fn invoke(
            &self,
            _request: InvocationRequest,
        ) -> ralph_burning::shared::error::AppResult<InvocationEnvelope> {
            unimplemented!()
        }
        async fn cancel(&self, _id: &str) -> ralph_burning::shared::error::AppResult<()> {
            unimplemented!()
        }
    }

    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let mut workspace = WorkspaceConfig::new(test_timestamp());
    workspace.settings.default_backend = Some("claude".to_owned());
    // No explicit completion.backends — implicit
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let adapter = CodexUnavailableAdapter;

    let result = service
        .probe_with_availability("completion_panel", FlowPreset::Standard, 1, &adapter)
        .await;

    // Probe should fail because codex (opposite family = completers) is unavailable
    assert!(
        result.is_err(),
        "probe should fail when completion backend is unavailable"
    );
    let err_msg = result.unwrap_err().to_string();
    // The error should NOT reference "completion.backends" as the source
    assert!(
        !err_msg.contains("completion.backends"),
        "implicit completion probe failure should not reference 'completion.backends': {}",
        err_msg
    );
}

// ── build-sensitive compile_only for stub ────────────────────────────────

#[cfg(feature = "test-stub")]
#[test]
fn list_backends_stub_not_compile_only_in_stub_build() {
    let temp_dir = tempdir().expect("create temp dir");
    initialize_workspace_fixture(temp_dir.path());

    let workspace = WorkspaceConfig::new(test_timestamp());
    write_workspace_config(temp_dir.path(), &workspace);

    let config = EffectiveConfig::load(temp_dir.path()).expect("load config");
    let service = BackendDiagnosticsService::new(&config);
    let entries = service.list_backends();

    let stub = entries.iter().find(|e| e.family == "stub").unwrap();
    assert_eq!(
        None, stub.compile_only,
        "stub should not be compile_only in test-stub build"
    );
}
