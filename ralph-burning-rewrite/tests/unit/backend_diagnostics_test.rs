use chrono::TimeZone;
use tempfile::tempdir;

use ralph_burning::adapters::fs::FileSystem;
use ralph_burning::contexts::agent_execution::diagnostics::{
    BackendCheckFailureKind, BackendDiagnosticsService,
};
use ralph_burning::contexts::workspace_governance::config::{CliBackendOverrides, EffectiveConfig};
use ralph_burning::shared::domain::{
    BackendFamily, BackendRuntimeSettings, BackendSelection, CompletionSettings, FlowPreset,
    PanelBackendSpec, WorkspaceConfig, FinalReviewSettings,
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
    let arbiter = panel.arbiter.expect("should have arbiter in final_review panel");
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
    let panel_failure = result
        .failures
        .iter()
        .find(|f| f.role == "completion_panel")
        .expect("expected completion_panel failure");

    // The failure should identify the exact backend, not just "mixed"
    assert_eq!(
        BackendCheckFailureKind::RequiredMemberUnavailable,
        panel_failure.failure_kind
    );
    assert_eq!("openrouter", panel_failure.backend_family);
    assert_eq!("completion.backends", panel_failure.config_source);
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
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
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
    assert!(result.passed, "all-available adapter should pass: {:?}", result.failures);
}

#[tokio::test]
async fn check_with_availability_reports_panel_member_failure() {
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
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
    use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
    use ralph_burning::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationRequest,
    };
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
