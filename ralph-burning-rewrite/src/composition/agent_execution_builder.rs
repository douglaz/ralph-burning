//! Shared builder for `BackendAdapter` and `AgentExecutionService`.
//!
//! Centralises runtime adapter selection so that CLI `run`, CLI `requirements`,
//! and daemon requirements dispatch all resolve the same backend family/model
//! defaults from `EffectiveConfig`. The `process` adapter is the production
//! default and dispatches OpenRouter-resolved requests to the HTTP adapter in
//! direct mode. When `execution.mode = "tmux"` is active, adapter selection
//! stays on the tmux/process stack so OpenRouter targets fail through the same
//! explicit tmux-mode rejection path instead of silently downgrading to direct
//! execution. The `stub` branch remains a test seam and is compiled only with
//! the `test-stub` Cargo feature.

#[cfg(feature = "test-stub")]
use std::collections::HashMap;

use crate::adapters::fs::{FsRawOutputStore, FsRequirementsStore, FsSessionStore};
use crate::adapters::openrouter_backend::OpenRouterBackendAdapter;
use crate::adapters::process_backend::ProcessBackendAdapter;
#[cfg(feature = "test-stub")]
use crate::adapters::stub_backend::StubBackendAdapter;
use crate::adapters::tmux::TmuxAdapter;
use crate::adapters::BackendAdapter;
use crate::contexts::agent_execution::service::{AgentExecutionService, BackendSelectionConfig};
use crate::contexts::requirements_drafting::service::RequirementsService;
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::shared::domain::ExecutionMode;
#[cfg(feature = "test-stub")]
use crate::shared::domain::StageId;
use crate::shared::error::{AppError, AppResult};

/// The concrete `AgentExecutionService` type used by production CLI and daemon paths.
pub type ProductionAgentService =
    AgentExecutionService<BackendAdapter, FsRawOutputStore, FsSessionStore>;

/// The concrete `RequirementsService` type used by production CLI and daemon paths.
pub type ProductionRequirementsService =
    RequirementsService<BackendAdapter, FsRawOutputStore, FsSessionStore, FsRequirementsStore>;

fn backend_selector_reason() -> &'static str {
    #[cfg(feature = "test-stub")]
    {
        "expected one of stub, process, openrouter"
    }

    #[cfg(not(feature = "test-stub"))]
    {
        "expected one of process, openrouter; 'stub' requires the test-stub cargo feature"
    }
}

// ── Agent execution service builder ─────────────────────────────────────────

/// Build a `BackendAdapter` from the `RALPH_BURNING_BACKEND` environment variable.
/// Defaults to `process` when the variable is unset.
pub fn build_backend_adapter() -> AppResult<BackendAdapter> {
    build_backend_adapter_with_config(None)
}

pub fn build_backend_adapter_with_config(
    effective_config: Option<&EffectiveConfig>,
) -> AppResult<BackendAdapter> {
    let tmux_enabled = effective_config
        .is_some_and(|config| config.effective_execution_mode() == ExecutionMode::Tmux);
    let backend_selector = match std::env::var("RALPH_BURNING_BACKEND") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "process".to_owned(),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(AppError::InvalidConfigValue {
                key: "RALPH_BURNING_BACKEND".to_owned(),
                value: "<non-unicode>".to_owned(),
                reason: backend_selector_reason().to_owned(),
            });
        }
    };

    match backend_selector.as_str() {
        #[cfg(feature = "test-stub")]
        "stub" => Ok(BackendAdapter::Stub(build_stub_backend_adapter())),
        #[cfg(not(feature = "test-stub"))]
        "stub" => Err(AppError::InvalidConfigValue {
            key: "RALPH_BURNING_BACKEND".to_owned(),
            value: "stub".to_owned(),
            reason: "stub backend requires the test-stub cargo feature".to_owned(),
        }),
        "process" | "openrouter" if tmux_enabled => {
            let process = ProcessBackendAdapter::new();
            Ok(BackendAdapter::Tmux(TmuxAdapter::new(
                process,
                effective_config
                    .map(|config| config.effective_stream_output())
                    .unwrap_or(false),
            )))
        }
        "process" => {
            let process = ProcessBackendAdapter::new();
            Ok(BackendAdapter::Process(process))
        }
        "openrouter" => Ok(BackendAdapter::OpenRouter(OpenRouterBackendAdapter::new())),
        other => Err(AppError::InvalidConfigValue {
            key: "RALPH_BURNING_BACKEND".to_owned(),
            value: other.to_owned(),
            reason: backend_selector_reason().to_owned(),
        }),
    }
}

/// Build a backend adapter for diagnostics, using the same routing as normal
/// execution but ignoring `RALPH_BURNING_BACKEND` env var. This ensures
/// diagnostics check the actual configured backends with correct per-target
/// dispatch (Process for Claude/Codex, OpenRouter for OpenRouter targets).
pub fn build_backend_adapter_for_diagnostics(
    effective_config: &EffectiveConfig,
) -> AppResult<BackendAdapter> {
    // In test-stub mode, always use the stub adapter for diagnostics.
    // In production, use the normal config-driven builder but override
    // any env var to "process" so RALPH_BURNING_BACKEND=openrouter
    // doesn't redirect checks to the wrong transport.
    #[cfg(feature = "test-stub")]
    {
        let saved = std::env::var("RALPH_BURNING_BACKEND").ok();
        std::env::set_var("RALPH_BURNING_BACKEND", "stub");
        let result = build_backend_adapter_with_config(Some(effective_config));
        match saved {
            Some(val) => std::env::set_var("RALPH_BURNING_BACKEND", val),
            None => std::env::remove_var("RALPH_BURNING_BACKEND"),
        }
        return result;
    }
    #[cfg(not(feature = "test-stub"))]
    {
        let saved = std::env::var("RALPH_BURNING_BACKEND").ok();
        std::env::remove_var("RALPH_BURNING_BACKEND");
        let result = build_backend_adapter_with_config(Some(effective_config));
        if let Some(val) = saved {
            std::env::set_var("RALPH_BURNING_BACKEND", val);
        }
        result
    }
}

/// Build an `AgentExecutionService` backed by the environment-selected adapter.
pub fn build_agent_execution_service() -> AppResult<ProductionAgentService> {
    let adapter = build_backend_adapter()?;
    Ok(AgentExecutionService::new(
        adapter,
        FsRawOutputStore,
        FsSessionStore,
    ))
}

pub fn build_agent_execution_service_for_config(
    effective_config: &EffectiveConfig,
) -> AppResult<ProductionAgentService> {
    let adapter = build_backend_adapter_with_config(Some(effective_config))?;
    Ok(AgentExecutionService::new(
        adapter,
        FsRawOutputStore,
        FsSessionStore,
    ))
}

// ── Requirements service builder ────────────────────────────────────────────

/// Build a `RequirementsService` backed by the environment-selected adapter and
/// workspace defaults from `EffectiveConfig`.  Both CLI and daemon requirements
/// paths call this, ensuring consistent backend resolution.
pub fn build_requirements_service(
    effective_config: &EffectiveConfig,
) -> AppResult<ProductionRequirementsService> {
    let adapter = build_backend_adapter_with_config(Some(effective_config))?;

    #[cfg(feature = "test-stub")]
    // When using the stub adapter, apply test-only label overrides.
    let adapter = apply_label_overrides_if_stub(adapter);

    let workspace_defaults = BackendSelectionConfig::from_effective_config(effective_config)?;
    let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
    let requirements_store = FsRequirementsStore;
    Ok(RequirementsService::new(agent_service, requirements_store)
        .with_workspace_defaults(workspace_defaults))
}

// ── Test-only stub construction ─────────────────────────────────────────────

/// If the adapter is `Stub`, apply environment-driven test overrides.  For
/// `Process` adapters this is a no-op.
#[cfg(feature = "test-stub")]
fn apply_label_overrides_if_stub(adapter: BackendAdapter) -> BackendAdapter {
    match adapter {
        BackendAdapter::Stub(stub) => BackendAdapter::Stub(apply_test_label_overrides(stub)),
        other => other,
    }
}

/// Apply `RALPH_BURNING_TEST_LABEL_OVERRIDES` to a `StubBackendAdapter`.
/// Public so that in-process test harnesses (e.g. conformance daemon helper)
/// can replicate the same override behaviour without spawning a CLI binary.
#[cfg(feature = "test-stub")]
pub fn apply_test_label_overrides(mut adapter: StubBackendAdapter) -> StubBackendAdapter {
    if let Ok(overrides_json) = std::env::var("RALPH_BURNING_TEST_LABEL_OVERRIDES") {
        if let Ok(overrides) =
            serde_json::from_str::<HashMap<String, serde_json::Value>>(&overrides_json)
        {
            for (label, payload) in overrides {
                let full_label = if label.starts_with("requirements:")
                    || label.starts_with("prompt_review:")
                    || label.starts_with("completion_panel:")
                    || label.starts_with("final_review:")
                {
                    label
                } else {
                    format!("requirements:{label}")
                };
                // If the payload is an array, treat it as a sequence so the stub
                // returns different responses on successive invocations with the
                // same label (e.g. validation → needs_questions, then pass).
                if let Some(arr) = payload.as_array() {
                    adapter =
                        adapter.with_label_payload_sequence(full_label, arr.clone());
                } else {
                    adapter = adapter.with_label_payload(full_label, payload);
                }
            }
        }
    }
    adapter
}

/// Build a `StubBackendAdapter` with all environment-driven test injection seams
/// applied.  This matches the set of seams previously inlined in `cli/run.rs`.
#[cfg(feature = "test-stub")]
fn build_stub_backend_adapter() -> StubBackendAdapter {
    let mut adapter = StubBackendAdapter::default();

    if std::env::var("RALPH_BURNING_TEST_BACKEND_UNAVAILABLE").is_ok() {
        adapter = adapter.unavailable();
    }
    if let Ok(stage_str) = std::env::var("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE") {
        if let Ok(stage_id) = stage_str.parse::<StageId>() {
            adapter = adapter.with_invoke_failure(stage_id);
        }
    }
    if let Ok(spec) = std::env::var("RALPH_BURNING_TEST_TRANSIENT_FAILURE") {
        if let Some((stage_str, count_str)) = spec.split_once(':') {
            if let (Ok(stage_id), Ok(count)) =
                (stage_str.parse::<StageId>(), count_str.parse::<u32>())
            {
                adapter = adapter.with_transient_failure(stage_id, count);
            }
        }
    }
    if let Ok(overrides_json) = std::env::var("RALPH_BURNING_TEST_STAGE_OVERRIDES") {
        if let Ok(overrides) =
            serde_json::from_str::<HashMap<String, serde_json::Value>>(&overrides_json)
        {
            for (stage_str, payload) in overrides {
                if let Ok(stage_id) = stage_str.parse::<StageId>() {
                    if let Some(arr) = payload.as_array() {
                        adapter = adapter.with_stage_payload_sequence(stage_id, arr.clone());
                    } else {
                        adapter = adapter.with_stage_payload(stage_id, payload);
                    }
                }
            }
        }
    }

    // Also apply label overrides so that stub-backed requirements and panel
    // paths can drive role-specific payloads in tests.
    apply_test_label_overrides(adapter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::Mutex;
    use tempfile::tempdir;

    use crate::contexts::workspace_governance::{initialize_workspace, CliBackendOverrides};

    /// Serialize tests that mutate `RALPH_BURNING_BACKEND` to avoid races.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn tmux_effective_config() -> EffectiveConfig {
        let temp_dir = tempdir().expect("create temp dir");
        let created_at = chrono::Utc
            .with_ymd_and_hms(2026, 3, 19, 8, 30, 28)
            .single()
            .expect("valid timestamp");
        initialize_workspace(temp_dir.path(), created_at).expect("initialize workspace");

        EffectiveConfig::load_for_project(
            temp_dir.path(),
            None,
            CliBackendOverrides {
                execution_mode: Some(ExecutionMode::Tmux),
                ..Default::default()
            },
        )
        .expect("load tmux effective config")
    }

    #[test]
    #[cfg(feature = "test-stub")]
    fn build_backend_adapter_selects_stub_when_env_is_stub() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("RALPH_BURNING_BACKEND", "stub");
        let adapter = build_backend_adapter().expect("stub build should succeed");
        assert!(matches!(adapter, BackendAdapter::Stub(_)));
        std::env::remove_var("RALPH_BURNING_BACKEND");
    }

    #[test]
    #[cfg(not(feature = "test-stub"))]
    fn build_backend_adapter_rejects_stub_when_test_stub_feature_disabled() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("RALPH_BURNING_BACKEND", "stub");

        let error = match build_backend_adapter() {
            Ok(_) => panic!("stub should be rejected without feature"),
            Err(error) => error,
        };

        match error {
            AppError::InvalidConfigValue { key, value, reason } => {
                assert_eq!(key, "RALPH_BURNING_BACKEND");
                assert_eq!(value, "stub");
                assert!(
                    reason.contains("test-stub"),
                    "expected test-stub guidance in reason, got: {reason}"
                );
            }
            other => panic!("expected InvalidConfigValue, got: {other}"),
        }

        std::env::remove_var("RALPH_BURNING_BACKEND");
    }

    #[test]
    fn build_backend_adapter_defaults_to_process_when_env_unset() {
        // Directly cover the unset-env → process default branch.
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::remove_var("RALPH_BURNING_BACKEND");
        let adapter = build_backend_adapter().expect("unset env should default to process");
        assert!(
            matches!(adapter, BackendAdapter::Process(_)),
            "expected BackendAdapter::Process when RALPH_BURNING_BACKEND is unset"
        );
    }

    #[test]
    fn build_backend_adapter_selects_process_when_env_is_process() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("RALPH_BURNING_BACKEND", "process");
        let adapter = build_backend_adapter().expect("explicit process should succeed");
        assert!(matches!(adapter, BackendAdapter::Process(_)));
        std::env::remove_var("RALPH_BURNING_BACKEND");
    }

    #[test]
    fn build_backend_adapter_selects_openrouter_when_env_is_openrouter() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("RALPH_BURNING_BACKEND", "openrouter");
        let adapter = build_backend_adapter().expect("explicit openrouter should succeed");
        assert!(matches!(adapter, BackendAdapter::OpenRouter(_)));
        std::env::remove_var("RALPH_BURNING_BACKEND");
    }

    #[test]
    fn build_backend_adapter_with_config_selects_tmux_for_openrouter_when_tmux_mode_enabled() {
        let _guard = ENV_LOCK.lock().unwrap();
        let effective = tmux_effective_config();

        std::env::set_var("RALPH_BURNING_BACKEND", "openrouter");
        let adapter = build_backend_adapter_with_config(Some(&effective))
            .expect("tmux mode should stay on the tmux adapter stack");
        assert!(
            matches!(adapter, BackendAdapter::Tmux(_)),
            "tmux execution mode must not return the direct OpenRouter adapter"
        );
        std::env::remove_var("RALPH_BURNING_BACKEND");
    }

    #[test]
    fn build_backend_adapter_rejects_invalid_value() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("RALPH_BURNING_BACKEND", "invalid");
        let result = build_backend_adapter();
        assert!(result.is_err());
        std::env::remove_var("RALPH_BURNING_BACKEND");
    }
}
