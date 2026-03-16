//! Shared builder for `BackendAdapter` and `AgentExecutionService`.
//!
//! Centralises runtime adapter selection so that CLI `run`, CLI `requirements`,
//! and daemon requirements dispatch all resolve the same backend family/model
//! defaults from `EffectiveConfig`.  The `process` adapter is the production
//! default and dispatches OpenRouter-resolved requests to the HTTP adapter; the
//! `stub` branch is preserved only as a test seam activated by
//! `RALPH_BURNING_BACKEND=stub`.

use std::collections::HashMap;

use crate::adapters::fs::{FsRawOutputStore, FsRequirementsStore, FsSessionStore};
use crate::adapters::openrouter_backend::OpenRouterBackendAdapter;
use crate::adapters::process_backend::ProcessBackendAdapter;
use crate::adapters::stub_backend::StubBackendAdapter;
use crate::adapters::BackendAdapter;
use crate::contexts::agent_execution::service::{AgentExecutionService, BackendSelectionConfig};
use crate::contexts::requirements_drafting::service::RequirementsService;
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::shared::domain::StageId;
use crate::shared::error::{AppError, AppResult};

/// The concrete `AgentExecutionService` type used by production CLI and daemon paths.
pub type ProductionAgentService =
    AgentExecutionService<BackendAdapter, FsRawOutputStore, FsSessionStore>;

/// The concrete `RequirementsService` type used by production CLI and daemon paths.
pub type ProductionRequirementsService =
    RequirementsService<BackendAdapter, FsRawOutputStore, FsSessionStore, FsRequirementsStore>;

// ── Agent execution service builder ─────────────────────────────────────────

/// Build a `BackendAdapter` from the `RALPH_BURNING_BACKEND` environment variable.
/// Defaults to `process` when the variable is unset.
pub fn build_backend_adapter() -> AppResult<BackendAdapter> {
    let backend_selector = match std::env::var("RALPH_BURNING_BACKEND") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "process".to_owned(),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(AppError::InvalidConfigValue {
                key: "RALPH_BURNING_BACKEND".to_owned(),
                value: "<non-unicode>".to_owned(),
                reason: "expected one of stub, process, openrouter".to_owned(),
            });
        }
    };

    match backend_selector.as_str() {
        "stub" => Ok(BackendAdapter::Stub(build_stub_backend_adapter())),
        "process" => Ok(BackendAdapter::Process(ProcessBackendAdapter::new())),
        "openrouter" => Ok(BackendAdapter::OpenRouter(OpenRouterBackendAdapter::new())),
        other => Err(AppError::InvalidConfigValue {
            key: "RALPH_BURNING_BACKEND".to_owned(),
            value: other.to_owned(),
            reason: "expected one of stub, process, openrouter".to_owned(),
        }),
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

// ── Requirements service builder ────────────────────────────────────────────

/// Build a `RequirementsService` backed by the environment-selected adapter and
/// workspace defaults from `EffectiveConfig`.  Both CLI and daemon requirements
/// paths call this, ensuring consistent backend resolution.
pub fn build_requirements_service(
    effective_config: &EffectiveConfig,
) -> AppResult<ProductionRequirementsService> {
    let adapter = build_backend_adapter()?;

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
fn apply_label_overrides_if_stub(adapter: BackendAdapter) -> BackendAdapter {
    match adapter {
        BackendAdapter::Stub(stub) => BackendAdapter::Stub(apply_label_overrides_to_stub(stub)),
        other => other,
    }
}

/// Apply `RALPH_BURNING_TEST_LABEL_OVERRIDES` to a `StubBackendAdapter`.
fn apply_label_overrides_to_stub(mut adapter: StubBackendAdapter) -> StubBackendAdapter {
    if let Ok(overrides_json) = std::env::var("RALPH_BURNING_TEST_LABEL_OVERRIDES") {
        if let Ok(overrides) =
            serde_json::from_str::<HashMap<String, serde_json::Value>>(&overrides_json)
        {
            for (label, payload) in overrides {
                let full_label = if label.starts_with("requirements:") {
                    label
                } else {
                    format!("requirements:{label}")
                };
                adapter = adapter.with_label_payload(full_label, payload);
            }
        }
    }
    adapter
}

/// Build a `StubBackendAdapter` with all environment-driven test injection seams
/// applied.  This matches the set of seams previously inlined in `cli/run.rs`.
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

    // Also apply label overrides so that stub-backed requirements paths work.
    apply_label_overrides_to_stub(adapter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Serialize tests that mutate `RALPH_BURNING_BACKEND` to avoid races.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn build_backend_adapter_selects_stub_when_env_is_stub() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("RALPH_BURNING_BACKEND", "stub");
        let adapter = build_backend_adapter().expect("stub build should succeed");
        assert!(matches!(adapter, BackendAdapter::Stub(_)));
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
    fn build_backend_adapter_rejects_invalid_value() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("RALPH_BURNING_BACKEND", "invalid");
        let result = build_backend_adapter();
        assert!(result.is_err());
        std::env::remove_var("RALPH_BURNING_BACKEND");
    }
}
