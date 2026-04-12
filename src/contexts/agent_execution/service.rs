use std::path::Path;
use std::time::{Duration, Instant};

use chrono::Utc;
use serde_json::Value;

use crate::contexts::agent_execution::model::{
    CapabilityCheck, InvocationContract, InvocationEnvelope, InvocationRequest, RawOutputReference,
};
use crate::contexts::agent_execution::session::{SessionManager, SessionStorePort};
use crate::contexts::workspace_governance::EffectiveConfig;
use crate::shared::domain::{
    BackendFamily, BackendRole, BackendSpec, FailureClass, ModelSpec, ResolvedBackendTarget,
};
use crate::shared::error::{AppError, AppResult, ContractError};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BackendSelectionConfig {
    pub backend_family: Option<BackendFamily>,
    pub model_id: Option<String>,
}

impl BackendSelectionConfig {
    pub fn from_effective_config(config: &EffectiveConfig) -> AppResult<Self> {
        Ok(Self {
            backend_family: config
                .default_backend()
                .map(|value| parse_backend_family("default_backend", value))
                .transpose()?,
            model_id: config.default_model().map(|value| value.to_owned()),
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct BackendResolver;

impl BackendResolver {
    pub fn new() -> Self {
        Self
    }

    pub fn resolve(
        &self,
        role: BackendRole,
        explicit_model_override: Option<ModelSpec>,
        project_config: Option<&BackendSelectionConfig>,
        workspace_defaults: Option<&BackendSelectionConfig>,
    ) -> AppResult<ResolvedBackendTarget> {
        let mut target = role.default_target();

        if let Some(workspace_defaults) = workspace_defaults {
            Self::apply_selection(role, &mut target, workspace_defaults);
        }
        if let Some(project_config) = project_config {
            Self::apply_selection(role, &mut target, project_config);
        }
        if let Some(model_override) = explicit_model_override {
            Self::apply_model_override(&mut target, model_override);
        }

        Ok(target)
    }

    pub fn resolve_from_effective_config(
        &self,
        role: BackendRole,
        explicit_model_override: Option<ModelSpec>,
        project_config: Option<&BackendSelectionConfig>,
        effective_config: &EffectiveConfig,
    ) -> AppResult<ResolvedBackendTarget> {
        let workspace_defaults = BackendSelectionConfig::from_effective_config(effective_config)?;
        self.resolve(
            role,
            explicit_model_override,
            project_config,
            Some(&workspace_defaults),
        )
    }

    fn apply_selection(
        role: BackendRole,
        target: &mut ResolvedBackendTarget,
        selection: &BackendSelectionConfig,
    ) {
        if let Some(backend_family) = selection.backend_family {
            target.backend = BackendSpec::from_family(backend_family);
            if target.model.backend_family != backend_family {
                let default_model = if role.default_target().backend.family == backend_family {
                    role.default_target().model.model_id
                } else {
                    backend_family.default_model_id().to_owned()
                };
                target.model = ModelSpec::new(backend_family, default_model);
            }
        }

        if let Some(model_id) = &selection.model_id {
            target.model = ModelSpec::new(target.backend.family, model_id.clone());
        }
    }

    fn apply_model_override(target: &mut ResolvedBackendTarget, model_override: ModelSpec) {
        if target.backend.family != model_override.backend_family {
            target.backend = BackendSpec::from_family(model_override.backend_family);
        }
        target.model = model_override;
    }
}

#[allow(async_fn_in_trait)]
pub trait AgentExecutionPort {
    async fn check_capability(
        &self,
        backend: &ResolvedBackendTarget,
        contract: &InvocationContract,
    ) -> AppResult<()>;

    async fn check_availability(&self, backend: &ResolvedBackendTarget) -> AppResult<()>;

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope>;

    async fn cancel(&self, invocation_id: &str) -> AppResult<()>;

    /// Whether this adapter enforces `request.timeout` internally (e.g. via
    /// process-level timeout).  When `true`, the service adds a buffer to
    /// `service_timeout` so the adapter's timeout fires first.  When `false`
    /// (default), the service-level timeout *is* the canonical timeout.
    fn enforces_timeout(&self) -> bool {
        false
    }
}

pub trait RawOutputPort {
    fn persist_raw_output(
        &self,
        project_root: &Path,
        invocation_id: &str,
        contents: &str,
    ) -> AppResult<RawOutputReference>;
}

pub struct AgentExecutionService<A, R, S> {
    adapter: A,
    raw_output_store: R,
    session_manager: SessionManager<S>,
    resolver: BackendResolver,
}

impl<A, R, S> AgentExecutionService<A, R, S> {
    pub fn new(adapter: A, raw_output_store: R, session_store: S) -> Self {
        Self {
            adapter,
            raw_output_store,
            session_manager: SessionManager::new(session_store),
            resolver: BackendResolver::new(),
        }
    }

    pub fn resolver(&self) -> &BackendResolver {
        &self.resolver
    }

    pub fn adapter(&self) -> &A {
        &self.adapter
    }
}

impl<A, R, S> AgentExecutionService<A, R, S>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    pub async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        let _ = CapabilityCheck::success(&request.resolved_target, &request.contract);

        self.adapter
            .check_capability(&request.resolved_target, &request.contract)
            .await
            .map_err(|error| map_capability_error(error, &request))?;

        self.adapter
            .check_availability(&request.resolved_target)
            .await
            .map_err(|error| map_availability_error(error, &request))?;

        let prior_session = self.session_manager.load_reusable_session(
            &request.project_root,
            request.role,
            &request.resolved_target,
            request.session_policy,
        )?;

        let mut request = request;
        request.prior_session = prior_session;

        if request.cancellation_token.is_cancelled() {
            return Err(AppError::InvocationCancelled {
                backend: request.resolved_target.backend.family.to_string(),
                contract_id: request.contract.label(),
            });
        }

        let started_at = Utc::now();
        let started = Instant::now();
        let invocation_id = request.invocation_id.clone();
        let timeout_ms = request.timeout.as_millis().min(u64::MAX as u128) as u64;

        let invoke_future = self.adapter.invoke(request.clone());
        tokio::pin!(invoke_future);

        // When the adapter enforces its own hard timeout (e.g. process-level
        // kill), adding a 30-second buffer lets the adapter fire first and
        // handle artifact preservation / child cleanup.  When the adapter
        // does *not* enforce a timeout, the service-level timeout is the
        // canonical enforcement and must match `request.timeout` exactly.
        let service_timeout = if self.adapter.enforces_timeout() {
            request.timeout.saturating_add(Duration::from_secs(30))
        } else {
            request.timeout
        };

        let mut envelope = tokio::select! {
            _ = request.cancellation_token.cancelled() => {
                let _ = self.adapter.cancel(&invocation_id).await;
                return Err(AppError::InvocationCancelled {
                    backend: request.resolved_target.backend.family.to_string(),
                    contract_id: request.contract.label(),
                });
            }
            result = tokio::time::timeout(service_timeout, &mut invoke_future) => {
                match result {
                    Ok(result) => result.map_err(|error| map_invoke_error(error, &request))?,
                    Err(_) => {
                        let _ = self.adapter.cancel(&invocation_id).await;
                        return Err(AppError::InvocationTimeout {
                            backend: request.resolved_target.backend.family.to_string(),
                            contract_id: request.contract.label(),
                            timeout_ms,
                            details: "service-level safety-net timeout fired".to_owned(),
                        });
                    }
                }
            }
        };

        let duration = started.elapsed();
        let adapter_reported_backend = Some(envelope.metadata.backend_used.clone());
        let adapter_reported_model = Some(envelope.metadata.model_used.clone());
        envelope.metadata.duration = duration;
        envelope.metadata.attempt_number = request.attempt_number;
        envelope.metadata.adapter_reported_backend = adapter_reported_backend;
        envelope.metadata.adapter_reported_model = adapter_reported_model;
        envelope.metadata.backend_used = request.resolved_target.backend.clone();
        envelope.metadata.model_used = request.resolved_target.model.clone();
        envelope.timestamp = Utc::now();

        // Emit structured trace immediately after metadata finalization, before
        // any fallible post-processing (persist, extract, validate) so that
        // token/cache data is always recorded even for malformed responses.
        let tc = &envelope.metadata.token_counts;
        tracing::info!(
            invocation_id = %envelope.metadata.invocation_id,
            backend = %envelope.metadata.backend_used,
            model = %envelope.metadata.model_used,
            attempt = envelope.metadata.attempt_number,
            duration_ms = duration.as_millis() as u64,
            prompt_tokens = tc.prompt_tokens.map(|v| v as i64).unwrap_or(-1),
            completion_tokens = tc.completion_tokens.map(|v| v as i64).unwrap_or(-1),
            cache_read_tokens = tc.cache_read_tokens.map(|v| v as i64).unwrap_or(-1),
            cache_creation_tokens = tc.cache_creation_tokens.map(|v| v as i64).unwrap_or(-1),
            tokens_reported = tc.prompt_tokens.is_some() || tc.completion_tokens.is_some(),
            cache_reported = tc.cache_read_tokens.is_some() || tc.cache_creation_tokens.is_some(),
            session_reused = envelope.metadata.session_reused,
            "invocation completed"
        );

        let raw_output = extract_raw_output(&envelope.raw_output_reference)?;
        let stored_reference = self.raw_output_store.persist_raw_output(
            &request.project_root,
            &request.invocation_id,
            &raw_output,
        )?;

        let parsed_payload =
            extract_structured_payload(&request, envelope.parsed_payload, &raw_output)?;

        // Only validate stage contracts within agent execution;
        // requirements contracts are validated by the caller.
        if let Some(stage_contract) = request.contract.stage_contract() {
            stage_contract
                .evaluate_permissive(&parsed_payload)
                .map_err(|error| map_contract_error(error, &request))?;
        }

        envelope.raw_output_reference = stored_reference;
        envelope.parsed_payload = parsed_payload;
        envelope.timestamp = started_at + duration;

        self.session_manager.record_session(
            &request.project_root,
            request.role,
            &request.resolved_target,
            envelope.metadata.session_id.as_deref(),
            envelope.timestamp,
        )?;

        Ok(envelope)
    }
}

fn parse_backend_family(key: &str, value: &str) -> AppResult<BackendFamily> {
    match value {
        "claude" => Ok(BackendFamily::Claude),
        "codex" => Ok(BackendFamily::Codex),
        "openrouter" => Ok(BackendFamily::OpenRouter),
        "stub" => Ok(BackendFamily::Stub),
        _ => Err(AppError::InvalidConfigValue {
            key: key.to_owned(),
            value: value.to_owned(),
            reason: "expected one of claude, codex, openrouter, stub".to_owned(),
        }),
    }
}

fn extract_raw_output(raw_output_reference: &RawOutputReference) -> AppResult<String> {
    match raw_output_reference {
        RawOutputReference::Inline(contents) => Ok(contents.clone()),
        RawOutputReference::Stored(path) => std::fs::read_to_string(path).map_err(AppError::from),
    }
}

fn extract_structured_payload(
    request: &InvocationRequest,
    parsed_payload: Value,
    raw_output: &str,
) -> AppResult<Value> {
    if !parsed_payload.is_null() {
        return Ok(parsed_payload);
    }

    serde_json::from_str(raw_output).map_err(|error| AppError::InvocationFailed {
        backend: request.resolved_target.backend.family.to_string(),
        contract_id: request.contract.label(),
        failure_class: FailureClass::SchemaValidationFailure,
        details: error.to_string(),
    })
}

fn map_capability_error(error: AppError, request: &InvocationRequest) -> AppError {
    match error {
        AppError::CapabilityMismatch { .. } => error,
        other => AppError::CapabilityMismatch {
            backend: request.resolved_target.backend.family.to_string(),
            contract_id: request.contract.label(),
            details: other.to_string(),
        },
    }
}

fn map_availability_error(error: AppError, request: &InvocationRequest) -> AppError {
    match error {
        AppError::BackendUnavailable { .. } => error,
        other => AppError::BackendUnavailable {
            backend: request.resolved_target.backend.family.to_string(),
            details: other.to_string(),
            failure_class: None,
        },
    }
}

fn map_invoke_error(error: AppError, request: &InvocationRequest) -> AppError {
    match error {
        // Adapter-enforced process timeouts arrive as InvocationFailed with
        // FailureClass::Timeout.  Promote them to InvocationTimeout so that
        // consumers (e.g. worktree rebase) can match on the canonical variant.
        // The adapter-level details (e.g. "claude exceeded timeout of 600s")
        // are preserved for operator diagnostics.
        AppError::InvocationFailed {
            failure_class: FailureClass::Timeout,
            details,
            ..
        } => AppError::InvocationTimeout {
            backend: request.resolved_target.backend.family.to_string(),
            contract_id: request.contract.label(),
            timeout_ms: request.timeout.as_millis().min(u64::MAX as u128) as u64,
            details,
        },
        AppError::InvocationFailed { .. }
        | AppError::InvocationTimeout { .. }
        | AppError::InvocationCancelled { .. }
        | AppError::BackendUnavailable { .. } => error,
        other => AppError::InvocationFailed {
            backend: request.resolved_target.backend.family.to_string(),
            contract_id: request.contract.label(),
            failure_class: FailureClass::TransportFailure,
            details: other.to_string(),
        },
    }
}

fn map_contract_error(error: ContractError, request: &InvocationRequest) -> AppError {
    AppError::InvocationFailed {
        backend: request.resolved_target.backend.family.to_string(),
        contract_id: request.contract.label(),
        failure_class: error.failure_class(),
        details: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use chrono::Utc;
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::contexts::agent_execution::model::{
        CancellationToken, InvocationContract, InvocationMetadata, InvocationPayload, TokenCounts,
    };
    use crate::contexts::agent_execution::session::PersistedSessions;
    use crate::shared::domain::SessionPolicy;

    #[derive(Clone)]
    struct ReportingAdapter {
        reported_backend: BackendSpec,
        reported_model: ModelSpec,
    }

    impl ReportingAdapter {
        fn new(reported_backend: BackendSpec, reported_model: ModelSpec) -> Self {
            Self {
                reported_backend,
                reported_model,
            }
        }
    }

    impl AgentExecutionPort for ReportingAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> AppResult<()> {
            Ok(())
        }

        async fn check_availability(&self, _backend: &ResolvedBackendTarget) -> AppResult<()> {
            Ok(())
        }

        async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
            Ok(InvocationEnvelope {
                raw_output_reference: RawOutputReference::Inline(r#"{"status":"ok"}"#.to_owned()),
                parsed_payload: json!({"status": "ok"}),
                metadata: InvocationMetadata {
                    invocation_id: request.invocation_id.clone(),
                    duration: Duration::ZERO,
                    token_counts: TokenCounts::default(),
                    backend_used: self.reported_backend.clone(),
                    model_used: self.reported_model.clone(),
                    adapter_reported_backend: None,
                    adapter_reported_model: None,
                    attempt_number: 0,
                    session_id: None,
                    session_reused: false,
                },
                timestamp: Utc::now(),
            })
        }

        async fn cancel(&self, _invocation_id: &str) -> AppResult<()> {
            Ok(())
        }
    }

    struct InlineRawOutputStore;

    impl RawOutputPort for InlineRawOutputStore {
        fn persist_raw_output(
            &self,
            _project_root: &Path,
            _invocation_id: &str,
            contents: &str,
        ) -> AppResult<RawOutputReference> {
            Ok(RawOutputReference::Inline(contents.to_owned()))
        }
    }

    struct NoopSessionStore;

    impl SessionStorePort for NoopSessionStore {
        fn load_sessions(&self, _project_root: &Path) -> AppResult<PersistedSessions> {
            Ok(PersistedSessions::empty())
        }

        fn save_sessions(
            &self,
            _project_root: &Path,
            _sessions: &PersistedSessions,
        ) -> AppResult<()> {
            Ok(())
        }
    }

    fn request_fixture(
        project_root: PathBuf,
        resolved_target: ResolvedBackendTarget,
    ) -> InvocationRequest {
        InvocationRequest {
            invocation_id: "service-invoke-1".to_owned(),
            project_root: project_root.clone(),
            working_dir: project_root,
            contract: InvocationContract::Requirements {
                label: "requirements:project_seed".to_owned(),
            },
            role: BackendRole::Planner,
            resolved_target,
            payload: InvocationPayload {
                prompt: "Prompt".to_owned(),
                context: json!({}),
            },
            timeout: Duration::from_secs(5),
            cancellation_token: CancellationToken::new(),
            session_policy: SessionPolicy::NewSession,
            prior_session: None,
            attempt_number: 3,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invoke_preserves_adapter_reported_values_before_normalizing_target() {
        let temp_dir = tempdir().expect("create temp dir");
        let resolved_target = ResolvedBackendTarget::new(BackendFamily::Claude, "claude-opus-4-6");
        let request = request_fixture(temp_dir.path().to_path_buf(), resolved_target.clone());
        let service = AgentExecutionService::new(
            ReportingAdapter::new(
                BackendSpec::from_family(BackendFamily::OpenRouter),
                ModelSpec::new(BackendFamily::OpenRouter, "openai/gpt-4.1"),
            ),
            InlineRawOutputStore,
            NoopSessionStore,
        );

        let envelope = service.invoke(request).await.expect("invoke succeeds");

        assert_eq!(envelope.metadata.backend_used.family, BackendFamily::Claude);
        assert_eq!(envelope.metadata.model_used.model_id, "claude-opus-4-6");
        assert_eq!(
            envelope
                .metadata
                .adapter_reported_backend
                .as_ref()
                .map(|backend| backend.family),
            Some(BackendFamily::OpenRouter)
        );
        assert_eq!(
            envelope
                .metadata
                .adapter_reported_model
                .as_ref()
                .map(|model| (model.backend_family, model.model_id.as_str())),
            Some((BackendFamily::OpenRouter, "openai/gpt-4.1"))
        );
        assert_eq!(envelope.metadata.attempt_number, 3);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invoke_always_populates_adapter_reported_values() {
        let temp_dir = tempdir().expect("create temp dir");
        let resolved_target = ResolvedBackendTarget::new(BackendFamily::Claude, "claude-opus-4-6");
        let request = request_fixture(temp_dir.path().to_path_buf(), resolved_target.clone());
        let service = AgentExecutionService::new(
            ReportingAdapter::new(
                resolved_target.backend.clone(),
                resolved_target.model.clone(),
            ),
            InlineRawOutputStore,
            NoopSessionStore,
        );

        let envelope = service.invoke(request).await.expect("invoke succeeds");

        // adapter_reported fields are always populated, even when matching the target
        assert_eq!(
            envelope
                .metadata
                .adapter_reported_backend
                .as_ref()
                .map(|b| b.family),
            Some(BackendFamily::Claude)
        );
        assert_eq!(
            envelope
                .metadata
                .adapter_reported_model
                .as_ref()
                .map(|m| m.model_id.as_str()),
            Some("claude-opus-4-6")
        );
        assert_eq!(envelope.metadata.backend_used.family, BackendFamily::Claude);
        assert_eq!(envelope.metadata.model_used.model_id, "claude-opus-4-6");
    }
}
