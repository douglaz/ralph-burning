use std::path::Path;
use std::time::Instant;

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
            Self::apply_selection(&mut target, workspace_defaults);
        }
        if let Some(project_config) = project_config {
            Self::apply_selection(&mut target, project_config);
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

    fn apply_selection(target: &mut ResolvedBackendTarget, selection: &BackendSelectionConfig) {
        if let Some(backend_family) = selection.backend_family {
            target.backend = BackendSpec::from_family(backend_family);
            if target.model.backend_family != backend_family {
                target.model = ModelSpec::default_for_backend(backend_family);
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

        let mut envelope = tokio::select! {
            _ = request.cancellation_token.cancelled() => {
                let _ = self.adapter.cancel(&invocation_id).await;
                return Err(AppError::InvocationCancelled {
                    backend: request.resolved_target.backend.family.to_string(),
                    contract_id: request.contract.label(),
                });
            }
            result = tokio::time::timeout(request.timeout, &mut invoke_future) => {
                match result {
                    Ok(result) => result.map_err(|error| map_invoke_error(error, &request))?,
                    Err(_) => {
                        let _ = self.adapter.cancel(&invocation_id).await;
                        return Err(AppError::InvocationTimeout {
                            backend: request.resolved_target.backend.family.to_string(),
                            contract_id: request.contract.label(),
                            timeout_ms,
                        });
                    }
                }
            }
        };

        let duration = started.elapsed();
        envelope.metadata.duration = duration;
        envelope.metadata.attempt_number = request.attempt_number;
        envelope.metadata.backend_used = request.resolved_target.backend.clone();
        envelope.metadata.model_used = request.resolved_target.model.clone();
        envelope.timestamp = Utc::now();

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
        },
    }
}

fn map_invoke_error(error: AppError, request: &InvocationRequest) -> AppError {
    match error {
        AppError::InvocationFailed { .. }
        | AppError::InvocationTimeout { .. }
        | AppError::InvocationCancelled { .. } => error,
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
