use std::collections::HashMap;
use std::sync::Arc;

use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::contexts::agent_execution::model::{
    InvocationContract, InvocationEnvelope, InvocationRequest,
};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::shared::domain::{BackendFamily, FailureClass, ResolvedBackendTarget};
use crate::shared::error::{AppError, AppResult};

#[derive(Clone, Default)]
pub struct ProcessBackendAdapter {
    pub active_children: Arc<Mutex<HashMap<String, Child>>>,
}

impl ProcessBackendAdapter {
    pub fn new() -> Self {
        Self {
            active_children: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn binary_name(backend: BackendFamily) -> Option<&'static str> {
        match backend {
            BackendFamily::Claude => Some("claude"),
            BackendFamily::Codex => Some("codex"),
            BackendFamily::OpenRouter | BackendFamily::Stub => None,
        }
    }

    fn capability_mismatch(
        backend: &ResolvedBackendTarget,
        contract: &InvocationContract,
        details: impl Into<String>,
    ) -> AppError {
        AppError::CapabilityMismatch {
            backend: backend.backend.family.to_string(),
            contract_id: contract.label(),
            details: details.into(),
        }
    }
}

impl AgentExecutionPort for ProcessBackendAdapter {
    async fn check_capability(
        &self,
        backend: &ResolvedBackendTarget,
        contract: &InvocationContract,
    ) -> AppResult<()> {
        match (backend.backend.family, contract) {
            (BackendFamily::Claude | BackendFamily::Codex, InvocationContract::Stage(_)) => Ok(()),
            (_, InvocationContract::Requirements { .. }) => Err(Self::capability_mismatch(
                backend,
                contract,
                "ProcessBackendAdapter currently supports workflow stage invocations only",
            )),
            (BackendFamily::OpenRouter | BackendFamily::Stub, _) => Err(Self::capability_mismatch(
                backend,
                contract,
                "ProcessBackendAdapter currently supports only claude and codex workflow stage backends",
            )),
        }
    }

    async fn check_availability(&self, backend: &ResolvedBackendTarget) -> AppResult<()> {
        let Some(binary_name) = Self::binary_name(backend.backend.family) else {
            return Err(AppError::BackendUnavailable {
                backend: backend.backend.family.to_string(),
                details: "ProcessBackendAdapter availability checks are only supported for claude and codex".to_owned(),
            });
        };

        let output = Command::new("which")
            .arg(binary_name)
            .output()
            .await
            .map_err(|error| AppError::BackendUnavailable {
                backend: backend.backend.family.to_string(),
                details: format!("failed to probe PATH for '{binary_name}': {error}"),
            })?;

        if output.status.success() {
            Ok(())
        } else {
            Err(AppError::BackendUnavailable {
                backend: backend.backend.family.to_string(),
                details: format!("required binary '{binary_name}' was not found on PATH"),
            })
        }
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        Err(AppError::InvocationFailed {
            backend: request.resolved_target.backend.family.to_string(),
            contract_id: request.contract.label(),
            failure_class: FailureClass::TransportFailure,
            details: "ProcessBackendAdapter invoke not yet implemented".to_owned(),
        })
    }

    async fn cancel(&self, invocation_id: &str) -> AppResult<()> {
        let mut active_children = self.active_children.lock().await;
        let Some(child) = active_children.get_mut(invocation_id) else {
            return Ok(());
        };
        let Some(pid) = child.id() else {
            return Ok(());
        };

        let status = Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .await
            .map_err(|error| AppError::InvocationFailed {
                backend: "process".to_owned(),
                contract_id: invocation_id.to_owned(),
                failure_class: FailureClass::TransportFailure,
                details: format!("failed to send SIGTERM to invocation '{invocation_id}': {error}"),
            })?;

        if !status.success() {
            return Err(AppError::InvocationFailed {
                backend: "process".to_owned(),
                contract_id: invocation_id.to_owned(),
                failure_class: FailureClass::TransportFailure,
                details: format!("kill -TERM exited unsuccessfully for invocation '{invocation_id}'"),
            });
        }

        Ok(())
    }
}
