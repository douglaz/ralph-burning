pub mod fs;
pub mod issue_watcher;
pub mod openrouter_backend;
pub mod process_backend;
pub mod stub_backend;
pub mod worktree;

use crate::contexts::agent_execution::model::{
    InvocationContract, InvocationEnvelope, InvocationRequest,
};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::shared::domain::{BackendFamily, ResolvedBackendTarget};
use crate::shared::error::AppResult;

use self::openrouter_backend::OpenRouterBackendAdapter;
use self::process_backend::ProcessBackendAdapter;
use self::stub_backend::StubBackendAdapter;

pub enum BackendAdapter {
    Stub(StubBackendAdapter),
    Process(ProcessBackendAdapter),
    OpenRouter(OpenRouterBackendAdapter),
}

impl AgentExecutionPort for BackendAdapter {
    async fn check_capability(
        &self,
        backend: &ResolvedBackendTarget,
        contract: &InvocationContract,
    ) -> AppResult<()> {
        match self {
            Self::Stub(adapter) => adapter.check_capability(backend, contract).await,
            Self::Process(adapter) => {
                if backend.backend.family == BackendFamily::OpenRouter {
                    OpenRouterBackendAdapter::new()
                        .check_capability(backend, contract)
                        .await
                } else {
                    adapter.check_capability(backend, contract).await
                }
            }
            Self::OpenRouter(adapter) => adapter.check_capability(backend, contract).await,
        }
    }

    async fn check_availability(&self, backend: &ResolvedBackendTarget) -> AppResult<()> {
        match self {
            Self::Stub(adapter) => adapter.check_availability(backend).await,
            Self::Process(adapter) => {
                if backend.backend.family == BackendFamily::OpenRouter {
                    OpenRouterBackendAdapter::new()
                        .check_availability(backend)
                        .await
                } else {
                    adapter.check_availability(backend).await
                }
            }
            Self::OpenRouter(adapter) => adapter.check_availability(backend).await,
        }
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        match self {
            Self::Stub(adapter) => adapter.invoke(request).await,
            Self::Process(adapter) => {
                if request.resolved_target.backend.family == BackendFamily::OpenRouter {
                    OpenRouterBackendAdapter::new().invoke(request).await
                } else {
                    adapter.invoke(request).await
                }
            }
            Self::OpenRouter(adapter) => adapter.invoke(request).await,
        }
    }

    async fn cancel(&self, invocation_id: &str) -> AppResult<()> {
        match self {
            Self::Stub(adapter) => adapter.cancel(invocation_id).await,
            Self::Process(adapter) => adapter.cancel(invocation_id).await,
            Self::OpenRouter(adapter) => adapter.cancel(invocation_id).await,
        }
    }
}
