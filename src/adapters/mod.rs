pub mod br_health;
pub mod br_models;
pub mod br_process;
pub mod bv_process;
pub mod fs;
pub mod github;
pub mod issue_watcher;
pub mod openrouter_backend;
pub mod process_backend;
#[cfg(feature = "test-stub")]
pub mod stub_backend;
pub mod tmux;
pub mod validation_runner;
pub mod worktree;

use crate::contexts::agent_execution::model::{
    InvocationContract, InvocationEnvelope, InvocationRequest,
};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::shared::domain::ResolvedBackendTarget;
use crate::shared::error::AppResult;

use self::openrouter_backend::OpenRouterBackendAdapter;
use self::process_backend::ProcessBackendAdapter;
#[cfg(feature = "test-stub")]
use self::stub_backend::StubBackendAdapter;
use self::tmux::TmuxAdapter;

#[allow(clippy::large_enum_variant)]
pub enum BackendAdapter {
    #[cfg(feature = "test-stub")]
    Stub(StubBackendAdapter),
    Process(ProcessBackendAdapter),
    Tmux(TmuxAdapter),
    OpenRouter(OpenRouterBackendAdapter),
}

impl AgentExecutionPort for BackendAdapter {
    async fn check_capability(
        &self,
        backend: &ResolvedBackendTarget,
        contract: &InvocationContract,
    ) -> AppResult<()> {
        match self {
            #[cfg(feature = "test-stub")]
            Self::Stub(adapter) => adapter.check_capability(backend, contract).await,
            Self::Process(adapter) => adapter.check_capability(backend, contract).await,
            Self::Tmux(adapter) => adapter.check_capability(backend, contract).await,
            Self::OpenRouter(adapter) => adapter.check_capability(backend, contract).await,
        }
    }

    async fn check_availability(&self, backend: &ResolvedBackendTarget) -> AppResult<()> {
        match self {
            #[cfg(feature = "test-stub")]
            Self::Stub(adapter) => adapter.check_availability(backend).await,
            Self::Process(adapter) => adapter.check_availability(backend).await,
            Self::Tmux(adapter) => adapter.check_availability(backend).await,
            Self::OpenRouter(adapter) => adapter.check_availability(backend).await,
        }
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        match self {
            #[cfg(feature = "test-stub")]
            Self::Stub(adapter) => adapter.invoke(request).await,
            Self::Process(adapter) => adapter.invoke(request).await,
            Self::Tmux(adapter) => adapter.invoke(request).await,
            Self::OpenRouter(adapter) => adapter.invoke(request).await,
        }
    }

    async fn cancel(&self, invocation_id: &str) -> AppResult<()> {
        match self {
            #[cfg(feature = "test-stub")]
            Self::Stub(adapter) => adapter.cancel(invocation_id).await,
            Self::Process(adapter) => adapter.cancel(invocation_id).await,
            Self::Tmux(adapter) => adapter.cancel(invocation_id).await,
            Self::OpenRouter(adapter) => adapter.cancel(invocation_id).await,
        }
    }

    fn enforces_timeout(&self) -> bool {
        match self {
            #[cfg(feature = "test-stub")]
            Self::Stub(adapter) => adapter.enforces_timeout(),
            Self::Process(adapter) => adapter.enforces_timeout(),
            Self::Tmux(adapter) => adapter.enforces_timeout(),
            Self::OpenRouter(adapter) => adapter.enforces_timeout(),
        }
    }
}
