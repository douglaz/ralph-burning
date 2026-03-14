pub mod fs;
pub mod issue_watcher;
pub mod process_backend;
pub mod stub_backend;
pub mod worktree;

use crate::contexts::agent_execution::model::{
    InvocationContract, InvocationEnvelope, InvocationRequest,
};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::shared::domain::ResolvedBackendTarget;
use crate::shared::error::AppResult;

use self::process_backend::ProcessBackendAdapter;
use self::stub_backend::StubBackendAdapter;

pub enum BackendAdapter {
    Stub(StubBackendAdapter),
    Process(ProcessBackendAdapter),
}

impl AgentExecutionPort for BackendAdapter {
    async fn check_capability(
        &self,
        backend: &ResolvedBackendTarget,
        contract: &InvocationContract,
    ) -> AppResult<()> {
        match self {
            Self::Stub(adapter) => adapter.check_capability(backend, contract).await,
            Self::Process(adapter) => adapter.check_capability(backend, contract).await,
        }
    }

    async fn check_availability(&self, backend: &ResolvedBackendTarget) -> AppResult<()> {
        match self {
            Self::Stub(adapter) => adapter.check_availability(backend).await,
            Self::Process(adapter) => adapter.check_availability(backend).await,
        }
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        match self {
            Self::Stub(adapter) => adapter.invoke(request).await,
            Self::Process(adapter) => adapter.invoke(request).await,
        }
    }

    async fn cancel(&self, invocation_id: &str) -> AppResult<()> {
        match self {
            Self::Stub(adapter) => adapter.cancel(invocation_id).await,
            Self::Process(adapter) => adapter.cancel(invocation_id).await,
        }
    }
}
