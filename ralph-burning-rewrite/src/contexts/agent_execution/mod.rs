pub mod model;
pub mod policy;
pub mod service;
pub mod session;

pub const CONTEXT_NAME: &str = "agent_execution";

pub use model::{
    CancellationToken, CapabilityCheck, InvocationContract, InvocationEnvelope, InvocationMetadata,
    InvocationPayload, InvocationRequest, RawOutputReference, TokenCounts,
};
pub use policy::{
    BackendPolicyService, CompletionPanelResolution, FinalReviewPanelResolution,
    PromptReviewPanelResolution,
};
pub use service::{
    AgentExecutionPort, AgentExecutionService, BackendResolver, BackendSelectionConfig,
    RawOutputPort,
};
pub use session::{PersistedSessions, SessionManager, SessionMetadata, SessionStorePort};
