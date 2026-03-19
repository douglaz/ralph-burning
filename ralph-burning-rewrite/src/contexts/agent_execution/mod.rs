pub mod diagnostics;
pub mod model;
pub mod policy;
pub mod service;
pub mod session;

pub const CONTEXT_NAME: &str = "agent_execution";

pub use model::{
    CancellationToken, CapabilityCheck, InvocationContract, InvocationEnvelope, InvocationMetadata,
    InvocationPayload, InvocationRequest, RawOutputReference, TokenCounts,
};
pub use diagnostics::BackendDiagnosticsService;
pub use policy::{
    BackendPolicyService, CompletionPanelResolution, FinalReviewPanelResolution,
    PromptReviewPanelResolution, ResolvedPanelMember, stage_to_policy_role,
};
pub use service::{
    AgentExecutionPort, AgentExecutionService, BackendResolver, BackendSelectionConfig,
    RawOutputPort,
};
pub use session::{PersistedSessions, SessionManager, SessionMetadata, SessionStorePort};
