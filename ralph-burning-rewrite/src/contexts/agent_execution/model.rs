use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::Notify;

use crate::contexts::agent_execution::session::SessionMetadata;
use crate::contexts::workflow_composition::contracts::StageContract;
use crate::shared::domain::{
    BackendRole, BackendSpec, ModelSpec, ResolvedBackendTarget, SessionPolicy,
};

#[derive(Clone)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }

        self.notify.notified().await;
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct InvocationPayload {
    pub prompt: String,
    pub context: Value,
}

#[derive(Debug, Clone)]
pub struct InvocationRequest {
    pub invocation_id: String,
    pub project_root: PathBuf,
    pub stage_contract: StageContract,
    pub role: BackendRole,
    pub resolved_target: ResolvedBackendTarget,
    pub payload: InvocationPayload,
    pub timeout: Duration,
    pub cancellation_token: CancellationToken,
    pub session_policy: SessionPolicy,
    pub prior_session: Option<SessionMetadata>,
    pub attempt_number: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityCheck {
    pub backend: BackendSpec,
    pub model: ModelSpec,
    pub stage_id: crate::shared::domain::StageId,
    pub supported: bool,
    pub details: Option<String>,
}

impl CapabilityCheck {
    pub fn success(target: &ResolvedBackendTarget, stage_contract: StageContract) -> Self {
        Self {
            backend: target.backend.clone(),
            model: target.model.clone(),
            stage_id: stage_contract.stage_id,
            supported: true,
            details: None,
        }
    }

    pub fn failure(
        target: &ResolvedBackendTarget,
        stage_contract: StageContract,
        details: impl Into<String>,
    ) -> Self {
        Self {
            backend: target.backend.clone(),
            model: target.model.clone(),
            stage_id: stage_contract.stage_id,
            supported: false,
            details: Some(details.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawOutputReference {
    Inline(String),
    Stored(PathBuf),
}

impl RawOutputReference {
    pub fn inline_contents(&self) -> Option<&str> {
        match self {
            Self::Inline(contents) => Some(contents),
            Self::Stored(_) => None,
        }
    }

    pub fn stored_path(&self) -> Option<&Path> {
        match self {
            Self::Inline(_) => None,
            Self::Stored(path) => Some(path.as_path()),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TokenCounts {
    pub prompt_tokens: Option<u32>,
    pub completion_tokens: Option<u32>,
    pub total_tokens: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvocationMetadata {
    pub invocation_id: String,
    pub duration: Duration,
    pub token_counts: TokenCounts,
    pub backend_used: BackendSpec,
    pub model_used: ModelSpec,
    pub attempt_number: u32,
    pub session_id: Option<String>,
    pub session_reused: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InvocationEnvelope {
    pub raw_output_reference: RawOutputReference,
    pub parsed_payload: Value,
    pub metadata: InvocationMetadata,
    pub timestamp: DateTime<Utc>,
}
