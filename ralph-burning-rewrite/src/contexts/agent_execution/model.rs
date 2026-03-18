use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::Value;
use tokio::sync::Notify;

use crate::contexts::agent_execution::session::SessionMetadata;
use crate::contexts::requirements_drafting::contracts::RequirementsContract;
use crate::contexts::requirements_drafting::model::RequirementsStageId;
use crate::contexts::workflow_composition::contracts::StageContract;
use crate::shared::domain::{
    BackendRole, BackendSpec, ModelSpec, ResolvedBackendTarget, SessionPolicy, StageId,
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

// ── Domain-neutral contract identifier ──────────────────────────────────────

/// A domain-neutral invocation contract. Wraps either a workflow `StageContract`,
/// a requirements-specific contract label, or a panel-specific contract for
/// prompt-review and completion work.
#[derive(Debug, Clone)]
pub enum InvocationContract {
    /// Workflow stage contract — validated by agent execution.
    Stage(StageContract),
    /// Requirements or other domain contract — caller validates after invocation.
    Requirements { label: String },
    /// Panel-specific contract (prompt-review refiner/validator, completion vote).
    Panel { stage_id: StageId, role: String },
}

impl InvocationContract {
    /// Human-readable label for error messages and logging.
    pub fn label(&self) -> String {
        match self {
            Self::Stage(c) => c.stage_id.as_str().to_owned(),
            Self::Requirements { label } => label.clone(),
            Self::Panel { stage_id, role } => format!("{}:{}", stage_id.as_str(), role),
        }
    }

    /// Returns the `StageId` if this is a workflow stage contract.
    pub fn stage_id(&self) -> Option<StageId> {
        match self {
            Self::Stage(c) => Some(c.stage_id),
            Self::Requirements { .. } => None,
            Self::Panel { stage_id, .. } => Some(*stage_id),
        }
    }

    /// Returns the `StageContract` if this is a workflow stage contract.
    pub fn stage_contract(&self) -> Option<&StageContract> {
        match self {
            Self::Stage(c) => Some(c),
            Self::Requirements { .. } | Self::Panel { .. } => None,
        }
    }

    pub fn family_name(&self) -> &'static str {
        match self {
            Self::Stage(_) => "workflow",
            Self::Requirements { .. } => "requirements",
            Self::Panel { .. } => "panel",
        }
    }

    pub fn json_schema_value(&self) -> Value {
        match self {
            Self::Stage(contract) => serde_json::to_value(contract.json_schema())
                .unwrap_or_else(|_| Value::Object(Default::default())),
            Self::Requirements { label } => requirements_contract_for_label(label)
                .and_then(|contract| serde_json::to_value(contract.json_schema()).ok())
                .unwrap_or_else(|| Value::Object(Default::default())),
            Self::Panel { stage_id, role } => {
                crate::contexts::workflow_composition::panel_contracts::panel_json_schema(
                    *stage_id, role,
                )
            }
        }
    }
}

fn requirements_contract_for_label(label: &str) -> Option<RequirementsContract> {
    let stage = label.strip_prefix("requirements:")?;
    let stage = match stage {
        "question_set" => RequirementsStageId::QuestionSet,
        "requirements_draft" => RequirementsStageId::RequirementsDraft,
        "requirements_review" => RequirementsStageId::RequirementsReview,
        "project_seed" => RequirementsStageId::ProjectSeed,
        "ideation" => RequirementsStageId::Ideation,
        "research" => RequirementsStageId::Research,
        "synthesis" => RequirementsStageId::Synthesis,
        "implementation_spec" => RequirementsStageId::ImplementationSpec,
        "gap_analysis" => RequirementsStageId::GapAnalysis,
        "validation" => RequirementsStageId::Validation,
        _ => return None,
    };

    Some(match stage {
        RequirementsStageId::QuestionSet => RequirementsContract::question_set(),
        RequirementsStageId::RequirementsDraft => RequirementsContract::draft(),
        RequirementsStageId::RequirementsReview => RequirementsContract::review(),
        RequirementsStageId::ProjectSeed => RequirementsContract::seed(),
        RequirementsStageId::Ideation => RequirementsContract::ideation(),
        RequirementsStageId::Research => RequirementsContract::research(),
        RequirementsStageId::Synthesis => RequirementsContract::synthesis(),
        RequirementsStageId::ImplementationSpec => RequirementsContract::implementation_spec(),
        RequirementsStageId::GapAnalysis => RequirementsContract::gap_analysis(),
        RequirementsStageId::Validation => RequirementsContract::validation(),
    })
}

#[derive(Debug, Clone)]
pub struct InvocationRequest {
    pub invocation_id: String,
    pub project_root: PathBuf,
    pub working_dir: PathBuf,
    pub contract: InvocationContract,
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
    pub contract_id: String,
    pub supported: bool,
    pub details: Option<String>,
}

impl CapabilityCheck {
    pub fn success(target: &ResolvedBackendTarget, contract: &InvocationContract) -> Self {
        Self {
            backend: target.backend.clone(),
            model: target.model.clone(),
            contract_id: contract.label(),
            supported: true,
            details: None,
        }
    }

    pub fn failure(
        target: &ResolvedBackendTarget,
        contract: &InvocationContract,
        details: impl Into<String>,
    ) -> Self {
        Self {
            backend: target.backend.clone(),
            model: target.model.clone(),
            contract_id: contract.label(),
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
