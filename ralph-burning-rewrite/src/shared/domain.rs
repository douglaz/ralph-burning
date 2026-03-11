use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use toml::Table;

use crate::shared::error::{AppError, AppResult};

pub const CURRENT_WORKSPACE_VERSION: u32 = 1;

/// Canonical failure classes for stage contract evaluation and retry semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClass {
    TransportFailure,
    SchemaValidationFailure,
    DomainValidationFailure,
    Timeout,
    Cancellation,
    QaReviewOutcomeFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendFamily {
    Claude,
    Codex,
    OpenRouter,
    Stub,
}

impl BackendFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::OpenRouter => "openrouter",
            Self::Stub => "stub",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::Claude => "Claude",
            Self::Codex => "Codex",
            Self::OpenRouter => "OpenRouter",
            Self::Stub => "Stub",
        }
    }

    pub fn default_model_id(self) -> &'static str {
        match self {
            Self::Claude => "opus-4.1",
            Self::Codex => "gpt-5-codex",
            Self::OpenRouter => "openai/gpt-5",
            Self::Stub => "stub-default",
        }
    }
}

impl fmt::Display for BackendFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for BackendFamily {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "claude" => Ok(Self::Claude),
            "codex" => Ok(Self::Codex),
            "openrouter" => Ok(Self::OpenRouter),
            "stub" => Ok(Self::Stub),
            _ => Err(AppError::InvalidConfigValue {
                key: "backend".to_owned(),
                value: value.to_owned(),
                reason: "expected one of claude, codex, openrouter, stub".to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityMetadata {
    pub supports_structured_output: bool,
    pub supports_session_reuse: bool,
    pub supports_cancellation: bool,
    pub supported_stages: Vec<StageId>,
}

impl CapabilityMetadata {
    pub fn for_all_stages(supports_session_reuse: bool, supports_cancellation: bool) -> Self {
        Self {
            supports_structured_output: true,
            supports_session_reuse,
            supports_cancellation,
            supported_stages: StageId::ALL.to_vec(),
        }
    }

    pub fn supports_stage(&self, stage_id: StageId) -> bool {
        self.supported_stages.contains(&stage_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendSpec {
    pub family: BackendFamily,
    pub display_name: String,
    pub capabilities: CapabilityMetadata,
}

impl BackendSpec {
    pub fn from_family(family: BackendFamily) -> Self {
        let supports_session_reuse = !matches!(family, BackendFamily::OpenRouter);
        Self {
            family,
            display_name: family.display_name().to_owned(),
            capabilities: CapabilityMetadata::for_all_stages(supports_session_reuse, true),
        }
    }
}

impl fmt::Display for BackendSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display_name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelSpec {
    pub backend_family: BackendFamily,
    pub model_id: String,
    pub display_name: String,
    pub capabilities: CapabilityMetadata,
}

impl ModelSpec {
    pub fn new(backend_family: BackendFamily, model_id: impl Into<String>) -> Self {
        let model_id = model_id.into();
        let display_name = format!("{} {}", backend_family.display_name(), model_id);
        let supports_session_reuse = !matches!(backend_family, BackendFamily::OpenRouter);
        Self {
            backend_family,
            model_id,
            display_name,
            capabilities: CapabilityMetadata::for_all_stages(supports_session_reuse, true),
        }
    }

    pub fn default_for_backend(backend_family: BackendFamily) -> Self {
        Self::new(backend_family, backend_family.default_model_id())
    }
}

impl fmt::Display for ModelSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display_name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedBackendTarget {
    pub backend: BackendSpec,
    pub model: ModelSpec,
}

impl ResolvedBackendTarget {
    pub fn new(backend_family: BackendFamily, model_id: impl Into<String>) -> Self {
        Self {
            backend: BackendSpec::from_family(backend_family),
            model: ModelSpec::new(backend_family, model_id),
        }
    }

    pub fn supports_stage(&self, stage_id: StageId) -> bool {
        self.backend.capabilities.supports_stage(stage_id)
            && self.model.capabilities.supports_stage(stage_id)
    }

    pub fn supports_session_reuse(&self) -> bool {
        self.backend.capabilities.supports_session_reuse
            && self.model.capabilities.supports_session_reuse
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendRole {
    Planner,
    Implementer,
    Reviewer,
    QaValidator,
    CompletionJudge,
}

impl BackendRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planner => "planner",
            Self::Implementer => "implementer",
            Self::Reviewer => "reviewer",
            Self::QaValidator => "qa_validator",
            Self::CompletionJudge => "completion_judge",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::Planner => "Planner",
            Self::Implementer => "Implementer",
            Self::Reviewer => "Reviewer",
            Self::QaValidator => "QA Validator",
            Self::CompletionJudge => "Completion Judge",
        }
    }

    pub fn default_target(self) -> ResolvedBackendTarget {
        match self {
            Self::Planner => ResolvedBackendTarget::new(BackendFamily::Claude, "opus-4.1"),
            Self::Implementer => ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5-codex"),
            Self::Reviewer => ResolvedBackendTarget::new(BackendFamily::Claude, "sonnet-4.0"),
            Self::QaValidator => {
                ResolvedBackendTarget::new(BackendFamily::OpenRouter, "openai/gpt-5")
            }
            Self::CompletionJudge => ResolvedBackendTarget::new(BackendFamily::Claude, "opus-4.1"),
        }
    }

    pub fn allows_session_reuse(self) -> bool {
        matches!(self, Self::Implementer | Self::Reviewer | Self::QaValidator)
    }

    pub fn for_stage(stage_id: StageId) -> Self {
        match stage_id {
            StageId::PromptReview | StageId::Planning | StageId::DocsPlan | StageId::CiPlan => {
                Self::Planner
            }
            StageId::Implementation
            | StageId::PlanAndImplement
            | StageId::ApplyFixes
            | StageId::DocsUpdate
            | StageId::CiUpdate => Self::Implementer,
            StageId::Qa
            | StageId::DocsValidation
            | StageId::CiValidation
            | StageId::AcceptanceQa => Self::QaValidator,
            StageId::CompletionPanel => Self::CompletionJudge,
            StageId::Review | StageId::FinalReview => Self::Reviewer,
        }
    }
}

impl fmt::Display for BackendRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPolicy {
    NewSession,
    ReuseIfAllowed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowPreset {
    Standard,
    QuickDev,
    DocsChange,
    CiImprovement,
}

impl FlowPreset {
    pub const ALL: [Self; 4] = [
        Self::Standard,
        Self::QuickDev,
        Self::DocsChange,
        Self::CiImprovement,
    ];

    pub fn all() -> &'static [Self] {
        &Self::ALL
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::QuickDev => "quick_dev",
            Self::DocsChange => "docs_change",
            Self::CiImprovement => "ci_improvement",
        }
    }

    pub fn description(self) -> &'static str {
        match self {
            Self::Standard => {
                "Full delivery flow with planning, implementation, QA, review, and acceptance."
            }
            Self::QuickDev => "Fast delivery flow for small code changes with lightweight review.",
            Self::DocsChange => {
                "Documentation-focused flow for planning, content updates, and validation."
            }
            Self::CiImprovement => {
                "CI improvement flow for automation planning, updates, and validation."
            }
        }
    }
}

impl fmt::Display for FlowPreset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for FlowPreset {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "standard" => Ok(Self::Standard),
            "quick_dev" => Ok(Self::QuickDev),
            "docs_change" => Ok(Self::DocsChange),
            "ci_improvement" => Ok(Self::CiImprovement),
            _ => Err(AppError::InvalidFlowPreset {
                flow_id: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageId {
    PromptReview,
    Planning,
    Implementation,
    Qa,
    Review,
    CompletionPanel,
    AcceptanceQa,
    FinalReview,
    PlanAndImplement,
    ApplyFixes,
    DocsPlan,
    DocsUpdate,
    DocsValidation,
    CiPlan,
    CiUpdate,
    CiValidation,
}

impl StageId {
    pub const ALL: [Self; 16] = [
        Self::PromptReview,
        Self::Planning,
        Self::Implementation,
        Self::Qa,
        Self::Review,
        Self::CompletionPanel,
        Self::AcceptanceQa,
        Self::FinalReview,
        Self::PlanAndImplement,
        Self::ApplyFixes,
        Self::DocsPlan,
        Self::DocsUpdate,
        Self::DocsValidation,
        Self::CiPlan,
        Self::CiUpdate,
        Self::CiValidation,
    ];

    pub fn display_name(self) -> &'static str {
        match self {
            Self::PromptReview => "Prompt Review",
            Self::Planning => "Planning",
            Self::Implementation => "Implementation",
            Self::Qa => "QA",
            Self::Review => "Review",
            Self::CompletionPanel => "Completion Panel",
            Self::AcceptanceQa => "Acceptance QA",
            Self::FinalReview => "Final Review",
            Self::PlanAndImplement => "Plan and Implement",
            Self::ApplyFixes => "Apply Fixes",
            Self::DocsPlan => "Docs Plan",
            Self::DocsUpdate => "Docs Update",
            Self::DocsValidation => "Docs Validation",
            Self::CiPlan => "CI Plan",
            Self::CiUpdate => "CI Update",
            Self::CiValidation => "CI Validation",
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::PromptReview => "prompt_review",
            Self::Planning => "planning",
            Self::Implementation => "implementation",
            Self::Qa => "qa",
            Self::Review => "review",
            Self::CompletionPanel => "completion_panel",
            Self::AcceptanceQa => "acceptance_qa",
            Self::FinalReview => "final_review",
            Self::PlanAndImplement => "plan_and_implement",
            Self::ApplyFixes => "apply_fixes",
            Self::DocsPlan => "docs_plan",
            Self::DocsUpdate => "docs_update",
            Self::DocsValidation => "docs_validation",
            Self::CiPlan => "ci_plan",
            Self::CiUpdate => "ci_update",
            Self::CiValidation => "ci_validation",
        }
    }
}

impl fmt::Display for StageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageCursor {
    pub stage: StageId,
    pub cycle: u32,
    pub attempt: u32,
    pub completion_round: u32,
}

impl StageCursor {
    pub fn new(stage: StageId, cycle: u32, attempt: u32, completion_round: u32) -> AppResult<Self> {
        if cycle == 0 {
            return Err(AppError::InvalidStageCursorField { field: "cycle" });
        }
        if attempt == 0 {
            return Err(AppError::InvalidStageCursorField { field: "attempt" });
        }
        if completion_round == 0 {
            return Err(AppError::InvalidStageCursorField {
                field: "completion_round",
            });
        }

        Ok(Self {
            stage,
            cycle,
            attempt,
            completion_round,
        })
    }

    pub fn initial(stage: StageId) -> Self {
        Self {
            stage,
            cycle: 1,
            attempt: 1,
            completion_round: 1,
        }
    }

    pub fn retry(&self) -> Self {
        Self {
            stage: self.stage,
            cycle: self.cycle,
            attempt: self.attempt + 1,
            completion_round: self.completion_round,
        }
    }

    pub fn advance_stage(&self, stage: StageId) -> Self {
        Self {
            stage,
            cycle: self.cycle,
            attempt: 1,
            completion_round: self.completion_round,
        }
    }

    pub fn advance_cycle(&self, stage: StageId) -> Self {
        Self {
            stage,
            cycle: self.cycle + 1,
            attempt: 1,
            completion_round: self.completion_round,
        }
    }

    pub fn advance_completion_round(&self, stage: StageId) -> Self {
        Self {
            stage,
            cycle: self.cycle,
            attempt: 1,
            completion_round: self.completion_round + 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProjectId(String);

impl ProjectId {
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let normalized = value.into().trim().to_owned();
        if normalized.is_empty()
            || normalized == "."
            || normalized == ".."
            || normalized.contains('/')
            || normalized.contains('\\')
        {
            return Err(AppError::InvalidIdentifier { value: normalized });
        }

        Ok(Self(normalized))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RunId(String);

impl RunId {
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let normalized = value.into().trim().to_owned();
        if normalized.is_empty() {
            return Err(AppError::InvalidIdentifier { value: normalized });
        }

        Ok(Self(normalized))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceConfig {
    pub version: u32,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "WorkspaceSettings::is_empty")]
    pub settings: WorkspaceSettings,
}

impl WorkspaceConfig {
    pub fn new(created_at: DateTime<Utc>) -> Self {
        Self {
            version: CURRENT_WORKSPACE_VERSION,
            created_at,
            settings: WorkspaceSettings::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct WorkspaceSettings {
    #[serde(default, skip_serializing_if = "PromptReviewSettings::is_empty")]
    pub prompt_review: PromptReviewSettings,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_flow: Option<FlowPreset>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(flatten)]
    pub extra: Table,
}

impl WorkspaceSettings {
    pub fn is_empty(&self) -> bool {
        self.prompt_review.is_empty()
            && self.default_flow.is_none()
            && self.default_backend.is_none()
            && self.default_model.is_none()
            && self.extra.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PromptReviewSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(flatten)]
    pub extra: Table,
}

impl PromptReviewSettings {
    pub fn is_empty(&self) -> bool {
        self.enabled.is_none() && self.extra.is_empty()
    }
}
