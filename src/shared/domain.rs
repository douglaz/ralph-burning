use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
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
    /// Terminal: required binary or infrastructure prerequisite is missing.
    /// Also used for fatal configuration issues (e.g. missing API key) that
    /// won't resolve between retry attempts.
    BinaryNotFound,
    /// Terminal: the backend has exhausted credits, hit a persistent usage
    /// limit, or is otherwise unavailable for reasons that won't resolve
    /// between retry attempts within the same run.
    BackendExhausted,
}

impl FailureClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TransportFailure => "transport_failure",
            Self::SchemaValidationFailure => "schema_validation_failure",
            Self::DomainValidationFailure => "domain_validation_failure",
            Self::Timeout => "timeout",
            Self::Cancellation => "cancellation",
            Self::QaReviewOutcomeFailure => "qa_review_outcome_failure",
            Self::BinaryNotFound => "binary_not_found",
            Self::BackendExhausted => "backend_exhausted",
        }
    }
}

impl fmt::Display for FailureClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
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
            Self::Claude => "claude-opus-4-6",
            Self::Codex => "gpt-5.4",
            Self::OpenRouter => "openai/gpt-5.4",
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
            Self::Planner => ResolvedBackendTarget::new(BackendFamily::Claude, "claude-opus-4-6"),
            Self::Implementer => {
                ResolvedBackendTarget::new(BackendFamily::Claude, "claude-opus-4-6")
            }
            Self::Reviewer => ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.4"),
            Self::QaValidator => {
                ResolvedBackendTarget::new(BackendFamily::OpenRouter, "openai/gpt-5.4")
            }
            Self::CompletionJudge => {
                ResolvedBackendTarget::new(BackendFamily::Claude, "claude-opus-4-6")
            }
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
            StageId::CompletionPanel | StageId::FinalReview => Self::CompletionJudge,
            StageId::Review => Self::Reviewer,
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
pub enum BackendPolicyRole {
    Planner,
    Implementer,
    Reviewer,
    Qa,
    Completer,
    FinalReviewer,
    PromptReviewer,
    PromptValidator,
    Arbiter,
    AcceptanceQa,
}

impl BackendPolicyRole {
    pub const ALL: [Self; 10] = [
        Self::Planner,
        Self::Implementer,
        Self::Reviewer,
        Self::Qa,
        Self::Completer,
        Self::FinalReviewer,
        Self::PromptReviewer,
        Self::PromptValidator,
        Self::Arbiter,
        Self::AcceptanceQa,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planner => "planner",
            Self::Implementer => "implementer",
            Self::Reviewer => "reviewer",
            Self::Qa => "qa",
            Self::Completer => "completer",
            Self::FinalReviewer => "final_reviewer",
            Self::PromptReviewer => "prompt_reviewer",
            Self::PromptValidator => "prompt_validator",
            Self::Arbiter => "arbiter",
            Self::AcceptanceQa => "acceptance_qa",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::Planner => "Planner",
            Self::Implementer => "Implementer",
            Self::Reviewer => "Reviewer",
            Self::Qa => "QA",
            Self::Completer => "Completer",
            Self::FinalReviewer => "Final Reviewer",
            Self::PromptReviewer => "Prompt Reviewer",
            Self::PromptValidator => "Prompt Validator",
            Self::Arbiter => "Arbiter",
            Self::AcceptanceQa => "Acceptance QA",
        }
    }
}

impl fmt::Display for BackendPolicyRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for BackendPolicyRole {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "planner" => Ok(Self::Planner),
            "implementer" => Ok(Self::Implementer),
            "reviewer" => Ok(Self::Reviewer),
            "qa" => Ok(Self::Qa),
            "completer" => Ok(Self::Completer),
            "final_reviewer" => Ok(Self::FinalReviewer),
            "prompt_reviewer" => Ok(Self::PromptReviewer),
            "prompt_validator" => Ok(Self::PromptValidator),
            "arbiter" => Ok(Self::Arbiter),
            "acceptance_qa" => Ok(Self::AcceptanceQa),
            _ => Err(AppError::InvalidConfigValue {
                key: "backend_role".to_owned(),
                value: value.to_owned(),
                reason: "unknown backend role".to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BackendSelection {
    pub family: BackendFamily,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

impl BackendSelection {
    pub fn new(family: BackendFamily, model: Option<String>) -> Self {
        Self { family, model }
    }

    pub fn from_backend_name(value: &str) -> AppResult<Self> {
        let normalized = value.trim();
        if normalized.is_empty() {
            return Err(AppError::InvalidConfigValue {
                key: "backend".to_owned(),
                value: value.to_owned(),
                reason: "backend name must not be empty".to_owned(),
            });
        }

        if let Some((backend, model)) = normalized.split_once('(') {
            let model = model
                .strip_suffix(')')
                .ok_or_else(|| AppError::InvalidConfigValue {
                    key: "backend".to_owned(),
                    value: value.to_owned(),
                    reason: "backend model spec must end with ')'".to_owned(),
                })?;
            let family = backend.trim().parse::<BackendFamily>()?;
            let model = model.trim();
            if model.is_empty() {
                return Err(AppError::InvalidConfigValue {
                    key: "backend".to_owned(),
                    value: value.to_owned(),
                    reason: "backend model spec must not be empty".to_owned(),
                });
            }
            Ok(Self::new(family, Some(model.to_owned())))
        } else {
            Ok(Self::new(normalized.parse::<BackendFamily>()?, None))
        }
    }

    pub fn display_string(&self) -> String {
        match &self.model {
            Some(model) => format!("{}({model})", self.family.as_str()),
            None => self.family.as_str().to_owned(),
        }
    }
}

impl fmt::Display for BackendSelection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PanelBackendSpec {
    Required(BackendSelection),
    Optional(BackendSelection),
}

impl PanelBackendSpec {
    pub fn required(backend: BackendFamily) -> Self {
        Self::required_selection(BackendSelection::new(backend, None))
    }

    pub fn required_selection(selection: BackendSelection) -> Self {
        Self::Required(selection)
    }

    pub fn optional(backend: BackendFamily) -> Self {
        Self::optional_selection(BackendSelection::new(backend, None))
    }

    pub fn optional_selection(selection: BackendSelection) -> Self {
        Self::Optional(selection)
    }

    pub fn selection(&self) -> &BackendSelection {
        match self {
            Self::Required(selection) | Self::Optional(selection) => selection,
        }
    }

    pub fn backend(&self) -> BackendFamily {
        self.selection().family
    }

    pub fn is_optional(&self) -> bool {
        matches!(self, Self::Optional(_))
    }
}

impl fmt::Display for PanelBackendSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Required(selection) => write!(f, "{selection}"),
            Self::Optional(selection) => write!(f, "?{selection}"),
        }
    }
}

impl FromStr for PanelBackendSpec {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim();
        if normalized.is_empty() {
            return Err(AppError::InvalidConfigValue {
                key: "panel_backend".to_owned(),
                value: value.to_owned(),
                reason: "backend spec must not be empty".to_owned(),
            });
        }

        if let Some(optional) = normalized.strip_prefix('?') {
            Ok(Self::optional_selection(
                BackendSelection::from_backend_name(optional)?,
            ))
        } else {
            Ok(Self::required_selection(
                BackendSelection::from_backend_name(normalized)?,
            ))
        }
    }
}

impl Serialize for PanelBackendSpec {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for PanelBackendSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value
            .parse::<PanelBackendSpec>()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPolicy {
    NewSession,
    ReuseIfAllowed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FlowPreset {
    Standard,
    QuickDev,
    DocsChange,
    CiImprovement,
    Minimal,
}

impl FlowPreset {
    pub const ALL: [Self; 5] = [
        Self::Standard,
        Self::QuickDev,
        Self::DocsChange,
        Self::CiImprovement,
        Self::Minimal,
    ];

    pub fn all() -> &'static [Self] {
        &Self::ALL
    }

    pub fn supported_csv() -> String {
        Self::all()
            .iter()
            .copied()
            .map(Self::as_str)
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::QuickDev => "quick_dev",
            Self::DocsChange => "docs_change",
            Self::CiImprovement => "ci_improvement",
            Self::Minimal => "minimal",
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
            Self::Minimal => "Minimal flow with plan+implement and final review only.",
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
            "minimal" => Ok(Self::Minimal),
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

    /// Returns `true` for stages that run local validation commands and do not
    /// require a backend agent (docs_validation, ci_validation).
    pub fn is_local_validation(self) -> bool {
        matches!(self, Self::DocsValidation | Self::CiValidation)
    }
}

impl fmt::Display for StageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for StageId {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "prompt_review" => Ok(Self::PromptReview),
            "planning" => Ok(Self::Planning),
            "implementation" => Ok(Self::Implementation),
            "qa" => Ok(Self::Qa),
            "review" => Ok(Self::Review),
            "completion_panel" => Ok(Self::CompletionPanel),
            "acceptance_qa" => Ok(Self::AcceptanceQa),
            "final_review" => Ok(Self::FinalReview),
            "plan_and_implement" => Ok(Self::PlanAndImplement),
            "apply_fixes" => Ok(Self::ApplyFixes),
            "docs_plan" => Ok(Self::DocsPlan),
            "docs_update" => Ok(Self::DocsUpdate),
            "docs_validation" => Ok(Self::DocsValidation),
            "ci_plan" => Ok(Self::CiPlan),
            "ci_update" => Ok(Self::CiUpdate),
            "ci_validation" => Ok(Self::CiValidation),
            _ => Err(AppError::InvalidConfigValue {
                key: "stage_id".to_owned(),
                value: value.to_owned(),
                reason: "unknown stage identifier".to_owned(),
            }),
        }
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

    pub fn retry(&self) -> AppResult<Self> {
        let attempt = self
            .attempt
            .checked_add(1)
            .ok_or(AppError::StageCursorOverflow {
                field: "attempt",
                value: self.attempt,
            })?;

        Self::new(self.stage, self.cycle, attempt, self.completion_round)
    }

    pub fn advance_stage(&self, stage: StageId) -> Self {
        Self {
            stage,
            cycle: self.cycle,
            attempt: 1,
            completion_round: self.completion_round,
        }
    }

    pub fn advance_cycle(&self, stage: StageId) -> AppResult<Self> {
        let cycle = self
            .cycle
            .checked_add(1)
            .ok_or(AppError::StageCursorOverflow {
                field: "cycle",
                value: self.cycle,
            })?;

        Self::new(stage, cycle, 1, self.completion_round)
    }

    pub fn advance_completion_round(&self, stage: StageId) -> AppResult<Self> {
        let completion_round =
            self.completion_round
                .checked_add(1)
                .ok_or(AppError::StageCursorOverflow {
                    field: "completion_round",
                    value: self.completion_round,
                })?;

        Self::new(stage, self.cycle, 1, completion_round)
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptChangeAction {
    Continue,
    Abort,
    RestartCycle,
}

impl PromptChangeAction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Abort => "abort",
            Self::RestartCycle => "restart_cycle",
        }
    }
}

impl fmt::Display for PromptChangeAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PromptChangeAction {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "continue" => Ok(Self::Continue),
            "abort" => Ok(Self::Abort),
            "restart_cycle" => Ok(Self::RestartCycle),
            _ => Err(AppError::InvalidConfigValue {
                key: "prompt_change_action".to_owned(),
                value: value.to_owned(),
                reason: "expected one of continue, abort, restart_cycle".to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrPolicy {
    SkipOnNoDiff,
    CloseOnNoDiff,
}

impl PrPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SkipOnNoDiff => "skip_on_no_diff",
            Self::CloseOnNoDiff => "close_on_no_diff",
        }
    }
}

impl fmt::Display for PrPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PrPolicy {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "skip_on_no_diff" => Ok(Self::SkipOnNoDiff),
            "close_on_no_diff" => Ok(Self::CloseOnNoDiff),
            _ => Err(AppError::InvalidConfigValue {
                key: "daemon.pr.no_diff_action".to_owned(),
                value: value.to_owned(),
                reason: "expected one of skip_on_no_diff, close_on_no_diff".to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Direct,
    Tmux,
}

impl ExecutionMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Tmux => "tmux",
        }
    }
}

impl fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ExecutionMode {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "direct" => Ok(Self::Direct),
            "tmux" => Ok(Self::Tmux),
            _ => Err(AppError::InvalidConfigValue {
                key: "execution.mode".to_owned(),
                value: value.to_owned(),
                reason: "expected one of direct, tmux".to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceConfig {
    pub version: u32,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "WorkspaceSettings::is_empty")]
    pub settings: WorkspaceSettings,
    #[serde(default, skip_serializing_if = "ExecutionSettings::is_empty")]
    pub execution: ExecutionSettings,
    #[serde(default, skip_serializing_if = "PromptReviewSettings::is_empty")]
    pub prompt_review: PromptReviewSettings,
    #[serde(default, skip_serializing_if = "WorkflowSettings::is_empty")]
    pub workflow: WorkflowSettings,
    #[serde(default, skip_serializing_if = "CompletionSettings::is_empty")]
    pub completion: CompletionSettings,
    #[serde(default, skip_serializing_if = "FinalReviewSettings::is_empty")]
    pub final_review: FinalReviewSettings,
    #[serde(default, skip_serializing_if = "ValidationSettings::is_empty")]
    pub validation: ValidationSettings,
    #[serde(default, skip_serializing_if = "DaemonSettings::is_empty")]
    pub daemon: DaemonSettings,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub backends: BTreeMap<String, BackendRuntimeSettings>,
    #[serde(flatten)]
    pub extra: Table,
}

impl WorkspaceConfig {
    pub fn new(created_at: DateTime<Utc>) -> Self {
        Self {
            version: CURRENT_WORKSPACE_VERSION,
            created_at,
            settings: WorkspaceSettings::default(),
            execution: ExecutionSettings::default(),
            prompt_review: PromptReviewSettings::default(),
            workflow: WorkflowSettings::default(),
            completion: CompletionSettings::default(),
            final_review: FinalReviewSettings::default(),
            validation: ValidationSettings::default(),
            daemon: DaemonSettings::default(),
            backends: BTreeMap::new(),
            extra: Table::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ProjectConfig {
    #[serde(default, skip_serializing_if = "WorkspaceSettings::is_empty")]
    pub settings: WorkspaceSettings,
    #[serde(default, skip_serializing_if = "ExecutionSettings::is_empty")]
    pub execution: ExecutionSettings,
    #[serde(default, skip_serializing_if = "PromptReviewSettings::is_empty")]
    pub prompt_review: PromptReviewSettings,
    #[serde(default, skip_serializing_if = "WorkflowSettings::is_empty")]
    pub workflow: WorkflowSettings,
    #[serde(default, skip_serializing_if = "CompletionSettings::is_empty")]
    pub completion: CompletionSettings,
    #[serde(default, skip_serializing_if = "FinalReviewSettings::is_empty")]
    pub final_review: FinalReviewSettings,
    #[serde(default, skip_serializing_if = "ValidationSettings::is_empty")]
    pub validation: ValidationSettings,
    #[serde(default, skip_serializing_if = "DaemonSettings::is_empty")]
    pub daemon: DaemonSettings,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub backends: BTreeMap<String, BackendRuntimeSettings>,
    #[serde(flatten)]
    pub extra: Table,
}

impl ProjectConfig {
    pub fn is_empty(&self) -> bool {
        self.settings.is_empty()
            && self.execution.is_empty()
            && self.prompt_review.is_empty()
            && self.workflow.is_empty()
            && self.completion.is_empty()
            && self.final_review.is_empty()
            && self.validation.is_empty()
            && self.daemon.is_empty()
            && self.backends.is_empty()
            && self.extra.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct WorkspaceSettings {
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
        self.default_flow.is_none()
            && self.default_backend.is_none()
            && self.default_model.is_none()
            && self.extra.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ExecutionSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<ExecutionMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_output: Option<bool>,
    #[serde(flatten)]
    pub extra: Table,
}

impl ExecutionSettings {
    pub fn is_empty(&self) -> bool {
        self.mode.is_none() && self.stream_output.is_none() && self.extra.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct PromptReviewSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refiner_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validator_backends: Option<Vec<PanelBackendSpec>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_reviewers: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_refinement_retries: Option<u32>,
    #[serde(flatten)]
    pub extra: Table,
}

impl PromptReviewSettings {
    pub fn is_empty(&self) -> bool {
        self.enabled.is_none()
            && self.refiner_backend.is_none()
            && self.validator_backends.is_none()
            && self.min_reviewers.is_none()
            && self.max_refinement_retries.is_none()
            && self.extra.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct WorkflowSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planner_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implementer_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qa_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_qa_iterations: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_review_iterations: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_completion_rounds: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_change_action: Option<PromptChangeAction>,
    #[serde(flatten)]
    pub extra: Table,
}

impl WorkflowSettings {
    pub fn is_empty(&self) -> bool {
        self.planner_backend.is_none()
            && self.implementer_backend.is_none()
            && self.reviewer_backend.is_none()
            && self.qa_backend.is_none()
            && self.max_qa_iterations.is_none()
            && self.max_review_iterations.is_none()
            && self.max_completion_rounds.is_none()
            && self.prompt_change_action.is_none()
            && self.extra.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct CompletionSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backends: Option<Vec<PanelBackendSpec>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_completers: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consensus_threshold: Option<f64>,
    #[serde(flatten)]
    pub extra: Table,
}

impl CompletionSettings {
    pub fn is_empty(&self) -> bool {
        self.backends.is_none()
            && self.min_completers.is_none()
            && self.consensus_threshold.is_none()
            && self.extra.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct FinalReviewSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backends: Option<Vec<PanelBackendSpec>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planner_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arbiter_backend: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_reviewers: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consensus_threshold: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_restarts: Option<u32>,
    #[serde(flatten)]
    pub extra: Table,
}

impl FinalReviewSettings {
    pub fn is_empty(&self) -> bool {
        self.enabled.is_none()
            && self.backends.is_none()
            && self.planner_backend.is_none()
            && self.arbiter_backend.is_none()
            && self.min_reviewers.is_none()
            && self.consensus_threshold.is_none()
            && self.max_restarts.is_none()
            && self.extra.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ValidationSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub standard_commands: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docs_commands: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci_commands: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_commit_fmt: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_commit_clippy: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_commit_nix_build: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_commit_fmt_auto_fix: Option<bool>,
    #[serde(flatten)]
    pub extra: Table,
}

impl ValidationSettings {
    pub fn is_empty(&self) -> bool {
        self.standard_commands.is_none()
            && self.docs_commands.is_none()
            && self.ci_commands.is_none()
            && self.pre_commit_fmt.is_none()
            && self.pre_commit_clippy.is_none()
            && self.pre_commit_nix_build.is_none()
            && self.pre_commit_fmt_auto_fix.is_none()
            && self.extra.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct DaemonSettings {
    #[serde(default, skip_serializing_if = "DaemonPrSettings::is_empty")]
    pub pr: DaemonPrSettings,
    #[serde(default, skip_serializing_if = "RebasePolicy::is_empty")]
    pub rebase: RebasePolicy,
    #[serde(flatten)]
    pub extra: Table,
}

impl DaemonSettings {
    pub fn is_empty(&self) -> bool {
        self.pr.is_empty() && self.rebase.is_empty() && self.extra.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct DaemonPrSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_diff_action: Option<PrPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_whitelist: Option<ReviewWhitelistConfig>,
    #[serde(flatten)]
    pub extra: Table,
}

impl DaemonPrSettings {
    pub fn is_empty(&self) -> bool {
        self.no_diff_action.is_none() && self.review_whitelist.is_none() && self.extra.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct ReviewWhitelistConfig(pub Vec<String>);

impl ReviewWhitelistConfig {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn usernames(&self) -> &[String] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct RebasePolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_resolution_enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_timeout: Option<u64>,
    #[serde(flatten)]
    pub extra: Table,
}

impl RebasePolicy {
    pub fn is_empty(&self) -> bool {
        self.agent_resolution_enabled.is_none()
            && self.agent_timeout.is_none()
            && self.extra.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BackendRuntimeSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "BackendRoleModels::is_empty")]
    pub role_models: BackendRoleModels,
    #[serde(default, skip_serializing_if = "BackendRoleTimeouts::is_empty")]
    pub role_timeouts: BackendRoleTimeouts,
    #[serde(flatten)]
    pub extra: Table,
}

impl BackendRuntimeSettings {
    pub fn is_empty(&self) -> bool {
        self.enabled.is_none()
            && self.command.is_none()
            && self.args.is_none()
            && self.timeout_seconds.is_none()
            && self.role_models.is_empty()
            && self.role_timeouts.is_empty()
            && self.extra.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BackendRoleModels {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implementer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qa: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_reviewer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_reviewer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_validator: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arbiter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_qa: Option<String>,
    #[serde(flatten)]
    pub extra: Table,
}

impl BackendRoleModels {
    pub fn is_empty(&self) -> bool {
        self.planner.is_none()
            && self.implementer.is_none()
            && self.reviewer.is_none()
            && self.qa.is_none()
            && self.completer.is_none()
            && self.final_reviewer.is_none()
            && self.prompt_reviewer.is_none()
            && self.prompt_validator.is_none()
            && self.arbiter.is_none()
            && self.acceptance_qa.is_none()
            && self.extra.is_empty()
    }

    pub fn model_for(&self, role: BackendPolicyRole) -> Option<&str> {
        match role {
            BackendPolicyRole::Planner => self.planner.as_deref(),
            BackendPolicyRole::Implementer => self.implementer.as_deref(),
            BackendPolicyRole::Reviewer => self.reviewer.as_deref(),
            BackendPolicyRole::Qa => self.qa.as_deref(),
            BackendPolicyRole::Completer => self.completer.as_deref(),
            BackendPolicyRole::FinalReviewer => self.final_reviewer.as_deref(),
            BackendPolicyRole::PromptReviewer => self.prompt_reviewer.as_deref(),
            BackendPolicyRole::PromptValidator => self.prompt_validator.as_deref(),
            BackendPolicyRole::Arbiter => self.arbiter.as_deref(),
            BackendPolicyRole::AcceptanceQa => self.acceptance_qa.as_deref(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct BackendRoleTimeouts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planner: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implementer: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qa: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completer: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_reviewer: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_reviewer: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_validator: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arbiter: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_qa: Option<u64>,
    #[serde(flatten)]
    pub extra: Table,
}

impl BackendRoleTimeouts {
    pub fn is_empty(&self) -> bool {
        self.planner.is_none()
            && self.implementer.is_none()
            && self.reviewer.is_none()
            && self.qa.is_none()
            && self.completer.is_none()
            && self.final_reviewer.is_none()
            && self.prompt_reviewer.is_none()
            && self.prompt_validator.is_none()
            && self.arbiter.is_none()
            && self.acceptance_qa.is_none()
            && self.extra.is_empty()
    }

    pub fn timeout_for(&self, role: BackendPolicyRole) -> Option<u64> {
        match role {
            BackendPolicyRole::Planner => self.planner,
            BackendPolicyRole::Implementer => self.implementer,
            BackendPolicyRole::Reviewer => self.reviewer,
            BackendPolicyRole::Qa => self.qa,
            BackendPolicyRole::Completer => self.completer,
            BackendPolicyRole::FinalReviewer => self.final_reviewer,
            BackendPolicyRole::PromptReviewer => self.prompt_reviewer,
            BackendPolicyRole::PromptValidator => self.prompt_validator,
            BackendPolicyRole::Arbiter => self.arbiter,
            BackendPolicyRole::AcceptanceQa => self.acceptance_qa,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveRunPolicy {
    pub default_flow: FlowPreset,
    pub max_qa_iterations: u32,
    pub max_review_iterations: u32,
    pub max_completion_rounds: u32,
    pub prompt_change_action: PromptChangeAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectivePromptReviewPolicy {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refiner_backend: Option<BackendSelection>,
    pub validator_backends: Vec<PanelBackendSpec>,
    pub min_reviewers: usize,
    pub max_refinement_retries: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectiveCompletionPolicy {
    pub backends: Vec<PanelBackendSpec>,
    pub min_completers: usize,
    pub consensus_threshold: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectiveFinalReviewPolicy {
    pub enabled: bool,
    pub backends: Vec<PanelBackendSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arbiter_backend: Option<BackendSelection>,
    pub min_reviewers: usize,
    pub consensus_threshold: f64,
    pub max_restarts: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveValidationPolicy {
    pub standard_commands: Vec<String>,
    pub docs_commands: Vec<String>,
    pub ci_commands: Vec<String>,
    pub pre_commit_fmt: bool,
    pub pre_commit_clippy: bool,
    pub pre_commit_nix_build: bool,
    pub pre_commit_fmt_auto_fix: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveDaemonPrPolicy {
    pub no_diff_action: PrPolicy,
    pub review_whitelist: ReviewWhitelistConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveRebasePolicy {
    pub agent_resolution_enabled: bool,
    pub agent_timeout: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EffectiveBackendPolicy {
    pub base_backend: BackendSelection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub planner_backend: Option<BackendSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_review_planner_backend: Option<BackendSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implementer_backend: Option<BackendSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reviewer_backend: Option<BackendSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qa_backend: Option<BackendSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_review_refiner_backend: Option<BackendSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_review_arbiter_backend: Option<BackendSelection>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub backends: BTreeMap<String, BackendRuntimeSettings>,
}
