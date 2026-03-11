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
