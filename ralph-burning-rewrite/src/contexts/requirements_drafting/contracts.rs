#![forbid(unsafe_code)]

//! Structured contracts for requirements stages.
//!
//! Follows the same invariant as workflow stage contracts:
//! 1. Schema validation (JSON → typed payload via serde)
//! 2. Domain validation (semantic rules)
//! 3. Rendering (deterministic Markdown artifact)
//!
//! Rendered Markdown is never parsed for control flow.

use schemars::schema::RootSchema;

use crate::shared::domain::FlowPreset;
use crate::shared::error::ContractError;

use super::model::{
    ProjectSeedPayload, QuestionSetPayload, RequirementsDraftPayload, RequirementsReviewOutcome,
    RequirementsReviewPayload, RequirementsStageId,
};
use super::renderers;

/// A validated requirements contract output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequirementsValidatedBundle {
    pub payload: RequirementsPayload,
    pub artifact: String,
}

/// Typed requirements payload wrapper.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequirementsPayload {
    QuestionSet(QuestionSetPayload),
    Draft(RequirementsDraftPayload),
    Review(RequirementsReviewPayload),
    Seed(ProjectSeedPayload),
}

/// Requirements contract binding a stage to its validation and rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequirementsContract {
    pub stage_id: RequirementsStageId,
}

impl RequirementsContract {
    pub fn question_set() -> Self {
        Self {
            stage_id: RequirementsStageId::QuestionSet,
        }
    }

    pub fn draft() -> Self {
        Self {
            stage_id: RequirementsStageId::RequirementsDraft,
        }
    }

    pub fn review() -> Self {
        Self {
            stage_id: RequirementsStageId::RequirementsReview,
        }
    }

    pub fn seed() -> Self {
        Self {
            stage_id: RequirementsStageId::ProjectSeed,
        }
    }

    pub fn json_schema(&self) -> RootSchema {
        match self.stage_id {
            RequirementsStageId::QuestionSet => schemars::schema_for!(QuestionSetPayload),
            RequirementsStageId::RequirementsDraft => {
                schemars::schema_for!(RequirementsDraftPayload)
            }
            RequirementsStageId::RequirementsReview => {
                schemars::schema_for!(RequirementsReviewPayload)
            }
            RequirementsStageId::ProjectSeed => schemars::schema_for!(ProjectSeedPayload),
        }
    }

    /// Evaluate a raw JSON value through schema → domain → render.
    pub fn evaluate(
        &self,
        raw_json: &serde_json::Value,
    ) -> Result<RequirementsValidatedBundle, ContractError> {
        let payload = self.validate_schema(raw_json)?;
        self.validate_semantics(&payload)?;
        let artifact = self.render(&payload);
        Ok(RequirementsValidatedBundle { payload, artifact })
    }

    fn validate_schema(
        &self,
        raw: &serde_json::Value,
    ) -> Result<RequirementsPayload, ContractError> {
        match self.stage_id {
            RequirementsStageId::QuestionSet => {
                let p: QuestionSetPayload = serde_json::from_value(raw.clone()).map_err(|e| {
                    ContractError::SchemaValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: format!("question_set: {e}"),
                    }
                })?;
                Ok(RequirementsPayload::QuestionSet(p))
            }
            RequirementsStageId::RequirementsDraft => {
                let p: RequirementsDraftPayload =
                    serde_json::from_value(raw.clone()).map_err(|e| {
                        ContractError::SchemaValidation {
                            stage_id: self.stage_id.as_str().to_owned(),
                            details: format!("requirements_draft: {e}"),
                        }
                    })?;
                Ok(RequirementsPayload::Draft(p))
            }
            RequirementsStageId::RequirementsReview => {
                let p: RequirementsReviewPayload =
                    serde_json::from_value(raw.clone()).map_err(|e| {
                        ContractError::SchemaValidation {
                            stage_id: self.stage_id.as_str().to_owned(),
                            details: format!("requirements_review: {e}"),
                        }
                    })?;
                Ok(RequirementsPayload::Review(p))
            }
            RequirementsStageId::ProjectSeed => {
                let p: ProjectSeedPayload = serde_json::from_value(raw.clone()).map_err(|e| {
                    ContractError::SchemaValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: format!("project_seed: {e}"),
                    }
                })?;
                Ok(RequirementsPayload::Seed(p))
            }
        }
    }

    fn validate_semantics(&self, payload: &RequirementsPayload) -> Result<(), ContractError> {
        match payload {
            RequirementsPayload::QuestionSet(p) => {
                let mut errors = Vec::new();
                let mut seen_ids = std::collections::HashSet::new();
                for (i, q) in p.questions.iter().enumerate() {
                    if q.id.trim().is_empty() {
                        errors.push(format!("questions[{i}].id must not be empty"));
                    } else if !q
                        .id
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
                    {
                        errors.push(format!(
                            "questions[{i}].id '{}' contains characters not allowed in TOML bare keys \
                             (only ASCII alphanumeric, underscore, and hyphen permitted)",
                            q.id
                        ));
                    }
                    if !seen_ids.insert(&q.id) {
                        errors.push(format!("questions[{i}].id '{}' is duplicate", q.id));
                    }
                    if q.prompt.trim().is_empty() {
                        errors.push(format!("questions[{i}].prompt must not be empty"));
                    }
                }
                if !errors.is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: format!("question_set: {}", errors.join("; ")),
                    });
                }
            }
            RequirementsPayload::Draft(p) => {
                let mut errors = Vec::new();
                if p.problem_summary.trim().is_empty() {
                    errors.push("problem_summary must not be empty".to_string());
                }
                if p.goals.is_empty() {
                    errors.push("goals must contain at least one item".to_string());
                }
                if p.acceptance_criteria.is_empty() {
                    errors.push("acceptance_criteria must contain at least one item".to_string());
                }
                // Validate recommended_flow is a built-in preset
                let valid_flows = [
                    FlowPreset::Standard,
                    FlowPreset::QuickDev,
                    FlowPreset::DocsChange,
                    FlowPreset::CiImprovement,
                ];
                if !valid_flows.contains(&p.recommended_flow) {
                    errors.push("recommended_flow must be a built-in preset".to_string());
                }
                if !errors.is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: format!("requirements_draft: {}", errors.join("; ")),
                    });
                }
            }
            RequirementsPayload::Review(p) => {
                if !p.outcome.allows_completion() && p.findings.is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: "requirements_review: findings required for non-approval outcome"
                            .to_string(),
                    });
                }
                if p.outcome == RequirementsReviewOutcome::ConditionallyApproved
                    && p.follow_ups.is_empty()
                {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: "requirements_review: conditionally_approved requires at least one follow-up"
                            .to_string(),
                    });
                }
            }
            RequirementsPayload::Seed(p) => {
                let mut errors = Vec::new();
                if p.project_id.trim().is_empty() {
                    errors.push("project_id must not be empty".to_string());
                }
                if p.project_name.trim().is_empty() {
                    errors.push("project_name must not be empty".to_string());
                }
                if p.prompt_body.trim().is_empty() {
                    errors.push("prompt_body must not be empty".to_string());
                }
                if p.handoff_summary.trim().is_empty() {
                    errors.push("handoff_summary must not be empty".to_string());
                }
                if !errors.is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: format!("project_seed: {}", errors.join("; ")),
                    });
                }
            }
        }
        Ok(())
    }

    fn render(&self, payload: &RequirementsPayload) -> String {
        match payload {
            RequirementsPayload::QuestionSet(p) => renderers::render_question_set(p),
            RequirementsPayload::Draft(p) => renderers::render_requirements_draft(p),
            RequirementsPayload::Review(p) => renderers::render_requirements_review(p),
            RequirementsPayload::Seed(p) => renderers::render_project_seed(p),
        }
    }
}
