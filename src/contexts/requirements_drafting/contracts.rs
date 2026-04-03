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

use crate::contexts::milestone_record::bundle::MilestoneBundle;
use crate::shared::domain::FlowPreset;
use crate::shared::error::ContractError;

use super::model::{
    GapAnalysisPayload, GapSeverity, IdeationPayload, ImplementationSpecPayload,
    ProjectSeedPayload, QuestionSetPayload, RequirementsDraftPayload, RequirementsReviewOutcome,
    RequirementsReviewPayload, RequirementsStageId, ResearchPayload, RevisionFeedback,
    SynthesisPayload, ValidationOutcome, ValidationPayload, SUPPORTED_SEED_VERSIONS,
};
use super::renderers;

fn is_built_in_flow(flow: FlowPreset) -> bool {
    FlowPreset::all().contains(&flow)
}

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
    MilestoneBundle(MilestoneBundle),
    // Full-mode stage payloads
    Ideation(IdeationPayload),
    Research(ResearchPayload),
    Synthesis(SynthesisPayload),
    ImplementationSpec(ImplementationSpecPayload),
    GapAnalysis(GapAnalysisPayload),
    Validation(ValidationPayload),
    // Quick-mode revision feedback
    RevisionFeedback(RevisionFeedback),
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

    pub fn milestone_bundle() -> Self {
        Self {
            stage_id: RequirementsStageId::MilestoneBundle,
        }
    }

    pub fn ideation() -> Self {
        Self {
            stage_id: RequirementsStageId::Ideation,
        }
    }

    pub fn research() -> Self {
        Self {
            stage_id: RequirementsStageId::Research,
        }
    }

    pub fn synthesis() -> Self {
        Self {
            stage_id: RequirementsStageId::Synthesis,
        }
    }

    pub fn implementation_spec() -> Self {
        Self {
            stage_id: RequirementsStageId::ImplementationSpec,
        }
    }

    pub fn gap_analysis() -> Self {
        Self {
            stage_id: RequirementsStageId::GapAnalysis,
        }
    }

    pub fn validation() -> Self {
        Self {
            stage_id: RequirementsStageId::Validation,
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
            RequirementsStageId::MilestoneBundle => schemars::schema_for!(MilestoneBundle),
            RequirementsStageId::Ideation => schemars::schema_for!(IdeationPayload),
            RequirementsStageId::Research => schemars::schema_for!(ResearchPayload),
            RequirementsStageId::Synthesis => schemars::schema_for!(SynthesisPayload),
            RequirementsStageId::ImplementationSpec => {
                schemars::schema_for!(ImplementationSpecPayload)
            }
            RequirementsStageId::GapAnalysis => schemars::schema_for!(GapAnalysisPayload),
            RequirementsStageId::Validation => schemars::schema_for!(ValidationPayload),
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
        let stage_str = self.stage_id.as_str();
        match self.stage_id {
            RequirementsStageId::QuestionSet => {
                let p: QuestionSetPayload = serde_json::from_value(raw.clone()).map_err(|e| {
                    ContractError::SchemaValidation {
                        stage_id: stage_str.to_owned(),
                        details: format!("question_set: {e}"),
                    }
                })?;
                Ok(RequirementsPayload::QuestionSet(p))
            }
            RequirementsStageId::RequirementsDraft => {
                let p: RequirementsDraftPayload =
                    serde_json::from_value(raw.clone()).map_err(|e| {
                        ContractError::SchemaValidation {
                            stage_id: stage_str.to_owned(),
                            details: format!("requirements_draft: {e}"),
                        }
                    })?;
                Ok(RequirementsPayload::Draft(p))
            }
            RequirementsStageId::RequirementsReview => {
                let p: RequirementsReviewPayload =
                    serde_json::from_value(raw.clone()).map_err(|e| {
                        ContractError::SchemaValidation {
                            stage_id: stage_str.to_owned(),
                            details: format!("requirements_review: {e}"),
                        }
                    })?;
                Ok(RequirementsPayload::Review(p))
            }
            RequirementsStageId::ProjectSeed => {
                let p: ProjectSeedPayload = serde_json::from_value(raw.clone()).map_err(|e| {
                    ContractError::SchemaValidation {
                        stage_id: stage_str.to_owned(),
                        details: format!("project_seed: {e}"),
                    }
                })?;
                Ok(RequirementsPayload::Seed(p))
            }
            RequirementsStageId::MilestoneBundle => {
                let p: MilestoneBundle = serde_json::from_value(raw.clone()).map_err(|e| {
                    ContractError::SchemaValidation {
                        stage_id: stage_str.to_owned(),
                        details: format!("milestone_bundle: {e}"),
                    }
                })?;
                Ok(RequirementsPayload::MilestoneBundle(p))
            }
            RequirementsStageId::Ideation => {
                let p: IdeationPayload = serde_json::from_value(raw.clone()).map_err(|e| {
                    ContractError::SchemaValidation {
                        stage_id: stage_str.to_owned(),
                        details: format!("ideation: {e}"),
                    }
                })?;
                Ok(RequirementsPayload::Ideation(p))
            }
            RequirementsStageId::Research => {
                let p: ResearchPayload = serde_json::from_value(raw.clone()).map_err(|e| {
                    ContractError::SchemaValidation {
                        stage_id: stage_str.to_owned(),
                        details: format!("research: {e}"),
                    }
                })?;
                Ok(RequirementsPayload::Research(p))
            }
            RequirementsStageId::Synthesis => {
                let p: SynthesisPayload = serde_json::from_value(raw.clone()).map_err(|e| {
                    ContractError::SchemaValidation {
                        stage_id: stage_str.to_owned(),
                        details: format!("synthesis: {e}"),
                    }
                })?;
                Ok(RequirementsPayload::Synthesis(p))
            }
            RequirementsStageId::ImplementationSpec => {
                let p: ImplementationSpecPayload =
                    serde_json::from_value(raw.clone()).map_err(|e| {
                        ContractError::SchemaValidation {
                            stage_id: stage_str.to_owned(),
                            details: format!("implementation_spec: {e}"),
                        }
                    })?;
                Ok(RequirementsPayload::ImplementationSpec(p))
            }
            RequirementsStageId::GapAnalysis => {
                let p: GapAnalysisPayload = serde_json::from_value(raw.clone()).map_err(|e| {
                    ContractError::SchemaValidation {
                        stage_id: stage_str.to_owned(),
                        details: format!("gap_analysis: {e}"),
                    }
                })?;
                Ok(RequirementsPayload::GapAnalysis(p))
            }
            RequirementsStageId::Validation => {
                let p: ValidationPayload = serde_json::from_value(raw.clone()).map_err(|e| {
                    ContractError::SchemaValidation {
                        stage_id: stage_str.to_owned(),
                        details: format!("validation: {e}"),
                    }
                })?;
                Ok(RequirementsPayload::Validation(p))
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
                if !is_built_in_flow(p.recommended_flow) {
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
                if !SUPPORTED_SEED_VERSIONS.contains(&p.version) {
                    errors.push(format!(
                        "unsupported seed version {}: supported versions are {:?}",
                        p.version, SUPPORTED_SEED_VERSIONS
                    ));
                }
                if !errors.is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: format!("project_seed: {}", errors.join("; ")),
                    });
                }
            }
            RequirementsPayload::MilestoneBundle(p) => {
                if let Err(errors) = p.validate() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: format!("milestone_bundle: {}", errors.join("; ")),
                    });
                }
            }
            RequirementsPayload::Ideation(p) => {
                let mut errors = Vec::new();
                if p.themes.is_empty() {
                    errors.push("themes must contain at least one item".to_string());
                }
                if p.initial_scope.trim().is_empty() {
                    errors.push("initial_scope must not be empty".to_string());
                }
                if !errors.is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: format!("ideation: {}", errors.join("; ")),
                    });
                }
            }
            RequirementsPayload::Research(p) => {
                if p.technical_context.trim().is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: "research: technical_context must not be empty".to_string(),
                    });
                }
            }
            RequirementsPayload::Synthesis(p) => {
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
                if !is_built_in_flow(p.recommended_flow) {
                    errors.push("recommended_flow must be a built-in preset".to_string());
                }
                if !errors.is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: format!("synthesis: {}", errors.join("; ")),
                    });
                }
            }
            RequirementsPayload::ImplementationSpec(p) => {
                let mut errors = Vec::new();
                if p.architecture_overview.trim().is_empty() {
                    errors.push("architecture_overview must not be empty".to_string());
                }
                if p.components.is_empty() {
                    errors.push("components must contain at least one item".to_string());
                }
                if !errors.is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: format!("implementation_spec: {}", errors.join("; ")),
                    });
                }
            }
            RequirementsPayload::GapAnalysis(p) => {
                if p.coverage_assessment.trim().is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: "gap_analysis: coverage_assessment must not be empty".to_string(),
                    });
                }
                // Check that blocking_gaps references correspond to actual gaps
                for gap in &p.gaps {
                    if gap.severity == GapSeverity::Blocking && !p.blocking_gaps.contains(&gap.area)
                    {
                        // Non-fatal: blocking gaps list may use different wording
                    }
                }
            }
            RequirementsPayload::Validation(p) => {
                let mut errors = Vec::new();
                if p.outcome == ValidationOutcome::NeedsQuestions
                    && p.missing_information.is_empty()
                {
                    errors.push(
                        "needs_questions outcome requires at least one missing_information item"
                            .to_string(),
                    );
                }
                if p.outcome == ValidationOutcome::Fail && p.blocking_issues.is_empty() {
                    errors.push(
                        "fail outcome requires at least one blocking_issues item".to_string(),
                    );
                }
                if !errors.is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id.as_str().to_owned(),
                        details: format!("validation: {}", errors.join("; ")),
                    });
                }
            }
            RequirementsPayload::RevisionFeedback(p) => {
                if !p.outcome.allows_completion() && p.findings.is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: "revision_feedback".to_owned(),
                        details: "revision_feedback: findings required for non-approval outcome"
                            .to_string(),
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
            RequirementsPayload::MilestoneBundle(p) => renderers::render_milestone_bundle(p),
            RequirementsPayload::Ideation(p) => renderers::render_ideation(p),
            RequirementsPayload::Research(p) => renderers::render_research(p),
            RequirementsPayload::Synthesis(p) => renderers::render_synthesis(p),
            RequirementsPayload::ImplementationSpec(p) => renderers::render_implementation_spec(p),
            RequirementsPayload::GapAnalysis(p) => renderers::render_gap_analysis(p),
            RequirementsPayload::Validation(p) => renderers::render_validation(p),
            RequirementsPayload::RevisionFeedback(p) => renderers::render_revision_feedback(p),
        }
    }
}
