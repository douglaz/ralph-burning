#![forbid(unsafe_code)]

//! Stage contract registry and deterministic evaluation pipeline.
//!
//! Every stage in every built-in flow preset resolves to exactly one
//! [`StageContract`]. Contract evaluation enforces a fixed order:
//!
//! 1. **Schema validation** — deserialize raw JSON into the typed payload.
//! 2. **Semantic validation** — apply domain rules to the typed payload.
//! 3. **Rendering** — produce a deterministic Markdown artifact.
//!
//! On schema failure, semantic validation and rendering do not run.
//! On domain failure, rendering does not run.
//! On any failure, no success bundle is returned.

use schemars::schema::RootSchema;

use crate::shared::domain::StageId;
use crate::shared::error::ContractError;

use super::payloads::{ExecutionPayload, PlanningPayload, StagePayload, ValidationPayload};
use super::renderers;

// ── Contract family ─────────────────────────────────────────────────────────

/// Classifies a stage into one of three payload families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContractFamily {
    /// prompt_review, planning, docs_plan, ci_plan
    Planning,
    /// implementation, plan_and_implement, apply_fixes, docs_update, ci_update
    Execution,
    /// qa, docs_validation, ci_validation, acceptance_qa, review, final_review,
    /// completion_panel
    Validation,
}

// ── Stage contract ──────────────────────────────────────────────────────────

/// A stage contract binds a [`StageId`] to its payload family, schema,
/// semantic validation rules, and renderer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StageContract {
    pub stage_id: StageId,
    pub family: ContractFamily,
}

/// Bundle of a validated payload and its rendered Markdown artifact.
///
/// Callers receive either a `ValidatedBundle` or an error — never one without
/// the other. On any failure (schema, domain, or QA/review outcome), no bundle
/// is returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedBundle {
    pub payload: StagePayload,
    pub artifact: String,
}

// ── Registry ────────────────────────────────────────────────────────────────

/// Look up the contract for a given stage. Every [`StageId`] variant resolves
/// to exactly one contract.
pub fn contract_for_stage(stage_id: StageId) -> StageContract {
    let family = match stage_id {
        StageId::PromptReview | StageId::Planning | StageId::DocsPlan | StageId::CiPlan => {
            ContractFamily::Planning
        }

        StageId::Implementation
        | StageId::PlanAndImplement
        | StageId::ApplyFixes
        | StageId::DocsUpdate
        | StageId::CiUpdate => ContractFamily::Execution,

        StageId::Qa
        | StageId::DocsValidation
        | StageId::CiValidation
        | StageId::AcceptanceQa
        | StageId::Review
        | StageId::FinalReview
        | StageId::CompletionPanel => ContractFamily::Validation,
    };

    StageContract { stage_id, family }
}

/// Return contracts for all stages, in [`StageId::ALL`] order.
pub fn all_contracts() -> Vec<StageContract> {
    StageId::ALL
        .iter()
        .map(|&id| contract_for_stage(id))
        .collect()
}

// ── Contract implementation ─────────────────────────────────────────────────

impl StageContract {
    /// Generate the JSON Schema for this contract's payload family.
    pub fn json_schema(&self) -> RootSchema {
        match self.family {
            ContractFamily::Planning => schemars::schema_for!(PlanningPayload),
            ContractFamily::Execution => schemars::schema_for!(ExecutionPayload),
            ContractFamily::Validation => schemars::schema_for!(ValidationPayload),
        }
    }

    /// Evaluate a raw JSON value through the full contract pipeline.
    ///
    /// Schema validation → semantic validation → outcome check → rendering.
    ///
    /// Returns [`ValidatedBundle`] on success or a [`ContractError`] on any
    /// failure. Non-passing QA/review outcomes are errors classified as
    /// [`FailureClass::QaReviewOutcomeFailure`] — no success bundle is returned.
    pub fn evaluate(&self, raw_json: &serde_json::Value) -> Result<ValidatedBundle, ContractError> {
        let bundle = self.evaluate_permissive(raw_json)?;

        self.check_outcome(&bundle.payload)?;

        Ok(bundle)
    }

    /// Evaluate a raw JSON value without enforcing passing review outcomes.
    ///
    /// Schema validation → semantic validation → rendering.
    ///
    /// This is used by the engine so it can durably persist non-passing
    /// validation payloads before applying remediation or terminal semantics.
    pub fn evaluate_permissive(
        &self,
        raw_json: &serde_json::Value,
    ) -> Result<ValidatedBundle, ContractError> {
        // Step 1: Schema validation (deserialization into typed payload).
        let payload = self.validate_schema(raw_json)?;

        // Step 2: Semantic / domain validation.
        self.validate_semantics(&payload)?;

        // Step 3: Deterministic Markdown rendering (without outcome enforcement).
        let artifact = self.render(&payload);

        Ok(ValidatedBundle { payload, artifact })
    }

    // ── Private helpers ─────────────────────────────────────────────────

    fn validate_schema(&self, raw: &serde_json::Value) -> Result<StagePayload, ContractError> {
        match self.family {
            ContractFamily::Planning => {
                let p: PlanningPayload = serde_json::from_value(raw.clone()).map_err(|e| {
                    ContractError::SchemaValidation {
                        stage_id: self.stage_id,
                        details: e.to_string(),
                    }
                })?;
                Ok(StagePayload::Planning(p))
            }
            ContractFamily::Execution => {
                let p: ExecutionPayload = serde_json::from_value(raw.clone()).map_err(|e| {
                    ContractError::SchemaValidation {
                        stage_id: self.stage_id,
                        details: e.to_string(),
                    }
                })?;
                Ok(StagePayload::Execution(p))
            }
            ContractFamily::Validation => {
                let p: ValidationPayload = serde_json::from_value(raw.clone()).map_err(|e| {
                    ContractError::SchemaValidation {
                        stage_id: self.stage_id,
                        details: e.to_string(),
                    }
                })?;
                Ok(StagePayload::Validation(p))
            }
        }
    }

    fn validate_semantics(&self, payload: &StagePayload) -> Result<(), ContractError> {
        match payload {
            StagePayload::Planning(p) => {
                let mut errors = Vec::new();
                if p.problem_framing.trim().is_empty() {
                    errors.push("problem_framing must not be empty".to_string());
                }
                if p.proposed_work.is_empty() {
                    errors.push("proposed_work must contain at least one item".to_string());
                }
                for (i, item) in p.proposed_work.iter().enumerate() {
                    if item.order == 0 {
                        errors.push(format!("proposed_work[{i}].order must be positive"));
                    }
                }
                if !errors.is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id,
                        details: errors.join("; "),
                    });
                }
            }
            StagePayload::Execution(p) => {
                let mut errors = Vec::new();
                if p.change_summary.trim().is_empty() {
                    errors.push("change_summary must not be empty".to_string());
                }
                if p.steps.is_empty() {
                    errors.push("steps must contain at least one item".to_string());
                }
                for (i, step) in p.steps.iter().enumerate() {
                    if step.order == 0 {
                        errors.push(format!("steps[{i}].order must be positive"));
                    }
                }
                if !errors.is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id,
                        details: errors.join("; "),
                    });
                }
            }
            StagePayload::Validation(p) => {
                if !p.outcome.is_passing() && p.follow_up_or_amendments.is_empty() {
                    return Err(ContractError::DomainValidation {
                        stage_id: self.stage_id,
                        details: "follow_up_or_amendments required when outcome is not approved"
                            .to_string(),
                    });
                }
            }
        }
        Ok(())
    }

    fn render(&self, payload: &StagePayload) -> String {
        match payload {
            StagePayload::Planning(p) => renderers::render_planning(self.stage_id, p),
            StagePayload::Execution(p) => renderers::render_execution(self.stage_id, p),
            StagePayload::Validation(p) => renderers::render_validation(self.stage_id, p),
        }
    }

    fn check_outcome(&self, payload: &StagePayload) -> Result<(), ContractError> {
        if let StagePayload::Validation(p) = payload {
            if !p.outcome.is_passing() {
                return Err(ContractError::QaReviewOutcome {
                    stage_id: self.stage_id,
                    outcome: p.outcome.to_string(),
                });
            }
        }
        Ok(())
    }
}
