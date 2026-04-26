#![forbid(unsafe_code)]

//! Typed stage payload models for each contract family.
//!
//! Three payload families cover all 16 stages:
//! - **Planning**: prompt_review, planning, docs_plan, ci_plan
//! - **Execution**: implementation, plan_and_implement, apply_fixes, docs_update, ci_update
//! - **Validation**: qa, docs_validation, ci_validation, acceptance_qa, review, final_review,
//!   completion_panel

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::contexts::workflow_composition::review_classification::{
    default_review_finding_class, ReviewFindingClass,
};

// ── Planning family ─────────────────────────────────────────────────────────

/// Payload for planning-style stages.
///
/// Requires structured problem framing, assumptions or open questions,
/// ordered proposed work, and explicit readiness/risk fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PlanningPayload {
    pub problem_framing: String,
    pub assumptions_or_open_questions: Vec<String>,
    pub proposed_work: Vec<ProposedWorkItem>,
    pub readiness: ReadinessAssessment,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProposedWorkItem {
    pub order: u32,
    pub summary: String,
    pub details: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ReadinessAssessment {
    pub ready: bool,
    pub risks: Vec<String>,
}

// ── Execution family ────────────────────────────────────────────────────────

/// Payload for execution/update stages.
///
/// Requires structured change summaries, intended or completed steps,
/// validation evidence or plans, and outstanding-risk fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExecutionPayload {
    pub change_summary: String,
    pub steps: Vec<ExecutionStep>,
    pub validation_evidence: Vec<String>,
    pub outstanding_risks: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExecutionStep {
    pub order: u32,
    pub description: String,
    pub status: StepStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Intended,
    Completed,
    Skipped,
}

impl std::fmt::Display for StepStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Intended => f.write_str("Intended"),
            Self::Completed => f.write_str("Completed"),
            Self::Skipped => f.write_str("Skipped"),
        }
    }
}

// ── Validation / Review family ──────────────────────────────────────────────

/// A finding with an explicit classification for milestone-aware routing.
#[derive(Debug, Clone, Deserialize)]
struct ClassifiedFindingWire {
    body: String,
    #[serde(default)]
    classification: Option<ReviewFindingClass>,
    #[serde(default)]
    covered_by_bead_id: Option<String>,
    #[serde(default)]
    mapped_to_bead_id: Option<String>,
    #[serde(default)]
    proposed_bead_summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(from = "ClassifiedFindingWire")]
pub struct ClassifiedFinding {
    /// The finding body (equivalent to an item in `follow_up_or_amendments`).
    pub body: String,
    /// How this finding should be routed.
    #[serde(default = "default_review_finding_class")]
    #[schemars(default = "default_review_finding_class")]
    pub classification: ReviewFindingClass,
    /// Bead ID when classification is covered-by-existing-bead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub covered_by_bead_id: Option<String>,
    /// Legacy name for older internal payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mapped_to_bead_id: Option<String>,
    /// One-line proposed bead summary when classification is propose-new-bead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_bead_summary: Option<String>,
}

impl From<ClassifiedFindingWire> for ClassifiedFinding {
    fn from(raw: ClassifiedFindingWire) -> Self {
        let mut classification = raw.classification.unwrap_or_default();
        let covered_by_bead_id = raw
            .covered_by_bead_id
            .or_else(|| raw.mapped_to_bead_id.clone());

        match classification {
            ReviewFindingClass::CoveredByExistingBead if covered_by_bead_id.is_none() => {
                tracing::warn!(
                    "covered_by_existing_bead review finding missing covered_by_bead_id; falling back to fix_current_bead"
                );
                classification = ReviewFindingClass::FixCurrentBead;
            }
            ReviewFindingClass::ProposeNewBead if raw.proposed_bead_summary.is_none() => {
                tracing::warn!(
                    "propose_new_bead review finding missing proposed_bead_summary; falling back to fix_current_bead"
                );
                classification = ReviewFindingClass::FixCurrentBead;
            }
            _ => {}
        }

        Self {
            body: raw.body,
            classification,
            covered_by_bead_id,
            mapped_to_bead_id: raw.mapped_to_bead_id,
            proposed_bead_summary: raw.proposed_bead_summary,
        }
    }
}

/// Payload for validation/review stages.
///
/// Requires an explicit outcome/decision enum plus evidence, findings or gaps,
/// and follow-up or amendment data when the outcome is not cleanly approved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ValidationPayload {
    pub outcome: ReviewOutcome,
    pub evidence: Vec<String>,
    pub findings_or_gaps: Vec<String>,
    pub follow_up_or_amendments: Vec<String>,
    /// Classified findings for milestone-aware review stages. When present,
    /// classification metadata is preserved for downstream routing while all
    /// findings remain fix-now equivalent in the current engine.
    ///
    /// When absent (non-milestone mode or older LLM output), all items in
    /// `follow_up_or_amendments` are treated as fix-now for backward compat.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub classified_findings: Vec<ClassifiedFinding>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReviewOutcome {
    Approved,
    ConditionallyApproved,
    RequestChanges,
    Rejected,
}

impl ReviewOutcome {
    /// Returns `true` only for a clean approval with no conditions.
    pub fn is_passing(self) -> bool {
        matches!(self, Self::Approved)
    }
}

impl std::fmt::Display for ReviewOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Approved => f.write_str("Approved"),
            Self::ConditionallyApproved => f.write_str("Conditionally Approved"),
            Self::RequestChanges => f.write_str("Request Changes"),
            Self::Rejected => f.write_str("Rejected"),
        }
    }
}

// ── Wrapper enum ────────────────────────────────────────────────────────────

/// Typed stage payload, wrapping one of the three contract families.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "family", content = "data")]
pub enum StagePayload {
    Planning(PlanningPayload),
    Execution(ExecutionPayload),
    Validation(ValidationPayload),
}
