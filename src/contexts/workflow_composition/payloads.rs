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
///
/// The shared schema deliberately does NOT require `validation_evidence` to be
/// non-empty: that constraint applies only to the codex transport (added at
/// schema-emit time in `process_backend::processed_contract_schema_value`) so
/// that Claude execution stages — whose semantic validators and renderers
/// already tolerate empty validation evidence — are not affected. The codex
/// constraint exists only to defeat issue #188, where codex's
/// `--output-schema` flag treats the first matching JSON message (an interim
/// status emitted before tool calls return) as the terminal output for the
/// turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExecutionPayload {
    pub change_summary: String,
    pub steps: Vec<ExecutionStep>,
    pub validation_evidence: Vec<String>,
    pub outstanding_risks: Vec<String>,
}

impl ExecutionPayload {
    /// Returns `true` when this payload looks like the codex "interim"
    /// status message emitted *before* tool calls return on gpt-5.5-high
    /// (GitHub issue #188).
    ///
    /// The signal: every step is still in `Intended`. We deliberately do
    /// NOT also require `validation_evidence` to be empty: the original
    /// PR #189 detector did, but issue #188 was reopened after codex was
    /// observed satisfying the codex-only `minItems: 1` schema constraint
    /// with a single placeholder string (e.g. "Workspace: …; task accepted
    /// for execution.") while keeping every step intended. A bogus-but-
    /// schema-conformant evidence string does not change the substantive
    /// fact: nothing has been completed or skipped yet, so the turn cannot
    /// be terminal.
    ///
    /// A real terminal payload always has at least one `Completed` or
    /// `Skipped` step (the implementer contract requires forward
    /// progress). A mixed-status terminal payload — completed steps plus
    /// one deferred `Intended` follow-up — still passes because not every
    /// step is intended.
    pub fn looks_like_codex_interim_message(&self) -> bool {
        !self.steps.is_empty()
            && self
                .steps
                .iter()
                .all(|step| matches!(step.status, StepStatus::Intended))
    }
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
    /// How this finding should be routed. The `#[schemars(default = ...)]`
    /// attribute is intentionally omitted — gpt-5.5 strict-mode structured
    /// outputs reject the `allOf` schemars emits when an enum is paired with a
    /// `default` function. Serde still applies the default at deserialize time.
    #[serde(default = "default_review_finding_class")]
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
        let covered_by_bead_id = normalize_required_classification_field(
            raw.covered_by_bead_id.as_deref(),
            "covered_by_bead_id",
        )
        .or_else(|| {
            normalize_required_classification_field(
                raw.mapped_to_bead_id.as_deref(),
                "mapped_to_bead_id",
            )
        });
        let mapped_to_bead_id = normalize_required_classification_field(
            raw.mapped_to_bead_id.as_deref(),
            "mapped_to_bead_id",
        );
        let proposed_bead_summary = normalize_required_classification_field(
            raw.proposed_bead_summary.as_deref(),
            "proposed_bead_summary",
        );

        match classification {
            ReviewFindingClass::CoveredByExistingBead if covered_by_bead_id.is_none() => {
                tracing::warn!(
                    "covered_by_existing_bead review finding missing nonblank covered_by_bead_id; falling back to fix_current_bead"
                );
                classification = ReviewFindingClass::FixCurrentBead;
            }
            ReviewFindingClass::ProposeNewBead if proposed_bead_summary.is_none() => {
                tracing::warn!(
                    "propose_new_bead review finding missing nonblank proposed_bead_summary; falling back to fix_current_bead"
                );
                classification = ReviewFindingClass::FixCurrentBead;
            }
            _ => {}
        }

        Self {
            body: raw.body,
            classification,
            covered_by_bead_id,
            mapped_to_bead_id,
            proposed_bead_summary,
        }
    }
}

fn normalize_required_classification_field(
    value: Option<&str>,
    field_name: &str,
) -> Option<String> {
    let trimmed = value?.trim();
    if trimmed.is_empty() {
        tracing::warn!(
            field = field_name,
            "review finding classification field is blank after trimming"
        );
        None
    } else {
        Some(trimmed.to_owned())
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
    /// only fix-current findings are queued for immediate remediation; other
    /// classifications are preserved for terminal reconciliation.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn step(order: u32, status: StepStatus) -> ExecutionStep {
        ExecutionStep {
            order,
            description: "test step".to_owned(),
            status,
        }
    }

    #[test]
    fn looks_like_codex_interim_message_matches_canonical_issue_188_shape() {
        // The exact shape codex emits as the *interim* status before tool
        // calls return: every step intended, validation_evidence empty.
        let payload = ExecutionPayload {
            change_summary: "Starting required reading and repository \
                inspection before editing the documentation file."
                .to_owned(),
            steps: vec![step(1, StepStatus::Intended)],
            validation_evidence: vec![],
            outstanding_risks: vec![],
        };
        assert!(payload.looks_like_codex_interim_message());
    }

    #[test]
    fn looks_like_codex_interim_message_rejects_terminal_payload() {
        // Real terminal: completed/skipped steps with non-empty evidence.
        let payload = ExecutionPayload {
            change_summary: "did the thing".to_owned(),
            steps: vec![step(1, StepStatus::Completed), step(2, StepStatus::Skipped)],
            validation_evidence: vec!["cargo test passed".to_owned()],
            outstanding_risks: vec![],
        };
        assert!(!payload.looks_like_codex_interim_message());
    }

    #[test]
    fn looks_like_codex_interim_message_rejects_terminal_with_one_deferred_step() {
        // Codex-review #188 P1 finding: the contract permits a
        // mixed-status terminal payload — completed steps + one intended
        // follow-up — so long as evidence is present. We MUST NOT misfire
        // the interim detector on this shape.
        let payload = ExecutionPayload {
            change_summary: "did the main work; one item deferred".to_owned(),
            steps: vec![
                step(1, StepStatus::Completed),
                step(2, StepStatus::Intended),
            ],
            validation_evidence: vec!["cargo test passed".to_owned()],
            outstanding_risks: vec![],
        };
        assert!(
            !payload.looks_like_codex_interim_message(),
            "terminal payload with a deferred step + real evidence must not \
             be treated as the codex interim shape"
        );
    }

    #[test]
    fn looks_like_codex_interim_message_matches_issue_188_reopen_shape_with_placeholder_evidence() {
        // Issue #188 reopen: codex satisfies the codex-only minItems=1
        // schema constraint with a placeholder evidence string while
        // keeping every step in Intended. The placeholder describes the
        // workspace, not actual validation work. Original PR #189 detector
        // missed this because it required validation_evidence to be empty.
        let payload = ExecutionPayload {
            change_summary: "Starting the documentation review/edit task".to_owned(),
            steps: vec![
                step(1, StepStatus::Intended),
                step(2, StepStatus::Intended),
                step(3, StepStatus::Intended),
            ],
            validation_evidence: vec![
                "Workspace: /home/user/work; task accepted for execution.".to_owned()
            ],
            outstanding_risks: vec![],
        };
        assert!(
            payload.looks_like_codex_interim_message(),
            "all-intended steps + bogus placeholder evidence is the canonical \
             issue #188 reopen shape — must be detected"
        );
    }

    #[test]
    fn looks_like_codex_interim_message_rejects_empty_steps_array() {
        // Defensive: a payload with no steps at all is not the interim
        // shape (codex's interim always names at least one intended step).
        let payload = ExecutionPayload {
            change_summary: "no work needed".to_owned(),
            steps: vec![],
            validation_evidence: vec![],
            outstanding_risks: vec![],
        };
        assert!(!payload.looks_like_codex_interim_message());
    }
}
