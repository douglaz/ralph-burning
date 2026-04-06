#![forbid(unsafe_code)]

//! Typed panel contracts, schemas, and canonical aggregate payload shapes
//! for prompt-review, completion, and final-review work.
//!
//! These contracts must not reuse the generic planning/validation payloads
//! where that would lose panel-specific fields such as refined prompt text,
//! vote counts, amendment metadata, or aggregate verdict metadata.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::shared::domain::StageId;

// ── Prompt-Review Contracts ────────────────────────────────────────────────

/// Payload returned by the prompt-review refiner.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PromptRefinementPayload {
    /// The refined prompt text produced by the refiner.
    pub refined_prompt: String,
    /// Summary of changes made during refinement.
    pub refinement_summary: String,
    /// Areas of the prompt that were improved.
    pub improvements: Vec<String>,
}

/// Payload returned by each prompt-review validator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PromptValidationPayload {
    /// Whether the validator accepts the refined prompt.
    pub accepted: bool,
    /// Evidence supporting the validation decision.
    pub evidence: Vec<String>,
    /// Specific concerns or issues found (populated on rejection).
    pub concerns: Vec<String>,
}

/// Decision outcome for the prompt-review stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PromptReviewDecision {
    Accepted,
    Rejected,
}

impl std::fmt::Display for PromptReviewDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Accepted => f.write_str("Accepted"),
            Self::Rejected => f.write_str("Rejected"),
        }
    }
}

/// Canonical primary record for the prompt-review stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PromptReviewPrimaryPayload {
    /// The final decision: accepted or rejected.
    pub decision: PromptReviewDecision,
    /// The refined prompt text (only meaningful on accept).
    pub refined_prompt: String,
    /// Number of validators that executed.
    pub executed_reviewers: usize,
    /// Number of validators that accepted.
    pub accept_count: usize,
    /// Number of validators that rejected.
    pub reject_count: usize,
    /// Summary from the refinement phase.
    pub refinement_summary: String,
}

// ── Completion Contracts ───────────────────────────────────────────────────

/// Payload returned by each completion panel voter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CompletionVotePayload {
    /// Whether this voter considers the work complete.
    pub vote_complete: bool,
    /// Evidence supporting the vote.
    pub evidence: Vec<String>,
    /// Remaining concerns or work items (populated when voting continue).
    pub remaining_work: Vec<String>,
}

/// Aggregate verdict for the completion panel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CompletionVerdict {
    Complete,
    ContinueWork,
}

impl std::fmt::Display for CompletionVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Complete => f.write_str("Complete"),
            Self::ContinueWork => f.write_str("Continue Work"),
        }
    }
}

/// Canonical aggregate record for the completion panel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct CompletionAggregatePayload {
    /// The aggregate verdict.
    pub verdict: CompletionVerdict,
    /// Number of votes for "complete".
    pub complete_votes: usize,
    /// Number of votes for "continue work".
    pub continue_votes: usize,
    /// Total number of executed voters.
    pub total_voters: usize,
    /// Consensus threshold used for the decision.
    pub consensus_threshold: f64,
    /// Originally configured minimum completers.
    pub min_completers: usize,
    /// Effective minimum after reducing for exhausted backends.
    /// When no backends are exhausted this equals `min_completers`.
    #[serde(default)]
    pub effective_min_completers: usize,
    /// Number of completers skipped due to backend exhaustion.
    #[serde(default)]
    pub exhausted_count: usize,
    /// Identifiers of the executed voters (backend family / model pairs).
    pub executed_voters: Vec<String>,
}

// ── Final-Review Contracts ─────────────────────────────────────────────────

/// A raw amendment proposal returned by a final-review reviewer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FinalReviewProposal {
    /// The free-form amendment body. This is canonicalized and deduplicated by
    /// the orchestration layer before voting.
    pub body: String,
    /// Optional reviewer-provided rationale for why this amendment matters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
}

/// Payload returned by each final-review reviewer during proposal collection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FinalReviewProposalPayload {
    /// Summary of the reviewer pass.
    pub summary: String,
    /// Proposed amendments. An empty list means "no amendments".
    pub amendments: Vec<FinalReviewProposal>,
}

/// Per-amendment vote used by both the planner-position step and reviewer voting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FinalReviewVoteDecision {
    Accept,
    Reject,
}

impl std::fmt::Display for FinalReviewVoteDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Accept => f.write_str("Accept"),
            Self::Reject => f.write_str("Reject"),
        }
    }
}

/// A vote on a single amendment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FinalReviewVote {
    pub amendment_id: String,
    pub decision: FinalReviewVoteDecision,
    pub rationale: String,
}

/// Payload returned by the planner-position step and each reviewer voting pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FinalReviewVotePayload {
    pub summary: String,
    pub votes: Vec<FinalReviewVote>,
}

/// A disputed-amendment ruling returned by the arbiter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FinalReviewArbiterRuling {
    pub amendment_id: String,
    pub decision: FinalReviewVoteDecision,
    pub rationale: String,
}

/// Payload returned by the final-review arbiter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FinalReviewArbiterPayload {
    pub summary: String,
    pub rulings: Vec<FinalReviewArbiterRuling>,
}

/// Source metadata preserved when duplicate amendments are merged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FinalReviewAmendmentSource {
    pub reviewer_id: String,
    pub backend_family: String,
    pub model_id: String,
}

/// Canonical amendment metadata used by the final-review aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FinalReviewCanonicalAmendment {
    pub amendment_id: String,
    pub normalized_body: String,
    pub sources: Vec<FinalReviewAmendmentSource>,
}

/// Canonical aggregate record for the final-review panel.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FinalReviewAggregatePayload {
    pub restart_required: bool,
    pub force_completed: bool,
    pub total_reviewers: usize,
    pub total_proposed_amendments: usize,
    pub unique_amendment_count: usize,
    pub accepted_amendment_ids: Vec<String>,
    pub rejected_amendment_ids: Vec<String>,
    pub disputed_amendment_ids: Vec<String>,
    pub amendments: Vec<FinalReviewCanonicalAmendment>,
    pub final_accepted_amendments: Vec<FinalReviewCanonicalAmendment>,
    pub final_review_restart_count: u32,
    pub max_restarts: u32,
    pub summary: String,
    /// Number of reviewers skipped due to backend exhaustion.
    #[serde(default)]
    pub exhausted_count: usize,
    /// Effective minimum reviewers after reducing for exhausted backends.
    /// When no backends are exhausted this equals the configured minimum.
    #[serde(default)]
    pub effective_min_reviewers: usize,
}

// ── Record Kind ────────────────────────────────────────────────────────────

/// Discriminant for payload/artifact record types within a stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RecordKind {
    /// The canonical primary record for a single-agent stage.
    StagePrimary,
    /// A per-agent supporting record within a panel stage.
    StageSupporting,
    /// The canonical aggregate record for a panel stage.
    StageAggregate,
}

impl std::fmt::Display for RecordKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StagePrimary => f.write_str("primary"),
            Self::StageSupporting => f.write_str("supporting"),
            Self::StageAggregate => f.write_str("aggregate"),
        }
    }
}

/// Producer metadata describing who produced a particular record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RecordProducer {
    /// Produced by a backend agent invocation.
    Agent {
        backend_family: String,
        model_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        adapter_reported_backend_family: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        adapter_reported_model_id: Option<String>,
    },
    /// Produced by local validation (e.g., pre-commit checks).
    LocalValidation { command: String },
    /// Produced by the system (e.g., aggregate computation).
    System { component: String },
}

impl std::fmt::Display for RecordProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Agent {
                backend_family,
                model_id,
                ..
            } => write!(f, "agent:{backend_family}/{model_id}"),
            Self::LocalValidation { command } => write!(f, "local:{command}"),
            Self::System { component } => write!(f, "system:{component}"),
        }
    }
}

// ── Panel payload wrapper ──────────────────────────────────────────────────

/// Typed wrapper for all panel-specific payloads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "panel_type", content = "data")]
pub enum PanelPayload {
    PromptRefinement(PromptRefinementPayload),
    PromptValidation(PromptValidationPayload),
    PromptReviewPrimary(PromptReviewPrimaryPayload),
    CompletionVote(CompletionVotePayload),
    CompletionAggregate(CompletionAggregatePayload),
    FinalReviewProposal(FinalReviewProposalPayload),
    FinalReviewVote(FinalReviewVotePayload),
    FinalReviewArbiter(FinalReviewArbiterPayload),
    FinalReviewAggregate(FinalReviewAggregatePayload),
}

/// Schema routing helper: returns the JSON schema for a given panel contract.
pub fn panel_json_schema(stage_id: StageId, role: &str) -> serde_json::Value {
    let schema = match (stage_id, role) {
        (StageId::PromptReview, "refiner") => {
            serde_json::to_value(schemars::schema_for!(PromptRefinementPayload))
        }
        (StageId::PromptReview, "validator") => {
            serde_json::to_value(schemars::schema_for!(PromptValidationPayload))
        }
        (StageId::CompletionPanel, "completer") => {
            serde_json::to_value(schemars::schema_for!(CompletionVotePayload))
        }
        (StageId::FinalReview, "reviewer") => {
            serde_json::to_value(schemars::schema_for!(FinalReviewProposalPayload))
        }
        (StageId::FinalReview, "voter") => {
            serde_json::to_value(schemars::schema_for!(FinalReviewVotePayload))
        }
        (StageId::FinalReview, "arbiter") => {
            serde_json::to_value(schemars::schema_for!(FinalReviewArbiterPayload))
        }
        _ => Ok(serde_json::Value::Object(Default::default())),
    };
    schema.unwrap_or_else(|_| serde_json::Value::Object(Default::default()))
}
