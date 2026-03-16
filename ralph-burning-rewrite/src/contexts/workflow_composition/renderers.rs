#![forbid(unsafe_code)]

//! Deterministic Markdown renderers for validated stage payloads.
//!
//! Each renderer produces byte-identical output for the same input, uses canonical
//! `cycle` terminology, and derives content only from validated structured fields —
//! never from raw backend text.

use std::fmt::Write;

use crate::shared::domain::StageId;

use super::panel_contracts::{
    CompletionAggregatePayload, CompletionVotePayload, PromptRefinementPayload,
    PromptReviewPrimaryPayload, PromptValidationPayload, FinalReviewAggregatePayload,
    FinalReviewArbiterPayload, FinalReviewProposalPayload, FinalReviewVotePayload,
};
use super::payloads::{ExecutionPayload, PlanningPayload, ValidationPayload};

/// Render a planning payload to deterministic Markdown.
pub fn render_planning(stage_id: StageId, payload: &PlanningPayload) -> String {
    let mut out = String::new();

    writeln!(out, "# {}", stage_id.display_name()).unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Problem Framing").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.problem_framing).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Assumptions and Open Questions").unwrap();
    writeln!(out).unwrap();
    if payload.assumptions_or_open_questions.is_empty() {
        writeln!(out, "None.").unwrap();
    } else {
        for item in &payload.assumptions_or_open_questions {
            writeln!(out, "- {item}").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Proposed Work").unwrap();
    writeln!(out).unwrap();
    for item in &payload.proposed_work {
        writeln!(out, "{}. **{}**", item.order, item.summary).unwrap();
        if !item.details.is_empty() {
            writeln!(out, "   {}", item.details).unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Readiness").unwrap();
    writeln!(out).unwrap();
    let ready_str = if payload.readiness.ready { "Yes" } else { "No" };
    writeln!(out, "- **Ready:** {ready_str}").unwrap();
    if payload.readiness.risks.is_empty() {
        writeln!(out, "- **Risks:** None identified.").unwrap();
    } else {
        writeln!(out, "- **Risks:**").unwrap();
        for risk in &payload.readiness.risks {
            writeln!(out, "  - {risk}").unwrap();
        }
    }

    out
}

/// Render an execution payload to deterministic Markdown.
pub fn render_execution(stage_id: StageId, payload: &ExecutionPayload) -> String {
    let mut out = String::new();

    writeln!(out, "# {}", stage_id.display_name()).unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Change Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.change_summary).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Steps").unwrap();
    writeln!(out).unwrap();
    for step in &payload.steps {
        writeln!(
            out,
            "{}. [{}] {}",
            step.order, step.status, step.description
        )
        .unwrap();
    }
    writeln!(out).unwrap();

    writeln!(out, "## Validation Evidence").unwrap();
    writeln!(out).unwrap();
    if payload.validation_evidence.is_empty() {
        writeln!(out, "None provided.").unwrap();
    } else {
        for item in &payload.validation_evidence {
            writeln!(out, "- {item}").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Outstanding Risks").unwrap();
    writeln!(out).unwrap();
    if payload.outstanding_risks.is_empty() {
        writeln!(out, "None identified.").unwrap();
    } else {
        for risk in &payload.outstanding_risks {
            writeln!(out, "- {risk}").unwrap();
        }
    }

    out
}

/// Render a validation/review payload to deterministic Markdown.
pub fn render_validation(stage_id: StageId, payload: &ValidationPayload) -> String {
    let mut out = String::new();

    writeln!(out, "# {}", stage_id.display_name()).unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Outcome").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "**{}**", payload.outcome).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Evidence").unwrap();
    writeln!(out).unwrap();
    if payload.evidence.is_empty() {
        writeln!(out, "None provided.").unwrap();
    } else {
        for item in &payload.evidence {
            writeln!(out, "- {item}").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Findings and Gaps").unwrap();
    writeln!(out).unwrap();
    if payload.findings_or_gaps.is_empty() {
        writeln!(out, "None identified.").unwrap();
    } else {
        for item in &payload.findings_or_gaps {
            writeln!(out, "- {item}").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Follow-up and Amendments").unwrap();
    writeln!(out).unwrap();
    if payload.follow_up_or_amendments.is_empty() {
        writeln!(out, "None.").unwrap();
    } else {
        for item in &payload.follow_up_or_amendments {
            writeln!(out, "- {item}").unwrap();
        }
    }

    out
}

// ── Panel renderers ────────────────────────────────────────────────────────

/// Render a prompt refinement supporting artifact to deterministic Markdown.
pub fn render_prompt_refinement(
    _stage_id: StageId,
    payload: &PromptRefinementPayload,
    producer: &str,
) -> String {
    let mut out = String::new();

    writeln!(out, "# Prompt Refinement").unwrap();
    writeln!(out, "**Producer:** {producer}").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Refinement Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.refinement_summary).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Improvements").unwrap();
    writeln!(out).unwrap();
    if payload.improvements.is_empty() {
        writeln!(out, "None.").unwrap();
    } else {
        for item in &payload.improvements {
            writeln!(out, "- {item}").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Refined Prompt").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.refined_prompt).unwrap();

    out
}

/// Render a prompt validation supporting artifact to deterministic Markdown.
pub fn render_prompt_validation(
    _stage_id: StageId,
    payload: &PromptValidationPayload,
    producer: &str,
) -> String {
    let mut out = String::new();

    writeln!(out, "# Prompt Validation").unwrap();
    writeln!(out, "**Producer:** {producer}").unwrap();
    writeln!(out).unwrap();
    let verdict = if payload.accepted {
        "Accepted"
    } else {
        "Rejected"
    };
    writeln!(out, "## Verdict: {verdict}").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Evidence").unwrap();
    writeln!(out).unwrap();
    if payload.evidence.is_empty() {
        writeln!(out, "None provided.").unwrap();
    } else {
        for item in &payload.evidence {
            writeln!(out, "- {item}").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Concerns").unwrap();
    writeln!(out).unwrap();
    if payload.concerns.is_empty() {
        writeln!(out, "None.").unwrap();
    } else {
        for item in &payload.concerns {
            writeln!(out, "- {item}").unwrap();
        }
    }

    out
}

/// Render a prompt-review primary decision artifact to deterministic Markdown.
pub fn render_prompt_review_decision(
    _stage_id: StageId,
    payload: &PromptReviewPrimaryPayload,
) -> String {
    let mut out = String::new();

    writeln!(out, "# Prompt Review Decision").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "**Decision:** {}", payload.decision).unwrap();
    writeln!(
        out,
        "**Executed Reviewers:** {}",
        payload.executed_reviewers
    )
    .unwrap();
    writeln!(out, "**Accept:** {}", payload.accept_count).unwrap();
    writeln!(out, "**Reject:** {}", payload.reject_count).unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Refinement Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.refinement_summary).unwrap();

    out
}

/// Render a completion vote supporting artifact to deterministic Markdown.
pub fn render_completion_vote(
    _stage_id: StageId,
    payload: &CompletionVotePayload,
    producer: &str,
) -> String {
    let mut out = String::new();

    let vote = if payload.vote_complete {
        "Complete"
    } else {
        "Continue Work"
    };
    writeln!(out, "# Completion Vote").unwrap();
    writeln!(out, "**Producer:** {producer}").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Vote: {vote}").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Evidence").unwrap();
    writeln!(out).unwrap();
    if payload.evidence.is_empty() {
        writeln!(out, "None provided.").unwrap();
    } else {
        for item in &payload.evidence {
            writeln!(out, "- {item}").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Remaining Work").unwrap();
    writeln!(out).unwrap();
    if payload.remaining_work.is_empty() {
        writeln!(out, "None.").unwrap();
    } else {
        for item in &payload.remaining_work {
            writeln!(out, "- {item}").unwrap();
        }
    }

    out
}

/// Render a completion aggregate artifact to deterministic Markdown.
pub fn render_completion_aggregate(
    _stage_id: StageId,
    payload: &CompletionAggregatePayload,
) -> String {
    let mut out = String::new();

    writeln!(out, "# Completion Aggregate").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "**Verdict:** {}", payload.verdict).unwrap();
    writeln!(out, "**Complete Votes:** {}", payload.complete_votes).unwrap();
    writeln!(out, "**Continue Votes:** {}", payload.continue_votes).unwrap();
    writeln!(out, "**Total Voters:** {}", payload.total_voters).unwrap();
    writeln!(
        out,
        "**Consensus Threshold:** {:.2}",
        payload.consensus_threshold
    )
    .unwrap();
    writeln!(out, "**Min Completers:** {}", payload.min_completers).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Executed Voters").unwrap();
    writeln!(out).unwrap();
    if payload.executed_voters.is_empty() {
        writeln!(out, "None.").unwrap();
    } else {
        for voter in &payload.executed_voters {
            writeln!(out, "- {voter}").unwrap();
        }
    }

    out
}

/// Render a final-review proposal supporting artifact to deterministic Markdown.
pub fn render_final_review_proposal(
    payload: &FinalReviewProposalPayload,
    producer: &str,
) -> String {
    let mut out = String::new();

    writeln!(out, "# Final Review Proposal").unwrap();
    writeln!(out, "**Producer:** {producer}").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.summary).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Amendments").unwrap();
    writeln!(out).unwrap();
    if payload.amendments.is_empty() {
        writeln!(out, "None.").unwrap();
    } else {
        for (idx, amendment) in payload.amendments.iter().enumerate() {
            writeln!(out, "{}. {}", idx + 1, amendment.body).unwrap();
            if let Some(rationale) = &amendment.rationale {
                writeln!(out, "   Rationale: {rationale}").unwrap();
            }
        }
    }

    out
}

/// Render a final-review vote supporting artifact to deterministic Markdown.
pub fn render_final_review_vote(
    title: &str,
    payload: &FinalReviewVotePayload,
    producer: &str,
) -> String {
    let mut out = String::new();

    writeln!(out, "# {title}").unwrap();
    writeln!(out, "**Producer:** {producer}").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.summary).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Votes").unwrap();
    writeln!(out).unwrap();
    if payload.votes.is_empty() {
        writeln!(out, "None.").unwrap();
    } else {
        for vote in &payload.votes {
            writeln!(out, "- `{}`: {}", vote.amendment_id, vote.decision).unwrap();
            writeln!(out, "  {}", vote.rationale).unwrap();
        }
    }

    out
}

/// Render a final-review arbiter supporting artifact to deterministic Markdown.
pub fn render_final_review_arbiter(
    payload: &FinalReviewArbiterPayload,
    producer: &str,
) -> String {
    let mut out = String::new();

    writeln!(out, "# Final Review Arbiter").unwrap();
    writeln!(out, "**Producer:** {producer}").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "## Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.summary).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Rulings").unwrap();
    writeln!(out).unwrap();
    if payload.rulings.is_empty() {
        writeln!(out, "None.").unwrap();
    } else {
        for ruling in &payload.rulings {
            writeln!(out, "- `{}`: {}", ruling.amendment_id, ruling.decision).unwrap();
            writeln!(out, "  {}", ruling.rationale).unwrap();
        }
    }

    out
}

/// Render a final-review aggregate artifact to deterministic Markdown.
pub fn render_final_review_aggregate(payload: &FinalReviewAggregatePayload) -> String {
    let mut out = String::new();

    writeln!(out, "# Final Review Aggregate").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "**Summary:** {}", payload.summary).unwrap();
    writeln!(out, "**Restart Required:** {}", payload.restart_required).unwrap();
    writeln!(out, "**Force Completed:** {}", payload.force_completed).unwrap();
    writeln!(out, "**Total Reviewers:** {}", payload.total_reviewers).unwrap();
    writeln!(
        out,
        "**Unique Amendments:** {}",
        payload.unique_amendment_count
    )
    .unwrap();
    writeln!(
        out,
        "**Final Review Restarts:** {}/{}",
        payload.final_review_restart_count,
        payload.max_restarts
    )
    .unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Accepted").unwrap();
    writeln!(out).unwrap();
    if payload.accepted_amendment_ids.is_empty() {
        writeln!(out, "None.").unwrap();
    } else {
        for id in &payload.accepted_amendment_ids {
            writeln!(out, "- `{id}`").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Rejected").unwrap();
    writeln!(out).unwrap();
    if payload.rejected_amendment_ids.is_empty() {
        writeln!(out, "None.").unwrap();
    } else {
        for id in &payload.rejected_amendment_ids {
            writeln!(out, "- `{id}`").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Disputed").unwrap();
    writeln!(out).unwrap();
    if payload.disputed_amendment_ids.is_empty() {
        writeln!(out, "None.").unwrap();
    } else {
        for id in &payload.disputed_amendment_ids {
            writeln!(out, "- `{id}`").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Final Accepted Amendments").unwrap();
    writeln!(out).unwrap();
    if payload.final_accepted_amendments.is_empty() {
        writeln!(out, "None.").unwrap();
    } else {
        for amendment in &payload.final_accepted_amendments {
            writeln!(out, "### `{}`", amendment.amendment_id).unwrap();
            writeln!(out).unwrap();
            writeln!(out, "{}", amendment.normalized_body).unwrap();
            writeln!(out).unwrap();
        }
    }

    out
}
