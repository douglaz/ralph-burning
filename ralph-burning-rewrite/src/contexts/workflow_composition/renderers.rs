#![forbid(unsafe_code)]

//! Deterministic Markdown renderers for validated stage payloads.
//!
//! Each renderer produces byte-identical output for the same input, uses canonical
//! `cycle` terminology, and derives content only from validated structured fields —
//! never from raw backend text.

use std::fmt::Write;

use crate::shared::domain::StageId;

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
