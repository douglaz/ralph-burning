#![forbid(unsafe_code)]

//! Deterministic Markdown renderers for requirements payloads.
//!
//! Produces byte-identical output for the same input. Derives content only
//! from validated structured fields — never from raw backend text.

use std::fmt::Write;

use super::model::{
    ProjectSeedPayload, QuestionSetPayload, RequirementsDraftPayload, RequirementsReviewPayload,
};

/// Render a question set payload to deterministic Markdown.
pub fn render_question_set(payload: &QuestionSetPayload) -> String {
    let mut out = String::new();

    writeln!(out, "# Clarifying Questions").unwrap();
    writeln!(out).unwrap();

    if payload.questions.is_empty() {
        writeln!(out, "No clarifying questions needed.").unwrap();
        return out;
    }

    for q in &payload.questions {
        let required_marker = if q.required { " **(required)**" } else { "" };
        writeln!(out, "## {}{}", q.id, required_marker).unwrap();
        writeln!(out).unwrap();
        writeln!(out, "{}", q.prompt).unwrap();
        writeln!(out).unwrap();
        if !q.rationale.is_empty() {
            writeln!(out, "_Rationale:_ {}", q.rationale).unwrap();
            writeln!(out).unwrap();
        }
        if let Some(default) = &q.suggested_default {
            writeln!(out, "_Suggested default:_ {default}").unwrap();
            writeln!(out).unwrap();
        }
    }

    out
}

/// Render a requirements draft payload to deterministic Markdown.
pub fn render_requirements_draft(payload: &RequirementsDraftPayload) -> String {
    let mut out = String::new();

    writeln!(out, "# Requirements Draft").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Problem Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.problem_summary).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Goals").unwrap();
    writeln!(out).unwrap();
    for goal in &payload.goals {
        writeln!(out, "- {goal}").unwrap();
    }
    writeln!(out).unwrap();

    writeln!(out, "## Non-Goals").unwrap();
    writeln!(out).unwrap();
    if payload.non_goals.is_empty() {
        writeln!(out, "None specified.").unwrap();
    } else {
        for item in &payload.non_goals {
            writeln!(out, "- {item}").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Constraints").unwrap();
    writeln!(out).unwrap();
    if payload.constraints.is_empty() {
        writeln!(out, "None specified.").unwrap();
    } else {
        for item in &payload.constraints {
            writeln!(out, "- {item}").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Acceptance Criteria").unwrap();
    writeln!(out).unwrap();
    for item in &payload.acceptance_criteria {
        writeln!(out, "- {item}").unwrap();
    }
    writeln!(out).unwrap();

    writeln!(out, "## Risks and Open Questions").unwrap();
    writeln!(out).unwrap();
    if payload.risks_or_open_questions.is_empty() {
        writeln!(out, "None identified.").unwrap();
    } else {
        for item in &payload.risks_or_open_questions {
            writeln!(out, "- {item}").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Recommended Flow").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "**{}**", payload.recommended_flow).unwrap();

    out
}

/// Render a requirements review payload to deterministic Markdown.
pub fn render_requirements_review(payload: &RequirementsReviewPayload) -> String {
    let mut out = String::new();

    writeln!(out, "# Requirements Review").unwrap();
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

    writeln!(out, "## Findings").unwrap();
    writeln!(out).unwrap();
    if payload.findings.is_empty() {
        writeln!(out, "None identified.").unwrap();
    } else {
        for item in &payload.findings {
            writeln!(out, "- {item}").unwrap();
        }
    }
    writeln!(out).unwrap();

    if !payload.follow_ups.is_empty() {
        writeln!(out, "## Follow-ups").unwrap();
        writeln!(out).unwrap();
        for item in &payload.follow_ups {
            writeln!(out, "- {item}").unwrap();
        }
    }

    out
}

/// Render a project seed payload to deterministic Markdown.
pub fn render_project_seed(payload: &ProjectSeedPayload) -> String {
    let mut out = String::new();

    writeln!(out, "# Project Seed").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Project").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "- **ID:** {}", payload.project_id).unwrap();
    writeln!(out, "- **Name:** {}", payload.project_name).unwrap();
    writeln!(out, "- **Flow:** {}", payload.flow).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Handoff Summary").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.handoff_summary).unwrap();
    writeln!(out).unwrap();

    if !payload.follow_ups.is_empty() {
        writeln!(out, "## Follow-ups").unwrap();
        writeln!(out).unwrap();
        for item in &payload.follow_ups {
            writeln!(out, "- {item}").unwrap();
        }
        writeln!(out).unwrap();
    }

    writeln!(out, "## Suggested Command").unwrap();
    writeln!(out).unwrap();
    writeln!(
        out,
        "```\nralph-burning project create --id {} --name \"{}\" --flow {} --prompt <seed/prompt.md>\n```",
        payload.project_id, payload.project_name, payload.flow
    )
    .unwrap();

    out
}
