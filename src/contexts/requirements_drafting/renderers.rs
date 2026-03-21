#![forbid(unsafe_code)]

//! Deterministic Markdown renderers for requirements payloads.
//!
//! Produces byte-identical output for the same input. Derives content only
//! from validated structured fields — never from raw backend text.

use std::fmt::Write;

use super::model::{
    GapAnalysisPayload, IdeationPayload, ImplementationSpecPayload, ProjectSeedPayload,
    QuestionSetPayload, RequirementsDraftPayload, RequirementsReviewPayload, ResearchPayload,
    RevisionFeedback, SynthesisPayload, ValidationPayload,
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
    writeln!(out, "- **Version:** {}", payload.version).unwrap();
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

    if let Some(ref source) = payload.source {
        writeln!(out, "## Source").unwrap();
        writeln!(out).unwrap();
        writeln!(out, "- **Mode:** {}", source.mode).unwrap();
        writeln!(out, "- **Run ID:** {}", source.run_id).unwrap();
        writeln!(out, "- **Question Rounds:** {}", source.question_rounds).unwrap();
        if let Some(revisions) = source.quick_revisions {
            writeln!(out, "- **Quick Revisions:** {revisions}").unwrap();
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

// ── Full-mode stage renderers ───────────────────────────────────────────────

/// Render an ideation payload to deterministic Markdown.
pub fn render_ideation(payload: &IdeationPayload) -> String {
    let mut out = String::new();

    writeln!(out, "# Ideation").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Themes").unwrap();
    writeln!(out).unwrap();
    for theme in &payload.themes {
        writeln!(out, "- {theme}").unwrap();
    }
    writeln!(out).unwrap();

    writeln!(out, "## Key Concepts").unwrap();
    writeln!(out).unwrap();
    if payload.key_concepts.is_empty() {
        writeln!(out, "None identified.").unwrap();
    } else {
        for concept in &payload.key_concepts {
            writeln!(out, "- {concept}").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Initial Scope").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.initial_scope).unwrap();
    writeln!(out).unwrap();

    if !payload.open_questions.is_empty() {
        writeln!(out, "## Open Questions").unwrap();
        writeln!(out).unwrap();
        for q in &payload.open_questions {
            writeln!(out, "- {q}").unwrap();
        }
        writeln!(out).unwrap();
    }

    out
}

/// Render a research payload to deterministic Markdown.
pub fn render_research(payload: &ResearchPayload) -> String {
    let mut out = String::new();

    writeln!(out, "# Research").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Findings").unwrap();
    writeln!(out).unwrap();
    if payload.findings.is_empty() {
        writeln!(out, "No findings.").unwrap();
    } else {
        for finding in &payload.findings {
            writeln!(out, "### {}", finding.area).unwrap();
            writeln!(out).unwrap();
            writeln!(out, "{}", finding.summary).unwrap();
            writeln!(out).unwrap();
            writeln!(out, "_Relevance:_ {}", finding.relevance).unwrap();
            writeln!(out).unwrap();
        }
    }

    writeln!(out, "## Constraints Discovered").unwrap();
    writeln!(out).unwrap();
    if payload.constraints_discovered.is_empty() {
        writeln!(out, "None discovered.").unwrap();
    } else {
        for c in &payload.constraints_discovered {
            writeln!(out, "- {c}").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Prior Art").unwrap();
    writeln!(out).unwrap();
    if payload.prior_art.is_empty() {
        writeln!(out, "None identified.").unwrap();
    } else {
        for item in &payload.prior_art {
            writeln!(out, "- {item}").unwrap();
        }
    }
    writeln!(out).unwrap();

    writeln!(out, "## Technical Context").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.technical_context).unwrap();

    out
}

/// Render a synthesis payload to deterministic Markdown.
pub fn render_synthesis(payload: &SynthesisPayload) -> String {
    let mut out = String::new();

    writeln!(out, "# Synthesis").unwrap();
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

/// Render an implementation spec payload to deterministic Markdown.
pub fn render_implementation_spec(payload: &ImplementationSpecPayload) -> String {
    let mut out = String::new();

    writeln!(out, "# Implementation Spec").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Architecture Overview").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.architecture_overview).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Components").unwrap();
    writeln!(out).unwrap();
    for component in &payload.components {
        writeln!(out, "### {}", component.name).unwrap();
        writeln!(out).unwrap();
        writeln!(out, "{}", component.responsibility).unwrap();
        writeln!(out).unwrap();
        if !component.interfaces.is_empty() {
            writeln!(out, "**Interfaces:**").unwrap();
            for iface in &component.interfaces {
                writeln!(out, "- {iface}").unwrap();
            }
            writeln!(out).unwrap();
        }
    }

    if !payload.integration_points.is_empty() {
        writeln!(out, "## Integration Points").unwrap();
        writeln!(out).unwrap();
        for point in &payload.integration_points {
            writeln!(out, "- {point}").unwrap();
        }
        writeln!(out).unwrap();
    }

    if !payload.migration_notes.is_empty() {
        writeln!(out, "## Migration Notes").unwrap();
        writeln!(out).unwrap();
        for note in &payload.migration_notes {
            writeln!(out, "- {note}").unwrap();
        }
        writeln!(out).unwrap();
    }

    out
}

/// Render a gap analysis payload to deterministic Markdown.
pub fn render_gap_analysis(payload: &GapAnalysisPayload) -> String {
    let mut out = String::new();

    writeln!(out, "# Gap Analysis").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Gaps").unwrap();
    writeln!(out).unwrap();
    if payload.gaps.is_empty() {
        writeln!(out, "No gaps identified.").unwrap();
    } else {
        for gap in &payload.gaps {
            writeln!(out, "### {} ({})", gap.area, gap.severity).unwrap();
            writeln!(out).unwrap();
            writeln!(out, "{}", gap.description).unwrap();
            writeln!(out).unwrap();
            writeln!(out, "_Suggested resolution:_ {}", gap.suggested_resolution).unwrap();
            writeln!(out).unwrap();
        }
    }

    writeln!(out, "## Coverage Assessment").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "{}", payload.coverage_assessment).unwrap();
    writeln!(out).unwrap();

    if !payload.blocking_gaps.is_empty() {
        writeln!(out, "## Blocking Gaps").unwrap();
        writeln!(out).unwrap();
        for gap in &payload.blocking_gaps {
            writeln!(out, "- {gap}").unwrap();
        }
        writeln!(out).unwrap();
    }

    out
}

/// Render a validation payload to deterministic Markdown.
pub fn render_validation(payload: &ValidationPayload) -> String {
    let mut out = String::new();

    writeln!(out, "# Validation").unwrap();
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

    if !payload.blocking_issues.is_empty() {
        writeln!(out, "## Blocking Issues").unwrap();
        writeln!(out).unwrap();
        for issue in &payload.blocking_issues {
            writeln!(out, "- {issue}").unwrap();
        }
        writeln!(out).unwrap();
    }

    if !payload.missing_information.is_empty() {
        writeln!(out, "## Missing Information").unwrap();
        writeln!(out).unwrap();
        for item in &payload.missing_information {
            writeln!(out, "- {item}").unwrap();
        }
        writeln!(out).unwrap();
    }

    out
}

/// Render revision feedback to deterministic Markdown.
pub fn render_revision_feedback(payload: &RevisionFeedback) -> String {
    let mut out = String::new();

    writeln!(out, "# Revision Feedback").unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Outcome").unwrap();
    writeln!(out).unwrap();
    writeln!(out, "**{}**", payload.outcome).unwrap();
    writeln!(out).unwrap();

    writeln!(out, "## Revision Notes").unwrap();
    writeln!(out).unwrap();
    if payload.revision_notes.is_empty() {
        writeln!(out, "No revision notes.").unwrap();
    } else {
        for note in &payload.revision_notes {
            writeln!(out, "- {note}").unwrap();
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
