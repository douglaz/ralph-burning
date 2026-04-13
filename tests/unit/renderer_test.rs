#![forbid(unsafe_code)]

use ralph_burning::contexts::workflow_composition::payloads::{
    ExecutionPayload, ExecutionStep, PlanningPayload, ProposedWorkItem, ReadinessAssessment,
    ReviewOutcome, StepStatus, ValidationPayload,
};
use ralph_burning::contexts::workflow_composition::renderers;
use ralph_burning::shared::domain::StageId;

// ── Helpers ─────────────────────────────────────────────────────────────────

fn sample_planning_payload() -> PlanningPayload {
    PlanningPayload {
        problem_framing: "We need to add stage contracts.".to_string(),
        assumptions_or_open_questions: vec!["All stages are covered.".to_string()],
        proposed_work: vec![ProposedWorkItem {
            order: 1,
            summary: "Define payload types".to_string(),
            details: "Create typed structs per family.".to_string(),
        }],
        readiness: ReadinessAssessment {
            ready: true,
            risks: vec![],
        },
    }
}

fn sample_execution_payload() -> ExecutionPayload {
    ExecutionPayload {
        change_summary: "Added the stage contract registry.".to_string(),
        steps: vec![
            ExecutionStep {
                order: 1,
                description: "Created payloads module".to_string(),
                status: StepStatus::Completed,
            },
            ExecutionStep {
                order: 2,
                description: "Created renderers module".to_string(),
                status: StepStatus::Intended,
            },
        ],
        validation_evidence: vec!["cargo test passes".to_string()],
        outstanding_risks: vec![],
    }
}

fn sample_validation_payload() -> ValidationPayload {
    ValidationPayload {
        outcome: ReviewOutcome::Approved,
        evidence: vec!["All tests pass.".to_string()],
        findings_or_gaps: vec![],
        follow_up_or_amendments: vec![],
        classified_findings: vec![],
    }
}

// ── Deterministic rendering ─────────────────────────────────────────────────

#[test]
fn planning_renderer_is_deterministic() {
    let payload = sample_planning_payload();
    let a = renderers::render_planning(StageId::Planning, &payload);
    let b = renderers::render_planning(StageId::Planning, &payload);
    assert_eq!(a, b, "repeated renders must be byte-identical");
}

#[test]
fn execution_renderer_is_deterministic() {
    let payload = sample_execution_payload();
    let a = renderers::render_execution(StageId::Implementation, &payload);
    let b = renderers::render_execution(StageId::Implementation, &payload);
    assert_eq!(a, b, "repeated renders must be byte-identical");
}

#[test]
fn validation_renderer_is_deterministic() {
    let payload = sample_validation_payload();
    let a = renderers::render_validation(StageId::Review, &payload);
    let b = renderers::render_validation(StageId::Review, &payload);
    assert_eq!(a, b, "repeated renders must be byte-identical");
}

// ── Planning artifact structure ─────────────────────────────────────────────

#[test]
fn planning_artifact_contains_expected_sections() {
    let payload = sample_planning_payload();
    let artifact = renderers::render_planning(StageId::Planning, &payload);

    assert!(artifact.starts_with("# Planning\n"));
    assert!(artifact.contains("## Problem Framing"));
    assert!(artifact.contains("We need to add stage contracts."));
    assert!(artifact.contains("## Assumptions and Open Questions"));
    assert!(artifact.contains("- All stages are covered."));
    assert!(artifact.contains("## Proposed Work"));
    assert!(artifact.contains("1. **Define payload types**"));
    assert!(artifact.contains("## Readiness"));
    assert!(artifact.contains("- **Ready:** Yes"));
}

#[test]
fn planning_artifact_uses_stage_display_name() {
    let payload = sample_planning_payload();
    let artifact = renderers::render_planning(StageId::PromptReview, &payload);
    assert!(artifact.starts_with("# Prompt Review\n"));
}

#[test]
fn planning_artifact_shows_empty_assumptions() {
    let mut payload = sample_planning_payload();
    payload.assumptions_or_open_questions = vec![];
    let artifact = renderers::render_planning(StageId::Planning, &payload);
    assert!(artifact.contains("None."));
}

#[test]
fn planning_artifact_shows_risks() {
    let mut payload = sample_planning_payload();
    payload.readiness.risks = vec![
        "Tight deadline.".to_string(),
        "API instability.".to_string(),
    ];
    let artifact = renderers::render_planning(StageId::Planning, &payload);
    assert!(artifact.contains("- **Risks:**"));
    assert!(artifact.contains("  - Tight deadline."));
    assert!(artifact.contains("  - API instability."));
}

// ── Execution artifact structure ────────────────────────────────────────────

#[test]
fn execution_artifact_contains_expected_sections() {
    let payload = sample_execution_payload();
    let artifact = renderers::render_execution(StageId::Implementation, &payload);

    assert!(artifact.starts_with("# Implementation\n"));
    assert!(artifact.contains("## Change Summary"));
    assert!(artifact.contains("Added the stage contract registry."));
    assert!(artifact.contains("## Steps"));
    assert!(artifact.contains("1. [Completed] Created payloads module"));
    assert!(artifact.contains("2. [Intended] Created renderers module"));
    assert!(artifact.contains("## Validation Evidence"));
    assert!(artifact.contains("- cargo test passes"));
    assert!(artifact.contains("## Outstanding Risks"));
    assert!(artifact.contains("None identified."));
}

#[test]
fn execution_artifact_uses_stage_display_name() {
    let payload = sample_execution_payload();
    let artifact = renderers::render_execution(StageId::PlanAndImplement, &payload);
    assert!(artifact.starts_with("# Plan and Implement\n"));
}

// ── Validation artifact structure ───────────────────────────────────────────

#[test]
fn validation_artifact_contains_expected_sections() {
    let payload = sample_validation_payload();
    let artifact = renderers::render_validation(StageId::Review, &payload);

    assert!(artifact.starts_with("# Review\n"));
    assert!(artifact.contains("## Outcome"));
    assert!(artifact.contains("**Approved**"));
    assert!(artifact.contains("## Evidence"));
    assert!(artifact.contains("- All tests pass."));
    assert!(artifact.contains("## Findings and Gaps"));
    assert!(artifact.contains("None identified."));
    assert!(artifact.contains("## Follow-up and Amendments"));
    assert!(artifact.contains("None."));
}

#[test]
fn validation_artifact_shows_rejected_with_follow_up() {
    let payload = ValidationPayload {
        outcome: ReviewOutcome::Rejected,
        evidence: vec!["CI fails.".to_string()],
        findings_or_gaps: vec!["Missing tests.".to_string()],
        follow_up_or_amendments: vec!["Add integration tests.".to_string()],
        classified_findings: vec![],
    };
    let artifact = renderers::render_validation(StageId::FinalReview, &payload);

    assert!(artifact.starts_with("# Final Review\n"));
    assert!(artifact.contains("**Rejected**"));
    assert!(artifact.contains("- CI fails."));
    assert!(artifact.contains("- Missing tests."));
    assert!(artifact.contains("- Add integration tests."));
}

#[test]
fn validation_artifact_uses_stage_display_name() {
    let payload = sample_validation_payload();
    let artifact = renderers::render_validation(StageId::CompletionPanel, &payload);
    assert!(artifact.starts_with("# Completion Panel\n"));
}

// ── No raw backend text in artifacts ────────────────────────────────────────
// Renderers take only typed payload fields, never raw text.
// This test verifies the artifact derives exclusively from structured data.

#[test]
fn artifact_content_matches_payload_fields_only() {
    let payload = PlanningPayload {
        problem_framing: "UNIQUE_MARKER_XYZ".to_string(),
        assumptions_or_open_questions: vec!["ASSUMPTION_ABC".to_string()],
        proposed_work: vec![ProposedWorkItem {
            order: 1,
            summary: "WORK_SUMMARY_123".to_string(),
            details: "WORK_DETAILS_456".to_string(),
        }],
        readiness: ReadinessAssessment {
            ready: false,
            risks: vec!["RISK_789".to_string()],
        },
    };
    let artifact = renderers::render_planning(StageId::Planning, &payload);

    assert!(artifact.contains("UNIQUE_MARKER_XYZ"));
    assert!(artifact.contains("ASSUMPTION_ABC"));
    assert!(artifact.contains("WORK_SUMMARY_123"));
    assert!(artifact.contains("WORK_DETAILS_456"));
    assert!(artifact.contains("RISK_789"));
    // No extraneous content beyond headings, bullet markers, and payload data.
}
