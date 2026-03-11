#![forbid(unsafe_code)]

use ralph_burning::contexts::workflow_composition::contracts::{
    all_contracts, contract_for_stage, ContractFamily,
};
use ralph_burning::contexts::workflow_composition::{built_in_flows, flow_definition};
use ralph_burning::shared::domain::{FailureClass, FlowPreset, StageId};
use ralph_burning::shared::error::ContractError;
use serde_json::json;

// ── Helpers ─────────────────────────────────────────────────────────────────

fn valid_planning_json() -> serde_json::Value {
    json!({
        "problem_framing": "Implement stage contracts for the orchestrator.",
        "assumptions_or_open_questions": ["Schema coverage is exhaustive."],
        "proposed_work": [
            {
                "order": 1,
                "summary": "Define payload types",
                "details": "Create typed structs for each contract family."
            }
        ],
        "readiness": {
            "ready": true,
            "risks": []
        }
    })
}

fn valid_execution_json() -> serde_json::Value {
    json!({
        "change_summary": "Added stage contract registry.",
        "steps": [
            {
                "order": 1,
                "description": "Created payloads module",
                "status": "completed"
            }
        ],
        "validation_evidence": ["cargo test passes"],
        "outstanding_risks": []
    })
}

fn valid_validation_approved_json() -> serde_json::Value {
    json!({
        "outcome": "approved",
        "evidence": ["All tests pass."],
        "findings_or_gaps": [],
        "follow_up_or_amendments": []
    })
}

fn valid_validation_rejected_json() -> serde_json::Value {
    json!({
        "outcome": "rejected",
        "evidence": ["Tests fail on CI."],
        "findings_or_gaps": ["Missing edge-case coverage."],
        "follow_up_or_amendments": ["Add tests for empty inputs."]
    })
}

// ── Registry completeness ───────────────────────────────────────────────────

#[test]
fn every_stage_id_has_a_contract() {
    let contracts = all_contracts();
    assert_eq!(contracts.len(), StageId::ALL.len());

    for &stage_id in &StageId::ALL {
        let contract = contract_for_stage(stage_id);
        assert_eq!(contract.stage_id, stage_id);
    }
}

#[test]
fn every_stage_in_every_built_in_flow_has_contract_coverage() {
    for flow in built_in_flows() {
        for &stage_id in flow.stages {
            let contract = contract_for_stage(stage_id);
            assert_eq!(
                contract.stage_id, stage_id,
                "stage {} in flow {} has no matching contract",
                stage_id, flow.preset
            );
        }
    }
}

#[test]
fn planning_stages_map_to_planning_family() {
    for stage_id in [
        StageId::PromptReview,
        StageId::Planning,
        StageId::DocsPlan,
        StageId::CiPlan,
    ] {
        assert_eq!(
            contract_for_stage(stage_id).family,
            ContractFamily::Planning,
            "{stage_id} should be Planning family"
        );
    }
}

#[test]
fn execution_stages_map_to_execution_family() {
    for stage_id in [
        StageId::Implementation,
        StageId::PlanAndImplement,
        StageId::ApplyFixes,
        StageId::DocsUpdate,
        StageId::CiUpdate,
    ] {
        assert_eq!(
            contract_for_stage(stage_id).family,
            ContractFamily::Execution,
            "{stage_id} should be Execution family"
        );
    }
}

#[test]
fn validation_stages_map_to_validation_family() {
    for stage_id in [
        StageId::Qa,
        StageId::DocsValidation,
        StageId::CiValidation,
        StageId::AcceptanceQa,
        StageId::Review,
        StageId::FinalReview,
        StageId::CompletionPanel,
    ] {
        assert_eq!(
            contract_for_stage(stage_id).family,
            ContractFamily::Validation,
            "{stage_id} should be Validation family"
        );
    }
}

// ── Schema validation ───────────────────────────────────────────────────────

#[test]
fn schema_failure_short_circuits_semantic_validation() {
    let contract = contract_for_stage(StageId::Planning);
    // Missing required fields → schema error, no domain error.
    let bad_json = json!({"not_a_real_field": true});
    let err = contract.evaluate(&bad_json).unwrap_err();
    assert!(
        matches!(err, ContractError::SchemaValidation { .. }),
        "expected SchemaValidation, got: {err:?}"
    );
    assert_eq!(err.failure_class(), FailureClass::SchemaValidationFailure);
}

#[test]
fn schema_failure_short_circuits_rendering() {
    // An execution contract with invalid JSON should never reach the renderer.
    let contract = contract_for_stage(StageId::Implementation);
    let bad_json = json!(42);
    let err = contract.evaluate(&bad_json).unwrap_err();
    assert!(matches!(err, ContractError::SchemaValidation { .. }));
}

// ── Semantic / domain validation ────────────────────────────────────────────

#[test]
fn domain_failure_short_circuits_rendering() {
    let contract = contract_for_stage(StageId::Planning);
    // Schema-valid but domain-invalid: empty problem_framing.
    let json = json!({
        "problem_framing": "   ",
        "assumptions_or_open_questions": [],
        "proposed_work": [],
        "readiness": { "ready": false, "risks": [] }
    });
    let err = contract.evaluate(&json).unwrap_err();
    assert!(
        matches!(err, ContractError::DomainValidation { .. }),
        "expected DomainValidation, got: {err:?}"
    );
    assert_eq!(err.failure_class(), FailureClass::DomainValidationFailure);
}

#[test]
fn planning_rejects_zero_order_work_item() {
    let contract = contract_for_stage(StageId::Planning);
    let json = json!({
        "problem_framing": "Valid framing.",
        "assumptions_or_open_questions": [],
        "proposed_work": [
            { "order": 0, "summary": "Bad order", "details": "" }
        ],
        "readiness": { "ready": true, "risks": [] }
    });
    let err = contract.evaluate(&json).unwrap_err();
    assert!(matches!(err, ContractError::DomainValidation { .. }));
}

#[test]
fn execution_rejects_empty_change_summary() {
    let contract = contract_for_stage(StageId::Implementation);
    let json = json!({
        "change_summary": "",
        "steps": [{ "order": 1, "description": "step", "status": "completed" }],
        "validation_evidence": [],
        "outstanding_risks": []
    });
    let err = contract.evaluate(&json).unwrap_err();
    assert!(matches!(err, ContractError::DomainValidation { .. }));
}

#[test]
fn execution_rejects_empty_steps() {
    let contract = contract_for_stage(StageId::Implementation);
    let json = json!({
        "change_summary": "Did things.",
        "steps": [],
        "validation_evidence": [],
        "outstanding_risks": []
    });
    let err = contract.evaluate(&json).unwrap_err();
    assert!(matches!(err, ContractError::DomainValidation { .. }));
}

#[test]
fn validation_rejects_non_passing_without_follow_up() {
    let contract = contract_for_stage(StageId::Review);
    let json = json!({
        "outcome": "request_changes",
        "evidence": ["Some evidence."],
        "findings_or_gaps": ["A gap."],
        "follow_up_or_amendments": []
    });
    let err = contract.evaluate(&json).unwrap_err();
    assert!(matches!(err, ContractError::DomainValidation { .. }));
}

// ── Successful evaluation ───────────────────────────────────────────────────

#[test]
fn planning_contract_evaluates_successfully() {
    let contract = contract_for_stage(StageId::Planning);
    let bundle = contract.evaluate(&valid_planning_json()).unwrap();
    assert!(!bundle.artifact.is_empty());
    assert!(bundle.artifact.contains("# Planning"));
}

#[test]
fn execution_contract_evaluates_successfully() {
    let contract = contract_for_stage(StageId::Implementation);
    let bundle = contract.evaluate(&valid_execution_json()).unwrap();
    assert!(bundle.artifact.contains("# Implementation"));
}

#[test]
fn validation_approved_evaluates_with_no_outcome_failure() {
    let contract = contract_for_stage(StageId::Review);
    let bundle = contract
        .evaluate(&valid_validation_approved_json())
        .unwrap();
    assert!(bundle.artifact.contains("**Approved**"));
}

// ── QA/Review outcome failure mapping ───────────────────────────────────────

#[test]
fn non_passing_review_maps_to_qa_review_outcome_failure() {
    let contract = contract_for_stage(StageId::Review);
    let err = contract
        .evaluate(&valid_validation_rejected_json())
        .unwrap_err();
    assert!(
        matches!(err, ContractError::QaReviewOutcome { .. }),
        "expected QaReviewOutcome, got: {err:?}"
    );
    assert_eq!(err.failure_class(), FailureClass::QaReviewOutcomeFailure);
}

#[test]
fn conditionally_approved_maps_to_qa_review_outcome_failure() {
    let contract = contract_for_stage(StageId::Qa);
    let json = json!({
        "outcome": "conditionally_approved",
        "evidence": ["Mostly good."],
        "findings_or_gaps": ["Minor issue."],
        "follow_up_or_amendments": ["Fix minor issue."]
    });
    let err = contract.evaluate(&json).unwrap_err();
    assert!(matches!(err, ContractError::QaReviewOutcome { .. }));
    assert_eq!(err.failure_class(), FailureClass::QaReviewOutcomeFailure);
}

// ── JSON Schema generation ──────────────────────────────────────────────────

#[test]
fn json_schema_is_generated_for_each_family() {
    for &stage_id in &StageId::ALL {
        let contract = contract_for_stage(stage_id);
        let schema = contract.json_schema();
        // Schema must have a root schema object with definitions.
        assert!(
            schema.schema.metadata.is_some()
                || !schema.definitions.is_empty()
                || schema.schema.object.is_some()
                || schema.schema.subschemas.is_some(),
            "schema for {stage_id} should have content"
        );
    }
}

// ── Conformance scenario IDs ────────────────────────────────────────────────
// These tests verify the scenario IDs referenced in
// tests/conformance/features/stage_contracts.feature

#[test]
fn sc_eval_001_successful_planning_contract_evaluation() {
    let contract = contract_for_stage(StageId::Planning);
    let bundle = contract.evaluate(&valid_planning_json()).unwrap();
    assert!(!bundle.artifact.is_empty());
}

#[test]
fn sc_eval_002_successful_execution_contract_evaluation() {
    let contract = contract_for_stage(StageId::Implementation);
    let bundle = contract.evaluate(&valid_execution_json()).unwrap();
    assert!(!bundle.artifact.is_empty());
}

#[test]
fn sc_eval_003_successful_validation_contract_evaluation() {
    let contract = contract_for_stage(StageId::Review);
    let bundle = contract
        .evaluate(&valid_validation_approved_json())
        .unwrap();
    assert!(!bundle.artifact.is_empty());
}

#[test]
fn sc_eval_004_schema_failure_prevents_semantic_validation() {
    let contract = contract_for_stage(StageId::Planning);
    let err = contract.evaluate(&json!({})).unwrap_err();
    assert!(matches!(err, ContractError::SchemaValidation { .. }));
}

#[test]
fn sc_eval_005_domain_failure_prevents_rendering() {
    let contract = contract_for_stage(StageId::Implementation);
    let json = json!({
        "change_summary": "   ",
        "steps": [],
        "validation_evidence": [],
        "outstanding_risks": []
    });
    let err = contract.evaluate(&json).unwrap_err();
    assert!(matches!(err, ContractError::DomainValidation { .. }));
}

#[test]
fn sc_eval_006_qa_review_outcome_failure() {
    let contract = contract_for_stage(StageId::FinalReview);
    let json = json!({
        "outcome": "rejected",
        "evidence": ["Fails acceptance."],
        "findings_or_gaps": ["Critical gap."],
        "follow_up_or_amendments": ["Rework needed."]
    });
    let err = contract.evaluate(&json).unwrap_err();
    assert!(matches!(err, ContractError::QaReviewOutcome { .. }));
    assert_eq!(err.failure_class(), FailureClass::QaReviewOutcomeFailure);
}

// ── All flows have complete contract coverage ───────────────────────────────

#[test]
fn sc_eval_007_all_presets_have_complete_contract_coverage() {
    for preset in FlowPreset::all() {
        let flow = flow_definition(*preset);
        for &stage_id in flow.stages {
            let _contract = contract_for_stage(stage_id);
            // No panic = contract exists.
        }
    }
}

// ── SC-EVAL-009: Non-passing outcomes are distinct from schema/domain ────────

#[test]
fn sc_eval_009_non_passing_review_not_schema_or_domain_failure() {
    let contract = contract_for_stage(StageId::Review);
    let json = json!({
        "outcome": "request_changes",
        "evidence": ["Some evidence."],
        "findings_or_gaps": ["A gap."],
        "follow_up_or_amendments": ["Fix it."]
    });
    let err = contract.evaluate(&json).unwrap_err();
    assert!(matches!(err, ContractError::QaReviewOutcome { .. }));
    assert_eq!(err.failure_class(), FailureClass::QaReviewOutcomeFailure);
    assert_ne!(err.failure_class(), FailureClass::SchemaValidationFailure);
    assert_ne!(err.failure_class(), FailureClass::DomainValidationFailure);
}

// ── Runtime validation alignment with generated schema ───────────────────────

#[test]
fn schema_validation_rejects_what_generated_schema_marks_required() {
    // Proves that runtime deserialization (evaluate) rejects a payload missing
    // fields that the generated JSON Schema marks as required, ensuring the two
    // validation paths stay aligned.
    use jsonschema::JSONSchema;

    let contract = contract_for_stage(StageId::Planning);
    let schema_value = serde_json::to_value(contract.json_schema()).expect("schema serializes");
    let compiled = JSONSchema::compile(&schema_value).expect("schema compiles");

    let missing_fields = json!({"not_a_real_field": true});

    // Generated schema rejects this payload.
    assert!(
        !compiled.is_valid(&missing_fields),
        "generated schema should reject payload missing required fields"
    );
    // Runtime evaluation also rejects it.
    let err = contract.evaluate(&missing_fields).unwrap_err();
    assert!(
        matches!(err, ContractError::SchemaValidation { .. }),
        "runtime evaluation should also reject: {err:?}"
    );
}

#[test]
fn schema_validation_accepts_what_generated_schema_accepts() {
    use jsonschema::JSONSchema;

    let contract = contract_for_stage(StageId::Planning);
    let schema_value = serde_json::to_value(contract.json_schema()).expect("schema serializes");
    let compiled = JSONSchema::compile(&schema_value).expect("schema compiles");

    let valid = valid_planning_json();

    // Generated schema accepts this payload.
    assert!(
        compiled.is_valid(&valid),
        "generated schema should accept valid payload"
    );
    // Runtime evaluation also accepts it.
    assert!(
        contract.evaluate(&valid).is_ok(),
        "runtime evaluation should also accept valid payload"
    );
}
