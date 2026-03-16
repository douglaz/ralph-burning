#![forbid(unsafe_code)]

use ralph_burning::contexts::workflow_composition::completion::compute_completion_verdict;
use ralph_burning::contexts::workflow_composition::panel_contracts::{
    CompletionAggregatePayload, CompletionVerdict, CompletionVotePayload,
    FinalReviewAggregatePayload, FinalReviewArbiterPayload, FinalReviewCanonicalAmendment,
    FinalReviewProposal, FinalReviewProposalPayload, FinalReviewVote,
    FinalReviewVoteDecision, FinalReviewVotePayload, PromptReviewDecision,
    PromptReviewPrimaryPayload, PromptRefinementPayload, PromptValidationPayload, RecordKind,
    RecordProducer, panel_json_schema,
};
use ralph_burning::shared::domain::StageId;

// ── Completion consensus math ────────────────────────────────────────────────

#[test]
fn completion_verdict_all_agree_meets_min_and_threshold() {
    // 3/3 = 1.0 >= 0.5, and 3 >= 2
    let verdict = compute_completion_verdict(3, 3, 2, 0.5);
    assert_eq!(verdict, CompletionVerdict::Complete);
}

#[test]
fn completion_verdict_below_min_completers() {
    // 1 complete vote but min_completers is 2
    let verdict = compute_completion_verdict(1, 3, 2, 0.5);
    assert_eq!(verdict, CompletionVerdict::ContinueWork);
}

#[test]
fn completion_verdict_below_consensus_threshold() {
    // 2/4 = 0.5 < 0.75 threshold, even though 2 >= 2 min_completers
    let verdict = compute_completion_verdict(2, 4, 2, 0.75);
    assert_eq!(verdict, CompletionVerdict::ContinueWork);
}

#[test]
fn completion_verdict_zero_voters_returns_continue() {
    // No voters at all; guard against division by zero
    let verdict = compute_completion_verdict(0, 0, 1, 0.5);
    assert_eq!(verdict, CompletionVerdict::ContinueWork);
}

#[test]
fn completion_verdict_majority_meets_threshold() {
    // 2/3 ≈ 0.667 >= 0.5, and 2 >= 2
    let verdict = compute_completion_verdict(2, 3, 2, 0.5);
    assert_eq!(verdict, CompletionVerdict::Complete);
}

#[test]
fn completion_verdict_exact_threshold_boundary() {
    // 3/4 = 0.75 >= 0.75 (exact boundary), and 3 >= 3
    let verdict = compute_completion_verdict(3, 4, 3, 0.75);
    assert_eq!(verdict, CompletionVerdict::Complete);
}

// ── Panel contract serialization round-trips ─────────────────────────────────

#[test]
fn prompt_refinement_payload_round_trips() {
    let payload = PromptRefinementPayload {
        refined_prompt: "Rewritten prompt text.".to_string(),
        refinement_summary: "Clarified requirements.".to_string(),
        improvements: vec!["Added acceptance criteria.".to_string(), "Removed ambiguity.".to_string()],
    };
    let json = serde_json::to_string(&payload).expect("serializes");
    let restored: PromptRefinementPayload = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(payload, restored);
}

#[test]
fn prompt_validation_payload_round_trips() {
    let payload = PromptValidationPayload {
        accepted: true,
        evidence: vec!["All criteria met.".to_string()],
        concerns: vec![],
    };
    let json = serde_json::to_string(&payload).expect("serializes");
    let restored: PromptValidationPayload = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(payload, restored);
}

#[test]
fn completion_vote_payload_round_trips() {
    let payload = CompletionVotePayload {
        vote_complete: false,
        evidence: vec!["Tests still failing.".to_string()],
        remaining_work: vec!["Fix integration tests.".to_string()],
    };
    let json = serde_json::to_string(&payload).expect("serializes");
    let restored: CompletionVotePayload = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(payload, restored);
}

#[test]
fn completion_aggregate_payload_round_trips() {
    // Note: CompletionAggregatePayload contains f64, so uses PartialEq (not Eq)
    let payload = CompletionAggregatePayload {
        verdict: CompletionVerdict::Complete,
        complete_votes: 3,
        continue_votes: 0,
        total_voters: 3,
        consensus_threshold: 0.5,
        min_completers: 2,
        executed_voters: vec!["claude/claude-3-5-sonnet".to_string()],
    };
    let json = serde_json::to_string(&payload).expect("serializes");
    let restored: CompletionAggregatePayload = serde_json::from_str(&json).expect("deserializes");
    assert!(payload == restored);
}

#[test]
fn prompt_review_primary_payload_round_trips() {
    let payload = PromptReviewPrimaryPayload {
        decision: PromptReviewDecision::Accepted,
        refined_prompt: "Final prompt.".to_string(),
        executed_reviewers: 2,
        accept_count: 2,
        reject_count: 0,
        refinement_summary: "Minor edits applied.".to_string(),
    };
    let json = serde_json::to_string(&payload).expect("serializes");
    let restored: PromptReviewPrimaryPayload = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(payload, restored);
}

#[test]
fn final_review_proposal_payload_round_trips() {
    let payload = FinalReviewProposalPayload {
        summary: "Found one final amendment.".to_string(),
        amendments: vec![FinalReviewProposal {
            body: "Tighten the final wording.".to_string(),
            rationale: Some("Clarifies the edge case.".to_string()),
        }],
    };
    let json = serde_json::to_string(&payload).expect("serializes");
    let restored: FinalReviewProposalPayload = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(payload, restored);
}

#[test]
fn final_review_vote_payload_round_trips() {
    let payload = FinalReviewVotePayload {
        summary: "Planner positions captured.".to_string(),
        votes: vec![FinalReviewVote {
            amendment_id: "fr-1-deadbeef".to_string(),
            decision: FinalReviewVoteDecision::Accept,
            rationale: "Necessary for correctness.".to_string(),
        }],
    };
    let json = serde_json::to_string(&payload).expect("serializes");
    let restored: FinalReviewVotePayload = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(payload, restored);
}

#[test]
fn final_review_arbiter_payload_round_trips() {
    let payload = FinalReviewArbiterPayload {
        summary: "Resolved the disputed amendment.".to_string(),
        rulings: vec![ralph_burning::contexts::workflow_composition::panel_contracts::FinalReviewArbiterRuling {
            amendment_id: "fr-1-deadbeef".to_string(),
            decision: FinalReviewVoteDecision::Reject,
            rationale: "Not worth the restart.".to_string(),
        }],
    };
    let json = serde_json::to_string(&payload).expect("serializes");
    let restored: FinalReviewArbiterPayload = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(payload, restored);
}

#[test]
fn final_review_aggregate_payload_round_trips() {
    let amendment = FinalReviewCanonicalAmendment {
        amendment_id: "fr-1-deadbeef".to_string(),
        normalized_body: "Tighten the final wording.".to_string(),
        sources: vec![],
    };
    let payload = FinalReviewAggregatePayload {
        restart_required: true,
        force_completed: false,
        total_reviewers: 2,
        total_proposed_amendments: 2,
        unique_amendment_count: 1,
        accepted_amendment_ids: vec!["fr-1-deadbeef".to_string()],
        rejected_amendment_ids: vec![],
        disputed_amendment_ids: vec![],
        amendments: vec![amendment.clone()],
        final_accepted_amendments: vec![amendment],
        final_review_restart_count: 1,
        max_restarts: 2,
        summary: "Restart required.".to_string(),
    };
    let json = serde_json::to_string(&payload).expect("serializes");
    let restored: FinalReviewAggregatePayload = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(payload, restored);
}

#[test]
fn record_kind_serializes_to_snake_case() {
    assert_eq!(
        serde_json::to_value(RecordKind::StagePrimary).unwrap(),
        serde_json::Value::String("stage_primary".to_string())
    );
    assert_eq!(
        serde_json::to_value(RecordKind::StageSupporting).unwrap(),
        serde_json::Value::String("stage_supporting".to_string())
    );
    assert_eq!(
        serde_json::to_value(RecordKind::StageAggregate).unwrap(),
        serde_json::Value::String("stage_aggregate".to_string())
    );
}

#[test]
fn record_kind_deserializes_from_snake_case() {
    let primary: RecordKind = serde_json::from_str(r#""stage_primary""#).unwrap();
    assert_eq!(primary, RecordKind::StagePrimary);

    let supporting: RecordKind = serde_json::from_str(r#""stage_supporting""#).unwrap();
    assert_eq!(supporting, RecordKind::StageSupporting);

    let aggregate: RecordKind = serde_json::from_str(r#""stage_aggregate""#).unwrap();
    assert_eq!(aggregate, RecordKind::StageAggregate);
}

#[test]
fn record_producer_serializes_with_tagged_union_format() {
    let agent = RecordProducer::Agent {
        backend_family: "claude".to_string(),
        model_id: "model-1".to_string(),
    };
    let json = serde_json::to_value(&agent).unwrap();
    assert_eq!(json["type"], "agent");
    assert_eq!(json["backend_family"], "claude");
    assert_eq!(json["model_id"], "model-1");

    let system = RecordProducer::System {
        component: "completion_aggregator".to_string(),
    };
    let json = serde_json::to_value(&system).unwrap();
    assert_eq!(json["type"], "system");
    assert_eq!(json["component"], "completion_aggregator");

    let local = RecordProducer::LocalValidation {
        command: "cargo test".to_string(),
    };
    let json = serde_json::to_value(&local).unwrap();
    assert_eq!(json["type"], "local_validation");
    assert_eq!(json["command"], "cargo test");
}

#[test]
fn record_producer_round_trips() {
    let producers = vec![
        RecordProducer::Agent {
            backend_family: "claude".to_string(),
            model_id: "model-1".to_string(),
        },
        RecordProducer::System {
            component: "completion_aggregator".to_string(),
        },
        RecordProducer::LocalValidation {
            command: "cargo test".to_string(),
        },
    ];

    for producer in producers {
        let json = serde_json::to_string(&producer).expect("serializes");
        let restored: RecordProducer = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(producer, restored);
    }
}

// ── Panel JSON schema generation ─────────────────────────────────────────────

#[test]
fn panel_json_schema_prompt_review_refiner_returns_non_empty_schema_with_properties() {
    let schema = panel_json_schema(StageId::PromptReview, "refiner");
    assert!(!schema.is_null(), "schema must not be null");
    assert!(
        !schema.as_object().map_or(true, |o| o.is_empty()),
        "schema must not be an empty object"
    );
    // JSON Schema must contain a "properties" key somewhere
    let schema_str = serde_json::to_string(&schema).unwrap();
    assert!(
        schema_str.contains("properties"),
        "schema for refiner must contain 'properties'"
    );
}

#[test]
fn panel_json_schema_prompt_review_validator_returns_non_empty_schema() {
    let schema = panel_json_schema(StageId::PromptReview, "validator");
    assert!(!schema.is_null());
    assert!(
        !schema.as_object().map_or(true, |o| o.is_empty()),
        "schema must not be an empty object"
    );
    let schema_str = serde_json::to_string(&schema).unwrap();
    assert!(
        schema_str.contains("properties"),
        "schema for validator must contain 'properties'"
    );
}

#[test]
fn panel_json_schema_completion_panel_completer_returns_non_empty_schema() {
    let schema = panel_json_schema(StageId::CompletionPanel, "completer");
    assert!(!schema.is_null());
    assert!(
        !schema.as_object().map_or(true, |o| o.is_empty()),
        "schema must not be an empty object"
    );
    let schema_str = serde_json::to_string(&schema).unwrap();
    assert!(
        schema_str.contains("properties"),
        "schema for completer must contain 'properties'"
    );
}

#[test]
fn panel_json_schema_final_review_roles_return_non_empty_schema() {
    for role in ["reviewer", "voter", "arbiter"] {
        let schema = panel_json_schema(StageId::FinalReview, role);
        assert!(!schema.is_null(), "schema must not be null for role {role}");
        assert!(
            !schema.as_object().map_or(true, |o| o.is_empty()),
            "schema must not be empty for role {role}"
        );
        let schema_str = serde_json::to_string(&schema).unwrap();
        assert!(
            schema_str.contains("properties"),
            "schema for role {role} must contain 'properties'"
        );
    }
}

#[test]
fn panel_json_schema_unknown_role_returns_empty_object() {
    let schema = panel_json_schema(StageId::Planning, "unknown");
    assert!(
        schema.as_object().map_or(false, |o| o.is_empty()),
        "unknown stage/role combination must return empty object, got: {schema}"
    );
}

// ── RecordKind Display ───────────────────────────────────────────────────────

#[test]
fn record_kind_display_stage_primary() {
    assert_eq!(RecordKind::StagePrimary.to_string(), "primary");
}

#[test]
fn record_kind_display_stage_supporting() {
    assert_eq!(RecordKind::StageSupporting.to_string(), "supporting");
}

#[test]
fn record_kind_display_stage_aggregate() {
    assert_eq!(RecordKind::StageAggregate.to_string(), "aggregate");
}

// ── RecordProducer Display ───────────────────────────────────────────────────

#[test]
fn record_producer_display_agent() {
    let producer = RecordProducer::Agent {
        backend_family: "claude".to_string(),
        model_id: "model-1".to_string(),
    };
    assert_eq!(producer.to_string(), "agent:claude/model-1");
}

#[test]
fn record_producer_display_system() {
    let producer = RecordProducer::System {
        component: "completion_aggregator".to_string(),
    };
    assert_eq!(producer.to_string(), "system:completion_aggregator");
}

#[test]
fn record_producer_display_local_validation() {
    let producer = RecordProducer::LocalValidation {
        command: "cargo test".to_string(),
    };
    assert_eq!(producer.to_string(), "local:cargo test");
}

// ── CompletionVerdict Display ────────────────────────────────────────────────

#[test]
fn completion_verdict_display_complete() {
    assert_eq!(CompletionVerdict::Complete.to_string(), "Complete");
}

#[test]
fn completion_verdict_display_continue_work() {
    assert_eq!(CompletionVerdict::ContinueWork.to_string(), "Continue Work");
}

// ── PromptReviewDecision Display ─────────────────────────────────────────────

#[test]
fn prompt_review_decision_display_accepted() {
    assert_eq!(PromptReviewDecision::Accepted.to_string(), "Accepted");
}

#[test]
fn prompt_review_decision_display_rejected() {
    assert_eq!(PromptReviewDecision::Rejected.to_string(), "Rejected");
}
