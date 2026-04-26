#![forbid(unsafe_code)]

use ralph_burning::contexts::workflow_composition::completion::compute_completion_verdict;
use ralph_burning::contexts::workflow_composition::panel_contracts::{
    panel_json_schema, AmendmentClassification, CompletionAggregatePayload, CompletionVerdict,
    CompletionVotePayload, FinalReviewAggregatePayload, FinalReviewArbiterPayload,
    FinalReviewCanonicalAmendment, FinalReviewProposal, FinalReviewProposalPayload,
    FinalReviewVote, FinalReviewVoteDecision, FinalReviewVotePayload, PromptRefinementPayload,
    PromptReviewDecision, PromptReviewPrimaryPayload, PromptValidationPayload, RecordKind,
    RecordProducer,
};
use ralph_burning::contexts::workflow_composition::review_classification::ReviewFindingClass;
use ralph_burning::shared::domain::StageId;
use ralph_burning::test_support::logging::log_capture;

fn parsed_final_review_amendment(amendment: serde_json::Value) -> FinalReviewProposal {
    let payload: FinalReviewProposalPayload = serde_json::from_value(serde_json::json!({
        "summary": "review summary",
        "amendments": [amendment]
    }))
    .expect("final-review payload should parse");
    payload.amendments[0].clone()
}

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
        improvements: vec![
            "Added acceptance criteria.".to_string(),
            "Removed ambiguity.".to_string(),
        ],
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
        effective_min_completers: 2,
        exhausted_count: 0,
        probe_exhausted_count: 0,
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
            mapped_to_bead_id: None,
            covered_by_bead_id: None,
            classification: Default::default(),
            proposed_title: None,
            proposed_scope: None,
            proposed_bead_summary: None,
            severity: None,
        }],
    };
    let json = serde_json::to_string(&payload).expect("serializes");
    let restored: FinalReviewProposalPayload = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(payload, restored);
}

#[test]
fn final_review_amendment_fix_current_bead_round_trips() {
    let amendment = parsed_final_review_amendment(serde_json::json!({
        "body": "Fix the issue.",
        "classification": "fix_current_bead"
    }));
    assert_eq!(amendment.classification, ReviewFindingClass::FixCurrentBead);
}

#[test]
fn final_review_amendment_covered_by_existing_bead_round_trips() {
    let amendment = parsed_final_review_amendment(serde_json::json!({
        "body": "Covered elsewhere.",
        "classification": "covered_by_existing_bead",
        "covered_by_bead_id": "9ni.8.5"
    }));
    assert_eq!(
        amendment.classification,
        ReviewFindingClass::CoveredByExistingBead
    );
    assert_eq!(amendment.covered_by_bead_id.as_deref(), Some("9ni.8.5"));
}

#[test]
fn final_review_amendment_covered_by_existing_bead_without_id_falls_back_and_warns() {
    let capture = log_capture();
    let amendment = capture.in_scope(|| {
        parsed_final_review_amendment(serde_json::json!({
            "body": "Missing target.",
            "classification": "covered_by_existing_bead"
        }))
    });

    assert_eq!(amendment.classification, ReviewFindingClass::FixCurrentBead);
    capture.assert_event_has_fields(&[("level", "WARN")]);
}

#[test]
fn final_review_amendment_propose_new_bead_round_trips() {
    let amendment = parsed_final_review_amendment(serde_json::json!({
        "body": "Missing substantial work.",
        "classification": "propose_new_bead",
        "proposed_bead_summary": "Add substantial follow-up"
    }));
    assert_eq!(amendment.classification, ReviewFindingClass::ProposeNewBead);
    assert_eq!(
        amendment.proposed_bead_summary.as_deref(),
        Some("Add substantial follow-up")
    );
}

#[test]
fn final_review_amendment_propose_new_bead_without_summary_falls_back_and_warns() {
    let capture = log_capture();
    let amendment = capture.in_scope(|| {
        parsed_final_review_amendment(serde_json::json!({
            "body": "Missing substantial work.",
            "classification": "propose_new_bead"
        }))
    });

    assert_eq!(amendment.classification, ReviewFindingClass::FixCurrentBead);
    capture.assert_event_has_fields(&[("level", "WARN")]);
}

#[test]
fn final_review_amendment_informational_only_round_trips() {
    let amendment = parsed_final_review_amendment(serde_json::json!({
        "body": "No action.",
        "classification": "informational_only"
    }));
    assert_eq!(
        amendment.classification,
        ReviewFindingClass::InformationalOnly
    );
    assert!(amendment.classification.triggers_restart());
}

#[test]
fn legacy_final_review_amendment_defaults_to_fix_current_bead() {
    let amendment = parsed_final_review_amendment(serde_json::json!({
        "body": "Legacy amendment."
    }));
    assert_eq!(amendment.classification, ReviewFindingClass::FixCurrentBead);
}

#[test]
fn legacy_kebab_case_final_review_proposal_classifications_parse() {
    let fix_now = parsed_final_review_amendment(serde_json::json!({
        "body": "Legacy fix-now amendment.",
        "classification": "fix-now"
    }));
    assert_eq!(fix_now.classification, ReviewFindingClass::FixCurrentBead);

    let planned_elsewhere = parsed_final_review_amendment(serde_json::json!({
        "body": "Legacy planned-elsewhere amendment.",
        "classification": "planned-elsewhere",
        "mapped_to_bead_id": "9ni.8.5"
    }));
    assert_eq!(
        planned_elsewhere.classification,
        ReviewFindingClass::CoveredByExistingBead
    );
    assert_eq!(
        planned_elsewhere.covered_by_bead_id.as_deref(),
        Some("9ni.8.5")
    );

    let propose_new_bead = parsed_final_review_amendment(serde_json::json!({
        "body": "Legacy proposed bead amendment.",
        "classification": "propose-new-bead",
        "proposed_bead_summary": "Add legacy follow-up"
    }));
    assert_eq!(
        propose_new_bead.classification,
        ReviewFindingClass::ProposeNewBead
    );
}

#[test]
fn final_review_vote_payload_round_trips() {
    let payload = FinalReviewVotePayload {
        summary: "Reviewer votes captured.".to_string(),
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
        mapped_to_bead_id: None,
        covered_by_bead_id: None,
        classification: AmendmentClassification::FixCurrentBead,
        rationale: None,
        proposed_title: None,
        proposed_scope: None,
        proposed_bead_summary: None,
        severity: None,
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
        exhausted_count: 0,
        probe_exhausted_count: 0,
        effective_min_reviewers: 2,
    };
    let json = serde_json::to_string(&payload).expect("serializes");
    let restored: FinalReviewAggregatePayload = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(payload, restored);
}

#[test]
fn final_review_aggregate_covered_by_existing_bead_round_trips_classification() {
    let amendment = FinalReviewCanonicalAmendment {
        amendment_id: "fr-1-deadbeef".to_string(),
        normalized_body: "Tighten the final wording.".to_string(),
        sources: vec![],
        mapped_to_bead_id: Some("other-bead-42".to_string()),
        covered_by_bead_id: Some("other-bead-42".to_string()),
        classification: AmendmentClassification::CoveredByExistingBead,
        rationale: None,
        proposed_title: None,
        proposed_scope: None,
        proposed_bead_summary: None,
        severity: None,
    };
    let payload = FinalReviewAggregatePayload {
        restart_required: true,
        force_completed: false,
        total_reviewers: 2,
        total_proposed_amendments: 1,
        unique_amendment_count: 1,
        accepted_amendment_ids: vec!["fr-1-deadbeef".to_string()],
        rejected_amendment_ids: vec![],
        disputed_amendment_ids: vec![],
        amendments: vec![amendment.clone()],
        final_accepted_amendments: vec![amendment],
        final_review_restart_count: 1,
        max_restarts: 2,
        summary: "Final review accepted 1 amendment(s); restart required.".to_string(),
        exhausted_count: 0,
        probe_exhausted_count: 0,
        effective_min_reviewers: 2,
    };

    let json = serde_json::to_string(&payload).expect("serializes");
    let restored: FinalReviewAggregatePayload = serde_json::from_str(&json).expect("deserializes");
    assert_eq!(payload, restored);
    assert!(restored.restart_required);
    assert_eq!(
        restored.final_accepted_amendments[0].classification,
        AmendmentClassification::CoveredByExistingBead
    );
    assert_eq!(
        restored.final_accepted_amendments[0]
            .mapped_to_bead_id
            .as_deref(),
        Some("other-bead-42"),
        "mapped_to_bead_id must survive serialization round-trip"
    );
}

#[test]
fn legacy_kebab_case_final_review_aggregate_classifications_parse() {
    let payload: FinalReviewAggregatePayload = serde_json::from_value(serde_json::json!({
        "restart_required": true,
        "force_completed": false,
        "total_reviewers": 1,
        "total_proposed_amendments": 3,
        "unique_amendment_count": 3,
        "accepted_amendment_ids": ["a-fix", "a-pe", "a-new"],
        "rejected_amendment_ids": [],
        "disputed_amendment_ids": [],
        "amendments": [],
        "final_accepted_amendments": [
            {
                "amendment_id": "a-fix",
                "normalized_body": "Legacy fix-now",
                "sources": [],
                "classification": "fix-now"
            },
            {
                "amendment_id": "a-pe",
                "normalized_body": "Legacy planned elsewhere",
                "sources": [],
                "mapped_to_bead_id": "9ni.8.5",
                "classification": "planned-elsewhere"
            },
            {
                "amendment_id": "a-new",
                "normalized_body": "Legacy proposed bead",
                "sources": [],
                "classification": "propose-new-bead",
                "proposed_bead_summary": "Add legacy follow-up"
            }
        ],
        "final_review_restart_count": 1,
        "max_restarts": 3,
        "summary": "Legacy aggregate",
        "exhausted_count": 0,
        "probe_exhausted_count": 0,
        "effective_min_reviewers": 1
    }))
    .expect("legacy aggregate classifications should parse");

    assert_eq!(
        payload.final_accepted_amendments[0].classification,
        AmendmentClassification::FixCurrentBead
    );
    assert_eq!(
        payload.final_accepted_amendments[1].classification,
        AmendmentClassification::CoveredByExistingBead
    );
    assert_eq!(
        payload.final_accepted_amendments[2].classification,
        AmendmentClassification::ProposeNewBead
    );
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
        requested_backend_family: "claude".to_string(),
        requested_model_id: "model-1".to_string(),
        actual_backend_family: "claude".to_string(),
        actual_model_id: "model-1".to_string(),
    };
    let json = serde_json::to_value(&agent).unwrap();
    assert_eq!(json["type"], "agent");
    assert_eq!(json["requested_backend_family"], "claude");
    assert_eq!(json["requested_model_id"], "model-1");
    assert_eq!(json["actual_backend_family"], "claude");
    assert_eq!(json["actual_model_id"], "model-1");

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
            requested_backend_family: "claude".to_string(),
            requested_model_id: "model-1".to_string(),
            actual_backend_family: "claude".to_string(),
            actual_model_id: "model-1".to_string(),
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

// ── Adapter-reported fields serialization ────────────────────────────────────

#[test]
fn record_producer_agent_with_adapter_reported_values_round_trips() {
    let with_different_actual = RecordProducer::Agent {
        requested_backend_family: "claude".to_string(),
        requested_model_id: "claude-opus-4-7".to_string(),
        actual_backend_family: "openrouter".to_string(),
        actual_model_id: "openai/gpt-4.1".to_string(),
    };

    // Serialize and verify actual values are present
    let json = serde_json::to_value(&with_different_actual).unwrap();
    assert_eq!(json["actual_backend_family"], "openrouter");
    assert_eq!(json["actual_model_id"], "openai/gpt-4.1");
    assert_eq!(json["requested_backend_family"], "claude");
    assert_eq!(json["requested_model_id"], "claude-opus-4-7");

    // Round-trip
    let json_str = serde_json::to_string(&with_different_actual).unwrap();
    let restored: RecordProducer = serde_json::from_str(&json_str).unwrap();
    assert_eq!(with_different_actual, restored);
}

#[test]
fn record_producer_agent_actual_fields_always_present_in_json() {
    let producer = RecordProducer::Agent {
        requested_backend_family: "claude".to_string(),
        requested_model_id: "claude-opus-4-7".to_string(),
        actual_backend_family: "claude".to_string(),
        actual_model_id: "claude-opus-4-7".to_string(),
    };

    let json = serde_json::to_value(&producer).unwrap();
    assert_eq!(
        json["actual_backend_family"], "claude",
        "actual_backend_family should always be present"
    );
    assert_eq!(
        json["actual_model_id"], "claude-opus-4-7",
        "actual_model_id should always be present"
    );
}

#[test]
fn record_producer_agent_deserializes_from_old_field_names() {
    // Simulates old JSON with the legacy field names
    let old_json = r#"{"type":"agent","backend_family":"claude","model_id":"claude-opus-4-7"}"#;
    let restored: RecordProducer = serde_json::from_str(old_json).unwrap();
    assert_eq!(
        restored,
        RecordProducer::Agent {
            requested_backend_family: "claude".to_string(),
            requested_model_id: "claude-opus-4-7".to_string(),
            actual_backend_family: "claude".to_string(),
            actual_model_id: "claude-opus-4-7".to_string(),
        }
    );

    // Old JSON with adapter_reported fields maps to actual fields
    let old_json_with_adapter = r#"{"type":"agent","backend_family":"claude","model_id":"claude-opus-4-7","adapter_reported_backend_family":"openrouter","adapter_reported_model_id":"openai/gpt-4.1"}"#;
    let restored: RecordProducer = serde_json::from_str(old_json_with_adapter).unwrap();
    assert_eq!(
        restored,
        RecordProducer::Agent {
            requested_backend_family: "claude".to_string(),
            requested_model_id: "claude-opus-4-7".to_string(),
            actual_backend_family: "openrouter".to_string(),
            actual_model_id: "openai/gpt-4.1".to_string(),
        }
    );
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
fn record_producer_display_agent_matching() {
    let producer = RecordProducer::Agent {
        requested_backend_family: "claude".to_string(),
        requested_model_id: "model-1".to_string(),
        actual_backend_family: "claude".to_string(),
        actual_model_id: "model-1".to_string(),
    };
    assert_eq!(producer.to_string(), "agent:claude/model-1");
}

#[test]
fn record_producer_display_agent_mismatched() {
    let producer = RecordProducer::Agent {
        requested_backend_family: "claude".to_string(),
        requested_model_id: "claude-opus-4-7".to_string(),
        actual_backend_family: "openrouter".to_string(),
        actual_model_id: "openai/gpt-4.1".to_string(),
    };
    assert_eq!(
        producer.to_string(),
        "agent:openrouter/openai/gpt-4.1 (requested claude/claude-opus-4-7)"
    );
}

#[test]
fn record_producer_display_agent_legacy_empty_actual_falls_back_to_requested() {
    let producer = RecordProducer::Agent {
        requested_backend_family: "claude".to_string(),
        requested_model_id: "claude-opus-4-7".to_string(),
        actual_backend_family: String::new(),
        actual_model_id: String::new(),
    };
    assert_eq!(producer.to_string(), "agent:claude/claude-opus-4-7");
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
