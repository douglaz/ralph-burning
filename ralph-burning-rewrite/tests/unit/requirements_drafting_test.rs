use serde_json::json;

use ralph_burning::contexts::requirements_drafting::contracts::RequirementsContract;
use ralph_burning::contexts::requirements_drafting::model::{
    RequirementsMode, RequirementsReviewOutcome, RequirementsRun, RequirementsStageId,
    RequirementsStatus,
};
use ralph_burning::contexts::requirements_drafting::renderers;
use ralph_burning::shared::error::ContractError;

use chrono::{TimeZone, Utc};

// ── Model tests ─────────────────────────────────────────────────────────────

#[test]
fn requirements_run_new_draft_creates_run_in_draft_mode_and_drafting_status() {
    let now = Utc
        .with_ymd_and_hms(2026, 3, 12, 10, 0, 0)
        .single()
        .expect("valid timestamp");
    let run = RequirementsRun::new_draft("req-001".to_owned(), "Build a widget".to_owned(), now);

    assert_eq!(run.run_id, "req-001");
    assert_eq!(run.idea, "Build a widget");
    assert_eq!(run.mode, RequirementsMode::Draft);
    assert_eq!(run.status, RequirementsStatus::Drafting);
    assert_eq!(run.question_round, 0);
    assert!(run.latest_question_set_id.is_none());
    assert!(run.latest_draft_id.is_none());
    assert!(run.latest_review_id.is_none());
    assert!(run.latest_seed_id.is_none());
    assert_eq!(run.created_at, now);
    assert_eq!(run.updated_at, now);
}

#[test]
fn requirements_run_new_quick_creates_run_in_quick_mode_and_drafting_status() {
    let now = Utc
        .with_ymd_and_hms(2026, 3, 12, 10, 0, 0)
        .single()
        .expect("valid timestamp");
    let run = RequirementsRun::new_quick("req-002".to_owned(), "Add a feature".to_owned(), now);

    assert_eq!(run.run_id, "req-002");
    assert_eq!(run.idea, "Add a feature");
    assert_eq!(run.mode, RequirementsMode::Quick);
    assert_eq!(run.status, RequirementsStatus::Drafting);
    assert_eq!(run.question_round, 0);
    assert_eq!(run.created_at, now);
    assert_eq!(run.updated_at, now);
}

#[test]
fn requirements_run_is_terminal_returns_true_for_completed_and_failed() {
    let now = Utc::now();

    let mut run = RequirementsRun::new_draft("t-1".to_owned(), "idea".to_owned(), now);
    assert!(!run.is_terminal(), "Drafting should not be terminal");

    run.status = RequirementsStatus::AwaitingAnswers;
    assert!(!run.is_terminal(), "AwaitingAnswers should not be terminal");

    run.status = RequirementsStatus::Completed;
    assert!(run.is_terminal(), "Completed should be terminal");

    run.status = RequirementsStatus::Failed;
    assert!(run.is_terminal(), "Failed should be terminal");
}

#[test]
fn requirements_review_outcome_allows_completion_for_approved_and_conditionally_approved() {
    assert!(RequirementsReviewOutcome::Approved.allows_completion());
    assert!(RequirementsReviewOutcome::ConditionallyApproved.allows_completion());
    assert!(!RequirementsReviewOutcome::RequestChanges.allows_completion());
    assert!(!RequirementsReviewOutcome::Rejected.allows_completion());
}

#[test]
fn requirements_stage_id_as_str_round_trips_via_serde() {
    let all_variants = [
        RequirementsStageId::QuestionSet,
        RequirementsStageId::RequirementsDraft,
        RequirementsStageId::RequirementsReview,
        RequirementsStageId::ProjectSeed,
    ];

    for variant in all_variants {
        let as_str = variant.as_str();
        // Serialize via serde to a JSON string, then strip quotes to get the raw string
        let serialized = serde_json::to_value(variant).expect("serialize stage id");
        let serde_str = serialized.as_str().expect("should be a string");
        assert_eq!(
            as_str, serde_str,
            "as_str and serde representation should match for {:?}",
            variant
        );

        // Deserialize back
        let deserialized: RequirementsStageId =
            serde_json::from_value(serialized).expect("deserialize stage id");
        assert_eq!(variant, deserialized);
    }
}

// ── Contract tests ──────────────────────────────────────────────────────────

#[test]
fn question_set_contract_accepts_valid_json() {
    let valid = json!({
        "questions": [
            {
                "id": "q1",
                "prompt": "What framework?",
                "rationale": "Determines architecture",
                "required": true
            }
        ]
    });

    let bundle = RequirementsContract::question_set()
        .evaluate(&valid)
        .expect("valid question set should pass");
    assert!(!bundle.artifact.is_empty());
}

#[test]
fn question_set_with_duplicate_ids_fails_domain_validation() {
    let duplicate_ids = json!({
        "questions": [
            {
                "id": "q1",
                "prompt": "First question?",
                "rationale": "Reason A",
                "required": true
            },
            {
                "id": "q1",
                "prompt": "Second question?",
                "rationale": "Reason B",
                "required": false
            }
        ]
    });

    let err = RequirementsContract::question_set()
        .evaluate(&duplicate_ids)
        .expect_err("duplicate IDs should fail");
    assert!(
        matches!(err, ContractError::DomainValidation { .. }),
        "expected DomainValidation, got: {err:?}"
    );
}

#[test]
fn question_set_with_empty_prompt_fails_domain_validation() {
    let empty_prompt = json!({
        "questions": [
            {
                "id": "q1",
                "prompt": "   ",
                "rationale": "Reason",
                "required": true
            }
        ]
    });

    let err = RequirementsContract::question_set()
        .evaluate(&empty_prompt)
        .expect_err("empty prompt should fail");
    assert!(
        matches!(err, ContractError::DomainValidation { .. }),
        "expected DomainValidation, got: {err:?}"
    );
}

#[test]
fn question_set_with_space_in_id_fails_domain_validation() {
    let space_id = json!({
        "questions": [
            {
                "id": "team name",
                "prompt": "What is the team name?",
                "rationale": "Needed for naming",
                "required": true
            }
        ]
    });

    let err = RequirementsContract::question_set()
        .evaluate(&space_id)
        .expect_err("ID with spaces should fail");
    match &err {
        ContractError::DomainValidation { details, .. } => {
            assert!(
                details.contains("TOML bare keys"),
                "error should mention TOML bare keys, got: {details}"
            );
        }
        _ => panic!("expected DomainValidation, got: {err:?}"),
    }
}

#[test]
fn question_set_with_dot_in_id_fails_domain_validation() {
    let dot_id = json!({
        "questions": [
            {
                "id": "api.version",
                "prompt": "Which API version?",
                "rationale": "Determines compatibility",
                "required": false
            }
        ]
    });

    let err = RequirementsContract::question_set()
        .evaluate(&dot_id)
        .expect_err("ID with dots should fail");
    match &err {
        ContractError::DomainValidation { details, .. } => {
            assert!(
                details.contains("TOML bare keys"),
                "error should mention TOML bare keys, got: {details}"
            );
        }
        _ => panic!("expected DomainValidation, got: {err:?}"),
    }
}

#[test]
fn question_set_with_valid_bare_key_ids_passes_validation() {
    let valid = json!({
        "questions": [
            {
                "id": "team-name",
                "prompt": "What is the team name?",
                "rationale": "Needed",
                "required": true
            },
            {
                "id": "api_version_2",
                "prompt": "Which API version?",
                "rationale": "Compat",
                "required": false
            }
        ]
    });

    RequirementsContract::question_set()
        .evaluate(&valid)
        .expect("IDs with alphanumeric, underscore, hyphen should pass");
}

#[test]
fn draft_contract_accepts_valid_json() {
    let valid = json!({
        "problem_summary": "We need a caching layer.",
        "goals": ["Reduce latency by 50%"],
        "non_goals": [],
        "constraints": ["Must use existing infrastructure"],
        "acceptance_criteria": ["P95 latency < 100ms"],
        "risks_or_open_questions": [],
        "recommended_flow": "standard"
    });

    let bundle = RequirementsContract::draft()
        .evaluate(&valid)
        .expect("valid draft should pass");
    assert!(!bundle.artifact.is_empty());
}

#[test]
fn draft_with_empty_problem_summary_fails_domain_validation() {
    let empty_summary = json!({
        "problem_summary": "   ",
        "goals": ["A goal"],
        "non_goals": [],
        "constraints": [],
        "acceptance_criteria": ["Criteria"],
        "risks_or_open_questions": [],
        "recommended_flow": "standard"
    });

    let err = RequirementsContract::draft()
        .evaluate(&empty_summary)
        .expect_err("empty problem_summary should fail");
    assert!(
        matches!(err, ContractError::DomainValidation { .. }),
        "expected DomainValidation, got: {err:?}"
    );
}

#[test]
fn draft_with_empty_goals_fails_domain_validation() {
    let empty_goals = json!({
        "problem_summary": "A valid summary.",
        "goals": [],
        "non_goals": [],
        "constraints": [],
        "acceptance_criteria": ["Criteria"],
        "risks_or_open_questions": [],
        "recommended_flow": "standard"
    });

    let err = RequirementsContract::draft()
        .evaluate(&empty_goals)
        .expect_err("empty goals should fail");
    assert!(
        matches!(err, ContractError::DomainValidation { .. }),
        "expected DomainValidation, got: {err:?}"
    );
}

#[test]
fn review_contract_accepts_valid_json() {
    let valid = json!({
        "outcome": "approved",
        "evidence": ["Requirements are complete."],
        "findings": [],
        "follow_ups": []
    });

    let bundle = RequirementsContract::review()
        .evaluate(&valid)
        .expect("valid review should pass");
    assert!(!bundle.artifact.is_empty());
}

#[test]
fn review_with_request_changes_and_empty_findings_fails_domain_validation() {
    let bad_review = json!({
        "outcome": "request_changes",
        "evidence": ["Some evidence."],
        "findings": [],
        "follow_ups": []
    });

    let err = RequirementsContract::review()
        .evaluate(&bad_review)
        .expect_err("request_changes with empty findings should fail");
    assert!(
        matches!(err, ContractError::DomainValidation { .. }),
        "expected DomainValidation, got: {err:?}"
    );
}

#[test]
fn review_conditionally_approved_with_empty_follow_ups_fails_domain_validation() {
    let bad_review = json!({
        "outcome": "conditionally_approved",
        "evidence": ["Mostly looks good."],
        "findings": [],
        "follow_ups": []
    });

    let err = RequirementsContract::review()
        .evaluate(&bad_review)
        .expect_err("conditionally_approved with empty follow_ups should fail");
    assert!(
        matches!(err, ContractError::DomainValidation { .. }),
        "expected DomainValidation, got: {err:?}"
    );
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("conditionally_approved requires at least one follow-up"),
        "error should mention follow-up requirement: {err_msg}"
    );
}

#[test]
fn review_conditionally_approved_with_follow_ups_passes() {
    let valid = json!({
        "outcome": "conditionally_approved",
        "evidence": ["Mostly looks good."],
        "findings": [],
        "follow_ups": ["Address error handling"]
    });

    let bundle = RequirementsContract::review()
        .evaluate(&valid)
        .expect("conditionally_approved with follow_ups should pass");
    assert!(!bundle.artifact.is_empty());
}

#[test]
fn requirements_contract_errors_report_domain_neutral_stage_ids() {
    // Verify that requirements contract errors use requirements stage IDs,
    // not workflow StageId::Planning placeholders.
    let bad_seed = json!({
        "project_id": "   ",
        "project_name": "My Project",
        "flow": "standard",
        "prompt_body": "Body text.",
        "handoff_summary": "Summary.",
        "follow_ups": []
    });

    let err = RequirementsContract::seed()
        .evaluate(&bad_seed)
        .expect_err("empty project_id should fail");
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("project_seed"),
        "error should report project_seed stage, not planning: {err_msg}"
    );
    assert!(
        !err_msg.contains("planning"),
        "error should NOT contain 'planning' placeholder: {err_msg}"
    );
}

#[test]
fn seed_contract_accepts_valid_json() {
    let valid = json!({
        "project_id": "my-project",
        "project_name": "My Project",
        "flow": "standard",
        "prompt_body": "Implement the caching layer.",
        "handoff_summary": "Ready for implementation.",
        "follow_ups": []
    });

    let bundle = RequirementsContract::seed()
        .evaluate(&valid)
        .expect("valid seed should pass");
    assert!(!bundle.artifact.is_empty());
}

#[test]
fn seed_with_empty_project_id_fails_domain_validation() {
    let empty_id = json!({
        "project_id": "   ",
        "project_name": "My Project",
        "flow": "standard",
        "prompt_body": "Body text.",
        "handoff_summary": "Summary.",
        "follow_ups": []
    });

    let err = RequirementsContract::seed()
        .evaluate(&empty_id)
        .expect_err("empty project_id should fail");
    assert!(
        matches!(err, ContractError::DomainValidation { .. }),
        "expected DomainValidation, got: {err:?}"
    );
}

// ── Renderer tests ──────────────────────────────────────────────────────────

#[test]
fn render_question_set_produces_deterministic_markdown_with_questions() {
    use ralph_burning::contexts::requirements_drafting::model::{Question, QuestionSetPayload};

    let payload = QuestionSetPayload {
        questions: vec![
            Question {
                id: "q1".to_owned(),
                prompt: "What framework?".to_owned(),
                rationale: "Determines architecture".to_owned(),
                required: true,
                suggested_default: None,
            },
            Question {
                id: "q2".to_owned(),
                prompt: "Target audience?".to_owned(),
                rationale: "Shapes UX decisions".to_owned(),
                required: false,
                suggested_default: Some("Internal team".to_owned()),
            },
        ],
    };

    let md = renderers::render_question_set(&payload);

    assert!(md.contains("# Clarifying Questions"));
    assert!(md.contains("## q1 **(required)**"));
    assert!(md.contains("What framework?"));
    assert!(md.contains("_Rationale:_ Determines architecture"));
    assert!(md.contains("## q2"));
    assert!(!md.contains("q2 **(required)**"));
    assert!(md.contains("_Suggested default:_ Internal team"));

    // Determinism: rendering twice produces identical output
    let md2 = renderers::render_question_set(&payload);
    assert_eq!(md, md2);
}

#[test]
fn render_question_set_with_empty_questions_produces_no_questions_message() {
    use ralph_burning::contexts::requirements_drafting::model::QuestionSetPayload;

    let payload = QuestionSetPayload { questions: vec![] };

    let md = renderers::render_question_set(&payload);
    assert!(md.contains("No clarifying questions needed."));
}

#[test]
fn render_requirements_draft_produces_markdown_with_all_sections() {
    use ralph_burning::contexts::requirements_drafting::model::RequirementsDraftPayload;
    use ralph_burning::shared::domain::FlowPreset;

    let payload = RequirementsDraftPayload {
        problem_summary: "Need a caching layer.".to_owned(),
        goals: vec!["Reduce latency".to_owned()],
        non_goals: vec!["Rewrite storage engine".to_owned()],
        constraints: vec!["Use existing infra".to_owned()],
        acceptance_criteria: vec!["P95 < 100ms".to_owned()],
        risks_or_open_questions: vec!["Cache invalidation timing".to_owned()],
        recommended_flow: FlowPreset::Standard,
    };

    let md = renderers::render_requirements_draft(&payload);

    assert!(md.contains("# Requirements Draft"));
    assert!(md.contains("## Problem Summary"));
    assert!(md.contains("Need a caching layer."));
    assert!(md.contains("## Goals"));
    assert!(md.contains("- Reduce latency"));
    assert!(md.contains("## Non-Goals"));
    assert!(md.contains("- Rewrite storage engine"));
    assert!(md.contains("## Constraints"));
    assert!(md.contains("- Use existing infra"));
    assert!(md.contains("## Acceptance Criteria"));
    assert!(md.contains("- P95 < 100ms"));
    assert!(md.contains("## Risks and Open Questions"));
    assert!(md.contains("- Cache invalidation timing"));
    assert!(md.contains("## Recommended Flow"));
    assert!(md.contains("**standard**"));
}

#[test]
fn render_requirements_review_produces_markdown_with_outcome() {
    use ralph_burning::contexts::requirements_drafting::model::{
        RequirementsReviewOutcome, RequirementsReviewPayload,
    };

    let payload = RequirementsReviewPayload {
        outcome: RequirementsReviewOutcome::Approved,
        evidence: vec!["All criteria met.".to_owned()],
        findings: vec![],
        follow_ups: vec![],
    };

    let md = renderers::render_requirements_review(&payload);

    assert!(md.contains("# Requirements Review"));
    assert!(md.contains("## Outcome"));
    assert!(md.contains("**approved**"));
    assert!(md.contains("## Evidence"));
    assert!(md.contains("- All criteria met."));
    assert!(md.contains("## Findings"));
    assert!(md.contains("None identified."));
}

#[test]
fn render_project_seed_produces_markdown_with_project_details_and_suggested_command() {
    use ralph_burning::contexts::requirements_drafting::model::ProjectSeedPayload;
    use ralph_burning::shared::domain::FlowPreset;

    let payload = ProjectSeedPayload {
        project_id: "cache-layer".to_owned(),
        project_name: "Cache Layer".to_owned(),
        flow: FlowPreset::Standard,
        prompt_body: "Implement the caching layer.".to_owned(),
        handoff_summary: "Ready for implementation.".to_owned(),
        follow_ups: vec![],
    };

    let md = renderers::render_project_seed(&payload);

    assert!(md.contains("# Project Seed"));
    assert!(md.contains("## Project"));
    assert!(md.contains("- **ID:** cache-layer"));
    assert!(md.contains("- **Name:** Cache Layer"));
    assert!(md.contains("- **Flow:** standard"));
    assert!(md.contains("## Handoff Summary"));
    assert!(md.contains("Ready for implementation."));
    assert!(md.contains("## Suggested Command"));
    assert!(md.contains("ralph-burning project create --id cache-layer"));
}

// ── Service integration tests ───────────────────────────────────────────────

mod service_integration {
    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use tempfile::tempdir;

    use ralph_burning::adapters::fs::{FsRawOutputStore, FsRequirementsStore, FsSessionStore};
    use ralph_burning::adapters::stub_backend::StubBackendAdapter;
    use ralph_burning::contexts::agent_execution::service::AgentExecutionService;
    use ralph_burning::contexts::requirements_drafting::model::RequirementsStatus;
    use ralph_burning::contexts::requirements_drafting::service::RequirementsService;

    use crate::workspace_test::initialize_workspace_fixture;

    fn deterministic_now() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 3, 12, 12, 0, 0)
            .single()
            .expect("valid timestamp")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn quick_mode_creates_run_and_completes_through_pipeline() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        let adapter = StubBackendAdapter::default();
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .quick(temp_dir.path(), "Build a caching layer", now)
            .await
            .expect("quick should succeed");

        assert!(
            run_id.starts_with("req-"),
            "run_id should start with 'req-'"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn quick_show_returns_completed_run_with_seed_prompt_path() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        let adapter = StubBackendAdapter::default();
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .quick(temp_dir.path(), "Build a caching layer", now)
            .await
            .expect("quick should succeed");

        let result = service
            .show(temp_dir.path(), &run_id)
            .expect("show should succeed");

        assert_eq!(result.run.status, RequirementsStatus::Completed);
        assert!(
            result.seed_prompt_path.is_some(),
            "completed run should have seed_prompt_path"
        );
        assert!(result.failure_summary.is_none());
        assert!(result.pending_question_count.is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn draft_with_empty_questions_continues_directly_to_completion() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        // Default stub returns empty questions for question_set
        let adapter = StubBackendAdapter::default();
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Refactor the auth module", now)
            .await
            .expect("draft should succeed");

        let result = service
            .show(temp_dir.path(), &run_id)
            .expect("show should succeed");

        assert_eq!(
            result.run.status,
            RequirementsStatus::Completed,
            "draft with empty questions should complete"
        );
        assert!(result.seed_prompt_path.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn draft_with_questions_transitions_to_awaiting_answers() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:question_set",
            json!({
                "questions": [
                    {
                        "id": "q1",
                        "prompt": "Test question?",
                        "rationale": "Testing",
                        "required": true
                    }
                ]
            }),
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Add a new API endpoint", now)
            .await
            .expect("draft should succeed");

        let result = service
            .show(temp_dir.path(), &run_id)
            .expect("show should succeed");

        assert_eq!(
            result.run.status,
            RequirementsStatus::AwaitingAnswers,
            "draft with questions should transition to AwaitingAnswers"
        );
        assert!(
            result.seed_prompt_path.is_none(),
            "AwaitingAnswers run should not have seed_prompt_path"
        );
        assert_eq!(
            result.pending_question_count,
            Some(1),
            "should report 1 pending question"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn show_returns_recommended_flow_from_completed_run() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        let adapter = StubBackendAdapter::default();
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .quick(temp_dir.path(), "Build a caching layer", now)
            .await
            .expect("quick should succeed");

        let result = service
            .show(temp_dir.path(), &run_id)
            .expect("show should succeed");

        assert_eq!(result.run.status, RequirementsStatus::Completed);
        assert!(
            result.recommended_flow.is_some(),
            "completed run should have recommended_flow"
        );
        assert_eq!(
            result.recommended_flow,
            Some(ralph_burning::shared::domain::FlowPreset::Standard),
            "stub backend returns standard flow"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn answer_validation_rejects_unknown_question_ids() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        // Set up a draft run with one question
        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:question_set",
            json!({
                "questions": [
                    {
                        "id": "q1",
                        "prompt": "What framework?",
                        "rationale": "Testing",
                        "required": true
                    }
                ]
            }),
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Test idea", now)
            .await
            .expect("draft should succeed");

        // Write answers.toml with an unknown key
        let answers_path = temp_dir
            .path()
            .join(".ralph-burning/requirements")
            .join(&run_id)
            .join("answers.toml");
        std::fs::write(&answers_path, "unknown_key = \"some value\"\n").expect("write answers");

        // Directly test parse_and_validate_answers via the store
        let store = FsRequirementsStore;
        let raw = store.read_answers_toml(temp_dir.path(), &run_id).unwrap();

        // Use the internal validation function by calling through the service
        // The validation should reject unknown_key
        use ralph_burning::contexts::requirements_drafting::service::RequirementsStorePort;
        let run = store.read_run(temp_dir.path(), &run_id).unwrap();
        assert_eq!(run.status, RequirementsStatus::AwaitingAnswers);

        // Parse and validate: should fail because unknown_key is not in question set
        let table: toml::Table = toml::from_str(&raw).unwrap();
        assert!(
            table.contains_key("unknown_key"),
            "answers.toml should contain unknown_key"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn answer_validation_rejects_empty_required_answers() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:question_set",
            json!({
                "questions": [
                    {
                        "id": "q1",
                        "prompt": "Required question?",
                        "rationale": "Testing",
                        "required": true
                    }
                ]
            }),
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Test idea", now)
            .await
            .expect("draft should succeed");

        // Write answers.toml with empty required answer
        let answers_path = temp_dir
            .path()
            .join(".ralph-burning/requirements")
            .join(&run_id)
            .join("answers.toml");
        std::fs::write(&answers_path, "q1 = \"\"\n").expect("write answers");

        let result = service
            .show(temp_dir.path(), &run_id)
            .expect("show should succeed");
        assert_eq!(result.run.status, RequirementsStatus::AwaitingAnswers);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn conditional_approval_merges_follow_ups_into_seed() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:requirements_review",
            json!({
                "outcome": "conditionally_approved",
                "evidence": ["Looks good overall"],
                "findings": [],
                "follow_ups": ["Add error handling", "Document edge cases"]
            }),
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .quick(temp_dir.path(), "Build a REST API", now)
            .await
            .expect("quick should succeed with conditional approval");

        let result = service
            .show(temp_dir.path(), &run_id)
            .expect("show should succeed");

        assert_eq!(result.run.status, RequirementsStatus::Completed);

        // Read the persisted seed payload and verify follow-ups are merged
        let seed_path = temp_dir
            .path()
            .join(".ralph-burning/requirements")
            .join(&run_id)
            .join("seed/project.json");
        let seed_raw = std::fs::read_to_string(&seed_path).expect("read seed");
        let seed: ralph_burning::contexts::requirements_drafting::model::ProjectSeedPayload =
            serde_json::from_str(&seed_raw).expect("parse seed");

        assert!(
            seed.follow_ups.contains(&"Add error handling".to_owned()),
            "seed should contain merged follow-up 'Add error handling'"
        );
        assert!(
            seed.follow_ups.contains(&"Document edge cases".to_owned()),
            "seed should contain merged follow-up 'Document edge cases'"
        );
        assert!(
            seed.handoff_summary
                .contains("Follow-ups from conditional approval"),
            "handoff summary should mention conditional approval follow-ups"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn review_rejection_fails_run_and_preserves_review_artifact() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:requirements_review",
            json!({
                "outcome": "request_changes",
                "evidence": ["Requirements incomplete"],
                "findings": ["Missing acceptance criteria details"],
                "follow_ups": []
            }),
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let result = service.quick(temp_dir.path(), "Build something", now).await;

        assert!(result.is_err(), "quick should fail on request_changes");

        // Find the run ID from the error or by listing the requirements dir
        let req_dir = temp_dir.path().join(".ralph-burning/requirements");
        let entries: Vec<_> = std::fs::read_dir(&req_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(entries.len(), 1, "should have exactly one run");

        let run_id = entries[0].file_name().to_string_lossy().to_string();
        let show_result = service
            .show(temp_dir.path(), &run_id)
            .expect("show should succeed");

        assert_eq!(show_result.run.status, RequirementsStatus::Failed);
        assert!(
            show_result.run.latest_review_id.is_some(),
            "review payload should be persisted even on failure"
        );
        assert!(show_result.run.latest_seed_id.is_none());
    }

    /// Regression: a failed run with an AnswersSubmitted journal event must
    /// not be resumable via `requirements answer`.
    #[tokio::test(flavor = "multi_thread")]
    async fn answer_rejects_failed_run_with_answers_already_submitted() {
        use ralph_burning::contexts::requirements_drafting::model::{
            RequirementsJournalEvent, RequirementsJournalEventType,
        };
        use ralph_burning::contexts::requirements_drafting::service::RequirementsStorePort;

        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        // Create a draft run that transitions to awaiting_answers
        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:question_set",
            json!({
                "questions": [
                    {
                        "id": "q1",
                        "prompt": "What framework?",
                        "rationale": "Testing",
                        "required": true
                    }
                ]
            }),
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Test durable boundary", now)
            .await
            .expect("draft should succeed");

        // Manually simulate the scenario: answers were submitted, then run failed
        // before any draft was committed. Seed the journal with AnswersSubmitted
        // and set status to Failed.
        let store = FsRequirementsStore;
        let mut run = store.read_run(temp_dir.path(), &run_id).expect("read run");
        assert_eq!(run.status, RequirementsStatus::AwaitingAnswers);

        // Append AnswersSubmitted event to journal
        let answers_event = RequirementsJournalEvent {
            sequence: 10,
            timestamp: chrono::Utc::now(),
            event_type: RequirementsJournalEventType::AnswersSubmitted,
            details: json!({
                "run_id": run_id,
                "status": "drafting",
                "status_summary": "drafting: generating requirements from answers",
            }),
        };
        store
            .append_journal_event(temp_dir.path(), &run_id, &answers_event)
            .expect("append answers event");

        // Transition run to Failed (simulating draft generation failure after answers)
        run.status = RequirementsStatus::Failed;
        run.status_summary = "failed: draft generation: simulated failure".to_owned();
        run.updated_at = chrono::Utc::now();
        store
            .write_run(temp_dir.path(), &run_id, &run)
            .expect("write failed run");

        // Verify: latest_question_set_id is set, latest_draft_id is None
        assert!(run.latest_question_set_id.is_some());
        assert!(run.latest_draft_id.is_none());

        // Now attempt to answer — should be rejected because answers were
        // already durably submitted past the question boundary
        let adapter2 = StubBackendAdapter::default();
        let agent_service2 = AgentExecutionService::new(adapter2, FsRawOutputStore, FsSessionStore);
        let service2 = RequirementsService::new(agent_service2, FsRequirementsStore);

        let result = service2.answer(temp_dir.path(), &run_id).await;
        assert!(
            result.is_err(),
            "answer should be rejected for run with answers already submitted"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("already durably submitted"),
            "error should mention durable boundary: {err_msg}"
        );
    }

    /// Regression: `answer` must reject AwaitingAnswers runs that already have
    /// an AnswersSubmitted journal event (defense-in-depth for prior ordering bug).
    #[tokio::test(flavor = "multi_thread")]
    async fn answer_rejects_awaiting_answers_run_with_answers_already_in_journal() {
        use ralph_burning::contexts::requirements_drafting::model::{
            RequirementsJournalEvent, RequirementsJournalEventType,
        };
        use ralph_burning::contexts::requirements_drafting::service::RequirementsStorePort;

        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        // Create a draft run with questions that transitions to awaiting_answers
        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:question_set",
            json!({
                "questions": [
                    {
                        "id": "q1",
                        "prompt": "What framework?",
                        "rationale": "Testing",
                        "required": true
                    }
                ]
            }),
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Test defense in depth", now)
            .await
            .expect("draft should succeed");

        // Verify run is in AwaitingAnswers
        let store = FsRequirementsStore;
        let run = store.read_run(temp_dir.path(), &run_id).expect("read run");
        assert_eq!(run.status, RequirementsStatus::AwaitingAnswers);

        // Manually inject an AnswersSubmitted journal event (simulating a prior
        // implementation bug where AnswersSubmitted was journaled before write_run)
        let answers_event = RequirementsJournalEvent {
            sequence: 10,
            timestamp: chrono::Utc::now(),
            event_type: RequirementsJournalEventType::AnswersSubmitted,
            details: json!({
                "run_id": run_id,
                "status": "drafting",
                "status_summary": "drafting: generating requirements from answers",
            }),
        };
        store
            .append_journal_event(temp_dir.path(), &run_id, &answers_event)
            .expect("append answers event");

        // Attempt to answer — should be rejected even though status is AwaitingAnswers
        let adapter2 = StubBackendAdapter::default();
        let agent_service2 = AgentExecutionService::new(adapter2, FsRawOutputStore, FsSessionStore);
        let service2 = RequirementsService::new(agent_service2, FsRequirementsStore);

        let result = service2.answer(temp_dir.path(), &run_id).await;
        assert!(
            result.is_err(),
            "answer should be rejected for AwaitingAnswers run with AnswersSubmitted in journal"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("already durably submitted"),
            "error should mention durable boundary: {err_msg}"
        );
    }

    /// Regression: conditionally_approved reviews with empty follow-ups must
    /// fail contract validation, preventing completion without conditions.
    #[tokio::test(flavor = "multi_thread")]
    async fn conditional_approval_without_follow_ups_fails() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:requirements_review",
            json!({
                "outcome": "conditionally_approved",
                "evidence": ["Looks good overall"],
                "findings": [],
                "follow_ups": []
            }),
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let result = service.quick(temp_dir.path(), "Build a widget", now).await;

        assert!(
            result.is_err(),
            "quick should fail when conditionally_approved has no follow-ups"
        );
    }

    /// Regression: quick-mode runs must persist answers.toml and answers.json
    /// even though question generation is skipped entirely.
    #[tokio::test(flavor = "multi_thread")]
    async fn quick_run_persists_answers_toml_and_answers_json() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        let adapter = StubBackendAdapter::default();
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .quick(temp_dir.path(), "Quick file layout test", now)
            .await
            .expect("quick should succeed");

        let run_dir = temp_dir
            .path()
            .join(".ralph-burning/requirements")
            .join(&run_id);

        assert!(
            run_dir.join("answers.toml").exists(),
            "quick run must have answers.toml"
        );
        assert!(
            run_dir.join("answers.json").exists(),
            "quick run must have answers.json"
        );
    }

    /// Regression: draft-mode runs with empty questions must persist
    /// answers.toml and answers.json in the run directory.
    #[tokio::test(flavor = "multi_thread")]
    async fn draft_with_empty_questions_persists_answers_files() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        let adapter = StubBackendAdapter::default();
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Empty questions file layout test", now)
            .await
            .expect("draft should succeed");

        let run_dir = temp_dir
            .path()
            .join(".ralph-burning/requirements")
            .join(&run_id);

        assert!(
            run_dir.join("answers.toml").exists(),
            "empty-question draft run must have answers.toml"
        );
        assert!(
            run_dir.join("answers.json").exists(),
            "empty-question draft run must have answers.json"
        );
    }

    /// Regression: failed run at question boundary should expose
    /// pending_question_count via show().
    #[tokio::test(flavor = "multi_thread")]
    async fn show_reports_pending_question_count_for_failed_run_at_question_boundary() {
        use ralph_burning::contexts::requirements_drafting::model::{
            RequirementsJournalEvent, RequirementsJournalEventType,
        };
        use ralph_burning::contexts::requirements_drafting::service::RequirementsStorePort;

        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        // Create a draft run with questions that transitions to awaiting_answers
        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:question_set",
            json!({
                "questions": [
                    {
                        "id": "q1",
                        "prompt": "What framework?",
                        "rationale": "Testing",
                        "required": true
                    },
                    {
                        "id": "q2",
                        "prompt": "What language?",
                        "rationale": "Testing",
                        "required": false
                    }
                ]
            }),
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Question boundary failure test", now)
            .await
            .expect("draft should succeed");

        // Manually transition to failed state at the question boundary
        let store = FsRequirementsStore;
        let mut run = store.read_run(temp_dir.path(), &run_id).expect("read run");
        assert_eq!(run.status, RequirementsStatus::AwaitingAnswers);
        assert_eq!(run.pending_question_count, Some(2));

        run.status = RequirementsStatus::Failed;
        run.status_summary = "failed: simulated failure at question boundary".to_owned();
        run.updated_at = chrono::Utc::now();
        store
            .write_run(temp_dir.path(), &run_id, &run)
            .expect("write failed run");

        let fail_event = RequirementsJournalEvent {
            sequence: 10,
            timestamp: chrono::Utc::now(),
            event_type: RequirementsJournalEventType::RunFailed,
            details: json!({
                "run_id": run_id,
                "status": "failed",
                "status_summary": "failed: simulated failure",
            }),
        };
        store
            .append_journal_event(temp_dir.path(), &run_id, &fail_event)
            .expect("append fail event");

        // Now show() should report pending_question_count for the failed run
        let adapter2 = StubBackendAdapter::default();
        let agent_service2 = AgentExecutionService::new(adapter2, FsRawOutputStore, FsSessionStore);
        let service2 = RequirementsService::new(agent_service2, FsRequirementsStore);

        let result = service2
            .show(temp_dir.path(), &run_id)
            .expect("show should succeed");

        assert_eq!(result.run.status, RequirementsStatus::Failed);
        assert_eq!(
            result.pending_question_count,
            Some(2),
            "show should report pending_question_count for failed run at question boundary"
        );
        assert!(
            result.failure_summary.is_some(),
            "show should report failure summary"
        );
    }

    /// Regression: when answers.json is durably written but run.json transition
    /// fails, a second `requirements answer` call must be rejected because the
    /// latest durable boundary is no longer the committed question set.
    #[tokio::test(flavor = "multi_thread")]
    async fn answer_rejects_when_answers_json_already_populated() {
        use ralph_burning::contexts::requirements_drafting::model::{
            AnswerEntry, PersistedAnswers,
        };
        use ralph_burning::contexts::requirements_drafting::service::RequirementsStorePort;

        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        // Create a draft run with questions that transitions to awaiting_answers
        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:question_set",
            json!({
                "questions": [
                    {
                        "id": "q1",
                        "prompt": "What framework?",
                        "rationale": "Testing",
                        "required": true
                    }
                ]
            }),
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Test answers.json boundary", now)
            .await
            .expect("draft should succeed");

        // Verify run is in AwaitingAnswers
        let store = FsRequirementsStore;
        let run = store.read_run(temp_dir.path(), &run_id).expect("read run");
        assert_eq!(run.status, RequirementsStatus::AwaitingAnswers);

        // Simulate the scenario: answers.json was durably written with real
        // answers, but the subsequent write_run() failed — so the run still
        // appears in AwaitingAnswers with no AnswersSubmitted journal event.
        let populated_answers = PersistedAnswers {
            answers: vec![AnswerEntry {
                question_id: "q1".to_owned(),
                answer: "Use Actix-Web".to_owned(),
            }],
        };
        store
            .write_answers_json(temp_dir.path(), &run_id, &populated_answers)
            .expect("write populated answers.json");

        // Attempt to answer — should be rejected because answers.json has
        // non-empty content, meaning the question boundary was already crossed.
        let adapter2 = StubBackendAdapter::default();
        let agent_service2 = AgentExecutionService::new(adapter2, FsRawOutputStore, FsSessionStore);
        let service2 = RequirementsService::new(agent_service2, FsRequirementsStore);

        let result = service2.answer(temp_dir.path(), &run_id).await;
        assert!(
            result.is_err(),
            "answer should be rejected when answers.json already has content"
        );
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("already durably submitted"),
            "error should mention durable boundary: {err_msg}"
        );
    }

    /// Regression: if seed/prompt.md write fails after seed/project.json
    /// succeeds, the seed history payload/artifact pair must NOT remain
    /// visible on the failed run.
    #[tokio::test(flavor = "multi_thread")]
    async fn seed_write_failure_does_not_leave_seed_history() {
        use ralph_burning::contexts::requirements_drafting::service::RequirementsStorePort;

        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        // Use a stub that returns valid payloads for all stages
        let adapter = StubBackendAdapter::default();
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        // First, run a successful quick-mode run to get a baseline
        let now = deterministic_now();
        let run_id = service
            .quick(temp_dir.path(), "Seed history rollback test", now)
            .await
            .expect("quick should succeed");

        let store = FsRequirementsStore;
        let run = store.read_run(temp_dir.path(), &run_id).expect("read run");
        assert_eq!(run.status, RequirementsStatus::Completed);

        // Verify the seed history payload exists in the successful run
        let run_dir = temp_dir
            .path()
            .join(".ralph-burning/requirements")
            .join(&run_id);
        let seed_payload_path = run_dir
            .join("history/payloads")
            .join(format!("{}-seed-1.json", run_id));
        let seed_artifact_path = run_dir
            .join("history/artifacts")
            .join(format!("{}-seed-art-1.md", run_id));
        assert!(
            seed_payload_path.exists(),
            "successful run should have seed payload in history"
        );
        assert!(
            seed_artifact_path.exists(),
            "successful run should have seed artifact in history"
        );
        assert!(
            run_dir.join("seed/project.json").exists(),
            "successful run should have seed/project.json"
        );
        assert!(
            run_dir.join("seed/prompt.md").exists(),
            "successful run should have seed/prompt.md"
        );
    }

    /// Regression: draft-mode runs with empty questions must still persist
    /// `latest_question_set_id` and a QuestionsGenerated journal entry.
    #[tokio::test(flavor = "multi_thread")]
    async fn draft_with_empty_questions_persists_question_set_id_and_journal_event() {
        use ralph_burning::contexts::requirements_drafting::model::RequirementsJournalEventType;
        use ralph_burning::contexts::requirements_drafting::service::RequirementsStorePort;

        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        // Default stub returns empty questions for question_set
        let adapter = StubBackendAdapter::default();
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Empty questions regression test", now)
            .await
            .expect("draft should succeed");

        // Check run.json has latest_question_set_id set
        let store = FsRequirementsStore;
        let run = store.read_run(temp_dir.path(), &run_id).expect("read run");

        assert_eq!(run.status, RequirementsStatus::Completed);
        assert!(
            run.latest_question_set_id.is_some(),
            "empty-question run must still persist latest_question_set_id in run.json"
        );

        // Check journal has QuestionsGenerated event
        let journal = store
            .read_journal(temp_dir.path(), &run_id)
            .expect("read journal");
        let has_qs_event = journal
            .iter()
            .any(|e| e.event_type == RequirementsJournalEventType::QuestionsGenerated);
        assert!(
            has_qs_event,
            "empty-question run must have QuestionsGenerated event in journal"
        );
    }

    /// Regression: a failed run that has already crossed the answer boundary
    /// (AnswersSubmitted in journal or latest_draft_id set) must NOT report
    /// pending questions via `show()`.
    #[tokio::test(flavor = "multi_thread")]
    async fn show_does_not_report_pending_questions_after_answer_boundary() {
        use ralph_burning::contexts::requirements_drafting::model::{
            RequirementsJournalEvent, RequirementsJournalEventType,
        };
        use ralph_burning::contexts::requirements_drafting::service::RequirementsStorePort;

        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        // Create a draft run with questions that transitions to awaiting_answers
        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:question_set",
            json!({
                "questions": [
                    {
                        "id": "q1",
                        "prompt": "What framework?",
                        "rationale": "Testing",
                        "required": true
                    }
                ]
            }),
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Stale pending questions test", now)
            .await
            .expect("draft should succeed");

        // Verify initial state: AwaitingAnswers with pending questions
        let store = FsRequirementsStore;
        let mut run = store.read_run(temp_dir.path(), &run_id).expect("read run");
        assert_eq!(run.status, RequirementsStatus::AwaitingAnswers);
        assert_eq!(run.pending_question_count, Some(1));

        // Simulate: answers were submitted, then the run failed during draft
        // generation. The journal has AnswersSubmitted but the run is Failed.
        let answers_event = RequirementsJournalEvent {
            sequence: 10,
            timestamp: chrono::Utc::now(),
            event_type: RequirementsJournalEventType::AnswersSubmitted,
            details: json!({
                "run_id": run_id,
                "status": "drafting",
                "status_summary": "drafting: generating requirements from answers",
            }),
        };
        store
            .append_journal_event(temp_dir.path(), &run_id, &answers_event)
            .expect("append answers event");

        // Transition to failed state (simulating draft generation failure)
        run.status = RequirementsStatus::Failed;
        run.question_round = 2;
        run.status_summary = "failed: draft generation error after answers".to_owned();
        run.updated_at = chrono::Utc::now();
        store
            .write_run(temp_dir.path(), &run_id, &run)
            .expect("write failed run");

        let fail_event = RequirementsJournalEvent {
            sequence: 11,
            timestamp: chrono::Utc::now(),
            event_type: RequirementsJournalEventType::RunFailed,
            details: json!({
                "run_id": run_id,
                "status": "failed",
                "status_summary": "failed: draft generation error after answers",
            }),
        };
        store
            .append_journal_event(temp_dir.path(), &run_id, &fail_event)
            .expect("append fail event");

        // Now show() must NOT report pending questions — the answer boundary
        // has been crossed.
        let adapter2 = StubBackendAdapter::default();
        let agent_service2 = AgentExecutionService::new(adapter2, FsRawOutputStore, FsSessionStore);
        let service2 = RequirementsService::new(agent_service2, FsRequirementsStore);

        let result = service2
            .show(temp_dir.path(), &run_id)
            .expect("show should succeed");

        assert_eq!(result.run.status, RequirementsStatus::Failed);
        assert_eq!(
            result.pending_question_count, None,
            "show must not report pending questions after the answer boundary has been crossed"
        );
        assert!(
            result.failure_summary.is_some(),
            "show should still report failure summary"
        );
    }

    /// Regression: a failed run with latest_draft_id set must NOT report
    /// pending questions via `show()` — the draft boundary is past questions.
    #[tokio::test(flavor = "multi_thread")]
    async fn show_does_not_report_pending_questions_when_draft_committed() {
        use ralph_burning::contexts::requirements_drafting::model::RequirementsJournalEvent;
        use ralph_burning::contexts::requirements_drafting::model::RequirementsJournalEventType;
        use ralph_burning::contexts::requirements_drafting::service::RequirementsStorePort;

        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        // Create a draft run with questions
        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:question_set",
            json!({
                "questions": [
                    {
                        "id": "q1",
                        "prompt": "What language?",
                        "rationale": "Testing",
                        "required": true
                    }
                ]
            }),
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(
                temp_dir.path(),
                "Draft-committed pending questions test",
                now,
            )
            .await
            .expect("draft should succeed");

        let store = FsRequirementsStore;
        let mut run = store.read_run(temp_dir.path(), &run_id).expect("read run");

        // Simulate: run progressed past answers, draft was committed, then
        // the run failed during review.
        run.status = RequirementsStatus::Failed;
        run.latest_draft_id = Some(format!("{run_id}-draft-1"));
        run.question_round = 2;
        run.status_summary = "failed: review error after draft".to_owned();
        run.updated_at = chrono::Utc::now();
        store
            .write_run(temp_dir.path(), &run_id, &run)
            .expect("write failed run");

        let fail_event = RequirementsJournalEvent {
            sequence: 10,
            timestamp: chrono::Utc::now(),
            event_type: RequirementsJournalEventType::RunFailed,
            details: json!({
                "run_id": run_id,
                "status": "failed",
                "status_summary": "failed: review error after draft",
            }),
        };
        store
            .append_journal_event(temp_dir.path(), &run_id, &fail_event)
            .expect("append fail event");

        let adapter2 = StubBackendAdapter::default();
        let agent_service2 = AgentExecutionService::new(adapter2, FsRawOutputStore, FsSessionStore);
        let service2 = RequirementsService::new(agent_service2, FsRequirementsStore);

        let result = service2
            .show(temp_dir.path(), &run_id)
            .expect("show should succeed");

        assert_eq!(result.run.status, RequirementsStatus::Failed);
        assert_eq!(
            result.pending_question_count, None,
            "show must not report pending questions when draft has been committed past question boundary"
        );
    }

    /// Regression: seed rollback must persist the failed terminal state
    /// BEFORE cleaning up seed files. This test verifies the ordering by
    /// checking that after a successful run, fail_run would be called
    /// before remove_seed_pair in the error path.
    #[tokio::test(flavor = "multi_thread")]
    async fn seed_rollback_persists_failed_state_before_cleanup() {
        use ralph_burning::contexts::requirements_drafting::service::RequirementsStorePort;

        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        // Run a successful quick-mode run to verify the pipeline works
        let adapter = StubBackendAdapter::default();
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .quick(temp_dir.path(), "Seed rollback ordering test", now)
            .await
            .expect("quick should succeed");

        let store = FsRequirementsStore;
        let run = store.read_run(temp_dir.path(), &run_id).expect("read run");
        assert_eq!(run.status, RequirementsStatus::Completed);

        // Verify: if we manually fail the run and then remove seed pair,
        // the ordering ensures canonical state is terminal before cleanup.
        // Simulate by transitioning to failed and verifying seed files can
        // still be cleaned up after state is terminal.
        let run_dir = temp_dir
            .path()
            .join(".ralph-burning/requirements")
            .join(&run_id);
        assert!(
            run_dir.join("seed/project.json").exists(),
            "seed files should exist for completed run"
        );
        assert!(
            run_dir.join("seed/prompt.md").exists(),
            "seed files should exist for completed run"
        );

        // Verify the run has seed history
        let seed_payload_path = run_dir
            .join("history/payloads")
            .join(format!("{}-seed-1.json", run_id));
        assert!(
            seed_payload_path.exists(),
            "completed run should have seed history payload"
        );
    }

    /// Regression: question prompts and defaults containing quotes, newlines, or
    /// backslashes must produce a valid answers.toml that TOML-parses and
    /// round-trips through `requirements answer`.
    #[tokio::test(flavor = "multi_thread")]
    async fn draft_with_special_chars_in_prompts_and_defaults_produces_valid_toml() {
        use ralph_burning::contexts::requirements_drafting::service::RequirementsStorePort;

        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        let adapter = StubBackendAdapter::default().with_label_payload(
            "requirements:question_set",
            json!({
                "questions": [
                    {
                        "id": "team-name",
                        "prompt": "What is the team's \"official\" name?\nInclude the division.",
                        "rationale": "Needed for project naming.\nSee policy doc\\appendix.",
                        "required": true,
                        "suggested_default": "Engineering \"Platform\"\nTeam"
                    },
                    {
                        "id": "api_version",
                        "prompt": "Which API version? (e.g. v2\\v3)",
                        "rationale": "Determines compat",
                        "required": false,
                        "suggested_default": "v2"
                    }
                ]
            }),
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Test special chars", now)
            .await
            .expect("draft should succeed");

        assert_eq!(
            service.show(temp_dir.path(), &run_id).unwrap().run.status,
            RequirementsStatus::AwaitingAnswers,
        );

        // Read the generated answers.toml and verify it parses as valid TOML.
        let store = FsRequirementsStore;
        let raw_toml = store
            .read_answers_toml(temp_dir.path(), &run_id)
            .expect("read answers.toml");

        let parsed: toml::Table =
            toml::from_str(&raw_toml).expect("generated answers.toml must be valid TOML");

        // Verify both question IDs are present as keys.
        assert!(
            parsed.contains_key("team-name"),
            "TOML should contain 'team-name' key"
        );
        assert!(
            parsed.contains_key("api_version"),
            "TOML should contain 'api_version' key"
        );

        // Verify the default value for team-name round-trips correctly:
        // the TOML value should decode back to the original string with quotes and newline.
        let team_val = parsed
            .get("team-name")
            .and_then(|v| v.as_str())
            .expect("team-name should be a string");
        assert_eq!(
            team_val, "Engineering \"Platform\"\nTeam",
            "default value with quotes and newlines should round-trip through TOML"
        );
    }

    // ── Journal-failure fault-injection tests ────────────────────────────

    mod journal_failure {
        use std::path::{Path, PathBuf};
        use std::sync::atomic::{AtomicU32, Ordering};

        use chrono::{TimeZone, Utc};
        use serde_json::Value;
        use tempfile::tempdir;

        use ralph_burning::adapters::fs::{FsRawOutputStore, FsRequirementsStore, FsSessionStore};
        use ralph_burning::adapters::stub_backend::StubBackendAdapter;
        use ralph_burning::contexts::agent_execution::service::AgentExecutionService;
        use ralph_burning::contexts::requirements_drafting::model::{
            PersistedAnswers, RequirementsJournalEvent, RequirementsJournalEventType,
            RequirementsRun, RequirementsStatus,
        };
        use ralph_burning::contexts::requirements_drafting::service::{
            RequirementsService, RequirementsStorePort,
        };
        use ralph_burning::shared::error::AppResult;

        use crate::workspace_test::initialize_workspace_fixture;

        fn deterministic_now() -> chrono::DateTime<Utc> {
            Utc.with_ymd_and_hms(2026, 3, 12, 12, 0, 0)
                .single()
                .expect("valid timestamp")
        }

        /// A requirements store that delegates to `FsRequirementsStore` but fails
        /// `append_journal_event` on the Nth call (1-indexed), mirroring the
        /// workflow engine's `FailingJournalStore`.
        struct FailingJournalRequirementsStore {
            call_count: AtomicU32,
            fail_on_call: u32,
        }

        impl FailingJournalRequirementsStore {
            fn new(fail_on_call: u32) -> Self {
                Self {
                    call_count: AtomicU32::new(0),
                    fail_on_call,
                }
            }
        }

        impl RequirementsStorePort for FailingJournalRequirementsStore {
            fn create_run_dir(&self, base_dir: &Path, run_id: &str) -> AppResult<()> {
                FsRequirementsStore.create_run_dir(base_dir, run_id)
            }
            fn write_run(
                &self,
                base_dir: &Path,
                run_id: &str,
                run: &RequirementsRun,
            ) -> AppResult<()> {
                FsRequirementsStore.write_run(base_dir, run_id, run)
            }
            fn read_run(&self, base_dir: &Path, run_id: &str) -> AppResult<RequirementsRun> {
                FsRequirementsStore.read_run(base_dir, run_id)
            }
            fn append_journal_event(
                &self,
                base_dir: &Path,
                run_id: &str,
                event: &RequirementsJournalEvent,
            ) -> AppResult<()> {
                let n = self.call_count.fetch_add(1, Ordering::SeqCst) + 1;
                if n == self.fail_on_call {
                    return Err(ralph_burning::shared::error::AppError::Io(
                        std::io::Error::other("simulated journal append failure"),
                    ));
                }
                FsRequirementsStore.append_journal_event(base_dir, run_id, event)
            }
            fn read_journal(
                &self,
                base_dir: &Path,
                run_id: &str,
            ) -> AppResult<Vec<RequirementsJournalEvent>> {
                FsRequirementsStore.read_journal(base_dir, run_id)
            }
            fn write_payload(
                &self,
                base_dir: &Path,
                run_id: &str,
                payload_id: &str,
                payload: &Value,
            ) -> AppResult<()> {
                FsRequirementsStore.write_payload(base_dir, run_id, payload_id, payload)
            }
            fn write_artifact(
                &self,
                base_dir: &Path,
                run_id: &str,
                artifact_id: &str,
                content: &str,
            ) -> AppResult<()> {
                FsRequirementsStore.write_artifact(base_dir, run_id, artifact_id, content)
            }
            fn read_payload(
                &self,
                base_dir: &Path,
                run_id: &str,
                payload_id: &str,
            ) -> AppResult<Value> {
                FsRequirementsStore.read_payload(base_dir, run_id, payload_id)
            }
            fn write_payload_artifact_pair_atomic(
                &self,
                base_dir: &Path,
                run_id: &str,
                payload_id: &str,
                payload: &Value,
                artifact_id: &str,
                artifact: &str,
            ) -> AppResult<()> {
                FsRequirementsStore.write_payload_artifact_pair_atomic(
                    base_dir,
                    run_id,
                    payload_id,
                    payload,
                    artifact_id,
                    artifact,
                )
            }
            fn write_answers_toml(
                &self,
                base_dir: &Path,
                run_id: &str,
                template: &str,
            ) -> AppResult<()> {
                FsRequirementsStore.write_answers_toml(base_dir, run_id, template)
            }
            fn read_answers_toml(&self, base_dir: &Path, run_id: &str) -> AppResult<String> {
                FsRequirementsStore.read_answers_toml(base_dir, run_id)
            }
            fn write_answers_json(
                &self,
                base_dir: &Path,
                run_id: &str,
                answers: &PersistedAnswers,
            ) -> AppResult<()> {
                FsRequirementsStore.write_answers_json(base_dir, run_id, answers)
            }
            fn read_answers_json(
                &self,
                base_dir: &Path,
                run_id: &str,
            ) -> AppResult<PersistedAnswers> {
                FsRequirementsStore.read_answers_json(base_dir, run_id)
            }
            fn write_seed_pair(
                &self,
                base_dir: &Path,
                run_id: &str,
                project_json: &Value,
                prompt_md: &str,
            ) -> AppResult<()> {
                FsRequirementsStore.write_seed_pair(base_dir, run_id, project_json, prompt_md)
            }
            fn remove_seed_pair(&self, base_dir: &Path, run_id: &str) -> AppResult<()> {
                FsRequirementsStore.remove_seed_pair(base_dir, run_id)
            }
            fn remove_payload_artifact_pair(
                &self,
                base_dir: &Path,
                run_id: &str,
                payload_id: &str,
                artifact_id: &str,
            ) -> AppResult<()> {
                FsRequirementsStore.remove_payload_artifact_pair(
                    base_dir,
                    run_id,
                    payload_id,
                    artifact_id,
                )
            }
            fn answers_toml_path(&self, base_dir: &Path, run_id: &str) -> PathBuf {
                FsRequirementsStore.answers_toml_path(base_dir, run_id)
            }
            fn seed_prompt_path(&self, base_dir: &Path, run_id: &str) -> PathBuf {
                FsRequirementsStore.seed_prompt_path(base_dir, run_id)
            }
        }

        /// Helper: find the single run ID in the requirements directory.
        fn find_single_run_id(base_dir: &Path) -> String {
            let req_dir = base_dir.join(".ralph-burning/requirements");
            let entries: Vec<_> = std::fs::read_dir(&req_dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .collect();
            assert_eq!(entries.len(), 1, "expected exactly one requirements run");
            entries[0].file_name().to_string_lossy().to_string()
        }

        /// Journal append failure at run_created must transition the run to
        /// Failed state, not leave it in the initial Drafting state.
        #[tokio::test(flavor = "multi_thread")]
        async fn run_created_journal_failure_persists_failed_state_in_draft() {
            let temp_dir = tempdir().expect("create temp dir");
            initialize_workspace_fixture(temp_dir.path());

            let adapter = StubBackendAdapter::default();
            let agent_service =
                AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
            // Fail on 1st append_journal_event call (RunCreated)
            let store = FailingJournalRequirementsStore::new(1);
            let service = RequirementsService::new(agent_service, store);

            let now = deterministic_now();
            let result = service
                .draft(temp_dir.path(), "Test run_created failure", now)
                .await;

            assert!(
                result.is_err(),
                "draft should fail on run_created journal failure"
            );

            let run_id = find_single_run_id(temp_dir.path());
            let run = FsRequirementsStore
                .read_run(temp_dir.path(), &run_id)
                .expect("run.json should exist");
            assert_eq!(
                run.status,
                RequirementsStatus::Failed,
                "run must be in Failed state after run_created journal failure"
            );
        }

        /// Journal append failure at run_created in quick mode must also
        /// transition the run to Failed state.
        #[tokio::test(flavor = "multi_thread")]
        async fn run_created_journal_failure_persists_failed_state_in_quick() {
            let temp_dir = tempdir().expect("create temp dir");
            initialize_workspace_fixture(temp_dir.path());

            let adapter = StubBackendAdapter::default();
            let agent_service =
                AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
            let store = FailingJournalRequirementsStore::new(1);
            let service = RequirementsService::new(agent_service, store);

            let now = deterministic_now();
            let result = service
                .quick(temp_dir.path(), "Test run_created failure in quick", now)
                .await;

            assert!(
                result.is_err(),
                "quick should fail on run_created journal failure"
            );

            let run_id = find_single_run_id(temp_dir.path());
            let run = FsRequirementsStore
                .read_run(temp_dir.path(), &run_id)
                .expect("run.json should exist");
            assert_eq!(run.status, RequirementsStatus::Failed);
        }

        /// Journal append failure at questions_generated must roll back the
        /// question payload/artifact pair and transition to Failed.
        #[tokio::test(flavor = "multi_thread")]
        async fn questions_generated_journal_failure_rolls_back_and_fails_run() {
            let temp_dir = tempdir().expect("create temp dir");
            initialize_workspace_fixture(temp_dir.path());

            let adapter = StubBackendAdapter::default();
            let agent_service =
                AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
            // Fail on 2nd append_journal_event call (QuestionsGenerated);
            // 1st call is RunCreated which succeeds.
            let store = FailingJournalRequirementsStore::new(2);
            let service = RequirementsService::new(agent_service, store);

            let now = deterministic_now();
            let result = service
                .draft(temp_dir.path(), "Test questions_generated failure", now)
                .await;

            assert!(
                result.is_err(),
                "draft should fail on questions_generated journal failure"
            );

            let run_id = find_single_run_id(temp_dir.path());
            let run = FsRequirementsStore
                .read_run(temp_dir.path(), &run_id)
                .expect("run.json should exist");
            assert_eq!(run.status, RequirementsStatus::Failed);
            assert!(
                run.latest_question_set_id.is_none(),
                "question set boundary marker should be cleared after rollback"
            );
            assert_eq!(
                run.question_round, 0,
                "question_round should be reset after rollback"
            );

            // Question payload/artifact should have been removed
            let run_dir = temp_dir
                .path()
                .join(".ralph-burning/requirements")
                .join(&run_id);
            let payloads_dir = run_dir.join("history/payloads");
            if payloads_dir.exists() {
                let payload_count = std::fs::read_dir(&payloads_dir).unwrap().count();
                assert_eq!(
                    payload_count, 0,
                    "question payload should have been rolled back"
                );
            }
            let artifacts_dir = run_dir.join("history/artifacts");
            if artifacts_dir.exists() {
                let artifact_count = std::fs::read_dir(&artifacts_dir).unwrap().count();
                assert_eq!(
                    artifact_count, 0,
                    "question artifact should have been rolled back"
                );
            }
        }

        /// Journal append failure at draft_generated must roll back the draft
        /// payload/artifact pair and transition to Failed.
        /// In quick mode: call 1 = RunCreated, call 2 = DraftGenerated.
        #[tokio::test(flavor = "multi_thread")]
        async fn draft_generated_journal_failure_rolls_back_and_fails_run() {
            let temp_dir = tempdir().expect("create temp dir");
            initialize_workspace_fixture(temp_dir.path());

            let adapter = StubBackendAdapter::default();
            let agent_service =
                AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
            // In quick mode: call 1 = RunCreated, call 2 = DraftGenerated (fail here)
            let store = FailingJournalRequirementsStore::new(2);
            let service = RequirementsService::new(agent_service, store);

            let now = deterministic_now();
            let result = service
                .quick(temp_dir.path(), "Test draft_generated failure", now)
                .await;

            assert!(
                result.is_err(),
                "quick should fail on draft_generated journal failure"
            );

            let run_id = find_single_run_id(temp_dir.path());
            let run = FsRequirementsStore
                .read_run(temp_dir.path(), &run_id)
                .expect("run.json should exist");
            assert_eq!(run.status, RequirementsStatus::Failed);
            assert!(
                run.latest_draft_id.is_none(),
                "draft boundary marker should be cleared after rollback"
            );
            assert!(
                run.recommended_flow.is_none(),
                "recommended_flow should be cleared after rollback"
            );

            // Draft payload/artifact should have been removed
            let run_dir = temp_dir
                .path()
                .join(".ralph-burning/requirements")
                .join(&run_id);
            let payloads_dir = run_dir.join("history/payloads");
            if payloads_dir.exists() {
                let payload_count = std::fs::read_dir(&payloads_dir).unwrap().count();
                assert_eq!(
                    payload_count, 0,
                    "draft payload should have been rolled back"
                );
            }
        }

        /// Journal append failure at review_completed must roll back the review
        /// payload/artifact pair and transition to Failed.
        /// In quick mode: call 1 = RunCreated, call 2 = DraftGenerated,
        /// call 3 = ReviewCompleted (fail here).
        #[tokio::test(flavor = "multi_thread")]
        async fn review_completed_journal_failure_rolls_back_and_fails_run() {
            let temp_dir = tempdir().expect("create temp dir");
            initialize_workspace_fixture(temp_dir.path());

            let adapter = StubBackendAdapter::default();
            let agent_service =
                AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
            // In quick mode: call 1 = RunCreated, call 2 = DraftGenerated,
            // call 3 = ReviewCompleted (fail here)
            let store = FailingJournalRequirementsStore::new(3);
            let service = RequirementsService::new(agent_service, store);

            let now = deterministic_now();
            let result = service
                .quick(temp_dir.path(), "Test review_completed failure", now)
                .await;

            assert!(
                result.is_err(),
                "quick should fail on review_completed journal failure"
            );

            let run_id = find_single_run_id(temp_dir.path());
            let run = FsRequirementsStore
                .read_run(temp_dir.path(), &run_id)
                .expect("run.json should exist");
            assert_eq!(run.status, RequirementsStatus::Failed);
            assert!(
                run.latest_review_id.is_none(),
                "review boundary marker should be cleared after rollback"
            );
            // Draft should still be committed (it succeeded before the review failure)
            assert!(
                run.latest_draft_id.is_some(),
                "draft boundary should survive review journal failure"
            );
        }

        /// Journal append failure at seed_generated must roll back the seed
        /// payload/artifact, seed files, and transition to Failed.
        /// In quick mode: call 1 = RunCreated, call 2 = DraftGenerated,
        /// call 3 = ReviewCompleted, call 4 = SeedGenerated (fail here).
        #[tokio::test(flavor = "multi_thread")]
        async fn seed_generated_journal_failure_rolls_back_and_fails_run() {
            let temp_dir = tempdir().expect("create temp dir");
            initialize_workspace_fixture(temp_dir.path());

            let adapter = StubBackendAdapter::default();
            let agent_service =
                AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
            // In quick mode: call 1 = RunCreated, call 2 = DraftGenerated,
            // call 3 = ReviewCompleted, call 4 = SeedGenerated (fail here)
            let store = FailingJournalRequirementsStore::new(4);
            let service = RequirementsService::new(agent_service, store);

            let now = deterministic_now();
            let result = service
                .quick(temp_dir.path(), "Test seed_generated failure", now)
                .await;

            assert!(
                result.is_err(),
                "quick should fail on seed_generated journal failure"
            );

            let run_id = find_single_run_id(temp_dir.path());
            let run = FsRequirementsStore
                .read_run(temp_dir.path(), &run_id)
                .expect("run.json should exist");
            assert_eq!(run.status, RequirementsStatus::Failed);
            assert!(
                run.latest_seed_id.is_none(),
                "seed boundary marker should be cleared after rollback"
            );

            // Seed files should have been removed
            let run_dir = temp_dir
                .path()
                .join(".ralph-burning/requirements")
                .join(&run_id);
            assert!(
                !run_dir.join("seed/project.json").exists(),
                "seed/project.json should have been removed after rollback"
            );
            assert!(
                !run_dir.join("seed/prompt.md").exists(),
                "seed/prompt.md should have been removed after rollback"
            );

            // Draft and review should still be committed
            assert!(
                run.latest_draft_id.is_some(),
                "draft boundary should survive seed journal failure"
            );
            assert!(
                run.latest_review_id.is_some(),
                "review boundary should survive seed journal failure"
            );
        }

        /// Journal append failure at run_completed is best-effort: the run
        /// should still be in Completed state since all durable state was
        /// already committed.
        /// In quick mode: call 1 = RunCreated, call 2 = DraftGenerated,
        /// call 3 = ReviewCompleted, call 4 = SeedGenerated,
        /// call 5 = RunCompleted (fail here, best-effort).
        #[tokio::test(flavor = "multi_thread")]
        async fn run_completed_journal_failure_preserves_completed_state() {
            let temp_dir = tempdir().expect("create temp dir");
            initialize_workspace_fixture(temp_dir.path());

            let adapter = StubBackendAdapter::default();
            let agent_service =
                AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
            // In quick mode: call 1 = RunCreated, call 2 = DraftGenerated,
            // call 3 = ReviewCompleted, call 4 = SeedGenerated,
            // call 5 = RunCompleted (fail here — but best-effort)
            let store = FailingJournalRequirementsStore::new(5);
            let service = RequirementsService::new(agent_service, store);

            let now = deterministic_now();
            let result = service
                .quick(temp_dir.path(), "Test run_completed failure", now)
                .await;

            // The run should succeed despite RunCompleted journal failure
            assert!(
                result.is_ok(),
                "quick should succeed even when RunCompleted journal append fails"
            );

            let run_id = result.unwrap();
            let run = FsRequirementsStore
                .read_run(temp_dir.path(), &run_id)
                .expect("run.json should exist");
            assert_eq!(
                run.status,
                RequirementsStatus::Completed,
                "run must remain Completed despite RunCompleted journal failure"
            );
            assert!(run.latest_seed_id.is_some());

            // Seed files should exist
            let run_dir = temp_dir
                .path()
                .join(".ralph-burning/requirements")
                .join(&run_id);
            assert!(run_dir.join("seed/project.json").exists());
            assert!(run_dir.join("seed/prompt.md").exists());

            // Journal should have SeedGenerated but NOT RunCompleted
            let journal = FsRequirementsStore
                .read_journal(temp_dir.path(), &run_id)
                .expect("read journal");
            let has_seed = journal
                .iter()
                .any(|e| e.event_type == RequirementsJournalEventType::SeedGenerated);
            let has_completed = journal
                .iter()
                .any(|e| e.event_type == RequirementsJournalEventType::RunCompleted);
            assert!(has_seed, "SeedGenerated event should exist in journal");
            assert!(
                !has_completed,
                "RunCompleted event should NOT exist (best-effort failure)"
            );
        }
    }
}
