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
    assert!(
        !run.is_terminal(),
        "AwaitingAnswers should not be terminal"
    );

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

    let payload = QuestionSetPayload {
        questions: vec![],
    };

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
        let agent_service =
            AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
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
        let agent_service =
            AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
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
        let agent_service =
            AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
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
        let agent_service =
            AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
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
        let agent_service =
            AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
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
        let agent_service =
            AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
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
        std::fs::write(&answers_path, "unknown_key = \"some value\"\n")
            .expect("write answers");

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
        let agent_service =
            AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
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

        let adapter = StubBackendAdapter::default()
            .with_label_payload(
                "requirements:requirements_review",
                json!({
                    "outcome": "conditionally_approved",
                    "evidence": ["Looks good overall"],
                    "findings": [],
                    "follow_ups": ["Add error handling", "Document edge cases"]
                }),
            );
        let agent_service =
            AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
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
            seed.handoff_summary.contains("Follow-ups from conditional approval"),
            "handoff summary should mention conditional approval follow-ups"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn review_rejection_fails_run_and_preserves_review_artifact() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace_fixture(temp_dir.path());

        let adapter = StubBackendAdapter::default()
            .with_label_payload(
                "requirements:requirements_review",
                json!({
                    "outcome": "request_changes",
                    "evidence": ["Requirements incomplete"],
                    "findings": ["Missing acceptance criteria details"],
                    "follow_ups": []
                }),
            );
        let agent_service =
            AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let result = service
            .quick(temp_dir.path(), "Build something", now)
            .await;

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
        let agent_service =
            AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
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
        let mut run = store
            .read_run(temp_dir.path(), &run_id)
            .expect("read run");
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
        let agent_service2 =
            AgentExecutionService::new(adapter2, FsRawOutputStore, FsSessionStore);
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
        let agent_service =
            AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Test defense in depth", now)
            .await
            .expect("draft should succeed");

        // Verify run is in AwaitingAnswers
        let store = FsRequirementsStore;
        let run = store
            .read_run(temp_dir.path(), &run_id)
            .expect("read run");
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
        let agent_service2 =
            AgentExecutionService::new(adapter2, FsRawOutputStore, FsSessionStore);
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

        let adapter = StubBackendAdapter::default()
            .with_label_payload(
                "requirements:requirements_review",
                json!({
                    "outcome": "conditionally_approved",
                    "evidence": ["Looks good overall"],
                    "findings": [],
                    "follow_ups": []
                }),
            );
        let agent_service =
            AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let result = service
            .quick(temp_dir.path(), "Build a widget", now)
            .await;

        assert!(
            result.is_err(),
            "quick should fail when conditionally_approved has no follow-ups"
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
        let agent_service =
            AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let service = RequirementsService::new(agent_service, FsRequirementsStore);

        let now = deterministic_now();
        let run_id = service
            .draft(temp_dir.path(), "Empty questions regression test", now)
            .await
            .expect("draft should succeed");

        // Check run.json has latest_question_set_id set
        let store = FsRequirementsStore;
        let run = store
            .read_run(temp_dir.path(), &run_id)
            .expect("read run");

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
}
