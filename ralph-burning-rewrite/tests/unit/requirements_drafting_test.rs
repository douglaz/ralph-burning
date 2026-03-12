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
    }
}
