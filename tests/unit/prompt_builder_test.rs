use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Utc;
use serde_json::json;
use tempfile::tempdir;

use ralph_burning::adapters::process_backend::ProcessBackendAdapter;
use ralph_burning::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
use ralph_burning::contexts::project_run_record::journal;
use ralph_burning::contexts::project_run_record::model::{
    ArtifactRecord, JournalEvent, JournalEventType, PayloadRecord, QueuedAmendment,
};
use ralph_burning::contexts::project_run_record::service::{
    render_bead_task_prompt, ArtifactStorePort, BeadProjectContext,
};
use ralph_burning::contexts::project_run_record::task_prompt_contract;
use ralph_burning::contexts::workflow_composition::contracts::contract_for_stage;
use ralph_burning::contexts::workflow_composition::engine::build_stage_prompt;
use ralph_burning::contexts::workflow_composition::panel_contracts::RecordKind;
use ralph_burning::shared::domain::{
    BackendFamily, BackendRole, FlowPreset, ProjectId, ResolvedBackendTarget, RunId, SessionPolicy,
    StageCursor, StageId,
};
use ralph_burning::shared::error::{AppError, AppResult};

struct InMemoryArtifactStore {
    payloads: Vec<PayloadRecord>,
}

impl ArtifactStorePort for InMemoryArtifactStore {
    fn list_payloads(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<Vec<PayloadRecord>> {
        Ok(self.payloads.clone())
    }

    fn list_artifacts(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<Vec<ArtifactRecord>> {
        Ok(Vec::new())
    }
}

fn project_root(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
    base_dir
        .join(".ralph-burning")
        .join("projects")
        .join(project_id.as_str())
}

fn write_prompt_fixture(
    base_dir: &Path,
    project_id: &ProjectId,
    prompt_reference: &str,
    prompt_text: &str,
    events: &[JournalEvent],
) {
    let root = project_root(base_dir, project_id);
    fs::create_dir_all(&root).expect("create project root");
    fs::write(root.join(prompt_reference), prompt_text).expect("write prompt file");

    let journal_lines = events
        .iter()
        .map(journal::serialize_event)
        .collect::<AppResult<Vec<_>>>()
        .expect("serialize journal events")
        .join("\n");
    fs::write(root.join("journal.ndjson"), journal_lines).expect("write journal");
}

fn project_created_event(project_id: &ProjectId) -> JournalEvent {
    JournalEvent {
        sequence: 1,
        timestamp: Utc::now(),
        event_type: JournalEventType::ProjectCreated,
        details: json!({
            "project_id": project_id.as_str(),
            "flow": FlowPreset::Standard.as_str(),
        }),
    }
}

fn amendment(body: &str) -> QueuedAmendment {
    let source = ralph_burning::contexts::project_run_record::model::AmendmentSource::WorkflowStage;
    let dedup_key = QueuedAmendment::compute_dedup_key(&source, body);
    QueuedAmendment {
        amendment_id: format!("amd-{}", body.replace(' ', "-")),
        source_stage: StageId::Review,
        source_cycle: 1,
        source_completion_round: 1,
        body: body.to_owned(),
        created_at: Utc::now(),
        batch_sequence: 1,
        source,
        dedup_key,
    }
}

fn sample_bead_context() -> BeadProjectContext {
    BeadProjectContext {
        milestone_id: "ms-alpha".to_owned(),
        milestone_name: "Alpha Milestone".to_owned(),
        milestone_description: "Deliver the alpha milestone.".to_owned(),
        milestone_summary: Some("Ship milestone-aware task execution.".to_owned()),
        milestone_status: ralph_burning::contexts::milestone_record::model::MilestoneStatus::Ready,
        milestone_progress: ralph_burning::contexts::milestone_record::model::MilestoneProgress {
            total_beads: 4,
            completed_beads: 1,
            in_progress_beads: 1,
            failed_beads: 0,
            skipped_beads: 0,
            blocked_beads: 1,
        },
        milestone_goals: vec!["Create bead-backed tasks without manual setup".to_owned()],
        milestone_non_goals: vec!["Avoid unrelated milestone work".to_owned()],
        milestone_constraints: vec!["Reuse the current project substrate".to_owned()],
        agents_guidance: Some("Keep changes inspectable and deterministic.".to_owned()),
        bead_id: "ms-alpha.bead-2".to_owned(),
        bead_title: "Bootstrap bead-backed task creation".to_owned(),
        bead_description: Some(
            "Create a project directly from milestone and bead context.".to_owned(),
        ),
        bead_acceptance_criteria: vec![
            "Controller can create the project without manual setup".to_owned()
        ],
        upstream_dependencies: vec![
            ralph_burning::contexts::project_run_record::service::BeadDependencyPromptContext {
                id: "ms-alpha.bead-1".to_owned(),
                title: Some("Define task-source metadata".to_owned()),
                relationship: "blocking dependency".to_owned(),
                status: Some("closed".to_owned()),
                outcome: Some("completed".to_owned()),
            },
        ],
        downstream_dependents: vec![
            ralph_burning::contexts::project_run_record::service::BeadDependencyPromptContext {
                id: "ms-alpha.bead-4".to_owned(),
                title: Some("Automate milestone follow-up".to_owned()),
                relationship: "downstream dependent".to_owned(),
                status: Some("open".to_owned()),
                outcome: Some("pending".to_owned()),
            },
        ],
        planned_elsewhere: vec![
            ralph_burning::contexts::project_run_record::service::PlannedElsewherePromptContext {
                id: "ms-alpha.bead-4".to_owned(),
                title: "Automate milestone follow-up".to_owned(),
                relationship: "downstream dependent".to_owned(),
                status: Some("open".to_owned()),
                summary: Some("Handle the next automation pass after creation works.".to_owned()),
            },
        ],
        review_policy: task_prompt_contract::default_review_policy(),
        parent_epic_id: Some("ms-alpha.epic-1".to_owned()),
        flow: FlowPreset::QuickDev,
        plan_hash: Some("plan-hash-123".to_owned()),
        plan_version: Some(3),
    }
}

fn extract_prompt_schema(prompt: &str) -> serde_json::Value {
    let (_, after_heading) = prompt
        .split_once("## Authoritative JSON Schema")
        .expect("prompt should contain authoritative schema heading");
    let (_, after_open) = after_heading
        .split_once("```json\n")
        .expect("prompt should contain schema code fence");
    let (schema_text, _) = after_open
        .split_once("\n```")
        .expect("prompt schema fence should close");
    serde_json::from_str(schema_text).expect("prompt schema should parse")
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).expect("write executable");
    let mut permissions = fs::metadata(path).expect("stat executable").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("chmod executable");
}

fn extract_stdin_schema(stdin_payload: &str) -> serde_json::Value {
    let (_, schema_text) = stdin_payload
        .split_once("Return ONLY valid JSON matching the following schema:\n")
        .expect("stdin should contain schema marker");
    serde_json::from_str(schema_text.trim()).expect("stdin schema should parse")
}

fn write_fake_claude(bin_dir: &Path, envelope_json: &str) {
    write_executable(
        &bin_dir.join("claude"),
        &format!(
            r#"#!/bin/sh
next_is_schema=0
for arg in "$@"; do
    if [ "$next_is_schema" = "1" ]; then
        printf '%s' "$arg" > "$PWD/claude-json-schema.json"
        next_is_schema=0
    fi
    if [ "$arg" = "--json-schema" ]; then
        next_is_schema=1
    fi
done
cat > "$PWD/claude-stdin.txt"
printf '%s' '{envelope_json}'
"#
        ),
    );
}

#[test]
fn build_stage_prompt_includes_project_prompt_role_prior_outputs_remediation_amendments_and_schema()
{
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-builder-rich").unwrap();
    let run_id = RunId::new("run-20260314193203").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::Implementation, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::Implementation);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::PromptReview, 20),
        journal::stage_completed_event(
            3,
            Utc::now(),
            &run_id,
            StageId::PromptReview,
            1,
            1,
            "payload-prompt-review",
            "artifact-prompt-review",
        ),
        journal::stage_completed_event(
            4,
            Utc::now(),
            &run_id,
            StageId::Planning,
            1,
            1,
            "payload-planning",
            "artifact-planning",
        ),
    ];
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        "# Original Project Prompt\n\nShip the workflow prompt enrichment feature.",
        &events,
    );

    let artifact_store = InMemoryArtifactStore {
        payloads: vec![
            PayloadRecord {
                payload_id: "payload-planning".to_owned(),
                stage_id: StageId::Planning,
                cycle: 1,
                attempt: 1,
                created_at: Utc::now(),
                payload: json!({
                    "problem_framing": "second-output",
                    "proposed_work": [{"order": 1, "summary": "implement", "details": "details"}],
                }),
                record_kind: RecordKind::StagePrimary,
                producer: None,
                completion_round: 0,
            },
            PayloadRecord {
                payload_id: "payload-prompt-review".to_owned(),
                stage_id: StageId::PromptReview,
                cycle: 1,
                attempt: 1,
                created_at: Utc::now(),
                payload: json!({
                    "problem_framing": "first-output",
                    "readiness": {"ready": true, "risks": []},
                }),
                record_kind: RecordKind::StagePrimary,
                producer: None,
                completion_round: 0,
            },
        ],
    };
    let remediation = json!({
        "source_stage": "qa",
        "cycle": 2,
        "follow_up_or_amendments": ["fix the failing edge case"],
    });
    let amendments = vec![
        amendment("Tighten the validation copy"),
        amendment("Add a retry note"),
    ];

    let prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Implementer,
        &contract,
        &run_id,
        &cursor,
        Some(&remediation),
        Some(&amendments),
    )
    .expect("build prompt");

    assert!(prompt.contains("Ship the workflow prompt enrichment feature."));
    assert!(prompt.contains(
        "You are the Implementer. Your objective for this Implementation stage is to deliver the implementation work"
    ));
    assert!(prompt.contains("## Prior Stage Outputs This Cycle"));
    assert!(prompt.contains("payload-prompt-review"));
    assert!(prompt.contains("payload-planning"));
    assert!(prompt.contains("## Remediation / Pending Amendments"));
    assert!(prompt.contains("### Remediation Context"));
    assert!(prompt.contains("fix the failing edge case"));
    assert!(prompt.contains("### Pending Amendments"));
    assert!(prompt.contains("Tighten the validation copy"));
    assert!(prompt.contains("## Authoritative JSON Schema"));
    assert!(prompt.contains("\"data\""));
    assert!(prompt.contains("\"additionalProperties\": false"));

    let first_index = prompt.find("first-output").expect("first prior output");
    let second_index = prompt.find("second-output").expect("second prior output");
    assert!(
        first_index < second_index,
        "prior outputs should preserve journal order"
    );
}

#[tokio::test]
async fn build_stage_prompt_keeps_claude_prompt_schema_in_sync_with_transport_schema() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-builder-claude-schema-sync").unwrap();
    let run_id = RunId::new("run-20260314193209").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::PlanAndImplement, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::PlanAndImplement);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning, 20),
    ];
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        &render_bead_task_prompt(&sample_bead_context()),
        &events,
    );

    let artifact_store = InMemoryArtifactStore { payloads: vec![] };
    let prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Implementer,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    )
    .expect("build prompt");

    let request = InvocationRequest {
        invocation_id: "stage-schema-sync".to_owned(),
        project_root: project_root(base_dir, &project_id),
        working_dir: base_dir.to_path_buf(),
        contract: InvocationContract::Stage(contract),
        role: BackendRole::Implementer,
        resolved_target: ResolvedBackendTarget::new(BackendFamily::Claude, "claude-test"),
        payload: InvocationPayload {
            prompt: prompt.clone(),
            context: json!({ "kind": "stage" }),
        },
        timeout: Duration::from_secs(30),
        cancellation_token: CancellationToken::new(),
        session_policy: SessionPolicy::NewSession,
        prior_session: None,
        attempt_number: 1,
    };

    let bin_dir = tempdir().expect("bin dir");
    write_fake_claude(
        bin_dir.path(),
        r#"{"type":"result","session_id":"sess-stage","structured_output":{"__rb_wrapped":true,"data":{"change_summary":"ok","outstanding_risks":[],"steps":[{"description":"done","order":1,"status":"completed"}],"validation_evidence":[]}}}"#,
    );
    let adapter = ProcessBackendAdapter::with_search_paths(vec![bin_dir.path().to_path_buf()]);
    adapter.invoke(request).await.expect("invoke stage prompt");
    let prompt_schema = extract_prompt_schema(&prompt);
    let transport_schema: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(base_dir.join("claude-json-schema.json"))
            .expect("transport schema log"),
    )
    .expect("transport schema should parse");
    let stdin_schema = extract_stdin_schema(
        &fs::read_to_string(base_dir.join("claude-stdin.txt")).expect("stdin log"),
    );

    assert_eq!(prompt_schema, transport_schema);
    assert_eq!(stdin_schema, transport_schema);
}

#[test]
fn build_stage_prompt_omits_prior_outputs_section_when_current_cycle_has_no_completed_stages() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-builder-no-prior").unwrap();
    let run_id = RunId::new("run-20260314193204").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::Implementation, 2, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::Implementation);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning, 20),
        journal::stage_completed_event(
            3,
            Utc::now(),
            &run_id,
            StageId::Planning,
            1,
            1,
            "payload-cycle-one",
            "artifact-cycle-one",
        ),
    ];
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        "# Prompt\n\nOnly current-cycle outputs should appear.",
        &events,
    );

    let artifact_store = InMemoryArtifactStore {
        payloads: vec![PayloadRecord {
            payload_id: "payload-cycle-one".to_owned(),
            stage_id: StageId::Planning,
            cycle: 1,
            attempt: 1,
            created_at: Utc::now(),
            payload: json!({"problem_framing": "old-cycle"}),
            record_kind: RecordKind::StagePrimary,
            producer: None,
            completion_round: 0,
        }],
    };

    let prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Implementer,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    )
    .expect("build prompt");

    assert!(!prompt.contains("## Prior Stage Outputs This Cycle"));
    assert!(prompt.contains("Only current-cycle outputs should appear."));
}

#[test]
fn build_stage_prompt_surfaces_shared_bead_task_prompt_contract_guidance() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-builder-bead-contract").unwrap();
    let run_id = RunId::new("run-20260314193208").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::PlanAndImplement, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::PlanAndImplement);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning, 20),
    ];
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        &render_bead_task_prompt(&sample_bead_context()),
        &events,
    );

    let artifact_store = InMemoryArtifactStore { payloads: vec![] };
    let prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Implementer,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    )
    .expect("build prompt");

    assert!(prompt.contains("## Task Prompt Contract"));
    assert!(prompt.contains("bead_execution_prompt/1"));
    assert!(prompt.contains("## Must-Do Scope"));
    assert!(prompt.contains("## Already Planned Elsewhere"));
}

#[test]
fn build_stage_prompt_injects_scope_guidance_for_plan_and_implement() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-builder-scope-guidance-quickdev").unwrap();
    let run_id = RunId::new("run-20260314193210").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::PlanAndImplement, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::PlanAndImplement);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning, 20),
    ];
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        &render_bead_task_prompt(&sample_bead_context()),
        &events,
    );

    let artifact_store = InMemoryArtifactStore { payloads: vec![] };
    let prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Implementer,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    )
    .expect("build prompt");

    assert!(prompt.contains("## Scope Guidance"));
    assert!(prompt.contains(
        "Only include work that is required by `Must-Do Scope` and `Acceptance Criteria`."
    ));
    assert!(prompt.contains("Treat `Explicit Non-Goals` as out of scope."));
    assert!(prompt.contains("deferred work with a brief rationale"));
    assert!(prompt
        .contains("Use `Milestone Summary` and other milestone context as read-only background"));
    assert!(prompt.contains("Do not absorb work owned by `Already Planned Elsewhere`."));
}

#[test]
fn build_stage_prompt_injects_scope_guidance_for_planning_and_omits_it_for_generic_prompts() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-builder-scope-guidance-planning").unwrap();
    let run_id = RunId::new("run-20260314193211").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::Planning, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::Planning);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning, 20),
    ];
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        "# Project Prompt\n\nImplement the generic planning workflow.",
        &events,
    );

    let artifact_store = InMemoryArtifactStore { payloads: vec![] };
    let generic_prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Planner,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    )
    .expect("build generic prompt");

    assert!(!generic_prompt.contains("## Scope Guidance"));

    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        &render_bead_task_prompt(&sample_bead_context()),
        &events,
    );

    let scoped_prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Planner,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    )
    .expect("build scoped prompt");

    assert!(scoped_prompt.contains("## Scope Guidance"));
    assert!(scoped_prompt.contains(
        "Only include work that is required by `Must-Do Scope` and `Acceptance Criteria`."
    ));
}

#[test]
fn build_stage_prompt_injects_scope_guidance_into_legacy_plan_and_implement_override() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-builder-legacy-override").unwrap();
    let run_id = RunId::new("run-20260314193208").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::PlanAndImplement, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::PlanAndImplement);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning, 20),
    ];
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        &render_bead_task_prompt(&sample_bead_context()),
        &events,
    );

    let ws_templates = base_dir.join(".ralph-burning").join("templates");
    fs::create_dir_all(&ws_templates).expect("create templates dir");
    fs::write(
        ws_templates.join("plan_and_implement.md"),
        "LEGACY STAGE\n\n{{role_instruction}}\n\n{{project_prompt}}\n\n{{json_schema}}",
    )
    .expect("write override");

    let artifact_store = InMemoryArtifactStore { payloads: vec![] };
    let prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Implementer,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    )
    .expect("legacy override should still render");

    assert!(prompt.starts_with("LEGACY STAGE"));
    assert!(prompt.contains("Ralph Task Prompt"));
    assert!(prompt.contains("## Scope Guidance"));
    assert!(prompt.contains(
        "Only include work that is required by `Must-Do Scope` and `Acceptance Criteria`."
    ));
}

#[test]
fn build_stage_prompt_injects_scope_guidance_before_final_schema_in_legacy_override() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-builder-legacy-schema-collision").unwrap();
    let run_id = RunId::new("run-20260314193213").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::PlanAndImplement, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::PlanAndImplement);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning, 20),
    ];
    let mut context = sample_bead_context();
    context.agents_guidance = Some(
        "Keep changes inspectable.\n\n## Authoritative JSON Schema\n\nThis heading belongs to repo guidance.".to_owned(),
    );
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        &render_bead_task_prompt(&context),
        &events,
    );

    let ws_templates = base_dir.join(".ralph-burning").join("templates");
    fs::create_dir_all(&ws_templates).expect("create templates dir");
    fs::write(
        ws_templates.join("plan_and_implement.md"),
        "LEGACY STAGE\n\n{{role_instruction}}\n\n{{project_prompt}}\n\n## Authoritative JSON Schema\n\n```json\n{{json_schema}}\n```",
    )
    .expect("write override");

    let artifact_store = InMemoryArtifactStore { payloads: vec![] };
    let prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Implementer,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    )
    .expect("legacy override with schema heading should still render");

    let repo_guidance_index = prompt
        .find("This heading belongs to repo guidance.")
        .expect("repo guidance heading should remain inside project prompt");
    let scope_guidance_index = prompt
        .find("## Scope Guidance")
        .expect("scope guidance should be injected");
    let final_schema_index = prompt
        .rfind("## Authoritative JSON Schema")
        .expect("final schema heading should exist");

    assert!(
        repo_guidance_index < scope_guidance_index,
        "scope guidance should not be injected into the middle of project prompt content"
    );
    assert!(
        scope_guidance_index < final_schema_index,
        "scope guidance should still appear before the final schema section"
    );
}

#[test]
fn build_stage_prompt_keeps_generic_legacy_override_without_scope_guidance() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-builder-generic-legacy-override").unwrap();
    let run_id = RunId::new("run-20260314193209").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::Planning, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::Planning);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning, 20),
    ];
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        "Build a calculator.",
        &events,
    );

    let ws_templates = base_dir.join(".ralph-burning").join("templates");
    fs::create_dir_all(&ws_templates).expect("create templates dir");
    fs::write(
        ws_templates.join("planning.md"),
        "LEGACY STAGE\n\n{{role_instruction}}\n\n{{project_prompt}}\n\n{{json_schema}}",
    )
    .expect("write override");

    let artifact_store = InMemoryArtifactStore { payloads: vec![] };
    let prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Planner,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    )
    .expect("generic legacy override should still render");

    assert!(prompt.starts_with("LEGACY STAGE"));
    assert!(!prompt.contains("## Scope Guidance"));
}

#[test]
fn build_stage_prompt_skips_scope_guidance_for_marker_only_drifted_prompt() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-builder-marker-only-scope").unwrap();
    let run_id = RunId::new("run-20260314193212").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::Planning, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::Planning);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning, 20),
    ];
    let marker_only_prompt = format!(
        "# Drifted Prompt\n\n{}\n\n## Acceptance Criteria\n\nLater sections only.",
        task_prompt_contract::contract_marker()
    );
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        &marker_only_prompt,
        &events,
    );

    let artifact_store = InMemoryArtifactStore { payloads: vec![] };
    let prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Planner,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    )
    .expect("marker-only prompt should still render");

    assert!(prompt.contains("## Task Prompt Contract"));
    assert!(!prompt.contains("## Scope Guidance"));
}

#[test]
fn build_stage_prompt_excludes_rolled_back_prior_outputs() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-builder-rollback").unwrap();
    let run_id = RunId::new("run-20260314193207").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::Implementation, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::Implementation);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::PromptReview, 20),
        journal::stage_completed_event(
            3,
            Utc::now(),
            &run_id,
            StageId::PromptReview,
            1,
            1,
            "payload-visible-review",
            "artifact-visible-review",
        ),
        journal::stage_completed_event(
            4,
            Utc::now(),
            &run_id,
            StageId::Planning,
            1,
            1,
            "payload-rolled-back-planning",
            "artifact-rolled-back-planning",
        ),
        journal::rollback_performed_event(
            5,
            Utc::now(),
            "rb-1",
            StageId::Planning,
            1,
            3,
            false,
            None,
            1,
        ),
        journal::stage_completed_event(
            6,
            Utc::now(),
            &run_id,
            StageId::Planning,
            1,
            1,
            "payload-visible-planning",
            "artifact-visible-planning",
        ),
    ];
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        "# Prompt\n\nUse only visible branch outputs after rollback.",
        &events,
    );

    let artifact_store = InMemoryArtifactStore {
        payloads: vec![
            PayloadRecord {
                payload_id: "payload-visible-review".to_owned(),
                stage_id: StageId::PromptReview,
                cycle: 1,
                attempt: 1,
                created_at: Utc::now(),
                payload: json!({"problem_framing": "visible-review"}),
                record_kind: RecordKind::StagePrimary,
                producer: None,
                completion_round: 0,
            },
            PayloadRecord {
                payload_id: "payload-rolled-back-planning".to_owned(),
                stage_id: StageId::Planning,
                cycle: 1,
                attempt: 1,
                created_at: Utc::now(),
                payload: json!({"problem_framing": "rolled-back-branch"}),
                record_kind: RecordKind::StagePrimary,
                producer: None,
                completion_round: 0,
            },
            PayloadRecord {
                payload_id: "payload-visible-planning".to_owned(),
                stage_id: StageId::Planning,
                cycle: 1,
                attempt: 1,
                created_at: Utc::now(),
                payload: json!({"problem_framing": "visible-replacement"}),
                record_kind: RecordKind::StagePrimary,
                producer: None,
                completion_round: 0,
            },
        ],
    };

    let prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Implementer,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    )
    .expect("build prompt");

    assert!(prompt.contains("visible-review"));
    assert!(prompt.contains("visible-replacement"));
    assert!(
        !prompt.contains("rolled-back-branch"),
        "rolled-back branch output should be excluded from the prompt"
    );
}

#[test]
fn build_stage_prompt_omits_remediation_and_amendments_section_when_inputs_are_empty() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-builder-no-remediation").unwrap();
    let run_id = RunId::new("run-20260314193205").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::Qa, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::Qa);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning, 20),
        journal::stage_completed_event(
            3,
            Utc::now(),
            &run_id,
            StageId::Implementation,
            1,
            1,
            "payload-implementation",
            "artifact-implementation",
        ),
    ];
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        "# Prompt\n\nKeep remediation out when there is none.",
        &events,
    );

    let artifact_store = InMemoryArtifactStore {
        payloads: vec![PayloadRecord {
            payload_id: "payload-implementation".to_owned(),
            stage_id: StageId::Implementation,
            cycle: 1,
            attempt: 1,
            created_at: Utc::now(),
            payload: json!({"change_summary": "implemented"}),
            record_kind: RecordKind::StagePrimary,
            producer: None,
            completion_round: 0,
        }],
    };
    let no_amendments: Vec<QueuedAmendment> = Vec::new();

    let prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::QaValidator,
        &contract,
        &run_id,
        &cursor,
        None,
        Some(&no_amendments),
    )
    .expect("build prompt");

    assert!(prompt.contains("payload-implementation"));
    assert!(!prompt.contains("## Remediation / Pending Amendments"));
}

#[test]
fn build_stage_prompt_returns_diagnostic_error_when_journal_references_missing_payload() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-builder-missing-payload").unwrap();
    let run_id = RunId::new("run-20260314193206").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::Implementation, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::Implementation);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning, 20),
        journal::stage_completed_event(
            3,
            Utc::now(),
            &run_id,
            StageId::Planning,
            1,
            1,
            "payload-missing",
            "artifact-missing",
        ),
    ];
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        "# Prompt\n\nTrigger missing payload handling.",
        &events,
    );

    let artifact_store = InMemoryArtifactStore {
        payloads: Vec::new(),
    };
    let error = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Implementer,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    )
    .expect_err("missing payload should fail");

    match error {
        AppError::CorruptRecord { details, .. } => {
            assert!(
                details.contains("payload-missing"),
                "details should identify the missing payload: {details}"
            );
        }
        other => panic!("expected CorruptRecord, got {other:?}"),
    }
}

// ── Template override regression tests ──────────────────────────────────

#[test]
fn build_stage_prompt_with_workspace_template_override() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-override").unwrap();
    let run_id = RunId::new("run-20260319120000").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::Planning, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::Planning);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning, 20),
    ];
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        "Build a calculator.",
        &events,
    );

    // Install a workspace template override for "planning"
    let ws_templates = base_dir.join(".ralph-burning").join("templates");
    fs::create_dir_all(&ws_templates).expect("create templates dir");
    fs::write(
        ws_templates.join("planning.md"),
        "CUSTOM PLANNING\n\nRole: {{role_instruction}}\nContract: {{task_prompt_contract}}\nPrompt: {{project_prompt}}\nSchema: {{json_schema}}",
    )
    .expect("write override");

    let artifact_store = InMemoryArtifactStore { payloads: vec![] };

    let prompt = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Planner,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    )
    .expect("build prompt with override");

    assert!(
        prompt.starts_with("CUSTOM PLANNING"),
        "override should be used"
    );
    assert!(prompt.contains("Build a calculator."));
    assert!(prompt.contains("You are the Planner."));
}

#[test]
fn build_stage_prompt_fails_on_malformed_workspace_override() {
    let temp_dir = tempdir().expect("create temp dir");
    let base_dir = temp_dir.path();
    let project_id = ProjectId::new("prompt-malformed").unwrap();
    let run_id = RunId::new("run-20260319120001").unwrap();
    let prompt_reference = "prompt.md";
    let cursor = StageCursor::new(StageId::Planning, 1, 1, 1).unwrap();
    let contract = contract_for_stage(StageId::Planning);

    let events = vec![
        project_created_event(&project_id),
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning, 20),
    ];
    write_prompt_fixture(
        base_dir,
        &project_id,
        prompt_reference,
        "Build something.",
        &events,
    );

    // Install a malformed override (missing required placeholders)
    let ws_templates = base_dir.join(".ralph-burning").join("templates");
    fs::create_dir_all(&ws_templates).expect("create templates dir");
    fs::write(
        ws_templates.join("planning.md"),
        "This template has no placeholders.",
    )
    .expect("write malformed override");

    let artifact_store = InMemoryArtifactStore { payloads: vec![] };

    let result = build_stage_prompt(
        &artifact_store,
        base_dir,
        &project_id,
        &project_root(base_dir, &project_id),
        prompt_reference,
        BackendFamily::Claude,
        BackendRole::Planner,
        &contract,
        &run_id,
        &cursor,
        None,
        None,
    );

    assert!(result.is_err(), "malformed override should cause failure");
    let err = result.unwrap_err().to_string();
    assert!(err.contains("malformed template override"));
}
