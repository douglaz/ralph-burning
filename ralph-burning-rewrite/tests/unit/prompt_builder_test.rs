use std::fs;
use std::path::{Path, PathBuf};

use chrono::Utc;
use serde_json::json;
use tempfile::tempdir;

use ralph_burning::contexts::agent_execution::model::InvocationContract;
use ralph_burning::contexts::project_run_record::journal;
use ralph_burning::contexts::project_run_record::model::{
    ArtifactRecord, JournalEvent, JournalEventType, PayloadRecord, QueuedAmendment,
};
use ralph_burning::contexts::project_run_record::service::ArtifactStorePort;
use ralph_burning::contexts::workflow_composition::contracts::contract_for_stage;
use ralph_burning::contexts::workflow_composition::engine::build_stage_prompt;
use ralph_burning::contexts::workflow_composition::panel_contracts::RecordKind;
use ralph_burning::shared::domain::{
    BackendRole, FlowPreset, ProjectId, RunId, StageCursor, StageId,
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
        journal::run_started_event(2, Utc::now(), &run_id, StageId::PromptReview),
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
    assert!(prompt.contains(
        &serde_json::to_string_pretty(&InvocationContract::Stage(contract).json_schema_value())
            .expect("serialize schema"),
    ));

    let first_index = prompt.find("first-output").expect("first prior output");
    let second_index = prompt.find("second-output").expect("second prior output");
    assert!(
        first_index < second_index,
        "prior outputs should preserve journal order"
    );
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
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning),
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
        journal::run_started_event(2, Utc::now(), &run_id, StageId::PromptReview),
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
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning),
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
        journal::run_started_event(2, Utc::now(), &run_id, StageId::Planning),
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
