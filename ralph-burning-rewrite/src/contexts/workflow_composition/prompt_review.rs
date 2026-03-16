#![forbid(unsafe_code)]

//! Prompt-review refiner-plus-validator orchestration.
//!
//! Implements the prompt-review panel workflow:
//! 1. Run the refiner to produce a refined prompt.
//! 2. Persist the refinement result as `StageSupporting`.
//! 3. Run validators against the refined prompt.
//! 4. Persist each validator result as `StageSupporting`.
//! 5. If any validator rejects, prompt review fails.
//! 6. If executed validators < `min_reviewers`, prompt review fails.
//! 7. On success: persist `prompt.original.md`, replace `prompt.md`,
//!    recompute prompt hash, and persist one canonical `StagePrimary` record.

use std::path::Path;

use chrono::Utc;
use serde_json::Value;

use std::time::Duration;

use crate::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use crate::contexts::agent_execution::policy::PromptReviewPanelResolution;
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::AgentExecutionService;
use crate::contexts::project_run_record::model::{ArtifactRecord, PayloadRecord};
use crate::contexts::project_run_record::service::{PayloadArtifactWritePort, RuntimeLogWritePort};
use crate::contexts::workflow_composition::panel_contracts::{
    PromptRefinementPayload, PromptReviewDecision,
    PromptReviewPrimaryPayload, PromptValidationPayload, RecordKind, RecordProducer,
};
use crate::contexts::workflow_composition::renderers;
use crate::shared::domain::{
    BackendFamily, BackendRole, ProjectId, ResolvedBackendTarget, RunId, SessionPolicy, StageCursor,
    StageId,
};
use crate::shared::error::{AppError, AppResult};

/// Result of a successful prompt review execution.
pub struct PromptReviewResult {
    /// The primary payload for journal persistence.
    pub primary_payload: Value,
    /// The primary artifact text.
    pub primary_artifact: String,
    /// The original prompt text (before refinement) for writing prompt.original.md.
    pub original_prompt: String,
    /// The refined prompt text for replacing prompt.md.
    pub refined_prompt: String,
}

/// Execute the prompt-review panel workflow.
///
/// Returns `Ok(PromptReviewResult)` on success (all validators accept, min_reviewers met).
/// Returns `Err` on failure. Supporting records are already persisted regardless of outcome.
#[allow(clippy::too_many_arguments)]
pub async fn execute_prompt_review<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    artifact_write: &dyn PayloadArtifactWritePort,
    _log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_root: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    cursor: &StageCursor,
    panel: &PromptReviewPanelResolution,
    min_reviewers: usize,
    prompt_reference: &str,
    rollback_count: u32,
    refiner_timeout: Duration,
    timeout_for_backend: &dyn Fn(BackendFamily) -> Duration,
    cancellation_token: CancellationToken,
) -> AppResult<PromptReviewResult>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    let stage_id = StageId::PromptReview;
    let prompt_path = project_root.join(prompt_reference);
    let original_prompt =
        std::fs::read_to_string(&prompt_path).map_err(|e| AppError::CorruptRecord {
            file: prompt_path.display().to_string(),
            details: format!("failed to read prompt for review: {e}"),
        })?;

    // ── Step 1: Run the refiner ────────────────────────────────────────────
    let refiner_target = &panel.refiner;
    let refinement_payload = invoke_panel_member(
        agent_service,
        base_dir,
        project_root,
        run_id,
        stage_id,
        cursor,
        refiner_target,
        &original_prompt,
        "refiner",
        refiner_timeout,
        cancellation_token.clone(),
    )
    .await?;

    let refinement: PromptRefinementPayload =
        serde_json::from_value(refinement_payload.clone()).map_err(|e| {
            AppError::InvocationFailed {
                backend: refiner_target.backend.family.to_string(),
                contract_id: "prompt_review:refiner".to_owned(),
                failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                details: format!("refiner output schema validation failed: {e}"),
            }
        })?;

    // ── Step 2: Persist refinement as StageSupporting ───────────────────────
    let refiner_producer = RecordProducer::Agent {
        backend_family: refiner_target.backend.family.to_string(),
        model_id: refiner_target.model.model_id.clone(),
    };
    let refiner_artifact =
        renderers::render_prompt_refinement(stage_id, &refinement, &refiner_producer.to_string());
    persist_supporting_record(
        artifact_write,
        base_dir,
        project_id,
        run_id,
        cursor,
        stage_id,
        rollback_count,
        &refinement_payload,
        &refiner_artifact,
        refiner_producer,
        "refinement",
    )?;

    // ── Step 3: Invoke validators ──────────────────────────────────────────
    // Availability filtering is done by the engine before snapshot
    // persistence; all validators in the panel are available and expected
    // to execute. Any invocation error is propagated directly.
    let mut executed_count = 0usize;
    let mut accept_count = 0usize;
    let mut reject_count = 0usize;

    for (i, member) in panel.validators.iter().enumerate() {
        let validator_target = &member.target;
        let validator_timeout = timeout_for_backend(validator_target.backend.family);
        let validation_payload = invoke_panel_member(
            agent_service,
            base_dir,
            project_root,
            run_id,
            stage_id,
            cursor,
            validator_target,
            &refinement.refined_prompt,
            "validator",
            validator_timeout,
            cancellation_token.clone(),
        )
        .await?;

        let validation: PromptValidationPayload =
            serde_json::from_value(validation_payload.clone()).map_err(|e| {
                AppError::InvocationFailed {
                    backend: validator_target.backend.family.to_string(),
                    contract_id: "prompt_review:validator".to_owned(),
                    failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                    details: format!("validator output schema validation failed: {e}"),
                }
            })?;

        let validator_producer = RecordProducer::Agent {
            backend_family: validator_target.backend.family.to_string(),
            model_id: validator_target.model.model_id.clone(),
        };
        let validator_artifact = renderers::render_prompt_validation(
            stage_id,
            &validation,
            &validator_producer.to_string(),
        );
        persist_supporting_record(
            artifact_write,
            base_dir,
            project_id,
            run_id,
            cursor,
            stage_id,
            rollback_count,
            &validation_payload,
            &validator_artifact,
            validator_producer,
            &format!("validator-{i}"),
        )?;

        executed_count += 1;
        if validation.accepted {
            accept_count += 1;
        } else {
            reject_count += 1;
        }
    }

    // ── Step 4: Enforce min_reviewers ──────────────────────────────────────
    if executed_count < min_reviewers {
        return Err(AppError::InsufficientPanelMembers {
            panel: "prompt_review".to_owned(),
            resolved: executed_count,
            minimum: min_reviewers,
        });
    }

    // ── Step 5: Check acceptance ──────────────────────────────────────────
    if reject_count > 0 {
        // Failure: supporting records already persisted as durable evidence.
        return Err(AppError::PromptReviewRejected {
            details: format!(
                "{reject_count} of {executed_count} validators rejected the refined prompt"
            ),
        });
    }

    // ── Step 6: Success path ──────────────────────────────────────────────
    // Do NOT mutate prompt files here. Return the texts so the caller can
    // write prompt.original.md / prompt.md AFTER the primary record and
    // stage_completed have been durably committed. This prevents file
    // mutations when the subsequent journal append fails.
    let refined = refinement.refined_prompt.clone();
    let primary = PromptReviewPrimaryPayload {
        decision: PromptReviewDecision::Accepted,
        refined_prompt: refinement.refined_prompt,
        executed_reviewers: executed_count,
        accept_count,
        reject_count,
        refinement_summary: refinement.refinement_summary,
    };

    let primary_payload = serde_json::to_value(&primary)?;
    let primary_artifact = renderers::render_prompt_review_decision(stage_id, &primary);

    Ok(PromptReviewResult {
        primary_payload,
        primary_artifact,
        original_prompt,
        refined_prompt: refined,
    })
}

/// Invoke a single panel member (refiner or validator) and return the raw parsed payload.
#[allow(clippy::too_many_arguments)]
async fn invoke_panel_member<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    _base_dir: &Path,
    project_root: &Path,
    run_id: &RunId,
    stage_id: StageId,
    cursor: &StageCursor,
    target: &ResolvedBackendTarget,
    prompt_text: &str,
    role_label: &str,
    timeout: Duration,
    cancellation_token: CancellationToken,
) -> AppResult<Value>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    let invocation_id = format!(
        "{}-{}-{role_label}-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );

    let schema = super::panel_contracts::panel_json_schema(stage_id, role_label);

    let prompt = format!(
        "# Prompt Review: {role_label}\n\n## Prompt to Review\n\n{prompt_text}\n\n## JSON Schema\n\n```json\n{}\n```",
        serde_json::to_string_pretty(&schema)?
    );

    let request = InvocationRequest {
        invocation_id,
        project_root: project_root.to_path_buf(),
        working_dir: project_root.to_path_buf(),
        contract: InvocationContract::Panel {
            stage_id,
            role: role_label.to_owned(),
        },
        role: BackendRole::Planner,
        resolved_target: target.clone(),
        payload: InvocationPayload {
            prompt,
            context: Value::Null,
        },
        timeout,
        cancellation_token,
        session_policy: SessionPolicy::NewSession,
        prior_session: None,
        attempt_number: cursor.attempt,
    };

    let envelope = agent_service.invoke(request).await?;
    Ok(envelope.parsed_payload)
}

/// Persist a supporting record (refinement or validation result).
#[allow(clippy::too_many_arguments)]
fn persist_supporting_record(
    artifact_write: &dyn PayloadArtifactWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    cursor: &StageCursor,
    stage_id: StageId,
    rollback_count: u32,
    payload: &Value,
    artifact_content: &str,
    producer: RecordProducer,
    suffix: &str,
) -> AppResult<()> {
    let now = Utc::now();
    let base_id = format!(
        "{}-{}-{suffix}-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );
    let payload_id = if rollback_count == 0 {
        format!("{base_id}-payload")
    } else {
        format!("{base_id}-rb{rollback_count}-payload")
    };
    let artifact_id = if rollback_count == 0 {
        format!("{base_id}-artifact")
    } else {
        format!("{base_id}-rb{rollback_count}-artifact")
    };

    let payload_record = PayloadRecord {
        payload_id: payload_id.clone(),
        stage_id,
        cycle: cursor.cycle,
        attempt: cursor.attempt,
        created_at: now,
        payload: payload.clone(),
        record_kind: RecordKind::StageSupporting,
        producer: Some(producer.clone()),
        completion_round: cursor.completion_round,
    };

    let artifact_record = ArtifactRecord {
        artifact_id,
        payload_id,
        stage_id,
        created_at: now,
        content: artifact_content.to_owned(),
        record_kind: RecordKind::StageSupporting,
        producer: Some(producer),
        completion_round: cursor.completion_round,
    };

    artifact_write.write_payload_artifact_pair(base_dir, project_id, &payload_record, &artifact_record)
}
