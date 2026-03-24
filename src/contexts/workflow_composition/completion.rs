#![forbid(unsafe_code)]

//! Completion panel orchestration, consensus math, and aggregate persistence.
//!
//! Implements the completion panel workflow:
//! 1. Resolve completer panel backends.
//! 2. Invoke every resolved completer.
//! 3. Persist each completer response as `StageSupporting`.
//! 4. Compute aggregate verdict: `complete` only when
//!    `complete_votes >= min_completers` AND
//!    `complete_votes / total_voters >= consensus_threshold`.
//! 5. Persist the aggregate as canonical `StageAggregate`.
//! 6. If `complete`, advance to acceptance_qa.
//! 7. Otherwise reopen work (advance completion_round, return to planning).

use std::path::Path;
use std::time::Duration;

use chrono::Utc;
use serde_json::Value;

use crate::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use crate::contexts::agent_execution::policy::ResolvedPanelMember;
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::AgentExecutionService;
use crate::contexts::project_run_record::model::{ArtifactRecord, PayloadRecord};
use crate::contexts::project_run_record::service::{PayloadArtifactWritePort, RuntimeLogWritePort};
use crate::contexts::workflow_composition::panel_contracts::{
    CompletionAggregatePayload, CompletionVerdict, CompletionVotePayload, RecordKind,
    RecordProducer,
};
use crate::contexts::workflow_composition::renderers;
use crate::contexts::workspace_governance::template_catalog;
use crate::shared::domain::{
    BackendFamily, BackendRole, ProjectId, ResolvedBackendTarget, RunId, SessionPolicy,
    StageCursor, StageId,
};
use crate::shared::error::{AppError, AppResult};

/// Result of a completion panel execution.
pub struct CompletionResult {
    /// The aggregate payload for journal persistence.
    pub aggregate_payload: Value,
    /// The aggregate artifact text.
    pub aggregate_artifact: String,
    /// The computed verdict.
    pub verdict: CompletionVerdict,
}

/// Compute the completion verdict from vote counts and thresholds.
pub fn compute_completion_verdict(
    complete_votes: usize,
    total_voters: usize,
    min_completers: usize,
    consensus_threshold: f64,
) -> CompletionVerdict {
    if total_voters == 0 {
        return CompletionVerdict::ContinueWork;
    }

    let ratio = complete_votes as f64 / total_voters as f64;
    if complete_votes >= min_completers && ratio >= consensus_threshold {
        CompletionVerdict::Complete
    } else {
        CompletionVerdict::ContinueWork
    }
}

/// Execute the completion panel workflow.
///
/// Invokes all completers, persists supporting records, computes the aggregate,
/// and returns the result. The caller is responsible for persisting the aggregate
/// record and transitioning the run state.
#[allow(clippy::too_many_arguments)]
pub async fn execute_completion_panel<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    artifact_write: &dyn PayloadArtifactWritePort,
    _log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_root: &Path,
    backend_working_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    cursor: &StageCursor,
    completers: &[ResolvedPanelMember],
    min_completers: usize,
    consensus_threshold: f64,
    prompt_reference: &str,
    rollback_count: u32,
    timeout_for_backend: &dyn Fn(BackendFamily) -> Duration,
    cancellation_token: CancellationToken,
) -> AppResult<CompletionResult>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    let stage_id = StageId::CompletionPanel;
    let prompt_path = project_root.join(prompt_reference);
    let project_prompt =
        std::fs::read_to_string(&prompt_path).map_err(|e| AppError::CorruptRecord {
            file: prompt_path.display().to_string(),
            details: format!("failed to read prompt for completion: {e}"),
        })?;

    // ── Invoke completers and persist supporting records ──────────────────
    // Availability filtering is done by the engine before snapshot
    // persistence; all completers in the panel are available and expected
    // to execute. Any invocation error is propagated directly.
    let mut complete_votes = 0usize;
    let mut continue_votes = 0usize;
    let mut executed_voters: Vec<String> = Vec::new();

    for (i, member) in completers.iter().enumerate() {
        let completer_target = &member.target;
        let completer_timeout = timeout_for_backend(completer_target.backend.family);
        let (vote_payload, producer) = invoke_completer(
            agent_service,
            base_dir,
            project_root,
            backend_working_dir,
            run_id,
            stage_id,
            cursor,
            completer_target,
            &project_prompt,
            i,
            completer_timeout,
            cancellation_token.clone(),
            Some(project_id),
        )
        .await?;

        let vote: CompletionVotePayload =
            serde_json::from_value(vote_payload.clone()).map_err(|e| {
                AppError::InvocationFailed {
                    backend: completer_target.backend.family.to_string(),
                    contract_id: "completion:completer".to_owned(),
                    failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                    details: format!("completer output schema validation failed: {e}"),
                }
            })?;

        let (backend_family, model_id) = super::require_agent_record_producer(
            &producer,
            completer_target.backend.family.as_str(),
            "completion:completer",
            "completion panel invocations must produce agent metadata",
        )?;
        let voter_id = format!("{backend_family}:{model_id}");
        let vote_artifact =
            renderers::render_completion_vote(stage_id, &vote, &producer.to_string());

        persist_supporting_record(
            artifact_write,
            base_dir,
            project_id,
            run_id,
            cursor,
            stage_id,
            rollback_count,
            &vote_payload,
            &vote_artifact,
            producer,
            &format!("completer-{i}"),
        )?;

        executed_voters.push(voter_id);
        if vote.vote_complete {
            complete_votes += 1;
        } else {
            continue_votes += 1;
        }
    }

    // ── Check min_completers after execution ──────────────────────────────
    let total_voters = executed_voters.len();
    if total_voters < min_completers {
        return Err(AppError::InsufficientPanelMembers {
            panel: "completion".to_owned(),
            resolved: total_voters,
            minimum: min_completers,
        });
    }

    // ── Compute aggregate verdict ─────────────────────────────────────────
    let verdict = compute_completion_verdict(
        complete_votes,
        total_voters,
        min_completers,
        consensus_threshold,
    );

    let aggregate = CompletionAggregatePayload {
        verdict,
        complete_votes,
        continue_votes,
        total_voters,
        consensus_threshold,
        min_completers,
        executed_voters,
    };

    let aggregate_payload = serde_json::to_value(&aggregate)?;
    let aggregate_artifact = renderers::render_completion_aggregate(stage_id, &aggregate);

    Ok(CompletionResult {
        aggregate_payload,
        aggregate_artifact,
        verdict,
    })
}

/// Invoke a single completer and return the raw parsed payload plus producer.
#[allow(clippy::too_many_arguments)]
async fn invoke_completer<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    base_dir: &Path,
    project_root: &Path,
    backend_working_dir: &Path,
    run_id: &RunId,
    stage_id: StageId,
    cursor: &StageCursor,
    target: &ResolvedBackendTarget,
    prompt_text: &str,
    index: usize,
    timeout: Duration,
    cancellation_token: CancellationToken,
    project_id: Option<&ProjectId>,
) -> AppResult<(Value, RecordProducer)>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    let invocation_id = format!(
        "{}-{}-completer-{index}-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );

    let schema = super::panel_contracts::panel_json_schema(stage_id, "completer");
    let schema_str = serde_json::to_string_pretty(&schema)?;

    let prompt = template_catalog::resolve_and_render(
        "completion_panel_completer",
        base_dir,
        project_id,
        &[("prompt_text", prompt_text), ("json_schema", &schema_str)],
    )?;

    let request = InvocationRequest {
        invocation_id,
        project_root: project_root.to_path_buf(),
        working_dir: backend_working_dir.to_path_buf(),
        contract: InvocationContract::Panel {
            stage_id,
            role: "completer".to_owned(),
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
    let producer = super::agent_record_producer(&envelope.metadata);
    Ok((envelope.parsed_payload, producer))
}

/// Persist a completion supporting record.
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

    artifact_write.write_payload_artifact_pair(
        base_dir,
        project_id,
        &payload_record,
        &artifact_record,
    )
}

/// Persist the aggregate record for the completion panel.
#[allow(clippy::too_many_arguments)]
pub fn persist_aggregate_record(
    artifact_write: &dyn PayloadArtifactWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    cursor: &StageCursor,
    stage_id: StageId,
    rollback_count: u32,
    payload: &Value,
    artifact_content: &str,
) -> AppResult<()> {
    let now = Utc::now();
    let base_id = format!(
        "{}-{}-aggregate-c{}-a{}-cr{}",
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

    let producer = RecordProducer::System {
        component: "completion_aggregator".to_owned(),
    };

    let payload_record = PayloadRecord {
        payload_id: payload_id.clone(),
        stage_id,
        cycle: cursor.cycle,
        attempt: cursor.attempt,
        created_at: now,
        payload: payload.clone(),
        record_kind: RecordKind::StageAggregate,
        producer: Some(producer.clone()),
        completion_round: cursor.completion_round,
    };

    let artifact_record = ArtifactRecord {
        artifact_id,
        payload_id,
        stage_id,
        created_at: now,
        content: artifact_content.to_owned(),
        record_kind: RecordKind::StageAggregate,
        producer: Some(producer),
        completion_round: cursor.completion_round,
    };

    artifact_write.write_payload_artifact_pair(
        base_dir,
        project_id,
        &payload_record,
        &artifact_record,
    )
}
