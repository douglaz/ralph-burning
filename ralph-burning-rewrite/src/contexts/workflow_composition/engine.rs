use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use crate::contexts::agent_execution::model::{
    CancellationToken, InvocationPayload, InvocationRequest,
};
use crate::contexts::agent_execution::service::{
    AgentExecutionPort, BackendSelectionConfig, RawOutputPort,
};
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::AgentExecutionService;
use crate::contexts::project_run_record::journal;
use crate::contexts::project_run_record::model::{
    ActiveRun, ArtifactRecord, CycleHistoryEntry, JournalEvent, LogLevel, PayloadRecord,
    RunSnapshot, RunStatus, RuntimeLogEntry,
};
use crate::contexts::project_run_record::service::{
    JournalStorePort, PayloadArtifactWritePort, RunSnapshotPort, RunSnapshotWritePort,
    RuntimeLogWritePort,
};
use crate::contexts::workflow_composition::payloads::{ReviewOutcome, StagePayload};
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::shared::domain::{
    BackendRole, FailureClass, FlowPreset, ProjectId, ResolvedBackendTarget, RunId, SessionPolicy,
    StageCursor, StageId,
};
use crate::shared::error::{AppError, AppResult};

use super::contracts::{self, ValidatedBundle};
use super::retry_policy::RetryPolicy;

/// Derives the executable stage plan for the standard flow given prompt_review config.
pub fn standard_stage_plan(prompt_review_enabled: bool) -> Vec<StageId> {
    let flow_def = super::flow_definition(FlowPreset::Standard);
    if prompt_review_enabled {
        flow_def.stages.to_vec()
    } else {
        flow_def
            .stages
            .iter()
            .copied()
            .filter(|s| *s != StageId::PromptReview)
            .collect()
    }
}

/// Deterministic stage-to-role mapping per spec.
pub fn role_for_stage(stage_id: StageId) -> BackendRole {
    BackendRole::for_stage(stage_id)
}

/// Resolved target per stage for preflight.
pub struct StagePlan {
    pub stage_id: StageId,
    pub role: BackendRole,
    pub contract: contracts::StageContract,
    pub target: ResolvedBackendTarget,
}

/// Resolve all stage targets ahead of execution for preflight validation.
pub fn resolve_stage_plan(
    stages: &[StageId],
    resolver: &crate::contexts::agent_execution::service::BackendResolver,
    workspace_defaults: Option<&BackendSelectionConfig>,
) -> AppResult<Vec<StagePlan>> {
    let mut plan = Vec::with_capacity(stages.len());
    for &stage_id in stages {
        let role = role_for_stage(stage_id);
        let contract = contracts::contract_for_stage(stage_id);
        let target = resolver.resolve(role, None, None, workspace_defaults)?;
        plan.push(StagePlan {
            stage_id,
            role,
            contract,
            target,
        });
    }
    Ok(plan)
}

/// Preflight: check capability and availability for every stage target.
pub async fn preflight_check<A: AgentExecutionPort>(
    adapter: &A,
    plan: &[StagePlan],
) -> AppResult<()> {
    for entry in plan {
        adapter
            .check_capability(&entry.target, &entry.contract)
            .await
            .map_err(|e| AppError::PreflightFailed {
                stage_id: entry.stage_id,
                details: e.to_string(),
            })?;
        adapter
            .check_availability(&entry.target)
            .await
            .map_err(|e| AppError::PreflightFailed {
                stage_id: entry.stage_id,
                details: e.to_string(),
            })?;
    }
    Ok(())
}

/// Generate a new run ID from a timestamp.
fn generate_run_id() -> AppResult<RunId> {
    let now = Utc::now();
    RunId::new(format!("run-{}", now.format("%Y%m%d%H%M%S")))
}

#[derive(Debug, Clone, Copy)]
enum ExecutionOrigin {
    Start,
    Resume,
}

impl ExecutionOrigin {
    fn error(self, reason: String) -> AppError {
        match self {
            Self::Start => AppError::RunStartFailed { reason },
            Self::Resume => AppError::ResumeFailed { reason },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunOutcome {
    Completed,
    Paused,
}

#[derive(Debug)]
struct ResumeState {
    run_id: RunId,
    started_at: DateTime<Utc>,
    stage_index: usize,
    cursor: StageCursor,
}

/// The standard-flow orchestration engine.
#[allow(clippy::too_many_arguments)]
pub async fn execute_standard_run<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_read: &dyn RunSnapshotPort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    effective_config: &EffectiveConfig,
) -> AppResult<()>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    execute_standard_run_with_retry(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_write,
        log_write,
        base_dir,
        project_id,
        effective_config,
        &RetryPolicy::default_policy(),
        CancellationToken::new(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_standard_run_with_retry<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_read: &dyn RunSnapshotPort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    effective_config: &EffectiveConfig,
    retry_policy: &RetryPolicy,
    cancellation_token: CancellationToken,
) -> AppResult<()>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let snapshot = run_snapshot_read.read_run_snapshot(base_dir, project_id)?;

    match snapshot.status {
        RunStatus::NotStarted => {}
        RunStatus::Failed | RunStatus::Paused => {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "project snapshot status is '{}'; use `ralph-burning run resume`",
                    snapshot.status
                ),
            });
        }
        status => {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "project snapshot status is '{}'; run start requires 'not_started'",
                    status
                ),
            });
        }
    }
    if snapshot.has_active_run() {
        return Err(AppError::RunStartFailed {
            reason: "project already has an active run".to_owned(),
        });
    }

    let stage_ids = standard_stage_plan(effective_config.prompt_review_enabled());
    let workspace_defaults = BackendSelectionConfig::from_effective_config(effective_config)?;
    let stage_plan = resolve_stage_plan(
        stage_ids.as_slice(),
        agent_service.resolver(),
        Some(&workspace_defaults),
    )?;
    preflight_check(agent_service.adapter(), &stage_plan).await?;

    let run_id = generate_run_id()?;
    let now = Utc::now();
    let events = journal_store.read_journal(base_dir, project_id)?;
    let mut seq = journal::last_sequence(&events);
    let first_stage = stage_plan[0].stage_id;
    let initial_cursor = StageCursor::initial(first_stage);
    let mut current_snapshot = RunSnapshot {
        active_run: Some(ActiveRun {
            run_id: run_id.as_str().to_owned(),
            stage_cursor: initial_cursor.clone(),
            started_at: now,
        }),
        status: RunStatus::Running,
        cycle_history: snapshot.cycle_history.clone(),
        completion_rounds: 1,
        rollback_point_meta: snapshot.rollback_point_meta.clone(),
        amendment_queue: snapshot.amendment_queue.clone(),
        status_summary: format!("running: {}", first_stage.display_name()),
    };
    run_snapshot_write.write_run_snapshot(base_dir, project_id, &current_snapshot)?;

    seq += 1;
    let run_started = journal::run_started_event(seq, now, &run_id, first_stage);
    let run_started_line = journal::serialize_event(&run_started)?;
    if let Err(error) = journal_store.append_event(base_dir, project_id, &run_started_line) {
        seq -= 1;
        return fail_run(
            &AppError::RunStartFailed {
                reason: format!("failed to persist run_started event: {}", error),
            },
            first_stage,
            &run_id,
            &mut seq,
            &mut current_snapshot,
            journal_store,
            run_snapshot_write,
            base_dir,
            project_id,
            ExecutionOrigin::Start,
        )
        .await;
    }

    execute_standard_run_internal(
        agent_service,
        run_snapshot_write,
        journal_store,
        artifact_write,
        log_write,
        base_dir,
        project_id,
        &run_id,
        &mut seq,
        &mut current_snapshot,
        &stage_plan,
        0,
        initial_cursor,
        retry_policy,
        cancellation_token,
        ExecutionOrigin::Start,
        None,
    )
    .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn resume_standard_run<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_read: &dyn RunSnapshotPort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    effective_config: &EffectiveConfig,
) -> AppResult<()>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    resume_standard_run_with_retry(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_write,
        log_write,
        base_dir,
        project_id,
        effective_config,
        &RetryPolicy::default_policy(),
        CancellationToken::new(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn resume_standard_run_with_retry<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_read: &dyn RunSnapshotPort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    effective_config: &EffectiveConfig,
    retry_policy: &RetryPolicy,
    cancellation_token: CancellationToken,
) -> AppResult<()>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let mut snapshot = run_snapshot_read.read_run_snapshot(base_dir, project_id)?;
    match snapshot.status {
        RunStatus::Failed | RunStatus::Paused => {}
        RunStatus::NotStarted => {
            return Err(AppError::ResumeFailed {
                reason: "project has not started a run yet; use `ralph-burning run start`"
                    .to_owned(),
            });
        }
        RunStatus::Running => {
            return Err(AppError::ResumeFailed {
                reason: "project already has a running run; `run resume` only works from failed or paused snapshots".to_owned(),
            });
        }
        RunStatus::Completed => {
            return Err(AppError::ResumeFailed {
                reason: "project is already completed; there is nothing to resume".to_owned(),
            });
        }
    }
    if snapshot.has_active_run() {
        return Err(AppError::ResumeFailed {
            reason: "failed or paused snapshots must not retain an active run".to_owned(),
        });
    }

    let events = journal_store.read_journal(base_dir, project_id)?;
    let stage_ids = standard_stage_plan_for_resume(&events, effective_config)?;
    let workspace_defaults = BackendSelectionConfig::from_effective_config(effective_config)?;
    let stage_plan = resolve_stage_plan(
        stage_ids.as_slice(),
        agent_service.resolver(),
        Some(&workspace_defaults),
    )?;
    let resume_state = derive_resume_state(&events, &snapshot, &stage_plan)?;

    preflight_check(
        agent_service.adapter(),
        &stage_plan[resume_state.stage_index..],
    )
    .await
    .map_err(|error| AppError::ResumeFailed {
        reason: error.to_string(),
    })?;

    let mut seq = journal::last_sequence(&events);
    snapshot.status = RunStatus::Running;
    snapshot.active_run = Some(ActiveRun {
        run_id: resume_state.run_id.as_str().to_owned(),
        stage_cursor: resume_state.cursor.clone(),
        started_at: resume_state.started_at,
    });
    snapshot.completion_rounds = snapshot
        .completion_rounds
        .max(resume_state.cursor.completion_round);
    snapshot.status_summary = format!("running: {}", resume_state.cursor.stage.display_name());
    run_snapshot_write.write_run_snapshot(base_dir, project_id, &snapshot)?;

    seq += 1;
    let run_resumed = journal::run_resumed_event(
        seq,
        Utc::now(),
        &resume_state.run_id,
        resume_state.cursor.stage,
        resume_state.cursor.cycle,
    );
    let run_resumed_line = journal::serialize_event(&run_resumed)?;
    if let Err(error) = journal_store.append_event(base_dir, project_id, &run_resumed_line) {
        seq -= 1;
        return fail_run(
            &AppError::ResumeFailed {
                reason: format!("failed to persist run_resumed event: {}", error),
            },
            resume_state.cursor.stage,
            &resume_state.run_id,
            &mut seq,
            &mut snapshot,
            journal_store,
            run_snapshot_write,
            base_dir,
            project_id,
            ExecutionOrigin::Resume,
        )
        .await;
    }

    execute_standard_run_internal(
        agent_service,
        run_snapshot_write,
        journal_store,
        artifact_write,
        log_write,
        base_dir,
        project_id,
        &resume_state.run_id,
        &mut seq,
        &mut snapshot,
        &stage_plan,
        resume_state.stage_index,
        resume_state.cursor,
        retry_policy,
        cancellation_token,
        ExecutionOrigin::Resume,
        None,
    )
    .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn execute_standard_run_internal<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &mut RunSnapshot,
    stage_plan: &[StagePlan],
    start_stage_index: usize,
    start_cursor: StageCursor,
    retry_policy: &RetryPolicy,
    cancellation_token: CancellationToken,
    origin: ExecutionOrigin,
    mut implementation_context: Option<Value>,
) -> AppResult<RunOutcome>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let project_root = project_root_path(base_dir, project_id);
    let mut stage_index = start_stage_index;
    let mut cursor = start_cursor;

    while stage_index < stage_plan.len() {
        let stage_entry = &stage_plan[stage_index];
        let stage_id = stage_entry.stage_id;
        cursor = StageCursor::new(
            stage_id,
            cursor.cycle,
            cursor.attempt,
            cursor.completion_round,
        )?;

        let (completed_cursor, bundle) = execute_stage_with_retry(
            agent_service,
            run_snapshot_write,
            journal_store,
            log_write,
            base_dir,
            project_id,
            run_id,
            seq,
            snapshot,
            stage_entry,
            &cursor,
            retry_policy,
            cancellation_token.clone(),
            origin,
            implementation_context
                .as_ref()
                .filter(|_| stage_id == StageId::Implementation),
            &project_root,
        )
        .await?;

        persist_stage_success(
            artifact_write,
            journal_store,
            run_snapshot_write,
            log_write,
            base_dir,
            project_id,
            run_id,
            seq,
            snapshot,
            stage_id,
            &completed_cursor,
            &bundle,
            origin,
        )
        .await?;

        cursor = completed_cursor;

        if stage_id == StageId::Implementation {
            implementation_context = None;
        }

        if stage_id == StageId::PromptReview && prompt_review_requires_pause(&bundle.payload) {
            if let Err(error) = pause_run(
                snapshot,
                run_snapshot_write,
                base_dir,
                project_id,
                "paused after Prompt Review: readiness marked not ready; revise the prompt and run `ralph-burning run resume`".to_owned(),
            ) {
                return fail_run_result(
                    &AppError::StageCommitFailed {
                        stage_id,
                        details: format!(
                            "failed to persist paused snapshot after {}: {}",
                            stage_id.as_str(),
                            error
                        ),
                    },
                    stage_id,
                    run_id,
                    seq,
                    snapshot,
                    journal_store,
                    run_snapshot_write,
                    base_dir,
                    project_id,
                    origin,
                )
                .await;
            }
            return Ok(RunOutcome::Paused);
        }

        if let Some(outcome) = validation_outcome(&bundle.payload) {
            match outcome {
                ReviewOutcome::Approved => {}
                ReviewOutcome::ConditionallyApproved => {
                    append_amendments(snapshot, validation_follow_ups(&bundle.payload));
                }
                ReviewOutcome::RequestChanges if is_remediation_stage(stage_id) => {
                    let next_cycle = cursor.cycle + 1;
                    if next_cycle > retry_policy.max_remediation_cycles() {
                        return fail_run_result(
                            &AppError::RemediationExhausted {
                                cycle: next_cycle,
                                max: retry_policy.max_remediation_cycles(),
                            },
                            stage_id,
                            run_id,
                            seq,
                            snapshot,
                            journal_store,
                            run_snapshot_write,
                            base_dir,
                            project_id,
                            origin,
                        )
                        .await;
                    }

                    let next_stage_index = stage_index_for(stage_plan, StageId::Implementation)?;
                    let next_cursor = cursor.advance_cycle(StageId::Implementation);
                    record_cycle_advance(snapshot, next_cursor.cycle);
                    *seq += 1;
                    let cycle_advanced = journal::cycle_advanced_event(
                        *seq,
                        Utc::now(),
                        run_id,
                        stage_id,
                        cursor.cycle,
                        next_cursor.cycle,
                        StageId::Implementation,
                    );
                    let cycle_advanced_line = journal::serialize_event(&cycle_advanced)?;
                    if let Err(error) =
                        journal_store.append_event(base_dir, project_id, &cycle_advanced_line)
                    {
                        *seq -= 1;
                        return fail_run_result(
                            &AppError::StageCommitFailed {
                                stage_id,
                                details: format!(
                                    "failed to persist cycle_advanced event for {}: {}",
                                    stage_id.as_str(),
                                    error
                                ),
                            },
                            stage_id,
                            run_id,
                            seq,
                            snapshot,
                            journal_store,
                            run_snapshot_write,
                            base_dir,
                            project_id,
                            origin,
                        )
                        .await;
                    }

                    snapshot.status = RunStatus::Running;
                    snapshot.active_run = Some(ActiveRun {
                        run_id: run_id.as_str().to_owned(),
                        stage_cursor: next_cursor.clone(),
                        started_at: snapshot_started_at(snapshot)?,
                    });
                    snapshot.status_summary = format!(
                        "running: remediation cycle {} -> {}",
                        next_cursor.cycle,
                        next_cursor.stage.display_name()
                    );
                    if let Err(error) =
                        run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)
                    {
                        return fail_run_result(
                            &AppError::StageCommitFailed {
                                stage_id,
                                details: format!(
                                    "failed to persist remediation cursor for {}: {}",
                                    stage_id.as_str(),
                                    error
                                ),
                            },
                            stage_id,
                            run_id,
                            seq,
                            snapshot,
                            journal_store,
                            run_snapshot_write,
                            base_dir,
                            project_id,
                            origin,
                        )
                        .await;
                    }

                    implementation_context =
                        Some(remediation_context(stage_id, next_cursor.cycle, &bundle));
                    stage_index = next_stage_index;
                    cursor = next_cursor;
                    continue;
                }
                ReviewOutcome::RequestChanges | ReviewOutcome::Rejected => {
                    let failure = AppError::InvocationFailed {
                        backend: stage_entry.target.backend.family.to_string(),
                        stage_id,
                        failure_class: FailureClass::QaReviewOutcomeFailure,
                        details: format!("non-passing QA/review outcome: {}", outcome),
                    };
                    return fail_run_result(
                        &failure,
                        stage_id,
                        run_id,
                        seq,
                        snapshot,
                        journal_store,
                        run_snapshot_write,
                        base_dir,
                        project_id,
                        origin,
                    )
                    .await;
                }
            }
        }

        if stage_index + 1 == stage_plan.len() {
            complete_run(
                snapshot,
                run_snapshot_write,
                journal_store,
                base_dir,
                project_id,
                run_id,
                seq,
            )?;
            return Ok(RunOutcome::Completed);
        }

        let next_stage = stage_plan[stage_index + 1].stage_id;
        cursor = cursor.advance_stage(next_stage);
        snapshot.status = RunStatus::Running;
        snapshot.active_run = Some(ActiveRun {
            run_id: run_id.as_str().to_owned(),
            stage_cursor: cursor.clone(),
            started_at: snapshot_started_at(snapshot)?,
        });
        snapshot.status_summary = format!(
            "running: completed {}, next {}",
            stage_id.display_name(),
            next_stage.display_name()
        );
        if let Err(error) = run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot) {
            return fail_run_result(
                &AppError::StageCommitFailed {
                    stage_id,
                    details: format!(
                        "failed to persist next-stage cursor after {}: {}",
                        stage_id.as_str(),
                        error
                    ),
                },
                stage_id,
                run_id,
                seq,
                snapshot,
                journal_store,
                run_snapshot_write,
                base_dir,
                project_id,
                origin,
            )
            .await;
        }
        stage_index += 1;
    }

    complete_run(
        snapshot,
        run_snapshot_write,
        journal_store,
        base_dir,
        project_id,
        run_id,
        seq,
    )?;
    Ok(RunOutcome::Completed)
}

#[allow(clippy::too_many_arguments)]
async fn execute_stage_with_retry<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &mut RunSnapshot,
    stage_entry: &StagePlan,
    starting_cursor: &StageCursor,
    retry_policy: &RetryPolicy,
    cancellation_token: CancellationToken,
    origin: ExecutionOrigin,
    implementation_context: Option<&Value>,
    project_root: &Path,
) -> AppResult<(StageCursor, ValidatedBundle)>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let stage_id = stage_entry.stage_id;
    let mut cursor = starting_cursor.clone();

    loop {
        *seq += 1;
        let stage_entered = journal::stage_entered_event(
            *seq,
            Utc::now(),
            run_id,
            stage_id,
            cursor.cycle,
            cursor.attempt,
        );
        let stage_entered_line = journal::serialize_event(&stage_entered)?;
        if let Err(error) = journal_store.append_event(base_dir, project_id, &stage_entered_line) {
            *seq -= 1;
            return fail_run_result(
                &AppError::StageCommitFailed {
                    stage_id,
                    details: format!(
                        "failed to persist stage_entered event for {}: {}",
                        stage_id.as_str(),
                        error
                    ),
                },
                stage_id,
                run_id,
                seq,
                snapshot,
                journal_store,
                run_snapshot_write,
                base_dir,
                project_id,
                origin,
            )
            .await;
        }

        snapshot.status = RunStatus::Running;
        snapshot.active_run = Some(ActiveRun {
            run_id: run_id.as_str().to_owned(),
            stage_cursor: cursor.clone(),
            started_at: snapshot_started_at(snapshot)?,
        });
        snapshot.status_summary = format!("running: {}", stage_id.display_name());
        if let Err(error) = run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot) {
            return fail_run_result(
                &AppError::StageCommitFailed {
                    stage_id,
                    details: format!(
                        "failed to update snapshot for stage {} attempt {}: {}",
                        stage_id.as_str(),
                        cursor.attempt,
                        error
                    ),
                },
                stage_id,
                run_id,
                seq,
                snapshot,
                journal_store,
                run_snapshot_write,
                base_dir,
                project_id,
                origin,
            )
            .await;
        }

        let _ = log_write.append_runtime_log(
            base_dir,
            project_id,
            &RuntimeLogEntry {
                timestamp: Utc::now(),
                level: LogLevel::Info,
                source: "engine".to_owned(),
                message: format!(
                    "stage_entered: {} cycle={} attempt={}",
                    stage_id.as_str(),
                    cursor.cycle,
                    cursor.attempt
                ),
            },
        );

        let request = InvocationRequest {
            invocation_id: format!(
                "{}-{}-c{}-a{}",
                run_id.as_str(),
                stage_id.as_str(),
                cursor.cycle,
                cursor.attempt
            ),
            project_root: project_root.to_path_buf(),
            stage_contract: stage_entry.contract,
            role: stage_entry.role,
            resolved_target: stage_entry.target.clone(),
            payload: InvocationPayload {
                prompt: format!("Execute stage: {}", stage_id.display_name()),
                context: invocation_context(&cursor, implementation_context),
            },
            timeout: Duration::from_secs(300),
            cancellation_token: cancellation_token.clone(),
            session_policy: SessionPolicy::ReuseIfAllowed,
            prior_session: None,
            attempt_number: cursor.attempt,
        };

        let result = agent_service.invoke(request).await.and_then(|envelope| {
            stage_entry
                .contract
                .evaluate_permissive(&envelope.parsed_payload)
                .map_err(|contract_error| AppError::InvocationFailed {
                    backend: stage_entry.target.backend.family.to_string(),
                    stage_id,
                    failure_class: contract_error.failure_class(),
                    details: contract_error.to_string(),
                })
        });

        match result {
            Ok(bundle) => return Ok((cursor.clone(), bundle)),
            Err(error) => {
                let Some(failure_class) = error.failure_class() else {
                    return fail_run_result(
                        &error,
                        stage_id,
                        run_id,
                        seq,
                        snapshot,
                        journal_store,
                        run_snapshot_write,
                        base_dir,
                        project_id,
                        origin,
                    )
                    .await;
                };

                let max_attempts = retry_policy.max_attempts(failure_class);
                let will_retry = retry_policy.is_retryable(failure_class)
                    && cursor.attempt < max_attempts
                    && !matches!(failure_class, FailureClass::Cancellation)
                    && !cancellation_token.is_cancelled();

                *seq += 1;
                let stage_failed = journal::stage_failed_event(
                    *seq,
                    Utc::now(),
                    run_id,
                    stage_id,
                    cursor.cycle,
                    cursor.attempt,
                    failure_class,
                    &error.to_string(),
                    will_retry,
                );
                let stage_failed_line = journal::serialize_event(&stage_failed)?;
                if let Err(append_error) =
                    journal_store.append_event(base_dir, project_id, &stage_failed_line)
                {
                    *seq -= 1;
                    return fail_run_result(
                        &AppError::StageCommitFailed {
                            stage_id,
                            details: format!(
                                "failed to persist stage_failed event for {}: {}",
                                stage_id.as_str(),
                                append_error
                            ),
                        },
                        stage_id,
                        run_id,
                        seq,
                        snapshot,
                        journal_store,
                        run_snapshot_write,
                        base_dir,
                        project_id,
                        origin,
                    )
                    .await;
                }

                let _ = log_write.append_runtime_log(
                    base_dir,
                    project_id,
                    &RuntimeLogEntry {
                        timestamp: Utc::now(),
                        level: LogLevel::Warn,
                        source: "engine".to_owned(),
                        message: format!(
                            "stage_failed: {} cycle={} attempt={} retry={}",
                            stage_id.as_str(),
                            cursor.cycle,
                            cursor.attempt,
                            will_retry
                        ),
                    },
                );

                if will_retry {
                    cursor = cursor.retry();
                    continue;
                }

                return fail_run_result(
                    &error,
                    stage_id,
                    run_id,
                    seq,
                    snapshot,
                    journal_store,
                    run_snapshot_write,
                    base_dir,
                    project_id,
                    origin,
                )
                .await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn persist_stage_success(
    artifact_write: &dyn PayloadArtifactWritePort,
    journal_store: &dyn JournalStorePort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &mut RunSnapshot,
    stage_id: StageId,
    cursor: &StageCursor,
    bundle: &ValidatedBundle,
    origin: ExecutionOrigin,
) -> AppResult<()> {
    let stage_now = Utc::now();
    let payload_id = format!(
        "{}-{}-c{}-a{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt
    );
    let artifact_id = format!("{}-artifact", payload_id);

    let payload_record = PayloadRecord {
        payload_id: payload_id.clone(),
        stage_id,
        cycle: cursor.cycle,
        attempt: cursor.attempt,
        created_at: stage_now,
        payload: serde_json::to_value(&bundle.payload)?,
    };
    let artifact_record = ArtifactRecord {
        artifact_id: artifact_id.clone(),
        payload_id: payload_id.clone(),
        stage_id,
        created_at: stage_now,
        content: bundle.artifact.clone(),
    };

    if let Err(error) = artifact_write.write_payload_artifact_pair(
        base_dir,
        project_id,
        &payload_record,
        &artifact_record,
    ) {
        let _ = artifact_write.remove_payload_artifact_pair(
            base_dir,
            project_id,
            &payload_id,
            &artifact_id,
        );
        return fail_run(
            &AppError::StageCommitFailed {
                stage_id,
                details: format!("payload/artifact persistence failed: {}", error),
            },
            stage_id,
            run_id,
            seq,
            snapshot,
            journal_store,
            run_snapshot_write,
            base_dir,
            project_id,
            origin,
        )
        .await;
    }

    *seq += 1;
    let stage_completed = journal::stage_completed_event(
        *seq,
        Utc::now(),
        run_id,
        stage_id,
        cursor.cycle,
        cursor.attempt,
        &payload_id,
        &artifact_id,
    );
    let stage_completed_line = journal::serialize_event(&stage_completed)?;
    if let Err(error) = journal_store.append_event(base_dir, project_id, &stage_completed_line) {
        let _ = artifact_write.remove_payload_artifact_pair(
            base_dir,
            project_id,
            &payload_id,
            &artifact_id,
        );
        *seq -= 1;
        return fail_run(
            &AppError::StageCommitFailed {
                stage_id,
                details: format!("journal append failed during stage commit: {}", error),
            },
            stage_id,
            run_id,
            seq,
            snapshot,
            journal_store,
            run_snapshot_write,
            base_dir,
            project_id,
            origin,
        )
        .await;
    }

    let _ = log_write.append_runtime_log(
        base_dir,
        project_id,
        &RuntimeLogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Info,
            source: "engine".to_owned(),
            message: format!("stage_completed: {}", stage_id.as_str()),
        },
    );

    Ok(())
}

fn complete_run(
    snapshot: &mut RunSnapshot,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
) -> AppResult<()> {
    snapshot.status = RunStatus::Completed;
    snapshot.active_run = None;
    snapshot.completion_rounds = snapshot.completion_rounds.max(1);
    snapshot.status_summary = "completed".to_owned();
    run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)?;

    *seq += 1;
    let run_completed =
        journal::run_completed_event(*seq, Utc::now(), run_id, snapshot.completion_rounds);
    let run_completed_line = journal::serialize_event(&run_completed)?;
    journal_store.append_event(base_dir, project_id, &run_completed_line)?;
    Ok(())
}

fn pause_run(
    snapshot: &mut RunSnapshot,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    summary: String,
) -> AppResult<()> {
    snapshot.status = RunStatus::Paused;
    snapshot.active_run = None;
    snapshot.status_summary = summary;
    run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)
}

/// Record a run failure: persist failed snapshot, then journal event, return error.
#[allow(clippy::too_many_arguments)]
async fn fail_run(
    err: &AppError,
    stage_id: StageId,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &mut RunSnapshot,
    journal_store: &dyn JournalStorePort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    origin: ExecutionOrigin,
) -> AppResult<()> {
    let failure_class = failure_label(err);
    let message = err.to_string();

    snapshot.status = RunStatus::Failed;
    snapshot.active_run = None;
    snapshot.status_summary = format!("failed at {}: {}", stage_id.display_name(), message);
    run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)?;

    *seq += 1;
    let run_failed =
        journal::run_failed_event(*seq, Utc::now(), run_id, stage_id, &failure_class, &message);
    if let Ok(run_failed_line) = journal::serialize_event(&run_failed) {
        let _ = journal_store.append_event(base_dir, project_id, &run_failed_line);
    }

    Err(origin.error(format!("stage {} failed: {}", stage_id.as_str(), message)))
}

#[allow(clippy::too_many_arguments)]
async fn fail_run_result<T>(
    err: &AppError,
    stage_id: StageId,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &mut RunSnapshot,
    journal_store: &dyn JournalStorePort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    origin: ExecutionOrigin,
) -> AppResult<T> {
    fail_run(
        err,
        stage_id,
        run_id,
        seq,
        snapshot,
        journal_store,
        run_snapshot_write,
        base_dir,
        project_id,
        origin,
    )
    .await?;
    unreachable!("fail_run always returns an error")
}

fn failure_label(error: &AppError) -> String {
    if let Some(failure_class) = error.failure_class() {
        return failure_class.as_str().to_owned();
    }

    match error {
        AppError::RemediationExhausted { .. } => "remediation_exhausted".to_owned(),
        AppError::ResumeFailed { .. } => "resume_failed".to_owned(),
        _ => "unknown".to_owned(),
    }
}

fn prompt_review_requires_pause(payload: &StagePayload) -> bool {
    matches!(
        payload,
        StagePayload::Planning(planning) if !planning.readiness.ready
    )
}

fn validation_outcome(payload: &StagePayload) -> Option<ReviewOutcome> {
    match payload {
        StagePayload::Validation(validation) => Some(validation.outcome),
        _ => None,
    }
}

fn validation_follow_ups(payload: &StagePayload) -> &[String] {
    match payload {
        StagePayload::Validation(validation) => &validation.follow_up_or_amendments,
        _ => &[],
    }
}

fn append_amendments(snapshot: &mut RunSnapshot, amendments: &[String]) {
    snapshot
        .amendment_queue
        .pending
        .extend(amendments.iter().cloned().map(Value::String));
}

fn is_remediation_stage(stage_id: StageId) -> bool {
    matches!(stage_id, StageId::Qa | StageId::Review)
}

fn remediation_context(stage_id: StageId, next_cycle: u32, bundle: &ValidatedBundle) -> Value {
    match &bundle.payload {
        StagePayload::Validation(validation) => json!({
            "source_stage": stage_id.as_str(),
            "cycle": next_cycle,
            "follow_up_or_amendments": validation.follow_up_or_amendments,
            "findings_or_gaps": validation.findings_or_gaps,
        }),
        _ => json!({}),
    }
}

fn invocation_context(cursor: &StageCursor, implementation_context: Option<&Value>) -> Value {
    let mut context = json!({
        "cycle": cursor.cycle,
        "attempt": cursor.attempt,
        "completion_round": cursor.completion_round,
    });

    if let Some(implementation_context) = implementation_context {
        context["remediation"] = implementation_context.clone();
    }

    context
}

fn stage_index_for(stage_plan: &[StagePlan], stage_id: StageId) -> AppResult<usize> {
    stage_plan
        .iter()
        .position(|entry| entry.stage_id == stage_id)
        .ok_or_else(|| AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: format!(
                "stage '{}' is not part of the standard stage plan",
                stage_id
            ),
        })
}

fn record_cycle_advance(snapshot: &mut RunSnapshot, next_cycle: u32) {
    snapshot.cycle_history.push(CycleHistoryEntry {
        cycle: next_cycle,
        stage_id: StageId::Implementation,
        started_at: Utc::now(),
        completed_at: None,
    });
}

fn pending_remediation_cycle(
    snapshot: &RunSnapshot,
    current_cycle: u32,
    last_completed_stage: Option<StageId>,
) -> Option<u32> {
    let last_entry = snapshot.cycle_history.last()?;
    if !matches!(last_completed_stage, Some(stage_id) if is_remediation_stage(stage_id)) {
        return None;
    }

    (last_entry.stage_id == StageId::Implementation && last_entry.cycle > current_cycle)
        .then_some(last_entry.cycle)
}

fn standard_stage_plan_for_resume(
    events: &[JournalEvent],
    effective_config: &EffectiveConfig,
) -> AppResult<Vec<StageId>> {
    let run_started = events
        .iter()
        .rev()
        .find(|event| {
            event.event_type
                == crate::contexts::project_run_record::model::JournalEventType::RunStarted
        })
        .ok_or_else(|| AppError::ResumeFailed {
            reason: "run journal does not contain a run_started event".to_owned(),
        })?;

    let first_stage = detail_stage_id(run_started, "first_stage")?;
    match first_stage {
        StageId::PromptReview => Ok(standard_stage_plan(true)),
        StageId::Planning => Ok(standard_stage_plan(false)),
        _ => Ok(standard_stage_plan(
            effective_config.prompt_review_enabled(),
        )),
    }
}

fn derive_resume_state(
    events: &[JournalEvent],
    snapshot: &RunSnapshot,
    stage_plan: &[StagePlan],
) -> AppResult<ResumeState> {
    let run_started = events
        .iter()
        .rev()
        .find(|event| {
            event.event_type
                == crate::contexts::project_run_record::model::JournalEventType::RunStarted
        })
        .ok_or_else(|| AppError::ResumeFailed {
            reason: "run journal does not contain a run_started event".to_owned(),
        })?;
    let run_id = RunId::new(detail_string(run_started, "run_id")?.to_owned())?;
    let started_at = run_started.timestamp;
    let implementation_stage_index = stage_index_for(stage_plan, StageId::Implementation)?;
    let mut current_cycle = snapshot
        .cycle_history
        .last()
        .map(|entry| entry.cycle)
        .unwrap_or(1);
    let mut next_stage_index = 0usize;
    let mut last_completed_stage = None;

    for event in events {
        match event.event_type {
            crate::contexts::project_run_record::model::JournalEventType::StageCompleted => {
                let stage_id = detail_stage_id(event, "stage_id")?;
                current_cycle = detail_u32(event, "cycle").unwrap_or(current_cycle);
                next_stage_index = stage_index_for(stage_plan, stage_id)? + 1;
                last_completed_stage = Some(stage_id);
            }
            crate::contexts::project_run_record::model::JournalEventType::CycleAdvanced => {
                current_cycle = detail_u32(event, "to_cycle").unwrap_or(current_cycle + 1);
                next_stage_index = implementation_stage_index;
            }
            crate::contexts::project_run_record::model::JournalEventType::RunCompleted => {
                next_stage_index = stage_plan.len();
            }
            _ => {}
        }
    }

    if let Some(pending_cycle) =
        pending_remediation_cycle(snapshot, current_cycle, last_completed_stage)
    {
        current_cycle = pending_cycle;
        next_stage_index = implementation_stage_index;
    }

    if next_stage_index >= stage_plan.len() {
        return Err(AppError::ResumeFailed {
            reason: "all standard-flow stages are already complete; there is nothing to resume"
                .to_owned(),
        });
    }

    let completion_round = snapshot.completion_rounds.max(1);
    let cursor = StageCursor::new(
        stage_plan[next_stage_index].stage_id,
        current_cycle.max(1),
        1,
        completion_round,
    )?;

    Ok(ResumeState {
        run_id,
        started_at,
        stage_index: next_stage_index,
        cursor,
    })
}

fn detail_string<'a>(event: &'a JournalEvent, key: &str) -> AppResult<&'a str> {
    event
        .details
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: format!(
                "event sequence {} is missing string field '{}'",
                event.sequence, key
            ),
        })
}

fn detail_stage_id(event: &JournalEvent, key: &str) -> AppResult<StageId> {
    detail_string(event, key)?.parse::<StageId>()
}

fn detail_u32(event: &JournalEvent, key: &str) -> Option<u32> {
    event
        .details
        .get(key)
        .and_then(Value::as_u64)
        .map(|value| value as u32)
}

fn snapshot_started_at(snapshot: &RunSnapshot) -> AppResult<DateTime<Utc>> {
    snapshot
        .active_run
        .as_ref()
        .map(|active| active.started_at)
        .ok_or_else(|| AppError::CorruptRecord {
            file: "run.json".to_owned(),
            details: "running snapshot lost active_run metadata".to_owned(),
        })
}

/// Helper to get project root path.
fn project_root_path(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
    base_dir
        .join(".ralph-burning")
        .join("projects")
        .join(project_id.as_str())
}
