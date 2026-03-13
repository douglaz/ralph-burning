use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::adapters::fs::FsRollbackPointStore;
use crate::adapters::worktree::WorktreeAdapter;
use crate::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use crate::contexts::agent_execution::service::{
    AgentExecutionPort, BackendSelectionConfig, RawOutputPort,
};
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::AgentExecutionService;
use crate::contexts::project_run_record::journal;
use crate::contexts::project_run_record::model::{
    ActiveRun, ArtifactRecord, CycleHistoryEntry, JournalEvent, LogLevel, PayloadRecord,
    QueuedAmendment, RollbackPoint, RunSnapshot, RunStatus, RuntimeLogEntry,
};
use crate::contexts::project_run_record::queries;
use crate::contexts::project_run_record::service::{
    AmendmentQueuePort, ArtifactStorePort, JournalStorePort, PayloadArtifactWritePort,
    RollbackPointStorePort, RunSnapshotPort, RunSnapshotWritePort, RuntimeLogWritePort,
};
use crate::contexts::workflow_composition::payloads::{
    ReviewOutcome, StagePayload, ValidationPayload,
};
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::shared::domain::{
    BackendRole, FailureClass, FlowPreset, ProjectId, ResolvedBackendTarget, RunId, SessionPolicy,
    StageCursor, StageId,
};
use crate::shared::error::{AppError, AppResult};

use super::contracts::{self, ValidatedBundle};
use super::retry_policy::RetryPolicy;
use super::{flow_semantics, stage_plan_for_flow, FlowSemantics};

/// Compatibility wrapper for the legacy standard-flow helper.
pub fn standard_stage_plan(prompt_review_enabled: bool) -> Vec<StageId> {
    stage_plan_for_flow(FlowPreset::Standard, prompt_review_enabled)
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
            .check_capability(
                &entry.target,
                &InvocationContract::Stage(entry.contract.clone()),
            )
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

fn history_record_base_id(
    run_id: &RunId,
    stage_id: StageId,
    cursor: &StageCursor,
    rollback_count: u32,
) -> String {
    let base_id = format!(
        "{}-{}-c{}-a{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt
    );
    if rollback_count == 0 {
        base_id
    } else {
        format!("{base_id}-rb{rollback_count}")
    }
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

#[allow(clippy::too_many_arguments)]
pub async fn execute_run<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_read: &dyn RunSnapshotPort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    amendment_queue_port: &dyn AmendmentQueuePort,
    base_dir: &Path,
    project_id: &ProjectId,
    preset: FlowPreset,
    effective_config: &EffectiveConfig,
) -> AppResult<()>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let rollback_store = FsRollbackPointStore;
    execute_run_with_retry_internal(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        &rollback_store,
        base_dir,
        project_id,
        preset,
        effective_config,
        &RetryPolicy::default_policy(),
        CancellationToken::new(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_run_with_retry<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_read: &dyn RunSnapshotPort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    amendment_queue_port: &dyn AmendmentQueuePort,
    base_dir: &Path,
    project_id: &ProjectId,
    preset: FlowPreset,
    effective_config: &EffectiveConfig,
    retry_policy: &RetryPolicy,
    cancellation_token: CancellationToken,
) -> AppResult<()>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let rollback_store = FsRollbackPointStore;
    execute_run_with_retry_internal(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        &rollback_store,
        base_dir,
        project_id,
        preset,
        effective_config,
        retry_policy,
        cancellation_token,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_run_with_retry_internal<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_read: &dyn RunSnapshotPort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    amendment_queue_port: &dyn AmendmentQueuePort,
    rollback_store: &dyn RollbackPointStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    preset: FlowPreset,
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

    let stage_ids = stage_plan_for_flow(preset, effective_config.prompt_review_enabled());
    let semantics = flow_semantics(preset);
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

    execute_run_internal(
        agent_service,
        run_snapshot_write,
        journal_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        rollback_store,
        base_dir,
        project_id,
        &run_id,
        &mut seq,
        &mut current_snapshot,
        semantics,
        &stage_plan,
        0,
        initial_cursor,
        retry_policy,
        cancellation_token,
        ExecutionOrigin::Start,
        None,
        effective_config,
    )
    .await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_standard_run<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_read: &dyn RunSnapshotPort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    amendment_queue_port: &dyn AmendmentQueuePort,
    base_dir: &Path,
    project_id: &ProjectId,
    effective_config: &EffectiveConfig,
) -> AppResult<()>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    execute_run(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        base_dir,
        project_id,
        FlowPreset::Standard,
        effective_config,
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
    amendment_queue_port: &dyn AmendmentQueuePort,
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
    execute_run_with_retry(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        base_dir,
        project_id,
        FlowPreset::Standard,
        effective_config,
        retry_policy,
        cancellation_token,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn resume_run<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_read: &dyn RunSnapshotPort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_store: &dyn ArtifactStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    amendment_queue_port: &dyn AmendmentQueuePort,
    base_dir: &Path,
    project_id: &ProjectId,
    preset: FlowPreset,
    effective_config: &EffectiveConfig,
) -> AppResult<()>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let rollback_store = FsRollbackPointStore;
    resume_run_with_retry_internal(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        &rollback_store,
        base_dir,
        project_id,
        preset,
        effective_config,
        &RetryPolicy::default_policy(),
        CancellationToken::new(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn resume_run_with_retry<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_read: &dyn RunSnapshotPort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_store: &dyn ArtifactStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    amendment_queue_port: &dyn AmendmentQueuePort,
    base_dir: &Path,
    project_id: &ProjectId,
    preset: FlowPreset,
    effective_config: &EffectiveConfig,
    retry_policy: &RetryPolicy,
    cancellation_token: CancellationToken,
) -> AppResult<()>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let rollback_store = FsRollbackPointStore;
    resume_run_with_retry_internal(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        &rollback_store,
        base_dir,
        project_id,
        preset,
        effective_config,
        retry_policy,
        cancellation_token,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn resume_run_with_retry_internal<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_read: &dyn RunSnapshotPort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_store: &dyn ArtifactStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    amendment_queue_port: &dyn AmendmentQueuePort,
    rollback_store: &dyn RollbackPointStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    preset: FlowPreset,
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
    let visible_events =
        queries::visible_journal_events(&events).map_err(|error| AppError::ResumeFailed {
            reason: error.to_string(),
        })?;
    let stage_ids = stage_plan_for_resume(preset, &visible_events, effective_config)?;
    let semantics = flow_semantics(preset);
    let workspace_defaults = BackendSelectionConfig::from_effective_config(effective_config)?;
    let stage_plan = resolve_stage_plan(
        stage_ids.as_slice(),
        agent_service.resolver(),
        Some(&workspace_defaults),
    )?;
    // Reconcile amendments from disk into snapshot before deriving resume state.
    reconcile_amendments_from_disk(&mut snapshot, amendment_queue_port, base_dir, project_id)?;

    let resume_state = derive_resume_state(&visible_events, &snapshot, &stage_plan, semantics)?;
    let execution_context = derive_resume_execution_context(
        artifact_store,
        base_dir,
        project_id,
        &resume_state.cursor,
        &visible_events,
        semantics,
    )?;

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

    execute_run_internal(
        agent_service,
        run_snapshot_write,
        journal_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        rollback_store,
        base_dir,
        project_id,
        &resume_state.run_id,
        &mut seq,
        &mut snapshot,
        semantics,
        &stage_plan,
        resume_state.stage_index,
        resume_state.cursor,
        retry_policy,
        cancellation_token,
        ExecutionOrigin::Resume,
        execution_context,
        effective_config,
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
    artifact_store: &dyn ArtifactStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    amendment_queue_port: &dyn AmendmentQueuePort,
    base_dir: &Path,
    project_id: &ProjectId,
    effective_config: &EffectiveConfig,
) -> AppResult<()>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    resume_run(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        base_dir,
        project_id,
        FlowPreset::Standard,
        effective_config,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn resume_standard_run_with_retry<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_read: &dyn RunSnapshotPort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_store: &dyn ArtifactStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    amendment_queue_port: &dyn AmendmentQueuePort,
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
    resume_run_with_retry(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        base_dir,
        project_id,
        FlowPreset::Standard,
        effective_config,
        retry_policy,
        cancellation_token,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn execute_run_internal<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    amendment_queue_port: &dyn AmendmentQueuePort,
    rollback_store: &dyn RollbackPointStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &mut RunSnapshot,
    semantics: FlowSemantics,
    stage_plan: &[StagePlan],
    start_stage_index: usize,
    start_cursor: StageCursor,
    retry_policy: &RetryPolicy,
    cancellation_token: CancellationToken,
    origin: ExecutionOrigin,
    mut execution_context: Option<Value>,
    effective_config: &EffectiveConfig,
) -> AppResult<RunOutcome>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let project_root = project_root_path(base_dir, project_id);
    let mut stage_index = start_stage_index;
    let mut cursor = start_cursor;
    let _ = effective_config;

    while stage_index < stage_plan.len() {
        let stage_entry = &stage_plan[stage_index];
        let stage_id = stage_entry.stage_id;
        cursor = StageCursor::new(
            stage_id,
            cursor.cycle,
            cursor.attempt,
            cursor.completion_round,
        )?;

        // Inject pending amendments into planning invocation context.
        let planning_amendments: Option<Vec<QueuedAmendment>> =
            if stage_id == semantics.planning_stage && !snapshot.amendment_queue.pending.is_empty()
            {
                Some(snapshot.amendment_queue.pending.clone())
            } else {
                None
            };

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
            execution_context
                .as_ref()
                .filter(|_| stage_id == semantics.execution_stage),
            planning_amendments
                .as_deref()
                .filter(|_| stage_id == semantics.planning_stage),
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

        if stage_id == semantics.execution_stage {
            execution_context = None;
        }

        if Some(stage_id) == semantics.prompt_review_stage
            && prompt_review_requires_pause(&bundle.payload)
        {
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
            if let Err(error) = persist_rollback_point(
                rollback_store,
                journal_store,
                base_dir,
                project_id,
                run_id,
                seq,
                snapshot,
                stage_id,
                cursor.cycle,
            ) {
                return checkpoint_failure_result(
                    error,
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
                ReviewOutcome::ConditionallyApproved | ReviewOutcome::RequestChanges
                    if semantics.late_stages.contains(&stage_id) =>
                {
                    // Late-stage conditional approval or request changes:
                    // Queue durable amendments, advance completion round, restart from planning.
                    let follow_ups = validation_follow_ups(&bundle.payload);
                    let amendments = build_queued_amendments(
                        follow_ups,
                        stage_id,
                        cursor.cycle,
                        cursor.completion_round,
                        run_id,
                    );

                    // Persist amendment files atomically to disk first.
                    // Track written IDs so we can roll back on partial failure.
                    let mut written_ids: Vec<String> = Vec::new();
                    for amendment in &amendments {
                        if let Err(error) =
                            amendment_queue_port.write_amendment(base_dir, project_id, amendment)
                        {
                            // Roll back already-written amendment files from this batch.
                            for id in &written_ids {
                                let _ =
                                    amendment_queue_port.remove_amendment(base_dir, project_id, id);
                            }
                            // Failure invariant: if amendment persistence fails, no queue
                            // entry becomes visible in run.json.
                            return fail_run_result(
                                &AppError::AmendmentQueueError {
                                    details: format!(
                                        "failed to persist amendment '{}': {}",
                                        amendment.amendment_id, error
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
                        written_ids.push(amendment.amendment_id.clone());
                    }

                    // Emit amendment_queued journal events.
                    for amendment in &amendments {
                        *seq += 1;
                        let amendment_event = journal::amendment_queued_event(
                            *seq,
                            Utc::now(),
                            run_id,
                            &amendment.amendment_id,
                            amendment.source_stage,
                            &amendment.body,
                        );
                        let event_line = journal::serialize_event(&amendment_event)?;
                        if let Err(error) =
                            journal_store.append_event(base_dir, project_id, &event_line)
                        {
                            *seq -= 1;
                            return fail_run_result(
                                &AppError::AmendmentQueueError {
                                    details: format!(
                                        "failed to persist amendment_queued event: {}",
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
                    }

                    // Add amendments to snapshot queue.
                    snapshot.amendment_queue.pending.extend(amendments);

                    // Emit completion_round_advanced event.
                    let from_round = cursor.completion_round;
                    let to_round = from_round + 1;
                    let amendment_count = snapshot.amendment_queue.pending.len() as u32;
                    *seq += 1;
                    let round_event = journal::completion_round_advanced_event(
                        *seq,
                        Utc::now(),
                        run_id,
                        stage_id,
                        from_round,
                        to_round,
                        amendment_count,
                    );
                    let round_event_line = journal::serialize_event(&round_event)?;
                    if let Err(error) =
                        journal_store.append_event(base_dir, project_id, &round_event_line)
                    {
                        *seq -= 1;
                        return fail_run_result(
                            &AppError::StageCommitFailed {
                                stage_id,
                                details: format!(
                                    "failed to persist completion_round_advanced event: {}",
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

                    // Advance completion round and restart from the flow's planning stage.
                    let planning_index = stage_index_for(stage_plan, semantics.planning_stage)?;
                    let next_cursor = cursor.advance_completion_round(semantics.planning_stage)?;
                    snapshot.completion_rounds = snapshot.completion_rounds.max(to_round);
                    snapshot.status = RunStatus::Running;
                    snapshot.active_run = Some(ActiveRun {
                        run_id: run_id.as_str().to_owned(),
                        stage_cursor: next_cursor.clone(),
                        started_at: snapshot_started_at(snapshot)?,
                    });
                    snapshot.status_summary = format!(
                        "running: completion round {} -> {}",
                        from_round,
                        next_cursor.stage.display_name()
                    );
                    if let Err(error) =
                        run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)
                    {
                        return fail_run_result(
                            &AppError::StageCommitFailed {
                                stage_id,
                                details: format!(
                                    "failed to persist completion round cursor: {}",
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
                    if let Err(error) = persist_rollback_point(
                        rollback_store,
                        journal_store,
                        base_dir,
                        project_id,
                        run_id,
                        seq,
                        snapshot,
                        stage_id,
                        cursor.cycle,
                    ) {
                        return checkpoint_failure_result(
                            error,
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

                    execution_context = None;
                    stage_index = planning_index;
                    cursor = next_cursor;
                    continue;
                }
                ReviewOutcome::ConditionallyApproved if semantics.late_stages.is_empty() => {
                    // Docs/CI flows do not enter completion rounds, but their follow-ups
                    // still need to be preserved in canonical snapshot state.
                    let recorded_follow_ups = build_recorded_follow_ups(
                        validation_follow_ups(&bundle.payload),
                        stage_id,
                        cursor.cycle,
                        cursor.completion_round,
                        run_id,
                    );
                    snapshot
                        .amendment_queue
                        .recorded_follow_ups
                        .extend(recorded_follow_ups);
                    if let Err(error) =
                        run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)
                    {
                        return fail_run_result(
                            &AppError::StageCommitFailed {
                                stage_id,
                                details: format!(
                                    "failed to persist recorded follow-ups after {}: {}",
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
                }
                ReviewOutcome::ConditionallyApproved => {}
                ReviewOutcome::RequestChanges
                    if semantics.remediation_trigger_stages.contains(&stage_id) =>
                {
                    let next_cycle = cursor.cycle.checked_add(1).ok_or_else(|| {
                        AppError::StageCursorOverflow {
                            field: "cycle",
                            value: cursor.cycle,
                        }
                    })?;
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

                    let next_stage_index = stage_index_for(stage_plan, semantics.execution_stage)?;
                    let next_cursor = cursor.advance_cycle(semantics.execution_stage)?;
                    record_cycle_advance(snapshot, next_cursor.cycle, semantics.execution_stage);
                    *seq += 1;
                    let cycle_advanced = journal::cycle_advanced_event(
                        *seq,
                        Utc::now(),
                        run_id,
                        stage_id,
                        cursor.cycle,
                        next_cursor.cycle,
                        semantics.execution_stage,
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
                    if let Err(error) = persist_rollback_point(
                        rollback_store,
                        journal_store,
                        base_dir,
                        project_id,
                        run_id,
                        seq,
                        snapshot,
                        stage_id,
                        cursor.cycle,
                    ) {
                        return checkpoint_failure_result(
                            error,
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

                    execution_context =
                        Some(remediation_context(stage_id, next_cursor.cycle, &bundle));
                    stage_index = next_stage_index;
                    cursor = next_cursor;
                    continue;
                }
                ReviewOutcome::RequestChanges | ReviewOutcome::Rejected => {
                    let failure = AppError::InvocationFailed {
                        backend: stage_entry.target.backend.family.to_string(),
                        contract_id: stage_id.to_string(),
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

        // After planning commit succeeds, drain pending amendments.
        if stage_id == semantics.planning_stage && !snapshot.amendment_queue.pending.is_empty() {
            let drained = snapshot.amendment_queue.pending.len() as u32;
            // Drain from disk first.
            if let Err(error) = amendment_queue_port.drain_amendments(base_dir, project_id) {
                return fail_run_result(
                    &AppError::AmendmentQueueError {
                        details: format!("failed to drain amendment files from disk: {}", error),
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
            // Clear from snapshot.
            snapshot.amendment_queue.processed_count += drained;
            snapshot.amendment_queue.pending.clear();
            if let Err(error) =
                run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)
            {
                return fail_run_result(
                    &AppError::StageCommitFailed {
                        stage_id,
                        details: format!(
                            "failed to persist snapshot after amendment drain: {}",
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
        }

        // Completion-round restarts only advance `completion_round`; `cycle`
        // remains remediation-only state.

        if stage_index + 1 == stage_plan.len() {
            complete_run(
                snapshot,
                run_snapshot_write,
                journal_store,
                amendment_queue_port,
                base_dir,
                project_id,
                run_id,
                seq,
            )?;
            if let Err(error) = persist_rollback_point(
                rollback_store,
                journal_store,
                base_dir,
                project_id,
                run_id,
                seq,
                snapshot,
                stage_id,
                cursor.cycle,
            ) {
                return checkpoint_failure_result(
                    error,
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
        if let Err(error) = persist_rollback_point(
            rollback_store,
            journal_store,
            base_dir,
            project_id,
            run_id,
            seq,
            snapshot,
            stage_id,
            cursor.cycle,
        ) {
            return checkpoint_failure_result(
                error,
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
        amendment_queue_port,
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
    execution_context: Option<&Value>,
    pending_amendments: Option<&[QueuedAmendment]>,
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
        if cancellation_token.is_cancelled() {
            return fail_run_result(
                &AppError::InvocationCancelled {
                    backend: stage_entry.target.backend.family.to_string(),
                    contract_id: stage_id.to_string(),
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
            contract: InvocationContract::Stage(stage_entry.contract),
            role: stage_entry.role,
            resolved_target: stage_entry.target.clone(),
            payload: InvocationPayload {
                prompt: format!("Execute stage: {}", stage_id.display_name()),
                context: invocation_context(&cursor, execution_context, pending_amendments),
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
                    contract_id: stage_id.to_string(),
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
                    cursor = cursor.retry()?;
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
    // After a rollback, durable history must branch instead of overwriting the
    // abandoned payload/artifact files from the previous branch.
    let payload_id = history_record_base_id(
        run_id,
        stage_id,
        cursor,
        snapshot.rollback_point_meta.rollback_count,
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

fn persist_rollback_point(
    rollback_store: &dyn RollbackPointStorePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &RunSnapshot,
    stage_id: StageId,
    cycle: u32,
) -> AppResult<()> {
    let worktree = WorktreeAdapter;
    let created_at = Utc::now();
    let rollback_point = RollbackPoint {
        rollback_id: Uuid::new_v4().to_string(),
        created_at,
        stage_id,
        cycle,
        git_sha: worktree.current_head_sha(base_dir).ok().flatten(),
        run_snapshot: snapshot.clone(),
    };

    rollback_store.write_rollback_point(base_dir, project_id, &rollback_point)?;

    *seq += 1;
    let event = journal::rollback_created_event(
        *seq,
        created_at,
        run_id,
        rollback_point.rollback_id.as_str(),
        rollback_point.stage_id,
        rollback_point.cycle,
        rollback_point.git_sha.as_deref(),
    );
    let line = journal::serialize_event(&event)?;
    if let Err(error) = journal_store.append_event(base_dir, project_id, &line) {
        *seq -= 1;
        return Err(AppError::StageCommitFailed {
            stage_id,
            details: format!("failed to persist rollback_created event: {}", error),
        });
    }

    Ok(())
}

fn complete_run(
    snapshot: &mut RunSnapshot,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    amendment_queue_port: &dyn AmendmentQueuePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
) -> AppResult<()> {
    // Completion guard: block completion if pending amendments remain.
    // On CompletionBlocked, persist a resumable (Failed, active_run=None) snapshot
    // so that `run resume` can pick the run back up.
    if let Err(e) = completion_guard(snapshot, amendment_queue_port, base_dir, project_id) {
        if matches!(&e, AppError::CompletionBlocked { .. }) {
            snapshot.status = RunStatus::Failed;
            snapshot.active_run = None;
            snapshot.status_summary = format!("blocked: {}", e);
            run_snapshot_write
                .write_run_snapshot(base_dir, project_id, snapshot)
                .map_err(|write_err| AppError::CompletionGuardSnapshotFailed {
                    details: format!(
                        "completion guard fired ({}) but resumable snapshot could not be persisted: {}",
                        e, write_err
                    ),
                })?;
        }
        return Err(e);
    }

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

#[allow(clippy::too_many_arguments)]
async fn checkpoint_failure_result<T>(
    error: AppError,
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
    if snapshot.status == RunStatus::Completed {
        return Err(origin.error(format!(
            "stage {} checkpoint failed after completion: {}",
            stage_id.as_str(),
            error
        )));
    }

    fail_run_result(
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
    .await
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

/// Build typed QueuedAmendment records from follow-up strings.
fn build_queued_amendments(
    follow_ups: &[String],
    source_stage: StageId,
    source_cycle: u32,
    source_completion_round: u32,
    run_id: &RunId,
) -> Vec<QueuedAmendment> {
    let now = Utc::now();
    follow_ups
        .iter()
        .enumerate()
        .map(|(idx, body)| QueuedAmendment {
            amendment_id: format!(
                "{}-{}-cr{}-amd{}",
                run_id.as_str(),
                source_stage.as_str(),
                source_completion_round,
                idx + 1
            ),
            source_stage,
            source_cycle,
            source_completion_round,
            body: body.clone(),
            created_at: now,
            batch_sequence: (idx + 1) as u32,
        })
        .collect()
}

fn build_recorded_follow_ups(
    follow_ups: &[String],
    source_stage: StageId,
    source_cycle: u32,
    source_completion_round: u32,
    run_id: &RunId,
) -> Vec<QueuedAmendment> {
    let now = Utc::now();
    follow_ups
        .iter()
        .enumerate()
        .map(|(idx, body)| QueuedAmendment {
            amendment_id: format!(
                "{}-{}-c{}-cr{}-follow-up{}",
                run_id.as_str(),
                source_stage.as_str(),
                source_cycle,
                source_completion_round,
                idx + 1
            ),
            source_stage,
            source_cycle,
            source_completion_round,
            body: body.clone(),
            created_at: now,
            batch_sequence: (idx + 1) as u32,
        })
        .collect()
}

/// Completion guard: blocks run_completed when pending amendments remain.
fn completion_guard(
    snapshot: &RunSnapshot,
    amendment_queue_port: &dyn AmendmentQueuePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<()> {
    // Check snapshot queue.
    if !snapshot.amendment_queue.pending.is_empty() {
        return Err(AppError::CompletionBlocked {
            details: format!(
                "completion blocked: {} pending amendments remain in snapshot queue",
                snapshot.amendment_queue.pending.len()
            ),
        });
    }

    // Check disk.
    if amendment_queue_port.has_pending_amendments(base_dir, project_id)? {
        return Err(AppError::CompletionBlocked {
            details: "completion blocked: pending amendment files exist on disk".to_owned(),
        });
    }

    Ok(())
}

/// Reconcile amendments from disk into the snapshot during resume.
fn reconcile_amendments_from_disk(
    snapshot: &mut RunSnapshot,
    amendment_queue_port: &dyn AmendmentQueuePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<()> {
    let disk_amendments = amendment_queue_port.list_pending_amendments(base_dir, project_id)?;
    if disk_amendments.is_empty() {
        return Ok(());
    }

    // Merge disk amendments into snapshot, avoiding duplicates by ID.
    let existing_ids: std::collections::HashSet<String> = snapshot
        .amendment_queue
        .pending
        .iter()
        .map(|a| a.amendment_id.clone())
        .collect();

    for amendment in disk_amendments {
        if !existing_ids.contains(&amendment.amendment_id) {
            snapshot.amendment_queue.pending.push(amendment);
        }
    }

    // Sort by (created_at, batch_sequence) for deterministic ordering.
    snapshot.amendment_queue.pending.sort_by(|a, b| {
        a.created_at
            .cmp(&b.created_at)
            .then_with(|| a.batch_sequence.cmp(&b.batch_sequence))
    });

    Ok(())
}

fn remediation_context(stage_id: StageId, next_cycle: u32, bundle: &ValidatedBundle) -> Value {
    match &bundle.payload {
        StagePayload::Validation(validation) => {
            remediation_context_from_validation(stage_id, next_cycle, validation)
        }
        _ => json!({}),
    }
}

fn remediation_context_from_validation(
    stage_id: StageId,
    next_cycle: u32,
    validation: &ValidationPayload,
) -> Value {
    json!({
        "source_stage": stage_id.as_str(),
        "cycle": next_cycle,
        "follow_up_or_amendments": validation.follow_up_or_amendments,
        "findings_or_gaps": validation.findings_or_gaps,
    })
}

fn invocation_context(
    cursor: &StageCursor,
    execution_context: Option<&Value>,
    pending_amendments: Option<&[QueuedAmendment]>,
) -> Value {
    let mut context = json!({
        "cycle": cursor.cycle,
        "attempt": cursor.attempt,
        "completion_round": cursor.completion_round,
    });

    if let Some(execution_context) = execution_context {
        context["remediation"] = execution_context.clone();
    }

    if let Some(amendments) = pending_amendments {
        if !amendments.is_empty() {
            let amendment_bodies: Vec<&str> = amendments.iter().map(|a| a.body.as_str()).collect();
            context["pending_amendments"] = json!(amendment_bodies);
        }
    }

    context
}

fn stage_index_for(stage_plan: &[StagePlan], stage_id: StageId) -> AppResult<usize> {
    stage_plan
        .iter()
        .position(|entry| entry.stage_id == stage_id)
        .ok_or_else(|| AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: format!("stage '{}' is not part of the active stage plan", stage_id),
        })
}

fn record_cycle_advance(snapshot: &mut RunSnapshot, next_cycle: u32, execution_stage: StageId) {
    snapshot.cycle_history.push(CycleHistoryEntry {
        cycle: next_cycle,
        stage_id: execution_stage,
        started_at: Utc::now(),
        completed_at: None,
    });
}

fn pending_remediation_cycle(
    snapshot: &RunSnapshot,
    current_cycle: u32,
    last_completed_stage: Option<StageId>,
    semantics: FlowSemantics,
) -> Option<u32> {
    let last_entry = snapshot.cycle_history.last()?;
    if !matches!(
        last_completed_stage,
        Some(stage_id) if semantics.remediation_trigger_stages.contains(&stage_id)
    ) {
        return None;
    }

    (last_entry.stage_id == semantics.execution_stage && last_entry.cycle > current_cycle)
        .then_some(last_entry.cycle)
}

fn derive_resume_execution_context(
    artifact_store: &dyn ArtifactStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    cursor: &StageCursor,
    events: &[JournalEvent],
    semantics: FlowSemantics,
) -> AppResult<Option<Value>> {
    if cursor.stage != semantics.execution_stage || cursor.cycle <= 1 {
        return Ok(None);
    }

    let prior_cycle = cursor.cycle - 1;
    let execution_label = semantics.execution_stage.as_str();
    let mut remediation_source = None;
    for event in events.iter().rev() {
        if event.event_type
            != crate::contexts::project_run_record::model::JournalEventType::StageCompleted
        {
            continue;
        }

        let stage_id = detail_stage_id(event, "stage_id")?;
        if !semantics.remediation_trigger_stages.contains(&stage_id) {
            continue;
        }

        if detail_u32(event, "cycle") != Some(prior_cycle) {
            continue;
        }

        remediation_source = Some((stage_id, detail_string(event, "payload_id")?.to_owned()));
        break;
    }

    let Some((stage_id, payload_id)) = remediation_source else {
        return Err(AppError::ResumeFailed {
            reason: format!(
                "failed to reconstruct remediation context for {} cycle {}; no durable validation payload was found for cycle {}",
                execution_label, cursor.cycle, prior_cycle
            ),
        });
    };

    let payloads = artifact_store
        .list_payloads(base_dir, project_id)
        .map_err(|error| AppError::ResumeFailed {
            reason: format!(
                "failed to load durable payload history for resume: {}",
                error
            ),
        })?;
    let payload_record = payloads
        .into_iter()
        .find(|record| record.payload_id == payload_id)
        .ok_or_else(|| AppError::ResumeFailed {
            reason: format!(
                "failed to reconstruct remediation context for {} cycle {}; payload '{}' is missing from durable history",
                execution_label, cursor.cycle, payload_id
            ),
        })?;
    let payload: StagePayload =
        serde_json::from_value(payload_record.payload).map_err(|error| AppError::ResumeFailed {
            reason: format!(
                "failed to parse remediation payload '{}' during resume: {}",
                payload_id, error
            ),
        })?;

    match payload {
        StagePayload::Validation(validation)
            if validation.outcome == ReviewOutcome::RequestChanges =>
        {
            Ok(Some(remediation_context_from_validation(
                stage_id,
                cursor.cycle,
                &validation,
            )))
        }
        StagePayload::Validation(validation) => Err(AppError::ResumeFailed {
            reason: format!(
                "failed to reconstruct remediation context for {} cycle {}; payload '{}' recorded outcome '{}' instead of 'Request Changes'",
                execution_label, cursor.cycle, payload_id, validation.outcome
            ),
        }),
        _ => Err(AppError::ResumeFailed {
            reason: format!(
                "failed to reconstruct remediation context for {} cycle {}; payload '{}' is not a validation payload",
                execution_label, cursor.cycle, payload_id
            ),
        }),
    }
}

fn stage_plan_for_resume(
    preset: FlowPreset,
    events: &[JournalEvent],
    effective_config: &EffectiveConfig,
) -> AppResult<Vec<StageId>> {
    match preset {
        FlowPreset::Standard => {
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
                StageId::PromptReview => Ok(stage_plan_for_flow(FlowPreset::Standard, true)),
                StageId::Planning => Ok(stage_plan_for_flow(FlowPreset::Standard, false)),
                _ => Ok(stage_plan_for_flow(
                    FlowPreset::Standard,
                    effective_config.prompt_review_enabled(),
                )),
            }
        }
        _ => Ok(stage_plan_for_flow(
            preset,
            effective_config.prompt_review_enabled(),
        )),
    }
}

fn derive_resume_state(
    events: &[JournalEvent],
    snapshot: &RunSnapshot,
    stage_plan: &[StagePlan],
    semantics: FlowSemantics,
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
    let execution_stage_index = stage_index_for(stage_plan, semantics.execution_stage)?;
    let planning_stage_index = stage_index_for(stage_plan, semantics.planning_stage)?;
    let mut current_cycle = snapshot
        .cycle_history
        .last()
        .map(|entry| entry.cycle)
        .unwrap_or(1);
    let mut current_completion_round = snapshot.completion_rounds.max(1);
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
                current_cycle = match detail_u32(event, "to_cycle") {
                    Some(to_cycle) => to_cycle,
                    None => current_cycle.checked_add(1).ok_or_else(|| {
                        AppError::StageCursorOverflow {
                            field: "cycle",
                            value: current_cycle,
                        }
                    })?,
                };
                next_stage_index = execution_stage_index;
            }
            crate::contexts::project_run_record::model::JournalEventType::CompletionRoundAdvanced => {
                current_completion_round = match detail_u32(event, "to_round") {
                    Some(to_round) => to_round,
                    None => current_completion_round.checked_add(1).ok_or_else(|| {
                        AppError::StageCursorOverflow {
                            field: "completion_round",
                            value: current_completion_round,
                        }
                    })?,
                };
                next_stage_index = planning_stage_index;
            }
            crate::contexts::project_run_record::model::JournalEventType::RunCompleted => {
                next_stage_index = stage_plan.len();
            }
            _ => {}
        }
    }

    if let Some(pending_cycle) =
        pending_remediation_cycle(snapshot, current_cycle, last_completed_stage, semantics)
    {
        current_cycle = pending_cycle;
        next_stage_index = execution_stage_index;
    }

    // If pending amendments exist, resume from planning to process them.
    if !snapshot.amendment_queue.pending.is_empty() && next_stage_index > planning_stage_index {
        next_stage_index = planning_stage_index;
    }

    if next_stage_index >= stage_plan.len() {
        return Err(AppError::ResumeFailed {
            reason:
                "all stages in the current flow are already complete; there is nothing to resume"
                    .to_owned(),
        });
    }

    let completion_round = current_completion_round;
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
