use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::adapters::fs::{FileSystem, FsArtifactStore, FsProjectStore, FsRollbackPointStore};
use crate::adapters::worktree::WorktreeAdapter;
use crate::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use crate::contexts::agent_execution::policy::{
    BackendPolicyService, FinalReviewPanelResolution, PromptReviewPanelResolution,
    ResolvedPanelMember,
};
use crate::contexts::agent_execution::service::{
    AgentExecutionPort, BackendSelectionConfig, RawOutputPort,
};
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::AgentExecutionService;
use crate::contexts::project_run_record::journal;
use crate::contexts::project_run_record::model::{
    ActiveRun, ArtifactRecord, CycleHistoryEntry, JournalEvent, JournalEventType, LogLevel,
    PayloadRecord, QueuedAmendment, ResolvedTargetRecord, RollbackPoint, RunSnapshot, RunStatus,
    RuntimeLogEntry, StageResolutionSnapshot,
};
use crate::contexts::project_run_record::queries;
use crate::contexts::project_run_record::service::{
    AmendmentQueuePort, ArtifactStorePort, JournalStorePort, PayloadArtifactWritePort,
    ProjectStorePort, RollbackPointStorePort, RunSnapshotPort, RunSnapshotWritePort,
    RuntimeLogWritePort,
};
use crate::contexts::workflow_composition::payloads::{
    ReviewOutcome, StagePayload, ValidationPayload,
};
use crate::contexts::workspace_governance::config::{
    EffectiveConfig, DEFAULT_MAX_COMPLETION_ROUNDS,
};
use crate::contexts::workspace_governance::template_catalog;
use crate::shared::domain::{
    BackendFamily, BackendPolicyRole, BackendRole, FailureClass, FlowPreset, ProjectId,
    ResolvedBackendTarget, RunId, SessionPolicy, StageCursor, StageId,
};
use crate::shared::error::{AppError, AppResult};

use super::checkpoints::VcsCheckpointPort;
use super::completion;
use super::contracts::{self, ValidatedBundle};
use super::drift::{self, PromptChangeResumeDecision};
use super::final_review;
use super::panel_contracts::{CompletionVerdict, RecordKind, RecordProducer};
use super::prompt_review;
use super::retry_policy::RetryPolicy;
use super::validation;
use super::{agent_record_producer, flow_semantics, stage_plan_for_flow, FlowSemantics};
use crate::adapters::validation_runner::ValidationGroupResult;

/// Compatibility wrapper for the legacy standard-flow helper.
pub fn standard_stage_plan(prompt_review_enabled: bool) -> Vec<StageId> {
    stage_plan_for_flow(FlowPreset::Standard, prompt_review_enabled)
}

/// Deterministic stage-to-role mapping per spec.
pub fn role_for_stage(stage_id: StageId) -> BackendRole {
    BackendRole::for_stage(stage_id)
}

#[allow(clippy::too_many_arguments)]
pub fn build_stage_prompt(
    artifact_store: &dyn ArtifactStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    project_root: &Path,
    prompt_reference: &str,
    role: BackendRole,
    contract: &contracts::StageContract,
    run_id: &RunId,
    cursor: &StageCursor,
    execution_context: Option<&Value>,
    pending_amendments: Option<&[QueuedAmendment]>,
) -> AppResult<String> {
    let prompt_path = project_root.join(prompt_reference);
    let project_prompt =
        fs::read_to_string(&prompt_path).map_err(|error| AppError::CorruptRecord {
            file: prompt_path.display().to_string(),
            details: format!(
                "failed to read project prompt '{}' while building stage prompt: {}",
                prompt_reference, error
            ),
        })?;

    let prior_outputs = load_prior_stage_outputs_this_cycle(
        artifact_store,
        base_dir,
        project_id,
        project_root,
        run_id,
        cursor,
    )?;
    let schema =
        serde_json::to_string_pretty(&InvocationContract::Stage(*contract).json_schema_value())?;

    // Pre-render optional sections
    let prior_outputs_block = if !prior_outputs.is_empty() {
        let mut section = String::from("## Prior Stage Outputs This Cycle");
        for record in &prior_outputs {
            let payload = serde_json::to_string_pretty(&record.payload)?;
            section.push_str(&format!(
                "\n\n### {} (`{}`)\n- Payload ID: `{}`\n- Attempt: `{}`\n\n```json\n{}\n```",
                record.stage_id.display_name(),
                record.stage_id.as_str(),
                record.payload_id,
                record.attempt,
                payload
            ));
        }
        section
    } else {
        String::new()
    };

    let remediation_block = if execution_context.is_some()
        || pending_amendments.is_some_and(|amendments| !amendments.is_empty())
    {
        let mut section = String::from("## Remediation / Pending Amendments");
        if let Some(remediation) = execution_context {
            section.push_str(&format!(
                "\n\n### Remediation Context\n\n```json\n{}\n```",
                serde_json::to_string_pretty(remediation)?
            ));
        }
        if let Some(amendments) = pending_amendments.filter(|amendments| !amendments.is_empty()) {
            let amendment_bodies: Vec<&str> = amendments
                .iter()
                .map(|amendment| amendment.body.as_str())
                .collect();
            section.push_str(&format!(
                "\n\n### Pending Amendments\n\n```json\n{}\n```",
                serde_json::to_string_pretty(&amendment_bodies)?
            ));
        }
        section
    } else {
        String::new()
    };

    let template_id = template_catalog::stage_template_id(contract.stage_id);
    let role_instruction = stage_role_instruction(role, contract.stage_id);

    template_catalog::resolve_and_render(
        template_id,
        base_dir,
        Some(project_id),
        &[
            ("role_instruction", &role_instruction),
            ("project_prompt", project_prompt.trim_end()),
            ("json_schema", &schema),
            ("prior_outputs", &prior_outputs_block),
            ("remediation", &remediation_block),
        ],
    )
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

fn resolve_stage_plan_for_cycle(
    stages: &[StageId],
    effective_config: &EffectiveConfig,
    cycle: u32,
) -> AppResult<Vec<StagePlan>> {
    let policy = BackendPolicyService::new(effective_config);
    let mut plan = Vec::with_capacity(stages.len());
    for &stage_id in stages {
        let role = role_for_stage(stage_id);
        let contract = contracts::contract_for_stage(stage_id);
        // Local-validation stages (docs_validation, ci_validation) run commands
        // locally and never invoke a backend agent, so they use a placeholder
        // target and skip backend resolution entirely.
        let target = if stage_id.is_local_validation() {
            ResolvedBackendTarget::new(BackendFamily::Claude, "local-validation-stub")
        } else {
            policy.resolve_stage_target(stage_id, cycle)?
        };
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
/// Local-validation stages are skipped since they run commands locally.
pub async fn preflight_check<A: AgentExecutionPort>(
    adapter: &A,
    effective_config: &EffectiveConfig,
    cycle: u32,
    plan: &[StagePlan],
) -> AppResult<()> {
    for entry in plan {
        if entry.stage_id.is_local_validation() {
            continue;
        }
        match entry.stage_id {
            StageId::PromptReview => {
                let policy = BackendPolicyService::new(effective_config);
                let panel = resolve_prompt_review_panel_for_preflight(&policy, cycle)?;
                preflight_required_panel_target(
                    adapter,
                    entry.stage_id,
                    "refiner",
                    &panel.refiner,
                    "prompt-review refiner",
                )
                .await?;
                preflight_panel_members(
                    adapter,
                    entry.stage_id,
                    "validator",
                    "prompt_review",
                    "prompt-review validator",
                    &panel.validators,
                    effective_config.prompt_review_policy().min_reviewers,
                )
                .await?;
            }
            StageId::CompletionPanel => {
                let policy = BackendPolicyService::new(effective_config);
                let panel = policy.resolve_completion_panel(cycle).map_err(|error| {
                    AppError::PreflightFailed {
                        stage_id: entry.stage_id,
                        details: format!("completion panel resolution failed: {error}"),
                    }
                })?;
                preflight_panel_members(
                    adapter,
                    entry.stage_id,
                    "completer",
                    "completion",
                    "completion completer",
                    &panel.completers,
                    effective_config.completion_policy().min_completers,
                )
                .await?;
            }
            StageId::FinalReview => {
                let policy = BackendPolicyService::new(effective_config);
                let panel = resolve_final_review_panel_for_preflight(&policy, cycle)?;
                preflight_required_panel_target(
                    adapter,
                    entry.stage_id,
                    "planner",
                    &panel.planner,
                    "final-review planner",
                )
                .await?;
                preflight_required_panel_target(
                    adapter,
                    entry.stage_id,
                    "arbiter",
                    &panel.arbiter,
                    "final-review arbiter",
                )
                .await?;
                preflight_panel_members(
                    adapter,
                    entry.stage_id,
                    "reviewer",
                    "final_review",
                    "final-review reviewer",
                    &panel.reviewers,
                    effective_config.final_review_policy().min_reviewers,
                )
                .await?;
            }
            _ => {
                adapter
                    .check_capability(&entry.target, &InvocationContract::Stage(entry.contract))
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
        }
    }
    Ok(())
}

fn resolve_prompt_review_panel_for_preflight(
    policy: &BackendPolicyService<'_>,
    cycle: u32,
) -> AppResult<PromptReviewPanelResolution> {
    let refiner = policy
        .resolve_role_target(BackendPolicyRole::PromptReviewer, cycle)
        .map_err(|error| AppError::PreflightFailed {
            stage_id: StageId::PromptReview,
            details: format!("required prompt-review refiner resolution failed: {error}"),
        })?;
    let mut panel =
        policy
            .resolve_prompt_review_panel(cycle)
            .map_err(|error| AppError::PreflightFailed {
                stage_id: StageId::PromptReview,
                details: format!("prompt-review validator resolution failed: {error}"),
            })?;
    panel.refiner = refiner;
    Ok(panel)
}

fn resolve_final_review_panel_for_preflight(
    policy: &BackendPolicyService<'_>,
    cycle: u32,
) -> AppResult<FinalReviewPanelResolution> {
    let planner = policy
        .resolve_role_target(BackendPolicyRole::Planner, cycle)
        .map_err(|error| AppError::PreflightFailed {
            stage_id: StageId::FinalReview,
            details: format!("required final-review planner resolution failed: {error}"),
        })?;
    let arbiter = policy
        .resolve_role_target(BackendPolicyRole::Arbiter, cycle)
        .map_err(|error| AppError::PreflightFailed {
            stage_id: StageId::FinalReview,
            details: format!("required final-review arbiter resolution failed: {error}"),
        })?;
    let mut panel =
        policy
            .resolve_final_review_panel(cycle)
            .map_err(|error| AppError::PreflightFailed {
                stage_id: StageId::FinalReview,
                details: format!("final-review reviewer resolution failed: {error}"),
            })?;
    panel.planner = planner;
    panel.arbiter = arbiter;
    Ok(panel)
}

async fn preflight_required_panel_target<A: AgentExecutionPort>(
    adapter: &A,
    stage_id: StageId,
    role: &'static str,
    target: &ResolvedBackendTarget,
    member_name: &str,
) -> AppResult<()> {
    let contract = InvocationContract::Panel {
        stage_id,
        role: role.to_owned(),
    };
    adapter
        .check_capability(target, &contract)
        .await
        .map_err(|error| AppError::PreflightFailed {
            stage_id,
            details: format!(
                "required {member_name} failed capability preflight for '{}': {error}",
                contract.label()
            ),
        })?;
    adapter
        .check_availability(target)
        .await
        .map_err(|error| AppError::PreflightFailed {
            stage_id,
            details: format!("required {member_name} failed availability preflight: {error}"),
        })?;
    Ok(())
}

async fn preflight_panel_members<A: AgentExecutionPort>(
    adapter: &A,
    stage_id: StageId,
    role: &'static str,
    panel_name: &'static str,
    member_name: &str,
    members: &[ResolvedPanelMember],
    minimum: usize,
) -> AppResult<()> {
    let mut available_members = 0usize;

    for member in members {
        let contract = InvocationContract::Panel {
            stage_id,
            role: role.to_owned(),
        };
        let required_prefix = if member.required {
            "required"
        } else {
            "optional"
        };

        match adapter.check_capability(&member.target, &contract).await {
            Ok(()) => {}
            Err(error) => {
                if member.required {
                    return Err(AppError::PreflightFailed {
                        stage_id,
                        details: format!(
                            "{required_prefix} {member_name} failed capability preflight for '{}': {error}",
                            contract.label()
                        ),
                    });
                }
                continue;
            }
        }

        match adapter.check_availability(&member.target).await {
            Ok(()) => available_members += 1,
            Err(error) => {
                if member.required {
                    return Err(AppError::PreflightFailed {
                        stage_id,
                        details: format!(
                            "{required_prefix} {member_name} failed availability preflight: {error}",
                        ),
                    });
                }
            }
        }
    }

    if available_members < minimum {
        return Err(AppError::PreflightFailed {
            stage_id,
            details: AppError::InsufficientPanelMembers {
                panel: panel_name.to_owned(),
                resolved: available_members,
                minimum,
            }
            .to_string(),
        });
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
        "{}-{}-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );
    if rollback_count == 0 {
        base_id
    } else {
        format!("{base_id}-rb{rollback_count}")
    }
}

fn project_prompt_hash(project_root: &Path, prompt_reference: &str) -> AppResult<String> {
    let prompt_path = project_root.join(prompt_reference);
    let prompt = fs::read_to_string(&prompt_path).map_err(|error| AppError::CorruptRecord {
        file: prompt_path.display().to_string(),
        details: format!("failed to read prompt while computing hash: {error}"),
    })?;
    Ok(FileSystem::prompt_hash(&prompt))
}

#[allow(clippy::too_many_arguments)]
fn build_active_run(
    run_id: &RunId,
    stage_cursor: StageCursor,
    started_at: DateTime<Utc>,
    prompt_hash_at_cycle_start: String,
    prompt_hash_at_stage_start: String,
    qa_iterations_current_cycle: u32,
    review_iterations_current_cycle: u32,
    final_review_restart_count: u32,
    stage_resolution_snapshot: Option<StageResolutionSnapshot>,
) -> ActiveRun {
    ActiveRun {
        run_id: run_id.as_str().to_owned(),
        stage_cursor,
        started_at,
        prompt_hash_at_cycle_start,
        prompt_hash_at_stage_start,
        qa_iterations_current_cycle,
        review_iterations_current_cycle,
        final_review_restart_count,
        stage_resolution_snapshot,
    }
}

fn current_active_run(snapshot: &RunSnapshot) -> AppResult<&ActiveRun> {
    snapshot
        .active_run
        .as_ref()
        .ok_or_else(|| AppError::CorruptRecord {
            file: "run.json".to_owned(),
            details: "running snapshot lost active_run metadata".to_owned(),
        })
}

fn interrupted_active_run(snapshot: &RunSnapshot) -> AppResult<&ActiveRun> {
    snapshot
        .interrupted_run
        .as_ref()
        .ok_or_else(|| AppError::ResumeFailed {
            reason: "run snapshot lost interrupted active_run metadata needed for resume"
                .to_owned(),
        })
}

fn preserve_interrupted_run(snapshot: &mut RunSnapshot) {
    snapshot.interrupted_run = snapshot.active_run.clone();
}

fn carry_forward_active_run(
    snapshot: &RunSnapshot,
    run_id: &RunId,
    stage_cursor: StageCursor,
    prompt_hash_at_stage_start: String,
    stage_resolution_snapshot: Option<StageResolutionSnapshot>,
) -> AppResult<ActiveRun> {
    let current = current_active_run(snapshot)?;
    Ok(build_active_run(
        run_id,
        stage_cursor,
        current.started_at,
        current.prompt_hash_at_cycle_start.clone(),
        prompt_hash_at_stage_start,
        current.qa_iterations_current_cycle,
        current.review_iterations_current_cycle,
        current.final_review_restart_count,
        stage_resolution_snapshot,
    ))
}

#[allow(clippy::too_many_arguments)]
fn reset_cycle_active_run(
    snapshot: &RunSnapshot,
    run_id: &RunId,
    stage_cursor: StageCursor,
    prompt_hash: String,
    qa_iterations_current_cycle: u32,
    review_iterations_current_cycle: u32,
    final_review_restart_count: u32,
    stage_resolution_snapshot: Option<StageResolutionSnapshot>,
) -> AppResult<ActiveRun> {
    let current = current_active_run(snapshot)?;
    Ok(build_active_run(
        run_id,
        stage_cursor,
        current.started_at,
        prompt_hash.clone(),
        prompt_hash,
        qa_iterations_current_cycle,
        review_iterations_current_cycle,
        final_review_restart_count,
        stage_resolution_snapshot,
    ))
}

fn advance_completion_round_active_run(
    snapshot: &RunSnapshot,
    run_id: &RunId,
    stage_cursor: StageCursor,
    prompt_hash: String,
    final_review_restart_count: u32,
    stage_resolution_snapshot: Option<StageResolutionSnapshot>,
) -> AppResult<ActiveRun> {
    let current = current_active_run(snapshot)?;
    Ok(build_active_run(
        run_id,
        stage_cursor,
        current.started_at,
        current.prompt_hash_at_cycle_start.clone(),
        prompt_hash,
        current.qa_iterations_current_cycle,
        current.review_iterations_current_cycle,
        final_review_restart_count,
        stage_resolution_snapshot,
    ))
}

fn count_final_review_restarts(events: &[JournalEvent]) -> u32 {
    events
        .iter()
        .filter(|event| {
            event.event_type == JournalEventType::CompletionRoundAdvanced
                && event.details.get("source_stage").and_then(Value::as_str)
                    == Some(StageId::FinalReview.as_str())
        })
        .count() as u32
}

fn resume_final_review_restart_count(
    snapshot: &RunSnapshot,
    events: &[JournalEvent],
) -> AppResult<u32> {
    let interrupted = interrupted_active_run(snapshot)?;
    Ok(interrupted
        .final_review_restart_count
        .max(count_final_review_restarts(events)))
}

fn prompt_change_baseline(snapshot: &RunSnapshot) -> AppResult<String> {
    Ok(interrupted_active_run(snapshot)?
        .prompt_hash_at_cycle_start
        .clone())
}

fn resume_iteration_counters(
    snapshot: &RunSnapshot,
    resume_cursor: &StageCursor,
) -> AppResult<(u32, u32)> {
    let interrupted = interrupted_active_run(snapshot)?;
    if interrupted.stage_cursor.cycle != resume_cursor.cycle {
        return Ok((0, 0));
    }

    Ok((
        interrupted.qa_iterations_current_cycle,
        interrupted.review_iterations_current_cycle,
    ))
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
    let checkpoint_port = WorktreeAdapter;
    execute_run_with_retry_internal(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        &rollback_store,
        &checkpoint_port,
        base_dir,
        None,
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
    execution_cwd: Option<&Path>,
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
    let checkpoint_port = WorktreeAdapter;
    execute_run_with_retry_internal(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        &rollback_store,
        &checkpoint_port,
        base_dir,
        execution_cwd,
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
    checkpoint_port: &dyn VcsCheckpointPort,
    base_dir: &Path,
    execution_cwd: Option<&Path>,
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
    let project_record = FsProjectStore.read_project_record(base_dir, project_id)?;
    let artifact_store = FsArtifactStore;
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
    let _workspace_defaults = BackendSelectionConfig::from_effective_config(effective_config)?;
    let stage_plan = resolve_stage_plan_for_cycle(stage_ids.as_slice(), effective_config, 1)?;
    preflight_check(agent_service.adapter(), effective_config, 1, &stage_plan).await?;

    let run_id = generate_run_id()?;
    let now = Utc::now();
    let project_root = project_root_path(base_dir, project_id);
    let current_prompt_hash =
        project_prompt_hash(&project_root, project_record.prompt_reference.as_str())?;
    let events = journal_store.read_journal(base_dir, project_id)?;
    let mut seq = journal::last_sequence(&events);
    let first_stage = stage_plan[0].stage_id;
    let initial_cursor = StageCursor::initial(first_stage);
    let mut current_snapshot = RunSnapshot {
        active_run: Some(build_active_run(
            &run_id,
            initial_cursor.clone(),
            now,
            current_prompt_hash.clone(),
            current_prompt_hash,
            0,
            0,
            0,
            None,
        )),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: snapshot.cycle_history.clone(),
        completion_rounds: 1,
        rollback_point_meta: snapshot.rollback_point_meta.clone(),
        amendment_queue: snapshot.amendment_queue.clone(),
        status_summary: format!("running: {}", first_stage.display_name()),
        last_stage_resolution_snapshot: None,
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
        &artifact_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        rollback_store,
        checkpoint_port,
        base_dir,
        execution_cwd,
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
        project_record.prompt_reference.as_str(),
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
        None,
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
    let checkpoint_port = WorktreeAdapter;
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
        &checkpoint_port,
        base_dir,
        None,
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
    execution_cwd: Option<&Path>,
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
    let checkpoint_port = WorktreeAdapter;
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
        &checkpoint_port,
        base_dir,
        execution_cwd,
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
    checkpoint_port: &dyn VcsCheckpointPort,
    base_dir: &Path,
    execution_cwd: Option<&Path>,
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
    let project_record = FsProjectStore.read_project_record(base_dir, project_id)?;
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
    let _workspace_defaults = BackendSelectionConfig::from_effective_config(effective_config)?;
    let resume_cycle = snapshot
        .cycle_history
        .last()
        .map(|entry| entry.cycle)
        .unwrap_or(1);
    let stage_plan =
        resolve_stage_plan_for_cycle(stage_ids.as_slice(), effective_config, resume_cycle)?;
    let project_root = project_root_path(base_dir, project_id);
    // Reconcile amendments from disk into snapshot before deriving resume state.
    reconcile_amendments_from_disk(
        &mut snapshot,
        &visible_events,
        amendment_queue_port,
        base_dir,
        project_id,
    )?;

    let mut resume_state = derive_resume_state(&visible_events, &snapshot, &stage_plan, semantics)?;
    let mut execution_context = derive_resume_execution_context(
        artifact_store,
        base_dir,
        project_id,
        &resume_state.cursor,
        &visible_events,
        semantics,
    )?;

    let mut seq = journal::last_sequence(&events);
    let prompt_change_baseline = prompt_change_baseline(&snapshot)?;
    let stage_id_plan = stage_plan
        .iter()
        .map(|entry| entry.stage_id)
        .collect::<Vec<_>>();
    let (current_prompt_hash, prompt_hash_at_cycle_start) =
        match drift::evaluate_prompt_change_on_resume(
            artifact_store,
            artifact_write,
            run_snapshot_write,
            journal_store,
            log_write,
            base_dir,
            project_id,
            &project_root,
            project_record.prompt_reference.as_str(),
            &resume_state.run_id,
            &mut seq,
            &mut snapshot,
            &resume_state.cursor,
            &stage_id_plan,
            semantics.planning_stage,
            &prompt_change_baseline,
            effective_config.run_policy().prompt_change_action,
        )? {
            PromptChangeResumeDecision::NoChange {
                current_prompt_hash,
            }
            | PromptChangeResumeDecision::Continue {
                current_prompt_hash,
            } => (current_prompt_hash, prompt_change_baseline.clone()),
            PromptChangeResumeDecision::RestartCycle {
                current_prompt_hash,
                next_cursor,
                next_stage_index,
            } => {
                resume_state.cursor = next_cursor;
                resume_state.stage_index = next_stage_index;
                execution_context = None;
                (current_prompt_hash.clone(), current_prompt_hash)
            }
        };

    // ── Resume drift detection (runs BEFORE preflight) ────────────────────────
    // Re-resolve the current stage or panel, compare against the persisted
    // snapshot, and either warn+continue or fail early. This must happen before
    // preflight so that drift-induced failures take precedence.
    if let Some(old_snapshot) = snapshot.last_stage_resolution_snapshot.clone() {
        let policy = BackendPolicyService::new(effective_config);
        let current_stage = resume_state.cursor.stage;
        // Re-resolve with runtime availability filtering so the drift
        // comparison reflects the actual executable panel, not just
        // config-enabled state. Required unavailable backends fail here;
        // optional unavailable backends are removed before comparison.
        let new_snapshot = match current_stage {
            StageId::PromptReview => {
                let mut panel = policy.resolve_prompt_review_panel(resume_state.cursor.cycle)?;
                // Refiner is always required — fail early if unavailable.
                agent_service
                    .adapter()
                    .check_availability(&panel.refiner)
                    .await
                    .map_err(|_| AppError::ResumeDriftFailure {
                        stage_id: current_stage,
                        details: format!(
                            "required prompt-review refiner ({}) unavailable on resume",
                            panel.refiner.backend.family,
                        ),
                    })?;
                let min_reviewers = effective_config.prompt_review_policy().min_reviewers;
                let mut available = Vec::new();
                for member in &panel.validators {
                    match agent_service
                        .adapter()
                        .check_availability(&member.target)
                        .await
                    {
                        Ok(()) => available.push(member.clone()),
                        Err(e) => {
                            if member.required {
                                return Err(AppError::ResumeDriftFailure {
                                    stage_id: current_stage,
                                    details: format!("required prompt-review validator unavailable on resume: {e}"),
                                });
                            }
                        }
                    }
                }
                if available.len() < min_reviewers {
                    return Err(AppError::ResumeDriftFailure {
                        stage_id: current_stage,
                        details: format!(
                            "available prompt-review validators ({}) < min_reviewers ({}) on resume",
                            available.len(), min_reviewers,
                        ),
                    });
                }
                panel.validators = available;
                build_prompt_review_snapshot(current_stage, &panel)
            }
            StageId::CompletionPanel => {
                let mut panel = policy.resolve_completion_panel(resume_state.cursor.cycle)?;
                let min_completers = effective_config.completion_policy().min_completers;
                let mut available = Vec::new();
                for member in &panel.completers {
                    match agent_service
                        .adapter()
                        .check_availability(&member.target)
                        .await
                    {
                        Ok(()) => available.push(member.clone()),
                        Err(e) => {
                            if member.required {
                                return Err(AppError::ResumeDriftFailure {
                                    stage_id: current_stage,
                                    details: format!(
                                        "required completer unavailable on resume: {e}"
                                    ),
                                });
                            }
                        }
                    }
                }
                if available.len() < min_completers {
                    return Err(AppError::ResumeDriftFailure {
                        stage_id: current_stage,
                        details: format!(
                            "available completers ({}) < min_completers ({}) on resume",
                            available.len(),
                            min_completers,
                        ),
                    });
                }
                panel.completers = available;
                build_completion_snapshot(current_stage, &panel.completers)
            }
            StageId::FinalReview => {
                let mut panel = policy.resolve_final_review_panel(resume_state.cursor.cycle)?;
                let min_reviewers = effective_config.final_review_policy().min_reviewers;
                agent_service
                    .adapter()
                    .check_availability(&panel.planner)
                    .await
                    .map_err(|_| AppError::ResumeDriftFailure {
                        stage_id: current_stage,
                        details: format!(
                            "required final-review planner ({}) unavailable on resume",
                            panel.planner.backend.family,
                        ),
                    })?;
                agent_service
                    .adapter()
                    .check_availability(&panel.arbiter)
                    .await
                    .map_err(|_| AppError::ResumeDriftFailure {
                        stage_id: current_stage,
                        details: format!(
                            "required final-review arbiter ({}) unavailable on resume",
                            panel.arbiter.backend.family,
                        ),
                    })?;
                let mut available = Vec::new();
                for member in &panel.reviewers {
                    match agent_service
                        .adapter()
                        .check_availability(&member.target)
                        .await
                    {
                        Ok(()) => available.push(member.clone()),
                        Err(e) => {
                            if member.required {
                                return Err(AppError::ResumeDriftFailure {
                                    stage_id: current_stage,
                                    details: format!(
                                        "required final-review reviewer unavailable on resume: {e}"
                                    ),
                                });
                            }
                        }
                    }
                }
                if available.len() < min_reviewers {
                    return Err(AppError::ResumeDriftFailure {
                        stage_id: current_stage,
                        details: format!(
                            "available final-review reviewers ({}) < min_reviewers ({}) on resume",
                            available.len(),
                            min_reviewers,
                        ),
                    });
                }
                panel.reviewers = available;
                build_final_review_snapshot(
                    current_stage,
                    &panel.reviewers,
                    &panel.planner,
                    &panel.arbiter,
                )
            }
            _ => {
                let target =
                    policy.resolve_stage_target(current_stage, resume_state.cursor.cycle)?;
                build_single_target_snapshot(current_stage, &target)
            }
        };

        let legacy_final_review_snapshot_needs_upgrade = current_stage == StageId::FinalReview
            && old_snapshot.final_review_planner.is_none()
            && new_snapshot.final_review_planner.is_some();

        if resolution_has_drifted(&old_snapshot, &new_snapshot) {
            // Fail early if requirements no longer met.
            drift_still_satisfies_requirements(&new_snapshot, current_stage, effective_config)?;
            // Warn and update snapshot.
            emit_resume_drift_warning(
                &old_snapshot,
                &new_snapshot,
                &resume_state.run_id,
                current_stage,
                &mut seq,
                &mut snapshot,
                journal_store,
                run_snapshot_write,
                log_write,
                base_dir,
                project_id,
            )?;
        } else if legacy_final_review_snapshot_needs_upgrade {
            // Old final-review snapshots created before planner tracking should
            // silently adopt the re-resolved planner so later resumes have a
            // durable baseline even though no drift warning is emitted.
            // Persist immediately so the upgrade survives a preflight failure.
            snapshot.last_stage_resolution_snapshot = Some(new_snapshot.clone());
            run_snapshot_write.write_run_snapshot(base_dir, project_id, &snapshot)?;
        }
    }

    preflight_check(
        agent_service.adapter(),
        effective_config,
        resume_state.cursor.cycle,
        &stage_plan[resume_state.stage_index..],
    )
    .await
    .map_err(|error| AppError::ResumeFailed {
        reason: error.to_string(),
    })?;

    // Seed the resumed ActiveRun with the (potentially updated) resolution
    // snapshot from drift detection so the stage can compare against it later.
    let resumed_snapshot = snapshot.last_stage_resolution_snapshot.clone();
    let (qa_iterations_current_cycle, review_iterations_current_cycle) =
        resume_iteration_counters(&snapshot, &resume_state.cursor)?;
    let final_review_restart_count = resume_final_review_restart_count(&snapshot, &visible_events)?;
    snapshot.status = RunStatus::Running;
    snapshot.active_run = Some(build_active_run(
        &resume_state.run_id,
        resume_state.cursor.clone(),
        resume_state.started_at,
        prompt_hash_at_cycle_start,
        current_prompt_hash.clone(),
        qa_iterations_current_cycle,
        review_iterations_current_cycle,
        final_review_restart_count,
        resumed_snapshot,
    ));
    snapshot.interrupted_run = None;
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
        resume_state.cursor.completion_round,
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
        artifact_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        rollback_store,
        checkpoint_port,
        base_dir,
        execution_cwd,
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
        project_record.prompt_reference.as_str(),
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
        None,
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
    artifact_store: &dyn ArtifactStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    amendment_queue_port: &dyn AmendmentQueuePort,
    rollback_store: &dyn RollbackPointStorePort,
    checkpoint_port: &dyn VcsCheckpointPort,
    base_dir: &Path,
    execution_cwd: Option<&Path>,
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
    prompt_reference: &str,
    effective_config: &EffectiveConfig,
) -> AppResult<RunOutcome>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let project_root = project_root_path(base_dir, project_id);
    // When a worktree is active, backends must execute there (not in base_dir)
    // so they see/edit the worktree's working tree. State storage still uses base_dir.
    let backend_working_dir = execution_cwd.unwrap_or(base_dir).to_path_buf();
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

        // ── Panel dispatch: PromptReview ──────────────────────────────────────
        if stage_id == StageId::PromptReview {
            let panel_result = dispatch_prompt_review_panel(
                agent_service,
                artifact_write,
                log_write,
                run_snapshot_write,
                journal_store,
                base_dir,
                &project_root,
                &backend_working_dir,
                project_id,
                run_id,
                seq,
                snapshot,
                &cursor,
                effective_config,
                prompt_reference,
                cancellation_token.clone(),
                origin,
            )
            .await;

            match panel_result {
                Ok(_completed_cursor) => {
                    // Persist rollback point after successful prompt review.
                    if let Err(error) = persist_rollback_point(
                        rollback_store,
                        journal_store,
                        log_write,
                        checkpoint_port,
                        base_dir,
                        execution_cwd.unwrap_or(base_dir),
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

                    // Advance to next stage.
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
                        return Ok(RunOutcome::Completed);
                    }
                    let next_stage = stage_plan[stage_index + 1].stage_id;
                    cursor = cursor.advance_stage(next_stage);
                    let current_prompt_hash = project_prompt_hash(&project_root, prompt_reference)?;
                    snapshot.status = RunStatus::Running;
                    snapshot.active_run = Some(build_active_run(
                        run_id,
                        cursor.clone(),
                        snapshot_started_at(snapshot)?,
                        current_prompt_hash.clone(),
                        current_prompt_hash,
                        0,
                        0,
                        current_active_run(snapshot)?.final_review_restart_count,
                        None,
                    ));
                    snapshot.status_summary = format!(
                        "running: completed {}, next {}",
                        stage_id.display_name(),
                        next_stage.display_name()
                    );
                    if let Err(error) =
                        run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)
                    {
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
                    continue;
                }
                Err(error) => {
                    // dispatch_prompt_review_panel persisted supporting records but
                    // did not write stage_completed or change prompt.md on failure.
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

        // ── Panel dispatch: CompletionPanel ───────────────────────────────────
        if stage_id == StageId::CompletionPanel {
            let panel_result = dispatch_completion_panel(
                agent_service,
                artifact_write,
                log_write,
                run_snapshot_write,
                journal_store,
                base_dir,
                &project_root,
                &backend_working_dir,
                project_id,
                run_id,
                seq,
                snapshot,
                &cursor,
                effective_config,
                prompt_reference,
                cancellation_token.clone(),
                origin,
            )
            .await;

            match panel_result {
                Ok(CompletionPanelOutcome::Complete(completed_cursor, commit_data)) => {
                    cursor = completed_cursor;

                    // ── Completion failure invariant ──────────────────────
                    // Persist aggregate records (payload/artifact) first
                    // (reversible), then write stage_completed LAST as the
                    // journal commit point.  If any step before
                    // stage_completed fails, we clean up aggregate records
                    // so no aggregate or stage_completed leaks and resume
                    // restarts from completion_panel.

                    // Step 1: persist aggregate payload/artifact (reversible).
                    if let Err(error) = persist_completion_aggregate_records(
                        artifact_write,
                        base_dir,
                        project_id,
                        &cursor,
                        stage_id,
                        &commit_data,
                    ) {
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

                    if stage_index + 1 == stage_plan.len() {
                        // Last stage: stage_completed is the commit point.
                        *seq += 1;
                        let sc = journal::stage_completed_event(
                            *seq,
                            Utc::now(),
                            run_id,
                            stage_id,
                            cursor.cycle,
                            cursor.attempt,
                            &commit_data.payload_id,
                            &commit_data.artifact_id,
                        );
                        let sc_line = journal::serialize_event(&sc)?;
                        if let Err(error) =
                            journal_store.append_event(base_dir, project_id, &sc_line)
                        {
                            *seq -= 1;
                            cleanup_completion_aggregate_records(
                                artifact_write,
                                base_dir,
                                project_id,
                                &commit_data,
                            );
                            return Err(AppError::StageCommitFailed {
                                stage_id,
                                details: format!("journal append failed during completion aggregate commit: {error}"),
                            });
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
                        return Ok(RunOutcome::Completed);
                    }

                    // Step 2: advance cursor snapshot (best-effort, overwritten on resume).
                    let next_stage = stage_plan[stage_index + 1].stage_id;
                    let advanced_cursor = cursor.advance_stage(next_stage);
                    // Preserve the completion panel's resolution snapshot before
                    // clearing active_run.  If the commit point (Step 3) fails,
                    // fail_run will retain this so resume drift detection still
                    // has the original panel resolution.
                    snapshot.last_stage_resolution_snapshot = snapshot
                        .active_run
                        .as_ref()
                        .and_then(|ar| ar.stage_resolution_snapshot.clone());
                    snapshot.status = RunStatus::Running;
                    snapshot.active_run = Some(carry_forward_active_run(
                        snapshot,
                        run_id,
                        advanced_cursor.clone(),
                        project_prompt_hash(&project_root, prompt_reference)?,
                        None,
                    )?);
                    snapshot.status_summary = format!(
                        "running: completed {}, next {}",
                        stage_id.display_name(),
                        next_stage.display_name()
                    );
                    if let Err(error) =
                        run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)
                    {
                        cleanup_completion_aggregate_records(
                            artifact_write,
                            base_dir,
                            project_id,
                            &commit_data,
                        );
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

                    // Step 3: stage_completed is the journal commit point (LAST write).
                    // After this succeeds, the completion is durable and resume
                    // advances past completion_panel.
                    *seq += 1;
                    let sc = journal::stage_completed_event(
                        *seq,
                        Utc::now(),
                        run_id,
                        stage_id,
                        cursor.cycle,
                        cursor.attempt,
                        &commit_data.payload_id,
                        &commit_data.artifact_id,
                    );
                    let sc_line = journal::serialize_event(&sc)?;
                    if let Err(error) = journal_store.append_event(base_dir, project_id, &sc_line) {
                        *seq -= 1;
                        cleanup_completion_aggregate_records(
                            artifact_write,
                            base_dir,
                            project_id,
                            &commit_data,
                        );
                        return fail_run_result(
                            &AppError::StageCommitFailed {
                                stage_id,
                                details: format!(
                                    "journal append failed during completion aggregate commit: {error}",
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
                            message: format!("stage_completed: {}", stage_id.as_str()),
                        },
                    );
                    cursor = advanced_cursor;

                    // Persist rollback point after completion aggregate.
                    if let Err(error) = persist_rollback_point(
                        rollback_store,
                        journal_store,
                        log_write,
                        checkpoint_port,
                        base_dir,
                        execution_cwd.unwrap_or(base_dir),
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
                    continue;
                }
                Ok(CompletionPanelOutcome::ContinueWork(next_cursor, commit_data)) => {
                    // Safety limit: prevent infinite completion round loops.
                    let max_rounds = std::env::var("RALPH_BURNING_TEST_MAX_COMPLETION_ROUNDS")
                        .ok()
                        .and_then(|v| v.parse::<u32>().ok())
                        .unwrap_or(DEFAULT_MAX_COMPLETION_ROUNDS);
                    if next_cursor.completion_round > max_rounds {
                        return fail_run_result(
                            &AppError::StageCommitFailed {
                                stage_id,
                                details: format!("max completion rounds ({}) exceeded", max_rounds),
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

                    let planning_index = stage_index_for(stage_plan, semantics.planning_stage)?;
                    let from_round = cursor.completion_round;
                    let to_round = next_cursor.completion_round;

                    // ── Completion failure invariant (ContinueWork) ───────
                    // Persist aggregate records and cursor snapshot first
                    // (both reversible), then write completion_round_advanced
                    // as the journal commit point LAST. NO stage_completed
                    // is written for ContinueWork: the round has not
                    // "completed" the stage, it has transitioned to a new
                    // round. Resume uses CompletionRoundAdvanced to restart
                    // from planning.
                    //
                    // If any step fails before the journal commit point,
                    // aggregate records are cleaned up and fail_run_result
                    // overwrites the snapshot, so resume restarts from
                    // completion_panel.

                    // Step 1: persist aggregate payload/artifact (reversible).
                    if let Err(error) = persist_completion_aggregate_records(
                        artifact_write,
                        base_dir,
                        project_id,
                        &cursor,
                        stage_id,
                        &commit_data,
                    ) {
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

                    // Step 2: advance cursor snapshot (reversible — overwritten
                    // by fail_run_result if the journal commit fails).
                    // Preserve the completion panel's resolution snapshot before
                    // clearing active_run.  If the commit point (Step 3) fails,
                    // fail_run will retain this so resume drift detection still
                    // has the original panel resolution.
                    snapshot.last_stage_resolution_snapshot = snapshot
                        .active_run
                        .as_ref()
                        .and_then(|ar| ar.stage_resolution_snapshot.clone());
                    snapshot.completion_rounds =
                        snapshot.completion_rounds.max(next_cursor.completion_round);
                    snapshot.status = RunStatus::Running;
                    snapshot.active_run = Some(advance_completion_round_active_run(
                        snapshot,
                        run_id,
                        next_cursor.clone(),
                        project_prompt_hash(&project_root, prompt_reference)?,
                        current_active_run(snapshot)?.final_review_restart_count,
                        None,
                    )?);
                    snapshot.status_summary = format!(
                        "running: completion round {} -> {}",
                        from_round,
                        next_cursor.stage.display_name()
                    );
                    if let Err(error) =
                        run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)
                    {
                        cleanup_completion_aggregate_records(
                            artifact_write,
                            base_dir,
                            project_id,
                            &commit_data,
                        );
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

                    // Step 3: completion_round_advanced is the journal commit
                    // point (LAST write). After this succeeds, the round
                    // transition is durable and resume goes to planning.
                    *seq += 1;
                    let round_event = journal::completion_round_advanced_event(
                        *seq,
                        Utc::now(),
                        run_id,
                        stage_id,
                        from_round,
                        to_round,
                        0, // no amendments from completion panel
                    );
                    let round_event_line = journal::serialize_event(&round_event)?;
                    if let Err(error) =
                        journal_store.append_event(base_dir, project_id, &round_event_line)
                    {
                        *seq -= 1;
                        cleanup_completion_aggregate_records(
                            artifact_write,
                            base_dir,
                            project_id,
                            &commit_data,
                        );
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
                    let _ = log_write.append_runtime_log(
                        base_dir,
                        project_id,
                        &RuntimeLogEntry {
                            timestamp: Utc::now(),
                            level: LogLevel::Info,
                            source: "engine".to_owned(),
                            message: format!(
                                "completion round advanced: {} -> {}",
                                from_round, to_round
                            ),
                        },
                    );

                    if let Err(error) = persist_rollback_point(
                        rollback_store,
                        journal_store,
                        log_write,
                        checkpoint_port,
                        base_dir,
                        execution_cwd.unwrap_or(base_dir),
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
                Err(error) => {
                    // dispatch_completion_panel persisted supporting records but
                    // did not write aggregate or transition on failure.
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

        // ── Panel dispatch: FinalReview ───────────────────────────────────────
        if stage_id == StageId::FinalReview {
            let panel_result = dispatch_final_review_panel(
                agent_service,
                artifact_write,
                log_write,
                run_snapshot_write,
                journal_store,
                base_dir,
                &project_root,
                &backend_working_dir,
                project_id,
                run_id,
                seq,
                snapshot,
                &cursor,
                semantics.planning_stage,
                effective_config,
                prompt_reference,
                cancellation_token.clone(),
            )
            .await;

            match panel_result {
                Ok(FinalReviewPanelOutcome::Complete(completed_cursor, commit_data)) => {
                    cursor = completed_cursor;

                    if let Err(error) = persist_final_review_aggregate_records(
                        artifact_write,
                        base_dir,
                        project_id,
                        &cursor,
                        stage_id,
                        &commit_data,
                    ) {
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

                    *seq += 1;
                    let stage_completed = journal::stage_completed_event(
                        *seq,
                        Utc::now(),
                        run_id,
                        stage_id,
                        cursor.cycle,
                        cursor.attempt,
                        &commit_data.payload_id,
                        &commit_data.artifact_id,
                    );
                    let stage_completed_line = journal::serialize_event(&stage_completed)?;
                    if let Err(error) =
                        journal_store.append_event(base_dir, project_id, &stage_completed_line)
                    {
                        *seq -= 1;
                        cleanup_final_review_aggregate_records(
                            artifact_write,
                            base_dir,
                            project_id,
                            &commit_data,
                        );
                        return fail_run_result(
                            &AppError::StageCommitFailed {
                                stage_id,
                                details: format!(
                                    "journal append failed during final-review aggregate commit: {error}"
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
                            log_write,
                            checkpoint_port,
                            base_dir,
                            execution_cwd.unwrap_or(base_dir),
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
                    snapshot.active_run = Some(carry_forward_active_run(
                        snapshot,
                        run_id,
                        cursor.clone(),
                        project_prompt_hash(&project_root, prompt_reference)?,
                        None,
                    )?);
                    snapshot.status_summary = format!(
                        "running: completed {}, next {}",
                        stage_id.display_name(),
                        next_stage.display_name()
                    );
                    if let Err(error) =
                        run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)
                    {
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
                    continue;
                }
                Ok(FinalReviewPanelOutcome::Restart(next_cursor, commit_data)) => {
                    let max_rounds = std::env::var("RALPH_BURNING_TEST_MAX_COMPLETION_ROUNDS")
                        .ok()
                        .and_then(|value| value.parse::<u32>().ok())
                        .unwrap_or(DEFAULT_MAX_COMPLETION_ROUNDS);
                    if next_cursor.completion_round > max_rounds {
                        return fail_run_result(
                            &AppError::StageCommitFailed {
                                stage_id,
                                details: format!("max completion rounds ({}) exceeded", max_rounds),
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

                    let planning_index = stage_index_for(stage_plan, semantics.planning_stage)?;
                    let from_round = cursor.completion_round;
                    let to_round = next_cursor.completion_round;

                    if let Err(error) = persist_final_review_aggregate_records(
                        artifact_write,
                        base_dir,
                        project_id,
                        &cursor,
                        stage_id,
                        &commit_data,
                    ) {
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

                    let mut written_ids: Vec<String> = Vec::new();
                    for amendment in &commit_data.accepted_amendments {
                        if let Err(error) =
                            amendment_queue_port.write_amendment(base_dir, project_id, amendment)
                        {
                            for written_id in &written_ids {
                                let _ = amendment_queue_port
                                    .remove_amendment(base_dir, project_id, written_id);
                            }
                            cleanup_final_review_aggregate_records(
                                artifact_write,
                                base_dir,
                                project_id,
                                &commit_data,
                            );
                            return fail_run_result(
                                &AppError::AmendmentQueueError {
                                    details: format!(
                                        "failed to persist final-review amendment '{}': {}",
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

                    let mut last_journaled_amendment_index = None;
                    for (index, amendment) in commit_data.accepted_amendments.iter().enumerate() {
                        *seq += 1;
                        let amendment_event = journal::amendment_queued_event(
                            *seq,
                            Utc::now(),
                            run_id,
                            &amendment.amendment_id,
                            amendment.source_stage,
                            &amendment.body,
                            amendment.source.as_str(),
                            &amendment.dedup_key,
                        );
                        let event_line = journal::serialize_event(&amendment_event)?;
                        if let Err(error) =
                            journal_store.append_event(base_dir, project_id, &event_line)
                        {
                            *seq -= 1;
                            let cleanup_errors: Vec<String> = commit_data.accepted_amendments
                                [index..]
                                .iter()
                                .filter_map(|pending| {
                                    amendment_queue_port
                                        .remove_amendment(
                                            base_dir,
                                            project_id,
                                            &pending.amendment_id,
                                        )
                                        .err()
                                        .map(|cleanup_error| {
                                            format!("{}: {}", pending.amendment_id, cleanup_error)
                                        })
                                })
                                .collect();
                            snapshot.completion_rounds = snapshot.completion_rounds.max(to_round);
                            if let Some(last_index) = last_journaled_amendment_index {
                                snapshot.amendment_queue.pending.extend(
                                    commit_data.accepted_amendments[..=last_index]
                                        .iter()
                                        .cloned(),
                                );
                            } else {
                                snapshot
                                    .amendment_queue
                                    .pending
                                    .extend(commit_data.accepted_amendments.iter().cloned());
                            }
                            let details = if cleanup_errors.is_empty() {
                                format!(
                                    "failed to persist final-review amendment_queued event: {}",
                                    error
                                )
                            } else {
                                format!(
                                    "failed to persist final-review amendment_queued event: {}; cleanup failed for {}",
                                    error,
                                    cleanup_errors.join(", ")
                                )
                            };
                            return fail_run_result(
                                &AppError::AmendmentQueueError { details },
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
                        last_journaled_amendment_index = Some(index);
                    }

                    snapshot.last_stage_resolution_snapshot = snapshot
                        .active_run
                        .as_ref()
                        .and_then(|active_run| active_run.stage_resolution_snapshot.clone());
                    snapshot
                        .amendment_queue
                        .pending
                        .extend(commit_data.accepted_amendments.iter().cloned());
                    snapshot.completion_rounds =
                        snapshot.completion_rounds.max(next_cursor.completion_round);
                    snapshot.status = RunStatus::Running;
                    let current = current_active_run(snapshot)?;
                    let prompt_hash = project_prompt_hash(&project_root, prompt_reference)?;
                    snapshot.active_run = Some(advance_completion_round_active_run(
                        snapshot,
                        run_id,
                        next_cursor.clone(),
                        prompt_hash,
                        current.final_review_restart_count.saturating_add(1),
                        None,
                    )?);
                    snapshot.status_summary = format!(
                        "running: final review restart round {} -> {}",
                        to_round,
                        next_cursor.stage.display_name()
                    );
                    if let Err(error) =
                        run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)
                    {
                        return fail_run_result(
                            &AppError::StageCommitFailed {
                                stage_id,
                                details: format!(
                                    "failed to persist final-review restart cursor: {}",
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

                    *seq += 1;
                    let round_event = journal::completion_round_advanced_event(
                        *seq,
                        Utc::now(),
                        run_id,
                        stage_id,
                        from_round,
                        to_round,
                        snapshot.amendment_queue.pending.len() as u32,
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
                                    "failed to persist final-review completion_round_advanced event: {}",
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
                        log_write,
                        checkpoint_port,
                        base_dir,
                        execution_cwd.unwrap_or(base_dir),
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
                Err(error) => {
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

        // ── Local validation dispatch for docs/CI stages ────────────────────

        if stage_id == StageId::DocsValidation || stage_id == StageId::CiValidation {
            // Emit stage_entered event for local validation.
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
            if let Err(error) =
                journal_store.append_event(base_dir, project_id, &stage_entered_line)
            {
                *seq -= 1;
                return fail_run_result(
                    &AppError::StageCommitFailed {
                        stage_id,
                        details: format!(
                            "failed to persist stage_entered event for local validation {}: {}",
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
            let _ = log_write.append_runtime_log(
                base_dir,
                project_id,
                &RuntimeLogEntry {
                    timestamp: Utc::now(),
                    level: LogLevel::Info,
                    source: "engine".to_owned(),
                    message: format!(
                        "stage_entered (local validation): {} cycle={} attempt={}",
                        stage_id.as_str(),
                        cursor.cycle,
                        cursor.attempt
                    ),
                },
            );

            let commands = match stage_id {
                StageId::DocsValidation => {
                    effective_config.validation_policy().docs_commands.clone()
                }
                StageId::CiValidation => effective_config.validation_policy().ci_commands.clone(),
                _ => vec![],
            };

            let (validation_payload, group_result) =
                validation::run_local_validation(stage_id, &commands, &project_root).await;

            // Persist local validation evidence as supporting records.
            let record_base = history_record_base_id(
                run_id,
                stage_id,
                &cursor,
                snapshot.rollback_point_meta.rollback_count,
            );
            if let Err(error) = validation::persist_local_validation_evidence(
                artifact_write,
                base_dir,
                project_id,
                stage_id,
                &cursor,
                &group_result,
                &record_base,
            ) {
                return fail_run_result(
                    &AppError::StageCommitFailed {
                        stage_id,
                        details: format!(
                            "failed to persist local validation evidence for {}: {}",
                            stage_id.as_str(),
                            error,
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

            let bundle = contracts::contract_for_stage(stage_id)
                .evaluate_permissive(&serde_json::to_value(&validation_payload)?)
                .map_err(|contract_error| AppError::InvocationFailed {
                    backend: "local".to_owned(),
                    contract_id: stage_id.to_string(),
                    failure_class: contract_error.failure_class(),
                    details: contract_error.to_string(),
                })?;

            let stage_producer = RecordProducer::LocalValidation {
                command: group_result.group_name.clone(),
            };

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
                &cursor,
                &bundle,
                stage_producer,
                origin,
            )
            .await?;

            // Handle validation outcome (remediation or advance).
            if let Some(outcome) = validation_outcome(&bundle.payload) {
                match outcome {
                    ReviewOutcome::Approved => {}
                    ReviewOutcome::RequestChanges
                        if semantics.remediation_trigger_stages.contains(&stage_id) =>
                    {
                        let current = current_active_run(snapshot)?;
                        let next_iteration = current.qa_iterations_current_cycle.saturating_add(1);
                        let max_iterations = effective_config.run_policy().max_qa_iterations;
                        if next_iteration > max_iterations {
                            return fail_run_result(
                                &AppError::StageCommitFailed {
                                    stage_id,
                                    details: format!(
                                        "qa iteration cap exceeded: {} > {}",
                                        next_iteration, max_iterations
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

                        let next_cycle =
                            cursor
                                .cycle
                                .checked_add(1)
                                .ok_or(AppError::StageCursorOverflow {
                                    field: "cycle",
                                    value: cursor.cycle,
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

                        let next_stage_index =
                            stage_index_for(stage_plan, semantics.execution_stage)?;
                        let next_cursor = cursor.advance_cycle(semantics.execution_stage)?;
                        record_cycle_advance(
                            snapshot,
                            next_cursor.cycle,
                            semantics.execution_stage,
                        );
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

                        let final_review_restart_count = {
                            let current = current_active_run(snapshot)?;
                            current.final_review_restart_count
                        };
                        snapshot.status = RunStatus::Running;
                        snapshot.active_run = Some(reset_cycle_active_run(
                            snapshot,
                            run_id,
                            next_cursor.clone(),
                            project_prompt_hash(&project_root, prompt_reference)?,
                            0,
                            0,
                            final_review_restart_count,
                            None,
                        )?);
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
                            log_write,
                            checkpoint_port,
                            base_dir,
                            execution_cwd.unwrap_or(base_dir),
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
                    ReviewOutcome::ConditionallyApproved
                    | ReviewOutcome::RequestChanges
                    | ReviewOutcome::Rejected => {
                        let failure = AppError::InvocationFailed {
                            backend: "local".to_owned(),
                            contract_id: stage_id.to_string(),
                            failure_class: FailureClass::QaReviewOutcomeFailure,
                            details: format!("non-passing local validation outcome: {}", outcome),
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

            // Advance to next stage.
            if let Err(error) = persist_rollback_point(
                rollback_store,
                journal_store,
                log_write,
                checkpoint_port,
                base_dir,
                execution_cwd.unwrap_or(base_dir),
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
                return Ok(RunOutcome::Completed);
            }
            let next_stage = stage_plan[stage_index + 1].stage_id;
            cursor = cursor.advance_stage(next_stage);
            let current_prompt_hash = project_prompt_hash(&project_root, prompt_reference)?;
            snapshot.status = RunStatus::Running;
            snapshot.active_run = Some(carry_forward_active_run(
                snapshot,
                run_id,
                cursor.clone(),
                current_prompt_hash,
                None,
            )?);
            snapshot.status_summary = format!(
                "running: completed {}, next {}",
                stage_id.display_name(),
                next_stage.display_name()
            );
            if let Err(error) =
                run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)
            {
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
            continue;
        }

        // ── Standard flow: inject local validation evidence into review context ──

        if stage_id == StageId::Review || stage_id == StageId::Qa {
            let standard_commands = effective_config
                .validation_policy()
                .standard_commands
                .clone();
            if let Some(group_result) =
                validation::run_standard_validation_evidence(&standard_commands, &project_root)
                    .await
            {
                let record_base = history_record_base_id(
                    run_id,
                    stage_id,
                    &cursor,
                    snapshot.rollback_point_meta.rollback_count,
                );
                if let Err(error) = validation::persist_local_validation_evidence(
                    artifact_write,
                    base_dir,
                    project_id,
                    stage_id,
                    &cursor,
                    &group_result,
                    &record_base,
                ) {
                    return fail_run_result(
                        &AppError::StageCommitFailed {
                            stage_id,
                            details: format!(
                                "failed to persist local validation evidence for {}: {}",
                                stage_id.as_str(),
                                error,
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
                // Merge local validation context into the execution context.
                let local_ctx = validation::build_local_validation_context(&group_result);
                execution_context = Some(match execution_context.take() {
                    Some(mut existing) => {
                        if let Some(obj) = existing.as_object_mut() {
                            if let Some(local_obj) = local_ctx.as_object() {
                                for (k, v) in local_obj {
                                    obj.insert(k.clone(), v.clone());
                                }
                            }
                        }
                        existing
                    }
                    None => local_ctx,
                });
            }
        }

        // ── Generic single-agent stage dispatch ───────────────────────────────

        // Inject pending amendments into planning invocation context.
        let planning_amendments: Option<Vec<QueuedAmendment>> =
            if stage_id == semantics.planning_stage && !snapshot.amendment_queue.pending.is_empty()
            {
                Some(snapshot.amendment_queue.pending.clone())
            } else {
                None
            };

        let (completed_cursor, bundle, stage_producer) = execute_stage_with_retry(
            agent_service,
            run_snapshot_write,
            journal_store,
            artifact_store,
            log_write,
            base_dir,
            execution_cwd,
            project_id,
            run_id,
            seq,
            snapshot,
            stage_entry,
            &cursor,
            retry_policy,
            cancellation_token.clone(),
            origin,
            execution_context.as_ref().filter(|_| {
                stage_id == semantics.execution_stage
                    || stage_id == StageId::Review
                    || stage_id == StageId::Qa
            }),
            planning_amendments
                .as_deref()
                .filter(|_| stage_id == semantics.planning_stage),
            &project_root,
            prompt_reference,
            effective_config,
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
            stage_producer,
            origin,
        )
        .await?;

        cursor = completed_cursor;

        if stage_id == semantics.execution_stage {
            execution_context = None;
        }

        // Prompt review pause check is no longer needed for generic path since
        // PromptReview is now dispatched through the panel path above.
        // The generic path only handles non-panel stages.

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
                log_write,
                checkpoint_port,
                base_dir,
                execution_cwd.unwrap_or(base_dir),
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
                ReviewOutcome::Approved => {
                    // ── Pre-commit gating after review approval ──────────
                    if stage_id == StageId::Review
                        && !validation::pre_commit_checks_disabled(effective_config)
                    {
                        let pre_commit_result =
                            validation::run_pre_commit(&project_root, effective_config).await;

                        // Only persist evidence and check results when commands actually ran.
                        if !pre_commit_result.commands.is_empty() {
                            let record_base = history_record_base_id(
                                run_id,
                                stage_id,
                                &cursor,
                                snapshot.rollback_point_meta.rollback_count,
                            );
                            // Persist pre-commit evidence regardless of pass/fail outcome.
                            // Failure invariant: if persistence fails, the run must not
                            // advance past the pre-transition stage boundary.
                            if let Err(error) = validation::persist_pre_commit_evidence(
                                artifact_write,
                                log_write,
                                base_dir,
                                project_id,
                                stage_id,
                                &cursor,
                                &pre_commit_result,
                                &record_base,
                            ) {
                                return fail_run_result(
                                    &AppError::StageCommitFailed {
                                        stage_id,
                                        details: format!(
                                            "failed to persist pre-commit evidence for {}: {}",
                                            stage_id.as_str(),
                                            error,
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

                            if !pre_commit_result.passed {
                                // Pre-commit failure: invalidate reviewer approval,
                                // return to implementation remediation.
                                // Failure invariant: only reviewer approval is cleared;
                                // all other durable history remains unchanged.

                                let current = current_active_run(snapshot)?;
                                let next_iteration =
                                    current.review_iterations_current_cycle.saturating_add(1);
                                let max_iterations =
                                    effective_config.run_policy().max_review_iterations;
                                if next_iteration > max_iterations {
                                    return fail_run_result(
                                    &AppError::StageCommitFailed {
                                        stage_id,
                                        details: format!(
                                            "review iteration cap exceeded after pre-commit failure: {} > {}",
                                            next_iteration, max_iterations
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

                                let next_cycle = cursor.cycle.checked_add(1).ok_or(
                                    AppError::StageCursorOverflow {
                                        field: "cycle",
                                        value: cursor.cycle,
                                    },
                                )?;
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

                                let next_stage_index =
                                    stage_index_for(stage_plan, semantics.execution_stage)?;
                                let next_cursor =
                                    cursor.advance_cycle(semantics.execution_stage)?;
                                record_cycle_advance(
                                    snapshot,
                                    next_cursor.cycle,
                                    semantics.execution_stage,
                                );
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
                                let cycle_advanced_line =
                                    journal::serialize_event(&cycle_advanced)?;
                                if let Err(error) = journal_store.append_event(
                                    base_dir,
                                    project_id,
                                    &cycle_advanced_line,
                                ) {
                                    *seq -= 1;
                                    return fail_run_result(
                                    &AppError::StageCommitFailed {
                                        stage_id,
                                        details: format!(
                                            "failed to persist cycle_advanced event after pre-commit failure for {}: {}",
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

                                let final_review_restart_count = {
                                    let current = current_active_run(snapshot)?;
                                    current.final_review_restart_count
                                };
                                snapshot.status = RunStatus::Running;
                                snapshot.active_run = Some(reset_cycle_active_run(
                                    snapshot,
                                    run_id,
                                    next_cursor.clone(),
                                    project_prompt_hash(&project_root, prompt_reference)?,
                                    0,
                                    0,
                                    final_review_restart_count,
                                    None,
                                )?);
                                snapshot.status_summary = format!(
                                    "running: pre-commit remediation cycle {} -> {}",
                                    next_cursor.cycle,
                                    next_cursor.stage.display_name()
                                );
                                if let Err(error) = run_snapshot_write
                                    .write_run_snapshot(base_dir, project_id, snapshot)
                                {
                                    return fail_run_result(
                                        &AppError::StageCommitFailed {
                                            stage_id,
                                            details: format!(
                                            "failed to persist pre-commit remediation cursor: {}",
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
                                    log_write,
                                    checkpoint_port,
                                    base_dir,
                                    execution_cwd.unwrap_or(base_dir),
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

                                execution_context = Some(
                                    validation::pre_commit_remediation_context(&pre_commit_result),
                                );
                                stage_index = next_stage_index;
                                cursor = next_cursor;
                                continue;
                            }
                        } // end: if !pre_commit_result.commands.is_empty()
                    }
                }
                ReviewOutcome::ConditionallyApproved | ReviewOutcome::RequestChanges
                    if semantics.late_stages.contains(&stage_id) =>
                {
                    // Late-stage conditional approval or request changes:
                    // Queue durable amendments, advance completion round, restart from planning.
                    let next_cursor = cursor.advance_completion_round(semantics.planning_stage)?;
                    let from_round = cursor.completion_round;
                    let to_round = next_cursor.completion_round;
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
                    let mut last_journaled_amendment_index = None;
                    for (index, amendment) in amendments.iter().enumerate() {
                        *seq += 1;
                        let amendment_event = journal::amendment_queued_event(
                            *seq,
                            Utc::now(),
                            run_id,
                            &amendment.amendment_id,
                            amendment.source_stage,
                            &amendment.body,
                            amendment.source.as_str(),
                            &amendment.dedup_key,
                        );
                        let event_line = journal::serialize_event(&amendment_event)?;
                        if let Err(error) =
                            journal_store.append_event(base_dir, project_id, &event_line)
                        {
                            *seq -= 1;
                            let cleanup_errors: Vec<String> = amendments[index..]
                                .iter()
                                .filter_map(|pending| {
                                    amendment_queue_port
                                        .remove_amendment(
                                            base_dir,
                                            project_id,
                                            &pending.amendment_id,
                                        )
                                        .err()
                                        .map(|cleanup_error| {
                                            format!("{}: {}", pending.amendment_id, cleanup_error)
                                        })
                                })
                                .collect();

                            snapshot.completion_rounds = snapshot.completion_rounds.max(to_round);
                            if let Some(last_index) = last_journaled_amendment_index {
                                snapshot
                                    .amendment_queue
                                    .pending
                                    .extend(amendments[..=last_index].iter().cloned());
                            } else {
                                // Preserve the full requested batch when the first journal append
                                // fails so resume can restart the new completion round instead of
                                // skipping a conditional approval outcome with an empty journal
                                // prefix.
                                snapshot
                                    .amendment_queue
                                    .pending
                                    .extend(amendments.iter().cloned());
                            }

                            let details = if cleanup_errors.is_empty() {
                                format!("failed to persist amendment_queued event: {}", error)
                            } else {
                                format!(
                                    "failed to persist amendment_queued event: {}; cleanup failed for {}",
                                    error,
                                    cleanup_errors.join(", ")
                                )
                            };
                            return fail_run_result(
                                &AppError::AmendmentQueueError { details },
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
                        last_journaled_amendment_index = Some(index);
                    }

                    // Add amendments to snapshot queue, deduplicating by
                    // amendment_id so retried late-stage approvals don't
                    // append duplicate entries from a prior failed attempt.
                    for amendment in amendments {
                        if !snapshot
                            .amendment_queue
                            .pending
                            .iter()
                            .any(|existing| existing.amendment_id == amendment.amendment_id)
                        {
                            snapshot.amendment_queue.pending.push(amendment);
                        }
                    }

                    // Advance the snapshot before the journal append so fail_run()
                    // persists the new round if the append itself fails.
                    snapshot.completion_rounds = snapshot.completion_rounds.max(to_round);

                    // Emit completion_round_advanced event.
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
                    snapshot.status = RunStatus::Running;
                    snapshot.active_run = Some(carry_forward_active_run(
                        snapshot,
                        run_id,
                        next_cursor.clone(),
                        project_prompt_hash(&project_root, prompt_reference)?,
                        None,
                    )?);
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
                        log_write,
                        checkpoint_port,
                        base_dir,
                        execution_cwd.unwrap_or(base_dir),
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
                    let current = current_active_run(snapshot)?;
                    let (next_iteration, max_iterations, counter_label) =
                        if stage_id == StageId::Review {
                            (
                                current.review_iterations_current_cycle.saturating_add(1),
                                effective_config.run_policy().max_review_iterations,
                                "review",
                            )
                        } else {
                            (
                                current.qa_iterations_current_cycle.saturating_add(1),
                                effective_config.run_policy().max_qa_iterations,
                                "qa",
                            )
                        };
                    if next_iteration > max_iterations {
                        return fail_run_result(
                            &AppError::StageCommitFailed {
                                stage_id,
                                details: format!(
                                    "{counter_label} iteration cap exceeded: {} > {}",
                                    next_iteration, max_iterations
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

                    let next_cycle =
                        cursor
                            .cycle
                            .checked_add(1)
                            .ok_or(AppError::StageCursorOverflow {
                                field: "cycle",
                                value: cursor.cycle,
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

                    let final_review_restart_count = {
                        let current = current_active_run(snapshot)?;
                        current.final_review_restart_count
                    };
                    snapshot.status = RunStatus::Running;
                    snapshot.active_run = Some(reset_cycle_active_run(
                        snapshot,
                        run_id,
                        next_cursor.clone(),
                        project_prompt_hash(&project_root, prompt_reference)?,
                        0,
                        0,
                        final_review_restart_count,
                        None,
                    )?);
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
                        log_write,
                        checkpoint_port,
                        base_dir,
                        execution_cwd.unwrap_or(base_dir),
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
                log_write,
                checkpoint_port,
                base_dir,
                execution_cwd.unwrap_or(base_dir),
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
        snapshot.active_run = Some(carry_forward_active_run(
            snapshot,
            run_id,
            cursor.clone(),
            project_prompt_hash(&project_root, prompt_reference)?,
            None,
        )?);
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
            log_write,
            checkpoint_port,
            base_dir,
            execution_cwd.unwrap_or(base_dir),
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
    artifact_store: &dyn ArtifactStorePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    execution_cwd: Option<&Path>,
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
    prompt_reference: &str,
    effective_config: &EffectiveConfig,
) -> AppResult<(StageCursor, ValidatedBundle, RecordProducer)>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let stage_id = stage_entry.stage_id;
    let mut cursor = starting_cursor.clone();
    let policy = BackendPolicyService::new(effective_config);

    loop {
        let resolved_target = match policy.resolve_stage_target(stage_id, cursor.cycle) {
            Ok(target) => target,
            Err(error) => {
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
                .await
            }
        };
        let timeout = policy.timeout_for_role(
            resolved_target.backend.family,
            policy.policy_role_for_stage(stage_id),
        );

        if cancellation_token.is_cancelled() {
            return fail_run_result(
                &AppError::InvocationCancelled {
                    backend: resolved_target.backend.family.to_string(),
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

        // Resolve and render the template BEFORE any durable state writes.
        // If the selected override is malformed, we must fail without
        // appending journal entries or updating snapshots (Slice 7 failure
        // invariant).
        let prompt = match build_stage_prompt(
            artifact_store,
            base_dir,
            project_id,
            project_root,
            prompt_reference,
            stage_entry.role,
            &stage_entry.contract,
            run_id,
            &cursor,
            execution_context,
            pending_amendments,
        ) {
            Ok(prompt) => prompt,
            Err(error) => {
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
        };

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

        // Persist stage resolution snapshot before any agent invocation.
        let resolution = build_single_target_snapshot(stage_id, &resolved_target);
        snapshot.status = RunStatus::Running;
        snapshot.active_run = Some(carry_forward_active_run(
            snapshot,
            run_id,
            cursor.clone(),
            project_prompt_hash(project_root, prompt_reference)?,
            Some(resolution),
        )?);
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

        let result = invoke_stage_on_backend(
            agent_service,
            base_dir,
            execution_cwd,
            project_root,
            run_id,
            stage_entry,
            &cursor,
            prompt,
            execution_context,
            pending_amendments,
            cancellation_token.clone(),
            resolved_target.clone(),
            timeout,
        )
        .await;

        match result {
            Ok((bundle, producer)) => return Ok((cursor.clone(), bundle, producer)),
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
async fn invoke_stage_on_backend<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    base_dir: &Path,
    execution_cwd: Option<&Path>,
    project_root: &Path,
    run_id: &RunId,
    stage_entry: &StagePlan,
    cursor: &StageCursor,
    prompt: String,
    execution_context: Option<&Value>,
    pending_amendments: Option<&[QueuedAmendment]>,
    cancellation_token: CancellationToken,
    resolved_target: ResolvedBackendTarget,
    timeout: Duration,
) -> AppResult<(ValidatedBundle, RecordProducer)>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let request = InvocationRequest {
        invocation_id: history_record_base_id(run_id, stage_entry.stage_id, cursor, 0),
        project_root: project_root.to_path_buf(),
        working_dir: execution_cwd.unwrap_or(base_dir).to_path_buf(),
        contract: InvocationContract::Stage(stage_entry.contract),
        role: stage_entry.role,
        resolved_target: resolved_target.clone(),
        payload: InvocationPayload {
            prompt,
            context: invocation_context(cursor, execution_context, pending_amendments),
        },
        timeout,
        cancellation_token,
        session_policy: SessionPolicy::ReuseIfAllowed,
        prior_session: None,
        attempt_number: cursor.attempt,
    };

    agent_service.invoke(request).await.and_then(|envelope| {
        let producer = agent_record_producer(&envelope.metadata);
        stage_entry
            .contract
            .evaluate_permissive(&envelope.parsed_payload)
            .map(|bundle| (bundle, producer))
            .map_err(|contract_error| AppError::InvocationFailed {
                backend: resolved_target.backend.family.to_string(),
                contract_id: stage_entry.stage_id.to_string(),
                failure_class: contract_error.failure_class(),
                details: contract_error.to_string(),
            })
    })
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
    producer: RecordProducer,
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
        record_kind: RecordKind::StagePrimary,
        producer: Some(producer.clone()),
        completion_round: cursor.completion_round,
    };
    let artifact_record = ArtifactRecord {
        artifact_id: artifact_id.clone(),
        payload_id: payload_id.clone(),
        stage_id,
        created_at: stage_now,
        content: bundle.artifact.clone(),
        record_kind: RecordKind::StagePrimary,
        producer: Some(producer),
        completion_round: cursor.completion_round,
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

fn checkpoint_completion_round(snapshot: &RunSnapshot) -> u32 {
    snapshot
        .active_run
        .as_ref()
        .map(|active_run| active_run.stage_cursor.completion_round)
        .or_else(|| {
            snapshot
                .interrupted_run
                .as_ref()
                .map(|active_run| active_run.stage_cursor.completion_round)
        })
        .unwrap_or_else(|| snapshot.completion_rounds.max(1))
}

#[allow(clippy::too_many_arguments)]
fn persist_rollback_point(
    rollback_store: &dyn RollbackPointStorePort,
    journal_store: &dyn JournalStorePort,
    log_write: &dyn RuntimeLogWritePort,
    checkpoint_port: &dyn VcsCheckpointPort,
    base_dir: &Path,
    checkpoint_root: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &RunSnapshot,
    stage_id: StageId,
    cycle: u32,
) -> AppResult<()> {
    let created_at = Utc::now();
    let completion_round = checkpoint_completion_round(snapshot);
    let git_sha = match checkpoint_port.create_checkpoint(
        checkpoint_root,
        project_id,
        run_id,
        stage_id,
        cycle,
        completion_round,
    ) {
        Ok(sha) => Some(sha),
        Err(error) => {
            let _ = log_write.append_runtime_log(
                base_dir,
                project_id,
                &RuntimeLogEntry {
                    timestamp: created_at,
                    level: LogLevel::Warn,
                    source: "engine".to_owned(),
                    message: format!(
                        "checkpoint creation failed: stage={} cycle={} round={} error={}",
                        stage_id.as_str(),
                        cycle,
                        completion_round,
                        error
                    ),
                },
            );
            None
        }
    };
    let rollback_point = RollbackPoint {
        rollback_id: Uuid::new_v4().to_string(),
        created_at,
        stage_id,
        cycle,
        git_sha,
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

#[allow(clippy::too_many_arguments)]
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
            preserve_interrupted_run(snapshot);
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
    snapshot.interrupted_run = None;
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
    // Preserve the stage resolution snapshot for resume drift detection.
    snapshot.last_stage_resolution_snapshot = snapshot
        .active_run
        .as_ref()
        .and_then(|ar| ar.stage_resolution_snapshot.clone());
    preserve_interrupted_run(snapshot);
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

    // Preserve the stage resolution snapshot for resume drift detection.
    // Only overwrite if the active run carries an explicit snapshot;
    // otherwise retain any snapshot previously copied forward (e.g. by
    // completion panel commit paths that clear active_run's snapshot
    // before the journal commit point).
    if let Some(resolution) = snapshot
        .active_run
        .as_ref()
        .and_then(|ar| ar.stage_resolution_snapshot.clone())
    {
        snapshot.last_stage_resolution_snapshot = Some(resolution);
    }
    preserve_interrupted_run(snapshot);
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
        .map(|(idx, body)| {
            let source = crate::contexts::project_run_record::model::AmendmentSource::WorkflowStage;
            let dedup_key = QueuedAmendment::compute_dedup_key(&source, body);
            QueuedAmendment {
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
                source,
                dedup_key,
            }
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
        .map(|(idx, body)| {
            let source = crate::contexts::project_run_record::model::AmendmentSource::WorkflowStage;
            let dedup_key = QueuedAmendment::compute_dedup_key(&source, body);
            QueuedAmendment {
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
                source,
                dedup_key,
            }
        })
        .collect()
}

/// Completion guard: blocks run_completed when pending amendments remain.
pub(crate) fn completion_guard(
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
    journal_events: &[JournalEvent],
    amendment_queue_port: &dyn AmendmentQueuePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<()> {
    let disk_amendments = amendment_queue_port.list_pending_amendments(base_dir, project_id)?;
    if disk_amendments.is_empty() {
        return Ok(());
    }

    let journaled_ids: std::collections::HashSet<String> = journal_events
        .iter()
        .filter(|event| {
            event.event_type
                == crate::contexts::project_run_record::model::JournalEventType::AmendmentQueued
        })
        .map(|event| detail_string(event, "amendment_id").map(str::to_owned))
        .collect::<AppResult<_>>()?;

    // Merge disk amendments into snapshot, avoiding duplicates by ID and
    // skipping entries already durably represented in the journal.
    let mut existing_ids: std::collections::HashSet<String> = snapshot
        .amendment_queue
        .pending
        .iter()
        .map(|a| a.amendment_id.clone())
        .collect();

    for amendment in disk_amendments {
        if journaled_ids.contains(&amendment.amendment_id) {
            continue;
        }
        if existing_ids.insert(amendment.amendment_id.clone()) {
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
        // Local validation evidence (from standard_commands for Review/Qa stages)
        // is surfaced at top level; remediation context is nested under "remediation".
        if execution_context.get("local_validation").is_some() {
            if let Some(obj) = execution_context.as_object() {
                for (k, v) in obj {
                    context[k] = v.clone();
                }
            }
        } else {
            context["remediation"] = execution_context.clone();
        }
    }

    if let Some(amendments) = pending_amendments {
        if !amendments.is_empty() {
            let amendment_bodies: Vec<&str> = amendments.iter().map(|a| a.body.as_str()).collect();
            context["pending_amendments"] = json!(amendment_bodies);
        }
    }

    context
}

fn load_prior_stage_outputs_this_cycle(
    artifact_store: &dyn ArtifactStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    project_root: &Path,
    run_id: &RunId,
    cursor: &StageCursor,
) -> AppResult<Vec<PayloadRecord>> {
    let journal_path = project_root.join("journal.ndjson");
    let journal_contents =
        fs::read_to_string(&journal_path).map_err(|error| AppError::CorruptRecord {
            file: journal_path.display().to_string(),
            details: format!(
                "failed to read journal.ndjson while building stage prompt: {}",
                error
            ),
        })?;
    let events = queries::visible_journal_events(&journal::parse_journal(&journal_contents)?)?;

    let payloads = artifact_store.list_payloads(base_dir, project_id)?;
    let payloads_by_id: HashMap<String, PayloadRecord> = payloads
        .into_iter()
        .map(|record| (record.payload_id.clone(), record))
        .collect();

    let mut prior_outputs = Vec::new();
    for event in events {
        if event.event_type != JournalEventType::StageCompleted {
            continue;
        }
        if detail_string(&event, "run_id")? != run_id.as_str() {
            continue;
        }
        if detail_u32(&event, "cycle") != Some(cursor.cycle) {
            continue;
        }

        let payload_id = detail_string(&event, "payload_id")?;
        let payload =
            payloads_by_id
                .get(payload_id)
                .cloned()
                .ok_or_else(|| AppError::CorruptRecord {
                    file: "journal.ndjson".to_owned(),
                    details: format!(
                        "journal references missing payload '{}' while building stage prompt",
                        payload_id
                    ),
                })?;
        prior_outputs.push(payload);
    }

    Ok(prior_outputs)
}

fn stage_role_instruction(role: BackendRole, stage_id: StageId) -> String {
    format!(
        "You are the {}. Your objective for this {} stage is to {}.",
        role.display_name(),
        stage_id.display_name(),
        stage_objective(stage_id)
    )
}

fn stage_objective(stage_id: StageId) -> &'static str {
    match stage_id {
        StageId::PromptReview => {
            "assess whether the project prompt is actionable, identify gaps, and decide readiness"
        }
        StageId::Planning => {
            "produce a concrete implementation plan, assumptions, and readiness assessment"
        }
        StageId::Implementation => {
            "deliver the implementation work, describe the changes made, and record verification"
        }
        StageId::Qa => {
            "validate the implementation against requirements, surface gaps, and determine the QA outcome"
        }
        StageId::Review => {
            "review the completed work for correctness, regressions, and completeness"
        }
        StageId::CompletionPanel => {
            "judge the late-stage outputs, consolidate follow-ups, and decide whether the work can advance"
        }
        StageId::AcceptanceQa => {
            "perform acceptance validation against the intended behavior and remaining risks"
        }
        StageId::FinalReview => {
            "issue the final completion decision and capture any remaining amendments"
        }
        StageId::PlanAndImplement => {
            "produce the plan and implementation details together in a single execution response"
        }
        StageId::ApplyFixes => {
            "apply the requested fixes, summarize the edits, and record the validation performed"
        }
        StageId::DocsPlan => {
            "plan the documentation updates required to support the requested change"
        }
        StageId::DocsUpdate => {
            "update the documentation accurately and summarize what changed"
        }
        StageId::DocsValidation => {
            "validate documentation accuracy, clarity, and completeness"
        }
        StageId::CiPlan => {
            "plan the CI workflow updates needed for the requested change"
        }
        StageId::CiUpdate => {
            "update CI configuration or automation and summarize the changes"
        }
        StageId::CiValidation => {
            "validate the CI changes for correctness, safety, and completeness"
        }
    }
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
        .iter()
        .find(|record| record.payload_id == payload_id)
        .ok_or_else(|| AppError::ResumeFailed {
            reason: format!(
                "failed to reconstruct remediation context for {} cycle {}; payload '{}' is missing from durable history",
                execution_label, cursor.cycle, payload_id
            ),
        })?;
    let payload: StagePayload =
        serde_json::from_value(payload_record.payload.clone()).map_err(|error| {
            AppError::ResumeFailed {
                reason: format!(
                    "failed to parse remediation payload '{}' during resume: {}",
                    payload_id, error
                ),
            }
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
        StagePayload::Validation(validation)
            if validation.outcome == ReviewOutcome::Approved =>
        {
            // Review approved but a pre-commit failure triggered remediation.
            // Derive the remediation context from the durable supporting
            // pre-commit evidence record instead of the primary payload.
            derive_remediation_from_pre_commit_evidence(
                &payloads,
                stage_id,
                prior_cycle,
                cursor,
                execution_label,
                &payload_id,
            )
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

/// Derive remediation context from durable supporting pre-commit evidence.
///
/// Called when the primary review payload has `Approved` outcome but a
/// pre-commit failure triggered a cycle advance into remediation. The
/// supporting evidence was persisted by `persist_pre_commit_evidence()` with
/// `RecordKind::StageSupporting` and `RecordProducer::LocalValidation`.
fn derive_remediation_from_pre_commit_evidence(
    payloads: &[PayloadRecord],
    stage_id: StageId,
    prior_cycle: u32,
    cursor: &StageCursor,
    execution_label: &str,
    primary_payload_id: &str,
) -> AppResult<Option<Value>> {
    // Find the supporting pre-commit evidence payload from the same
    // stage and cycle as the approved review.
    let pre_commit_record = payloads.iter().find(|record| {
        record.stage_id == stage_id
            && record.cycle == prior_cycle
            && record.record_kind == RecordKind::StageSupporting
            && matches!(
                &record.producer,
                Some(RecordProducer::LocalValidation { command })
                    if command == "pre_commit"
            )
    });

    let Some(record) = pre_commit_record else {
        return Err(AppError::ResumeFailed {
            reason: format!(
                "failed to reconstruct remediation context for {} cycle {}; primary payload '{}' has Approved outcome but no supporting pre-commit evidence was found for cycle {}",
                execution_label, cursor.cycle, primary_payload_id, prior_cycle
            ),
        });
    };

    let group_result: ValidationGroupResult = serde_json::from_value(record.payload.clone())
        .map_err(|error| AppError::ResumeFailed {
            reason: format!(
                "failed to parse pre-commit evidence payload '{}' during resume: {}",
                record.payload_id, error
            ),
        })?;

    if group_result.passed {
        return Err(AppError::ResumeFailed {
            reason: format!(
                "failed to reconstruct remediation context for {} cycle {}; pre-commit evidence '{}' shows passed, but cycle advanced to remediation",
                execution_label, cursor.cycle, record.payload_id
            ),
        });
    }

    Ok(Some(validation::pre_commit_remediation_context(
        &group_result,
    )))
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
                    None => current_cycle.checked_add(1).ok_or(AppError::StageCursorOverflow {
                        field: "cycle",
                        value: current_cycle,
                    })?,
                };
                next_stage_index = execution_stage_index;
            }
            crate::contexts::project_run_record::model::JournalEventType::CompletionRoundAdvanced => {
                current_completion_round = match detail_u32(event, "to_round") {
                    Some(to_round) => to_round,
                    None => current_completion_round.checked_add(1).ok_or(AppError::StageCursorOverflow {
                        field: "completion_round",
                        value: current_completion_round,
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

// ── Panel Dispatch ────────────────────────────────────────────────────────

/// Data needed to commit the completion panel result after the transition succeeds.
struct CompletionCommitData {
    aggregate_payload: serde_json::Value,
    aggregate_artifact: String,
    payload_id: String,
    artifact_id: String,
    /// The completion_round at which the aggregate was computed. In the
    /// ContinueWork path the cursor passed to `persist_completion_aggregate_records`
    /// already has an advanced round, so we store the original here.
    completion_round: u32,
}

enum CompletionPanelOutcome {
    Complete(StageCursor, CompletionCommitData),
    ContinueWork(StageCursor, CompletionCommitData),
}

struct FinalReviewCommitData {
    aggregate_payload: serde_json::Value,
    aggregate_artifact: String,
    payload_id: String,
    artifact_id: String,
    completion_round: u32,
    accepted_amendments: Vec<QueuedAmendment>,
}

enum FinalReviewPanelOutcome {
    Complete(StageCursor, FinalReviewCommitData),
    Restart(StageCursor, FinalReviewCommitData),
}

/// Dispatch the prompt-review panel stage: resolve panel, persist snapshot,
/// invoke refiner + validators, persist primary record, and emit
/// stage_completed. Returns the cursor on success.
#[allow(clippy::too_many_arguments)]
async fn dispatch_prompt_review_panel<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    project_root: &Path,
    backend_working_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &mut RunSnapshot,
    cursor: &StageCursor,
    effective_config: &EffectiveConfig,
    prompt_reference: &str,
    cancellation_token: CancellationToken,
    _origin: ExecutionOrigin,
) -> AppResult<StageCursor>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let stage_id = StageId::PromptReview;
    let policy = BackendPolicyService::new(effective_config);
    let mut panel = policy.resolve_prompt_review_panel(cursor.cycle)?;
    let min_reviewers = effective_config.prompt_review_policy().min_reviewers;

    // ── Pre-snapshot availability filtering ─────────────────────────────
    // Check runtime availability of the refiner and each validator BEFORE
    // building and persisting the snapshot. The refiner is always required;
    // if it is unavailable, the stage fails before any snapshot or
    // invocation side effects.
    agent_service
        .adapter()
        .check_availability(&panel.refiner)
        .await
        .map_err(|e| AppError::BackendUnavailable {
            backend: panel.refiner.backend.family.to_string(),
            details: format!("required prompt-review refiner unavailable: {e}"),
        })?;

    // Required unavailable validators fail resolution; optional
    // unavailable validators are removed so the snapshot only records
    // members that will actually execute.
    let mut available_validators = Vec::new();
    for member in &panel.validators {
        match agent_service
            .adapter()
            .check_availability(&member.target)
            .await
        {
            Ok(()) => available_validators.push(member.clone()),
            Err(e) => {
                if member.required {
                    return Err(e);
                }
                // Optional validator unavailable — remove before snapshot.
            }
        }
    }
    if available_validators.len() < min_reviewers {
        return Err(AppError::InsufficientPanelMembers {
            panel: "prompt_review".to_owned(),
            resolved: available_validators.len(),
            minimum: min_reviewers,
        });
    }
    panel.validators = available_validators;

    let resolution = build_prompt_review_snapshot(stage_id, &panel);
    // Resolve per-member timeouts using panel-specific roles: PromptReviewer
    // for the refiner and PromptValidator for validators, rather than the
    // generic stage-level Planner role.
    let refiner_timeout = policy.timeout_for_role(
        panel.refiner.backend.family,
        BackendPolicyRole::PromptReviewer,
    );
    let timeout_for_backend = |family: BackendFamily| -> Duration {
        policy.timeout_for_role(family, BackendPolicyRole::PromptValidator)
    };

    // Pre-validate panel template overrides BEFORE any durable state writes.
    // If a panel template override is malformed, we must fail without
    // appending journal entries or updating snapshots (Slice 7 failure invariant).
    template_catalog::resolve("prompt_review_refiner", base_dir, Some(project_id))?;
    template_catalog::resolve("prompt_review_validator", base_dir, Some(project_id))?;

    // Emit stage_entered journal event.
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
        return Err(AppError::StageCommitFailed {
            stage_id,
            details: format!("failed to persist stage_entered for prompt_review: {error}"),
        });
    }

    // Persist stage resolution snapshot before any agent invocation.
    persist_stage_resolution_snapshot(
        snapshot,
        run_snapshot_write,
        base_dir,
        project_id,
        resolution,
    )?;

    let _ = log_write.append_runtime_log(
        base_dir,
        project_id,
        &RuntimeLogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Info,
            source: "engine".to_owned(),
            message: format!(
                "prompt_review panel: refiner + {} validators",
                panel.validators.len()
            ),
        },
    );

    // Execute the prompt-review panel workflow.
    let result = prompt_review::execute_prompt_review(
        agent_service,
        artifact_write,
        log_write,
        base_dir,
        project_root,
        backend_working_dir,
        project_id,
        run_id,
        cursor,
        &panel,
        min_reviewers,
        prompt_reference,
        snapshot.rollback_point_meta.rollback_count,
        refiner_timeout,
        &timeout_for_backend,
        cancellation_token,
    )
    .await?;

    // Persist the canonical primary record.
    let payload_id = format!(
        "{}-{}-primary-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );
    let artifact_id = format!("{payload_id}-artifact");
    let now = Utc::now();

    let payload_record = PayloadRecord {
        payload_id: payload_id.clone(),
        stage_id,
        cycle: cursor.cycle,
        attempt: cursor.attempt,
        created_at: now,
        payload: result.primary_payload,
        record_kind: RecordKind::StagePrimary,
        producer: Some(RecordProducer::System {
            component: "prompt_review".to_owned(),
        }),
        completion_round: cursor.completion_round,
    };
    let artifact_record = ArtifactRecord {
        artifact_id: artifact_id.clone(),
        payload_id: payload_id.clone(),
        stage_id,
        created_at: now,
        content: result.primary_artifact,
        record_kind: RecordKind::StagePrimary,
        producer: Some(RecordProducer::System {
            component: "prompt_review".to_owned(),
        }),
        completion_round: cursor.completion_round,
    };
    artifact_write.write_payload_artifact_pair(
        base_dir,
        project_id,
        &payload_record,
        &artifact_record,
    )?;

    // ── Prompt-review failure invariant ──────────────────────────────────
    // The spec requires that prompt.md, prompt.original.md, project.toml
    // prompt metadata, stage_completed, and the stage cursor remain
    // unchanged if any commit step fails.
    //
    // Ordering:
    // 1. Primary payload/artifact (already written above, reversible)
    // 2. Replace prompt files (reversible via revert_prompt_replacement)
    // 3. stage_completed journal event (commit point — LAST write)
    //
    // If step 3 fails, we roll back steps 1 and 2 so that prompt files
    // and the journal are never in an inconsistent state.

    // Step 2: write prompt.original.md, replace prompt.md, update hash.
    if let Err(error) = crate::adapters::fs::FileSystem::replace_prompt_atomically(
        base_dir,
        project_id,
        &result.original_prompt,
        &result.refined_prompt,
    ) {
        // Prompt replacement failed — clean up primary records.
        let _ = artifact_write.remove_payload_artifact_pair(
            base_dir,
            project_id,
            &payload_id,
            &artifact_id,
        );
        return Err(error);
    }

    // Step 3: stage_completed is the journal commit point (LAST write).
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
        *seq -= 1;
        // Roll back prompt replacement so prompt.md stays at original.
        crate::adapters::fs::FileSystem::revert_prompt_replacement(
            base_dir,
            project_id,
            &result.original_prompt,
        );
        // Clean up primary records.
        let _ = artifact_write.remove_payload_artifact_pair(
            base_dir,
            project_id,
            &payload_id,
            &artifact_id,
        );
        return Err(AppError::StageCommitFailed {
            stage_id,
            details: format!("journal append failed during prompt_review commit: {error}"),
        });
    }

    let _ = log_write.append_runtime_log(
        base_dir,
        project_id,
        &RuntimeLogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Info,
            source: "engine".to_owned(),
            message: format!(
                "stage_completed: {} (prompt_review accepted)",
                stage_id.as_str()
            ),
        },
    );

    Ok(cursor.clone())
}

/// Dispatch the completion panel stage: resolve panel, persist snapshot,
/// invoke completers, compute aggregate, persist records, and determine
/// whether to advance or restart.
#[allow(clippy::too_many_arguments)]
async fn dispatch_completion_panel<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    project_root: &Path,
    backend_working_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &mut RunSnapshot,
    cursor: &StageCursor,
    effective_config: &EffectiveConfig,
    prompt_reference: &str,
    cancellation_token: CancellationToken,
    _origin: ExecutionOrigin,
) -> AppResult<CompletionPanelOutcome>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let stage_id = StageId::CompletionPanel;
    let policy = BackendPolicyService::new(effective_config);
    let mut panel = policy.resolve_completion_panel(cursor.cycle)?;
    let min_completers = effective_config.completion_policy().min_completers;
    let consensus_threshold = effective_config.completion_policy().consensus_threshold;

    // ── Pre-snapshot availability filtering ─────────────────────────────
    // Check runtime availability of each completer BEFORE building and
    // persisting the snapshot. Required unavailable backends fail
    // resolution; optional unavailable backends are removed so the
    // snapshot only records members that will actually execute.
    let mut available_completers = Vec::new();
    for member in &panel.completers {
        match agent_service
            .adapter()
            .check_availability(&member.target)
            .await
        {
            Ok(()) => available_completers.push(member.clone()),
            Err(e) => {
                if member.required {
                    return Err(e);
                }
                // Optional completer unavailable — remove before snapshot.
            }
        }
    }
    if available_completers.len() < min_completers {
        return Err(AppError::InsufficientPanelMembers {
            panel: "completion".to_owned(),
            resolved: available_completers.len(),
            minimum: min_completers,
        });
    }
    panel.completers = available_completers;

    let resolution = build_completion_snapshot(stage_id, &panel.completers);
    // Resolve per-member timeouts via the backend family of each invoked member.
    let policy_role = policy.policy_role_for_stage(stage_id);
    let timeout_for_backend =
        |family: BackendFamily| -> Duration { policy.timeout_for_role(family, policy_role) };

    // Pre-validate panel template overrides BEFORE any durable state writes.
    // If a panel template override is malformed, we must fail without
    // appending journal entries or updating snapshots (Slice 7 failure invariant).
    template_catalog::resolve("completion_panel_completer", base_dir, Some(project_id))?;

    // Emit stage_entered journal event.
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
        return Err(AppError::StageCommitFailed {
            stage_id,
            details: format!("failed to persist stage_entered for completion_panel: {error}"),
        });
    }

    // Persist stage resolution snapshot before any agent invocation.
    persist_stage_resolution_snapshot(
        snapshot,
        run_snapshot_write,
        base_dir,
        project_id,
        resolution,
    )?;

    let _ = log_write.append_runtime_log(
        base_dir,
        project_id,
        &RuntimeLogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Info,
            source: "engine".to_owned(),
            message: format!(
                "completion panel: {} completers, min={}, threshold={}",
                panel.completers.len(),
                min_completers,
                consensus_threshold
            ),
        },
    );

    // Execute the completion panel workflow.
    let result = completion::execute_completion_panel(
        agent_service,
        artifact_write,
        log_write,
        base_dir,
        project_root,
        backend_working_dir,
        project_id,
        run_id,
        cursor,
        &panel.completers,
        min_completers,
        consensus_threshold,
        prompt_reference,
        snapshot.rollback_point_meta.rollback_count,
        &timeout_for_backend,
        cancellation_token,
    )
    .await?;

    // Build the commit data but do NOT persist aggregate or emit stage_completed
    // yet. The caller commits these atomically with the post-panel transition so
    // that a failure after stage_completed but before the cursor transition cannot
    // leave the run in an inconsistent state.
    let base_id = format!(
        "{}-{}-aggregate-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );
    let rollback_count = snapshot.rollback_point_meta.rollback_count;
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

    let commit_data = CompletionCommitData {
        aggregate_payload: result.aggregate_payload,
        aggregate_artifact: result.aggregate_artifact,
        payload_id,
        artifact_id,
        completion_round: cursor.completion_round,
    };

    let _ = log_write.append_runtime_log(
        base_dir,
        project_id,
        &RuntimeLogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Info,
            source: "engine".to_owned(),
            message: format!("completion panel executed: verdict={}", result.verdict),
        },
    );

    // Determine outcome based on verdict.
    match result.verdict {
        CompletionVerdict::Complete => Ok(CompletionPanelOutcome::Complete(
            cursor.clone(),
            commit_data,
        )),
        CompletionVerdict::ContinueWork => {
            let next_cursor = cursor.advance_completion_round(StageId::Planning)?;
            Ok(CompletionPanelOutcome::ContinueWork(
                next_cursor,
                commit_data,
            ))
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_final_review_panel<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    project_root: &Path,
    backend_working_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &mut RunSnapshot,
    cursor: &StageCursor,
    planning_stage: StageId,
    effective_config: &EffectiveConfig,
    prompt_reference: &str,
    cancellation_token: CancellationToken,
) -> AppResult<FinalReviewPanelOutcome>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let stage_id = StageId::FinalReview;
    let policy = BackendPolicyService::new(effective_config);
    let mut panel = policy.resolve_final_review_panel(cursor.cycle)?;
    let min_reviewers = effective_config.final_review_policy().min_reviewers;
    let consensus_threshold = effective_config.final_review_policy().consensus_threshold;
    let max_restarts = effective_config.final_review_policy().max_restarts;

    agent_service
        .adapter()
        .check_availability(&panel.planner)
        .await
        .map_err(|error| AppError::BackendUnavailable {
            backend: panel.planner.backend.family.to_string(),
            details: format!("required final-review planner unavailable: {error}"),
        })?;
    agent_service
        .adapter()
        .check_availability(&panel.arbiter)
        .await
        .map_err(|error| AppError::BackendUnavailable {
            backend: panel.arbiter.backend.family.to_string(),
            details: format!("required final-review arbiter unavailable: {error}"),
        })?;

    let mut available_reviewers = Vec::new();
    for member in &panel.reviewers {
        match agent_service
            .adapter()
            .check_availability(&member.target)
            .await
        {
            Ok(()) => available_reviewers.push(member.clone()),
            Err(error) => {
                if member.required {
                    return Err(error);
                }
            }
        }
    }
    if available_reviewers.len() < min_reviewers {
        return Err(AppError::InsufficientPanelMembers {
            panel: "final_review".to_owned(),
            resolved: available_reviewers.len(),
            minimum: min_reviewers,
        });
    }
    panel.reviewers = available_reviewers;

    let resolution =
        build_final_review_snapshot(stage_id, &panel.reviewers, &panel.planner, &panel.arbiter);
    let planner_timeout =
        policy.timeout_for_role(panel.planner.backend.family, BackendPolicyRole::Planner);
    let reviewer_timeout_for_backend = |family: BackendFamily| -> Duration {
        policy.timeout_for_role(family, BackendPolicyRole::FinalReviewer)
    };
    let arbiter_timeout =
        policy.timeout_for_role(panel.arbiter.backend.family, BackendPolicyRole::Arbiter);

    // Pre-validate the reviewer template (always used) BEFORE durable state writes.
    // Voter and arbiter templates are validated lazily at invocation time since
    // they may not be needed (no amendments → no voters, no disputes → no arbiter).
    template_catalog::resolve("final_review_reviewer", base_dir, Some(project_id))?;

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
        return Err(AppError::StageCommitFailed {
            stage_id,
            details: format!("failed to persist stage_entered for final_review: {error}"),
        });
    }

    persist_stage_resolution_snapshot(
        snapshot,
        run_snapshot_write,
        base_dir,
        project_id,
        resolution,
    )?;

    let _ = log_write.append_runtime_log(
        base_dir,
        project_id,
        &RuntimeLogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Info,
            source: "engine".to_owned(),
            message: format!(
                "final_review panel: {} reviewers, threshold={}, max_restarts={}",
                panel.reviewers.len(),
                consensus_threshold,
                max_restarts
            ),
        },
    );

    let result = final_review::execute_final_review_panel(
        agent_service,
        artifact_write,
        log_write,
        base_dir,
        project_root,
        backend_working_dir,
        project_id,
        run_id,
        cursor,
        &panel,
        min_reviewers,
        consensus_threshold,
        max_restarts,
        current_active_run(snapshot)?.final_review_restart_count,
        prompt_reference,
        snapshot.rollback_point_meta.rollback_count,
        planner_timeout,
        &reviewer_timeout_for_backend,
        arbiter_timeout,
        cancellation_token,
    )
    .await?;

    let base_id = format!(
        "{}-{}-aggregate-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );
    let rollback_count = snapshot.rollback_point_meta.rollback_count;
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

    let accepted_amendments = result
        .final_accepted_amendments
        .iter()
        .enumerate()
        .map(|(index, amendment)| {
            let source = crate::contexts::project_run_record::model::AmendmentSource::WorkflowStage;
            let dedup_key = QueuedAmendment::compute_dedup_key(&source, &amendment.normalized_body);
            QueuedAmendment {
                amendment_id: amendment.amendment_id.clone(),
                source_stage: stage_id,
                source_cycle: cursor.cycle,
                source_completion_round: cursor.completion_round,
                body: amendment.normalized_body.clone(),
                created_at: Utc::now(),
                batch_sequence: (index + 1) as u32,
                source,
                dedup_key,
            }
        })
        .collect::<Vec<_>>();

    let commit_data = FinalReviewCommitData {
        aggregate_payload: result.aggregate_payload,
        aggregate_artifact: result.aggregate_artifact,
        payload_id,
        artifact_id,
        completion_round: cursor.completion_round,
        accepted_amendments,
    };

    if result.restart_required {
        let next_cursor = cursor.advance_completion_round(planning_stage)?;
        Ok(FinalReviewPanelOutcome::Restart(next_cursor, commit_data))
    } else {
        Ok(FinalReviewPanelOutcome::Complete(
            cursor.clone(),
            commit_data,
        ))
    }
}

// ── Completion Aggregate Commit ─────────────────────────────────────────────

/// Persist the completion aggregate payload/artifact records WITHOUT writing
/// any journal events. Returns `Ok(())` on success; on failure the records are
/// not written and resume restarts from `completion_panel`.
///
/// Journal events (`stage_completed`, `completion_round_advanced`) are written
/// by the caller AFTER the transition is fully committed, so that a transition
/// failure never leaves a leaked aggregate or `stage_completed` event.
#[allow(clippy::too_many_arguments)]
fn persist_completion_aggregate_records(
    artifact_write: &dyn PayloadArtifactWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    cursor: &StageCursor,
    stage_id: StageId,
    commit_data: &CompletionCommitData,
) -> AppResult<()> {
    // Write aggregate records using the pre-computed IDs from commit_data,
    // NOT from the cursor. In the ContinueWork path, the cursor passed here
    // has an advanced completion_round, but the IDs were computed with the
    // original round during dispatch_completion_panel.
    let now = Utc::now();
    let producer = RecordProducer::System {
        component: "completion_aggregator".to_owned(),
    };
    let payload_record = PayloadRecord {
        payload_id: commit_data.payload_id.clone(),
        stage_id,
        cycle: cursor.cycle,
        attempt: cursor.attempt,
        created_at: now,
        payload: commit_data.aggregate_payload.clone(),
        record_kind: RecordKind::StageAggregate,
        producer: Some(producer.clone()),
        completion_round: commit_data.completion_round,
    };
    let artifact_record = ArtifactRecord {
        artifact_id: commit_data.artifact_id.clone(),
        payload_id: commit_data.payload_id.clone(),
        stage_id,
        created_at: now,
        content: commit_data.aggregate_artifact.clone(),
        record_kind: RecordKind::StageAggregate,
        producer: Some(producer),
        completion_round: commit_data.completion_round,
    };
    artifact_write.write_payload_artifact_pair(
        base_dir,
        project_id,
        &payload_record,
        &artifact_record,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn persist_final_review_aggregate_records(
    artifact_write: &dyn PayloadArtifactWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    cursor: &StageCursor,
    stage_id: StageId,
    commit_data: &FinalReviewCommitData,
) -> AppResult<()> {
    let now = Utc::now();
    let producer = RecordProducer::System {
        component: "final_review_aggregator".to_owned(),
    };
    let payload_record = PayloadRecord {
        payload_id: commit_data.payload_id.clone(),
        stage_id,
        cycle: cursor.cycle,
        attempt: cursor.attempt,
        created_at: now,
        payload: commit_data.aggregate_payload.clone(),
        record_kind: RecordKind::StageAggregate,
        producer: Some(producer.clone()),
        completion_round: commit_data.completion_round,
    };
    let artifact_record = ArtifactRecord {
        artifact_id: commit_data.artifact_id.clone(),
        payload_id: commit_data.payload_id.clone(),
        stage_id,
        created_at: now,
        content: commit_data.aggregate_artifact.clone(),
        record_kind: RecordKind::StageAggregate,
        producer: Some(producer),
        completion_round: commit_data.completion_round,
    };
    artifact_write.write_payload_artifact_pair(
        base_dir,
        project_id,
        &payload_record,
        &artifact_record,
    )?;
    Ok(())
}

/// Clean up aggregate payload/artifact files that were persisted by
/// `persist_completion_aggregate_records` but whose journal commit failed.
fn cleanup_completion_aggregate_records(
    artifact_write: &dyn PayloadArtifactWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    commit_data: &CompletionCommitData,
) {
    let _ = artifact_write.remove_payload_artifact_pair(
        base_dir,
        project_id,
        &commit_data.payload_id,
        &commit_data.artifact_id,
    );
}

fn cleanup_final_review_aggregate_records(
    artifact_write: &dyn PayloadArtifactWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    commit_data: &FinalReviewCommitData,
) {
    let _ = artifact_write.remove_payload_artifact_pair(
        base_dir,
        project_id,
        &commit_data.payload_id,
        &commit_data.artifact_id,
    );
}

// ── Stage Resolution Snapshot ──────────────────────────────────────────────

fn resolved_target_to_record(target: &ResolvedBackendTarget) -> ResolvedTargetRecord {
    ResolvedTargetRecord {
        backend_family: target.backend.family.to_string(),
        model_id: target.model.model_id.clone(),
    }
}

/// Build a stage resolution snapshot for a single-target stage.
pub fn build_single_target_snapshot(
    stage_id: StageId,
    target: &ResolvedBackendTarget,
) -> StageResolutionSnapshot {
    StageResolutionSnapshot {
        stage_id,
        resolved_at: Utc::now(),
        primary_target: Some(resolved_target_to_record(target)),
        prompt_review_validators: Vec::new(),
        prompt_review_refiner: None,
        completion_completers: Vec::new(),
        final_review_reviewers: Vec::new(),
        final_review_planner: None,
        final_review_arbiter: None,
    }
}

/// Build a stage resolution snapshot for the prompt-review panel.
pub fn build_prompt_review_snapshot(
    stage_id: StageId,
    panel: &crate::contexts::agent_execution::policy::PromptReviewPanelResolution,
) -> StageResolutionSnapshot {
    StageResolutionSnapshot {
        stage_id,
        resolved_at: Utc::now(),
        primary_target: None,
        prompt_review_validators: panel
            .validators
            .iter()
            .map(|m| resolved_target_to_record(&m.target))
            .collect(),
        prompt_review_refiner: Some(resolved_target_to_record(&panel.refiner)),
        completion_completers: Vec::new(),
        final_review_reviewers: Vec::new(),
        final_review_planner: None,
        final_review_arbiter: None,
    }
}

/// Build a stage resolution snapshot for the completion panel.
pub fn build_completion_snapshot(
    stage_id: StageId,
    completers: &[crate::contexts::agent_execution::policy::ResolvedPanelMember],
) -> StageResolutionSnapshot {
    StageResolutionSnapshot {
        stage_id,
        resolved_at: Utc::now(),
        primary_target: None,
        prompt_review_validators: Vec::new(),
        prompt_review_refiner: None,
        completion_completers: completers
            .iter()
            .map(|m| resolved_target_to_record(&m.target))
            .collect(),
        final_review_reviewers: Vec::new(),
        final_review_planner: None,
        final_review_arbiter: None,
    }
}

/// Build a stage resolution snapshot for the final-review panel.
pub fn build_final_review_snapshot(
    stage_id: StageId,
    reviewers: &[crate::contexts::agent_execution::policy::ResolvedPanelMember],
    planner: &ResolvedBackendTarget,
    arbiter: &ResolvedBackendTarget,
) -> StageResolutionSnapshot {
    StageResolutionSnapshot {
        stage_id,
        resolved_at: Utc::now(),
        primary_target: None,
        prompt_review_validators: Vec::new(),
        prompt_review_refiner: None,
        completion_completers: Vec::new(),
        final_review_reviewers: reviewers
            .iter()
            .map(|member| resolved_target_to_record(&member.target))
            .collect(),
        final_review_planner: Some(resolved_target_to_record(planner)),
        final_review_arbiter: Some(resolved_target_to_record(arbiter)),
    }
}

/// Persist a stage resolution snapshot on the active run. If persistence
/// fails, the stage must abort with no agent side effects.
pub fn persist_stage_resolution_snapshot(
    snapshot: &mut RunSnapshot,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    resolution: StageResolutionSnapshot,
) -> AppResult<()> {
    if let Some(ref mut active) = snapshot.active_run {
        active.stage_resolution_snapshot = Some(resolution.clone());
    }
    run_snapshot_write
        .write_run_snapshot(base_dir, project_id, snapshot)
        .map_err(|e| AppError::SnapshotPersistFailed {
            stage_id: resolution.stage_id,
            details: format!("failed to persist stage resolution snapshot: {e}"),
        })
}

// ── Resume Drift Detection ─────────────────────────────────────────────────

/// Compare a new resolution against the persisted snapshot. Returns `true`
/// if the resolution changed (drift detected).
pub fn resolution_has_drifted(
    old: &StageResolutionSnapshot,
    new: &StageResolutionSnapshot,
) -> bool {
    old.primary_target != new.primary_target
        || old.prompt_review_validators != new.prompt_review_validators
        || old.prompt_review_refiner != new.prompt_review_refiner
        || old.completion_completers != new.completion_completers
        || old.final_review_reviewers != new.final_review_reviewers
        || match (&old.final_review_planner, &new.final_review_planner) {
            (Some(old_planner), Some(new_planner)) => old_planner != new_planner,
            (Some(_), None) => true,
            // Old snapshots created before planner tracking will have `None`;
            // don't flag drift for legacy resumes.
            (None, _) => false,
        }
        || old.final_review_arbiter != new.final_review_arbiter
}

/// Check whether a drifted resolution still satisfies the required-backend
/// and minimum-count constraints.
pub fn drift_still_satisfies_requirements(
    new_snapshot: &StageResolutionSnapshot,
    stage_id: StageId,
    effective_config: &EffectiveConfig,
) -> AppResult<()> {
    match stage_id {
        StageId::PromptReview => {
            let min = effective_config.prompt_review_policy().min_reviewers;
            if new_snapshot.prompt_review_validators.len() < min {
                return Err(AppError::ResumeDriftFailure {
                    stage_id,
                    details: format!(
                        "re-resolved prompt review validators ({}) < min_reviewers ({})",
                        new_snapshot.prompt_review_validators.len(),
                        min,
                    ),
                });
            }
        }
        StageId::CompletionPanel => {
            let min = effective_config.completion_policy().min_completers;
            if new_snapshot.completion_completers.len() < min {
                return Err(AppError::ResumeDriftFailure {
                    stage_id,
                    details: format!(
                        "re-resolved completion completers ({}) < min_completers ({})",
                        new_snapshot.completion_completers.len(),
                        min,
                    ),
                });
            }
        }
        StageId::FinalReview => {
            let min = effective_config.final_review_policy().min_reviewers;
            if new_snapshot.final_review_reviewers.len() < min {
                return Err(AppError::ResumeDriftFailure {
                    stage_id,
                    details: format!(
                        "re-resolved final-review reviewers ({}) < min_reviewers ({})",
                        new_snapshot.final_review_reviewers.len(),
                        min,
                    ),
                });
            }
            if new_snapshot.final_review_arbiter.is_none() {
                return Err(AppError::ResumeDriftFailure {
                    stage_id,
                    details: "re-resolved final-review panel has no arbiter".to_owned(),
                });
            }
            if new_snapshot.final_review_planner.is_none() {
                return Err(AppError::ResumeDriftFailure {
                    stage_id,
                    details: "re-resolved final-review panel has no planner".to_owned(),
                });
            }
        }
        _ => {
            // For single-target stages, check that a primary target still exists.
            if new_snapshot.primary_target.is_none() {
                return Err(AppError::ResumeDriftFailure {
                    stage_id,
                    details: "re-resolved stage has no primary target".to_owned(),
                });
            }
        }
    }
    Ok(())
}

/// Emit a runtime warning and a durable journal warning for resume drift,
/// then update the snapshot with the new resolution.
#[allow(clippy::too_many_arguments)]
pub fn emit_resume_drift_warning(
    old: &StageResolutionSnapshot,
    new: &StageResolutionSnapshot,
    run_id: &RunId,
    stage_id: StageId,
    seq: &mut u64,
    snapshot: &mut RunSnapshot,
    journal_store: &dyn JournalStorePort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<()> {
    let details = serde_json::json!({
        "old_resolution": serde_json::to_value(old).unwrap_or_default(),
        "new_resolution": serde_json::to_value(new).unwrap_or_default(),
    });

    // Runtime log (best-effort)
    let _ = log_write.append_runtime_log(
        base_dir,
        project_id,
        &RuntimeLogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Warn,
            source: "engine".to_owned(),
            message: format!(
                "resume drift detected for stage {}: resolution changed",
                stage_id.as_str()
            ),
        },
    );

    // Durable journal warning
    *seq += 1;
    let warning_event = journal::durable_warning_event(
        *seq,
        Utc::now(),
        run_id,
        stage_id,
        "resume_drift",
        &format!(
            "stage {} resolution changed between suspend and resume",
            stage_id.as_str()
        ),
        details,
    );
    let line = journal::serialize_event(&warning_event)?;
    if let Err(e) = journal_store.append_event(base_dir, project_id, &line) {
        *seq -= 1;
        return Err(AppError::StageCommitFailed {
            stage_id,
            details: format!("resume drift warning must be durably persisted before continuing, but journal append failed: {e}"),
        });
    }

    // Update both the active_run snapshot and the top-level
    // last_stage_resolution_snapshot so that:
    // 1. The resumed ActiveRun carries the new resolution forward.
    // 2. If the run fails/pauses again, fail_run/pause_run copies from
    //    active_run.stage_resolution_snapshot to last_stage_resolution_snapshot,
    //    so the next resume drift check uses the updated resolution.
    snapshot.last_stage_resolution_snapshot = Some(new.clone());
    if let Some(ref mut active) = snapshot.active_run {
        active.stage_resolution_snapshot = Some(new.clone());
    }
    run_snapshot_write
        .write_run_snapshot(base_dir, project_id, snapshot)
        .map_err(|e| AppError::SnapshotPersistFailed {
            stage_id,
            details: format!(
                "failed to persist updated resolution snapshot after drift warning: {e}"
            ),
        })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::Value;
    use tempfile::tempdir;

    use crate::contexts::agent_execution::policy::ResolvedPanelMember;
    use crate::contexts::workspace_governance::{initialize_workspace, EffectiveConfig};
    use crate::shared::domain::{BackendFamily, ResolvedBackendTarget, StageId};
    use crate::shared::error::AppError;

    use super::{
        build_final_review_snapshot, drift_still_satisfies_requirements, resolution_has_drifted,
    };

    fn final_review_reviewers() -> Vec<ResolvedPanelMember> {
        vec![
            ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-a"),
                required: true,
                configured_index: 0,
            },
            ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Codex, "reviewer-b"),
                required: false,
                configured_index: 1,
            },
        ]
    }

    fn final_review_effective_config() -> EffectiveConfig {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");
        EffectiveConfig::load(temp_dir.path()).expect("load effective config")
    }

    #[test]
    fn final_review_snapshot_serialization_includes_planner_target() {
        let reviewers = final_review_reviewers();
        let planner = ResolvedBackendTarget::new(BackendFamily::OpenRouter, "planner-model");
        let arbiter = ResolvedBackendTarget::new(BackendFamily::Stub, "arbiter-model");

        let snapshot =
            build_final_review_snapshot(StageId::FinalReview, &reviewers, &planner, &arbiter);
        let serialized = serde_json::to_value(&snapshot).expect("snapshot should serialize");

        assert_eq!(
            serialized.get("final_review_planner"),
            Some(&serde_json::json!({
                "backend_family": "openrouter",
                "model_id": "planner-model",
            }))
        );

        let serialized_reviewers = serialized
            .get("final_review_reviewers")
            .and_then(Value::as_array)
            .expect("reviewers should serialize as an array");
        assert_eq!(serialized_reviewers.len(), 2);
        assert_eq!(
            serialized.get("final_review_arbiter"),
            Some(&serde_json::json!({
                "backend_family": "stub",
                "model_id": "arbiter-model",
            }))
        );
    }

    #[test]
    fn final_review_planner_drift_detection_handles_new_and_legacy_snapshots() {
        let reviewers = final_review_reviewers();
        let arbiter = ResolvedBackendTarget::new(BackendFamily::Stub, "arbiter-model");
        let planner_a = ResolvedBackendTarget::new(BackendFamily::OpenRouter, "planner-a");
        let planner_b = ResolvedBackendTarget::new(BackendFamily::OpenRouter, "planner-b");

        let old =
            build_final_review_snapshot(StageId::FinalReview, &reviewers, &planner_a, &arbiter);
        let changed =
            build_final_review_snapshot(StageId::FinalReview, &reviewers, &planner_b, &arbiter);
        assert!(resolution_has_drifted(&old, &changed));

        let mut legacy_snapshot = old.clone();
        legacy_snapshot.final_review_planner = None;
        assert!(!resolution_has_drifted(&legacy_snapshot, &old));

        let mut missing_planner = old.clone();
        missing_planner.final_review_planner = None;
        assert!(resolution_has_drifted(&old, &missing_planner));
    }

    #[test]
    fn final_review_drift_requirements_fail_when_planner_is_missing() {
        let reviewers = final_review_reviewers();
        let planner = ResolvedBackendTarget::new(BackendFamily::OpenRouter, "planner-model");
        let arbiter = ResolvedBackendTarget::new(BackendFamily::Stub, "arbiter-model");
        let config = final_review_effective_config();

        let mut snapshot =
            build_final_review_snapshot(StageId::FinalReview, &reviewers, &planner, &arbiter);
        snapshot.final_review_planner = None;

        let error = drift_still_satisfies_requirements(&snapshot, StageId::FinalReview, &config)
            .expect_err("missing planner should fail final-review requirements");
        assert!(matches!(
            error,
            AppError::ResumeDriftFailure {
                stage_id: StageId::FinalReview,
                details,
            } if details == "re-resolved final-review panel has no planner"
        ));
    }
}
