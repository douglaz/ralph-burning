use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::adapters::fs::{
    FileSystem, FsArtifactStore, FsMilestoneControllerStore, FsMilestoneJournalStore,
    FsMilestoneSnapshotStore, FsPlannedElsewhereMappingStore, FsProjectStore, FsRollbackPointStore,
    FsTaskRunLineageStore, RunPidOwner,
};
use crate::adapters::openrouter_backend::recover_structured_payload_from_response_body;
use crate::adapters::process_backend::{
    is_codex_raw_transcript_envelope, processed_contract_schema_value,
    recover_codex_execution_payload_from_raw_transcript,
    recover_structured_payload_from_process_stdout,
};
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
use crate::contexts::milestone_record::controller as milestone_controller;
use crate::contexts::milestone_record::model::{MilestoneId, PlannedElsewhereMapping};
use crate::contexts::milestone_record::service as milestone_service;
use crate::contexts::project_run_record::journal;
use crate::contexts::project_run_record::model::{
    ActiveRun, ArtifactRecord, CycleHistoryEntry, IterativeImplementerLoopPolicy,
    IterativeImplementerState, JournalEvent, JournalEventType, LogLevel, PayloadRecord,
    ProjectRecord, QueuedAmendment, ResolvedTargetRecord, RollbackPoint, RunSnapshot, RunStatus,
    RuntimeLogEntry, StageResolutionSnapshot,
};
use crate::contexts::project_run_record::queries;
use crate::contexts::project_run_record::service::{
    AmendmentQueuePort, ArtifactStorePort, JournalStorePort, PayloadArtifactWritePort,
    ProjectStorePort, RollbackPointStorePort, RunSnapshotPort, RunSnapshotWritePort,
    RuntimeLogWritePort,
};
use crate::contexts::project_run_record::task_prompt_contract;
use crate::contexts::workflow_composition::payloads::{
    ClassifiedFinding, ReviewOutcome, StagePayload, ValidationPayload,
};
use crate::contexts::workflow_composition::review_classification::ReviewFindingClass;
use crate::contexts::workspace_governance::config::EffectiveConfig;
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
use super::panel_contracts::{
    CompletionVerdict, FinalReviewAggregatePayload, RecordKind, RecordProducer,
};
use super::prompt_review;
use super::retry_policy::RetryPolicy;
use super::review_classification;
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

fn should_retry_stage_failure(
    retry_policy: &RetryPolicy,
    failure_class: FailureClass,
    error: &AppError,
    cursor: &StageCursor,
    cancellation_token: &CancellationToken,
) -> bool {
    retry_policy.is_retryable(failure_class)
        && !final_review::is_final_review_invocation_retry_exhaustion_error(error)
        && !final_review::is_terminal_final_review_contract_failure(error)
        && cursor.attempt < retry_policy.max_attempts(failure_class)
        && !matches!(failure_class, FailureClass::Cancellation)
        && !cancellation_token.is_cancelled()
}

#[allow(clippy::too_many_arguments)]
pub fn build_stage_prompt(
    artifact_store: &dyn ArtifactStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    project_root: &Path,
    prompt_reference: &str,
    backend_family: BackendFamily,
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
    let schema = serde_json::to_string_pretty(&processed_contract_schema_value(
        &InvocationContract::Stage(*contract),
        backend_family,
    ))?;
    let task_prompt_contract_block = task_prompt_contract::stage_consumer_guidance_for_stage_prompt(
        contract.stage_id,
        &project_prompt,
    );

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

    let classification_guidance_block = match contract.stage_id {
        StageId::Review => {
            let planned_elsewhere_ids =
                task_prompt_contract::extract_planned_elsewhere_routing_bead_ids(&project_prompt);
            review_classification::render_classification_guidance(&planned_elsewhere_ids, false)
        }
        StageId::Planning | StageId::PlanAndImplement => {
            review_classification::render_scope_guidance(&project_prompt)
        }
        _ => String::new(),
    };

    let resolved_template = template_catalog::resolve(template_id, base_dir, Some(project_id))?;
    let template_has_classification_guidance =
        template_catalog::extract_placeholders(&resolved_template.content)
            .contains("classification_guidance");
    let rendered = template_catalog::render(
        &resolved_template,
        &[
            ("role_instruction", &role_instruction),
            ("task_prompt_contract", &task_prompt_contract_block),
            ("project_prompt", project_prompt.trim_end()),
            ("json_schema", &schema),
            ("prior_outputs", &prior_outputs_block),
            ("remediation", &remediation_block),
            ("classification_guidance", &classification_guidance_block),
        ],
    )?;

    if matches!(
        contract.stage_id,
        StageId::Planning | StageId::PlanAndImplement
    ) && !classification_guidance_block.is_empty()
        && !template_has_classification_guidance
    {
        Ok(inject_scope_guidance(
            &rendered,
            &classification_guidance_block,
        ))
    } else {
        Ok(rendered)
    }
}

fn inject_scope_guidance(rendered_prompt: &str, scope_guidance: &str) -> String {
    let scope_guidance = scope_guidance.trim_end();
    if scope_guidance.is_empty() || rendered_prompt.contains(scope_guidance) {
        return rendered_prompt.to_owned();
    }

    const SCHEMA_HEADING: &str = "\n## Authoritative JSON Schema";
    let schema_index = rendered_prompt.rfind(SCHEMA_HEADING).or_else(|| {
        rendered_prompt
            .starts_with(&SCHEMA_HEADING[1..])
            .then_some(0)
    });

    if let Some(index) = schema_index {
        let mut injected = String::with_capacity(rendered_prompt.len() + scope_guidance.len() + 2);
        injected.push_str(rendered_prompt[..index].trim_end());
        injected.push_str("\n\n");
        injected.push_str(scope_guidance);
        injected.push_str(&rendered_prompt[index..]);
        injected
    } else {
        format!("{}\n\n{}", rendered_prompt.trim_end(), scope_guidance)
    }
}

/// Resolved target per stage for preflight.
#[derive(Clone)]
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
                let mut probed = Vec::new();
                preflight_required_panel_target(
                    adapter,
                    entry.stage_id,
                    "refiner",
                    &panel.refiner,
                    "prompt-review refiner",
                    &mut probed,
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
                    &mut probed,
                    false, // prompt_review does not degrade on BackendExhausted
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
                let mut probed = Vec::new();
                preflight_panel_members(
                    adapter,
                    entry.stage_id,
                    "completer",
                    "completion",
                    "completion completer",
                    &panel.completers,
                    effective_config.completion_policy().min_completers,
                    &mut probed,
                    true, // completion supports graceful degradation
                )
                .await?;
            }
            StageId::FinalReview => {
                let policy = BackendPolicyService::new(effective_config);
                let panel = resolve_final_review_panel_for_preflight(&policy, cycle)?;
                let mut probed = Vec::new();
                preflight_final_review_required_panel_target(
                    adapter,
                    entry.stage_id,
                    "arbiter",
                    BackendPolicyRole::Arbiter,
                    &panel.arbiter,
                    "final-review arbiter",
                    &mut probed,
                )
                .await?;
                preflight_final_review_panel_members(
                    adapter,
                    entry.stage_id,
                    "reviewer",
                    "final_review",
                    "final-review reviewer",
                    &panel.reviewers,
                    BackendPolicyRole::FinalReviewer,
                    effective_config.final_review_policy().min_reviewers,
                    &mut probed,
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
    // Resolve each member individually for member-specific error attribution.
    // resolve_role_target(PromptReviewer) honours prompt_review.refiner_backend
    // via selection_for_role.
    let refiner = policy
        .resolve_role_target(BackendPolicyRole::PromptReviewer, cycle)
        .map_err(|error| AppError::PreflightFailed {
            stage_id: StageId::PromptReview,
            details: format!("required prompt-review refiner resolution failed: {error}"),
        })?;
    let validators =
        policy
            .resolve_prompt_review_validators()
            .map_err(|error| AppError::PreflightFailed {
                stage_id: StageId::PromptReview,
                details: format!("prompt-review validator resolution failed: {error}"),
            })?;
    Ok(PromptReviewPanelResolution {
        refiner,
        validators,
    })
}

fn resolve_final_review_panel_for_preflight(
    policy: &BackendPolicyService<'_>,
    cycle: u32,
) -> AppResult<FinalReviewPanelResolution> {
    // Resolve each member individually for member-specific error attribution.
    // resolve_role_target(Arbiter) honours final_review.arbiter_backend via
    // selection_for_role.
    let arbiter = policy
        .resolve_role_target(BackendPolicyRole::Arbiter, cycle)
        .map_err(|error| AppError::PreflightFailed {
            stage_id: StageId::FinalReview,
            details: format!("required final-review arbiter resolution failed: {error}"),
        })?;
    let reviewers =
        policy
            .resolve_final_review_reviewers()
            .map_err(|error| AppError::PreflightFailed {
                stage_id: StageId::FinalReview,
                details: format!("final-review reviewer resolution failed: {error}"),
            })?;
    Ok(FinalReviewPanelResolution { reviewers, arbiter })
}

async fn preflight_required_panel_target<A: AgentExecutionPort>(
    adapter: &A,
    stage_id: StageId,
    role: &'static str,
    target: &ResolvedBackendTarget,
    member_name: &str,
    probed: &mut Vec<ResolvedBackendTarget>,
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
    if !probed.contains(target) {
        adapter
            .check_availability(target)
            .await
            .map_err(|error| AppError::PreflightFailed {
                stage_id,
                details: format!("required {member_name} failed availability preflight: {error}"),
            })?;
        probed.push(target.clone());
    }
    Ok(())
}

async fn preflight_final_review_required_panel_target<A: AgentExecutionPort>(
    adapter: &A,
    stage_id: StageId,
    role: &'static str,
    policy_role: BackendPolicyRole,
    target: &ResolvedBackendTarget,
    member_name: &str,
    probed: &mut Vec<ResolvedBackendTarget>,
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
    if !probed.contains(target) {
        final_review::check_final_review_availability_with_retry_on_adapter(
            adapter,
            target,
            policy_role,
            role,
            role,
            CancellationToken::new(),
        )
        .await
        .map_err(|error| AppError::PreflightFailed {
            stage_id,
            details: format!("required {member_name} failed availability preflight: {error}"),
        })?;
        probed.push(target.clone());
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn preflight_panel_members<A: AgentExecutionPort>(
    adapter: &A,
    stage_id: StageId,
    role: &'static str,
    panel_name: &'static str,
    member_name: &str,
    members: &[ResolvedPanelMember],
    minimum: usize,
    probed: &mut Vec<ResolvedBackendTarget>,
    supports_degradation: bool,
) -> AppResult<()> {
    let mut available_members = 0usize;
    let mut exhausted_count = 0usize;

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

        if probed.contains(&member.target) {
            available_members += 1;
            continue;
        }

        match adapter.check_availability(&member.target).await {
            Ok(()) => {
                probed.push(member.target.clone());
                available_members += 1;
            }
            Err(error) => {
                // BackendExhausted → skip for graceful degradation instead
                // of aborting the entire preflight.  Only applies to panels
                // that support degradation (completion, final_review); for
                // prompt_review the exhaustion falls through to the normal
                // required/optional handling below.
                if supports_degradation
                    && error
                        .failure_class()
                        .is_some_and(|fc| fc == FailureClass::BackendExhausted)
                {
                    exhausted_count += 1;
                    continue;
                }
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

    // Only BackendExhausted skips reduce quorum — other optional
    // unavailability keeps the original configured minimum.
    let effective_min = minimum
        .min(members.len().saturating_sub(exhausted_count))
        .max(1);
    if available_members < effective_min {
        return Err(AppError::PreflightFailed {
            stage_id,
            details: AppError::InsufficientPanelMembers {
                panel: panel_name.to_owned(),
                resolved: available_members,
                minimum: effective_min,
            }
            .to_string(),
        });
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn preflight_final_review_panel_members<A: AgentExecutionPort>(
    adapter: &A,
    stage_id: StageId,
    role: &'static str,
    panel_name: &'static str,
    member_name: &str,
    members: &[ResolvedPanelMember],
    policy_role: BackendPolicyRole,
    minimum: usize,
    probed: &mut Vec<ResolvedBackendTarget>,
) -> AppResult<()> {
    let mut available_members = 0usize;
    let mut exhausted_count = 0usize;

    for (idx, member) in members.iter().enumerate() {
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

        if probed.contains(&member.target) {
            available_members += 1;
            continue;
        }

        let reviewer_id = final_review::final_review_reviewer_id(idx);
        match final_review::check_final_review_availability_with_retry_on_adapter(
            adapter,
            &member.target,
            policy_role,
            &reviewer_id,
            role,
            CancellationToken::new(),
        )
        .await
        {
            Ok(_) => {
                probed.push(member.target.clone());
                available_members += 1;
            }
            Err(error)
                if error
                    .failure_class()
                    .is_some_and(|fc| fc == FailureClass::BackendExhausted) =>
            {
                exhausted_count += 1;
            }
            Err(error)
                if final_review::is_final_review_availability_retry_exhaustion_error(&error)
                    && !member.required =>
            {
                tracing::warn!(
                    reviewer = reviewer_id,
                    backend = %member.target.backend.family,
                    model = %member.target.model.model_id,
                    error = %error,
                    "optional final-review reviewer preflight exhausted transient retries; preserving reviewer for invocation-time handling"
                );
                available_members += 1;
            }
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

    let effective_min = minimum
        .min(members.len().saturating_sub(exhausted_count))
        .max(1);
    if available_members < effective_min {
        return Err(AppError::PreflightFailed {
            stage_id,
            details: AppError::InsufficientPanelMembers {
                panel: panel_name.to_owned(),
                resolved: available_members,
                minimum: effective_min,
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

fn sync_milestone_bead_start(
    project_record: &ProjectRecord,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    started_at: DateTime<Utc>,
) -> AppResult<()> {
    let Some(task_source) = project_record.task_source.as_ref() else {
        return Ok(());
    };

    let milestone_id = MilestoneId::new(&task_source.milestone_id)?;
    let plan_hash = milestone_lineage_plan_hash(
        project_record,
        base_dir,
        project_id,
        &milestone_id,
        &task_source.bead_id,
        run_id.as_str(),
    )?;
    milestone_service::record_bead_start(
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        &FsTaskRunLineageStore,
        base_dir,
        &milestone_id,
        &task_source.bead_id,
        project_id.as_str(),
        run_id.as_str(),
        &plan_hash,
        started_at,
    )?;

    milestone_controller::sync_controller_task_running(
        &FsMilestoneControllerStore,
        base_dir,
        &milestone_id,
        &task_source.bead_id,
        project_id.as_str(),
        "workflow execution is running for the bead-linked project",
        started_at,
    )?;

    Ok(())
}

pub(crate) fn milestone_lineage_plan_hash(
    project_record: &ProjectRecord,
    base_dir: &Path,
    project_id: &ProjectId,
    milestone_id: &MilestoneId,
    bead_id: &str,
    run_id: &str,
) -> AppResult<String> {
    let task_source = project_record.task_source.as_ref();

    if let Some(plan_hash) = task_source.and_then(|source| source.plan_hash.clone()) {
        return Ok(plan_hash);
    }

    if let Some(plan_hash) = milestone_service::find_runs_for_bead(
        &FsTaskRunLineageStore,
        base_dir,
        milestone_id,
        bead_id,
    )?
    .into_iter()
    .find(|entry| {
        entry.project_id == project_id.as_str() && entry.run_id.as_deref() == Some(run_id)
    })
    .and_then(|entry| entry.plan_hash)
    {
        return Ok(plan_hash);
    }

    if let Some(plan_version) = task_source.and_then(|source| source.plan_version) {
        return Ok(format!("plan-version:{plan_version}"));
    }

    if task_source.is_some() {
        return Ok(format!("bead:{}:{}", milestone_id.as_str(), bead_id));
    }

    let snapshot =
        milestone_service::load_snapshot(&FsMilestoneSnapshotStore, base_dir, milestone_id)?;
    if let Some(plan_hash) = snapshot.plan_hash {
        return Ok(plan_hash);
    }

    Ok(format!("bead:{}:{}", milestone_id.as_str(), bead_id))
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
    iterative_implementer_state: Option<IterativeImplementerState>,
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
        iterative_implementer_state,
        stage_resolution_snapshot,
    }
}

fn carry_forward_iterative_state(
    current: &ActiveRun,
    next_cursor: &StageCursor,
) -> Option<IterativeImplementerState> {
    (iterative_state_carries_into_cursor(&current.stage_cursor, next_cursor))
        .then(|| current.iterative_implementer_state.clone())
        .flatten()
}

fn iterative_state_carries_into_cursor(
    source_cursor: &StageCursor,
    target_cursor: &StageCursor,
) -> bool {
    source_cursor.stage == target_cursor.stage
        && source_cursor.cycle == target_cursor.cycle
        && source_cursor.completion_round == target_cursor.completion_round
        && source_cursor.attempt <= target_cursor.attempt
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

fn resume_seed_active_run(snapshot: &RunSnapshot) -> AppResult<&ActiveRun> {
    snapshot
        .interrupted_run
        .as_ref()
        .or(snapshot.active_run.as_ref())
        .ok_or_else(|| AppError::ResumeFailed {
            reason: "run journal does not contain a run_started event and snapshot has no resumable run metadata"
                .to_owned(),
        })
}

fn preserve_interrupted_run(snapshot: &mut RunSnapshot) {
    snapshot.interrupted_run = snapshot.active_run.clone();
}

fn orchestrator_process_is_alive(base_dir: &Path, project_id: &ProjectId) -> AppResult<bool> {
    Ok(
        FileSystem::read_pid_file(base_dir, project_id)?.is_some_and(|record| {
            matches!(
                FileSystem::pid_record_live_state(&record),
                crate::adapters::fs::PidRecordLiveState::Live
                    | crate::adapters::fs::PidRecordLiveState::RunningUnverified
            )
        }),
    )
}

pub struct InterruptedRunUpdate<'a> {
    pub summary: &'a str,
    pub log_message: &'a str,
    pub failure_class: Option<&'a str>,
}

pub struct InterruptedRunContext<'a> {
    pub run_snapshot_read: &'a dyn RunSnapshotPort,
    pub run_snapshot_write: &'a dyn RunSnapshotWritePort,
    pub journal_store: &'a dyn JournalStorePort,
    pub log_write: &'a dyn RuntimeLogWritePort,
    pub base_dir: &'a Path,
    pub project_id: &'a ProjectId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningAttemptIdentity {
    pub run_id: String,
    pub started_at: DateTime<Utc>,
}

impl RunningAttemptIdentity {
    pub fn from_active_run(active_run: &ActiveRun) -> Self {
        Self {
            run_id: active_run.run_id.clone(),
            started_at: active_run.started_at,
        }
    }
}

fn current_running_attempt_identity(snapshot: &RunSnapshot) -> AppResult<RunningAttemptIdentity> {
    Ok(RunningAttemptIdentity::from_active_run(current_active_run(
        snapshot,
    )?))
}

fn snapshot_matches_running_attempt(
    snapshot: &RunSnapshot,
    expected_attempt: &RunningAttemptIdentity,
) -> bool {
    snapshot.status == RunStatus::Running
        && snapshot.active_run.as_ref().is_some_and(|active_run| {
            active_run.run_id == expected_attempt.run_id
                && active_run.started_at == expected_attempt.started_at
        })
}

fn snapshot_matches_interrupted_attempt(
    snapshot: &RunSnapshot,
    expected_attempt: &RunningAttemptIdentity,
) -> bool {
    snapshot.status == RunStatus::Failed
        && snapshot.active_run.is_none()
        && snapshot
            .interrupted_run
            .as_ref()
            .is_some_and(|interrupted_run| {
                interrupted_run.run_id == expected_attempt.run_id
                    && interrupted_run.started_at == expected_attempt.started_at
            })
}

pub fn mark_running_run_interrupted(
    context: InterruptedRunContext<'_>,
    expected_attempt: &RunningAttemptIdentity,
    update: InterruptedRunUpdate<'_>,
) -> AppResult<bool> {
    let mut snapshot = context
        .run_snapshot_read
        .read_run_snapshot(context.base_dir, context.project_id)?;
    if !snapshot_matches_running_attempt(&snapshot, expected_attempt) {
        return Ok(false);
    }

    if let Some(resolution) = snapshot
        .active_run
        .as_ref()
        .and_then(|active_run| active_run.stage_resolution_snapshot.clone())
    {
        snapshot.last_stage_resolution_snapshot = Some(resolution);
    }
    preserve_interrupted_run(&mut snapshot);
    snapshot.status = RunStatus::Failed;
    snapshot.active_run = None;
    snapshot.status_summary = update.summary.to_owned();
    context.run_snapshot_write.write_run_snapshot(
        context.base_dir,
        context.project_id,
        &snapshot,
    )?;
    let append_result = if let Some(failure_class) = update.failure_class {
        append_interrupted_run_failed_event(
            context.journal_store,
            context.base_dir,
            context.project_id,
            &snapshot,
            update.log_message,
            failure_class,
        )
    } else {
        Ok(())
    };
    let _ = FileSystem::remove_pid_file(context.base_dir, context.project_id);
    let _ = context.log_write.append_runtime_log(
        context.base_dir,
        context.project_id,
        &RuntimeLogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Warn,
            source: "engine".to_owned(),
            message: update.log_message.to_owned(),
        },
    );
    append_result?;
    Ok(true)
}

pub fn mark_current_process_running_run_interrupted(
    context: InterruptedRunContext<'_>,
    expected_writer_owner: Option<&str>,
    update: InterruptedRunUpdate<'_>,
) -> AppResult<bool> {
    let snapshot = context
        .run_snapshot_read
        .read_run_snapshot(context.base_dir, context.project_id)?;
    if snapshot.status != RunStatus::Running {
        return Ok(false);
    }
    let expected_attempt = current_running_attempt_identity(&snapshot)?;
    let Some(pid_record) = FileSystem::read_pid_file(context.base_dir, context.project_id)? else {
        return Ok(false);
    };
    if pid_record.pid != std::process::id() {
        return Ok(false);
    }
    if let Some(writer_owner) = expected_writer_owner {
        if pid_record.writer_owner.as_deref() != Some(writer_owner) {
            return Ok(false);
        }
    }
    if !FileSystem::pid_record_matches_attempt(
        &pid_record,
        &expected_attempt.run_id,
        expected_attempt.started_at,
    ) {
        return Ok(false);
    }

    mark_running_run_interrupted(context, &expected_attempt, update)
}

pub fn finalize_interrupted_run_failure_if_missing(
    context: InterruptedRunContext<'_>,
    expected_attempt: &RunningAttemptIdentity,
    log_message: &str,
    failure_class: &str,
) -> AppResult<bool> {
    let snapshot = context
        .run_snapshot_read
        .read_run_snapshot(context.base_dir, context.project_id)?;
    if !snapshot_matches_interrupted_attempt(&snapshot, expected_attempt) {
        return Ok(false);
    }

    let events = context
        .journal_store
        .read_journal(context.base_dir, context.project_id)?;
    if crate::contexts::project_run_record::queries::terminal_status_for_attempt(
        &expected_attempt.run_id,
        expected_attempt.started_at,
        &events,
    )
    .is_none()
    {
        append_interrupted_run_failed_event(
            context.journal_store,
            context.base_dir,
            context.project_id,
            &snapshot,
            log_message,
            failure_class,
        )?;
    }

    Ok(true)
}

fn append_interrupted_run_failed_event(
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    snapshot: &RunSnapshot,
    message: &str,
    failure_class: &str,
) -> AppResult<()> {
    let interrupted = interrupted_active_run(snapshot)?;
    let run_id = RunId::new(&interrupted.run_id).map_err(|error| AppError::CorruptRecord {
        file: "run.json".to_owned(),
        details: format!("interrupted_run contains invalid run_id: {error}"),
    })?;
    let events = journal_store.read_journal(base_dir, project_id)?;
    let event = journal::run_failed_event(
        journal::last_sequence(&events) + 1,
        Utc::now(),
        &run_id,
        interrupted.stage_cursor.stage,
        failure_class,
        message,
        snapshot.completion_rounds,
        snapshot.max_completion_rounds.unwrap_or(0),
        None,
    );
    let line = journal::serialize_event(&event)?;
    journal_store.append_event(base_dir, project_id, &line)
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
        stage_cursor.clone(),
        current.started_at,
        current.prompt_hash_at_cycle_start.clone(),
        prompt_hash_at_stage_start,
        current.qa_iterations_current_cycle,
        current.review_iterations_current_cycle,
        current.final_review_restart_count,
        carry_forward_iterative_state(current, &stage_cursor),
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
        None,
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
        stage_cursor.clone(),
        current.started_at,
        current.prompt_hash_at_cycle_start.clone(),
        prompt_hash,
        current.qa_iterations_current_cycle,
        current.review_iterations_current_cycle,
        final_review_restart_count,
        carry_forward_iterative_state(current, &stage_cursor),
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

fn event_type_label(event: &JournalEvent) -> String {
    format!("{:?}", event.event_type)
}

fn event_detail_u32(event: &JournalEvent, key: &str) -> AppResult<u32> {
    let value = event
        .details
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: format!(
                "{} event is missing numeric '{key}' detail",
                event_type_label(event)
            ),
        })?;
    u32::try_from(value).map_err(|_| AppError::CorruptRecord {
        file: "journal.ndjson".to_owned(),
        details: format!(
            "{} event '{key}' detail is out of range for u32",
            event_type_label(event)
        ),
    })
}

fn optional_event_detail_u32(event: &JournalEvent, key: &str) -> AppResult<Option<u32>> {
    let Some(value) = event.details.get(key) else {
        return Ok(None);
    };
    let Some(value) = value.as_u64() else {
        return Err(AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: format!(
                "{} event is missing numeric '{key}' detail",
                event_type_label(event)
            ),
        });
    };

    Ok(Some(u32::try_from(value).map_err(|_| {
        AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: format!(
                "{} event '{key}' detail is out of range for u32",
                event_type_label(event)
            ),
        }
    })?))
}

fn event_detail_bool(event: &JournalEvent, key: &str) -> AppResult<bool> {
    event
        .details
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: format!(
                "{} event is missing boolean '{key}' detail",
                event_type_label(event)
            ),
        })
}

fn event_matches_stage_cursor(event: &JournalEvent, cursor: &StageCursor) -> AppResult<bool> {
    let Some(stage_id) = event.details.get("stage_id").and_then(Value::as_str) else {
        return Ok(false);
    };

    Ok(stage_id == cursor.stage.as_str()
        && event_detail_u32(event, "cycle")? == cursor.cycle
        && event_detail_u32(event, "completion_round")? == cursor.completion_round)
}

fn event_matches_stage_attempt(event: &JournalEvent, cursor: &StageCursor) -> AppResult<bool> {
    match optional_event_detail_u32(event, "attempt")? {
        Some(attempt) => Ok(attempt == cursor.attempt),
        None => Ok(true),
    }
}

fn event_matches_stage_attempt_up_to_cursor(
    event: &JournalEvent,
    cursor: &StageCursor,
) -> AppResult<bool> {
    match optional_event_detail_u32(event, "attempt")? {
        Some(attempt) => Ok(attempt <= cursor.attempt),
        None => Ok(true),
    }
}

fn reconstruct_iterative_state_from_events(
    events: &[JournalEvent],
    run_id: &RunId,
    cursor: &StageCursor,
) -> AppResult<Option<IterativeImplementerState>> {
    let mut completed_iterations = 0;
    let mut stable_count = 0;
    let mut found = false;

    for event in events {
        if event.event_type != JournalEventType::ImplementerIterationCompleted
            || !event_matches_run(event, run_id)
            || !event_matches_stage_cursor(event, cursor)?
            || !event_matches_stage_attempt_up_to_cursor(event, cursor)?
        {
            continue;
        }

        let iteration = event_detail_u32(event, "iteration")?;
        if iteration <= completed_iterations {
            continue;
        }

        let diff_changed = event_detail_bool(event, "diff_changed")?;
        completed_iterations = iteration;
        stable_count = if diff_changed { 0 } else { stable_count + 1 };
        found = true;
    }

    Ok(found.then_some(IterativeImplementerState {
        completed_iterations,
        stable_count,
        loop_policy: None,
        stage_target: None,
    }))
}

fn resume_iteration_counters(
    snapshot: &RunSnapshot,
    resume_cursor: &StageCursor,
    resume_events: &[JournalEvent],
) -> AppResult<(u32, u32, Option<IterativeImplementerState>)> {
    let interrupted = interrupted_active_run(snapshot)?;
    if interrupted.stage_cursor.cycle != resume_cursor.cycle {
        return Ok((0, 0, None));
    }

    let snapshot_state = (interrupted.stage_cursor.stage == resume_cursor.stage
        && interrupted.stage_cursor.cycle == resume_cursor.cycle
        && interrupted.stage_cursor.completion_round == resume_cursor.completion_round
        && interrupted.stage_cursor.attempt <= resume_cursor.attempt)
        .then(|| interrupted.iterative_implementer_state.clone())
        .flatten();
    let snapshot_loop_policy = snapshot_state
        .as_ref()
        .and_then(|state| state.loop_policy.clone());
    let snapshot_stage_target = snapshot_state
        .as_ref()
        .and_then(|state| state.stage_target.clone())
        .or_else(|| {
            interrupted
                .stage_resolution_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.primary_target.clone())
        });
    let run_id = RunId::new(&interrupted.run_id)?;
    let recovered_state =
        reconstruct_iterative_state_from_events(resume_events, &run_id, resume_cursor)?;
    let mut iterative_state = match (snapshot_state, recovered_state) {
        (Some(snapshot_state), Some(recovered_state))
            if recovered_state.completed_iterations >= snapshot_state.completed_iterations =>
        {
            Some(recovered_state)
        }
        (Some(snapshot_state), Some(_)) => Some(snapshot_state),
        (Some(snapshot_state), None) => Some(snapshot_state),
        (None, Some(recovered_state)) => Some(recovered_state),
        (None, None) => None,
    };
    if let Some(state) = iterative_state.as_mut() {
        if state.loop_policy.is_none() {
            state.loop_policy = snapshot_loop_policy;
        }
        if state.stage_target.is_none() {
            state.stage_target = snapshot_stage_target;
        }
    }

    Ok((
        interrupted.qa_iterations_current_cycle,
        interrupted.review_iterations_current_cycle,
        iterative_state,
    ))
}

fn iterative_state_matches_cursor<'a>(
    active_run: &'a ActiveRun,
    cursor: &StageCursor,
) -> Option<&'a IterativeImplementerState> {
    (iterative_state_carries_into_cursor(&active_run.stage_cursor, cursor))
        .then_some(active_run.iterative_implementer_state.as_ref())
        .flatten()
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
    let result = execute_run_with_retry_internal(
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
        None,
        preset,
        effective_config,
        &RetryPolicy::default_policy()
            .with_max_remediation_cycles(effective_config.run_policy().max_review_iterations),
        CancellationToken::new(),
    )
    .await;
    let _ = FileSystem::remove_pid_file(base_dir, project_id);
    result.map(|_| ())
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
    writer_owner: Option<&str>,
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
    execute_run_with_retry_and_capture_run_id(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        base_dir,
        execution_cwd,
        project_id,
        writer_owner,
        preset,
        effective_config,
        retry_policy,
        cancellation_token,
    )
    .await
    .map(|_| ())
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_run_with_retry_and_capture_run_id<A, R, S>(
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
    writer_owner: Option<&str>,
    preset: FlowPreset,
    effective_config: &EffectiveConfig,
    retry_policy: &RetryPolicy,
    cancellation_token: CancellationToken,
) -> AppResult<String>
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
        writer_owner,
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
    writer_owner: Option<&str>,
    preset: FlowPreset,
    effective_config: &EffectiveConfig,
    retry_policy: &RetryPolicy,
    cancellation_token: CancellationToken,
) -> AppResult<String>
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
    let effective_max_rounds = std::env::var("RALPH_BURNING_TEST_MAX_COMPLETION_ROUNDS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(effective_config.run_policy().max_completion_rounds);
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
            None,
        )),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: snapshot.cycle_history.clone(),
        completion_rounds: 1,
        max_completion_rounds: Some(effective_max_rounds),
        rollback_point_meta: snapshot.rollback_point_meta.clone(),
        amendment_queue: snapshot.amendment_queue.clone(),
        status_summary: format!("running: {}", first_stage.display_name()),
        last_stage_resolution_snapshot: None,
    };
    let pid_owner = if execution_cwd.is_none() {
        RunPidOwner::Cli
    } else {
        RunPidOwner::Daemon
    };
    if let Err(error) = FileSystem::write_pid_file(
        base_dir,
        project_id,
        pid_owner,
        writer_owner,
        Some(run_id.as_str()),
        Some(now),
    ) {
        return Err(AppError::RunStartFailed {
            reason: format!("failed to persist run pid file: {error}"),
        });
    }
    if let Err(error) =
        run_snapshot_write.write_run_snapshot(base_dir, project_id, &current_snapshot)
    {
        let _ = FileSystem::remove_pid_file(base_dir, project_id);
        return Err(error);
    }

    seq += 1;
    let run_started =
        journal::run_started_event(seq, now, &run_id, first_stage, effective_max_rounds);
    let run_started_line = journal::serialize_event(&run_started)?;
    if let Err(error) = journal_store.append_event(base_dir, project_id, &run_started_line) {
        seq -= 1;
        return fail_run_result(
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

    if let Err(error) =
        sync_milestone_bead_start(&project_record, base_dir, project_id, &run_id, now)
    {
        return fail_run_result(
            &AppError::RunStartFailed {
                reason: format!("failed to sync milestone bead start: {error}"),
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

    let _ = log_write.append_runtime_log(
        base_dir,
        project_id,
        &RuntimeLogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Info,
            source: "engine".to_owned(),
            message: format!("run started: max_completion_rounds={effective_max_rounds}"),
        },
    );

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
        preset,
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

    Ok(run_id.as_str().to_owned())
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
    let result = execute_run_with_retry(
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
        None,
        FlowPreset::Standard,
        effective_config,
        retry_policy,
        cancellation_token,
    )
    .await;
    let _ = FileSystem::remove_pid_file(base_dir, project_id);
    result
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
    let result = resume_run_with_retry_internal(
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
        None,
        preset,
        effective_config,
        &RetryPolicy::default_policy()
            .with_max_remediation_cycles(effective_config.run_policy().max_review_iterations),
        CancellationToken::new(),
    )
    .await;
    let _ = FileSystem::remove_pid_file(base_dir, project_id);
    result.map(|_| ())
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
    writer_owner: Option<&str>,
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
    resume_run_with_retry_and_capture_run_id(
        agent_service,
        run_snapshot_read,
        run_snapshot_write,
        journal_store,
        artifact_store,
        artifact_write,
        log_write,
        amendment_queue_port,
        base_dir,
        execution_cwd,
        project_id,
        writer_owner,
        preset,
        effective_config,
        retry_policy,
        cancellation_token,
    )
    .await
    .map(|_| ())
}

#[allow(clippy::too_many_arguments)]
pub async fn resume_run_with_retry_and_capture_run_id<A, R, S>(
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
    writer_owner: Option<&str>,
    preset: FlowPreset,
    effective_config: &EffectiveConfig,
    retry_policy: &RetryPolicy,
    cancellation_token: CancellationToken,
) -> AppResult<String>
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
        writer_owner,
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
    writer_owner: Option<&str>,
    preset: FlowPreset,
    effective_config: &EffectiveConfig,
    retry_policy: &RetryPolicy,
    cancellation_token: CancellationToken,
) -> AppResult<String>
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
            let observed_attempt = current_running_attempt_identity(&snapshot)?;
            // The snapshot says Running, but the journal may record a terminal
            // event that the snapshot write missed (e.g. fail_run snapshot write
            // failed).  Cross-check the journal to reconcile.
            let events = journal_store.read_journal(base_dir, project_id)?;
            match queries::terminal_status_for_running_attempt(&snapshot, &events) {
                Some(RunStatus::Failed) => {
                    eprintln!(
                        "resume: snapshot shows Running but journal has run_failed — \
                         reconciling snapshot to Failed (stale snapshot from failed write)"
                    );
                    mark_running_run_interrupted(
                        InterruptedRunContext {
                            run_snapshot_read,
                            run_snapshot_write,
                            journal_store,
                            log_write,
                            base_dir,
                            project_id,
                        },
                        &observed_attempt,
                        InterruptedRunUpdate {
                            summary: "failed (reconciled from journal)",
                            log_message:
                                "reconciled stale running snapshot from durable run_failed journal event",
                            failure_class: None,
                        },
                    )?;
                    snapshot = run_snapshot_read.read_run_snapshot(base_dir, project_id)?;
                }
                Some(RunStatus::Completed) => {
                    return Err(AppError::ResumeFailed {
                        reason:
                            "project is already completed per journal; there is nothing to resume"
                                .to_owned(),
                    });
                }
                _ => {
                    if !orchestrator_process_is_alive(base_dir, project_id)? {
                        eprintln!(
                            "resume: snapshot shows Running but orchestrator pid is missing or dead — \
                             reconciling snapshot to Failed and continuing"
                        );
                        mark_running_run_interrupted(
                            InterruptedRunContext {
                                run_snapshot_read,
                                run_snapshot_write,
                                journal_store,
                                log_write,
                                base_dir,
                                project_id,
                            },
                            &observed_attempt,
                            InterruptedRunUpdate {
                                summary: "failed (stale running snapshot recovered for resume)",
                                log_message:
                                    "reconciled stale running snapshot because orchestrator process was not alive",
                                failure_class: Some("interruption"),
                            },
                        )?;
                        snapshot = run_snapshot_read.read_run_snapshot(base_dir, project_id)?;
                    } else {
                        return Err(AppError::ResumeFailed {
                            reason: "project already has a running run; `run resume` only works from failed or paused snapshots".to_owned(),
                        });
                    }
                }
            }
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
    let resume_run_id = run_id_for_resume(&snapshot)?;
    let resume_events = events_for_run(&visible_events, &resume_run_id);
    let stage_ids = stage_plan_for_resume(preset, &resume_events, &snapshot, effective_config)?;
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

    let mut resume_state = derive_resume_state(
        &resume_run_id,
        &resume_events,
        &snapshot,
        &stage_plan,
        semantics,
    )?;
    let mut execution_context = derive_resume_execution_context(
        artifact_store,
        base_dir,
        project_id,
        &resume_state.cursor,
        &resume_events,
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
                prompt_hash_at_cycle_start,
            } => (current_prompt_hash, prompt_hash_at_cycle_start),
            PromptChangeResumeDecision::Continue {
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
    let (qa_iterations_current_cycle, review_iterations_current_cycle, iterative_implementer_state) =
        resume_iteration_counters(&snapshot, &resume_state.cursor, &resume_events)?;

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
        // Track effective min when BackendExhausted members are skipped,
        // so drift_still_satisfies_requirements uses the reduced quorum.
        let mut resume_effective_min: Option<usize> = None;
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
                            // Prompt-review does NOT degrade on BackendExhausted
                            // — any unavailable validator follows normal rules.
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
                            available.len(),
                            min_reviewers,
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
                let mut resume_exhausted: usize = 0;
                for member in &panel.completers {
                    match agent_service
                        .adapter()
                        .check_availability(&member.target)
                        .await
                    {
                        Ok(()) => available.push(member.clone()),
                        Err(e) => {
                            // BackendExhausted on resume → skip for graceful
                            // degradation instead of aborting.
                            if e.failure_class()
                                .is_some_and(|fc| fc == FailureClass::BackendExhausted)
                            {
                                resume_exhausted += 1;
                                continue;
                            }
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
                let effective_min = min_completers
                    .min(panel.completers.len().saturating_sub(resume_exhausted))
                    .max(1);
                if resume_exhausted > 0 {
                    resume_effective_min = Some(effective_min);
                }
                if available.len() < effective_min {
                    return Err(AppError::ResumeDriftFailure {
                        stage_id: current_stage,
                        details: format!(
                            "available completers ({}) < effective min_completers ({}) on resume",
                            available.len(),
                            effective_min,
                        ),
                    });
                }
                panel.completers = available;
                build_completion_snapshot(current_stage, &panel.completers)
            }
            StageId::FinalReview => {
                match resolve_runtime_final_review_panel(
                    agent_service,
                    effective_config,
                    resume_state.cursor.cycle,
                    cancellation_token.clone(),
                )
                .await
                {
                    Ok(runtime_panel) => {
                        if runtime_panel.probe_exhausted_reviewers > 0 {
                            resume_effective_min = Some(runtime_panel.effective_min_reviewers);
                        }
                        build_final_review_snapshot(
                            current_stage,
                            &runtime_panel.panel.reviewers,
                            &runtime_panel.panel.arbiter,
                        )
                    }
                    Err(error)
                        if final_review::is_final_review_availability_retry_exhaustion_error(
                            &error,
                        ) =>
                    {
                        return Err(error);
                    }
                    Err(error) => {
                        return Err(AppError::ResumeDriftFailure {
                            stage_id: current_stage,
                            details: error.to_string(),
                        });
                    }
                }
            }
            _ => {
                let target =
                    policy.resolve_stage_target(current_stage, resume_state.cursor.cycle)?;
                let target = resolved_target_for_stage_attempt(
                    preset,
                    current_stage,
                    &target,
                    iterative_implementer_state.as_ref(),
                )?;
                build_single_target_snapshot(current_stage, &target)
            }
        };

        if resolution_has_drifted(&old_snapshot, &new_snapshot) {
            // Fail early if requirements no longer met.
            drift_still_satisfies_requirements(
                &new_snapshot,
                current_stage,
                effective_config,
                resume_effective_min,
            )?;
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
        }
    }

    let mut preflight_plan = stage_plan[resume_state.stage_index..].to_vec();
    if preset == FlowPreset::IterativeMinimal
        && resume_state.cursor.stage == StageId::PlanAndImplement
        && !preflight_plan.is_empty()
    {
        preflight_plan[0].target = resolved_target_for_stage_attempt(
            preset,
            preflight_plan[0].stage_id,
            &preflight_plan[0].target,
            iterative_implementer_state.as_ref(),
        )?;
    }
    let preflight_start_index = if preset == FlowPreset::IterativeMinimal
        && resume_state.cursor.stage == StageId::PlanAndImplement
        && resume_state.stage_index < stage_plan.len()
        && iterative_resume_skips_current_stage_preflight(
            &project_root,
            &preflight_plan[0].target,
            &resume_state.run_id,
            &stage_plan[resume_state.stage_index],
            &resume_state.cursor,
            iterative_implementer_state.as_ref(),
            effective_config,
        )? {
        resume_state.stage_index + 1
    } else {
        resume_state.stage_index
    };
    if preflight_start_index < stage_plan.len() {
        preflight_check(
            agent_service.adapter(),
            effective_config,
            resume_state.cursor.cycle,
            if preflight_start_index == resume_state.stage_index {
                &preflight_plan
            } else {
                &stage_plan[preflight_start_index..]
            },
        )
        .await
        .map_err(|error| AppError::ResumeFailed {
            reason: error.to_string(),
        })?;
    }

    // Seed the resumed ActiveRun with the (potentially updated) resolution
    // snapshot from drift detection so the stage can compare against it later.
    let resumed_snapshot = snapshot.last_stage_resolution_snapshot.clone();
    let final_review_restart_count = resume_final_review_restart_count(&snapshot, &resume_events)?;
    let resumed_at = Utc::now();
    snapshot.status = RunStatus::Running;
    snapshot.active_run = Some(build_active_run(
        &resume_state.run_id,
        resume_state.cursor.clone(),
        resumed_at,
        prompt_hash_at_cycle_start,
        current_prompt_hash.clone(),
        qa_iterations_current_cycle,
        review_iterations_current_cycle,
        final_review_restart_count,
        iterative_implementer_state,
        resumed_snapshot,
    ));
    snapshot.interrupted_run = None;
    snapshot.completion_rounds = snapshot
        .completion_rounds
        .max(resume_state.cursor.completion_round);
    snapshot.status_summary = stage_running_summary_for_active_run(
        resume_state.cursor.stage,
        snapshot.active_run.as_ref(),
        effective_config
            .run_policy()
            .iterative_minimal
            .max_consecutive_implementer_rounds,
    );
    snapshot.max_completion_rounds = Some(
        std::env::var("RALPH_BURNING_TEST_MAX_COMPLETION_ROUNDS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(effective_config.run_policy().max_completion_rounds),
    );
    let pid_owner = if execution_cwd.is_none() {
        RunPidOwner::Cli
    } else {
        RunPidOwner::Daemon
    };
    if let Err(error) = FileSystem::write_pid_file(
        base_dir,
        project_id,
        pid_owner,
        writer_owner,
        Some(resume_state.run_id.as_str()),
        Some(resumed_at),
    ) {
        return Err(AppError::ResumeFailed {
            reason: format!("failed to persist run pid file: {error}"),
        });
    }
    if let Err(error) = run_snapshot_write.write_run_snapshot(base_dir, project_id, &snapshot) {
        let _ = FileSystem::remove_pid_file(base_dir, project_id);
        return Err(error);
    }

    seq += 1;
    let run_resumed = journal::run_resumed_event(
        seq,
        resumed_at,
        &resume_state.run_id,
        resume_state.cursor.stage,
        resume_state.cursor.cycle,
        resume_state.cursor.completion_round,
        snapshot.max_completion_rounds.unwrap_or(0),
    );
    let run_resumed_line = journal::serialize_event(&run_resumed)?;
    if let Err(error) = journal_store.append_event(base_dir, project_id, &run_resumed_line) {
        seq -= 1;
        return fail_run_result(
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

    if let Err(error) = sync_milestone_bead_start(
        &project_record,
        base_dir,
        project_id,
        &resume_state.run_id,
        resumed_at,
    ) {
        return fail_run_result(
            &AppError::ResumeFailed {
                reason: format!("failed to sync milestone bead start: {error}"),
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

    let _ = log_write.append_runtime_log(
        base_dir,
        project_id,
        &RuntimeLogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Info,
            source: "engine".to_owned(),
            message: format!(
                "run resumed: max_completion_rounds={}",
                snapshot.max_completion_rounds.unwrap_or(0)
            ),
        },
    );

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
        preset,
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

    Ok(resume_run_id.as_str().to_owned())
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
    let result = resume_run_with_retry(
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
        None,
        FlowPreset::Standard,
        effective_config,
        retry_policy,
        cancellation_token,
    )
    .await;
    let _ = FileSystem::remove_pid_file(base_dir, project_id);
    result
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
    preset: FlowPreset,
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
                        .unwrap_or(effective_config.run_policy().max_completion_rounds);
                    snapshot.max_completion_rounds = Some(max_rounds);
                    if next_cursor.completion_round > max_rounds {
                        let _ = log_write.append_runtime_log(
                            base_dir,
                            project_id,
                            &RuntimeLogEntry {
                                timestamp: Utc::now(),
                                level: LogLevel::Error,
                                source: "engine".to_owned(),
                                message: format!(
                                    "max completion rounds exceeded: {}/{}",
                                    next_cursor.completion_round, max_rounds
                                ),
                            },
                        );
                        return fail_run_result(
                            &AppError::StageCommitFailed {
                                stage_id,
                                details: format!(
                                    "max completion rounds exceeded: {}/{}",
                                    next_cursor.completion_round, max_rounds
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
                        snapshot.max_completion_rounds.unwrap_or(0),
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
                                "completion round advanced: {} -> {} (max={})",
                                from_round,
                                to_round,
                                snapshot.max_completion_rounds.unwrap_or(0)
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

                    // Record planned-elsewhere amendments as mappings AFTER the
                    // stage commit succeeds, so a later failure does not leave
                    // orphaned mappings for an uncommitted stage.
                    // Always called (even with no PE amendments) so that a PE
                    // round sentinel is written for correct round supersession.
                    let (pe_amendments, _) = partition_final_review_amendments_by_route(
                        &commit_data.accepted_amendments,
                    );
                    record_planned_elsewhere_amendments(
                        log_write,
                        base_dir,
                        project_id,
                        &pe_amendments,
                        run_id,
                        cursor.completion_round,
                    );

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
                Ok(FinalReviewPanelOutcome::Restart(next_cursor, mut commit_data)) => {
                    let max_rounds = std::env::var("RALPH_BURNING_TEST_MAX_COMPLETION_ROUNDS")
                        .ok()
                        .and_then(|value| value.parse::<u32>().ok())
                        .unwrap_or(effective_config.run_policy().max_completion_rounds);
                    snapshot.max_completion_rounds = Some(max_rounds);
                    if next_cursor.completion_round > max_rounds {
                        let force_complete = deferred_final_review_amendments(
                            cursor.completion_round,
                            &commit_data.accepted_amendments,
                        );
                        mark_final_review_aggregate_force_completed(
                            &mut commit_data,
                            Some(&force_complete),
                        );
                        let _ = log_write.append_runtime_log(
                            base_dir,
                            project_id,
                            &RuntimeLogEntry {
                                timestamp: Utc::now(),
                                level: LogLevel::Warn,
                                source: "engine".to_owned(),
                                message: force_complete.status_message(),
                            },
                        );

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

                        if !force_complete.is_empty() {
                            *seq += 1;
                            let deferred_event = journal::force_complete_amendments_deferred_event(
                                *seq,
                                Utc::now(),
                                run_id,
                                force_complete.round,
                                Value::Array(force_complete.amendments.clone()),
                            );
                            let deferred_line = journal::serialize_event(&deferred_event)?;
                            if let Err(error) =
                                journal_store.append_event(base_dir, project_id, &deferred_line)
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
                                            "failed to persist force_complete_amendments_deferred event: {}",
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
                                        "journal append failed during final-review force-complete commit: {error}"
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

                        let (pe_amendments, _) = partition_final_review_amendments_by_route(
                            &commit_data.accepted_amendments,
                        );
                        record_planned_elsewhere_amendments(
                            log_write,
                            base_dir,
                            project_id,
                            &pe_amendments,
                            run_id,
                            cursor.completion_round,
                        );

                        snapshot.completion_rounds =
                            snapshot.completion_rounds.max(cursor.completion_round);
                        complete_run_with_force_complete_details(
                            snapshot,
                            run_snapshot_write,
                            journal_store,
                            amendment_queue_port,
                            base_dir,
                            project_id,
                            run_id,
                            seq,
                            Some(&force_complete),
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

                    // Partition: planned-elsewhere amendments are routed to
                    // the mapping handler; only regular amendments enter the queue.
                    // Recording is deferred until after the stage commit succeeds.
                    let (planned_elsewhere, regular_amendments) =
                        partition_final_review_amendments_by_route(
                            &commit_data.accepted_amendments,
                        );

                    let mut written_ids: Vec<String> = Vec::new();
                    for amendment in &regular_amendments {
                        if let Err(error) = amendment_queue_port.write_amendment(
                            base_dir,
                            project_id,
                            &amendment.queued,
                        ) {
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
                                        amendment.queued.amendment_id, error
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
                        written_ids.push(amendment.queued.amendment_id.clone());
                    }

                    let mut last_journaled_amendment_index = None;
                    for (index, amendment) in regular_amendments.iter().enumerate() {
                        *seq += 1;
                        let amendment_event = journal::amendment_queued_event(
                            *seq,
                            Utc::now(),
                            run_id,
                            &amendment.queued.amendment_id,
                            amendment.queued.source_stage,
                            &amendment.queued.body,
                            amendment.queued.source.as_str(),
                            &amendment.queued.dedup_key,
                            Some(&amendment.reviewer_sources),
                            Some(amendment.queued.classification),
                            amendment.queued.covered_by_bead_id.as_deref(),
                            amendment.queued.proposed_bead_summary.as_deref(),
                        );
                        let event_line = journal::serialize_event(&amendment_event)?;
                        if let Err(error) =
                            journal_store.append_event(base_dir, project_id, &event_line)
                        {
                            *seq -= 1;
                            let cleanup_errors: Vec<String> = regular_amendments[index..]
                                .iter()
                                .filter_map(|pending| {
                                    amendment_queue_port
                                        .remove_amendment(
                                            base_dir,
                                            project_id,
                                            &pending.queued.amendment_id,
                                        )
                                        .err()
                                        .map(|cleanup_error| {
                                            format!(
                                                "{}: {}",
                                                pending.queued.amendment_id, cleanup_error
                                            )
                                        })
                                })
                                .collect();
                            snapshot.completion_rounds = snapshot.completion_rounds.max(to_round);
                            if let Some(last_index) = last_journaled_amendment_index {
                                snapshot.amendment_queue.pending.extend(
                                    regular_amendments[..=last_index]
                                        .iter()
                                        .map(|amendment| amendment.queued.clone()),
                                );
                            } else {
                                snapshot.amendment_queue.pending.extend(
                                    regular_amendments
                                        .iter()
                                        .map(|amendment| amendment.queued.clone()),
                                );
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
                    snapshot.amendment_queue.pending.extend(
                        regular_amendments
                            .iter()
                            .map(|amendment| amendment.queued.clone()),
                    );
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
                        snapshot.max_completion_rounds.unwrap_or(0),
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

                    // Record planned-elsewhere amendments AFTER the stage commit
                    // and round-advance event succeed, so failures do not leave
                    // orphaned mappings for an uncommitted restart.
                    // Always called (even with no PE amendments) so that a PE
                    // round sentinel is written for correct round supersession.
                    record_planned_elsewhere_amendments(
                        log_write,
                        base_dir,
                        project_id,
                        &planned_elsewhere,
                        run_id,
                        from_round,
                    );

                    let _ = log_write.append_runtime_log(
                        base_dir,
                        project_id,
                        &RuntimeLogEntry {
                            timestamp: Utc::now(),
                            level: LogLevel::Info,
                            source: "engine".to_owned(),
                            message: format!(
                                "completion round advanced: {} -> {} (max={})",
                                from_round,
                                to_round,
                                snapshot.max_completion_rounds.unwrap_or(0)
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
            preset,
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
                    if semantics.late_stages.contains(&stage_id)
                        && has_restart_triggering_follow_up(&bundle.payload) =>
                {
                    // Late-stage conditional approval or request changes:
                    // Queue durable amendments, advance completion round, restart from planning.
                    let next_cursor = cursor.advance_completion_round(semantics.planning_stage)?;
                    let from_round = cursor.completion_round;
                    let to_round = next_cursor.completion_round;

                    // Safety limit: prevent infinite completion round loops.
                    let max_rounds = std::env::var("RALPH_BURNING_TEST_MAX_COMPLETION_ROUNDS")
                        .ok()
                        .and_then(|v| v.parse::<u32>().ok())
                        .unwrap_or(effective_config.run_policy().max_completion_rounds);
                    snapshot.max_completion_rounds = Some(max_rounds);
                    if next_cursor.completion_round > max_rounds {
                        let _ = log_write.append_runtime_log(
                            base_dir,
                            project_id,
                            &RuntimeLogEntry {
                                timestamp: Utc::now(),
                                level: LogLevel::Error,
                                source: "engine".to_owned(),
                                message: format!(
                                    "max completion rounds exceeded: {}/{}",
                                    next_cursor.completion_round, max_rounds
                                ),
                            },
                        );
                        return fail_run_result(
                            &AppError::StageCommitFailed {
                                stage_id,
                                details: format!(
                                    "max completion rounds exceeded: {}/{}",
                                    next_cursor.completion_round, max_rounds
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

                    let follow_ups = validation_follow_ups(&bundle.payload);

                    // Keep this observability hook for non-fix classifications.
                    // They are recorded in final-review aggregates and
                    // reconciled at terminal state, not queued for remediation.
                    if let StagePayload::Validation(ref validation) = bundle.payload {
                        let deferred: Vec<_> = validation
                            .classified_findings
                            .iter()
                            .filter(|f| !f.classification.triggers_restart())
                            .collect();
                        for finding in &deferred {
                            tracing::info!(
                                stage = %stage_id,
                                classification = %finding.classification,
                                mapped_to_bead_id = ?finding.mapped_to_bead_id,
                                "deferred finding (not queued for remediation): {}",
                                finding.body
                            );
                        }
                    }

                    let amendments = build_queued_amendments(
                        &follow_ups,
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
                            None,
                            Some(amendment.classification),
                            amendment.covered_by_bead_id.as_deref(),
                            amendment.proposed_bead_summary.as_deref(),
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
                        snapshot.max_completion_rounds.unwrap_or(0),
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

                    let _ = log_write.append_runtime_log(
                        base_dir,
                        project_id,
                        &RuntimeLogEntry {
                            timestamp: Utc::now(),
                            level: LogLevel::Info,
                            source: "engine".to_owned(),
                            message: format!(
                                "completion round advanced: {} -> {} (max={})",
                                from_round,
                                to_round,
                                snapshot.max_completion_rounds.unwrap_or(0)
                            ),
                        },
                    );

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
                ReviewOutcome::ConditionallyApproved | ReviewOutcome::RequestChanges
                    if semantics.late_stages.contains(&stage_id)
                        && has_deferred_classified_finding(&bundle.payload) =>
                {
                    tracing::info!(
                        stage = %stage_id,
                        "late-stage non-fix review classifications deferred without completion-round restart"
                    );
                }
                ReviewOutcome::ConditionallyApproved if semantics.late_stages.is_empty() => {
                    // Docs/CI flows do not enter completion rounds, but their follow-ups
                    // still need to be preserved in canonical snapshot state.
                    let follow_ups = validation_follow_ups(&bundle.payload);
                    let recorded_follow_ups = build_recorded_follow_ups(
                        &follow_ups,
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
                ReviewOutcome::ConditionallyApproved
                    if semantics.remediation_trigger_stages.contains(&stage_id)
                        && has_deferred_classified_finding(&bundle.payload)
                        && !has_restart_triggering_follow_up(&bundle.payload) =>
                {
                    tracing::info!(
                        stage = %stage_id,
                        "non-fix review classifications deferred without remediation cycle"
                    );
                }
                ReviewOutcome::ConditionallyApproved => {}
                ReviewOutcome::RequestChanges
                    if semantics.remediation_trigger_stages.contains(&stage_id)
                        && has_restart_triggering_follow_up(&bundle.payload) =>
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
                ReviewOutcome::RequestChanges
                    if semantics.remediation_trigger_stages.contains(&stage_id)
                        && has_deferred_classified_finding(&bundle.payload) =>
                {
                    tracing::info!(
                        stage = %stage_id,
                        "non-fix review classifications deferred without remediation cycle"
                    );
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

        // ── Skip ApplyFixes when there is nothing current-bead-scoped to fix ──────
        if let Some(skip_reason) = skip_next_apply_fixes_reason(
            &bundle.payload,
            stage_plan.get(stage_index + 1).map(|entry| entry.stage_id),
        ) {
            let _ = log_write.append_runtime_log(
                base_dir,
                project_id,
                &RuntimeLogEntry {
                    timestamp: Utc::now(),
                    level: LogLevel::Info,
                    source: "engine".to_owned(),
                    message: format!("skipping apply_fixes: {skip_reason}"),
                },
            );
            *seq += 1;
            let skipped_event = journal::stage_skipped_event(
                *seq,
                Utc::now(),
                run_id,
                StageId::ApplyFixes,
                cursor.cycle,
                skip_reason,
            );
            let skipped_line = journal::serialize_event(&skipped_event)?;
            if let Err(error) = journal_store.append_event(base_dir, project_id, &skipped_line) {
                *seq -= 1;
                return fail_run_result(
                    &AppError::StageCommitFailed {
                        stage_id: StageId::ApplyFixes,
                        details: format!(
                            "failed to persist stage_skipped event for apply_fixes: {error}",
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
    preset: FlowPreset,
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
        let resolved_target = match policy
            .resolve_stage_target(stage_id, cursor.cycle)
            .and_then(|target| {
                resolved_target_for_stage_attempt(
                    preset,
                    stage_id,
                    &target,
                    snapshot
                        .active_run
                        .as_ref()
                        .and_then(|active_run| iterative_state_matches_cursor(active_run, &cursor)),
                )
            }) {
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
            resolved_target.backend.family,
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
        snapshot.status_summary = stage_running_summary_for_active_run(
            stage_id,
            snapshot.active_run.as_ref(),
            effective_config
                .run_policy()
                .iterative_minimal
                .max_consecutive_implementer_rounds,
        );
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

        let result =
            if preset == FlowPreset::IterativeMinimal && stage_id == StageId::PlanAndImplement {
                execute_iterative_plan_and_implement_stage(
                    agent_service,
                    run_snapshot_write,
                    journal_store,
                    base_dir,
                    execution_cwd,
                    project_root,
                    project_id,
                    run_id,
                    seq,
                    snapshot,
                    stage_entry,
                    &cursor,
                    prompt,
                    execution_context,
                    pending_amendments,
                    cancellation_token.clone(),
                    resolved_target.clone(),
                    timeout,
                    effective_config,
                )
                .await
            } else {
                invoke_stage_on_backend(
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
                    None,
                    None,
                )
                .await
            };

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

                let will_retry = should_retry_stage_failure(
                    retry_policy,
                    failure_class,
                    &error,
                    &cursor,
                    &cancellation_token,
                );

                let error_display = error.to_string();
                let failed_invocation_id =
                    failed_invocation_id_for_stage(run_id, stage_id, &cursor, snapshot, preset);

                *seq += 1;
                let stage_failed = journal::stage_failed_event(
                    *seq,
                    Utc::now(),
                    run_id,
                    stage_id,
                    cursor.cycle,
                    cursor.attempt,
                    failure_class,
                    &error_display,
                    will_retry,
                    &failed_invocation_id,
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
                            "stage_failed: {} cycle={} attempt={} retry={} failure_class={} \
                             invocation_id={} error={:?}",
                            stage_id.as_str(),
                            cursor.cycle,
                            cursor.attempt,
                            will_retry,
                            failure_class.as_str(),
                            failed_invocation_id,
                            error_display,
                        ),
                    },
                );

                if will_retry {
                    let backoff = retry_policy.backoff_for_attempt(cursor.attempt);
                    if !backoff.is_zero() {
                        // Persist a resumable (Failed) snapshot before sleeping
                        // so that a crash or kill during the backoff window does
                        // not strand the project in Running state (which `run
                        // resume` refuses to recover).  The next loop iteration
                        // will re-set Running at the top of the stage dispatch.
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
                        snapshot.status_summary = format!(
                            "retrying {}: backoff {}s before attempt {}",
                            stage_id.display_name(),
                            backoff.as_secs(),
                            cursor.attempt + 1,
                        );
                        // The stage_failed journal event has already been
                        // durably appended above.  The snapshot MUST reach
                        // disk too — otherwise a crash during backoff leaves
                        // journal saying "failed/retrying" while run.json is
                        // still in a prior state, producing an unresumable
                        // journal/snapshot divergence.
                        //
                        // If the write fails, we cannot just propagate with `?`
                        // because that bypasses fail_run_result — leaving
                        // run.json in its old Running state on disk (the exact
                        // unresumable condition we are trying to prevent).
                        // Instead, restore active_run so fail_run_result can
                        // re-attempt the snapshot write and emit a run_failed
                        // journal event.
                        if let Err(snapshot_err) =
                            run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)
                        {
                            // Restore active_run so fail_run's
                            // preserve_interrupted_run sees valid state.
                            snapshot.active_run = snapshot.interrupted_run.take();
                            snapshot.status = RunStatus::Running;
                            let wrapper = AppError::StageCommitFailed {
                                stage_id,
                                details: format!(
                                    "pre-backoff snapshot write failed at attempt {}: {}",
                                    cursor.attempt, snapshot_err,
                                ),
                            };
                            return fail_run_result(
                                &wrapper,
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
                                    "retry_backoff: {} attempt={} delay={}s",
                                    stage_id.as_str(),
                                    cursor.attempt,
                                    backoff.as_secs(),
                                ),
                            },
                        );
                        tokio::select! {
                            () = tokio::time::sleep(backoff) => {}
                            () = cancellation_token.cancelled() => {}
                        }

                        // Restore the active_run that was saved into
                        // interrupted_run before sleep.  Without this,
                        // carry_forward_active_run at the top of the next
                        // loop iteration would hit CorruptRecord because
                        // active_run is None.
                        snapshot.active_run = snapshot.interrupted_run.take();
                        snapshot.status = RunStatus::Running;
                    }
                    if cancellation_token.is_cancelled() {
                        let cancellation_error = AppError::InvocationCancelled {
                            backend: "engine".to_owned(),
                            contract_id: stage_id.as_str().to_owned(),
                        };
                        return fail_run_result(
                            &cancellation_error,
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

fn invocation_id_for_stage(
    run_id: &RunId,
    stage_id: StageId,
    cursor: &StageCursor,
    suffix: Option<&str>,
) -> String {
    let base = history_record_base_id(run_id, stage_id, cursor, 0);
    match suffix {
        Some(suffix) => format!("{base}-{suffix}"),
        None => base,
    }
}

fn failed_invocation_id_for_stage(
    run_id: &RunId,
    stage_id: StageId,
    cursor: &StageCursor,
    snapshot: &RunSnapshot,
    preset: FlowPreset,
) -> String {
    if preset == FlowPreset::IterativeMinimal && stage_id == StageId::PlanAndImplement {
        let next_iteration = snapshot
            .active_run
            .as_ref()
            .filter(|active_run| {
                active_run.stage_cursor.stage == cursor.stage
                    && active_run.stage_cursor.cycle == cursor.cycle
                    && active_run.stage_cursor.attempt == cursor.attempt
                    && active_run.stage_cursor.completion_round == cursor.completion_round
            })
            .and_then(|active_run| active_run.iterative_implementer_state.as_ref())
            .map(|state| state.completed_iterations.saturating_add(1))
            .unwrap_or(1);
        let suffix = format!("it{next_iteration}");
        return invocation_id_for_stage(run_id, stage_id, cursor, Some(&suffix));
    }

    history_record_base_id(run_id, stage_id, cursor, 0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IterativeLoopExitReason {
    Stable,
    MaxRounds,
}

impl IterativeLoopExitReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stable => "stable",
            Self::MaxRounds => "max_rounds",
        }
    }
}

fn iterative_loop_exit_reason(
    stable_count: u32,
    iteration: u32,
    stable_rounds_required: u32,
    max_rounds: u32,
) -> Option<IterativeLoopExitReason> {
    if stable_count >= stable_rounds_required {
        return Some(IterativeLoopExitReason::Stable);
    }

    if iteration >= max_rounds {
        return Some(IterativeLoopExitReason::MaxRounds);
    }

    None
}

fn advance_iterative_loop_state(
    stable_count: u32,
    iteration: u32,
    diff_changed: bool,
    stable_rounds_required: u32,
    max_rounds: u32,
) -> (u32, Option<IterativeLoopExitReason>) {
    let next_stable_count = if diff_changed {
        0
    } else {
        stable_count.saturating_add(1)
    };

    (
        next_stable_count,
        iterative_loop_exit_reason(
            next_stable_count,
            iteration,
            stable_rounds_required,
            max_rounds,
        ),
    )
}

fn stage_running_summary(stage_id: StageId, iteration: Option<u32>, max_rounds: u32) -> String {
    match iteration {
        Some(iteration) if stage_id == StageId::PlanAndImplement => format!(
            "running: {} (iteration {iteration}/{max_rounds})",
            stage_id.display_name()
        ),
        _ => format!("running: {}", stage_id.display_name()),
    }
}

fn iterative_iteration_summary_state(
    stage_id: StageId,
    active_run: Option<&ActiveRun>,
) -> Option<&IterativeImplementerState> {
    active_run.and_then(|active_run| {
        (stage_id == StageId::PlanAndImplement
            && active_run.stage_cursor.stage == StageId::PlanAndImplement)
            .then_some(active_run.iterative_implementer_state.as_ref())
            .flatten()
    })
}

fn stage_running_summary_for_active_run(
    stage_id: StageId,
    active_run: Option<&ActiveRun>,
    default_max_rounds: u32,
) -> String {
    let iterative_state = iterative_iteration_summary_state(stage_id, active_run);
    let iteration = iterative_state
        .and_then(|state| (state.completed_iterations > 0).then_some(state.completed_iterations));
    let max_rounds = iterative_state
        .and_then(|state| state.loop_policy.as_ref())
        .map(|policy| policy.max_consecutive_implementer_rounds)
        .unwrap_or(default_max_rounds);
    stage_running_summary(stage_id, iteration, max_rounds)
}

fn iterative_iteration_outcome(bundle: &ValidatedBundle) -> String {
    serde_json::to_value(&bundle.payload)
        .ok()
        .and_then(|payload| {
            payload
                .get("outcome")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "completed".to_owned())
}

fn validate_iterative_minimal_loop_setting(key: &str, value: u32) -> AppResult<()> {
    if value == 0 {
        return Err(AppError::InvalidConfigValue {
            key: key.to_owned(),
            value: value.to_string(),
            reason: "expected a positive integer".to_owned(),
        });
    }

    Ok(())
}

fn validate_iterative_minimal_loop_settings(
    max_rounds: u32,
    stable_rounds_required: u32,
) -> AppResult<()> {
    validate_iterative_minimal_loop_setting(
        "workflow.iterative_minimal.max_consecutive_implementer_rounds",
        max_rounds,
    )?;
    validate_iterative_minimal_loop_setting(
        "workflow.iterative_minimal.stable_rounds_required",
        stable_rounds_required,
    )?;
    if stable_rounds_required > max_rounds {
        return Err(AppError::InvalidConfigValue {
            key: "workflow.iterative_minimal.stable_rounds_required".to_owned(),
            value: stable_rounds_required.to_string(),
            reason: format!(
                "must be less than or equal to workflow.iterative_minimal.max_consecutive_implementer_rounds ({max_rounds})"
            ),
        });
    }

    Ok(())
}

fn iterative_loop_policy(
    max_rounds: u32,
    stable_rounds_required: u32,
) -> IterativeImplementerLoopPolicy {
    IterativeImplementerLoopPolicy {
        max_consecutive_implementer_rounds: max_rounds,
        stable_rounds_required,
    }
}

fn iterative_loop_policy_for_attempt(
    state: Option<&IterativeImplementerState>,
    effective_config: &EffectiveConfig,
) -> AppResult<IterativeImplementerLoopPolicy> {
    let policy = state
        .and_then(|state| state.loop_policy.clone())
        .unwrap_or_else(|| {
            iterative_loop_policy(
                effective_config
                    .run_policy()
                    .iterative_minimal
                    .max_consecutive_implementer_rounds,
                effective_config
                    .run_policy()
                    .iterative_minimal
                    .stable_rounds_required,
            )
        });
    validate_iterative_minimal_loop_settings(
        policy.max_consecutive_implementer_rounds,
        policy.stable_rounds_required,
    )?;
    Ok(policy)
}

fn resolved_target_for_stage_attempt(
    preset: FlowPreset,
    stage_id: StageId,
    fallback_target: &ResolvedBackendTarget,
    iterative_state: Option<&IterativeImplementerState>,
) -> AppResult<ResolvedBackendTarget> {
    if preset == FlowPreset::IterativeMinimal && stage_id == StageId::PlanAndImplement {
        return iterative_resume_target(iterative_state, fallback_target, stage_id);
    }

    Ok(fallback_target.clone())
}

fn iterative_resume_target(
    state: Option<&IterativeImplementerState>,
    fallback_target: &ResolvedBackendTarget,
    stage_id: StageId,
) -> AppResult<ResolvedBackendTarget> {
    let Some(target) = state.and_then(|state| state.stage_target.as_ref()) else {
        return Ok(fallback_target.clone());
    };
    let backend_family = target
        .backend_family
        .parse::<BackendFamily>()
        .map_err(|error| AppError::StageCommitFailed {
            stage_id,
            details: format!(
                "persisted iterative_minimal target backend `{}` is invalid during resume recovery: {error}",
                target.backend_family
            ),
        })?;
    Ok(ResolvedBackendTarget::new(
        backend_family,
        target.model_id.clone(),
    ))
}

fn event_matches_run(event: &JournalEvent, run_id: &RunId) -> bool {
    event.details.get("run_id").and_then(Value::as_str) == Some(run_id.as_str())
}

fn iterative_loop_exit_recorded(
    events: &[JournalEvent],
    run_id: &RunId,
    cursor: &StageCursor,
    reason: IterativeLoopExitReason,
    total_iterations: u32,
) -> AppResult<bool> {
    for event in events {
        if event.event_type == JournalEventType::ImplementerLoopExited
            && event_matches_run(event, run_id)
            && event_matches_stage_cursor(event, cursor)?
            && event_matches_stage_attempt(event, cursor)?
            && event.details.get("reason").and_then(Value::as_str) == Some(reason.as_str())
            && event_detail_u32(event, "total_iterations")? == total_iterations
        {
            return Ok(true);
        }
    }

    Ok(false)
}

#[allow(clippy::too_many_arguments)]
fn append_implementer_loop_exited_event(
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    stage_id: StageId,
    cursor: &StageCursor,
    reason: IterativeLoopExitReason,
    total_iterations: u32,
) -> AppResult<()> {
    *seq += 1;
    let exited = journal::implementer_loop_exited_event(
        *seq,
        Utc::now(),
        run_id,
        stage_id,
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round,
        reason.as_str(),
        total_iterations,
    );
    let exited_line = journal::serialize_event(&exited)?;
    if let Err(error) = journal_store.append_event(base_dir, project_id, &exited_line) {
        *seq -= 1;
        return Err(AppError::StageCommitFailed {
            stage_id,
            details: format!(
                "failed to persist implementer_loop_exited event for iteration {total_iterations}: {error}"
            ),
        });
    }

    Ok(())
}

fn recovered_iterative_record_producer(resolved_target: &ResolvedBackendTarget) -> RecordProducer {
    RecordProducer::Agent {
        requested_backend_family: resolved_target.backend.family.to_string(),
        requested_model_id: resolved_target.model.model_id.clone(),
        actual_backend_family: resolved_target.backend.family.to_string(),
        actual_model_id: resolved_target.model.model_id.clone(),
    }
}

fn iterative_backend_raw_output_path(project_root: &Path, invocation_id: &str) -> PathBuf {
    project_root
        .join("runtime/backend")
        .join(format!("{invocation_id}.raw"))
}

fn iterative_backend_parsed_output_path(project_root: &Path, invocation_id: &str) -> PathBuf {
    project_root
        .join("runtime/backend")
        .join(format!("{invocation_id}.parsed.json"))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IterativeInvocationSidecar {
    parsed_payload: Value,
    producer: RecordProducer,
}

fn persist_invocation_parsed_payload(
    project_root: &Path,
    invocation_id: &str,
    stage_id: StageId,
    parsed_payload: &Value,
    producer: &RecordProducer,
) -> AppResult<()> {
    let parsed_output_path = iterative_backend_parsed_output_path(project_root, invocation_id);
    let serialized = serde_json::to_string_pretty(&IterativeInvocationSidecar {
        parsed_payload: parsed_payload.clone(),
        producer: producer.clone(),
    })
    .map_err(|error| {
        AppError::StageCommitFailed {
            stage_id,
            details: format!(
                "failed to serialize iterative parsed payload sidecar for invocation {invocation_id}: {error}"
            ),
        }
    })?;
    FileSystem::write_atomic(&parsed_output_path, &serialized).map_err(|error| {
        AppError::StageCommitFailed {
            stage_id,
            details: format!(
                "failed to persist iterative parsed payload sidecar for invocation {invocation_id} at {}: {error}",
                parsed_output_path.display()
            ),
        }
    })
}

fn persist_invocation_parsed_payload_best_effort(
    project_root: &Path,
    invocation_id: &str,
    stage_id: StageId,
    parsed_payload: &Value,
    producer: &RecordProducer,
) {
    if let Err(first_error) = persist_invocation_parsed_payload(
        project_root,
        invocation_id,
        stage_id,
        parsed_payload,
        producer,
    ) {
        tracing::warn!(
            stage = %stage_id,
            invocation_id,
            error = %first_error,
            "failed to persist iterative parsed payload sidecar; retrying once and continuing with in-memory result"
        );
        if let Err(retry_error) = persist_invocation_parsed_payload(
            project_root,
            invocation_id,
            stage_id,
            parsed_payload,
            producer,
        ) {
            tracing::warn!(
                stage = %stage_id,
                invocation_id,
                error = %retry_error,
                "failed to persist iterative parsed payload sidecar on retry; resume may need to re-invoke this iteration"
            );
        }
    }
}

enum IterativeIterationRecoveryFailure {
    SidecarUnavailable { details: String },
    InvalidPayload(AppError),
}

fn recovered_iterative_bundle_from_payload(
    stage_entry: &StagePlan,
    parsed_payload: &Value,
    iteration: u32,
    details_context: &str,
) -> Result<ValidatedBundle, IterativeIterationRecoveryFailure> {
    stage_entry
        .contract
        .evaluate_permissive(parsed_payload)
        .map_err(|error| {
            IterativeIterationRecoveryFailure::InvalidPayload(AppError::StageCommitFailed {
                stage_id: stage_entry.stage_id,
                details: format!(
                    "recovered iterative_minimal parsed payload for iteration {iteration} did not satisfy the stage contract: {error}; inspect {details_context}"
                ),
            })
        })
}

fn recover_iterative_payload_from_raw_output(
    raw_output_path: &Path,
    stage_entry: &StagePlan,
    resolved_target: &ResolvedBackendTarget,
    iteration: u32,
    sidecar_error: &str,
) -> Result<(ValidatedBundle, RecordProducer), IterativeIterationRecoveryFailure> {
    let raw_output = fs::read_to_string(raw_output_path).map_err(|error| {
        IterativeIterationRecoveryFailure::SidecarUnavailable {
            details: format!(
                "{sidecar_error}; failed to recover iterative_minimal raw output for iteration {iteration} from {}: {error}",
                raw_output_path.display()
            ),
        }
    })?;
    let backend_family = resolved_target.backend.family;
    let is_codex_family = matches!(
        backend_family,
        BackendFamily::Codex | BackendFamily::OpenRouter
    );
    let is_execution_contract = matches!(
        stage_entry.contract.family,
        contracts::ContractFamily::Execution
    );
    // Codex Execution stages run without `--output-schema` (issue #188 fix)
    // and store the model's natural-language last message rather than a
    // JSON payload in the raw transcript envelope. The generic
    // `recover_codex_structured_payload_from_stdout` cannot deserialize
    // that text — synthesize the same `ExecutionPayload` instead.
    //
    // Use `is_codex_raw_transcript_envelope` to disambiguate the codex CLI
    // path (envelope wrapper) from the direct `OpenRouterBackendAdapter`
    // path (chat-completions HTTP response body): both report
    // `BackendFamily::OpenRouter`, but their raw-output shapes are
    // completely different and only the codex envelope is safe to feed to
    // the synth recovery. A non-envelope OpenRouter raw output falls
    // through to `recover_structured_payload_from_response_body`.
    let parsed_payload = if is_codex_family
        && is_execution_contract
        && is_codex_raw_transcript_envelope(&raw_output)
    {
        recover_codex_execution_payload_from_raw_transcript(&raw_output)
    } else {
        match backend_family {
            BackendFamily::OpenRouter => {
                recover_structured_payload_from_response_body(&raw_output)
            }
            backend_family => {
                recover_structured_payload_from_process_stdout(&raw_output, backend_family)
            }
        }
    }
    .map_err(|error| {
        IterativeIterationRecoveryFailure::SidecarUnavailable {
            details: format!(
                "{sidecar_error}; failed to recover iterative_minimal parsed payload for iteration {iteration} from raw transcript {}: {error}",
                raw_output_path.display()
            ),
        }
    })?;
    let details_context = raw_output_path.display().to_string();
    let bundle = recovered_iterative_bundle_from_payload(
        stage_entry,
        &parsed_payload,
        iteration,
        details_context.as_str(),
    )?;
    Ok((bundle, recovered_iterative_record_producer(resolved_target)))
}

fn recover_iterative_iteration_result(
    project_root: &Path,
    run_id: &RunId,
    stage_entry: &StagePlan,
    cursor: &StageCursor,
    recovery_target: &ResolvedBackendTarget,
    iteration: u32,
) -> Result<(ValidatedBundle, RecordProducer), IterativeIterationRecoveryFailure> {
    let invocation_id = invocation_id_for_stage(
        run_id,
        stage_entry.stage_id,
        cursor,
        Some(&format!("it{iteration}")),
    );
    let raw_output_path = iterative_backend_raw_output_path(project_root, &invocation_id);
    let parsed_output_path = iterative_backend_parsed_output_path(project_root, &invocation_id);
    let parsed_sidecar = match fs::read_to_string(&parsed_output_path) {
        Ok(contents) => contents,
        Err(error) => {
            let sidecar_error = format!(
                "failed to recover iterative_minimal parsed payload for iteration {iteration} from {}: {error}",
                parsed_output_path.display()
            );
            return recover_iterative_payload_from_raw_output(
                &raw_output_path,
                stage_entry,
                recovery_target,
                iteration,
                &sidecar_error,
            );
        }
    };
    let parsed_sidecar = match serde_json::from_str::<Value>(&parsed_sidecar) {
        Ok(sidecar) => sidecar,
        Err(error) => {
            let sidecar_error = format!(
                "failed to parse recovered iterative_minimal parsed payload sidecar for iteration {iteration} from {}: {error}",
                parsed_output_path.display()
            );
            return recover_iterative_payload_from_raw_output(
                &raw_output_path,
                stage_entry,
                recovery_target,
                iteration,
                &sidecar_error,
            );
        }
    };
    let (parsed_payload, producer) = match parsed_sidecar {
        Value::Object(mut object)
            if object.contains_key("parsed_payload") || object.contains_key("producer") =>
        {
            let parsed_payload = match object.remove("parsed_payload") {
                Some(parsed_payload) => parsed_payload,
                None => {
                    let sidecar_error = format!(
                        "recovered iterative_minimal parsed payload sidecar for iteration {iteration} at {} is missing `parsed_payload`",
                        parsed_output_path.display()
                    );
                    return recover_iterative_payload_from_raw_output(
                        &raw_output_path,
                        stage_entry,
                        recovery_target,
                        iteration,
                        &sidecar_error,
                    );
                }
            };
            let producer = match object.remove("producer") {
                Some(producer) => producer,
                None => {
                    let sidecar_error = format!(
                        "recovered iterative_minimal parsed payload sidecar for iteration {iteration} at {} is missing producer metadata",
                        parsed_output_path.display()
                    );
                    return recover_iterative_payload_from_raw_output(
                        &raw_output_path,
                        stage_entry,
                        recovery_target,
                        iteration,
                        &sidecar_error,
                    );
                }
            };
            let producer = match serde_json::from_value::<RecordProducer>(producer) {
                Ok(producer) => producer,
                Err(error) => {
                    let sidecar_error = format!(
                        "failed to parse producer metadata from iterative_minimal parsed payload sidecar for iteration {iteration} at {}: {error}",
                        parsed_output_path.display()
                    );
                    return recover_iterative_payload_from_raw_output(
                        &raw_output_path,
                        stage_entry,
                        recovery_target,
                        iteration,
                        &sidecar_error,
                    );
                }
            };
            (parsed_payload, producer)
        }
        legacy_payload => (
            legacy_payload,
            recovered_iterative_record_producer(recovery_target),
        ),
    };
    let details_context = format!(
        "{} and {}",
        parsed_output_path.display(),
        raw_output_path.display()
    );
    let bundle = recovered_iterative_bundle_from_payload(
        stage_entry,
        &parsed_payload,
        iteration,
        details_context.as_str(),
    )?;

    Ok((bundle, producer))
}

fn iterative_terminal_resume_ready(
    project_root: &Path,
    resolved_target: &ResolvedBackendTarget,
    run_id: &RunId,
    stage_entry: &StagePlan,
    cursor: &StageCursor,
    state: &IterativeImplementerState,
    effective_config: &EffectiveConfig,
) -> AppResult<bool> {
    let loop_policy = iterative_loop_policy_for_attempt(Some(state), effective_config)?;
    let recovery_target =
        iterative_resume_target(Some(state), resolved_target, stage_entry.stage_id)?;
    let Some(_) = iterative_loop_exit_reason(
        state.stable_count,
        state.completed_iterations,
        loop_policy.stable_rounds_required,
        loop_policy.max_consecutive_implementer_rounds,
    ) else {
        return Ok(false);
    };

    match recover_iterative_iteration_result(
        project_root,
        run_id,
        stage_entry,
        cursor,
        &recovery_target,
        state.completed_iterations,
    ) {
        Ok(_) => Ok(true),
        Err(IterativeIterationRecoveryFailure::SidecarUnavailable { .. })
        | Err(IterativeIterationRecoveryFailure::InvalidPayload(_)) => Ok(false),
    }
}

fn iterative_resume_skips_current_stage_preflight(
    project_root: &Path,
    resolved_target: &ResolvedBackendTarget,
    run_id: &RunId,
    stage_entry: &StagePlan,
    cursor: &StageCursor,
    iterative_state: Option<&IterativeImplementerState>,
    effective_config: &EffectiveConfig,
) -> AppResult<bool> {
    let Some(state) = iterative_state else {
        return Ok(false);
    };

    iterative_terminal_resume_ready(
        project_root,
        resolved_target,
        run_id,
        stage_entry,
        cursor,
        state,
        effective_config,
    )
}

#[derive(Debug)]
enum TerminalIterativeResumeResult {
    NotTerminal,
    Recovered(Box<(ValidatedBundle, RecordProducer)>),
}

#[allow(clippy::too_many_arguments)]
fn resume_terminal_iterative_stage_result(
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    project_root: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    stage_entry: &StagePlan,
    cursor: &StageCursor,
    recovery_target: &ResolvedBackendTarget,
    completed_iterations: u32,
    stable_count: u32,
    stable_rounds_required: u32,
    max_rounds: u32,
) -> AppResult<TerminalIterativeResumeResult> {
    let Some(reason) = iterative_loop_exit_reason(
        stable_count,
        completed_iterations,
        stable_rounds_required,
        max_rounds,
    ) else {
        return Ok(TerminalIterativeResumeResult::NotTerminal);
    };

    let events = journal_store.read_journal(base_dir, project_id)?;
    let recovered = match recover_iterative_iteration_result(
        project_root,
        run_id,
        stage_entry,
        cursor,
        recovery_target,
        completed_iterations,
    ) {
        Ok(recovered) => recovered,
        Err(IterativeIterationRecoveryFailure::SidecarUnavailable { details }) => {
            return Err(AppError::StageCommitFailed {
                stage_id: stage_entry.stage_id,
                details: format!(
                    "unable to recover terminal iterative_minimal iteration {completed_iterations} safely during resume; refusing to re-invoke on the post-iteration workspace: {details}"
                ),
            });
        }
        Err(IterativeIterationRecoveryFailure::InvalidPayload(error)) => {
            return Err(error);
        }
    };

    if !iterative_loop_exit_recorded(&events, run_id, cursor, reason, completed_iterations)? {
        append_implementer_loop_exited_event(
            journal_store,
            base_dir,
            project_id,
            run_id,
            seq,
            stage_entry.stage_id,
            cursor,
            reason,
            completed_iterations,
        )?;
    }

    Ok(TerminalIterativeResumeResult::Recovered(Box::new(
        recovered,
    )))
}

fn iterative_fingerprint_excludes_path(relative_path: &Path) -> bool {
    let Some(first_component) = relative_path.components().next() else {
        return false;
    };
    let first_component = first_component.as_os_str().to_string_lossy();
    matches!(first_component.as_ref(), ".git" | ".ralph-burning")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IterativeFingerprintMode {
    Content,
    ChangeScope,
}

fn iterative_stability_fingerprint_mode(
    pending_amendments: Option<&[QueuedAmendment]>,
) -> IterativeFingerprintMode {
    if pending_amendments.is_some_and(|amendments| !amendments.is_empty()) {
        IterativeFingerprintMode::ChangeScope
    } else {
        IterativeFingerprintMode::Content
    }
}

fn iterative_fingerprint_git_pathspecs() -> Vec<&'static str> {
    vec![
        ".",
        ":(top,exclude).ralph-burning",
        ":(top,glob,exclude).ralph-burning/**",
        ":(top,exclude)target",
        ":(top,glob,exclude)target/**",
        ":(top,exclude)target-final",
        ":(top,glob,exclude)target-final/**",
        ":(top,exclude)result",
        ":(top,glob,exclude)result/**",
        ":(top,glob,exclude)result-*",
        ":(top,glob,exclude)result-*/**",
    ]
}

fn git_args_with_iterative_fingerprint_pathspecs<'a>(base_args: &'a [&'a str]) -> Vec<&'a str> {
    base_args
        .iter()
        .copied()
        .chain(iterative_fingerprint_git_pathspecs())
        .collect()
}

fn hash_file_contents(path: &Path, hasher: &mut Sha256) -> AppResult<()> {
    let mut file = fs::File::open(path)?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(())
}

fn hash_workspace_entries(
    workspace_root: &Path,
    current: &Path,
    hasher: &mut Sha256,
) -> AppResult<()> {
    let mut entries = fs::read_dir(current)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let relative_path = path.strip_prefix(workspace_root).unwrap_or(path.as_path());
        if iterative_fingerprint_excludes_path(relative_path) {
            continue;
        }
        let relative = relative_path.to_string_lossy();

        let metadata = fs::symlink_metadata(&path)?;
        if iterative_workspace_build_artifact(relative_path, &metadata) {
            continue;
        }
        if metadata.is_dir() {
            hasher.update(b"dir\0");
            hasher.update(relative.as_bytes());
            hash_metadata_mode(&metadata, hasher);
            hash_workspace_entries(workspace_root, &path, hasher)?;
            continue;
        }

        if metadata.file_type().is_symlink() {
            hasher.update(b"symlink\0");
            hasher.update(relative.as_bytes());
            hash_metadata_mode(&metadata, hasher);
            hasher.update(fs::read_link(&path)?.to_string_lossy().as_bytes());
            continue;
        }

        if metadata.is_file() {
            hasher.update(b"file\0");
            hasher.update(relative.as_bytes());
            hash_metadata_mode(&metadata, hasher);
            hasher.update(metadata.len().to_le_bytes());
            hash_file_contents(&path, hasher)?;
        }
    }

    Ok(())
}

fn workspace_diff_fingerprint(workspace_root: &Path) -> AppResult<String> {
    let mut hasher = Sha256::new();
    hash_workspace_entries(workspace_root, workspace_root, &mut hasher)?;
    Ok(format!("fs:{:x}", hasher.finalize()))
}

fn hash_workspace_entries_for_change_scope(
    workspace_root: &Path,
    current: &Path,
    hasher: &mut Sha256,
) -> AppResult<()> {
    let mut entries = fs::read_dir(current)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let relative_path = path.strip_prefix(workspace_root).unwrap_or(path.as_path());
        if iterative_fingerprint_excludes_path(relative_path) {
            continue;
        }
        let relative = relative_path.to_string_lossy();

        let metadata = fs::symlink_metadata(&path)?;
        if iterative_workspace_build_artifact(relative_path, &metadata) {
            continue;
        }
        if metadata.is_dir() {
            hasher.update(b"dir\0");
            hasher.update(relative.as_bytes());
            hash_metadata_mode(&metadata, hasher);
            hash_workspace_entries_for_change_scope(workspace_root, &path, hasher)?;
            continue;
        }

        if metadata.file_type().is_symlink() {
            hasher.update(b"symlink\0");
            hasher.update(relative.as_bytes());
            hash_metadata_mode(&metadata, hasher);
            continue;
        }

        if metadata.is_file() {
            hasher.update(b"file\0");
            hasher.update(relative.as_bytes());
            hash_metadata_mode(&metadata, hasher);
        }
    }

    Ok(())
}

fn workspace_change_scope_fingerprint(workspace_root: &Path) -> AppResult<String> {
    let mut hasher = Sha256::new();
    hash_workspace_entries_for_change_scope(workspace_root, workspace_root, &mut hasher)?;
    Ok(format!("fs-scope:{:x}", hasher.finalize()))
}

fn git_command_for_program(program: &str) -> Command {
    let mut command = Command::new(program);
    command.env("LC_ALL", "C");
    command.env("LANG", "C");
    command.env_remove("LANGUAGE");
    command
}

fn run_git_output(repo_root: &Path, args: &[&str]) -> AppResult<std::process::Output> {
    run_git_output_with_program("git", repo_root, args)
}

fn run_git_output_with_program(
    program: &str,
    repo_root: &Path,
    args: &[&str],
) -> AppResult<std::process::Output> {
    Ok(git_command_for_program(program)
        .args(args)
        .current_dir(repo_root)
        .output()?)
}

fn git_repo_available(repo_root: &Path) -> AppResult<bool> {
    git_repo_available_with_program(repo_root, "git")
}

fn git_repo_available_with_program(repo_root: &Path, program: &str) -> AppResult<bool> {
    let output = match run_git_output_with_program(
        program,
        repo_root,
        &["rev-parse", "--is-inside-work-tree"],
    ) {
        Ok(output) => output,
        Err(AppError::Io(error)) => {
            tracing::debug!(
                repo_root = %repo_root.display(),
                error = %error,
                "git unavailable during iterative_minimal repo probe; falling back to filesystem fingerprint"
            );
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    Ok(output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == "true")
}

fn git_command_stdout(repo_root: &Path, args: &[&str], description: &str) -> AppResult<Vec<u8>> {
    let output = run_git_output(repo_root, args)?;
    if output.status.success() {
        return Ok(output.stdout);
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let details = if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        format!("exit status {}", output.status)
    };

    Err(AppError::StageCommitFailed {
        stage_id: StageId::PlanAndImplement,
        details: format!(
            "git {description} for iterative_minimal diff detection failed: {details}"
        ),
    })
}

fn hash_metadata_mode(metadata: &fs::Metadata, hasher: &mut Sha256) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        hasher.update(metadata.permissions().mode().to_le_bytes());
    }

    #[cfg(not(unix))]
    {
        hasher.update([u8::from(metadata.permissions().readonly())]);
    }
}

fn hash_untracked_git_entry(
    repo_root: &Path,
    relative_path: &Path,
    hasher: &mut Sha256,
) -> AppResult<()> {
    let absolute_path = repo_root.join(relative_path);
    let metadata = fs::symlink_metadata(&absolute_path)?;
    if iterative_fingerprint_excludes_path(relative_path)
        || iterative_build_artifact_path(relative_path, &metadata)
    {
        return Ok(());
    }
    let display_path = relative_path.to_string_lossy();

    if metadata.file_type().is_symlink() {
        hasher.update(b"untracked-symlink\0");
        hasher.update(display_path.as_bytes());
        hash_metadata_mode(&metadata, hasher);
        hasher.update(fs::read_link(&absolute_path)?.to_string_lossy().as_bytes());
        return Ok(());
    }

    if metadata.is_file() {
        hasher.update(b"untracked-file\0");
        hasher.update(display_path.as_bytes());
        hash_metadata_mode(&metadata, hasher);
        hasher.update(metadata.len().to_le_bytes());
        hash_file_contents(&absolute_path, hasher)?;
        return Ok(());
    }

    if metadata.is_dir() {
        let mut nested = Sha256::new();
        hash_workspace_entries(repo_root, &absolute_path, &mut nested)?;
        hasher.update(b"untracked-dir\0");
        hasher.update(display_path.as_bytes());
        hash_metadata_mode(&metadata, hasher);
        hasher.update(nested.finalize());
    }

    Ok(())
}

fn iterative_build_artifact_path(relative_path: &Path, metadata: &fs::Metadata) -> bool {
    let Some(first_component) = relative_path.components().next() else {
        return false;
    };
    let first_component = first_component.as_os_str().to_string_lossy();
    matches!(first_component.as_ref(), "target" | "target-final")
        || ((first_component == "result" || first_component.starts_with("result-"))
            && (metadata.file_type().is_symlink() || metadata.is_dir()))
}

fn iterative_workspace_build_artifact(relative_path: &Path, metadata: &fs::Metadata) -> bool {
    iterative_build_artifact_path(relative_path, metadata)
}

fn git_head_fingerprint(repo_root: &Path) -> Vec<u8> {
    match run_git_output(repo_root, &["rev-parse", "--verify", "HEAD"]) {
        Ok(output) if output.status.success() => output.stdout,
        _ => b"unborn".to_vec(),
    }
}

fn hash_git_empty_directories(
    workspace_root: &Path,
    current: &Path,
    hasher: &mut Sha256,
) -> AppResult<()> {
    let mut entries = fs::read_dir(current)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let path = entry.path();
        let relative_path = path.strip_prefix(workspace_root).unwrap_or(path.as_path());
        if iterative_fingerprint_excludes_path(relative_path) {
            continue;
        }

        let metadata = fs::symlink_metadata(&path)?;
        if !metadata.is_dir() || iterative_build_artifact_path(relative_path, &metadata) {
            continue;
        }

        let child_count = fs::read_dir(&path)?.count();
        if child_count == 0 {
            hasher.update(b"empty-dir\0");
            hasher.update(relative_path.to_string_lossy().as_bytes());
            hash_metadata_mode(&metadata, hasher);
            continue;
        }

        hash_git_empty_directories(workspace_root, &path, hasher)?;
    }

    Ok(())
}

fn git_diff_fingerprint(repo_root: &Path) -> AppResult<String> {
    if !git_repo_available(repo_root)? {
        return workspace_diff_fingerprint(repo_root);
    }

    let staged = git_command_stdout(
        repo_root,
        &git_args_with_iterative_fingerprint_pathspecs(&[
            "diff",
            "--binary",
            "--cached",
            "--no-ext-diff",
            "--",
        ]),
        "diff --cached",
    )?;
    let unstaged = git_command_stdout(
        repo_root,
        &git_args_with_iterative_fingerprint_pathspecs(&[
            "diff",
            "--binary",
            "--no-ext-diff",
            "--",
        ]),
        "diff",
    )?;
    let untracked_listing = git_command_stdout(
        repo_root,
        &git_args_with_iterative_fingerprint_pathspecs(&[
            "ls-files",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
        ]),
        "ls-files --others",
    )?;

    let untracked_paths: Vec<PathBuf> = untracked_listing
        .split(|byte| *byte == b'\0')
        .filter(|entry| !entry.is_empty())
        .map(|entry| PathBuf::from(String::from_utf8_lossy(entry).into_owned()))
        .collect();

    let mut hasher = Sha256::new();
    hasher.update(b"head\0");
    hasher.update(git_head_fingerprint(repo_root));
    hasher.update(b"staged\0");
    hasher.update(&staged);
    hasher.update(b"unstaged\0");
    hasher.update(&unstaged);
    for path in &untracked_paths {
        hash_untracked_git_entry(repo_root, path, &mut hasher)?;
    }
    hash_git_empty_directories(repo_root, repo_root, &mut hasher)?;

    Ok(format!("git:{:x}", hasher.finalize()))
}

fn git_change_scope_fingerprint(repo_root: &Path) -> AppResult<String> {
    if !git_repo_available(repo_root)? {
        return workspace_change_scope_fingerprint(repo_root);
    }

    let status = git_command_stdout(
        repo_root,
        &git_args_with_iterative_fingerprint_pathspecs(&[
            "status",
            "--porcelain=v1",
            "-z",
            "--untracked-files=all",
            "--",
        ]),
        "status --porcelain",
    )?;

    let mut hasher = Sha256::new();
    hasher.update(b"head\0");
    hasher.update(git_head_fingerprint(repo_root));
    hasher.update(b"status\0");
    hasher.update(&status);
    hash_git_empty_directories(repo_root, repo_root, &mut hasher)?;

    Ok(format!("git-scope:{:x}", hasher.finalize()))
}

fn iterative_loop_fingerprint(
    repo_root: &Path,
    mode: IterativeFingerprintMode,
) -> AppResult<String> {
    match mode {
        IterativeFingerprintMode::Content => git_diff_fingerprint(repo_root),
        IterativeFingerprintMode::ChangeScope => git_change_scope_fingerprint(repo_root),
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_iterative_plan_and_implement_stage<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    execution_cwd: Option<&Path>,
    project_root: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &mut RunSnapshot,
    stage_entry: &StagePlan,
    cursor: &StageCursor,
    prompt: String,
    execution_context: Option<&Value>,
    pending_amendments: Option<&[QueuedAmendment]>,
    cancellation_token: CancellationToken,
    resolved_target: ResolvedBackendTarget,
    timeout: Duration,
    effective_config: &EffectiveConfig,
) -> AppResult<(ValidatedBundle, RecordProducer)>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let repo_root = execution_cwd.unwrap_or(base_dir);
    let persisted_state = current_active_run(snapshot)?
        .iterative_implementer_state
        .clone()
        .unwrap_or(IterativeImplementerState {
            completed_iterations: 0,
            stable_count: 0,
            loop_policy: None,
            stage_target: None,
        });
    let loop_policy = iterative_loop_policy_for_attempt(Some(&persisted_state), effective_config)?;
    let max_rounds = loop_policy.max_consecutive_implementer_rounds;
    let stable_rounds_required = loop_policy.stable_rounds_required;
    let recovery_target = iterative_resume_target(
        Some(&persisted_state),
        &resolved_target,
        stage_entry.stage_id,
    )?;
    let fingerprint_mode = iterative_stability_fingerprint_mode(pending_amendments);
    let mut stable_count = persisted_state.stable_count;
    let mut iteration = persisted_state.completed_iterations;
    loop {
        if cancellation_token.is_cancelled() {
            return Err(AppError::InvocationCancelled {
                backend: resolved_target.backend.family.to_string(),
                contract_id: stage_entry.stage_id.to_string(),
            });
        }

        match resume_terminal_iterative_stage_result(
            journal_store,
            base_dir,
            project_root,
            project_id,
            run_id,
            seq,
            stage_entry,
            cursor,
            &recovery_target,
            iteration,
            stable_count,
            stable_rounds_required,
            max_rounds,
        )? {
            TerminalIterativeResumeResult::Recovered(recovered) => return Ok(*recovered),
            TerminalIterativeResumeResult::NotTerminal => {}
        }

        iteration += 1;
        snapshot.status_summary =
            stage_running_summary(stage_entry.stage_id, Some(iteration), max_rounds);
        run_snapshot_write
            .write_run_snapshot(base_dir, project_id, snapshot)
            .map_err(|error| AppError::StageCommitFailed {
                stage_id: stage_entry.stage_id,
                details: format!(
                    "failed to update snapshot for iterative_minimal iteration {iteration}: {error}"
                ),
            })?;

        *seq += 1;
        let started = journal::implementer_iteration_started_event(
            *seq,
            Utc::now(),
            run_id,
            stage_entry.stage_id,
            cursor.cycle,
            cursor.attempt,
            cursor.completion_round,
            iteration,
        );
        let started_line = journal::serialize_event(&started)?;
        if let Err(error) = journal_store.append_event(base_dir, project_id, &started_line) {
            *seq -= 1;
            return Err(AppError::StageCommitFailed {
                stage_id: stage_entry.stage_id,
                details: format!(
                    "failed to persist implementer_iteration_started event for iteration {iteration}: {error}"
                ),
            });
        }

        let diff_before = iterative_loop_fingerprint(repo_root, fingerprint_mode)?;
        let iterative_invocation_context = json!({
            "iteration": iteration,
            "iterative_minimal": {
                "max_consecutive_implementer_rounds": max_rounds,
                "stable_rounds_required": stable_rounds_required,
            }
        });
        let iteration_result = invoke_stage_on_backend(
            agent_service,
            base_dir,
            execution_cwd,
            project_root,
            run_id,
            stage_entry,
            cursor,
            prompt.clone(),
            execution_context,
            pending_amendments,
            cancellation_token.clone(),
            recovery_target.clone(),
            timeout,
            Some(&format!("it{iteration}")),
            Some(&iterative_invocation_context),
        )
        .await;

        let (bundle, producer) = match iteration_result {
            Ok(result) => result,
            Err(error) => return Err(error),
        };

        let diff_after = iterative_loop_fingerprint(repo_root, fingerprint_mode)?;
        let diff_changed = diff_before != diff_after;
        let outcome = iterative_iteration_outcome(&bundle);

        *seq += 1;
        let completed = journal::implementer_iteration_completed_event(
            *seq,
            Utc::now(),
            run_id,
            stage_entry.stage_id,
            cursor.cycle,
            cursor.attempt,
            cursor.completion_round,
            iteration,
            diff_changed,
            &outcome,
        );
        let completed_line = journal::serialize_event(&completed)?;
        if let Err(error) = journal_store.append_event(base_dir, project_id, &completed_line) {
            *seq -= 1;
            return Err(AppError::StageCommitFailed {
                stage_id: stage_entry.stage_id,
                details: format!(
                    "failed to persist implementer_iteration_completed event for iteration {iteration}: {error}"
                ),
            });
        }

        let (next_stable_count, exit_reason) = advance_iterative_loop_state(
            stable_count,
            iteration,
            diff_changed,
            stable_rounds_required,
            max_rounds,
        );
        stable_count = next_stable_count;
        if let Some(active_run) = snapshot.active_run.as_mut() {
            active_run.iterative_implementer_state = Some(IterativeImplementerState {
                completed_iterations: iteration,
                stable_count,
                loop_policy: Some(loop_policy.clone()),
                stage_target: Some(resolved_target_to_record(&recovery_target)),
            });
        }
        run_snapshot_write
            .write_run_snapshot(base_dir, project_id, snapshot)
            .map_err(|error| AppError::StageCommitFailed {
                stage_id: stage_entry.stage_id,
                details: format!(
                    "failed to persist iterative_minimal state after iteration {iteration}: {error}"
                ),
            })?;

        if let Some(reason) = exit_reason {
            let events = journal_store.read_journal(base_dir, project_id)?;
            if !iterative_loop_exit_recorded(&events, run_id, cursor, reason, iteration)? {
                append_implementer_loop_exited_event(
                    journal_store,
                    base_dir,
                    project_id,
                    run_id,
                    seq,
                    stage_entry.stage_id,
                    cursor,
                    reason,
                    iteration,
                )?;
            }
            return Ok((bundle, producer));
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
    invocation_suffix: Option<&str>,
    additional_context: Option<&Value>,
) -> AppResult<(ValidatedBundle, RecordProducer)>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let invocation_id =
        invocation_id_for_stage(run_id, stage_entry.stage_id, cursor, invocation_suffix);
    let request = InvocationRequest {
        invocation_id: invocation_id.clone(),
        project_root: project_root.to_path_buf(),
        working_dir: execution_cwd.unwrap_or(base_dir).to_path_buf(),
        contract: InvocationContract::Stage(stage_entry.contract),
        role: stage_entry.role,
        resolved_target: resolved_target.clone(),
        payload: InvocationPayload {
            prompt,
            context: invocation_context(
                cursor,
                execution_context,
                pending_amendments,
                additional_context,
            ),
        },
        timeout,
        cancellation_token,
        session_policy: SessionPolicy::ReuseIfAllowed,
        prior_session: None,
        attempt_number: cursor.attempt,
    };

    agent_service.invoke(request).await.and_then(|envelope| {
        let producer = agent_record_producer(&envelope.metadata);
        let bundle = stage_entry
            .contract
            .evaluate_permissive(&envelope.parsed_payload)
            .map_err(|contract_error| AppError::InvocationFailed {
                backend: resolved_target.backend.family.to_string(),
                contract_id: stage_entry.stage_id.to_string(),
                failure_class: contract_error.failure_class(),
                details: contract_error.to_string(),
            })?;
        if invocation_suffix.is_some() {
            persist_invocation_parsed_payload_best_effort(
                project_root,
                &invocation_id,
                stage_entry.stage_id,
                &envelope.parsed_payload,
                &producer,
            );
        }
        Ok((bundle, producer))
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
    complete_run_with_force_complete_details(
        snapshot,
        run_snapshot_write,
        journal_store,
        amendment_queue_port,
        base_dir,
        project_id,
        run_id,
        seq,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn complete_run_with_force_complete_details(
    snapshot: &mut RunSnapshot,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    amendment_queue_port: &dyn AmendmentQueuePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    force_complete: Option<&ForceCompleteDeferredAmendments>,
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
        let _ = FileSystem::remove_pid_file(base_dir, project_id);
        return Err(e);
    }

    snapshot.status = RunStatus::Completed;
    snapshot.active_run = None;
    snapshot.interrupted_run = None;
    snapshot.completion_rounds = snapshot.completion_rounds.max(1);
    snapshot.status_summary = force_complete
        .map(ForceCompleteDeferredAmendments::status_message)
        .unwrap_or_else(|| "completed".to_owned());
    run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)?;
    let _ = FileSystem::remove_pid_file(base_dir, project_id);

    *seq += 1;
    let run_completed = if let Some(force_complete) = force_complete {
        journal::force_completed_run_completed_event(
            *seq,
            Utc::now(),
            run_id,
            snapshot.completion_rounds,
            snapshot.max_completion_rounds.unwrap_or(0),
            force_complete.round,
            force_complete.count(),
        )
    } else {
        journal::run_completed_event(
            *seq,
            Utc::now(),
            run_id,
            snapshot.completion_rounds,
            snapshot.max_completion_rounds.unwrap_or(0),
        )
    };
    let append_result = journal::serialize_event(&run_completed).and_then(|run_completed_line| {
        journal_store.append_event(base_dir, project_id, &run_completed_line)
    });
    append_result?;
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
    run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)?;
    let _ = FileSystem::remove_pid_file(base_dir, project_id);
    Ok(())
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
    let completion_rounds_progress = match err {
        AppError::StageCommitFailed { details, .. } => max_completion_rounds_progress(details),
        _ => None,
    };
    let completion_rounds = snapshot.completion_rounds;
    let max_completion_rounds = completion_rounds_progress
        .as_ref()
        .map(|&(_, max_rounds)| max_rounds)
        .or(snapshot.max_completion_rounds)
        .unwrap_or(0);
    let completion_rounds_display = completion_rounds_progress
        .as_ref()
        .map(|&(current_round, max_rounds)| format!("{current_round}/{max_rounds}"));

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
    // Best-effort: if this write fails (e.g. persistent disk I/O error
    // when called from the pre-backoff recovery path), we still proceed
    // to emit the run_failed journal event.  The journal is authoritative
    // for derive_resume_state, so recording the failure there is more
    // valuable than a consistent snapshot when the disk is degraded.
    //
    // We retry once after a short delay to handle transient I/O errors.
    // If both attempts fail, the journal's run_failed event is the
    // authoritative record and resume/status will reconcile from it.
    if let Err(first_err) = run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot) {
        eprintln!(
            "fail_run: snapshot write failed for stage {} (attempt 1): {first_err}",
            stage_id.as_str(),
        );
        // Brief delay before retry to let transient conditions clear.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if let Err(second_err) =
            run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)
        {
            eprintln!(
                "fail_run: snapshot write failed for stage {} (attempt 2): {second_err} — \
                 journal run_failed event is the authoritative record",
                stage_id.as_str(),
            );
        }
    }

    *seq += 1;
    let run_failed = journal::run_failed_event(
        *seq,
        Utc::now(),
        run_id,
        stage_id,
        &failure_class,
        &message,
        completion_rounds,
        max_completion_rounds,
        completion_rounds_display.as_deref(),
    );
    if let Ok(run_failed_line) = journal::serialize_event(&run_failed) {
        let _ = journal_store.append_event(base_dir, project_id, &run_failed_line);
    }
    let _ = FileSystem::remove_pid_file(base_dir, project_id);
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

fn max_completion_rounds_progress(details: &str) -> Option<(u32, u32)> {
    const PREFIX: &str = "max completion rounds exceeded: ";
    let remainder = details.strip_prefix(PREFIX)?;
    let mut parts = remainder.split('/');
    let current_round = parts.next()?.parse::<u32>().ok()?;
    let max_rounds = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((current_round, max_rounds))
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

#[derive(Debug, Clone)]
struct ReviewFollowUp {
    body: String,
    classification: ReviewFindingClass,
    covered_by_bead_id: Option<String>,
    proposed_bead_summary: Option<String>,
}

impl ReviewFollowUp {
    fn from_legacy(body: String) -> Self {
        Self {
            body,
            classification: ReviewFindingClass::FixCurrentBead,
            covered_by_bead_id: None,
            proposed_bead_summary: None,
        }
    }

    fn from_classified(finding: &ClassifiedFinding) -> Self {
        Self {
            body: finding.body.clone(),
            classification: finding.classification,
            covered_by_bead_id: finding.covered_by_bead_id.clone(),
            proposed_bead_summary: finding.proposed_bead_summary.clone(),
        }
    }
}

/// Returns the follow-ups/amendments that should be queued for remediation.
fn validation_follow_ups(payload: &StagePayload) -> Vec<ReviewFollowUp> {
    match payload {
        StagePayload::Validation(validation) => {
            let mut follow_ups = validation
                .classified_findings
                .iter()
                .map(|finding| {
                    if finding.classification != ReviewFindingClass::FixCurrentBead {
                        tracing::info!(
                            classification = %finding.classification,
                            covered_by_bead_id = ?finding.covered_by_bead_id,
                            proposed_bead_summary = ?finding.proposed_bead_summary,
                            "review finding classification surfaced"
                        );
                    }
                    ReviewFollowUp::from_classified(finding)
                })
                .collect::<Vec<_>>();

            for legacy in &validation.follow_up_or_amendments {
                let normalized_legacy = legacy.trim();
                if normalized_legacy.is_empty()
                    || follow_ups
                        .iter()
                        .any(|follow_up| follow_up.body.trim() == normalized_legacy)
                {
                    continue;
                }
                follow_ups.push(ReviewFollowUp::from_legacy(legacy.clone()));
            }

            follow_ups
        }
        _ => Vec::new(),
    }
}

fn has_restart_triggering_follow_up(payload: &StagePayload) -> bool {
    validation_follow_ups(payload)
        .iter()
        .any(|follow_up| follow_up.classification.triggers_restart())
}

fn has_deferred_classified_finding(payload: &StagePayload) -> bool {
    match payload {
        StagePayload::Validation(validation) => validation
            .classified_findings
            .iter()
            .any(|finding| !finding.classification.triggers_restart()),
        _ => false,
    }
}

fn validation_findings(payload: &StagePayload) -> &[String] {
    match payload {
        StagePayload::Validation(validation) => &validation.findings_or_gaps,
        _ => &[],
    }
}

fn skip_next_apply_fixes_reason(
    payload: &StagePayload,
    next_stage: Option<StageId>,
) -> Option<&'static str> {
    if next_stage != Some(StageId::ApplyFixes) {
        return None;
    }

    if validation_outcome(payload) == Some(ReviewOutcome::Approved)
        && validation_findings(payload).is_empty()
        && validation_follow_ups(payload).is_empty()
    {
        return Some("review approved with no findings");
    }

    if matches!(
        validation_outcome(payload),
        Some(
            ReviewOutcome::Approved
                | ReviewOutcome::ConditionallyApproved
                | ReviewOutcome::RequestChanges
        )
    ) && has_deferred_classified_finding(payload)
        && !has_restart_triggering_follow_up(payload)
    {
        return Some("review only has deferred non-fix classifications");
    }

    None
}

/// Build typed QueuedAmendment records from follow-up strings.
fn build_queued_amendments(
    follow_ups: &[ReviewFollowUp],
    source_stage: StageId,
    source_cycle: u32,
    source_completion_round: u32,
    run_id: &RunId,
) -> Vec<QueuedAmendment> {
    let now = Utc::now();
    follow_ups
        .iter()
        .filter(|follow_up| follow_up.classification.triggers_restart())
        .enumerate()
        .map(|(idx, follow_up)| {
            let source = crate::contexts::project_run_record::model::AmendmentSource::WorkflowStage;
            let dedup_key = QueuedAmendment::compute_dedup_key(&source, &follow_up.body);
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
                body: follow_up.body.clone(),
                created_at: now,
                batch_sequence: (idx + 1) as u32,
                source,
                dedup_key,
                classification: follow_up.classification,
                covered_by_bead_id: follow_up.covered_by_bead_id.clone(),
                proposed_bead_summary: follow_up.proposed_bead_summary.clone(),
            }
        })
        .collect()
}

fn build_recorded_follow_ups(
    follow_ups: &[ReviewFollowUp],
    source_stage: StageId,
    source_cycle: u32,
    source_completion_round: u32,
    run_id: &RunId,
) -> Vec<QueuedAmendment> {
    let now = Utc::now();
    follow_ups
        .iter()
        .filter(|follow_up| follow_up.classification.triggers_restart())
        .enumerate()
        .map(|(idx, follow_up)| {
            let source = crate::contexts::project_run_record::model::AmendmentSource::WorkflowStage;
            let dedup_key = QueuedAmendment::compute_dedup_key(&source, &follow_up.body);
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
                body: follow_up.body.clone(),
                created_at: now,
                batch_sequence: (idx + 1) as u32,
                source,
                dedup_key,
                classification: follow_up.classification,
                covered_by_bead_id: follow_up.covered_by_bead_id.clone(),
                proposed_bead_summary: follow_up.proposed_bead_summary.clone(),
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
    additional_context: Option<&Value>,
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

    if let Some(additional_context) = additional_context.and_then(Value::as_object) {
        for (key, value) in additional_context {
            context[key] = value.clone();
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
    snapshot: &RunSnapshot,
    effective_config: &EffectiveConfig,
) -> AppResult<Vec<StageId>> {
    match preset {
        FlowPreset::Standard => {
            let first_stage = if let Some(run_started) = events.iter().rev().find(|event| {
                event.event_type
                    == crate::contexts::project_run_record::model::JournalEventType::RunStarted
            }) {
                detail_stage_id(run_started, "first_stage")?
            } else {
                resume_seed_active_run(snapshot)?.stage_cursor.stage
            };
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
    run_id: &RunId,
    events: &[JournalEvent],
    snapshot: &RunSnapshot,
    stage_plan: &[StagePlan],
    semantics: FlowSemantics,
) -> AppResult<ResumeState> {
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
    // Track the highest retryable-failed attempt per (stage, cycle) so that
    // a crash during the backoff window does not reset the attempt counter
    // back to 1 and replenish the retry budget.
    let mut retryable_failed_attempts: HashMap<(StageId, u32), u32> = HashMap::new();

    for event in events {
        match event.event_type {
            crate::contexts::project_run_record::model::JournalEventType::StageCompleted => {
                let stage_id = detail_stage_id(event, "stage_id")?;
                current_cycle = detail_u32(event, "cycle").unwrap_or(current_cycle);
                next_stage_index = stage_index_for(stage_plan, stage_id)? + 1;
                last_completed_stage = Some(stage_id);
                // A successful completion resets retry history for this
                // stage+cycle so a future revisit (e.g. via completion
                // round advance) starts with a fresh budget.
                retryable_failed_attempts.remove(&(stage_id, current_cycle));
            }
            crate::contexts::project_run_record::model::JournalEventType::StageFailed => {
                if let (Ok(stage_id), Some(cycle), Some(attempt)) = (
                    detail_stage_id(event, "stage_id"),
                    detail_u32(event, "cycle"),
                    detail_u32(event, "attempt"),
                ) {
                    let will_retry = event
                        .details
                        .get("will_retry")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    if will_retry {
                        let entry =
                            retryable_failed_attempts.entry((stage_id, cycle)).or_insert(0);
                        *entry = (*entry).max(attempt);
                    } else {
                        // Terminal failure (non-retryable or retries exhausted):
                        // clear the entry so a resumed run starts at attempt 1
                        // rather than continuing from the stale counter.
                        retryable_failed_attempts.remove(&(stage_id, cycle));
                    }
                }
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
                // Drop retry history for cycles that are now behind the
                // cursor — they can never be revisited, so keeping them
                // wastes memory.
                retryable_failed_attempts.retain(|&(_, c), _| c >= current_cycle);
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
                // New completion round = fresh execution of all stages;
                // clear retry history so prior-round failures don't bleed
                // into the new round's budget.
                retryable_failed_attempts.clear();
            }
            crate::contexts::project_run_record::model::JournalEventType::StageSkipped => {
                let stage_id = detail_stage_id(event, "stage_id")?;
                current_cycle = detail_u32(event, "cycle").unwrap_or(current_cycle);
                next_stage_index = stage_index_for(stage_plan, stage_id)? + 1;
                last_completed_stage = Some(stage_id);
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
    let resume_stage_id = stage_plan[next_stage_index].stage_id;
    let resume_cycle = current_cycle.max(1);
    // If the last failure for this stage+cycle was retryable, resume at
    // failed_attempt + 1 so we don't replenish the retry budget on restart.
    let resume_attempt = retryable_failed_attempts
        .get(&(resume_stage_id, resume_cycle))
        .map(|&a| a + 1)
        .unwrap_or(1);
    let cursor = StageCursor::new(
        resume_stage_id,
        resume_cycle,
        resume_attempt,
        completion_round,
    )?;

    Ok(ResumeState {
        run_id: run_id.clone(),
        stage_index: next_stage_index,
        cursor,
    })
}

fn run_id_for_resume(snapshot: &RunSnapshot) -> AppResult<RunId> {
    RunId::new(resume_seed_active_run(snapshot)?.run_id.clone())
}

fn events_for_run(events: &[JournalEvent], run_id: &RunId) -> Vec<JournalEvent> {
    events
        .iter()
        .filter(|event| {
            event.details.get("run_id").and_then(Value::as_str) == Some(run_id.as_str())
        })
        .cloned()
        .collect()
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
    FileSystem::project_root(base_dir, project_id)
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
    accepted_amendments: Vec<FinalReviewQueuedAmendment>,
}

#[derive(Clone)]
struct FinalReviewQueuedAmendment {
    queued: QueuedAmendment,
    reviewer_sources:
        Vec<crate::contexts::workflow_composition::panel_contracts::FinalReviewAmendmentSource>,
    /// Legacy covered-by bead ID retained for downstream inspectability.
    mapped_to_bead_id: Option<String>,
}

struct ForceCompleteDeferredAmendments {
    round: u32,
    amendments: Vec<serde_json::Value>,
}

impl ForceCompleteDeferredAmendments {
    fn count(&self) -> u32 {
        self.amendments.len() as u32
    }

    fn is_empty(&self) -> bool {
        self.amendments.is_empty()
    }

    fn status_message(&self) -> String {
        if self.is_empty() {
            // No amendments to defer — don't point operators at a journal
            // event that won't be written for the empty case.
            format!(
                "force-completed at round {}: no amendments deferred",
                self.round
            )
        } else {
            format!(
                "force-completed at round {}: {} amendments deferred to journal (see force_complete_amendments_deferred event)",
                self.round,
                self.count()
            )
        }
    }
}

fn deferred_final_review_amendments(
    round: u32,
    amendments: &[FinalReviewQueuedAmendment],
) -> ForceCompleteDeferredAmendments {
    let amendments = amendments
        .iter()
        .map(|amendment| {
            json!({
                "id": amendment.queued.amendment_id,
                "summary": amendment
                    .queued
                    .proposed_bead_summary
                    .as_deref()
                    .unwrap_or(amendment.queued.body.as_str()),
                "classification": amendment.queued.classification.as_str(),
            })
        })
        .collect::<Vec<_>>();
    ForceCompleteDeferredAmendments { round, amendments }
}

fn mark_final_review_aggregate_force_completed(
    commit_data: &mut FinalReviewCommitData,
    force_complete: Option<&ForceCompleteDeferredAmendments>,
) {
    if let Some(object) = commit_data.aggregate_payload.as_object_mut() {
        object.insert("restart_required".to_owned(), Value::Bool(false));
        object.insert("force_completed".to_owned(), Value::Bool(true));
        if let Some(force_complete) = force_complete {
            object.insert(
                "summary".to_owned(),
                Value::String(force_complete.status_message()),
            );
        }
    }
    if let Ok(payload) =
        serde_json::from_value::<FinalReviewAggregatePayload>(commit_data.aggregate_payload.clone())
    {
        commit_data.aggregate_artifact = super::renderers::render_final_review_aggregate(&payload);
    }
}

fn partition_final_review_amendments_by_route(
    amendments: &[FinalReviewQueuedAmendment],
) -> (
    Vec<&FinalReviewQueuedAmendment>,
    Vec<&FinalReviewQueuedAmendment>,
) {
    let planned_elsewhere = Vec::new();
    let mut restart_queue = Vec::new();
    for amendment in amendments {
        if amendment.queued.classification.triggers_restart() {
            restart_queue.push(amendment);
        }
    }
    (planned_elsewhere, restart_queue)
}

enum FinalReviewPanelOutcome {
    Complete(StageCursor, FinalReviewCommitData),
    Restart(StageCursor, FinalReviewCommitData),
}

struct RuntimeFinalReviewPanelResolution {
    panel: FinalReviewPanelResolution,
    probe_exhausted_reviewers: usize,
    effective_min_reviewers: usize,
}

async fn probe_final_review_reviewers<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    reviewers: &[ResolvedPanelMember],
    min_reviewers: usize,
    cancellation_token: CancellationToken,
) -> AppResult<(Vec<ResolvedPanelMember>, usize, usize)>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let mut available_reviewers = Vec::new();
    let mut probe_exhausted_reviewers = 0usize;
    let mut first_optional_probe_failure: Option<(usize, AppError)> = None;
    let mut last_probe_exhaustion_error: Option<AppError> = None;

    for (idx, member) in reviewers.iter().enumerate() {
        let reviewer_id = final_review::final_review_reviewer_id(idx);
        match final_review::check_final_review_availability_with_retry(
            agent_service,
            &member.target,
            BackendPolicyRole::FinalReviewer,
            &reviewer_id,
            "reviewer",
            cancellation_token.clone(),
        )
        .await
        {
            Ok(_) => available_reviewers.push(member.clone()),
            Err(error)
                if error
                    .failure_class()
                    .is_some_and(|fc| fc == FailureClass::BackendExhausted) =>
            {
                probe_exhausted_reviewers += 1;
                tracing::warn!(
                    backend = %member.target.backend.family,
                    required = member.required,
                    "reviewer unavailable during probe (backend exhausted), skipping"
                );
                last_probe_exhaustion_error = Some(error);
            }
            Err(error)
                if final_review::is_final_review_availability_retry_exhaustion_error(&error)
                    && !member.required =>
            {
                tracing::warn!(
                    reviewer = reviewer_id,
                    backend = %member.target.backend.family,
                    model = %member.target.model.model_id,
                    error = %error,
                    "optional reviewer probe exhausted transient retries; preserving reviewer for invocation-time handling"
                );
                available_reviewers.push(member.clone());
            }
            Err(error) if member.required => return Err(error),
            Err(error) => match &first_optional_probe_failure {
                None => {
                    first_optional_probe_failure = Some((idx, error));
                }
                Some((prev_idx, _)) if idx < *prev_idx => {
                    first_optional_probe_failure = Some((idx, error));
                }
                _ => {}
            },
        }
    }

    let effective_min_reviewers = min_reviewers
        .min(reviewers.len().saturating_sub(probe_exhausted_reviewers))
        .max(1);
    if available_reviewers.len() < effective_min_reviewers {
        if let Some((_, error)) = first_optional_probe_failure {
            return Err(error);
        }
        if let Some(error) = last_probe_exhaustion_error {
            return Err(error);
        }
        return Err(AppError::InsufficientPanelMembers {
            panel: "final_review".to_owned(),
            resolved: available_reviewers.len(),
            minimum: effective_min_reviewers,
        });
    }

    Ok((
        available_reviewers,
        probe_exhausted_reviewers,
        effective_min_reviewers,
    ))
}

async fn resolve_runtime_final_review_panel<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    effective_config: &EffectiveConfig,
    cycle: u32,
    cancellation_token: CancellationToken,
) -> AppResult<RuntimeFinalReviewPanelResolution>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
{
    let policy = BackendPolicyService::new(effective_config);
    let mut panel = policy.resolve_final_review_panel(cycle)?;
    let min_reviewers = effective_config.final_review_policy().min_reviewers;

    final_review::check_final_review_availability_with_retry(
        agent_service,
        &panel.arbiter,
        BackendPolicyRole::Arbiter,
        "arbiter",
        "arbiter",
        cancellation_token.clone(),
    )
    .await
    .map_err(|error| {
        if final_review::is_final_review_availability_retry_exhaustion_error(&error) {
            error
        } else {
            let failure_class = match &error {
                AppError::BackendUnavailable { failure_class, .. } => *failure_class,
                _ => None,
            };
            AppError::BackendUnavailable {
                backend: panel.arbiter.backend.family.to_string(),
                details: format!("required final-review arbiter unavailable: {error}"),
                failure_class,
            }
        }
    })?;

    let (available_reviewers, probe_exhausted_reviewers, effective_min_reviewers) =
        probe_final_review_reviewers(
            agent_service,
            &panel.reviewers,
            min_reviewers,
            cancellation_token,
        )
        .await?;

    panel.reviewers = available_reviewers;

    Ok(RuntimeFinalReviewPanelResolution {
        panel,
        probe_exhausted_reviewers,
        effective_min_reviewers,
    })
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
    let max_refinement_retries = effective_config
        .prompt_review_policy()
        .max_refinement_retries;

    // ── Pre-snapshot availability filtering ─────────────────────────────
    // Check runtime availability of the refiner and each validator BEFORE
    // building and persisting the snapshot. The refiner is always required;
    // if it is unavailable, the stage fails before any snapshot or
    // invocation side effects.
    agent_service
        .adapter()
        .check_availability(&panel.refiner)
        .await
        .map_err(|e| {
            let failure_class = match &e {
                AppError::BackendUnavailable { failure_class, .. } => *failure_class,
                _ => None,
            };
            AppError::BackendUnavailable {
                backend: panel.refiner.backend.family.to_string(),
                details: format!("required prompt-review refiner unavailable: {e}"),
                failure_class,
            }
        })?;

    // Required unavailable validators fail resolution; optional
    // unavailable validators are removed so the snapshot only records
    // members that will actually execute.  Unlike completion/final-review,
    // prompt-review does NOT degrade on BackendExhausted — any unavailable
    // validator (exhausted or otherwise) follows normal required/optional rules.
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
    // generic stage-level planning role.
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
        max_refinement_retries,
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
    // BackendExhausted probes are treated as graceful degradation: the
    // member is skipped and the panel proceeds if quorum still holds.
    let mut available_completers = Vec::new();
    let mut probe_exhausted_completers: usize = 0;
    let mut probe_failed_completers: usize = 0;
    for member in &panel.completers {
        match agent_service
            .adapter()
            .check_availability(&member.target)
            .await
        {
            Ok(()) => available_completers.push(member.clone()),
            Err(e) => {
                probe_failed_completers += 1;
                // BackendExhausted during probe → skip for graceful
                // degradation instead of aborting the entire stage.
                if e.failure_class()
                    .is_some_and(|fc| fc == FailureClass::BackendExhausted)
                {
                    probe_exhausted_completers += 1;
                    tracing::warn!(
                        backend = %member.target.backend.family,
                        required = member.required,
                        "completer unavailable during probe (backend exhausted), skipping"
                    );
                    continue;
                }
                if member.required {
                    return Err(e);
                }
                // Optional completer unavailable — remove before snapshot.
            }
        }
    }
    let effective_min_completers = min_completers
        .min(
            panel
                .completers
                .len()
                .saturating_sub(probe_exhausted_completers),
        )
        .max(1);
    if available_completers.len() < effective_min_completers {
        // Only surface BackendExhausted when the shortfall is caused
        // solely by exhausted members.  Mixed failures (exhausted +
        // transiently unavailable) preserve the retryable
        // InsufficientPanelMembers path so transient errors can retry.
        if probe_exhausted_completers > 0 && probe_exhausted_completers == probe_failed_completers {
            return Err(AppError::BackendUnavailable {
                backend: "completion".to_owned(),
                details: format!(
                    "insufficient completers after exhaustion: {} available, {} needed (original min={}, {} exhausted)",
                    available_completers.len(),
                    effective_min_completers,
                    min_completers,
                    probe_exhausted_completers,
                ),
                failure_class: Some(FailureClass::BackendExhausted),
            });
        }
        return Err(AppError::InsufficientPanelMembers {
            panel: "completion".to_owned(),
            resolved: available_completers.len(),
            minimum: effective_min_completers,
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
                "completion panel: {} completers, min={} (effective={}), threshold={}",
                panel.completers.len(),
                min_completers,
                effective_min_completers,
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
        probe_exhausted_completers,
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
    let RuntimeFinalReviewPanelResolution {
        panel,
        probe_exhausted_reviewers,
        ..
    } = resolve_runtime_final_review_panel(
        agent_service,
        effective_config,
        cursor.cycle,
        cancellation_token.clone(),
    )
    .await?;
    let min_reviewers = effective_config.final_review_policy().min_reviewers;
    let consensus_threshold = effective_config.final_review_policy().consensus_threshold;
    let max_restarts = effective_config.final_review_policy().max_restarts;

    let resolution = build_final_review_snapshot(stage_id, &panel.reviewers, &panel.arbiter);
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
        journal_store,
        base_dir,
        project_root,
        backend_working_dir,
        project_id,
        run_id,
        seq,
        cursor,
        &panel,
        min_reviewers,
        probe_exhausted_reviewers,
        consensus_threshold,
        max_restarts,
        current_active_run(snapshot)?.final_review_restart_count,
        prompt_reference,
        snapshot.rollback_point_meta.rollback_count,
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
            FinalReviewQueuedAmendment {
                queued: QueuedAmendment {
                    amendment_id: amendment.amendment_id.clone(),
                    source_stage: stage_id,
                    source_cycle: cursor.cycle,
                    source_completion_round: cursor.completion_round,
                    body: amendment.normalized_body.clone(),
                    created_at: Utc::now(),
                    batch_sequence: (index + 1) as u32,
                    source,
                    dedup_key,
                    classification: amendment.classification,
                    covered_by_bead_id: amendment.covered_by_bead_id.clone(),
                    proposed_bead_summary: amendment.proposed_bead_summary.clone(),
                },
                reviewer_sources: amendment.sources.clone(),
                mapped_to_bead_id: amendment.mapped_to_bead_id.clone(),
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

/// Record planned-elsewhere mappings for amendments that are classified as
/// belonging to another bead. Best-effort: failures are logged but do not
/// block the active bead from proceeding.
fn record_planned_elsewhere_amendments(
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    amendments: &[&FinalReviewQueuedAmendment],
    run_id: &RunId,
    completion_round: u32,
) {
    // Read project record to get task_source (milestone_id + bead_id).
    let project_record = match FsProjectStore.read_project_record(base_dir, project_id) {
        Ok(record) => record,
        Err(e) => {
            let _ = log_write.append_runtime_log(
                base_dir,
                project_id,
                &RuntimeLogEntry {
                    timestamp: Utc::now(),
                    level: LogLevel::Warn,
                    source: "engine".to_owned(),
                    message: format!(
                        "cannot record planned-elsewhere mappings: failed to read project record: {e}"
                    ),
                },
            );
            return;
        }
    };

    let Some(task_source) = project_record.task_source.as_ref() else {
        let _ = log_write.append_runtime_log(
            base_dir,
            project_id,
            &RuntimeLogEntry {
                timestamp: Utc::now(),
                level: LogLevel::Warn,
                source: "engine".to_owned(),
                message: "cannot record planned-elsewhere mappings: no task_source on project"
                    .to_owned(),
            },
        );
        return;
    };

    let milestone_id = match MilestoneId::new(&task_source.milestone_id) {
        Ok(id) => id,
        Err(e) => {
            let _ = log_write.append_runtime_log(
                base_dir,
                project_id,
                &RuntimeLogEntry {
                    timestamp: Utc::now(),
                    level: LogLevel::Warn,
                    source: "engine".to_owned(),
                    message: format!(
                        "cannot record planned-elsewhere mappings: invalid milestone ID: {e}"
                    ),
                },
            );
            return;
        }
    };

    // PE validation is authoritative in final_review.rs (lines 644-656 and
    // 1526-1536) which strips invalid mapped_to_bead_id values before
    // acceptance.  No redundant re-read of the mutable prompt here.

    // Always write a PE round sentinel so that rebuild_planned_elsewhere_from_journal
    // knows this completion_round was processed — even if zero PE mappings exist.
    // This allows a later round with no PE findings to supersede an earlier round.
    // Retry once on failure since the sentinel is the sole mechanism for zero-PE
    // round supersession — a missing sentinel leaves stale mappings authoritative
    // permanently (reconstruction from aggregates cannot synthesise sentinels).
    let now = Utc::now();
    let sentinel_result = milestone_service::record_planned_elsewhere_round_sentinel(
        &FsMilestoneJournalStore,
        base_dir,
        &milestone_id,
        &task_source.bead_id,
        run_id.as_str(),
        completion_round,
        now,
    )
    .or_else(|first_err| {
        tracing::warn!(
            error = %first_err,
            completion_round,
            "PE round sentinel write failed, retrying once"
        );
        milestone_service::record_planned_elsewhere_round_sentinel(
            &FsMilestoneJournalStore,
            base_dir,
            &milestone_id,
            &task_source.bead_id,
            run_id.as_str(),
            completion_round,
            Utc::now(),
        )
    });
    if let Err(e) = sentinel_result {
        let _ = log_write.append_runtime_log(
            base_dir,
            project_id,
            &RuntimeLogEntry {
                timestamp: Utc::now(),
                level: LogLevel::Error,
                source: "engine".to_owned(),
                message: format!(
                    "failed to write PE round sentinel for completion_round={completion_round} \
                     after retry: {e} — stale planned-elsewhere mappings from earlier rounds \
                     may not be superseded"
                ),
            },
        );
    }
    for amendment in amendments {
        let Some(mapped_to) = amendment.mapped_to_bead_id.as_deref() else {
            continue;
        };
        let mapped_to = mapped_to.trim();
        if mapped_to.is_empty() {
            continue;
        }
        let mapping = PlannedElsewhereMapping {
            active_bead_id: task_source.bead_id.clone(),
            finding_summary: amendment.queued.body.clone(),
            mapped_to_bead_id: mapped_to.to_owned(),
            recorded_at: now,
            mapped_bead_verified: false, // Verification deferred to automation_runtime
            run_id: Some(run_id.as_str().to_owned()),
            completion_round: Some(completion_round),
        };

        match milestone_service::record_planned_elsewhere_mapping(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            base_dir,
            &milestone_id,
            &mapping,
        ) {
            Ok(()) => {
                let _ = log_write.append_runtime_log(
                    base_dir,
                    project_id,
                    &RuntimeLogEntry {
                        timestamp: Utc::now(),
                        level: LogLevel::Info,
                        source: "engine".to_owned(),
                        message: format!(
                            "recorded planned-elsewhere mapping: amendment={} mapped_to={}",
                            amendment.queued.amendment_id, mapped_to
                        ),
                    },
                );
            }
            Err(e) => {
                let _ = log_write.append_runtime_log(
                    base_dir,
                    project_id,
                    &RuntimeLogEntry {
                        timestamp: Utc::now(),
                        level: LogLevel::Warn,
                        source: "engine".to_owned(),
                        message: format!(
                            "failed to record planned-elsewhere mapping for amendment={}: {e}",
                            amendment.queued.amendment_id
                        ),
                    },
                );
            }
        }
    }
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
        final_review_arbiter: None,
    }
}

/// Build a stage resolution snapshot for the final-review panel.
pub fn build_final_review_snapshot(
    stage_id: StageId,
    reviewers: &[crate::contexts::agent_execution::policy::ResolvedPanelMember],
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
        || old.final_review_arbiter != new.final_review_arbiter
}

/// Check whether a drifted resolution still satisfies the required-backend
/// and minimum-count constraints.
///
/// When `effective_min_override` is `Some`, it replaces the configured minimum
/// for completion/final-review panels. This is used on the resume path when
/// `BackendExhausted` members were already skipped and the effective quorum was
/// reduced. Prompt-review does not support quorum degradation and rejects any
/// override.
pub fn drift_still_satisfies_requirements(
    new_snapshot: &StageResolutionSnapshot,
    stage_id: StageId,
    effective_config: &EffectiveConfig,
    effective_min_override: Option<usize>,
) -> AppResult<()> {
    match stage_id {
        StageId::PromptReview => {
            if let Some(override_min) = effective_min_override {
                return Err(AppError::ResumeDriftFailure {
                    stage_id,
                    details: format!(
                        "prompt-review does not support effective_min_override ({override_min})"
                    ),
                });
            }
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
            let min = effective_min_override
                .unwrap_or_else(|| effective_config.completion_policy().min_completers);
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
            let min = effective_min_override
                .unwrap_or_else(|| effective_config.final_review_policy().min_reviewers);
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
    use std::collections::{HashMap, VecDeque};
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::process::Command;
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use serde_json::{json, Value};
    use tempfile::tempdir;

    use crate::adapters::fs::{
        FileSystem, FsArtifactStore, FsJournalStore, FsMilestoneControllerStore,
        FsMilestoneJournalStore, FsMilestonePlanStore, FsMilestoneSnapshotStore, FsMilestoneStore,
        FsPayloadArtifactWriteStore, FsProjectStore, FsRawOutputStore, FsRunSnapshotStore,
        FsRunSnapshotWriteStore, FsRuntimeLogWriteStore, FsSessionStore, RunPidOwner, RunPidRecord,
    };
    use crate::contexts::agent_execution::model::{
        InvocationContract, InvocationEnvelope, InvocationMetadata, InvocationRequest,
        RawOutputReference, TokenCounts,
    };
    use crate::contexts::agent_execution::policy::ResolvedPanelMember;
    use crate::contexts::agent_execution::{AgentExecutionPort, AgentExecutionService};
    use crate::contexts::milestone_record::bundle::{
        AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
    };
    use crate::contexts::milestone_record::controller::{
        self as milestone_controller, MilestoneControllerState,
    };
    use crate::contexts::milestone_record::service::{
        create_milestone, persist_plan, CreateMilestoneInput,
    };
    use crate::contexts::project_run_record::journal;
    use crate::contexts::project_run_record::model::{
        ActiveRun, IterativeImplementerLoopPolicy, IterativeImplementerState, JournalEventType,
        ProjectRecord, ProjectStatusSummary, RunSnapshot, RunStatus, TaskOrigin, TaskSource,
    };
    use crate::contexts::project_run_record::service::{
        create_project, CreateProjectInput, JournalStorePort, RunSnapshotPort, RunSnapshotWritePort,
    };
    use crate::contexts::workflow_composition::contracts;
    use crate::contexts::workflow_composition::panel_contracts::RecordProducer;
    use crate::contexts::workflow_composition::payloads::{
        ClassifiedFinding, ReviewOutcome, StagePayload, ValidationPayload,
    };
    use crate::contexts::workflow_composition::retry_policy::RetryPolicy;
    use crate::contexts::workflow_composition::review_classification::ReviewFindingClass;
    use crate::contexts::workspace_governance::{initialize_workspace, EffectiveConfig};
    use crate::shared::domain::{
        BackendFamily, FailureClass, FlowPreset, ProjectId, ResolvedBackendTarget, RunId,
        StageCursor, StageId,
    };
    use crate::shared::error::{AppError, AppResult};

    use super::{
        advance_iterative_loop_state, build_final_review_snapshot, build_prompt_review_snapshot,
        build_queued_amendments, complete_run, drift_still_satisfies_requirements,
        failed_invocation_id_for_stage, git_change_scope_fingerprint, git_diff_fingerprint,
        git_repo_available_with_program, has_deferred_classified_finding,
        has_restart_triggering_follow_up, invocation_id_for_stage, iterative_loop_exit_reason,
        mark_running_run_interrupted, milestone_lineage_plan_hash,
        partition_final_review_amendments_by_route, pause_run, preflight_check,
        probe_final_review_reviewers, resolution_has_drifted, resolve_runtime_final_review_panel,
        resolve_stage_plan, resume_iteration_counters, resume_run_with_retry,
        resume_terminal_iterative_stage_result, role_for_stage, should_retry_stage_failure,
        skip_next_apply_fixes_reason, stage_running_summary_for_active_run,
        sync_milestone_bead_start, validate_iterative_minimal_loop_settings, validation_follow_ups,
        FinalReviewQueuedAmendment, InterruptedRunContext, InterruptedRunUpdate,
        IterativeInvocationSidecar, IterativeLoopExitReason, QueuedAmendment,
        RunningAttemptIdentity, StagePlan, TerminalIterativeResumeResult,
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

    #[derive(Clone, Default)]
    struct ScriptedAvailabilityAdapter {
        scripted_failures: Arc<Mutex<HashMap<String, VecDeque<ScriptedAvailabilityFailure>>>>,
        availability_checks: Arc<Mutex<Vec<String>>>,
    }

    #[derive(Clone)]
    enum ScriptedAvailabilityFailure {
        BackendUnavailable {
            backend: &'static str,
            details: &'static str,
            failure_class: Option<FailureClass>,
        },
    }

    impl ScriptedAvailabilityFailure {
        fn into_error(self) -> AppError {
            match self {
                Self::BackendUnavailable {
                    backend,
                    details,
                    failure_class,
                } => AppError::BackendUnavailable {
                    backend: backend.to_owned(),
                    details: details.to_owned(),
                    failure_class,
                },
            }
        }
    }

    impl ScriptedAvailabilityAdapter {
        fn with_failures(entries: &[(&str, Vec<ScriptedAvailabilityFailure>)]) -> Self {
            Self {
                scripted_failures: Arc::new(Mutex::new(
                    entries
                        .iter()
                        .map(|(model_id, failures)| {
                            (
                                (*model_id).to_owned(),
                                failures.iter().cloned().collect::<VecDeque<_>>(),
                            )
                        })
                        .collect(),
                )),
                ..Default::default()
            }
        }

        fn availability_checks_for(&self, model_id: &str) -> usize {
            self.availability_checks
                .lock()
                .expect("availability checks lock poisoned")
                .iter()
                .filter(|seen| seen.as_str() == model_id)
                .count()
        }
    }

    impl AgentExecutionPort for ScriptedAvailabilityAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> AppResult<()> {
            Ok(())
        }

        async fn check_availability(&self, backend: &ResolvedBackendTarget) -> AppResult<()> {
            self.availability_checks
                .lock()
                .expect("availability checks lock poisoned")
                .push(backend.model.model_id.clone());

            let mut scripted_failures = self
                .scripted_failures
                .lock()
                .expect("scripted failures lock poisoned");
            let key = backend.model.model_id.clone();
            let Some(queue) = scripted_failures.get_mut(&key) else {
                return Ok(());
            };
            let Some(error) = queue.pop_front() else {
                return Ok(());
            };
            if queue.is_empty() {
                scripted_failures.remove(&key);
            }
            Err(error.into_error())
        }

        async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
            Ok(InvocationEnvelope {
                raw_output_reference: RawOutputReference::Inline(r#"{"status":"ok"}"#.to_owned()),
                parsed_payload: serde_json::json!({"status": "ok"}),
                metadata: InvocationMetadata {
                    invocation_id: request.invocation_id,
                    duration: std::time::Duration::ZERO,
                    token_counts: TokenCounts::default(),
                    backend_used: request.resolved_target.backend.clone(),
                    model_used: request.resolved_target.model.clone(),
                    adapter_reported_backend: None,
                    adapter_reported_model: None,
                    attempt_number: 0,
                    session_id: None,
                    session_reused: false,
                },
                timestamp: Utc::now(),
            })
        }

        async fn cancel(&self, _invocation_id: &str) -> AppResult<()> {
            Ok(())
        }
    }

    #[test]
    fn stage_retry_budget_stops_after_final_review_member_retry_exhaustion() {
        let retry_policy = RetryPolicy::default_policy().with_no_backoff();
        let error = AppError::InvocationFailed {
            backend: "codex".to_owned(),
            contract_id: "final_review:reviewer".to_owned(),
            failure_class: FailureClass::TransportFailure,
            details: "reviewer-1 (codex/gpt-5.5-xhigh) exhausted 5 transient retries: ERROR: stream disconnected before completion".to_owned(),
        };
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");

        assert!(!should_retry_stage_failure(
            &retry_policy,
            FailureClass::TransportFailure,
            &error,
            &cursor,
            &crate::contexts::agent_execution::model::CancellationToken::new(),
        ));
    }

    #[test]
    fn stage_retry_budget_allows_final_review_availability_retry_exhaustion() {
        let retry_policy = RetryPolicy::default_policy().with_no_backoff();
        let error = AppError::BackendUnavailable {
            backend: "codex".to_owned(),
            details: "reviewer-1 (codex/gpt-5.5-xhigh) exhausted 5 transient retries: stream disconnected before completion".to_owned(),
            failure_class: Some(FailureClass::TransportFailure),
        };
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");

        assert!(should_retry_stage_failure(
            &retry_policy,
            FailureClass::TransportFailure,
            &error,
            &cursor,
            &crate::contexts::agent_execution::model::CancellationToken::new(),
        ));
    }

    #[test]
    fn stage_retry_budget_stops_after_final_review_contract_failure() {
        let retry_policy = RetryPolicy::default_policy().with_no_backoff();
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let run_id = RunId::new("run-final-review-contract-failure").expect("run_id");

        for error in [
            AppError::InvocationFailed {
                backend: "codex".to_owned(),
                contract_id: "final_review:reviewer".to_owned(),
                failure_class: FailureClass::SchemaValidationFailure,
                details: "final-review proposal schema validation failed: missing field".to_owned(),
            },
            AppError::InvocationFailed {
                backend: "codex".to_owned(),
                contract_id: "final_review:arbiter".to_owned(),
                failure_class: FailureClass::DomainValidationFailure,
                details: "arbiter selected amendment id not present in disputed set".to_owned(),
            },
        ] {
            let failure_class = error.failure_class().expect("failure_class");
            let will_retry = should_retry_stage_failure(
                &retry_policy,
                failure_class,
                &error,
                &cursor,
                &crate::contexts::agent_execution::model::CancellationToken::new(),
            );
            assert!(
                !will_retry,
                "final_review contract failures must be terminal at the stage level: {error:?}"
            );

            let event = crate::contexts::project_run_record::journal::stage_failed_event(
                1,
                Utc::now(),
                &run_id,
                StageId::FinalReview,
                cursor.cycle,
                cursor.attempt,
                failure_class,
                &error.to_string(),
                will_retry,
                "final_review-1",
            );
            assert_eq!(
                event.details["will_retry"],
                serde_json::Value::Bool(false),
                "final_review contract failures must emit StageFailed.will_retry=false"
            );
        }
    }

    #[tokio::test]
    async fn final_review_probe_retries_transient_optional_reviewers_before_shrinking_panel() {
        let adapter = ScriptedAvailabilityAdapter::with_failures(&[(
            "reviewer-b",
            vec![ScriptedAvailabilityFailure::BackendUnavailable {
                backend: "codex",
                details: "stream disconnected before completion",
                failure_class: None,
            }],
        )]);
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);

        let (available_reviewers, exhausted_reviewers, effective_min_reviewers) =
            probe_final_review_reviewers(
                &agent_service,
                &final_review_reviewers(),
                2,
                crate::contexts::agent_execution::CancellationToken::new(),
            )
            .await
            .expect("transient reviewer probe failure should retry and recover");

        assert_eq!(available_reviewers.len(), 2);
        assert_eq!(exhausted_reviewers, 0);
        assert_eq!(effective_min_reviewers, 2);
        assert_eq!(adapter.availability_checks_for("reviewer-b"), 2);
    }

    #[tokio::test]
    async fn final_review_probe_keeps_optional_reviewers_after_transient_retry_exhaustion() {
        let adapter = ScriptedAvailabilityAdapter::with_failures(&[(
            "reviewer-b",
            (0..5)
                .map(|_| ScriptedAvailabilityFailure::BackendUnavailable {
                    backend: "codex",
                    details: "stream disconnected before completion",
                    failure_class: None,
                })
                .collect(),
        )]);
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);

        let (available_reviewers, exhausted_reviewers, effective_min_reviewers) =
            probe_final_review_reviewers(
                &agent_service,
                &final_review_reviewers(),
                2,
                crate::contexts::agent_execution::CancellationToken::new(),
            )
            .await
            .expect("transient probe exhaustion should not shrink the runtime panel");

        assert_eq!(available_reviewers.len(), 2);
        assert_eq!(exhausted_reviewers, 0);
        assert_eq!(effective_min_reviewers, 2);
        assert_eq!(adapter.availability_checks_for("reviewer-b"), 5);
    }

    #[tokio::test]
    async fn resolve_runtime_final_review_panel_retries_transient_arbiter_probe_failures() {
        let config = final_review_effective_config();
        let policy = crate::contexts::agent_execution::policy::BackendPolicyService::new(&config);
        let resolved_panel = policy
            .resolve_final_review_panel(1)
            .expect("resolve final-review panel");
        let arbiter_model = resolved_panel.arbiter.model.model_id.clone();

        let adapter = ScriptedAvailabilityAdapter::with_failures(&[(
            arbiter_model.as_str(),
            vec![ScriptedAvailabilityFailure::BackendUnavailable {
                backend: resolved_panel.arbiter.backend.family.as_str(),
                details: "stream disconnected before completion",
                failure_class: None,
            }],
        )]);
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);

        let runtime_panel = resolve_runtime_final_review_panel(
            &agent_service,
            &config,
            1,
            crate::contexts::agent_execution::CancellationToken::new(),
        )
        .await
        .expect("transient arbiter probe failure should retry and recover");

        assert_eq!(
            runtime_panel.panel.arbiter.model.model_id,
            resolved_panel.arbiter.model.model_id
        );
        assert!(
            adapter.availability_checks_for(arbiter_model.as_str()) >= 2,
            "arbiter availability should be rechecked after the transient failure"
        );
    }

    #[tokio::test]
    async fn preflight_check_retries_transient_final_review_arbiter_probe_failures() {
        let config = final_review_effective_config();
        let resolver = crate::contexts::agent_execution::service::BackendResolver::new();
        let plan = resolve_stage_plan(&[StageId::FinalReview], &resolver, None)
            .expect("resolve final-review stage plan");
        let policy = crate::contexts::agent_execution::policy::BackendPolicyService::new(&config);
        let resolved_panel = policy
            .resolve_final_review_panel(1)
            .expect("resolve final-review panel");
        let arbiter_model = resolved_panel.arbiter.model.model_id.clone();

        let adapter = ScriptedAvailabilityAdapter::with_failures(&[(
            arbiter_model.as_str(),
            vec![ScriptedAvailabilityFailure::BackendUnavailable {
                backend: resolved_panel.arbiter.backend.family.as_str(),
                details: "stream disconnected before completion",
                failure_class: None,
            }],
        )]);

        preflight_check(&adapter, &config, 1, &plan)
            .await
            .expect("preflight should retry a transient final-review arbiter probe failure");
        assert!(
            adapter.availability_checks_for(arbiter_model.as_str()) >= 2,
            "preflight should recheck the arbiter after a transient availability failure"
        );
    }

    #[tokio::test]
    async fn preflight_check_retries_transient_final_review_arbiter_rate_limit_probe_failures() {
        let config = final_review_effective_config();
        let resolver = crate::contexts::agent_execution::service::BackendResolver::new();
        let plan = resolve_stage_plan(&[StageId::FinalReview], &resolver, None)
            .expect("resolve final-review stage plan");
        let policy = crate::contexts::agent_execution::policy::BackendPolicyService::new(&config);
        let resolved_panel = policy
            .resolve_final_review_panel(1)
            .expect("resolve final-review panel");
        let arbiter_model = resolved_panel.arbiter.model.model_id.clone();

        let adapter = ScriptedAvailabilityAdapter::with_failures(&[(
            arbiter_model.as_str(),
            vec![ScriptedAvailabilityFailure::BackendUnavailable {
                backend: resolved_panel.arbiter.backend.family.as_str(),
                details: "HTTP 429: Too Many Requests",
                failure_class: None,
            }],
        )]);

        preflight_check(&adapter, &config, 1, &plan)
            .await
            .expect("preflight should retry a transient final-review arbiter 429 probe failure");
        assert!(
            adapter.availability_checks_for(arbiter_model.as_str()) >= 2,
            "preflight should recheck the arbiter after a transient 429 availability failure"
        );
    }

    #[tokio::test]
    async fn preflight_check_keeps_optional_final_review_reviewers_after_transient_probe_exhaustion(
    ) {
        let config = final_review_effective_config();
        let resolver = crate::contexts::agent_execution::service::BackendResolver::new();
        let plan = resolve_stage_plan(&[StageId::FinalReview], &resolver, None)
            .expect("resolve final-review stage plan");
        let policy = crate::contexts::agent_execution::policy::BackendPolicyService::new(&config);
        let resolved_panel = policy
            .resolve_final_review_panel(1)
            .expect("resolve final-review panel");
        let optional_reviewer = resolved_panel
            .reviewers
            .iter()
            .find(|member| !member.required)
            .expect("expected optional final-review reviewer");

        let adapter = ScriptedAvailabilityAdapter::with_failures(&[(
            optional_reviewer.target.model.model_id.as_str(),
            (0..5)
                .map(|_| ScriptedAvailabilityFailure::BackendUnavailable {
                    backend: optional_reviewer.target.backend.family.as_str(),
                    details: "stream disconnected before completion",
                    failure_class: None,
                })
                .collect(),
        )]);

        preflight_check(&adapter, &config, 1, &plan)
            .await
            .expect("preflight should preserve optional final-review reviewers after transient probe exhaustion");
        assert_eq!(
            adapter.availability_checks_for(optional_reviewer.target.model.model_id.as_str()),
            5,
            "preflight should consume the configured final-review retry budget before preserving the optional reviewer"
        );
    }

    fn final_review_effective_config() -> EffectiveConfig {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");
        EffectiveConfig::load(temp_dir.path()).expect("load effective config")
    }

    fn run_git(repo_root: &Path, args: &[&str]) {
        let output = Command::new("git")
            .args(args)
            .current_dir(repo_root)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git {:?} failed: stdout={} stderr={}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn init_git_repo(repo_root: &Path) {
        run_git(repo_root, &["init"]);
        run_git(repo_root, &["config", "user.name", "Test User"]);
        run_git(repo_root, &["config", "user.email", "test@example.com"]);
        fs::write(repo_root.join("tracked.txt"), "baseline\n").expect("write tracked file");
        run_git(repo_root, &["add", "tracked.txt"]);
        run_git(repo_root, &["commit", "-m", "initial"]);
    }

    #[test]
    fn iterative_loop_state_stops_on_stability_or_max_rounds() {
        assert_eq!(advance_iterative_loop_state(0, 1, false, 2, 10), (1, None));
        assert_eq!(
            advance_iterative_loop_state(1, 2, false, 2, 10),
            (2, Some(IterativeLoopExitReason::Stable))
        );
        assert_eq!(
            advance_iterative_loop_state(0, 10, true, 2, 10),
            (0, Some(IterativeLoopExitReason::MaxRounds))
        );
        assert_eq!(advance_iterative_loop_state(1, 3, true, 2, 10), (0, None));
    }

    #[test]
    fn iterative_loop_exit_reason_detects_terminal_resume_state() {
        assert_eq!(
            iterative_loop_exit_reason(2, 2, 2, 10),
            Some(IterativeLoopExitReason::Stable)
        );
        assert_eq!(
            iterative_loop_exit_reason(0, 10, 2, 10),
            Some(IterativeLoopExitReason::MaxRounds)
        );
        assert_eq!(iterative_loop_exit_reason(1, 2, 2, 10), None);
    }

    #[test]
    fn iterative_loop_settings_reject_unreachable_stability_threshold() {
        let error = validate_iterative_minimal_loop_settings(5, 6)
            .expect_err("stable rounds above max rounds must be rejected");
        match error {
            AppError::InvalidConfigValue { key, value, reason } => {
                assert_eq!(key, "workflow.iterative_minimal.stable_rounds_required");
                assert_eq!(value, "6");
                assert!(reason.contains("max_consecutive_implementer_rounds (5)"));
            }
            other => panic!("expected InvalidConfigValue, got {other:?}"),
        }
    }

    #[test]
    fn resume_terminal_iterative_stage_result_recovers_last_iteration_output() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");

        let project_id = ProjectId::new("iter-terminal").expect("project id");
        let prompt_contents = "# Test prompt";
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            temp_dir.path(),
            CreateProjectInput {
                id: project_id.clone(),
                name: "Iterative terminal recovery".to_owned(),
                flow: FlowPreset::IterativeMinimal,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: prompt_contents.to_owned(),
                prompt_hash: FileSystem::prompt_hash(prompt_contents),
                created_at: Utc::now(),
                task_source: None,
            },
        )
        .expect("create project");

        let run_id = RunId::new("run-iter-terminal").expect("run id");
        let cursor = StageCursor::new(StageId::PlanAndImplement, 1, 1, 1).expect("cursor");
        let resolved_target = ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.5");
        let stage_entry = StagePlan {
            stage_id: StageId::PlanAndImplement,
            role: role_for_stage(StageId::PlanAndImplement),
            contract: contracts::contract_for_stage(StageId::PlanAndImplement),
            target: resolved_target.clone(),
        };
        let project_root = FileSystem::project_root(temp_dir.path(), &project_id);
        let invocation_id =
            invocation_id_for_stage(&run_id, StageId::PlanAndImplement, &cursor, Some("it2"));
        let raw_output_path = project_root
            .join("runtime/backend")
            .join(format!("{invocation_id}.raw"));
        let parsed_output_path = project_root
            .join("runtime/backend")
            .join(format!("{invocation_id}.parsed.json"));
        let payload = json!({
            "change_summary": "Recovered iterative execution output",
            "steps": [
                {
                    "order": 1,
                    "description": "Resume from the terminal iteration",
                    "status": "completed"
                }
            ],
            "validation_evidence": ["recovered from persisted parsed payload"],
            "outstanding_risks": []
        });
        fs::write(
            &raw_output_path,
            serde_json::to_string(&payload).expect("serialize raw payload"),
        )
        .expect("write raw output");
        fs::write(
            &parsed_output_path,
            serde_json::to_string(&payload).expect("serialize parsed payload"),
        )
        .expect("write parsed output");

        let mut seq = FsJournalStore
            .read_journal(temp_dir.path(), &project_id)
            .expect("read existing journal")
            .last()
            .map(|event| event.sequence)
            .unwrap_or(0);
        let recovered = resume_terminal_iterative_stage_result(
            &FsJournalStore,
            temp_dir.path(),
            &project_root,
            &project_id,
            &run_id,
            &mut seq,
            &stage_entry,
            &cursor,
            &resolved_target,
            2,
            2,
            2,
            10,
        )
        .expect("recover terminal result");

        match recovered {
            TerminalIterativeResumeResult::Recovered(recovered) => {
                let (bundle, _producer) = *recovered;
                match bundle.payload {
                    StagePayload::Execution(payload) => {
                        assert_eq!(
                            payload.change_summary,
                            "Recovered iterative execution output"
                        );
                    }
                    other => panic!("expected execution payload, got {other:?}"),
                }
            }
            other => panic!("expected recovered terminal result, got {other:?}"),
        }

        let events = FsJournalStore
            .read_journal(temp_dir.path(), &project_id)
            .expect("read journal");
        let exited_events: Vec<_> = events
            .iter()
            .filter(|event| event.event_type == JournalEventType::ImplementerLoopExited)
            .collect();
        assert_eq!(exited_events.len(), 1);
        assert_eq!(exited_events[0].details["reason"], "stable");
        assert_eq!(exited_events[0].details["total_iterations"], 2);
    }

    #[test]
    fn resume_terminal_iterative_stage_result_recovers_original_producer_metadata() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");

        let project_id = ProjectId::new("iter-terminal-producer").expect("project id");
        let prompt_contents = "# Test prompt";
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            temp_dir.path(),
            CreateProjectInput {
                id: project_id.clone(),
                name: "Iterative terminal recovery producer metadata".to_owned(),
                flow: FlowPreset::IterativeMinimal,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: prompt_contents.to_owned(),
                prompt_hash: FileSystem::prompt_hash(prompt_contents),
                created_at: Utc::now(),
                task_source: None,
            },
        )
        .expect("create project");

        let run_id = RunId::new("run-iter-terminal-producer").expect("run id");
        let cursor = StageCursor::new(StageId::PlanAndImplement, 1, 1, 1).expect("cursor");
        let stage_entry = StagePlan {
            stage_id: StageId::PlanAndImplement,
            role: role_for_stage(StageId::PlanAndImplement),
            contract: contracts::contract_for_stage(StageId::PlanAndImplement),
            target: ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.5"),
        };
        let project_root = FileSystem::project_root(temp_dir.path(), &project_id);
        let invocation_id =
            invocation_id_for_stage(&run_id, StageId::PlanAndImplement, &cursor, Some("it2"));
        let raw_output_path = project_root
            .join("runtime/backend")
            .join(format!("{invocation_id}.raw"));
        let parsed_output_path = project_root
            .join("runtime/backend")
            .join(format!("{invocation_id}.parsed.json"));
        let payload = json!({
            "change_summary": "Recovered iterative execution output",
            "steps": [
                {
                    "order": 1,
                    "description": "Resume from the terminal iteration",
                    "status": "completed"
                }
            ],
            "validation_evidence": ["recovered producer metadata"],
            "outstanding_risks": []
        });
        let producer = RecordProducer::Agent {
            requested_backend_family: "codex".to_owned(),
            requested_model_id: "gpt-5.5".to_owned(),
            actual_backend_family: "openrouter".to_owned(),
            actual_model_id: "openai/gpt-4.1".to_owned(),
        };
        let sidecar = IterativeInvocationSidecar {
            parsed_payload: payload.clone(),
            producer: producer.clone(),
        };
        fs::write(
            &raw_output_path,
            serde_json::to_string(&payload).expect("serialize raw payload"),
        )
        .expect("write raw output");
        fs::write(
            &parsed_output_path,
            serde_json::to_string(&sidecar).expect("serialize parsed sidecar"),
        )
        .expect("write parsed sidecar");

        let mut seq = FsJournalStore
            .read_journal(temp_dir.path(), &project_id)
            .expect("read existing journal")
            .last()
            .map(|event| event.sequence)
            .unwrap_or(0);
        let recovered = resume_terminal_iterative_stage_result(
            &FsJournalStore,
            temp_dir.path(),
            &project_root,
            &project_id,
            &run_id,
            &mut seq,
            &stage_entry,
            &cursor,
            &ResolvedBackendTarget::new(BackendFamily::Claude, "claude-opus-4-7"),
            2,
            2,
            2,
            10,
        )
        .expect("recover terminal result");

        match recovered {
            TerminalIterativeResumeResult::Recovered(recovered) => {
                let (_bundle, recovered_producer) = *recovered;
                assert_eq!(recovered_producer, producer);
            }
            other => panic!("expected recovered terminal result, got {other:?}"),
        }
    }

    #[test]
    fn resume_terminal_iterative_stage_result_recovers_from_raw_output_when_sidecar_is_missing() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");

        let project_id = ProjectId::new("iter-terminal-raw-fallback").expect("project id");
        let prompt_contents = "# Test prompt";
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            temp_dir.path(),
            CreateProjectInput {
                id: project_id.clone(),
                name: "Iterative terminal recovery raw fallback".to_owned(),
                flow: FlowPreset::IterativeMinimal,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: prompt_contents.to_owned(),
                prompt_hash: FileSystem::prompt_hash(prompt_contents),
                created_at: Utc::now(),
                task_source: None,
            },
        )
        .expect("create project");

        let run_id = RunId::new("run-iter-terminal-raw-fallback").expect("run id");
        let cursor = StageCursor::new(StageId::PlanAndImplement, 1, 1, 1).expect("cursor");
        let resolved_target = ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.5");
        let stage_entry = StagePlan {
            stage_id: StageId::PlanAndImplement,
            role: role_for_stage(StageId::PlanAndImplement),
            contract: contracts::contract_for_stage(StageId::PlanAndImplement),
            target: resolved_target.clone(),
        };
        let project_root = FileSystem::project_root(temp_dir.path(), &project_id);
        let invocation_id =
            invocation_id_for_stage(&run_id, StageId::PlanAndImplement, &cursor, Some("it2"));
        let raw_output_path = project_root
            .join("runtime/backend")
            .join(format!("{invocation_id}.raw"));
        let payload = json!({
            "change_summary": "Recovered iterative execution output from raw",
            "steps": [
                {
                    "order": 1,
                    "description": "Resume from the terminal iteration raw payload",
                    "status": "completed"
                }
            ],
            "validation_evidence": ["recovered from raw output"],
            "outstanding_risks": []
        });
        fs::write(
            &raw_output_path,
            serde_json::to_string(&payload).expect("serialize raw payload"),
        )
        .expect("write raw output");

        let mut seq = FsJournalStore
            .read_journal(temp_dir.path(), &project_id)
            .expect("read existing journal")
            .last()
            .map(|event| event.sequence)
            .unwrap_or(0);
        let recovered = resume_terminal_iterative_stage_result(
            &FsJournalStore,
            temp_dir.path(),
            &project_root,
            &project_id,
            &run_id,
            &mut seq,
            &stage_entry,
            &cursor,
            &resolved_target,
            2,
            2,
            2,
            10,
        )
        .expect("recover terminal result from raw output");

        match recovered {
            TerminalIterativeResumeResult::Recovered(recovered) => {
                let (bundle, producer) = *recovered;
                match bundle.payload {
                    StagePayload::Execution(payload) => {
                        assert_eq!(
                            payload.change_summary,
                            "Recovered iterative execution output from raw"
                        );
                    }
                    other => panic!("expected execution payload, got {other:?}"),
                }
                assert_eq!(
                    producer,
                    RecordProducer::Agent {
                        requested_backend_family: "codex".to_owned(),
                        requested_model_id: "gpt-5.5".to_owned(),
                        actual_backend_family: "codex".to_owned(),
                        actual_model_id: "gpt-5.5".to_owned(),
                    }
                );
            }
            other => panic!("expected recovered terminal result, got {other:?}"),
        }
    }

    #[test]
    fn resume_terminal_iterative_stage_result_recovers_from_claude_raw_envelope_when_sidecar_is_missing(
    ) {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");

        let project_id = ProjectId::new("iter-terminal-claude-raw").expect("project id");
        let prompt_contents = "# Test prompt";
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            temp_dir.path(),
            CreateProjectInput {
                id: project_id.clone(),
                name: "Iterative terminal recovery Claude raw fallback".to_owned(),
                flow: FlowPreset::IterativeMinimal,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: prompt_contents.to_owned(),
                prompt_hash: FileSystem::prompt_hash(prompt_contents),
                created_at: Utc::now(),
                task_source: None,
            },
        )
        .expect("create project");

        let run_id = RunId::new("run-iter-terminal-claude-raw").expect("run id");
        let cursor = StageCursor::new(StageId::PlanAndImplement, 1, 1, 1).expect("cursor");
        let resolved_target = ResolvedBackendTarget::new(BackendFamily::Claude, "claude-opus");
        let stage_entry = StagePlan {
            stage_id: StageId::PlanAndImplement,
            role: role_for_stage(StageId::PlanAndImplement),
            contract: contracts::contract_for_stage(StageId::PlanAndImplement),
            target: resolved_target.clone(),
        };
        let project_root = FileSystem::project_root(temp_dir.path(), &project_id);
        let invocation_id =
            invocation_id_for_stage(&run_id, StageId::PlanAndImplement, &cursor, Some("it2"));
        let raw_output_path = project_root
            .join("runtime/backend")
            .join(format!("{invocation_id}.raw"));
        let payload = json!({
            "change_summary": "Recovered iterative execution output from Claude envelope",
            "steps": [
                {
                    "order": 1,
                    "description": "Resume from Claude structured output",
                    "status": "completed"
                }
            ],
            "validation_evidence": ["recovered from raw Claude transcript"],
            "outstanding_risks": []
        });
        let raw_transcript = json!({
            "type": "result",
            "result": "assistant prose that is not the payload",
            "session_id": "sess-terminal",
            "structured_output": {
                "data": payload
            }
        });
        fs::write(
            &raw_output_path,
            serde_json::to_string(&raw_transcript).expect("serialize raw transcript"),
        )
        .expect("write raw output");

        let mut seq = FsJournalStore
            .read_journal(temp_dir.path(), &project_id)
            .expect("read existing journal")
            .last()
            .map(|event| event.sequence)
            .unwrap_or(0);
        let recovered = resume_terminal_iterative_stage_result(
            &FsJournalStore,
            temp_dir.path(),
            &project_root,
            &project_id,
            &run_id,
            &mut seq,
            &stage_entry,
            &cursor,
            &resolved_target,
            2,
            2,
            2,
            10,
        )
        .expect("recover terminal result from Claude raw transcript");

        match recovered {
            TerminalIterativeResumeResult::Recovered(recovered) => {
                let (bundle, producer) = *recovered;
                match bundle.payload {
                    StagePayload::Execution(payload) => {
                        assert_eq!(
                            payload.change_summary,
                            "Recovered iterative execution output from Claude envelope"
                        );
                    }
                    other => panic!("expected execution payload, got {other:?}"),
                }
                assert_eq!(
                    producer,
                    RecordProducer::Agent {
                        requested_backend_family: "claude".to_owned(),
                        requested_model_id: "claude-opus".to_owned(),
                        actual_backend_family: "claude".to_owned(),
                        actual_model_id: "claude-opus".to_owned(),
                    }
                );
            }
            other => panic!("expected recovered terminal result, got {other:?}"),
        }
    }

    #[test]
    fn resume_terminal_iterative_stage_result_recovers_from_codex_raw_envelope_when_sidecar_is_missing(
    ) {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");

        let project_id = ProjectId::new("iter-terminal-codex-raw").expect("project id");
        let prompt_contents = "# Test prompt";
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            temp_dir.path(),
            CreateProjectInput {
                id: project_id.clone(),
                name: "Iterative terminal recovery from Codex raw transcript".to_owned(),
                flow: FlowPreset::IterativeMinimal,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: prompt_contents.to_owned(),
                prompt_hash: FileSystem::prompt_hash(prompt_contents),
                created_at: Utc::now(),
                task_source: None,
            },
        )
        .expect("create project");

        let run_id = RunId::new("run-iter-terminal-codex-raw").expect("run id");
        let cursor = StageCursor::new(StageId::PlanAndImplement, 1, 1, 1).expect("cursor");
        let resolved_target = ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.5");
        let stage_entry = StagePlan {
            stage_id: StageId::PlanAndImplement,
            role: role_for_stage(StageId::PlanAndImplement),
            contract: contracts::contract_for_stage(StageId::PlanAndImplement),
            target: resolved_target.clone(),
        };
        let project_root = FileSystem::project_root(temp_dir.path(), &project_id);
        let invocation_id =
            invocation_id_for_stage(&run_id, StageId::PlanAndImplement, &cursor, Some("it2"));
        let raw_output_path = project_root
            .join("runtime/backend")
            .join(format!("{invocation_id}.raw"));
        let payload = json!({
            "change_summary": "Recovered iterative execution output from Codex transcript",
            "steps": [
                {
                    "order": 1,
                    "description": "Resume from Codex last-message transcript",
                    "status": "completed"
                }
            ],
            "validation_evidence": ["recovered from raw Codex transcript"],
            "outstanding_risks": []
        });
        let raw_transcript = json!({
            "transport": "rb_codex_process_v1",
            "stdout": "{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":12}}",
            "last_message": payload.to_string(),
        });
        fs::write(
            &raw_output_path,
            serde_json::to_string(&raw_transcript).expect("serialize raw transcript"),
        )
        .expect("write raw output");

        let mut seq = FsJournalStore
            .read_journal(temp_dir.path(), &project_id)
            .expect("read existing journal")
            .last()
            .map(|event| event.sequence)
            .unwrap_or(0);
        let recovered = resume_terminal_iterative_stage_result(
            &FsJournalStore,
            temp_dir.path(),
            &project_root,
            &project_id,
            &run_id,
            &mut seq,
            &stage_entry,
            &cursor,
            &resolved_target,
            2,
            2,
            2,
            10,
        )
        .expect("recover terminal result from Codex raw transcript");

        match recovered {
            TerminalIterativeResumeResult::Recovered(recovered) => {
                let (bundle, producer) = *recovered;
                match bundle.payload {
                    StagePayload::Execution(payload) => {
                        assert_eq!(
                            payload.change_summary,
                            "Recovered iterative execution output from Codex transcript"
                        );
                    }
                    other => panic!("expected execution payload, got {other:?}"),
                }
                assert_eq!(
                    producer,
                    RecordProducer::Agent {
                        requested_backend_family: "codex".to_owned(),
                        requested_model_id: "gpt-5.5".to_owned(),
                        actual_backend_family: "codex".to_owned(),
                        actual_model_id: "gpt-5.5".to_owned(),
                    }
                );
            }
            other => panic!("expected recovered terminal result, got {other:?}"),
        }
    }

    #[test]
    fn resume_terminal_iterative_stage_result_recovers_from_direct_openrouter_http_response_when_sidecar_is_missing(
    ) {
        // Round 3 amendment regression: when an iterative_minimal terminal
        // resume is missing the parsed sidecar AND the backend is the
        // direct `OpenRouterBackendAdapter` (raw output is an
        // OpenRouter chat-completions HTTP response body, NOT a codex
        // transport envelope), the engine must recover via
        // `recover_structured_payload_from_response_body` rather than
        // routing the response through the codex synth helper, which
        // would return the entire response JSON instead of extracting
        // `choices[0].message.content`.
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");

        let project_id = ProjectId::new("iter-terminal-or-http").expect("project id");
        let prompt_contents = "# Test prompt";
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            temp_dir.path(),
            CreateProjectInput {
                id: project_id.clone(),
                name: "Iterative terminal recovery from direct OpenRouter HTTP".to_owned(),
                flow: FlowPreset::IterativeMinimal,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: prompt_contents.to_owned(),
                prompt_hash: FileSystem::prompt_hash(prompt_contents),
                created_at: Utc::now(),
                task_source: None,
            },
        )
        .expect("create project");

        let run_id = RunId::new("run-iter-terminal-or-http").expect("run id");
        let cursor = StageCursor::new(StageId::PlanAndImplement, 1, 1, 1).expect("cursor");
        let resolved_target =
            ResolvedBackendTarget::new(BackendFamily::OpenRouter, "openai/gpt-4o");
        let stage_entry = StagePlan {
            stage_id: StageId::PlanAndImplement,
            role: role_for_stage(StageId::PlanAndImplement),
            contract: contracts::contract_for_stage(StageId::PlanAndImplement),
            target: resolved_target.clone(),
        };
        let project_root = FileSystem::project_root(temp_dir.path(), &project_id);
        let invocation_id =
            invocation_id_for_stage(&run_id, StageId::PlanAndImplement, &cursor, Some("it2"));
        let raw_output_path = project_root
            .join("runtime/backend")
            .join(format!("{invocation_id}.raw"));
        let payload_json = json!({
            "change_summary": "Recovered iterative execution output from OpenRouter HTTP response",
            "steps": [
                {
                    "order": 1,
                    "description": "Resume from direct OpenRouter chat-completions response",
                    "status": "completed"
                }
            ],
            "validation_evidence": ["recovered from raw OpenRouter HTTP response"],
            "outstanding_risks": []
        });
        // Direct OpenRouter HTTP response shape (no codex transport
        // envelope). The structured payload is JSON-encoded inside
        // `choices[0].message.content`.
        let raw_response = json!({
            "id": "chatcmpl-or-http",
            "model": "openai/gpt-4o",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": serde_json::to_string(&payload_json)
                        .expect("serialize content")
                },
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 50, "completion_tokens": 20, "total_tokens": 70}
        });
        fs::write(
            &raw_output_path,
            serde_json::to_string(&raw_response).expect("serialize raw response"),
        )
        .expect("write raw output");

        let mut seq = FsJournalStore
            .read_journal(temp_dir.path(), &project_id)
            .expect("read existing journal")
            .last()
            .map(|event| event.sequence)
            .unwrap_or(0);
        let recovered = resume_terminal_iterative_stage_result(
            &FsJournalStore,
            temp_dir.path(),
            &project_root,
            &project_id,
            &run_id,
            &mut seq,
            &stage_entry,
            &cursor,
            &resolved_target,
            2,
            2,
            2,
            10,
        )
        .expect("recover terminal result from direct OpenRouter HTTP response");

        match recovered {
            TerminalIterativeResumeResult::Recovered(recovered) => {
                let (bundle, producer) = *recovered;
                match bundle.payload {
                    StagePayload::Execution(payload) => {
                        assert_eq!(
                            payload.change_summary,
                            "Recovered iterative execution output from OpenRouter HTTP response",
                            "expected the inner payload's change_summary, NOT the entire \
                             chat-completions response — round 3 amendment regression"
                        );
                    }
                    other => panic!("expected execution payload, got {other:?}"),
                }
                assert_eq!(
                    producer,
                    RecordProducer::Agent {
                        requested_backend_family: "openrouter".to_owned(),
                        requested_model_id: "openai/gpt-4o".to_owned(),
                        actual_backend_family: "openrouter".to_owned(),
                        actual_model_id: "openai/gpt-4o".to_owned(),
                    }
                );
            }
            other => panic!("expected recovered terminal result, got {other:?}"),
        }
    }

    #[test]
    fn resume_terminal_iterative_stage_result_records_exit_for_current_run_only() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");

        let project_id = ProjectId::new("iter-terminal-run-scope").expect("project id");
        let prompt_contents = "# Test prompt";
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            temp_dir.path(),
            CreateProjectInput {
                id: project_id.clone(),
                name: "Iterative terminal recovery run scope".to_owned(),
                flow: FlowPreset::IterativeMinimal,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: prompt_contents.to_owned(),
                prompt_hash: FileSystem::prompt_hash(prompt_contents),
                created_at: Utc::now(),
                task_source: None,
            },
        )
        .expect("create project");

        let old_run_id = RunId::new("run-iter-old").expect("old run id");
        let run_id = RunId::new("run-iter-new").expect("new run id");
        let cursor = StageCursor::new(StageId::PlanAndImplement, 1, 1, 1).expect("cursor");
        let resolved_target = ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.5");
        let stage_entry = StagePlan {
            stage_id: StageId::PlanAndImplement,
            role: role_for_stage(StageId::PlanAndImplement),
            contract: contracts::contract_for_stage(StageId::PlanAndImplement),
            target: resolved_target.clone(),
        };
        let project_root = FileSystem::project_root(temp_dir.path(), &project_id);

        let mut seq = FsJournalStore
            .read_journal(temp_dir.path(), &project_id)
            .expect("read existing journal")
            .last()
            .map(|event| event.sequence)
            .unwrap_or(0);
        seq += 1;
        let prior_exit = journal::implementer_loop_exited_event(
            seq,
            Utc::now(),
            &old_run_id,
            StageId::PlanAndImplement,
            cursor.cycle,
            cursor.attempt,
            cursor.completion_round,
            "stable",
            2,
        );
        let prior_exit_line = journal::serialize_event(&prior_exit).expect("serialize prior exit");
        FsJournalStore
            .append_event(temp_dir.path(), &project_id, &prior_exit_line)
            .expect("append prior exit");

        let invocation_id =
            invocation_id_for_stage(&run_id, StageId::PlanAndImplement, &cursor, Some("it2"));
        let raw_output_path = project_root
            .join("runtime/backend")
            .join(format!("{invocation_id}.raw"));
        let parsed_output_path = project_root
            .join("runtime/backend")
            .join(format!("{invocation_id}.parsed.json"));
        let payload = json!({
            "change_summary": "Recovered iterative execution output",
            "steps": [
                {
                    "order": 1,
                    "description": "Resume from the terminal iteration",
                    "status": "completed"
                }
            ],
            "validation_evidence": ["recovered for current run"],
            "outstanding_risks": []
        });
        fs::write(
            &raw_output_path,
            serde_json::to_string(&payload).expect("serialize raw payload"),
        )
        .expect("write raw output");
        fs::write(
            &parsed_output_path,
            serde_json::to_string(&payload).expect("serialize parsed payload"),
        )
        .expect("write parsed output");

        let recovered = resume_terminal_iterative_stage_result(
            &FsJournalStore,
            temp_dir.path(),
            &project_root,
            &project_id,
            &run_id,
            &mut seq,
            &stage_entry,
            &cursor,
            &resolved_target,
            2,
            2,
            2,
            10,
        )
        .expect("recover terminal result");

        match recovered {
            TerminalIterativeResumeResult::Recovered(recovered) => {
                let (bundle, _producer) = *recovered;
                match bundle.payload {
                    StagePayload::Execution(payload) => {
                        assert_eq!(
                            payload.change_summary,
                            "Recovered iterative execution output"
                        );
                    }
                    other => panic!("expected execution payload, got {other:?}"),
                }
            }
            other => panic!("expected recovered terminal result, got {other:?}"),
        }

        let events = FsJournalStore
            .read_journal(temp_dir.path(), &project_id)
            .expect("read journal");
        let exited_events: Vec<_> = events
            .iter()
            .filter(|event| event.event_type == JournalEventType::ImplementerLoopExited)
            .collect();
        assert_eq!(exited_events.len(), 2);
        assert_eq!(exited_events[0].details["run_id"], old_run_id.as_str());
        assert_eq!(exited_events[1].details["run_id"], run_id.as_str());
    }

    #[test]
    fn resume_terminal_iterative_stage_result_fails_when_terminal_output_is_not_recoverable() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");

        let project_id = ProjectId::new("iter-terminal-missing-output").expect("project id");
        let prompt_contents = "# Test prompt";
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            temp_dir.path(),
            CreateProjectInput {
                id: project_id.clone(),
                name: "Iterative terminal recovery missing output".to_owned(),
                flow: FlowPreset::IterativeMinimal,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: prompt_contents.to_owned(),
                prompt_hash: FileSystem::prompt_hash(prompt_contents),
                created_at: Utc::now(),
                task_source: None,
            },
        )
        .expect("create project");

        let run_id = RunId::new("run-iter-terminal-rewind").expect("run id");
        let cursor = StageCursor::new(StageId::PlanAndImplement, 1, 1, 1).expect("cursor");
        let resolved_target = ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.5");
        let stage_entry = StagePlan {
            stage_id: StageId::PlanAndImplement,
            role: role_for_stage(StageId::PlanAndImplement),
            contract: contracts::contract_for_stage(StageId::PlanAndImplement),
            target: resolved_target,
        };

        let mut seq = FsJournalStore
            .read_journal(temp_dir.path(), &project_id)
            .expect("read existing journal")
            .last()
            .map(|event| event.sequence)
            .unwrap_or(0);
        for (iteration, diff_changed) in [(1, true), (2, false)] {
            seq += 1;
            let completed = journal::implementer_iteration_completed_event(
                seq,
                Utc::now(),
                &run_id,
                StageId::PlanAndImplement,
                cursor.cycle,
                cursor.attempt,
                cursor.completion_round,
                iteration,
                diff_changed,
                "completed",
            );
            let line = journal::serialize_event(&completed).expect("serialize completed event");
            FsJournalStore
                .append_event(temp_dir.path(), &project_id, &line)
                .expect("append completed event");
        }

        let result = resume_terminal_iterative_stage_result(
            &FsJournalStore,
            temp_dir.path(),
            &FileSystem::project_root(temp_dir.path(), &project_id),
            &project_id,
            &run_id,
            &mut seq,
            &stage_entry,
            &cursor,
            &ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.5"),
            2,
            1,
            1,
            10,
        )
        .expect_err("resume should fail when terminal output cannot be recovered safely");

        match result {
            AppError::StageCommitFailed { stage_id, details } => {
                assert_eq!(stage_id, StageId::PlanAndImplement);
                assert!(
                    details.contains(
                        "unable to recover terminal iterative_minimal iteration 2 safely during resume"
                    ),
                    "error should explain why replay is refused: {details}"
                );
                assert!(
                    details.contains("refusing to re-invoke on the post-iteration workspace"),
                    "error should make the safety guard explicit: {details}"
                );
            }
            other => panic!("expected StageCommitFailed, got {other:?}"),
        }

        let events = FsJournalStore
            .read_journal(temp_dir.path(), &project_id)
            .expect("read journal");
        assert!(
            events
                .iter()
                .all(|event| event.event_type != JournalEventType::ImplementerLoopExited),
            "resume fallback must not append a clean loop exit event before recovery is possible"
        );
    }

    #[test]
    fn git_diff_fingerprint_ignores_ralph_burning_state_but_detects_repo_changes() {
        let temp_dir = tempdir().expect("create temp dir");
        init_git_repo(temp_dir.path());

        let baseline = git_diff_fingerprint(temp_dir.path()).expect("baseline fingerprint");
        assert!(
            baseline.starts_with("git:"),
            "repo fingerprint should be hashed"
        );

        fs::create_dir_all(temp_dir.path().join(".ralph-burning/projects/demo"))
            .expect("create .ralph-burning");
        fs::write(
            temp_dir
                .path()
                .join(".ralph-burning/projects/demo/run.json"),
            "{}\n",
        )
        .expect("write runtime state");
        let ignored = git_diff_fingerprint(temp_dir.path()).expect("ignored fingerprint");
        assert_eq!(baseline, ignored, ".ralph-burning changes must be ignored");

        fs::write(temp_dir.path().join("tracked.txt"), "baseline\nchanged\n")
            .expect("modify tracked file");
        let changed = git_diff_fingerprint(temp_dir.path()).expect("changed fingerprint");
        assert_ne!(
            baseline, changed,
            "tracked repo changes must change the fingerprint"
        );
    }

    #[cfg(unix)]
    #[test]
    fn git_diff_fingerprint_ignores_untracked_build_artifact_paths_in_repo() {
        let temp_dir = tempdir().expect("create temp dir");
        let store_dir = tempdir().expect("create store dir");
        init_git_repo(temp_dir.path());

        let baseline = git_diff_fingerprint(temp_dir.path()).expect("baseline fingerprint");
        assert!(
            baseline.starts_with("git:"),
            "repo fingerprint should be hashed"
        );

        fs::create_dir_all(temp_dir.path().join("target/debug")).expect("create target dir");
        fs::write(temp_dir.path().join("target/debug/build.log"), "artifact\n")
            .expect("write build artifact");
        let ignored_target = git_diff_fingerprint(temp_dir.path()).expect("target fingerprint");
        assert_eq!(
            baseline, ignored_target,
            "target/ build artifacts must be ignored"
        );

        let result_path = temp_dir.path().join("result");
        let store_a = store_dir.path().join("store-a");
        fs::create_dir_all(&store_a).expect("create store-a");
        std::os::unix::fs::symlink(&store_a, &result_path).expect("create result symlink");
        let ignored_result = git_diff_fingerprint(temp_dir.path()).expect("result fingerprint");
        assert_eq!(
            baseline, ignored_result,
            "untracked result symlink churn must be ignored"
        );
    }

    #[cfg(unix)]
    #[test]
    fn git_diff_fingerprint_ignores_tracked_result_symlink_changes() {
        let temp_dir = tempdir().expect("create temp dir");
        let store_dir = tempdir().expect("create store dir");
        init_git_repo(temp_dir.path());

        let result_path = temp_dir.path().join("result");
        let store_a = store_dir.path().join("store-a");
        let store_b = store_dir.path().join("store-b");
        fs::create_dir_all(&store_a).expect("create store-a");
        fs::create_dir_all(&store_b).expect("create store-b");
        std::os::unix::fs::symlink(&store_a, &result_path).expect("create result symlink");
        run_git(temp_dir.path(), &["add", "result"]);
        run_git(temp_dir.path(), &["commit", "-m", "track result symlink"]);

        let baseline = git_diff_fingerprint(temp_dir.path()).expect("baseline fingerprint");
        fs::remove_file(&result_path).expect("remove result symlink");
        std::os::unix::fs::symlink(&store_b, &result_path).expect("recreate result symlink");

        let changed = git_diff_fingerprint(temp_dir.path()).expect("changed fingerprint");
        assert_eq!(
            baseline, changed,
            "tracked result symlink churn must be ignored for iterative stability"
        );
    }

    #[test]
    fn git_diff_fingerprint_changes_when_dirty_tracked_content_changes_again() {
        let temp_dir = tempdir().expect("create temp dir");
        init_git_repo(temp_dir.path());

        fs::write(
            temp_dir.path().join("tracked.txt"),
            "baseline\nchanged once\n",
        )
        .expect("modify tracked file");
        let first = git_diff_fingerprint(temp_dir.path()).expect("first fingerprint");

        fs::write(
            temp_dir.path().join("tracked.txt"),
            "baseline\nchanged twice\n",
        )
        .expect("modify tracked file again");
        let second = git_diff_fingerprint(temp_dir.path()).expect("second fingerprint");

        assert_ne!(
            first, second,
            "tracked content changes on an already-dirty file must change the fingerprint"
        );
    }

    #[test]
    fn git_change_scope_fingerprint_ignores_dirty_tracked_content_rewrites() {
        let temp_dir = tempdir().expect("create temp dir");
        init_git_repo(temp_dir.path());

        fs::write(
            temp_dir.path().join("tracked.txt"),
            "baseline\nchanged once\n",
        )
        .expect("modify tracked file");
        let first = git_change_scope_fingerprint(temp_dir.path()).expect("first fingerprint");

        fs::write(
            temp_dir.path().join("tracked.txt"),
            "baseline\nchanged twice\n",
        )
        .expect("modify tracked file again");
        let second = git_change_scope_fingerprint(temp_dir.path()).expect("second fingerprint");

        assert_eq!(
            first, second,
            "change-scope fingerprint should stay stable when the dirty path set is unchanged"
        );
    }

    #[test]
    fn git_change_scope_fingerprint_changes_when_dirty_path_set_expands() {
        let temp_dir = tempdir().expect("create temp dir");
        init_git_repo(temp_dir.path());

        fs::write(
            temp_dir.path().join("tracked.txt"),
            "baseline\nchanged once\n",
        )
        .expect("modify tracked file");
        let first = git_change_scope_fingerprint(temp_dir.path()).expect("first fingerprint");

        fs::write(temp_dir.path().join("second.txt"), "new dirty path\n")
            .expect("modify second tracked file");
        let second = git_change_scope_fingerprint(temp_dir.path()).expect("second fingerprint");

        assert_ne!(
            first, second,
            "change-scope fingerprint must still react when a new dirty path is introduced"
        );
    }

    #[test]
    fn git_diff_fingerprint_changes_when_dirty_untracked_content_changes_again() {
        let temp_dir = tempdir().expect("create temp dir");
        init_git_repo(temp_dir.path());

        fs::write(temp_dir.path().join("scratch.txt"), "draft one\n").expect("write untracked");
        let first = git_diff_fingerprint(temp_dir.path()).expect("first fingerprint");

        fs::write(temp_dir.path().join("scratch.txt"), "draft two\n").expect("rewrite untracked");
        let second = git_diff_fingerprint(temp_dir.path()).expect("second fingerprint");

        assert_ne!(
            first, second,
            "untracked content changes on an already-dirty file must change the fingerprint"
        );
    }

    #[cfg(unix)]
    #[test]
    fn git_diff_fingerprint_changes_when_untracked_file_mode_changes() {
        let temp_dir = tempdir().expect("create temp dir");
        init_git_repo(temp_dir.path());

        let script_path = temp_dir.path().join("script.sh");
        fs::write(&script_path, "#!/bin/sh\necho hi\n").expect("write script");
        let first = git_diff_fingerprint(temp_dir.path()).expect("first fingerprint");

        let mut permissions = fs::metadata(&script_path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).expect("chmod script");
        let second = git_diff_fingerprint(temp_dir.path()).expect("second fingerprint");

        assert_ne!(
            first, second,
            "untracked chmod-only changes must change the fingerprint"
        );
    }

    #[test]
    fn git_diff_fingerprint_falls_back_outside_git_repo_and_ignores_runtime_state() {
        let temp_dir = tempdir().expect("create temp dir");
        fs::write(temp_dir.path().join("tracked.txt"), "baseline\n").expect("write tracked file");

        let baseline = git_diff_fingerprint(temp_dir.path()).expect("baseline fingerprint");
        assert!(
            baseline.starts_with("fs:"),
            "non-git workspaces should use the filesystem fallback"
        );

        fs::create_dir_all(temp_dir.path().join(".ralph-burning/projects/demo"))
            .expect("create .ralph-burning");
        fs::write(
            temp_dir
                .path()
                .join(".ralph-burning/projects/demo/run.json"),
            "{}\n",
        )
        .expect("write runtime state");
        let ignored = git_diff_fingerprint(temp_dir.path()).expect("ignored fingerprint");
        assert_eq!(baseline, ignored, ".ralph-burning changes must be ignored");

        fs::write(temp_dir.path().join("tracked.txt"), "baseline\nchanged\n")
            .expect("modify tracked file");
        let changed = git_diff_fingerprint(temp_dir.path()).expect("changed fingerprint");
        assert_ne!(
            baseline, changed,
            "non-runtime workspace changes must change the fallback fingerprint"
        );
    }

    #[cfg(unix)]
    #[test]
    fn git_diff_fingerprint_fallback_ignores_build_artifact_named_paths() {
        let temp_dir = tempdir().expect("create temp dir");
        let store_dir = tempdir().expect("create store dir");

        fs::write(temp_dir.path().join("tracked.txt"), "baseline\n").expect("write tracked file");
        let baseline = git_diff_fingerprint(temp_dir.path()).expect("baseline fingerprint");
        assert!(
            baseline.starts_with("fs:"),
            "non-git workspaces should use the filesystem fallback"
        );

        fs::create_dir_all(temp_dir.path().join("target/debug")).expect("create target dir");
        fs::write(temp_dir.path().join("target/debug/build.log"), "artifact\n")
            .expect("write build artifact");

        let result_path = temp_dir.path().join("result");
        let store_a = store_dir.path().join("store-a");
        fs::create_dir_all(&store_a).expect("create store-a");
        std::os::unix::fs::symlink(&store_a, &result_path).expect("create result symlink");

        let changed = git_diff_fingerprint(temp_dir.path()).expect("changed fingerprint");
        assert_eq!(
            baseline, changed,
            "filesystem fallback must ignore top-level target/result build artifacts"
        );
    }

    #[test]
    fn git_diff_fingerprint_changes_when_head_commit_changes_without_worktree_diff() {
        let temp_dir = tempdir().expect("create temp dir");
        init_git_repo(temp_dir.path());

        let baseline = git_diff_fingerprint(temp_dir.path()).expect("baseline fingerprint");
        fs::write(temp_dir.path().join("tracked.txt"), "baseline\ncommitted\n")
            .expect("update tracked file");
        run_git(temp_dir.path(), &["commit", "-am", "commit change"]);

        let changed = git_diff_fingerprint(temp_dir.path()).expect("changed fingerprint");
        assert_ne!(
            baseline, changed,
            "HEAD movement without a remaining worktree diff must change the fingerprint"
        );
    }

    #[test]
    fn git_diff_fingerprint_changes_when_empty_directory_is_created_or_removed() {
        let temp_dir = tempdir().expect("create temp dir");
        init_git_repo(temp_dir.path());

        let baseline = git_diff_fingerprint(temp_dir.path()).expect("baseline fingerprint");

        let empty_dir = temp_dir.path().join("scratch");
        fs::create_dir(&empty_dir).expect("create empty dir");
        let created = git_diff_fingerprint(temp_dir.path()).expect("created fingerprint");
        assert_ne!(
            baseline, created,
            "empty directory creation must change the repo fingerprint"
        );

        fs::remove_dir(&empty_dir).expect("remove empty dir");
        let removed = git_diff_fingerprint(temp_dir.path()).expect("removed fingerprint");
        assert_eq!(
            baseline, removed,
            "removing the empty directory should restore the original fingerprint"
        );
    }

    #[cfg(unix)]
    #[test]
    fn git_diff_fingerprint_fallback_changes_when_file_mode_changes() {
        let temp_dir = tempdir().expect("create temp dir");
        let script_path = temp_dir.path().join("script.sh");
        fs::write(&script_path, "#!/bin/sh\necho hi\n").expect("write script");

        let first = git_diff_fingerprint(temp_dir.path()).expect("first fingerprint");

        let mut permissions = fs::metadata(&script_path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).expect("chmod script");
        let second = git_diff_fingerprint(temp_dir.path()).expect("second fingerprint");

        assert_ne!(
            first, second,
            "filesystem fallback must detect chmod-only changes"
        );
    }

    #[test]
    fn git_repo_available_returns_false_when_git_binary_is_unavailable() {
        let temp_dir = tempdir().expect("create temp dir");
        init_git_repo(temp_dir.path());

        assert!(
            !git_repo_available_with_program(temp_dir.path(), "git-does-not-exist")
                .expect("repo probe should fall back cleanly"),
            "missing git binary should be treated as a non-git workspace probe result"
        );
    }

    #[test]
    fn resume_iteration_counters_restore_iterative_loop_state_for_same_stage_round() {
        let cursor = StageCursor::new(StageId::PlanAndImplement, 2, 1, 3).expect("cursor");
        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-iter".to_owned(),
                stage_cursor: cursor.clone(),
                started_at: Utc::now(),
                prompt_hash_at_cycle_start: "cycle-hash".to_owned(),
                prompt_hash_at_stage_start: "stage-hash".to_owned(),
                qa_iterations_current_cycle: 4,
                review_iterations_current_cycle: 5,
                final_review_restart_count: 1,
                iterative_implementer_state: Some(IterativeImplementerState {
                    completed_iterations: 6,
                    stable_count: 1,
                    loop_policy: None,
                    stage_target: None,
                }),
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 3,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let (qa, review, iterative_state) =
            resume_iteration_counters(&snapshot, &cursor, &[]).expect("resume counters");

        assert_eq!(qa, 4);
        assert_eq!(review, 5);
        assert_eq!(
            iterative_state,
            Some(IterativeImplementerState {
                completed_iterations: 6,
                stable_count: 1,
                loop_policy: None,
                stage_target: None,
            })
        );
    }

    #[test]
    fn stage_running_summary_for_active_run_preserves_iterative_progress() {
        let active_run = ActiveRun {
            run_id: "run-iter".to_owned(),
            stage_cursor: StageCursor::new(StageId::PlanAndImplement, 2, 2, 3).expect("cursor"),
            started_at: Utc::now(),
            prompt_hash_at_cycle_start: "cycle-hash".to_owned(),
            prompt_hash_at_stage_start: "stage-hash".to_owned(),
            qa_iterations_current_cycle: 0,
            review_iterations_current_cycle: 0,
            final_review_restart_count: 0,
            iterative_implementer_state: Some(IterativeImplementerState {
                completed_iterations: 3,
                stable_count: 1,
                loop_policy: Some(IterativeImplementerLoopPolicy {
                    max_consecutive_implementer_rounds: 7,
                    stable_rounds_required: 2,
                }),
                stage_target: None,
            }),
            stage_resolution_snapshot: None,
        };

        let summary =
            stage_running_summary_for_active_run(StageId::PlanAndImplement, Some(&active_run), 10);

        assert_eq!(
            summary, "running: Plan and Implement (iteration 3/7)",
            "resume and retry boundaries must preserve the durable iterative progress summary"
        );
    }

    #[test]
    fn failed_invocation_id_for_iterative_minimal_uses_next_iteration_suffix() {
        let run_id = RunId::new("run-iter-failure").expect("run id");
        let cursor = StageCursor::new(StageId::PlanAndImplement, 2, 1, 3).expect("cursor");
        let snapshot = RunSnapshot {
            active_run: Some(ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: cursor.clone(),
                started_at: Utc::now(),
                prompt_hash_at_cycle_start: "cycle-hash".to_owned(),
                prompt_hash_at_stage_start: "stage-hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: Some(IterativeImplementerState {
                    completed_iterations: 2,
                    stable_count: 0,
                    loop_policy: None,
                    stage_target: None,
                }),
                stage_resolution_snapshot: None,
            }),
            interrupted_run: None,
            status: RunStatus::Running,
            cycle_history: Vec::new(),
            completion_rounds: 3,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "running".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let invocation_id = failed_invocation_id_for_stage(
            &run_id,
            StageId::PlanAndImplement,
            &cursor,
            &snapshot,
            FlowPreset::IterativeMinimal,
        );

        assert_eq!(
            invocation_id,
            format!("{}-plan_and_implement-c2-a1-cr3-it3", run_id.as_str())
        );
    }

    #[test]
    fn resume_iteration_counters_reconstruct_iterative_loop_state_from_journal() {
        let run_id = RunId::new("run-iter").expect("run id");
        let cursor = StageCursor::new(StageId::PlanAndImplement, 2, 1, 3).expect("cursor");
        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: cursor.clone(),
                started_at: Utc::now(),
                prompt_hash_at_cycle_start: "cycle-hash".to_owned(),
                prompt_hash_at_stage_start: "stage-hash".to_owned(),
                qa_iterations_current_cycle: 4,
                review_iterations_current_cycle: 5,
                final_review_restart_count: 1,
                iterative_implementer_state: Some(IterativeImplementerState {
                    completed_iterations: 1,
                    stable_count: 0,
                    loop_policy: None,
                    stage_target: None,
                }),
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 3,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed".to_owned(),
            last_stage_resolution_snapshot: None,
        };
        let resume_events = vec![
            journal::implementer_iteration_completed_event(
                1,
                Utc::now(),
                &run_id,
                StageId::PlanAndImplement,
                2,
                1,
                3,
                2,
                true,
                "completed",
            ),
            journal::implementer_iteration_completed_event(
                2,
                Utc::now(),
                &run_id,
                StageId::PlanAndImplement,
                2,
                1,
                3,
                3,
                false,
                "completed",
            ),
        ];

        let (qa, review, iterative_state) =
            resume_iteration_counters(&snapshot, &cursor, &resume_events).expect("resume counters");

        assert_eq!(qa, 4);
        assert_eq!(review, 5);
        assert_eq!(
            iterative_state,
            Some(IterativeImplementerState {
                completed_iterations: 3,
                stable_count: 1,
                loop_policy: None,
                stage_target: None,
            })
        );
    }

    #[test]
    fn resume_iteration_counters_preserve_iterative_loop_state_when_attempt_changes() {
        let run_id = RunId::new("run-iter").expect("run id");
        let interrupted_cursor =
            StageCursor::new(StageId::PlanAndImplement, 2, 1, 3).expect("cursor");
        let resume_cursor = StageCursor::new(StageId::PlanAndImplement, 2, 2, 3).expect("cursor");
        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: interrupted_cursor,
                started_at: Utc::now(),
                prompt_hash_at_cycle_start: "cycle-hash".to_owned(),
                prompt_hash_at_stage_start: "stage-hash".to_owned(),
                qa_iterations_current_cycle: 4,
                review_iterations_current_cycle: 5,
                final_review_restart_count: 1,
                iterative_implementer_state: Some(IterativeImplementerState {
                    completed_iterations: 6,
                    stable_count: 1,
                    loop_policy: None,
                    stage_target: None,
                }),
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 3,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed".to_owned(),
            last_stage_resolution_snapshot: None,
        };
        let (_, _, iterative_state) =
            resume_iteration_counters(&snapshot, &resume_cursor, &[]).expect("resume counters");

        assert_eq!(
            iterative_state,
            Some(IterativeImplementerState {
                completed_iterations: 6,
                stable_count: 1,
                loop_policy: None,
                stage_target: None,
            }),
            "retry attempts must preserve the completed iterative budget and stability streak"
        );
    }

    #[test]
    fn resume_iteration_counters_reconstruct_iterative_loop_state_across_retry_attempts() {
        let run_id = RunId::new("run-iter").expect("run id");
        let interrupted_cursor =
            StageCursor::new(StageId::PlanAndImplement, 2, 1, 3).expect("cursor");
        let resume_cursor = StageCursor::new(StageId::PlanAndImplement, 2, 2, 3).expect("cursor");
        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: interrupted_cursor,
                started_at: Utc::now(),
                prompt_hash_at_cycle_start: "cycle-hash".to_owned(),
                prompt_hash_at_stage_start: "stage-hash".to_owned(),
                qa_iterations_current_cycle: 4,
                review_iterations_current_cycle: 5,
                final_review_restart_count: 1,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 3,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed".to_owned(),
            last_stage_resolution_snapshot: None,
        };
        let resume_events = vec![
            journal::stage_entered_event(1, Utc::now(), &run_id, StageId::PlanAndImplement, 2, 1),
            journal::implementer_iteration_completed_event(
                2,
                Utc::now(),
                &run_id,
                StageId::PlanAndImplement,
                2,
                1,
                3,
                1,
                true,
                "completed",
            ),
            journal::implementer_iteration_completed_event(
                3,
                Utc::now(),
                &run_id,
                StageId::PlanAndImplement,
                2,
                1,
                3,
                2,
                false,
                "completed",
            ),
            journal::stage_failed_event(
                4,
                Utc::now(),
                &run_id,
                StageId::PlanAndImplement,
                2,
                1,
                FailureClass::TransportFailure,
                "retry me",
                true,
                "inv-1",
            ),
            journal::stage_entered_event(5, Utc::now(), &run_id, StageId::PlanAndImplement, 2, 2),
            journal::implementer_iteration_completed_event(
                6,
                Utc::now(),
                &run_id,
                StageId::PlanAndImplement,
                2,
                2,
                3,
                3,
                false,
                "completed",
            ),
        ];

        let (_, _, iterative_state) =
            resume_iteration_counters(&snapshot, &resume_cursor, &resume_events)
                .expect("resume counters");

        assert_eq!(
            iterative_state,
            Some(IterativeImplementerState {
                completed_iterations: 3,
                stable_count: 2,
                loop_policy: None,
                stage_target: None,
            }),
            "resume must accumulate iterative progress across retry attempts in the same stage round"
        );
    }

    #[test]
    fn resume_iteration_counters_clear_iterative_loop_state_for_new_round() {
        let interrupted_cursor =
            StageCursor::new(StageId::PlanAndImplement, 2, 1, 3).expect("cursor");
        let resume_cursor = StageCursor::new(StageId::PlanAndImplement, 2, 1, 4).expect("cursor");
        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-iter".to_owned(),
                stage_cursor: interrupted_cursor,
                started_at: Utc::now(),
                prompt_hash_at_cycle_start: "cycle-hash".to_owned(),
                prompt_hash_at_stage_start: "stage-hash".to_owned(),
                qa_iterations_current_cycle: 1,
                review_iterations_current_cycle: 2,
                final_review_restart_count: 0,
                iterative_implementer_state: Some(IterativeImplementerState {
                    completed_iterations: 3,
                    stable_count: 1,
                    loop_policy: None,
                    stage_target: None,
                }),
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 4,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed".to_owned(),
            last_stage_resolution_snapshot: None,
        };

        let (qa, review, iterative_state) =
            resume_iteration_counters(&snapshot, &resume_cursor, &[]).expect("resume counters");

        assert_eq!(qa, 1);
        assert_eq!(review, 2);
        assert_eq!(iterative_state, None);
    }

    #[test]
    fn final_review_routes_only_fix_current_into_restart_queue() {
        let queued = |id: &str, classification, mapped_to_bead_id: Option<&str>| {
            FinalReviewQueuedAmendment {
                queued: QueuedAmendment {
                    amendment_id: id.to_owned(),
                    source_stage: StageId::FinalReview,
                    source_cycle: 1,
                    source_completion_round: 2,
                    body: format!("body-{id}"),
                    created_at: Utc::now(),
                    batch_sequence: 1,
                    source:
                        crate::contexts::project_run_record::model::AmendmentSource::WorkflowStage,
                    dedup_key: format!("dedup-{id}"),
                    classification,
                    covered_by_bead_id: mapped_to_bead_id.map(str::to_owned),
                    proposed_bead_summary: None,
                },
                reviewer_sources: Vec::new(),
                mapped_to_bead_id: mapped_to_bead_id.map(str::to_owned),
            }
        };

        let amendments = vec![
            queued(
                "fix",
                crate::contexts::workflow_composition::panel_contracts::AmendmentClassification::FixCurrentBead,
                None,
            ),
            queued(
                "planned",
                crate::contexts::workflow_composition::panel_contracts::AmendmentClassification::CoveredByExistingBead,
                Some("bead-elsewhere"),
            ),
            queued(
                "proposed",
                crate::contexts::workflow_composition::panel_contracts::AmendmentClassification::ProposeNewBead,
                None,
            ),
        ];

        let (planned_elsewhere, restart_queue) =
            partition_final_review_amendments_by_route(&amendments);

        assert_eq!(planned_elsewhere.len(), 0);
        assert_eq!(restart_queue.len(), 1);
        assert_eq!(restart_queue[0].queued.amendment_id, "fix");
    }

    #[test]
    fn classified_review_findings_merge_non_duplicate_legacy_follow_ups() {
        let payload = StagePayload::Validation(ValidationPayload {
            outcome: ReviewOutcome::RequestChanges,
            evidence: vec!["evidence".to_owned()],
            findings_or_gaps: vec!["gap".to_owned()],
            follow_up_or_amendments: vec!["fix the current bead".to_owned()],
            classified_findings: vec![ClassifiedFinding {
                body: "covered elsewhere".to_owned(),
                classification: ReviewFindingClass::CoveredByExistingBead,
                covered_by_bead_id: Some("9ni.8.5".to_owned()),
                mapped_to_bead_id: None,
                proposed_bead_summary: None,
            }],
        });
        let run_id = RunId::new("run-classification").expect("run id");
        let follow_ups = validation_follow_ups(&payload);
        let amendments = build_queued_amendments(&follow_ups, StageId::Review, 1, 1, &run_id);

        assert_eq!(follow_ups.len(), 2);
        assert_eq!(amendments.len(), 1);
        assert_eq!(amendments[0].body, "fix the current bead");
        assert!(has_restart_triggering_follow_up(&payload));
    }

    #[test]
    fn classified_review_findings_do_not_duplicate_matching_legacy_follow_ups() {
        let payload = StagePayload::Validation(ValidationPayload {
            outcome: ReviewOutcome::RequestChanges,
            evidence: vec!["evidence".to_owned()],
            findings_or_gaps: vec!["gap".to_owned()],
            follow_up_or_amendments: vec![" covered elsewhere ".to_owned()],
            classified_findings: vec![ClassifiedFinding {
                body: "covered elsewhere".to_owned(),
                classification: ReviewFindingClass::CoveredByExistingBead,
                covered_by_bead_id: Some("9ni.8.5".to_owned()),
                mapped_to_bead_id: None,
                proposed_bead_summary: None,
            }],
        });
        let run_id = RunId::new("run-classification-dedupe").expect("run id");
        let follow_ups = validation_follow_ups(&payload);
        let amendments = build_queued_amendments(&follow_ups, StageId::Review, 1, 1, &run_id);

        assert_eq!(follow_ups.len(), 1);
        assert!(amendments.is_empty());
        assert!(!has_restart_triggering_follow_up(&payload));
    }

    #[test]
    fn non_fix_only_review_classification_defers_without_restart_trigger() {
        let payload = StagePayload::Validation(ValidationPayload {
            outcome: ReviewOutcome::RequestChanges,
            evidence: vec!["evidence".to_owned()],
            findings_or_gaps: vec!["gap".to_owned()],
            follow_up_or_amendments: Vec::new(),
            classified_findings: vec![
                ClassifiedFinding {
                    body: "covered elsewhere".to_owned(),
                    classification: ReviewFindingClass::CoveredByExistingBead,
                    covered_by_bead_id: Some("9ni.8.5".to_owned()),
                    mapped_to_bead_id: None,
                    proposed_bead_summary: None,
                },
                ClassifiedFinding {
                    body: "future work".to_owned(),
                    classification: ReviewFindingClass::ProposeNewBead,
                    covered_by_bead_id: None,
                    mapped_to_bead_id: None,
                    proposed_bead_summary: Some("Add future work".to_owned()),
                },
            ],
        });
        let run_id = RunId::new("run-classification-deferred").expect("run id");
        let follow_ups = validation_follow_ups(&payload);
        let amendments = build_queued_amendments(&follow_ups, StageId::Review, 1, 1, &run_id);

        assert!(amendments.is_empty());
        assert!(!has_restart_triggering_follow_up(&payload));
        assert!(has_deferred_classified_finding(&payload));
    }

    #[test]
    fn non_fix_only_review_classification_skips_apply_fixes() {
        let payload = StagePayload::Validation(ValidationPayload {
            outcome: ReviewOutcome::RequestChanges,
            evidence: vec!["evidence".to_owned()],
            findings_or_gaps: vec!["gap".to_owned()],
            follow_up_or_amendments: Vec::new(),
            classified_findings: vec![ClassifiedFinding {
                body: "covered elsewhere".to_owned(),
                classification: ReviewFindingClass::CoveredByExistingBead,
                covered_by_bead_id: Some("9ni.8.5".to_owned()),
                mapped_to_bead_id: None,
                proposed_bead_summary: None,
            }],
        });

        assert_eq!(
            skip_next_apply_fixes_reason(&payload, Some(StageId::ApplyFixes)),
            Some("review only has deferred non-fix classifications")
        );
    }

    #[test]
    fn conditionally_approved_non_fix_only_review_classification_skips_apply_fixes() {
        let payload = StagePayload::Validation(ValidationPayload {
            outcome: ReviewOutcome::ConditionallyApproved,
            evidence: vec!["evidence".to_owned()],
            findings_or_gaps: vec!["gap".to_owned()],
            follow_up_or_amendments: Vec::new(),
            classified_findings: vec![ClassifiedFinding {
                body: "covered elsewhere".to_owned(),
                classification: ReviewFindingClass::CoveredByExistingBead,
                covered_by_bead_id: Some("9ni.8.5".to_owned()),
                mapped_to_bead_id: None,
                proposed_bead_summary: None,
            }],
        });

        assert_eq!(
            skip_next_apply_fixes_reason(&payload, Some(StageId::ApplyFixes)),
            Some("review only has deferred non-fix classifications")
        );
    }

    #[test]
    fn approved_non_fix_only_review_classification_skips_apply_fixes() {
        let payload = StagePayload::Validation(ValidationPayload {
            outcome: ReviewOutcome::Approved,
            evidence: vec!["evidence".to_owned()],
            findings_or_gaps: Vec::new(),
            follow_up_or_amendments: Vec::new(),
            classified_findings: vec![
                ClassifiedFinding {
                    body: "covered elsewhere".to_owned(),
                    classification: ReviewFindingClass::CoveredByExistingBead,
                    covered_by_bead_id: Some("9ni.8.5".to_owned()),
                    mapped_to_bead_id: None,
                    proposed_bead_summary: None,
                },
                ClassifiedFinding {
                    body: "future work".to_owned(),
                    classification: ReviewFindingClass::ProposeNewBead,
                    covered_by_bead_id: None,
                    mapped_to_bead_id: None,
                    proposed_bead_summary: Some("Add future work".to_owned()),
                },
                ClassifiedFinding {
                    body: "observation".to_owned(),
                    classification: ReviewFindingClass::InformationalOnly,
                    covered_by_bead_id: None,
                    mapped_to_bead_id: None,
                    proposed_bead_summary: None,
                },
            ],
        });

        assert_eq!(
            skip_next_apply_fixes_reason(&payload, Some(StageId::ApplyFixes)),
            Some("review only has deferred non-fix classifications")
        );
    }

    #[test]
    fn mixed_fix_and_non_fix_review_classification_does_not_skip_apply_fixes() {
        let payload = StagePayload::Validation(ValidationPayload {
            outcome: ReviewOutcome::RequestChanges,
            evidence: vec!["evidence".to_owned()],
            findings_or_gaps: vec!["gap".to_owned()],
            follow_up_or_amendments: Vec::new(),
            classified_findings: vec![
                ClassifiedFinding {
                    body: "covered elsewhere".to_owned(),
                    classification: ReviewFindingClass::CoveredByExistingBead,
                    covered_by_bead_id: Some("9ni.8.5".to_owned()),
                    mapped_to_bead_id: None,
                    proposed_bead_summary: None,
                },
                ClassifiedFinding {
                    body: "fix here".to_owned(),
                    classification: ReviewFindingClass::FixCurrentBead,
                    covered_by_bead_id: None,
                    mapped_to_bead_id: None,
                    proposed_bead_summary: None,
                },
            ],
        });

        assert_eq!(
            skip_next_apply_fixes_reason(&payload, Some(StageId::ApplyFixes)),
            None
        );
    }

    fn sample_milestone_bundle(milestone_id: &str) -> MilestoneBundle {
        MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: milestone_id.to_owned(),
                name: "Alpha Milestone".to_owned(),
            },
            executive_summary: "Ship bead-backed task creation.".to_owned(),
            goals: vec!["Create the bead-backed task path.".to_owned()],
            non_goals: vec![],
            constraints: vec!["Keep the run substrate compatible.".to_owned()],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Task creation works".to_owned(),
                covered_by: vec!["bead-2".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Creation".to_owned(),
                description: Some("Project bootstrap flow.".to_owned()),
                beads: vec![BeadProposal {
                    bead_id: Some(format!("{milestone_id}.bead-2")),
                    explicit_id: None,
                    title: "Bootstrap bead-backed task creation".to_owned(),
                    description: Some("Create a project from milestone context.".to_owned()),
                    bead_type: Some("feature".to_owned()),
                    priority: Some(1),
                    labels: vec!["creation".to_owned()],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: Some(FlowPreset::Standard),
                }],
            }],
            default_flow: FlowPreset::Minimal,
            agents_guidance: Some("Keep it deterministic.".to_owned()),
        }
    }

    #[test]
    fn final_review_snapshot_serialization_includes_reviewer_and_arbiter_targets() {
        let reviewers = final_review_reviewers();
        let arbiter = ResolvedBackendTarget::new(BackendFamily::Stub, "arbiter-model");

        let snapshot = build_final_review_snapshot(StageId::FinalReview, &reviewers, &arbiter);
        let serialized = serde_json::to_value(&snapshot).expect("snapshot should serialize");

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
    fn final_review_arbiter_drift_detection_is_detected() {
        let reviewers = final_review_reviewers();
        let arbiter_a = ResolvedBackendTarget::new(BackendFamily::Stub, "arbiter-a");
        let arbiter_b = ResolvedBackendTarget::new(BackendFamily::Stub, "arbiter-b");

        let old = build_final_review_snapshot(StageId::FinalReview, &reviewers, &arbiter_a);
        let changed = build_final_review_snapshot(StageId::FinalReview, &reviewers, &arbiter_b);
        assert!(resolution_has_drifted(&old, &changed));
    }

    #[test]
    fn final_review_drift_requirements_fail_when_arbiter_is_missing() {
        let reviewers = final_review_reviewers();
        let arbiter = ResolvedBackendTarget::new(BackendFamily::Stub, "arbiter-model");
        let config = final_review_effective_config();

        let mut snapshot = build_final_review_snapshot(StageId::FinalReview, &reviewers, &arbiter);
        snapshot.final_review_arbiter = None;

        let error =
            drift_still_satisfies_requirements(&snapshot, StageId::FinalReview, &config, None)
                .expect_err("missing arbiter should fail final-review requirements");
        assert!(matches!(
            error,
            AppError::ResumeDriftFailure {
                stage_id: StageId::FinalReview,
                details,
            } if details == "re-resolved final-review panel has no arbiter"
        ));
    }

    #[test]
    fn prompt_review_drift_requirements_reject_effective_min_override() {
        let config = final_review_effective_config();
        let panel = crate::contexts::agent_execution::policy::PromptReviewPanelResolution {
            refiner: ResolvedBackendTarget::new(BackendFamily::Claude, "refiner-model"),
            validators: vec![ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Codex, "validator-0-model"),
                required: true,
                configured_index: 0,
            }],
        };
        let snapshot = build_prompt_review_snapshot(StageId::PromptReview, &panel);

        let error =
            drift_still_satisfies_requirements(&snapshot, StageId::PromptReview, &config, Some(1))
                .expect_err("prompt-review should reject effective_min_override");
        assert!(matches!(
            error,
            AppError::ResumeDriftFailure {
                stage_id: StageId::PromptReview,
                details,
            } if details == "prompt-review does not support effective_min_override (1)"
        ));
    }

    #[test]
    fn prompt_review_drift_requirements_accept_none_override_when_quorum_is_met() {
        let config = final_review_effective_config();
        let min_reviewers = config.prompt_review_policy().min_reviewers;
        let validators = (0..min_reviewers)
            .map(|idx| ResolvedPanelMember {
                target: ResolvedBackendTarget::new(
                    BackendFamily::Codex,
                    format!("validator-{idx}-model"),
                ),
                required: true,
                configured_index: idx,
            })
            .collect();
        let panel = crate::contexts::agent_execution::policy::PromptReviewPanelResolution {
            refiner: ResolvedBackendTarget::new(BackendFamily::Claude, "refiner-model"),
            validators,
        };
        let snapshot = build_prompt_review_snapshot(StageId::PromptReview, &panel);

        drift_still_satisfies_requirements(&snapshot, StageId::PromptReview, &config, None)
            .expect("prompt-review should accept a met quorum without override");
    }

    #[test]
    fn milestone_lineage_plan_hash_does_not_reattach_unconfirmed_beads_to_snapshot_plan() {
        let temp_dir = tempdir().expect("create temp dir");
        let base_dir = temp_dir.path();
        let now = Utc::now();
        initialize_workspace(base_dir, now).expect("initialize workspace");

        let milestone = create_milestone(
            &FsMilestoneStore,
            base_dir,
            CreateMilestoneInput {
                id: "ms-alpha".to_owned(),
                name: "Alpha".to_owned(),
                description: "Test milestone".to_owned(),
            },
            now,
        )
        .expect("create milestone");
        persist_plan(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base_dir,
            &milestone.id,
            &sample_milestone_bundle("ms-alpha"),
            now,
        )
        .expect("persist plan");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::Standard,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: now,
            status_summary: ProjectStatusSummary::Created,
            task_source: Some(TaskSource {
                milestone_id: milestone.id.to_string(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: None,
                plan_version: None,
                plan_workstream_index: None,
                plan_bead_index: None,
            }),
        };

        let plan_hash = milestone_lineage_plan_hash(
            &project_record,
            base_dir,
            &project_id,
            &milestone.id,
            "ms-alpha.bead-2",
            "run-1",
        )
        .expect("derive plan hash");

        assert_eq!(plan_hash, "bead:ms-alpha:ms-alpha.bead-2");
    }

    #[test]
    fn sync_milestone_bead_start_transitions_controller_to_running() {
        let temp_dir = tempdir().expect("create temp dir");
        let base_dir = temp_dir.path();
        let now = Utc::now();
        initialize_workspace(base_dir, now).expect("initialize workspace");

        let milestone = create_milestone(
            &FsMilestoneStore,
            base_dir,
            CreateMilestoneInput {
                id: "ms-alpha".to_owned(),
                name: "Alpha".to_owned(),
                description: "Test milestone".to_owned(),
            },
            now,
        )
        .expect("create milestone");
        persist_plan(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base_dir,
            &milestone.id,
            &sample_milestone_bundle("ms-alpha"),
            now,
        )
        .expect("persist plan");

        let project_id = ProjectId::new("bead-run").expect("project id");
        let project_record = ProjectRecord {
            id: project_id.clone(),
            name: "Alpha: Bead".to_owned(),
            flow: FlowPreset::Standard,
            prompt_reference: "prompt.md".to_owned(),
            prompt_hash: "prompt-hash".to_owned(),
            created_at: now,
            status_summary: ProjectStatusSummary::Created,
            task_source: Some(TaskSource {
                milestone_id: milestone.id.to_string(),
                bead_id: "ms-alpha.bead-2".to_owned(),
                parent_epic_id: None,
                origin: TaskOrigin::Milestone,
                plan_hash: None,
                plan_version: None,
                plan_workstream_index: None,
                plan_bead_index: None,
            }),
        };

        sync_milestone_bead_start(
            &project_record,
            base_dir,
            &project_id,
            &crate::shared::domain::RunId::new("run-1").expect("run id"),
            now,
        )
        .expect("sync bead start");

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base_dir,
            &milestone.id,
        )
        .expect("load controller")
        .expect("controller exists");
        assert_eq!(controller.state, MilestoneControllerState::Running);
        assert_eq!(
            controller.active_bead_id.as_deref(),
            Some("ms-alpha.bead-2")
        );
        assert_eq!(
            controller.active_task_id.as_deref(),
            Some(project_id.as_str())
        );
    }

    // ── derive_resume_state unit tests ─────────────────────────────────

    mod resume_state_tests {
        use chrono::Utc;

        use crate::contexts::project_run_record::journal;
        use crate::contexts::project_run_record::model::{JournalEvent, RunSnapshot};
        use crate::contexts::workflow_composition::contracts::contract_for_stage;
        use crate::contexts::workflow_composition::{
            flow_semantics, stage_plan_for_flow, FlowPreset,
        };
        use crate::shared::domain::{
            BackendFamily, BackendRole, FailureClass, ResolvedBackendTarget, RunId, StageId,
        };

        use super::super::{derive_resume_state, StagePlan};

        fn make_stage_plan(stages: &[StageId]) -> Vec<StagePlan> {
            stages
                .iter()
                .map(|&stage_id| StagePlan {
                    stage_id,
                    role: BackendRole::for_stage(stage_id),
                    contract: contract_for_stage(stage_id),
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "test"),
                })
                .collect()
        }

        #[test]
        fn resume_after_retryable_failure_preserves_attempt() {
            let run_id = RunId::new("test-run-1".to_owned()).unwrap();
            let stages = stage_plan_for_flow(FlowPreset::Standard, false);
            let stage_plan = make_stage_plan(&stages);
            let semantics = flow_semantics(FlowPreset::Standard);

            let events: Vec<JournalEvent> = vec![
                journal::run_started_event(1, Utc::now(), &run_id, StageId::Planning, 20),
                journal::stage_entered_event(2, Utc::now(), &run_id, StageId::Planning, 1, 1),
                journal::stage_completed_event(
                    3,
                    Utc::now(),
                    &run_id,
                    StageId::Planning,
                    1,
                    1,
                    "payload-1",
                    "artifact-1",
                ),
                journal::stage_entered_event(4, Utc::now(), &run_id, StageId::Implementation, 1, 1),
                journal::stage_failed_event(
                    5,
                    Utc::now(),
                    &run_id,
                    StageId::Implementation,
                    1,
                    1,
                    FailureClass::TransportFailure,
                    "transient failure 1",
                    true,
                    "test-invocation",
                ),
                journal::stage_failed_event(
                    6,
                    Utc::now(),
                    &run_id,
                    StageId::Implementation,
                    1,
                    2,
                    FailureClass::TransportFailure,
                    "transient failure 2",
                    true,
                    "test-invocation",
                ),
                journal::stage_failed_event(
                    7,
                    Utc::now(),
                    &run_id,
                    StageId::Implementation,
                    1,
                    3,
                    FailureClass::TransportFailure,
                    "transient failure 3",
                    true,
                    "test-invocation",
                ),
            ];

            let snapshot = RunSnapshot::initial(20);
            let resume =
                derive_resume_state(&run_id, &events, &snapshot, &stage_plan, semantics).unwrap();

            assert_eq!(resume.cursor.stage, StageId::Implementation);
            assert_eq!(resume.cursor.cycle, 1);
            // After 3 retryable failures, resume should be at attempt 4,
            // not 1 (which would replenish the retry budget).
            assert_eq!(resume.cursor.attempt, 4);
        }

        #[test]
        fn resume_without_prior_failures_starts_at_attempt_1() {
            let run_id = RunId::new("test-run-2".to_owned()).unwrap();
            let stages = stage_plan_for_flow(FlowPreset::Standard, false);
            let stage_plan = make_stage_plan(&stages);
            let semantics = flow_semantics(FlowPreset::Standard);

            let events: Vec<JournalEvent> = vec![
                journal::run_started_event(1, Utc::now(), &run_id, StageId::Planning, 20),
                journal::stage_entered_event(2, Utc::now(), &run_id, StageId::Planning, 1, 1),
                journal::stage_completed_event(
                    3,
                    Utc::now(),
                    &run_id,
                    StageId::Planning,
                    1,
                    1,
                    "payload-1",
                    "artifact-1",
                ),
            ];

            let snapshot = RunSnapshot::initial(20);
            let resume =
                derive_resume_state(&run_id, &events, &snapshot, &stage_plan, semantics).unwrap();

            assert_eq!(resume.cursor.stage, StageId::Implementation);
            assert_eq!(resume.cursor.attempt, 1);
        }

        #[test]
        fn terminal_failure_clears_retry_tracking() {
            // SC-RESUME-005: when a stage emits will_retry=false (terminal
            // failure or retries exhausted), the retry counter must be cleared
            // so that a resumed run starts at attempt 1, not at the stale
            // counter value + 1.
            let run_id = RunId::new("test-run-terminal".to_owned()).unwrap();
            let stages = stage_plan_for_flow(FlowPreset::Standard, false);
            let stage_plan = make_stage_plan(&stages);
            let semantics = flow_semantics(FlowPreset::Standard);

            let events: Vec<JournalEvent> = vec![
                journal::run_started_event(1, Utc::now(), &run_id, StageId::Planning, 20),
                journal::stage_entered_event(2, Utc::now(), &run_id, StageId::Planning, 1, 1),
                journal::stage_completed_event(
                    3,
                    Utc::now(),
                    &run_id,
                    StageId::Planning,
                    1,
                    1,
                    "payload-1",
                    "artifact-1",
                ),
                journal::stage_entered_event(4, Utc::now(), &run_id, StageId::Implementation, 1, 1),
                // 4 retryable failures accumulate
                journal::stage_failed_event(
                    5,
                    Utc::now(),
                    &run_id,
                    StageId::Implementation,
                    1,
                    1,
                    FailureClass::TransportFailure,
                    "transient 1",
                    true,
                    "test-invocation",
                ),
                journal::stage_failed_event(
                    6,
                    Utc::now(),
                    &run_id,
                    StageId::Implementation,
                    1,
                    2,
                    FailureClass::TransportFailure,
                    "transient 2",
                    true,
                    "test-invocation",
                ),
                journal::stage_failed_event(
                    7,
                    Utc::now(),
                    &run_id,
                    StageId::Implementation,
                    1,
                    3,
                    FailureClass::TransportFailure,
                    "transient 3",
                    true,
                    "test-invocation",
                ),
                journal::stage_failed_event(
                    8,
                    Utc::now(),
                    &run_id,
                    StageId::Implementation,
                    1,
                    4,
                    FailureClass::TransportFailure,
                    "transient 4",
                    true,
                    "test-invocation",
                ),
                // Terminal failure: will_retry=false (retries exhausted)
                journal::stage_failed_event(
                    9,
                    Utc::now(),
                    &run_id,
                    StageId::Implementation,
                    1,
                    5,
                    FailureClass::TransportFailure,
                    "retries exhausted",
                    false,
                    "test-invocation",
                ),
            ];

            let snapshot = RunSnapshot::initial(20);
            let resume =
                derive_resume_state(&run_id, &events, &snapshot, &stage_plan, semantics).unwrap();

            assert_eq!(resume.cursor.stage, StageId::Implementation);
            assert_eq!(resume.cursor.cycle, 1);
            // After terminal failure (will_retry=false), resume must start
            // at attempt 1, NOT 5+1=6 from the stale retryable counter.
            assert_eq!(resume.cursor.attempt, 1);
        }

        #[test]
        fn completion_round_advance_resets_retry_tracking() {
            let run_id = RunId::new("test-run-3".to_owned()).unwrap();
            let stages = stage_plan_for_flow(FlowPreset::Standard, false);
            let stage_plan = make_stage_plan(&stages);
            let semantics = flow_semantics(FlowPreset::Standard);

            let events: Vec<JournalEvent> = vec![
                journal::run_started_event(1, Utc::now(), &run_id, StageId::Planning, 20),
                // Round 1: planning fails at attempt 3 with will_retry
                journal::stage_entered_event(2, Utc::now(), &run_id, StageId::Planning, 1, 1),
                journal::stage_failed_event(
                    3,
                    Utc::now(),
                    &run_id,
                    StageId::Planning,
                    1,
                    1,
                    FailureClass::TransportFailure,
                    "fail 1",
                    true,
                    "test-invocation",
                ),
                journal::stage_failed_event(
                    4,
                    Utc::now(),
                    &run_id,
                    StageId::Planning,
                    1,
                    2,
                    FailureClass::TransportFailure,
                    "fail 2",
                    true,
                    "test-invocation",
                ),
                // Planning eventually succeeds at attempt 3
                journal::stage_completed_event(
                    5,
                    Utc::now(),
                    &run_id,
                    StageId::Planning,
                    1,
                    3,
                    "payload-1",
                    "artifact-1",
                ),
                // Implementation succeeds
                journal::stage_entered_event(6, Utc::now(), &run_id, StageId::Implementation, 1, 1),
                journal::stage_completed_event(
                    7,
                    Utc::now(),
                    &run_id,
                    StageId::Implementation,
                    1,
                    1,
                    "payload-2",
                    "artifact-2",
                ),
                // Completion round advances, revisiting planning
                journal::completion_round_advanced_event(
                    8,
                    Utc::now(),
                    &run_id,
                    StageId::Implementation,
                    1,
                    2,
                    0,
                    20,
                ),
            ];

            let snapshot = RunSnapshot::initial(20);
            let resume =
                derive_resume_state(&run_id, &events, &snapshot, &stage_plan, semantics).unwrap();

            assert_eq!(resume.cursor.stage, StageId::Planning);
            // Round 2: planning should start at attempt 1, not 3
            // (the prior round's failures should not carry over).
            assert_eq!(resume.cursor.attempt, 1);
        }
    }

    struct NoPendingAmendmentQueue;

    #[derive(Clone, Default)]
    struct NoopAgentExecutionAdapter;

    impl AgentExecutionPort for NoopAgentExecutionAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &crate::contexts::agent_execution::InvocationContract,
        ) -> AppResult<()> {
            Ok(())
        }

        async fn check_availability(&self, _backend: &ResolvedBackendTarget) -> AppResult<()> {
            Ok(())
        }

        async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
            Ok(InvocationEnvelope {
                raw_output_reference: RawOutputReference::Inline(r#"{"status":"ok"}"#.to_owned()),
                parsed_payload: serde_json::json!({"status": "ok"}),
                metadata: InvocationMetadata {
                    invocation_id: request.invocation_id,
                    duration: std::time::Duration::ZERO,
                    token_counts: TokenCounts::default(),
                    backend_used: request.resolved_target.backend.clone(),
                    model_used: request.resolved_target.model.clone(),
                    adapter_reported_backend: None,
                    adapter_reported_model: None,
                    attempt_number: 0,
                    session_id: None,
                    session_reused: false,
                },
                timestamp: Utc::now(),
            })
        }

        async fn cancel(&self, _invocation_id: &str) -> AppResult<()> {
            Ok(())
        }
    }

    impl crate::contexts::project_run_record::service::AmendmentQueuePort for NoPendingAmendmentQueue {
        fn write_amendment(
            &self,
            _base_dir: &Path,
            _project_id: &ProjectId,
            _amendment: &crate::contexts::project_run_record::model::QueuedAmendment,
        ) -> AppResult<()> {
            Ok(())
        }

        fn list_pending_amendments(
            &self,
            _base_dir: &Path,
            _project_id: &ProjectId,
        ) -> AppResult<Vec<crate::contexts::project_run_record::model::QueuedAmendment>> {
            Ok(Vec::new())
        }

        fn remove_amendment(
            &self,
            _base_dir: &Path,
            _project_id: &ProjectId,
            _amendment_id: &str,
        ) -> AppResult<()> {
            Ok(())
        }

        fn drain_amendments(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<u32> {
            Ok(0)
        }

        fn has_pending_amendments(
            &self,
            _base_dir: &Path,
            _project_id: &ProjectId,
        ) -> AppResult<bool> {
            Ok(false)
        }
    }

    struct AppendFailsJournalStore;

    impl crate::contexts::project_run_record::service::JournalStorePort for AppendFailsJournalStore {
        fn read_journal(
            &self,
            _base_dir: &Path,
            _project_id: &ProjectId,
        ) -> AppResult<Vec<crate::contexts::project_run_record::model::JournalEvent>> {
            Ok(Vec::new())
        }

        fn append_event(
            &self,
            _base_dir: &Path,
            _project_id: &ProjectId,
            _line: &str,
        ) -> AppResult<()> {
            Err(AppError::Io(std::io::Error::other(
                "forced journal append failure",
            )))
        }
    }

    fn running_snapshot_for_pid_cleanup(run_id: &crate::shared::domain::RunId) -> RunSnapshot {
        RunSnapshot {
            active_run: Some(ActiveRun {
                run_id: run_id.as_str().to_owned(),
                stage_cursor: crate::shared::domain::StageCursor::initial(StageId::Planning),
                started_at: Utc::now(),
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            }),
            interrupted_run: None,
            status: RunStatus::Running,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "running: planning".to_owned(),
            last_stage_resolution_snapshot: None,
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn resume_run_with_retry_keeps_live_legacy_pid_only_snapshot_running() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime");

        runtime.block_on(async {
            let temp_dir = tempdir().expect("create temp dir");
            initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");
            let effective_config =
                EffectiveConfig::load(temp_dir.path()).expect("load effective config");
            let project_id = ProjectId::new("resume-live-legacy-pid").expect("project id");
            let created_at = Utc::now();

            create_project(
                &FsProjectStore,
                &FsJournalStore,
                temp_dir.path(),
                CreateProjectInput {
                    id: project_id.clone(),
                    name: "Resume legacy pid".to_owned(),
                    flow: FlowPreset::Standard,
                    prompt_path: "prompt.md".to_owned(),
                    prompt_contents: "prompt".to_owned(),
                    prompt_hash: "prompt-hash".to_owned(),
                    created_at,
                    task_source: None,
                },
            )
            .expect("create project");

            let run_id =
                crate::shared::domain::RunId::new("run-resume-live-legacy-pid").expect("run id");
            let mut snapshot = running_snapshot_for_pid_cleanup(&run_id);
            let run_started_at = Utc::now();
            snapshot
                .active_run
                .as_mut()
                .expect("active run")
                .started_at = run_started_at;
            FsRunSnapshotWriteStore
                .write_run_snapshot(temp_dir.path(), &project_id, &snapshot)
                .expect("write running snapshot");

            let legacy_pid_record = RunPidRecord {
                pid: std::process::id(),
                started_at: Utc::now(),
                owner: RunPidOwner::Cli,
                writer_owner: None,
                run_id: None,
                run_started_at: None,
                proc_start_ticks: None,
                proc_start_marker: None,
            };
            FileSystem::write_atomic(
                &FileSystem::live_project_root(temp_dir.path(), &project_id).join("run.pid"),
                &serde_json::to_string_pretty(&legacy_pid_record).expect("serialize pid file"),
            )
            .expect("write legacy pid file");

            let agent_service =
                AgentExecutionService::new(NoopAgentExecutionAdapter, FsRawOutputStore, FsSessionStore);
            let error = resume_run_with_retry(
                &agent_service,
                &FsRunSnapshotStore,
                &FsRunSnapshotWriteStore,
                &FsJournalStore,
                &FsArtifactStore,
                &FsPayloadArtifactWriteStore,
                &FsRuntimeLogWriteStore,
                &NoPendingAmendmentQueue,
                temp_dir.path(),
                None,
                &project_id,
                None,
                FlowPreset::Standard,
                &effective_config,
                &RetryPolicy::default_policy(),
                crate::contexts::agent_execution::CancellationToken::new(),
            )
            .await
            .expect_err("live legacy pid should block resume instead of reconciling to failed");

            assert!(matches!(
                error,
                AppError::ResumeFailed { reason }
                    if reason == "project already has a running run; `run resume` only works from failed or paused snapshots"
            ));

            let final_snapshot = FsRunSnapshotStore
                .read_run_snapshot(temp_dir.path(), &project_id)
                .expect("read final snapshot");
            assert_eq!(final_snapshot.status, RunStatus::Running);
            assert!(
                FileSystem::read_pid_file(temp_dir.path(), &project_id)
                    .expect("read pid file")
                    .is_some(),
                "resume should keep a live legacy pid record instead of reconciling it away"
            );
        });
    }

    #[test]
    fn complete_run_removes_pid_file_when_run_completed_append_fails() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");
        let project_id = ProjectId::new("pid-cleanup-complete").expect("project id");
        let run_id = crate::shared::domain::RunId::new("run-pid-cleanup-complete").expect("run id");
        let mut snapshot = running_snapshot_for_pid_cleanup(&run_id);
        let mut seq = 0;

        FileSystem::write_pid_file(
            temp_dir.path(),
            &project_id,
            RunPidOwner::Cli,
            None,
            None,
            None,
        )
        .expect("write pid file");

        let err = complete_run(
            &mut snapshot,
            &crate::adapters::fs::FsRunSnapshotWriteStore,
            &AppendFailsJournalStore,
            &NoPendingAmendmentQueue,
            temp_dir.path(),
            &project_id,
            &run_id,
            &mut seq,
        )
        .expect_err("completion append should fail");

        assert!(
            err.to_string().contains("forced journal append failure"),
            "unexpected error: {err}"
        );
        assert!(
            FileSystem::read_pid_file(temp_dir.path(), &project_id)
                .expect("read pid file")
                .is_none(),
            "completion should remove run.pid before returning even when run_completed append fails"
        );
    }

    #[test]
    fn pause_run_removes_pid_file_when_snapshot_is_paused() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");
        let project_id = ProjectId::new("pid-cleanup-pause").expect("project id");
        let run_id = crate::shared::domain::RunId::new("run-pid-cleanup-pause").expect("run id");
        let mut snapshot = running_snapshot_for_pid_cleanup(&run_id);

        crate::adapters::fs::FsRunSnapshotWriteStore
            .write_run_snapshot(temp_dir.path(), &project_id, &snapshot)
            .expect("write running snapshot");
        FileSystem::write_pid_file(
            temp_dir.path(),
            &project_id,
            RunPidOwner::Cli,
            None,
            None,
            None,
        )
        .expect("write pid file");

        pause_run(
            &mut snapshot,
            &crate::adapters::fs::FsRunSnapshotWriteStore,
            temp_dir.path(),
            &project_id,
            "paused for test".to_owned(),
        )
        .expect("pause run");

        assert_eq!(snapshot.status, RunStatus::Paused);
        assert!(
            FileSystem::read_pid_file(temp_dir.path(), &project_id)
                .expect("read pid file")
                .is_none(),
            "pausing a run should remove run.pid before returning"
        );
    }

    #[test]
    fn mark_running_run_interrupted_preserves_pid_file_when_snapshot_is_not_running() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");
        let project_id = ProjectId::new("pid-preserved-on-stale-race").expect("project id");
        let mut snapshot = RunSnapshot::initial(20);
        snapshot.status = RunStatus::Failed;
        snapshot.status_summary = "failed: already reconciled".to_owned();

        crate::adapters::fs::FsRunSnapshotWriteStore
            .write_run_snapshot(temp_dir.path(), &project_id, &snapshot)
            .expect("write failed snapshot");
        FileSystem::write_pid_file(
            temp_dir.path(),
            &project_id,
            RunPidOwner::Cli,
            None,
            None,
            None,
        )
        .expect("write pid file");
        let expected_attempt = RunningAttemptIdentity {
            run_id: "run-stale-attempt".to_owned(),
            started_at: Utc::now(),
        };

        let updated = mark_running_run_interrupted(
            InterruptedRunContext {
                run_snapshot_read: &crate::adapters::fs::FsRunSnapshotStore,
                run_snapshot_write: &crate::adapters::fs::FsRunSnapshotWriteStore,
                journal_store: &crate::adapters::fs::FsJournalStore,
                log_write: &crate::adapters::fs::FsRuntimeLogWriteStore,
                base_dir: temp_dir.path(),
                project_id: &project_id,
            },
            &expected_attempt,
            InterruptedRunUpdate {
                summary: "failed (stale running snapshot recovered for resume)",
                log_message: "should be ignored because the snapshot is no longer running",
                failure_class: Some("interruption"),
            },
        )
        .expect("mark interrupted");

        assert!(!updated, "non-running snapshots should not be rewritten");
        assert!(
            FileSystem::read_pid_file(temp_dir.path(), &project_id)
                .expect("read pid file")
                .is_some(),
            "pid file should remain untouched when another process already reconciled the snapshot"
        );
    }

    #[test]
    fn mark_running_run_interrupted_appends_run_failed_event() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");
        let project_id = ProjectId::new("interrupted-journal").expect("project id");
        let run_id = crate::shared::domain::RunId::new("run-interrupted-journal").expect("run id");
        let snapshot = running_snapshot_for_pid_cleanup(&run_id);

        crate::adapters::fs::FsRunSnapshotWriteStore
            .write_run_snapshot(temp_dir.path(), &project_id, &snapshot)
            .expect("write running snapshot");
        crate::adapters::fs::FsJournalStore
            .append_event(
                temp_dir.path(),
                &project_id,
                &format!(
                    "{{\"sequence\":1,\"timestamp\":\"{}\",\"event_type\":\"project_created\",\"details\":{{\"project_id\":\"{}\",\"flow\":\"standard\"}}}}",
                    Utc::now().to_rfc3339(),
                    project_id
                ),
            )
            .expect("append project_created event");
        let expected_attempt = RunningAttemptIdentity::from_active_run(
            snapshot.active_run.as_ref().expect("active run"),
        );

        let updated = mark_running_run_interrupted(
            InterruptedRunContext {
                run_snapshot_read: &crate::adapters::fs::FsRunSnapshotStore,
                run_snapshot_write: &crate::adapters::fs::FsRunSnapshotWriteStore,
                journal_store: &crate::adapters::fs::FsJournalStore,
                log_write: &crate::adapters::fs::FsRuntimeLogWriteStore,
                base_dir: temp_dir.path(),
                project_id: &project_id,
            },
            &expected_attempt,
            InterruptedRunUpdate {
                summary: "failed (stopped by user; run `ralph-burning run resume` to continue)",
                log_message:
                    "run stop interrupted the orchestrator; outcome=terminated gracefully with SIGTERM",
                failure_class: Some("cancellation"),
            },
        )
        .expect("mark interrupted");

        assert!(updated, "running snapshot should be rewritten");
        let events = crate::adapters::fs::FsJournalStore
            .read_journal(temp_dir.path(), &project_id)
            .expect("read journal");
        let run_failed = events.last().expect("run_failed event");
        assert_eq!(run_failed.event_type, JournalEventType::RunFailed);
        assert_eq!(run_failed.details["run_id"], run_id.as_str());
        assert_eq!(run_failed.details["failure_class"], "cancellation");
    }

    #[test]
    fn mark_running_run_interrupted_removes_pid_file_when_run_failed_append_fails() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");
        let project_id = ProjectId::new("interrupted-pid-cleanup").expect("project id");
        let run_id =
            crate::shared::domain::RunId::new("run-interrupted-pid-cleanup").expect("run id");
        let snapshot = running_snapshot_for_pid_cleanup(&run_id);
        let expected_attempt = RunningAttemptIdentity::from_active_run(
            snapshot.active_run.as_ref().expect("active run"),
        );

        crate::adapters::fs::FsRunSnapshotWriteStore
            .write_run_snapshot(temp_dir.path(), &project_id, &snapshot)
            .expect("write running snapshot");
        FileSystem::write_pid_file(
            temp_dir.path(),
            &project_id,
            RunPidOwner::Cli,
            Some("lease-interrupted"),
            Some(run_id.as_str()),
            Some(expected_attempt.started_at),
        )
        .expect("write pid file");

        let error = mark_running_run_interrupted(
            InterruptedRunContext {
                run_snapshot_read: &crate::adapters::fs::FsRunSnapshotStore,
                run_snapshot_write: &crate::adapters::fs::FsRunSnapshotWriteStore,
                journal_store: &AppendFailsJournalStore,
                log_write: &crate::adapters::fs::FsRuntimeLogWriteStore,
                base_dir: temp_dir.path(),
                project_id: &project_id,
            },
            &expected_attempt,
            InterruptedRunUpdate {
                summary: "failed (interrupted)",
                log_message: "forced interruption cleanup path",
                failure_class: Some("cancellation"),
            },
        )
        .expect_err("forced journal append failure should bubble up");

        assert!(
            error.to_string().contains("forced journal append failure"),
            "unexpected error: {error}"
        );
        assert!(
            FileSystem::read_pid_file(temp_dir.path(), &project_id)
                .expect("read pid file")
                .is_none(),
            "pid file should be removed even when run_failed journal append fails"
        );
    }

    #[test]
    fn mark_running_run_interrupted_skips_newer_running_attempt_after_stale_resume_race() {
        let temp_dir = tempdir().expect("create temp dir");
        initialize_workspace(temp_dir.path(), Utc::now()).expect("initialize workspace");
        let project_id = ProjectId::new("stale-resume-race").expect("project id");
        let stale_run_id =
            crate::shared::domain::RunId::new("run-stale-resume-race").expect("stale run id");
        let fresh_run_id =
            crate::shared::domain::RunId::new("run-fresh-resume-race").expect("fresh run id");
        let stale_started_at = Utc::now();
        let fresh_started_at = stale_started_at + chrono::Duration::minutes(5);

        let mut stale_snapshot = running_snapshot_for_pid_cleanup(&stale_run_id);
        stale_snapshot
            .active_run
            .as_mut()
            .expect("stale active run")
            .started_at = stale_started_at;
        let expected_attempt = RunningAttemptIdentity::from_active_run(
            stale_snapshot
                .active_run
                .as_ref()
                .expect("stale active run"),
        );

        let mut fresh_snapshot = running_snapshot_for_pid_cleanup(&fresh_run_id);
        fresh_snapshot
            .active_run
            .as_mut()
            .expect("fresh active run")
            .started_at = fresh_started_at;

        crate::adapters::fs::FsRunSnapshotWriteStore
            .write_run_snapshot(temp_dir.path(), &project_id, &fresh_snapshot)
            .expect("write fresh running snapshot");
        FileSystem::write_pid_file(
            temp_dir.path(),
            &project_id,
            RunPidOwner::Cli,
            None,
            None,
            None,
        )
        .expect("write pid file");

        let updated = mark_running_run_interrupted(
            InterruptedRunContext {
                run_snapshot_read: &crate::adapters::fs::FsRunSnapshotStore,
                run_snapshot_write: &crate::adapters::fs::FsRunSnapshotWriteStore,
                journal_store: &crate::adapters::fs::FsJournalStore,
                log_write: &crate::adapters::fs::FsRuntimeLogWriteStore,
                base_dir: temp_dir.path(),
                project_id: &project_id,
            },
            &expected_attempt,
            InterruptedRunUpdate {
                summary: "failed (stale running snapshot recovered for resume)",
                log_message: "should not clobber a newer running attempt",
                failure_class: Some("interruption"),
            },
        )
        .expect("mark interrupted");

        assert!(
            !updated,
            "older stale attempt should not rewrite a newer running attempt"
        );
        let final_snapshot = crate::adapters::fs::FsRunSnapshotStore
            .read_run_snapshot(temp_dir.path(), &project_id)
            .expect("read final snapshot");
        assert_eq!(final_snapshot.status, RunStatus::Running);
        let active_run = final_snapshot.active_run.expect("active run");
        assert_eq!(active_run.run_id, fresh_run_id.as_str());
        assert_eq!(active_run.started_at, fresh_started_at);
        assert!(
            FileSystem::read_pid_file(temp_dir.path(), &project_id)
                .expect("read pid file")
                .is_some(),
            "pid file should remain for the newer live attempt"
        );
    }

    #[test]
    fn force_complete_status_message_does_not_dangle_when_amendments_empty() {
        // codex-review #192 P2 finding: when force-complete fires with no
        // pending amendments we skip writing
        // `force_complete_amendments_deferred`, but the status text used to
        // claim "see force_complete_amendments_deferred event" anyway, so
        // operators (and tooling parsing run status) followed a pointer to
        // an event that doesn't exist. The empty case must use a message
        // that doesn't reference the journal event.
        let empty = super::ForceCompleteDeferredAmendments {
            round: 25,
            amendments: vec![],
        };
        let msg = empty.status_message();
        assert!(
            !msg.contains("force_complete_amendments_deferred"),
            "empty force-complete status must not name the journal event: {msg}"
        );
        assert!(
            msg.contains("no amendments deferred"),
            "empty force-complete status must say 'no amendments deferred': {msg}"
        );
    }

    #[test]
    fn force_complete_status_message_points_at_journal_event_when_amendments_present() {
        // The non-empty case MUST keep pointing at the journal event so
        // operators can recover the deferred amendments.
        let with_amendments = super::ForceCompleteDeferredAmendments {
            round: 25,
            amendments: vec![serde_json::json!({
                "id": "fr-25-deadbeef",
                "summary": "missed validation in error path",
                "classification": "fix_current_bead",
            })],
        };
        let msg = with_amendments.status_message();
        assert!(
            msg.contains("force_complete_amendments_deferred"),
            "non-empty force-complete status must reference the journal event: {msg}"
        );
        assert!(
            msg.contains("1 amendments"),
            "non-empty force-complete status must include the count: {msg}"
        );
    }
}
