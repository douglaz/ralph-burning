use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Utc;

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
    ActiveRun, ArtifactRecord, LogLevel, PayloadRecord, RunSnapshot, RunStatus, RuntimeLogEntry,
};
use crate::contexts::project_run_record::service::{
    JournalStorePort, PayloadArtifactWritePort, RunSnapshotPort, RunSnapshotWritePort,
    RuntimeLogWritePort,
};
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::shared::domain::{
    BackendRole, FlowPreset, ProjectId, ResolvedBackendTarget, RunId, SessionPolicy, StageCursor,
    StageId,
};
use crate::shared::error::{AppError, AppResult};

use super::contracts;

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

/// The standard-flow orchestration engine.
///
/// Executes all enabled standard stages in canonical order against the provided
/// backend adapter, persisting validated payloads and rendered artifacts at
/// durable stage boundaries.
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
    // 1. Read current snapshot and validate preconditions
    let snapshot = run_snapshot_read.read_run_snapshot(base_dir, project_id)?;

    if snapshot.status != RunStatus::NotStarted {
        return Err(AppError::RunStartFailed {
            reason: format!(
                "project snapshot status is '{}'; run start requires 'not_started'",
                snapshot.status
            ),
        });
    }
    if snapshot.has_active_run() {
        return Err(AppError::RunStartFailed {
            reason: "project already has an active run".to_owned(),
        });
    }

    // 2. Derive the stage plan
    let prompt_review_enabled = effective_config.prompt_review_enabled();
    let stages = standard_stage_plan(prompt_review_enabled);

    // 3. Resolve backend targets for every stage
    let workspace_defaults = BackendSelectionConfig::from_effective_config(effective_config)?;
    let stage_plan = resolve_stage_plan(
        stages.as_slice(),
        agent_service.resolver(),
        Some(&workspace_defaults),
    )?;

    // 4. Preflight checks — if any fail, leave all state unchanged
    preflight_check(agent_service.adapter(), &stage_plan).await?;

    // 5. Generate run ID and read journal for sequence tracking
    let run_id = generate_run_id()?;
    let now = Utc::now();
    let events = journal_store.read_journal(base_dir, project_id)?;
    let mut seq = journal::last_sequence(&events);

    let first_stage = stage_plan[0].stage_id;

    // 6. Persist run_started: write run.json with running status FIRST, then journal events.
    // The snapshot must be durable before journal events become visible.
    let initial_cursor = StageCursor::initial(first_stage);
    let mut current_snapshot = RunSnapshot {
        active_run: Some(ActiveRun {
            run_id: run_id.as_str().to_owned(),
            stage_cursor: initial_cursor.clone(),
            started_at: now,
        }),
        status: RunStatus::Running,
        cycle_history: vec![],
        completion_rounds: 1,
        rollback_point_meta: snapshot.rollback_point_meta.clone(),
        amendment_queue: snapshot.amendment_queue.clone(),
        status_summary: format!("running: {}", first_stage.display_name()),
    };
    run_snapshot_write.write_run_snapshot(base_dir, project_id, &current_snapshot)?;

    seq += 1;
    let run_started = journal::run_started_event(seq, now, &run_id, first_stage);
    let run_started_line = journal::serialize_event(&run_started)?;
    if let Err(e) = journal_store.append_event(base_dir, project_id, &run_started_line) {
        seq -= 1; // event was not persisted
        return fail_run(
            &AppError::RunStartFailed {
                reason: format!("failed to persist run_started event: {}", e),
            },
            first_stage,
            &run_id,
            &mut seq,
            &mut current_snapshot,
            journal_store,
            run_snapshot_write,
            base_dir,
            project_id,
        )
        .await;
    }

    let project_root = project_root_path(base_dir, project_id);

    // 7. Execute stages in order
    for (idx, stage_entry) in stage_plan.iter().enumerate() {
        let stage_id = stage_entry.stage_id;
        let cursor = StageCursor::initial(stage_id);

        // Emit stage_entered — if journal append fails after run_started,
        // the run must persist failed state at this stage boundary.
        seq += 1;
        let stage_entered = journal::stage_entered_event(
            seq,
            Utc::now(),
            &run_id,
            stage_id,
            cursor.cycle,
            cursor.attempt,
        );
        let stage_entered_line = journal::serialize_event(&stage_entered)?;
        if let Err(e) = journal_store.append_event(base_dir, project_id, &stage_entered_line) {
            seq -= 1; // event was not persisted
            return fail_run(
                &AppError::RunStartFailed {
                    reason: format!(
                        "failed to persist stage_entered event for {}: {}",
                        stage_id.as_str(),
                        e
                    ),
                },
                stage_id,
                &run_id,
                &mut seq,
                &mut current_snapshot,
                journal_store,
                run_snapshot_write,
                base_dir,
                project_id,
            )
            .await;
        }

        // Update cursor in snapshot — if this fails after stage_entered,
        // the run must persist failed state at this stage boundary.
        current_snapshot.active_run.as_mut().unwrap().stage_cursor = cursor.clone();
        current_snapshot.status_summary = format!("running: {}", stage_id.display_name());
        if let Err(e) =
            run_snapshot_write.write_run_snapshot(base_dir, project_id, &current_snapshot)
        {
            return fail_run(
                &AppError::RunStartFailed {
                    reason: format!(
                        "failed to update snapshot for stage {}: {}",
                        stage_id.as_str(),
                        e
                    ),
                },
                stage_id,
                &run_id,
                &mut seq,
                &mut current_snapshot,
                journal_store,
                run_snapshot_write,
                base_dir,
                project_id,
            )
            .await;
        }

        // Best-effort runtime log
        let _ = log_write.append_runtime_log(
            base_dir,
            project_id,
            &RuntimeLogEntry {
                timestamp: Utc::now(),
                level: LogLevel::Info,
                source: "engine".to_owned(),
                message: format!("stage_entered: {}", stage_id.as_str()),
            },
        );

        // Invoke the backend
        let invocation_id = format!(
            "{}-{}-c{}-a{}",
            run_id.as_str(),
            stage_id.as_str(),
            cursor.cycle,
            cursor.attempt
        );
        let request = InvocationRequest {
            invocation_id,
            project_root: project_root.clone(),
            stage_contract: stage_entry.contract,
            role: stage_entry.role,
            resolved_target: stage_entry.target.clone(),
            payload: InvocationPayload {
                prompt: format!("Execute stage: {}", stage_id.display_name()),
                context: serde_json::json!({}),
            },
            timeout: Duration::from_secs(300),
            cancellation_token: CancellationToken::new(),
            session_policy: SessionPolicy::ReuseIfAllowed,
            prior_session: None,
            attempt_number: cursor.attempt,
        };

        let envelope = match agent_service.invoke(request).await {
            Ok(env) => env,
            Err(e) => {
                return fail_run(
                    &e,
                    stage_id,
                    &run_id,
                    &mut seq,
                    &mut current_snapshot,
                    journal_store,
                    run_snapshot_write,
                    base_dir,
                    project_id,
                )
                .await;
            }
        };

        // Evaluate the contract (schema + domain + outcome + render)
        let bundle = match stage_entry.contract.evaluate(&envelope.parsed_payload) {
            Ok(bundle) => bundle,
            Err(contract_err) => {
                let app_err = AppError::InvocationFailed {
                    backend: stage_entry.target.backend.family.to_string(),
                    stage_id,
                    failure_class: contract_err.failure_class(),
                    details: contract_err.to_string(),
                };
                return fail_run(
                    &app_err,
                    stage_id,
                    &run_id,
                    &mut seq,
                    &mut current_snapshot,
                    journal_store,
                    run_snapshot_write,
                    base_dir,
                    project_id,
                )
                .await;
            }
        };

        // Persist payload + artifact pair atomically
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

        // Atomic stage commit: payload + artifact + snapshot + journal.
        // All four must succeed for the stage to become durably visible.
        // Order: payload/artifact → snapshot cursor → stage_completed journal.
        // This ensures that if any step fails, no stage_completed event leaks
        // into the journal without a matching snapshot and durable files.

        // Step 1: Write payload + artifact pair.
        // If this fails after stage_entered, the run must persist failed state.
        // The fs adapter uses staging + rename so canonical files only appear on
        // full success, but as defense-in-depth we also call remove_payload_artifact_pair
        // to clean up any orphaned canonical files before failing the run.
        if let Err(e) =
            artifact_write.write_payload_artifact_pair(base_dir, project_id, &payload_record, &artifact_record)
        {
            // Defense-in-depth: remove any leaked canonical files.
            let _ = artifact_write.remove_payload_artifact_pair(
                base_dir,
                project_id,
                &payload_id,
                &artifact_id,
            );
            let commit_err = AppError::StageCommitFailed {
                stage_id,
                details: format!("payload/artifact persistence failed: {}", e),
            };
            return fail_run(
                &commit_err,
                stage_id,
                &run_id,
                &mut seq,
                &mut current_snapshot,
                journal_store,
                run_snapshot_write,
                base_dir,
                project_id,
            )
            .await;
        }

        // Step 2: Update snapshot cursor to reflect completed stage.
        // Written BEFORE stage_completed journal so that snapshot failure
        // never leaves a stage_completed event without a matching cursor.
        let pre_commit_snapshot = current_snapshot.clone();
        if idx + 1 < stage_plan.len() {
            let next_stage = stage_plan[idx + 1].stage_id;
            current_snapshot.active_run.as_mut().unwrap().stage_cursor =
                StageCursor::initial(next_stage);
            current_snapshot.status_summary = format!(
                "running: completed {}, next {}",
                stage_id.display_name(),
                next_stage.display_name()
            );
        }
        if let Err(e) = run_snapshot_write.write_run_snapshot(base_dir, project_id, &current_snapshot) {
            // Roll back payload/artifact; no stage_completed was written yet.
            // Propagate rollback failure so leaked durable history is surfaced.
            let rollback_detail = match artifact_write.remove_payload_artifact_pair(base_dir, project_id, &payload_id, &artifact_id) {
                Ok(()) => String::new(),
                Err(cleanup_err) => format!("; payload/artifact rollback failed: {} — leaked durable history may exist", cleanup_err),
            };
            current_snapshot = pre_commit_snapshot;
            let commit_err = AppError::StageCommitFailed {
                stage_id,
                details: format!("snapshot write failed during stage commit: {}{}", e, rollback_detail),
            };
            return fail_run(
                &commit_err,
                stage_id,
                &run_id,
                &mut seq,
                &mut current_snapshot,
                journal_store,
                run_snapshot_write,
                base_dir,
                project_id,
            )
            .await;
        }

        // Step 3: Append stage_completed journal event.
        // If this fails, roll back payload/artifact and overwrite snapshot
        // via fail_run so the stage remains uncommitted.
        seq += 1;
        let stage_completed = journal::stage_completed_event(
            seq,
            Utc::now(),
            &run_id,
            stage_id,
            cursor.cycle,
            cursor.attempt,
            &payload_id,
            &artifact_id,
        );
        let stage_completed_line = journal::serialize_event(&stage_completed)?;
        if let Err(e) = journal_store.append_event(base_dir, project_id, &stage_completed_line) {
            // Roll back payload/artifact so no partial durable history is visible.
            // Propagate rollback failure so leaked durable history is surfaced.
            let rollback_detail = match artifact_write.remove_payload_artifact_pair(base_dir, project_id, &payload_id, &artifact_id) {
                Ok(()) => String::new(),
                Err(cleanup_err) => format!("; payload/artifact rollback failed: {} — leaked durable history may exist", cleanup_err),
            };
            seq -= 1; // undo the seq increment since the event was not persisted
            let commit_err = AppError::StageCommitFailed {
                stage_id,
                details: format!("journal append failed during stage commit: {}{}", e, rollback_detail),
            };
            return fail_run(
                &commit_err,
                stage_id,
                &run_id,
                &mut seq,
                &mut current_snapshot,
                journal_store,
                run_snapshot_write,
                base_dir,
                project_id,
            )
            .await;
        }

        // Best-effort runtime log
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
    }

    // 8. All stages completed — mark run as completed.
    // Write completed snapshot FIRST so the run is marked as completed
    // even if journal append fails (consistent with snapshot-first ordering).
    current_snapshot.status = RunStatus::Completed;
    current_snapshot.active_run = None;
    current_snapshot.completion_rounds = 1;
    current_snapshot.status_summary = "completed".to_owned();
    run_snapshot_write.write_run_snapshot(base_dir, project_id, &current_snapshot)?;

    seq += 1;
    let run_completed = journal::run_completed_event(seq, Utc::now(), &run_id, 1);
    let run_completed_line = journal::serialize_event(&run_completed)?;
    journal_store.append_event(base_dir, project_id, &run_completed_line)?;

    Ok(())
}

/// Record a run failure: persist failed snapshot, then journal event, return error.
///
/// The snapshot is written first (critical path) so the run is never left in an
/// ambiguous running state. The journal append is best-effort relative to the
/// snapshot — if it fails, the snapshot already reflects the failed state.
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
) -> AppResult<()> {
    let failure_class = err
        .failure_class()
        .map(|fc| format!("{:?}", fc))
        .unwrap_or_else(|| "unknown".to_owned());
    let message = err.to_string();

    // Critical: persist the failed snapshot first so the run is never
    // left in an ambiguous running state after any failure.
    snapshot.status = RunStatus::Failed;
    snapshot.active_run = None;
    snapshot.status_summary = format!("failed at {}: {}", stage_id.display_name(), message);
    run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)?;

    // Best-effort: append run_failed journal event. If this fails,
    // the snapshot already reflects the failed state.
    *seq += 1;
    let run_failed =
        journal::run_failed_event(*seq, Utc::now(), run_id, stage_id, &failure_class, &message);
    if let Ok(run_failed_line) = journal::serialize_event(&run_failed) {
        let _ = journal_store.append_event(base_dir, project_id, &run_failed_line);
    }

    Err(AppError::RunStartFailed {
        reason: format!("stage {} failed: {}", stage_id.as_str(), message),
    })
}

/// Helper to get project root path.
fn project_root_path(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
    base_dir
        .join(".ralph-burning")
        .join("projects")
        .join(project_id.as_str())
}
