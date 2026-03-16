#![forbid(unsafe_code)]

//! Resume-time prompt drift handling.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use chrono::Utc;
use serde_json::json;

use crate::adapters::fs::FileSystem;
use crate::contexts::project_run_record::journal;
use crate::contexts::project_run_record::model::{LogLevel, RunSnapshot, RuntimeLogEntry};
use crate::contexts::project_run_record::service::{
    ArtifactStorePort, JournalStorePort, PayloadArtifactWritePort, RunSnapshotWritePort,
    RuntimeLogWritePort,
};
use crate::contexts::workflow_composition::panel_contracts::RecordKind;
use crate::shared::domain::{ProjectId, PromptChangeAction, RunId, StageCursor, StageId};
use crate::shared::error::{AppError, AppResult};

pub enum PromptChangeResumeDecision {
    NoChange { current_prompt_hash: String },
    Continue { current_prompt_hash: String },
    RestartCycle {
        current_prompt_hash: String,
        next_cursor: StageCursor,
        next_stage_index: usize,
    },
}

#[allow(clippy::too_many_arguments)]
pub fn evaluate_prompt_change_on_resume(
    artifact_store: &dyn ArtifactStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    project_root: &Path,
    prompt_reference: &str,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &mut RunSnapshot,
    cursor: &StageCursor,
    current_stage_index: usize,
    stage_plan: &[StageId],
    planning_stage: StageId,
    prompt_hash_at_cycle_start: &str,
    action: PromptChangeAction,
) -> AppResult<PromptChangeResumeDecision> {
    let prompt_path = project_root.join(prompt_reference);
    let prompt = fs::read_to_string(&prompt_path).map_err(|error| AppError::CorruptRecord {
        file: prompt_path.display().to_string(),
        details: format!("failed to read prompt during resume drift check: {error}"),
    })?;
    let current_prompt_hash = FileSystem::prompt_hash(&prompt);

    if current_prompt_hash == prompt_hash_at_cycle_start {
        return Ok(PromptChangeResumeDecision::NoChange { current_prompt_hash });
    }

    match action {
        PromptChangeAction::Continue => {
            sync_project_prompt_hash(base_dir, project_id, &current_prompt_hash)?;
            emit_prompt_change_warning(
                journal_store,
                run_snapshot_write,
                log_write,
                base_dir,
                project_id,
                run_id,
                seq,
                snapshot,
                cursor.stage,
                prompt_hash_at_cycle_start,
                &current_prompt_hash,
                "continue",
                "prompt changed after the cycle started; continuing with the updated prompt",
            )?;
            Ok(PromptChangeResumeDecision::Continue { current_prompt_hash })
        }
        PromptChangeAction::Abort => Err(AppError::ResumeFailed {
            reason: format!(
                "prompt hash mismatch on resume: cycle started with '{prompt_hash_at_cycle_start}' but current prompt hash is '{current_prompt_hash}'"
            ),
        }),
        PromptChangeAction::RestartCycle => {
            clear_abandoned_supporting_records(
                artifact_store,
                artifact_write,
                base_dir,
                project_id,
                cursor.cycle,
                cursor.completion_round,
                &stage_plan[current_stage_index..],
            )?;
            sync_project_prompt_hash(base_dir, project_id, &current_prompt_hash)?;
            snapshot.last_stage_resolution_snapshot = None;
            emit_prompt_change_warning(
                journal_store,
                run_snapshot_write,
                log_write,
                base_dir,
                project_id,
                run_id,
                seq,
                snapshot,
                cursor.stage,
                prompt_hash_at_cycle_start,
                &current_prompt_hash,
                "restart_cycle",
                "prompt changed after the cycle started; restarting the cycle from planning",
            )?;

            let next_stage_index = stage_plan
                .iter()
                .position(|stage_id| *stage_id == planning_stage)
                .ok_or_else(|| AppError::CorruptRecord {
                    file: "journal.ndjson".to_owned(),
                    details: format!(
                        "planning stage '{}' is not part of the active stage plan",
                        planning_stage.as_str()
                    ),
                })?;
            let next_cursor = StageCursor::new(
                planning_stage,
                cursor.cycle,
                1,
                cursor.completion_round,
            )?;

            Ok(PromptChangeResumeDecision::RestartCycle {
                current_prompt_hash,
                next_cursor,
                next_stage_index,
            })
        }
    }
}

fn clear_abandoned_supporting_records(
    artifact_store: &dyn ArtifactStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    cycle: u32,
    completion_round: u32,
    abandoned_stages: &[StageId],
) -> AppResult<()> {
    let stage_lookup: std::collections::HashSet<StageId> =
        abandoned_stages.iter().copied().collect();
    let payloads = artifact_store.list_payloads(base_dir, project_id)?;
    let artifacts = artifact_store.list_artifacts(base_dir, project_id)?;
    let artifact_ids_by_payload: HashMap<String, String> = artifacts
        .into_iter()
        .map(|artifact| (artifact.payload_id, artifact.artifact_id))
        .collect();

    for payload in payloads {
        if payload.record_kind != RecordKind::StageSupporting
            || payload.cycle != cycle
            || payload.completion_round != completion_round
            || !stage_lookup.contains(&payload.stage_id)
        {
            continue;
        }

        let artifact_id = artifact_ids_by_payload
            .get(&payload.payload_id)
            .cloned()
            .unwrap_or_else(|| format!("{}-artifact", payload.payload_id));
        artifact_write.remove_payload_artifact_pair(
            base_dir,
            project_id,
            &payload.payload_id,
            &artifact_id,
        )?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_prompt_change_warning(
    journal_store: &dyn JournalStorePort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    snapshot: &mut RunSnapshot,
    stage_id: StageId,
    old_hash: &str,
    new_hash: &str,
    action: &str,
    message: &str,
) -> AppResult<()> {
    let _ = log_write.append_runtime_log(
        base_dir,
        project_id,
        &RuntimeLogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Warn,
            source: "engine".to_owned(),
            message: message.to_owned(),
        },
    );

    *seq += 1;
    let event = journal::durable_warning_event(
        *seq,
        Utc::now(),
        run_id,
        stage_id,
        "prompt_change",
        message,
        json!({
            "action": action,
            "old_prompt_hash": old_hash,
            "new_prompt_hash": new_hash,
        }),
    );
    let line = journal::serialize_event(&event)?;
    if let Err(error) = journal_store.append_event(base_dir, project_id, &line) {
        *seq -= 1;
        return Err(AppError::StageCommitFailed {
            stage_id,
            details: format!(
                "prompt change warning must be durably persisted before resume continues: {error}"
            ),
        });
    }

    run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)?;
    Ok(())
}

fn sync_project_prompt_hash(
    base_dir: &Path,
    project_id: &ProjectId,
    prompt_hash: &str,
) -> AppResult<()> {
    let project_dir = base_dir
        .join(".ralph-burning")
        .join("projects")
        .join(project_id.as_str());
    let project_toml = project_dir.join("project.toml");
    let content = fs::read_to_string(&project_toml).map_err(|error| AppError::CorruptRecord {
        file: project_toml.display().to_string(),
        details: format!("failed to read project metadata for prompt-hash update: {error}"),
    })?;
    let mut record: crate::contexts::project_run_record::model::ProjectRecord =
        toml::from_str(&content).map_err(|error| AppError::CorruptRecord {
            file: project_toml.display().to_string(),
            details: format!("failed to parse project metadata for prompt-hash update: {error}"),
        })?;
    record.prompt_hash = prompt_hash.to_owned();
    let updated = toml::to_string_pretty(&record)?;
    FileSystem::write_atomic(&project_toml, &updated).map_err(|error| {
        AppError::PromptReplacementFailed {
            details: format!("failed to persist updated prompt hash: {error}"),
        }
    })
}
