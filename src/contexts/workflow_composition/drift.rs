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
    NoChange {
        current_prompt_hash: String,
    },
    Continue {
        current_prompt_hash: String,
    },
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
        return Ok(PromptChangeResumeDecision::NoChange {
            current_prompt_hash,
        });
    }

    match action {
        PromptChangeAction::Continue => {
            append_prompt_change_warning(
                journal_store,
                log_write,
                base_dir,
                project_id,
                run_id,
                seq,
                cursor.stage,
                prompt_hash_at_cycle_start,
                &current_prompt_hash,
                "continue",
                "prompt changed after the cycle started; continuing with the updated prompt",
            )?;
            sync_project_prompt_hash(base_dir, project_id, &current_prompt_hash)?;
            Ok(PromptChangeResumeDecision::Continue { current_prompt_hash })
        }
        PromptChangeAction::Abort => Err(AppError::ResumeFailed {
            reason: format!(
                "prompt hash mismatch on resume: cycle started with '{prompt_hash_at_cycle_start}' but current prompt hash is '{current_prompt_hash}'"
            ),
        }),
        PromptChangeAction::RestartCycle => {
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
            append_prompt_change_warning(
                journal_store,
                log_write,
                base_dir,
                project_id,
                run_id,
                seq,
                cursor.stage,
                prompt_hash_at_cycle_start,
                &current_prompt_hash,
                "restart_cycle",
                "prompt changed after the cycle started; restarting the cycle from planning",
            )?;
            clear_abandoned_supporting_records(
                artifact_store,
                artifact_write,
                base_dir,
                project_id,
                cursor.cycle,
                &stage_plan[next_stage_index..],
            )?;
            snapshot.last_stage_resolution_snapshot = None;
            run_snapshot_write.write_run_snapshot(base_dir, project_id, snapshot)?;
            sync_project_prompt_hash(base_dir, project_id, &current_prompt_hash)?;
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
fn append_prompt_change_warning(
    journal_store: &dyn JournalStorePort,
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
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

    let next_seq = seq.saturating_add(1);
    let event = journal::durable_warning_event(
        next_seq,
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
        return Err(AppError::StageCommitFailed {
            stage_id,
            details: format!(
                "prompt change warning must be durably persisted before resume continues: {error}"
            ),
        });
    }

    *seq = next_seq;
    Ok(())
}

fn sync_project_prompt_hash(
    base_dir: &Path,
    project_id: &ProjectId,
    prompt_hash: &str,
) -> AppResult<()> {
    let project_dir = FileSystem::project_root(base_dir, project_id);
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
    })?;
    FileSystem::mirror_project_file(base_dir, project_id, "project.toml", &updated);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    use crate::adapters::fs::{
        FileSystem, FsArtifactStore, FsJournalStore, FsPayloadArtifactWriteStore, FsProjectStore,
        FsRunSnapshotWriteStore, FsRuntimeLogWriteStore,
    };
    use crate::contexts::project_run_record::model::{
        ArtifactRecord, JournalEvent, PayloadRecord, ProjectRecord,
    };
    use crate::contexts::project_run_record::service::{
        create_project, CreateProjectInput, PayloadArtifactWritePort, ProjectStorePort,
    };
    use crate::contexts::workflow_composition::panel_contracts::{RecordKind, RecordProducer};
    use crate::shared::domain::{FlowPreset, RunId};

    struct FailingJournalStore;

    impl JournalStorePort for FailingJournalStore {
        fn read_journal(
            &self,
            base_dir: &Path,
            project_id: &ProjectId,
        ) -> AppResult<Vec<JournalEvent>> {
            FsJournalStore.read_journal(base_dir, project_id)
        }

        fn append_event(
            &self,
            _base_dir: &Path,
            _project_id: &ProjectId,
            _line: &str,
        ) -> AppResult<()> {
            Err(AppError::Io(std::io::Error::other(
                "simulated journal append failure",
            )))
        }
    }

    fn setup_project(base_dir: &Path, project_name: &str) -> AppResult<(ProjectId, RunId, String)> {
        FileSystem::create_workspace(&base_dir.join(".ralph-burning"), "", &["projects"])?;

        let project_id = ProjectId::new(project_name)?;
        let prompt_contents = "# Prompt\n\nOriginal prompt.\n";
        let prompt_hash = FileSystem::prompt_hash(prompt_contents);
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base_dir,
            CreateProjectInput {
                id: project_id.clone(),
                name: project_name.to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: prompt_contents.to_owned(),
                prompt_hash: prompt_hash.clone(),
                created_at: Utc::now(),
                task_source: None,
            },
        )?;

        Ok((project_id, RunId::new("run-1")?, prompt_hash))
    }

    fn read_project_record(base_dir: &Path, project_id: &ProjectId) -> ProjectRecord {
        FsProjectStore
            .read_project_record(base_dir, project_id)
            .expect("project record")
    }

    fn read_audit_project_record(base_dir: &Path, project_id: &ProjectId) -> ProjectRecord {
        let project_toml =
            FileSystem::audit_project_root(base_dir, project_id).join("project.toml");
        let raw = std::fs::read_to_string(project_toml).expect("audit project record");
        toml::from_str(&raw).expect("parse audit project record")
    }

    fn project_root(base_dir: &Path, project_id: &ProjectId) -> std::path::PathBuf {
        base_dir
            .join(".ralph-burning")
            .join("projects")
            .join(project_id.as_str())
    }

    fn supporting_record(
        stage_id: StageId,
        cycle: u32,
        completion_round: u32,
    ) -> (PayloadRecord, ArtifactRecord) {
        let record_id = format!("{}-c{}-cr{}", stage_id.as_str(), cycle, completion_round);
        let producer = RecordProducer::System {
            component: "test".to_owned(),
        };
        let payload = PayloadRecord {
            payload_id: format!("{record_id}-payload"),
            stage_id,
            cycle,
            attempt: 1,
            created_at: Utc::now(),
            payload: json!({ "ok": true }),
            record_kind: RecordKind::StageSupporting,
            producer: Some(producer.clone()),
            completion_round,
        };
        let artifact = ArtifactRecord {
            artifact_id: format!("{record_id}-artifact"),
            payload_id: payload.payload_id.clone(),
            stage_id,
            created_at: payload.created_at,
            content: "supporting artifact".to_owned(),
            record_kind: RecordKind::StageSupporting,
            producer: Some(producer),
            completion_round,
        };
        (payload, artifact)
    }

    #[test]
    fn continue_does_not_advance_prompt_hash_when_warning_append_fails() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let (project_id, run_id, original_prompt_hash) =
            setup_project(base_dir, "prompt-continue-fail").expect("project setup");
        std::fs::write(
            project_root(base_dir, &project_id).join("prompt.md"),
            "# Prompt\n\nChanged prompt.\n",
        )
        .expect("write changed prompt");

        let mut seq = 1;
        let mut snapshot = RunSnapshot::initial(20);
        let cursor = StageCursor::new(StageId::Review, 1, 1, 1).expect("cursor");
        let result = evaluate_prompt_change_on_resume(
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRunSnapshotWriteStore,
            &FailingJournalStore,
            &FsRuntimeLogWriteStore,
            base_dir,
            &project_id,
            &project_root(base_dir, &project_id),
            "prompt.md",
            &run_id,
            &mut seq,
            &mut snapshot,
            &cursor,
            &[StageId::Review, StageId::CompletionPanel],
            StageId::Planning,
            &original_prompt_hash,
            PromptChangeAction::Continue,
        );

        assert!(matches!(result, Err(AppError::StageCommitFailed { .. })));
        assert_eq!(seq, 1);
        assert_eq!(
            read_project_record(base_dir, &project_id).prompt_hash,
            original_prompt_hash
        );
    }

    #[test]
    fn restart_cycle_does_not_clear_records_before_warning_append_succeeds() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let (project_id, run_id, original_prompt_hash) =
            setup_project(base_dir, "prompt-restart-fail").expect("project setup");
        std::fs::write(
            project_root(base_dir, &project_id).join("prompt.md"),
            "# Prompt\n\nChanged prompt.\n",
        )
        .expect("write changed prompt");

        let (payload, artifact) = supporting_record(StageId::Review, 1, 1);
        FsPayloadArtifactWriteStore
            .write_payload_artifact_pair(base_dir, &project_id, &payload, &artifact)
            .expect("write supporting record");

        let mut seq = 1;
        let mut snapshot = RunSnapshot::initial(20);
        let cursor = StageCursor::new(StageId::Review, 1, 1, 1).expect("cursor");
        let result = evaluate_prompt_change_on_resume(
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRunSnapshotWriteStore,
            &FailingJournalStore,
            &FsRuntimeLogWriteStore,
            base_dir,
            &project_id,
            &project_root(base_dir, &project_id),
            "prompt.md",
            &run_id,
            &mut seq,
            &mut snapshot,
            &cursor,
            &[StageId::Planning, StageId::Review, StageId::CompletionPanel],
            StageId::Planning,
            &original_prompt_hash,
            PromptChangeAction::RestartCycle,
        );

        assert!(matches!(result, Err(AppError::StageCommitFailed { .. })));
        assert_eq!(seq, 1);
        assert_eq!(
            read_project_record(base_dir, &project_id).prompt_hash,
            original_prompt_hash
        );
        assert_eq!(
            FsArtifactStore
                .list_payloads(base_dir, &project_id)
                .expect("payload listing")
                .len(),
            1
        );
        assert_eq!(
            FsArtifactStore
                .list_artifacts(base_dir, &project_id)
                .expect("artifact listing")
                .len(),
            1
        );
    }

    #[test]
    fn restart_cycle_clears_current_cycle_supporting_records_from_planning_onward() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let (project_id, run_id, original_prompt_hash) =
            setup_project(base_dir, "prompt-restart-cleanup").expect("project setup");
        std::fs::write(
            project_root(base_dir, &project_id).join("prompt.md"),
            "# Prompt\n\nChanged prompt.\n",
        )
        .expect("write changed prompt");

        for stage_id in [
            StageId::PromptReview,
            StageId::Planning,
            StageId::CompletionPanel,
            StageId::FinalReview,
        ] {
            let (payload, artifact) = supporting_record(stage_id, 1, 1);
            FsPayloadArtifactWriteStore
                .write_payload_artifact_pair(base_dir, &project_id, &payload, &artifact)
                .expect("write supporting record");
        }

        let mut seq = 1;
        let mut snapshot = RunSnapshot::initial(20);
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let result = evaluate_prompt_change_on_resume(
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsRuntimeLogWriteStore,
            base_dir,
            &project_id,
            &project_root(base_dir, &project_id),
            "prompt.md",
            &run_id,
            &mut seq,
            &mut snapshot,
            &cursor,
            &[
                StageId::PromptReview,
                StageId::Planning,
                StageId::Review,
                StageId::CompletionPanel,
                StageId::FinalReview,
            ],
            StageId::Planning,
            &original_prompt_hash,
            PromptChangeAction::RestartCycle,
        )
        .expect("restart_cycle should succeed");

        let PromptChangeResumeDecision::RestartCycle {
            next_cursor,
            next_stage_index,
            ..
        } = result
        else {
            panic!("expected restart_cycle decision");
        };
        assert_eq!(next_cursor.stage, StageId::Planning);
        assert_eq!(next_stage_index, 1);

        let remaining_payloads = FsArtifactStore
            .list_payloads(base_dir, &project_id)
            .expect("payload listing");
        assert_eq!(remaining_payloads.len(), 1);
        assert_eq!(remaining_payloads[0].stage_id, StageId::PromptReview);

        let remaining_artifacts = FsArtifactStore
            .list_artifacts(base_dir, &project_id)
            .expect("artifact listing");
        assert_eq!(remaining_artifacts.len(), 1);
        assert_eq!(
            read_project_record(base_dir, &project_id).prompt_hash,
            FileSystem::prompt_hash("# Prompt\n\nChanged prompt.\n")
        );
        assert_eq!(
            read_audit_project_record(base_dir, &project_id).prompt_hash,
            FileSystem::prompt_hash("# Prompt\n\nChanged prompt.\n")
        );
    }

    #[test]
    fn restart_cycle_clears_abandoned_supporting_records_across_completion_rounds() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let (project_id, run_id, original_prompt_hash) =
            setup_project(base_dir, "prompt-restart-multi-round-cleanup").expect("project setup");
        std::fs::write(
            project_root(base_dir, &project_id).join("prompt.md"),
            "# Prompt\n\nChanged prompt.\n",
        )
        .expect("write changed prompt");

        for (stage_id, completion_round) in [
            (StageId::PromptReview, 1),
            (StageId::Planning, 1),
            (StageId::CompletionPanel, 1),
            (StageId::FinalReview, 1),
            (StageId::Planning, 2),
            (StageId::Review, 2),
            (StageId::CompletionPanel, 2),
            (StageId::FinalReview, 2),
        ] {
            let (payload, artifact) = supporting_record(stage_id, 1, completion_round);
            FsPayloadArtifactWriteStore
                .write_payload_artifact_pair(base_dir, &project_id, &payload, &artifact)
                .expect("write supporting record");
        }

        let mut seq = 1;
        let mut snapshot = RunSnapshot::initial(20);
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 2).expect("cursor");
        let result = evaluate_prompt_change_on_resume(
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsRuntimeLogWriteStore,
            base_dir,
            &project_id,
            &project_root(base_dir, &project_id),
            "prompt.md",
            &run_id,
            &mut seq,
            &mut snapshot,
            &cursor,
            &[
                StageId::PromptReview,
                StageId::Planning,
                StageId::Review,
                StageId::CompletionPanel,
                StageId::FinalReview,
            ],
            StageId::Planning,
            &original_prompt_hash,
            PromptChangeAction::RestartCycle,
        )
        .expect("restart_cycle should succeed");

        let PromptChangeResumeDecision::RestartCycle { next_cursor, .. } = result else {
            panic!("expected restart_cycle decision");
        };
        assert_eq!(next_cursor.stage, StageId::Planning);
        assert_eq!(next_cursor.completion_round, 2);

        let remaining_payloads = FsArtifactStore
            .list_payloads(base_dir, &project_id)
            .expect("payload listing");
        assert_eq!(remaining_payloads.len(), 1);
        assert_eq!(remaining_payloads[0].stage_id, StageId::PromptReview);
        assert_eq!(remaining_payloads[0].completion_round, 1);

        let remaining_artifacts = FsArtifactStore
            .list_artifacts(base_dir, &project_id)
            .expect("artifact listing");
        assert_eq!(remaining_artifacts.len(), 1);
        assert_eq!(remaining_artifacts[0].stage_id, StageId::PromptReview);
        assert_eq!(remaining_artifacts[0].completion_round, 1);
    }
}
