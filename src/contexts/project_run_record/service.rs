use std::path::Path;

use chrono::{DateTime, Utc};

use crate::adapters::fs::FileSystem;
use crate::contexts::requirements_drafting::service::SeedHandoff;
use crate::contexts::workflow_composition;
use crate::shared::domain::{FlowPreset, ProjectId, StageCursor, StageId};
use crate::shared::error::{AppError, AppResult};

use super::journal;
use super::model::{
    ActiveRun, AmendmentSource, ArtifactRecord, JournalEvent, JournalEventType, PayloadRecord,
    ProjectDetail, ProjectListEntry, ProjectRecord, ProjectStatusSummary, QueuedAmendment,
    RollbackPoint, RunSnapshot, RunStatus, RuntimeLogEntry, SessionStore,
};
use super::queries::{
    self, RunHistoryView, RunRollbackTargetView, RunStatusJsonView, RunStatusView, RunTailView,
};

/// Port for reading and writing project records.
pub trait ProjectStorePort {
    fn project_exists(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<bool>;
    fn read_project_record(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<ProjectRecord>;
    fn list_project_ids(&self, base_dir: &Path) -> AppResult<Vec<ProjectId>>;

    /// Stage a project for deletion: makes it invisible to list/show but
    /// keeps data on disk for potential rollback.
    fn stage_delete(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()>;

    /// Finalize a staged delete: permanently removes the project data.
    fn commit_delete(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()>;

    /// Roll back a staged delete: restores the project to its canonical path.
    fn rollback_delete(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()>;

    /// Atomically stage and commit a new project. If any step fails,
    /// the project must not be visible to list/show.
    fn create_project_atomic(
        &self,
        base_dir: &Path,
        record: &ProjectRecord,
        prompt_contents: &str,
        run_snapshot: &RunSnapshot,
        initial_journal_line: &str,
        sessions: &SessionStore,
    ) -> AppResult<()>;
}

/// Port for reading and appending journal events.
pub trait JournalStorePort {
    fn read_journal(&self, base_dir: &Path, project_id: &ProjectId)
        -> AppResult<Vec<JournalEvent>>;
    fn append_event(&self, base_dir: &Path, project_id: &ProjectId, line: &str) -> AppResult<()>;
}

/// Port for reading payload and artifact durable history.
pub trait ArtifactStorePort {
    fn list_payloads(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<PayloadRecord>>;
    fn list_artifacts(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<ArtifactRecord>>;

    fn read_payload_by_id(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        payload_id: &str,
    ) -> AppResult<PayloadRecord> {
        self.list_payloads(base_dir, project_id)?
            .into_iter()
            .find(|payload| payload.payload_id == payload_id)
            .ok_or_else(|| AppError::PayloadNotFound {
                payload_id: payload_id.to_owned(),
            })
    }

    fn read_artifact_by_id(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        artifact_id: &str,
    ) -> AppResult<ArtifactRecord> {
        self.list_artifacts(base_dir, project_id)?
            .into_iter()
            .find(|artifact| artifact.artifact_id == artifact_id)
            .ok_or_else(|| AppError::ArtifactNotFound {
                artifact_id: artifact_id.to_owned(),
            })
    }
}

/// Port for reading runtime logs (separate from durable history).
pub trait RuntimeLogStorePort {
    fn read_runtime_logs(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<RuntimeLogEntry>>;
}

/// Port for reading/writing the run snapshot.
pub trait RunSnapshotPort {
    fn read_run_snapshot(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<RunSnapshot>;
}

/// Port for writing the run snapshot atomically.
pub trait RunSnapshotWritePort {
    fn write_run_snapshot(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        snapshot: &RunSnapshot,
    ) -> AppResult<()>;
}

/// Port for durable rollback point persistence.
pub trait RollbackPointStorePort {
    fn write_rollback_point(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        rollback_point: &RollbackPoint,
    ) -> AppResult<()>;

    fn list_rollback_points(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<RollbackPoint>>;

    fn read_rollback_point_by_stage(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        stage_id: StageId,
    ) -> AppResult<Option<RollbackPoint>>;
}

/// Port for repository reset operations during hard rollback.
pub trait RepositoryResetPort {
    fn reset_to_sha(&self, repo_root: &Path, sha: &str) -> AppResult<()>;
}

/// Port for writing payload and artifact records atomically as a pair.
pub trait PayloadArtifactWritePort {
    fn write_payload_artifact_pair(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        payload: &PayloadRecord,
        artifact: &ArtifactRecord,
    ) -> AppResult<()>;

    /// Remove a previously written payload/artifact pair.
    /// Used to roll back a stage commit when journal or snapshot persistence
    /// fails after the pair was already written.
    fn remove_payload_artifact_pair(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        payload_id: &str,
        artifact_id: &str,
    ) -> AppResult<()>;
}

/// Port for appending runtime log entries (best-effort, not durable history).
pub trait RuntimeLogWritePort {
    fn append_runtime_log(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        entry: &RuntimeLogEntry,
    ) -> AppResult<()>;
}

/// Port for durable amendment queue persistence under `projects/<id>/amendments/`.
pub trait AmendmentQueuePort {
    /// Write a single amendment file atomically.
    fn write_amendment(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        amendment: &super::model::QueuedAmendment,
    ) -> AppResult<()>;

    /// List all pending amendment files from disk.
    fn list_pending_amendments(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<super::model::QueuedAmendment>>;

    /// Remove a single amendment file by ID.
    fn remove_amendment(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        amendment_id: &str,
    ) -> AppResult<()>;

    /// Remove all pending amendment files. Returns count removed.
    fn drain_amendments(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<u32>;

    /// Check if any amendment files exist on disk.
    fn has_pending_amendments(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<bool>;
}

/// Port for reading/writing/clearing the active project pointer.
pub trait ActiveProjectPort {
    fn read_active_project_id(&self, base_dir: &Path) -> AppResult<Option<String>>;
    fn clear_active_project(&self, base_dir: &Path) -> AppResult<()>;
    fn write_active_project(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()>;
}

/// Input for the `project create` use case.
pub struct CreateProjectInput {
    pub id: ProjectId,
    pub name: String,
    pub flow: FlowPreset,
    pub prompt_path: String,
    pub prompt_contents: String,
    pub prompt_hash: String,
    pub created_at: DateTime<Utc>,
}

/// Create a new project with all canonical files.
pub fn create_project(
    store: &dyn ProjectStorePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    input: CreateProjectInput,
) -> AppResult<ProjectRecord> {
    let initial_details = serde_json::json!({
        "project_id": input.id.as_str(),
        "flow": input.flow.as_str(),
    });
    create_project_with_initial_details(store, journal_store, base_dir, input, initial_details)
}

pub fn create_project_from_seed(
    store: &dyn ProjectStorePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    handoff: SeedHandoff,
    flow_override: Option<FlowPreset>,
    created_at: DateTime<Utc>,
) -> AppResult<ProjectRecord> {
    let SeedHandoff {
        requirements_run_id,
        project_id,
        project_name,
        flow: seed_flow,
        prompt_body,
        prompt_path: _,
        recommended_flow,
    } = handoff;

    let selected_flow = flow_override.unwrap_or(seed_flow);
    let prompt_hash = FileSystem::prompt_hash(&prompt_body);
    let project_id = ProjectId::new(project_id)?;

    let mut initial_details = serde_json::Map::from_iter([
        (
            "project_id".to_owned(),
            serde_json::Value::String(project_id.to_string()),
        ),
        (
            "flow".to_owned(),
            serde_json::Value::String(selected_flow.as_str().to_owned()),
        ),
        (
            "source".to_owned(),
            serde_json::Value::String("requirements".to_owned()),
        ),
        (
            "requirements_run_id".to_owned(),
            serde_json::Value::String(requirements_run_id),
        ),
        (
            "seed_flow".to_owned(),
            serde_json::Value::String(seed_flow.as_str().to_owned()),
        ),
    ]);
    if let Some(flow) = recommended_flow {
        initial_details.insert(
            "recommended_flow".to_owned(),
            serde_json::Value::String(flow.as_str().to_owned()),
        );
    }

    let input = CreateProjectInput {
        id: project_id,
        name: project_name,
        flow: selected_flow,
        prompt_path: "prompt.md".to_owned(),
        prompt_contents: prompt_body,
        prompt_hash,
        created_at,
    };

    create_project_with_initial_details(
        store,
        journal_store,
        base_dir,
        input,
        serde_json::Value::Object(initial_details),
    )
}

fn create_project_with_initial_details(
    store: &dyn ProjectStorePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    input: CreateProjectInput,
    initial_details: serde_json::Value,
) -> AppResult<ProjectRecord> {
    // Check for duplicate project ID
    if store.project_exists(base_dir, &input.id)? {
        return Err(AppError::DuplicateProject {
            project_id: input.id.to_string(),
        });
    }

    let record = ProjectRecord {
        id: input.id.clone(),
        name: input.name,
        flow: input.flow,
        prompt_reference: "prompt.md".to_owned(),
        prompt_hash: input.prompt_hash,
        created_at: input.created_at,
        status_summary: ProjectStatusSummary::Created,
    };

    let run_snapshot = RunSnapshot::initial();
    let sessions = SessionStore::empty();

    // Create the initial journal event
    let initial_event = JournalEvent {
        sequence: 1,
        timestamp: input.created_at,
        event_type: JournalEventType::ProjectCreated,
        details: initial_details,
    };
    let journal_line = journal::serialize_event(&initial_event)?;

    // Atomic creation: all files or nothing
    store.create_project_atomic(
        base_dir,
        &record,
        &input.prompt_contents,
        &run_snapshot,
        &journal_line,
        &sessions,
    )?;

    // create_project_atomic writes the journal as part of the atomic init,
    // but we don't call journal_store.append_event here because the atomic
    // creation already wrote the initial journal line. The journal_store
    // parameter is kept for interface consistency and future use.
    let _ = journal_store;

    Ok(record)
}

/// List all projects with summary data.
pub fn list_projects(
    store: &dyn ProjectStorePort,
    active_port: &dyn ActiveProjectPort,
    base_dir: &Path,
) -> AppResult<Vec<ProjectListEntry>> {
    let active_id = active_port.read_active_project_id(base_dir)?;
    let project_ids = store.list_project_ids(base_dir)?;

    let mut entries = Vec::new();
    for pid in project_ids {
        let record = store.read_project_record(base_dir, &pid)?;
        let is_active = active_id.as_deref() == Some(pid.as_str());
        entries.push(ProjectListEntry {
            id: record.id,
            name: record.name,
            flow: record.flow,
            status_summary: record.status_summary,
            created_at: record.created_at,
            is_active,
        });
    }

    Ok(entries)
}

/// Show detailed project information.
pub fn show_project(
    store: &dyn ProjectStorePort,
    run_port: &dyn RunSnapshotPort,
    journal_port: &dyn JournalStorePort,
    active_port: &dyn ActiveProjectPort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<ProjectDetail> {
    if !store.project_exists(base_dir, project_id)? {
        return Err(AppError::ProjectNotFound {
            project_id: project_id.to_string(),
        });
    }

    let record = store.read_project_record(base_dir, project_id)?;
    let run_snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    let events = journal_port.read_journal(base_dir, project_id)?;
    let journal_event_count = events.len() as u64;
    let active_id = active_port.read_active_project_id(base_dir)?;
    let is_active = active_id.as_deref() == Some(project_id.as_str());

    // Count rollback points from journal events
    let rollback_count = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::RollbackCreated)
        .count();

    Ok(ProjectDetail {
        record,
        run_snapshot,
        journal_event_count,
        rollback_count,
        is_active,
    })
}

/// Delete a project. Fails if project has an active run.
/// Clears active-project pointer if it pointed to the deleted project.
///
/// Transactional: if any post-validation step fails, the project remains
/// addressable at its canonical path and the active-project pointer is
/// unchanged.
pub fn delete_project(
    store: &dyn ProjectStorePort,
    run_port: &dyn RunSnapshotPort,
    active_port: &dyn ActiveProjectPort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<()> {
    if !store.project_exists(base_dir, project_id)? {
        return Err(AppError::ProjectNotFound {
            project_id: project_id.to_string(),
        });
    }

    // Check for active run
    let run_snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    if run_snapshot.has_active_run() {
        return Err(AppError::ActiveRunDelete {
            project_id: project_id.to_string(),
        });
    }

    // Check if this is the active project (before delete)
    let active_id = active_port.read_active_project_id(base_dir)?;
    let was_active = active_id.as_deref() == Some(project_id.as_str());

    // Phase 1: Stage the delete — project becomes invisible to list/show
    // but data remains on disk for rollback.
    store.stage_delete(base_dir, project_id)?;

    // Phase 2: Clear the active-project pointer if needed.
    // If this fails, roll back the staged delete so the project remains
    // addressable and the pointer is unchanged.
    if was_active {
        if let Err(clear_err) = active_port.clear_active_project(base_dir) {
            store
                .rollback_delete(base_dir, project_id)
                .map_err(|restore_err| AppError::CorruptRecord {
                    file: format!("projects/{}", project_id),
                    details: format!(
                        "delete partially failed: pointer clear error: {}, restore error: {}",
                        clear_err, restore_err
                    ),
                })?;
            return Err(clear_err);
        }
    }

    // Phase 3: Finalize — permanently remove the project data.
    // At this point the logical delete has succeeded (project is invisible,
    // pointer is cleared). Best-effort cleanup of the staged data.
    let _ = store.commit_delete(base_dir, project_id);

    Ok(())
}

/// Get run status for the active project.
pub fn run_status(
    run_port: &dyn RunSnapshotPort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<RunStatusView> {
    let snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    Ok(queries::build_status_view(project_id.as_str(), &snapshot))
}

/// Get stable JSON run status for the active project.
pub fn run_status_json(
    run_port: &dyn RunSnapshotPort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<RunStatusJsonView> {
    let snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    Ok(RunStatusJsonView::from_snapshot(
        project_id.as_str(),
        &snapshot,
    ))
}

/// Get run history (durable only, no runtime logs).
pub fn run_history(
    journal_port: &dyn JournalStorePort,
    artifact_port: &dyn ArtifactStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<RunHistoryView> {
    let events =
        queries::visible_journal_events(&journal_port.read_journal(base_dir, project_id)?)?;
    let (payloads, artifacts) = queries::filter_history_records(
        &events,
        artifact_port.list_payloads(base_dir, project_id)?,
        artifact_port.list_artifacts(base_dir, project_id)?,
    )?;

    queries::validate_history_consistency(&payloads, &artifacts)?;

    Ok(queries::build_history_view(
        project_id.as_str(),
        events,
        payloads,
        artifacts,
    ))
}

/// Get run tail: durable history by default, with optional runtime logs.
pub fn run_tail(
    journal_port: &dyn JournalStorePort,
    artifact_port: &dyn ArtifactStorePort,
    log_port: &dyn RuntimeLogStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    include_logs: bool,
) -> AppResult<RunTailView> {
    let events =
        queries::visible_journal_events(&journal_port.read_journal(base_dir, project_id)?)?;
    let (payloads, artifacts) = queries::filter_history_records(
        &events,
        artifact_port.list_payloads(base_dir, project_id)?,
        artifact_port.list_artifacts(base_dir, project_id)?,
    )?;

    queries::validate_history_consistency(&payloads, &artifacts)?;

    let runtime_logs = if include_logs {
        log_port.read_runtime_logs(base_dir, project_id)?
    } else {
        Vec::new()
    };

    Ok(queries::build_tail_view(
        project_id.as_str(),
        events,
        payloads,
        artifacts,
        include_logs,
        runtime_logs,
    ))
}

/// List visible rollback points for the current logical history branch.
pub fn list_rollback_points(
    rollback_store: &dyn RollbackPointStorePort,
    journal_port: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<Vec<RollbackPoint>> {
    let events =
        queries::visible_journal_events(&journal_port.read_journal(base_dir, project_id)?)?;
    let visible_ids = visible_rollback_ids(&events);
    let mut points = rollback_store
        .list_rollback_points(base_dir, project_id)?
        .into_iter()
        .filter(|point| visible_ids.contains(point.rollback_id.as_str()))
        .collect::<Vec<_>>();
    points.sort_by_key(|point| point.created_at);
    Ok(points)
}

/// List visible rollback targets in a CLI-ready view format.
pub fn list_rollback_targets(
    rollback_store: &dyn RollbackPointStorePort,
    journal_port: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<Vec<RunRollbackTargetView>> {
    Ok(
        list_rollback_points(rollback_store, journal_port, base_dir, project_id)?
            .into_iter()
            .map(|point| RunRollbackTargetView {
                rollback_id: point.rollback_id,
                stage_id: point.stage_id.as_str().to_owned(),
                cycle: point.cycle,
                created_at: point.created_at,
                git_sha: point.git_sha,
            })
            .collect(),
    )
}

/// Look up the latest visible rollback point for a stage.
pub fn get_rollback_point_for_stage(
    rollback_store: &dyn RollbackPointStorePort,
    journal_port: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    stage_id: StageId,
) -> AppResult<Option<RollbackPoint>> {
    let visible_events =
        queries::visible_journal_events(&journal_port.read_journal(base_dir, project_id)?)?;
    let visible_ids = visible_rollback_ids(&visible_events);

    Ok(rollback_store
        .list_rollback_points(base_dir, project_id)?
        .into_iter()
        .filter(|point| point.stage_id == stage_id)
        .filter(|point| visible_ids.contains(point.rollback_id.as_str()))
        .max_by_key(|point| point.created_at))
}

/// Resolve a visible payload record by ID.
pub fn get_payload_by_id(
    journal_port: &dyn JournalStorePort,
    artifact_port: &dyn ArtifactStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    payload_id: &str,
) -> AppResult<PayloadRecord> {
    let events =
        queries::visible_journal_events(&journal_port.read_journal(base_dir, project_id)?)?;
    let (payloads, artifacts) = queries::filter_history_records(
        &events,
        artifact_port.list_payloads(base_dir, project_id)?,
        artifact_port.list_artifacts(base_dir, project_id)?,
    )?;
    queries::validate_history_consistency(&payloads, &artifacts)?;

    if !payloads
        .iter()
        .any(|payload| payload.payload_id == payload_id)
    {
        return Err(AppError::PayloadNotFound {
            payload_id: payload_id.to_owned(),
        });
    }

    artifact_port.read_payload_by_id(base_dir, project_id, payload_id)
}

/// Resolve a visible artifact record by ID.
pub fn get_artifact_by_id(
    journal_port: &dyn JournalStorePort,
    artifact_port: &dyn ArtifactStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    artifact_id: &str,
) -> AppResult<ArtifactRecord> {
    let events =
        queries::visible_journal_events(&journal_port.read_journal(base_dir, project_id)?)?;
    let (payloads, artifacts) = queries::filter_history_records(
        &events,
        artifact_port.list_payloads(base_dir, project_id)?,
        artifact_port.list_artifacts(base_dir, project_id)?,
    )?;
    queries::validate_history_consistency(&payloads, &artifacts)?;

    if !artifacts
        .iter()
        .any(|artifact| artifact.artifact_id == artifact_id)
    {
        return Err(AppError::ArtifactNotFound {
            artifact_id: artifact_id.to_owned(),
        });
    }

    artifact_port.read_artifact_by_id(base_dir, project_id, artifact_id)
}

/// Perform a logical or hard rollback to a visible checkpoint.
#[allow(clippy::too_many_arguments)]
pub fn perform_rollback(
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    journal_port: &dyn JournalStorePort,
    rollback_store: &dyn RollbackPointStorePort,
    reset_port: Option<&dyn RepositoryResetPort>,
    base_dir: &Path,
    project_id: &ProjectId,
    flow: FlowPreset,
    target_stage: StageId,
    hard: bool,
) -> AppResult<RollbackPoint> {
    let current_snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    match current_snapshot.status {
        RunStatus::Failed | RunStatus::Paused => {}
        status => {
            return Err(AppError::RollbackInvalidStatus {
                project_id: project_id.to_string(),
                status: status.to_string(),
            });
        }
    }

    if !flow_stage_membership(flow, target_stage) {
        return Err(AppError::RollbackStageNotInFlow {
            project_id: project_id.to_string(),
            stage_id: target_stage.to_string(),
            flow: flow.to_string(),
        });
    }

    let rollback_point = get_rollback_point_for_stage(
        rollback_store,
        journal_port,
        base_dir,
        project_id,
        target_stage,
    )?
    .ok_or_else(|| AppError::RollbackPointNotFound {
        project_id: project_id.to_string(),
        stage_id: target_stage.to_string(),
    })?;

    let events = journal_port.read_journal(base_dir, project_id)?;
    let visible_through_sequence =
        rollback_created_sequence_for(&events, rollback_point.rollback_id.as_str())?;

    let mut restored_snapshot = rollback_point.run_snapshot.clone();
    restored_snapshot.status = RunStatus::Paused;
    restored_snapshot.interrupted_run = restored_snapshot
        .active_run
        .clone()
        .or_else(|| restored_snapshot.interrupted_run.clone());
    restored_snapshot.active_run = None;
    restored_snapshot.rollback_point_meta.rollback_count =
        current_snapshot.rollback_point_meta.rollback_count + 1;
    restored_snapshot.rollback_point_meta.last_rollback_id =
        Some(rollback_point.rollback_id.clone());
    restored_snapshot.status_summary = format!(
        "paused after rollback to {} (cycle {}); run `ralph-burning run resume` to continue",
        rollback_point.stage_id.display_name(),
        rollback_point.cycle
    );

    let sequence = journal::last_sequence(&events) + 1;
    let rollback_event = journal::rollback_performed_event(
        sequence,
        Utc::now(),
        rollback_point.rollback_id.as_str(),
        rollback_point.stage_id,
        rollback_point.cycle,
        visible_through_sequence,
        hard,
        rollback_point.git_sha.as_deref(),
        restored_snapshot.rollback_point_meta.rollback_count,
    );
    let rollback_line = journal::serialize_event(&rollback_event)?;
    run_write_port.write_run_snapshot(base_dir, project_id, &restored_snapshot)?;
    if let Err(append_error) = journal_port.append_event(base_dir, project_id, &rollback_line) {
        if let Err(restore_error) =
            run_write_port.write_run_snapshot(base_dir, project_id, &current_snapshot)
        {
            return Err(AppError::CorruptRecord {
                file: format!("projects/{}/run.json", project_id.as_str()),
                details: format!(
                    "rollback journal append failed after snapshot write: {append_error}; failed to restore the previous snapshot: {restore_error}"
                ),
            });
        }
        return Err(append_error);
    }

    if hard {
        let git_sha =
            rollback_point
                .git_sha
                .as_deref()
                .ok_or_else(|| AppError::RollbackGitResetFailed {
                    project_id: project_id.to_string(),
                    rollback_id: rollback_point.rollback_id.clone(),
                    details: "rollback point does not record a git commit SHA".to_owned(),
                })?;

        let reset_port = reset_port.ok_or_else(|| AppError::RollbackGitResetFailed {
            project_id: project_id.to_string(),
            rollback_id: rollback_point.rollback_id.clone(),
            details: "no repository reset adapter was provided".to_owned(),
        })?;

        if let Err(error) = reset_port.reset_to_sha(base_dir, git_sha) {
            return Err(AppError::RollbackGitResetFailed {
                project_id: project_id.to_string(),
                rollback_id: rollback_point.rollback_id.clone(),
                details: error.to_string(),
            });
        }
    }

    Ok(rollback_point)
}

fn flow_stage_membership(flow: FlowPreset, stage_id: StageId) -> bool {
    workflow_composition::flow_definition(flow)
        .stages
        .contains(&stage_id)
}

fn visible_rollback_ids(events: &[JournalEvent]) -> std::collections::HashSet<&str> {
    events
        .iter()
        .filter(|event| event.event_type == JournalEventType::RollbackCreated)
        .filter_map(|event| {
            event
                .details
                .get("rollback_id")
                .and_then(|value| value.as_str())
        })
        .collect()
}

fn rollback_created_sequence_for(events: &[JournalEvent], rollback_id: &str) -> AppResult<u64> {
    events
        .iter()
        .find(|event| {
            event.event_type == JournalEventType::RollbackCreated
                && event
                    .details
                    .get("rollback_id")
                    .and_then(|value| value.as_str())
                    == Some(rollback_id)
        })
        .map(|event| event.sequence)
        .ok_or_else(|| AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: format!(
                "missing rollback_created event for rollback point '{}'",
                rollback_id
            ),
        })
}

// ── Shared Amendment Service ──────────────────────────────────────────────

/// Result of staging a manual amendment.
#[derive(Debug)]
pub enum AmendmentAddResult {
    /// A new amendment was created.
    Created { amendment_id: String },
    /// An existing amendment with the same dedup key was found.
    Duplicate { amendment_id: String },
}

/// Add a manual amendment. Performs dedup check, writes durably, emits a journal
/// event, syncs the canonical snapshot, and reopens completed projects.
#[allow(clippy::too_many_arguments)]
pub fn add_manual_amendment(
    amendment_queue: &dyn AmendmentQueuePort,
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    journal_port: &dyn JournalStorePort,
    project_store: &dyn ProjectStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    body: &str,
) -> AppResult<AmendmentAddResult> {
    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;

    // Reject while a run is actively writing.
    if snapshot.status == RunStatus::Running {
        return Err(AppError::AmendmentLeaseConflict {
            project_id: project_id.to_string(),
        });
    }

    let source = AmendmentSource::Manual;
    let dedup_key = QueuedAmendment::compute_dedup_key(&source, body);

    // Dedup check against canonical snapshot pending queue.
    if let Some(existing) = snapshot
        .amendment_queue
        .pending
        .iter()
        .find(|a| a.dedup_key == dedup_key)
    {
        return Ok(AmendmentAddResult::Duplicate {
            amendment_id: existing.amendment_id.clone(),
        });
    }

    // Also check staged amendment files on disk to catch duplicates from
    // a prior failed attempt where the file was preserved but the snapshot
    // update failed (the file survives reopen failures by design).
    // Skip this check for completed projects — the retry needs to proceed
    // through the reopen path even if the file already exists on disk.
    if snapshot.status != RunStatus::Completed {
        let on_disk = amendment_queue.list_pending_amendments(base_dir, project_id)?;
        if let Some(existing) = on_disk.iter().find(|a| a.dedup_key == dedup_key) {
            return Ok(AmendmentAddResult::Duplicate {
                amendment_id: existing.amendment_id.clone(),
            });
        }
    }

    let now = Utc::now();
    let amendment_id = format!("manual-{}", uuid::Uuid::new_v4());

    let current_cycle = snapshot
        .cycle_history
        .last()
        .map(|entry| entry.cycle)
        .unwrap_or(1);
    let completion_round = snapshot.completion_rounds.max(1);

    let amendment = QueuedAmendment {
        amendment_id: amendment_id.clone(),
        source_stage: StageId::Planning,
        source_cycle: current_cycle,
        source_completion_round: completion_round,
        body: body.to_owned(),
        created_at: now,
        batch_sequence: 0,
        source,
        dedup_key: dedup_key.clone(),
    };

    // Prepare the journal line BEFORE any mutations so that fallible
    // read_journal / serialize_event calls cannot fail after canonical state
    // is already committed.
    let journal_line = {
        let events = journal_port.read_journal(base_dir, project_id)?;
        let seq = journal::last_sequence(&events) + 1;
        let journal_event = journal::amendment_queued_manual_event(
            seq,
            now,
            &amendment_id,
            body,
            "manual",
            "planning",
            &dedup_key,
        );
        journal::serialize_event(&journal_event)?
    };

    // Write durable amendment file.
    amendment_queue.write_amendment(base_dir, project_id, &amendment)?;

    // Save pre-mutation snapshot so we can restore it if a later step fails.
    let old_snapshot = snapshot.clone();

    // Sync canonical snapshot: add the amendment to run.json pending queue.
    // The snapshot is committed BEFORE the journal event so that a snapshot
    // write failure leaves no orphaned journal entry.
    snapshot.amendment_queue.pending.push(amendment.clone());

    // If the project is completed, reopen it with the pending amendment
    // already reflected in the snapshot.
    let snap_result = if snapshot.status == RunStatus::Completed {
        reopen_completed_project_with_snapshot(
            run_write_port,
            project_store,
            base_dir,
            project_id,
            &mut snapshot,
        )
    } else {
        // Persist the updated snapshot with the new pending amendment.
        run_write_port.write_run_snapshot(base_dir, project_id, &snapshot)
    };

    if let Err(snap_err) = snap_result {
        if old_snapshot.status == RunStatus::Completed {
            // Preserve the amendment file for completed-project reopen failures
            // so the operator's input is not lost on retry.
            return Err(snap_err);
        }
        // For non-completed projects, roll back the amendment file so it
        // doesn't become an orphan that blocks completion_guard.
        if let Err(cleanup_err) =
            amendment_queue.remove_amendment(base_dir, project_id, &amendment_id)
        {
            return Err(AppError::CorruptRecord {
                file: format!("projects/{}/run.json", project_id.as_str()),
                details: format!(
                    "snapshot/reopen write failed: {snap_err}; \
                     amendment file cleanup also failed: {cleanup_err}"
                ),
            });
        }
        return Err(snap_err);
    }

    // Durably append the journal event. A successful add must record the
    // amendment_queued event. If the append fails, roll back the snapshot
    // and amendment file so no amendment is visible without its history.
    if let Err(journal_err) = journal_port.append_event(base_dir, project_id, &journal_line) {
        // Attempt to restore pre-mutation state.
        let snap_result = run_write_port.write_run_snapshot(base_dir, project_id, &old_snapshot);
        let file_result = amendment_queue.remove_amendment(base_dir, project_id, &amendment_id);

        // If rollback itself failed, return a composite error so the caller
        // knows canonical state may be inconsistent, matching the pattern
        // used in execute_rollback.
        if snap_result.is_err() || file_result.is_err() {
            let snap_detail = snap_result
                .err()
                .map_or_else(|| "ok".to_owned(), |e| e.to_string());
            let file_detail = file_result
                .err()
                .map_or_else(|| "ok".to_owned(), |e| e.to_string());
            return Err(AppError::CorruptRecord {
                file: format!("projects/{}/run.json", project_id.as_str()),
                details: format!(
                    "amendment journal append failed: {journal_err}; \
                     rollback also failed — snapshot restore: {snap_detail}, \
                     file cleanup: {file_detail}"
                ),
            });
        }

        return Err(journal_err);
    }

    Ok(AmendmentAddResult::Created { amendment_id })
}

/// List pending amendments for a project from the canonical run.json snapshot.
pub fn list_amendments(
    run_port: &dyn RunSnapshotPort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<Vec<QueuedAmendment>> {
    let snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    Ok(snapshot.amendment_queue.pending)
}

fn restore_completed_state_after_reopen_if_no_amendments(
    snapshot: &mut RunSnapshot,
    project_id: &ProjectId,
) -> bool {
    let pre_reopen_completion_round = if snapshot.status == RunStatus::Paused {
        snapshot.interrupted_run.as_ref().and_then(|run| {
            (run.run_id == reopen_run_id(project_id))
                .then_some(run.stage_cursor.completion_round.saturating_sub(1).max(1))
        })
    } else {
        None
    };

    if let Some(pre_reopen_completion_round) =
        pre_reopen_completion_round.filter(|_| snapshot.amendment_queue.pending.is_empty())
    {
        snapshot.active_run = None;
        snapshot.status = RunStatus::Completed;
        snapshot.interrupted_run = None;
        snapshot.completion_rounds = pre_reopen_completion_round;
        snapshot.status_summary = "completed".to_owned();
        return true;
    }

    false
}

fn reopen_run_id(project_id: &ProjectId) -> String {
    format!("reopen-{}", project_id.as_str())
}

/// Remove a single pending amendment by ID. Updates both run.json and disk.
///
/// Deletes the durable file first, then updates the canonical snapshot. If the
/// file deletion fails, the amendment remains pending everywhere and the command
/// fails cleanly. If the snapshot write fails after a successful file deletion,
/// the amendment file is restored and the command fails.
pub fn remove_amendment(
    amendment_queue: &dyn AmendmentQueuePort,
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    amendment_id: &str,
) -> AppResult<()> {
    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;

    // Reject while a run is actively writing.
    if snapshot.status == RunStatus::Running {
        return Err(AppError::AmendmentLeaseConflict {
            project_id: project_id.to_string(),
        });
    }

    // Verify the amendment exists in canonical snapshot state.
    let amendment = snapshot
        .amendment_queue
        .pending
        .iter()
        .find(|a| a.amendment_id == amendment_id)
        .cloned();
    let amendment = match amendment {
        Some(a) => a,
        None => {
            return Err(AppError::AmendmentNotFound {
                amendment_id: amendment_id.to_owned(),
            });
        }
    };

    // Delete the durable file first. If this fails, the amendment stays pending
    // everywhere and completion will correctly block on it.
    amendment_queue.remove_amendment(base_dir, project_id, amendment_id)?;

    // Update canonical snapshot to reflect the removal.
    snapshot
        .amendment_queue
        .pending
        .retain(|a| a.amendment_id != amendment_id);
    restore_completed_state_after_reopen_if_no_amendments(&mut snapshot, project_id);
    if let Err(snap_err) = run_write_port.write_run_snapshot(base_dir, project_id, &snapshot) {
        // Restore the amendment file so disk and snapshot stay consistent.
        // If restore also fails, return a composite error so the caller knows
        // the amendment file is missing while the snapshot still lists it.
        if let Err(restore_err) = amendment_queue.write_amendment(base_dir, project_id, &amendment)
        {
            return Err(AppError::CorruptRecord {
                file: format!("projects/{}/run.json", project_id.as_str()),
                details: format!(
                    "snapshot write failed after amendment file deletion: {snap_err}; \
                     amendment file restore also failed: {restore_err}"
                ),
            });
        }
        return Err(snap_err);
    }

    Ok(())
}

/// Clear all pending amendments. Returns removed and remaining IDs.
/// On partial failure, reports exactly which amendments were removed and which remain.
///
/// Drives the pending set from canonical snapshot state. Files are deleted
/// first, then the snapshot is updated to reflect only the remaining amendments.
/// If the snapshot write fails after partial deletion, `AmendmentClearPartial`
/// is still returned so the caller always gets the exact removed/remaining IDs.
pub fn clear_amendments(
    amendment_queue: &dyn AmendmentQueuePort,
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<Vec<String>> {
    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;

    // Reject while a run is actively writing.
    if snapshot.status == RunStatus::Running {
        return Err(AppError::AmendmentLeaseConflict {
            project_id: project_id.to_string(),
        });
    }

    let pending: Vec<QueuedAmendment> = std::mem::take(&mut snapshot.amendment_queue.pending);
    if pending.is_empty() {
        if restore_completed_state_after_reopen_if_no_amendments(&mut snapshot, project_id) {
            run_write_port.write_run_snapshot(base_dir, project_id, &snapshot)?;
        }
        return Ok(Vec::new());
    }

    let total = pending.len();

    // Delete files first, track removed/remaining.
    let mut removed = Vec::new();
    let mut remaining = Vec::new();

    for amendment in &pending {
        match amendment_queue.remove_amendment(base_dir, project_id, &amendment.amendment_id) {
            Ok(()) => removed.push(amendment.amendment_id.clone()),
            Err(_) => remaining.push(amendment.amendment_id.clone()),
        }
    }

    // Update canonical snapshot to reflect only the remaining amendments.
    if remaining.is_empty() {
        // All files deleted — clear the snapshot pending queue.
        // snapshot.amendment_queue.pending is already empty from std::mem::take.
        restore_completed_state_after_reopen_if_no_amendments(&mut snapshot, project_id);
        if let Err(snap_err) = run_write_port.write_run_snapshot(base_dir, project_id, &snapshot) {
            // Restore all amendment files so disk and snapshot stay consistent.
            // If any restore fails, return a composite error so the caller knows
            // which files could not be restored.
            let mut restore_failures: Vec<String> = Vec::new();
            for a in &pending {
                if let Err(e) = amendment_queue.write_amendment(base_dir, project_id, a) {
                    restore_failures.push(format!("{}: {e}", a.amendment_id));
                }
            }
            if !restore_failures.is_empty() {
                return Err(AppError::CorruptRecord {
                    file: format!("projects/{}/run.json", project_id.as_str()),
                    details: format!(
                        "snapshot write failed after clearing amendments: {snap_err}; \
                         amendment file restore also failed: {}",
                        restore_failures.join("; ")
                    ),
                });
            }
            return Err(snap_err);
        }
    } else {
        // Partial failure — put remaining amendments back into canonical state.
        let remaining_set: std::collections::HashSet<&str> =
            remaining.iter().map(|s| s.as_str()).collect();
        for a in &pending {
            if remaining_set.contains(a.amendment_id.as_str()) {
                snapshot.amendment_queue.pending.push(a.clone());
            }
        }
        // The snapshot MUST reflect only the remaining amendments before we
        // report partial success. If this repair write fails, restore the
        // deleted files so disk matches the unmodified snapshot and return the
        // underlying I/O error instead of AmendmentClearPartial.
        if let Err(repair_err) = run_write_port.write_run_snapshot(base_dir, project_id, &snapshot)
        {
            // Restore deleted amendment files so disk stays consistent with
            // the unmodified on-disk snapshot. If restore fails, return a
            // composite error with both the repair and restore failures.
            let mut restore_failures: Vec<String> = Vec::new();
            for a in &pending {
                if !remaining_set.contains(a.amendment_id.as_str()) {
                    if let Err(e) = amendment_queue.write_amendment(base_dir, project_id, a) {
                        restore_failures.push(format!("{}: {e}", a.amendment_id));
                    }
                }
            }
            if !restore_failures.is_empty() {
                return Err(AppError::CorruptRecord {
                    file: format!("projects/{}/run.json", project_id.as_str()),
                    details: format!(
                        "snapshot repair write failed after partial clear: {repair_err}; \
                         amendment file restore also failed: {}",
                        restore_failures.join("; ")
                    ),
                });
            }
            return Err(repair_err);
        }

        return Err(AppError::AmendmentClearPartial {
            removed_count: removed.len(),
            total,
            removed,
            remaining,
        });
    }

    Ok(removed)
}

/// Stage a batch of amendments from an automated source (e.g. PR-review).
/// Writes each amendment durably, emits journal events, syncs the canonical
/// snapshot, and reopens the project if it is completed. This is the shared
/// path that both manual and automated amendment intake converge on.
/// Returns the IDs of amendments that were actually staged (after dedup).
#[allow(clippy::too_many_arguments)]
pub fn stage_amendment_batch(
    amendment_queue: &dyn AmendmentQueuePort,
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    journal_port: &dyn JournalStorePort,
    project_store: &dyn ProjectStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    amendments: &[QueuedAmendment],
) -> AppResult<Vec<String>> {
    if amendments.is_empty() {
        return Ok(Vec::new());
    }

    // Prepare the journal sequence number BEFORE any mutations so that the
    // fallible read_journal call cannot fail after canonical state is committed.
    let base_journal_seq = {
        let events = journal_port.read_journal(base_dir, project_id)?;
        journal::last_sequence(&events)
    };

    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    // Save pre-mutation snapshot so we can restore it if journal append fails.
    let old_snapshot = snapshot.clone();
    let mut staged_ids: Vec<String> = Vec::new();
    let mut staged_amendments: Vec<&QueuedAmendment> = Vec::new();

    for amendment in amendments {
        // Dedup check against canonical snapshot pending queue.
        if snapshot
            .amendment_queue
            .pending
            .iter()
            .any(|a| a.dedup_key == amendment.dedup_key)
        {
            continue;
        }

        // Write durable amendment file. If this fails mid-batch, roll back
        // all earlier file writes so no pre-commit files leak.
        if let Err(write_err) = amendment_queue.write_amendment(base_dir, project_id, amendment) {
            let mut cleanup_failures: Vec<String> = Vec::new();
            for id in &staged_ids {
                if let Err(e) = amendment_queue.remove_amendment(base_dir, project_id, id) {
                    cleanup_failures.push(format!("{id}: {e}"));
                }
            }
            if !cleanup_failures.is_empty() {
                return Err(AppError::CorruptRecord {
                    file: format!("projects/{}/run.json", project_id.as_str()),
                    details: format!(
                        "batch file write failed: {write_err}; \
                         amendment file cleanup also failed: {}",
                        cleanup_failures.join("; ")
                    ),
                });
            }
            return Err(write_err);
        }

        // Sync canonical snapshot (in memory — committed below).
        snapshot.amendment_queue.pending.push(amendment.clone());
        staged_ids.push(amendment.amendment_id.clone());
        staged_amendments.push(amendment);
    }

    if staged_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Commit canonical snapshot BEFORE journal events so that a snapshot
    // write failure leaves no orphaned journal entries.
    let was_completed = snapshot.status == RunStatus::Completed;
    let snap_result = if was_completed {
        reopen_completed_project_with_snapshot(
            run_write_port,
            project_store,
            base_dir,
            project_id,
            &mut snapshot,
        )
    } else {
        run_write_port.write_run_snapshot(base_dir, project_id, &snapshot)
    };

    if let Err(snap_err) = snap_result {
        if was_completed {
            // When the project was completed and the reopen/snapshot write
            // fails, amendment files that have already been written to disk
            // must remain. The project snapshot stays at its last committed
            // completed state and the caller must not advance cursors or
            // journal events. On the next poll cycle the daemon will re-
            // fetch, re-deduplicate (files overwrite safely), and retry the
            // reopen.
            return Err(snap_err);
        }
        // For non-completed projects, roll back all amendment files written
        // in this batch so no pre-commit files leak.
        let mut cleanup_failures: Vec<String> = Vec::new();
        for id in &staged_ids {
            if let Err(e) = amendment_queue.remove_amendment(base_dir, project_id, id) {
                cleanup_failures.push(format!("{id}: {e}"));
            }
        }
        if !cleanup_failures.is_empty() {
            return Err(AppError::CorruptRecord {
                file: format!("projects/{}/run.json", project_id.as_str()),
                details: format!(
                    "snapshot/reopen write failed: {snap_err}; \
                     amendment file cleanup also failed: {}",
                    cleanup_failures.join("; ")
                ),
            });
        }
        return Err(snap_err);
    }

    // Pre-serialize all journal events so serialization failures are caught
    // before any appends. This prevents partial journal writes on serialize
    // errors.
    let mut journal_lines: Vec<String> = Vec::new();
    let mut seq = base_journal_seq;
    for amendment in &staged_amendments {
        seq += 1;
        let journal_event = journal::amendment_queued_manual_event(
            seq,
            amendment.created_at,
            &amendment.amendment_id,
            &amendment.body,
            amendment.source.as_str(),
            amendment.source_stage.as_str(),
            &amendment.dedup_key,
        );
        match journal::serialize_event(&journal_event) {
            Ok(line) => journal_lines.push(line),
            Err(ser_err) => {
                // Serialization failed — roll back all staged files and snapshot.
                let snap_result =
                    run_write_port.write_run_snapshot(base_dir, project_id, &old_snapshot);
                let mut file_failures: Vec<String> = Vec::new();
                for id in &staged_ids {
                    if let Err(e) = amendment_queue.remove_amendment(base_dir, project_id, id) {
                        file_failures.push(format!("{id}: {e}"));
                    }
                }
                if snap_result.is_err() || !file_failures.is_empty() {
                    let snap_detail = snap_result
                        .err()
                        .map_or_else(|| "ok".to_owned(), |e| e.to_string());
                    let file_detail = if file_failures.is_empty() {
                        "ok".to_owned()
                    } else {
                        file_failures.join("; ")
                    };
                    return Err(AppError::CorruptRecord {
                        file: format!("projects/{}/run.json", project_id.as_str()),
                        details: format!(
                            "journal event serialization failed: {ser_err}; \
                             rollback also failed — snapshot restore: {snap_detail}, \
                             file cleanup: {file_detail}"
                        ),
                    });
                }
                return Err(ser_err);
            }
        }
    }

    // Durably append all journal events. A successful staging must record all
    // amendment_queued events. If any append fails, roll back the snapshot
    // and amendment files so no amendments are visible without history.
    //
    // Track successful appends so we can detect partial-journal state: if
    // earlier lines are already on disk when a later append fails, the
    // journal has orphaned entries that cannot be un-appended.
    for (appended_count, line) in journal_lines.iter().enumerate() {
        if let Err(journal_err) = journal_port.append_event(base_dir, project_id, line) {
            // Attempt rollback: restore snapshot and remove amendment files.
            let snap_result =
                run_write_port.write_run_snapshot(base_dir, project_id, &old_snapshot);
            let mut file_failures: Vec<String> = Vec::new();
            for id in &staged_ids {
                if let Err(e) = amendment_queue.remove_amendment(base_dir, project_id, id) {
                    file_failures.push(format!("{id}: {e}"));
                }
            }

            let rollback_failed = snap_result.is_err() || !file_failures.is_empty();

            // If earlier journal lines were already appended, canonical state
            // (snapshot + files) was rolled back but the journal still contains
            // orphaned amendment_queued entries. Surface this as an unrecovered
            // consistency failure rather than implying a clean rollback.
            // Similarly, if rollback itself failed, the caller needs to know
            // that canonical state may be inconsistent.
            if appended_count > 0 || rollback_failed {
                let snap_detail = snap_result
                    .err()
                    .map_or_else(|| "ok".to_owned(), |e| e.to_string());
                let file_detail = if file_failures.is_empty() {
                    "ok".to_owned()
                } else {
                    file_failures.join("; ")
                };
                return Err(AppError::CorruptRecord {
                    file: format!("projects/{}/run.json", project_id.as_str()),
                    details: format!(
                        "batch journal append failed after {appended_count} of {} events: \
                         {journal_err}; snapshot restore: {snap_detail}, file cleanup: {file_detail}",
                        journal_lines.len()
                    ),
                });
            }

            return Err(journal_err);
        }
    }

    Ok(staged_ids)
}

/// Reopen a completed project to paused state with an interrupted run pointing
/// at the flow planning stage. Shared between manual and PR-review amendment paths.
///
/// Reads the current snapshot from disk. If the caller already holds a modified
/// snapshot (e.g. with a pending amendment already added), use
/// `reopen_completed_project_with_snapshot` instead.
pub fn reopen_completed_project(
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    project_store: &dyn ProjectStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<()> {
    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    reopen_completed_project_with_snapshot(
        run_write_port,
        project_store,
        base_dir,
        project_id,
        &mut snapshot,
    )
}

/// Reopen a completed project using an already-loaded (possibly modified) snapshot.
/// The snapshot is mutated in place and persisted atomically.
pub fn reopen_completed_project_with_snapshot(
    run_write_port: &dyn RunSnapshotWritePort,
    project_store: &dyn ProjectStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    snapshot: &mut RunSnapshot,
) -> AppResult<()> {
    let record = project_store.read_project_record(base_dir, project_id)?;

    if snapshot.status != RunStatus::Completed {
        return Ok(());
    }

    let planning_stage = planning_stage_for_flow(record.flow);
    let current_cycle = snapshot
        .cycle_history
        .last()
        .map(|entry| entry.cycle)
        .unwrap_or(1);
    let completion_round = snapshot.completion_rounds.max(1);

    let project_root = FileSystem::project_root(base_dir, project_id);
    let prompt_path = project_root.join(&record.prompt_reference);
    let prompt_contents =
        std::fs::read_to_string(&prompt_path).map_err(|error| AppError::CorruptRecord {
            file: prompt_path.display().to_string(),
            details: format!("failed to read prompt for project reopen: {error}"),
        })?;
    let prompt_hash = FileSystem::prompt_hash(&prompt_contents);

    // Advance the completion round so the resumed run creates new
    // payload/artifact IDs instead of overwriting the original history.
    let next_completion_round = completion_round + 1;
    snapshot.completion_rounds = next_completion_round;

    snapshot.interrupted_run = Some(ActiveRun {
        run_id: reopen_run_id(project_id),
        stage_cursor: StageCursor::new(planning_stage, current_cycle, 1, next_completion_round)?,
        started_at: Utc::now(),
        prompt_hash_at_cycle_start: prompt_hash.clone(),
        prompt_hash_at_stage_start: prompt_hash,
        qa_iterations_current_cycle: 0,
        review_iterations_current_cycle: 0,
        final_review_restart_count: 0,
        stage_resolution_snapshot: snapshot.last_stage_resolution_snapshot.clone(),
    });
    snapshot.active_run = None;
    snapshot.status = RunStatus::Paused;
    snapshot.status_summary = "paused: amendments staged".to_owned();

    run_write_port.write_run_snapshot(base_dir, project_id, snapshot)?;
    Ok(())
}

fn planning_stage_for_flow(flow: FlowPreset) -> StageId {
    match flow {
        FlowPreset::Standard => StageId::Planning,
        FlowPreset::QuickDev => StageId::PlanAndImplement,
        FlowPreset::DocsChange => StageId::DocsPlan,
        FlowPreset::CiImprovement => StageId::CiPlan,
    }
}
