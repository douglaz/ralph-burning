use std::path::Path;

use chrono::{DateTime, Utc};

use crate::shared::domain::{FlowPreset, ProjectId};
use crate::shared::error::{AppError, AppResult};

use super::journal;
use super::model::{
    ArtifactRecord, JournalEvent, JournalEventType, PayloadRecord, ProjectDetail, ProjectListEntry,
    ProjectRecord, ProjectStatusSummary, RunSnapshot, RuntimeLogEntry, SessionStore,
};
use super::queries::{self, RunHistoryView, RunStatusView, RunTailView};

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
        details: serde_json::json!({
            "project_id": record.id.as_str(),
            "flow": record.flow.as_str(),
        }),
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

/// Get run history (durable only, no runtime logs).
pub fn run_history(
    journal_port: &dyn JournalStorePort,
    artifact_port: &dyn ArtifactStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<RunHistoryView> {
    let events = journal_port.read_journal(base_dir, project_id)?;
    let payloads = artifact_port.list_payloads(base_dir, project_id)?;
    let artifacts = artifact_port.list_artifacts(base_dir, project_id)?;

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
    let events = journal_port.read_journal(base_dir, project_id)?;
    let payloads = artifact_port.list_payloads(base_dir, project_id)?;
    let artifacts = artifact_port.list_artifacts(base_dir, project_id)?;

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
