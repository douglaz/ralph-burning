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

    // Dedup check against pending amendments on disk.
    let pending = amendment_queue.list_pending_amendments(base_dir, project_id)?;
    if let Some(existing) = pending.iter().find(|a| a.dedup_key == dedup_key) {
        return Ok(AmendmentAddResult::Duplicate {
            amendment_id: existing.amendment_id.clone(),
        });
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

    // Write durable amendment file.
    amendment_queue.write_amendment(base_dir, project_id, &amendment)?;

    // Emit journal event.
    let events = journal_port.read_journal(base_dir, project_id)?;
    let seq = journal::last_sequence(&events) + 1;
    let journal_event = journal::amendment_queued_manual_event(
        seq,
        now,
        &amendment_id,
        body,
        "manual",
        &dedup_key,
    );
    let line = journal::serialize_event(&journal_event)?;
    if let Err(journal_err) = journal_port.append_event(base_dir, project_id, &line) {
        // Roll back the amendment file on journal failure.
        let _ = amendment_queue.remove_amendment(base_dir, project_id, &amendment_id);
        return Err(journal_err);
    }

    // Sync canonical snapshot: add the amendment to run.json pending queue.
    snapshot.amendment_queue.pending.push(amendment.clone());

    // If the project is completed, reopen it with the pending amendment
    // already reflected in the snapshot.
    if snapshot.status == RunStatus::Completed {
        reopen_completed_project_with_snapshot(
            run_write_port,
            project_store,
            base_dir,
            project_id,
            &mut snapshot,
        )?;
    } else {
        // Persist the updated snapshot with the new pending amendment.
        run_write_port.write_run_snapshot(base_dir, project_id, &snapshot)?;
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

/// Remove a single pending amendment by ID. Updates both disk and run.json.
pub fn remove_amendment(
    amendment_queue: &dyn AmendmentQueuePort,
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    amendment_id: &str,
) -> AppResult<()> {
    // Verify the amendment exists before removing.
    let pending = amendment_queue.list_pending_amendments(base_dir, project_id)?;
    if !pending.iter().any(|a| a.amendment_id == amendment_id) {
        return Err(AppError::AmendmentNotFound {
            amendment_id: amendment_id.to_owned(),
        });
    }
    amendment_queue.remove_amendment(base_dir, project_id, amendment_id)?;

    // Sync canonical snapshot: remove from run.json pending queue.
    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    snapshot
        .amendment_queue
        .pending
        .retain(|a| a.amendment_id != amendment_id);
    run_write_port.write_run_snapshot(base_dir, project_id, &snapshot)?;
    Ok(())
}

/// Clear all pending amendments. Returns removed and remaining IDs.
/// On partial failure, reports exactly which amendments were removed and which remain.
/// Both disk and run.json are kept in sync.
pub fn clear_amendments(
    amendment_queue: &dyn AmendmentQueuePort,
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<Vec<String>> {
    let pending = amendment_queue.list_pending_amendments(base_dir, project_id)?;
    if pending.is_empty() {
        return Ok(Vec::new());
    }

    let total = pending.len();
    let mut removed = Vec::new();
    let mut remaining = Vec::new();

    for amendment in &pending {
        match amendment_queue.remove_amendment(base_dir, project_id, &amendment.amendment_id) {
            Ok(()) => removed.push(amendment.amendment_id.clone()),
            Err(_) => remaining.push(amendment.amendment_id.clone()),
        }
    }

    // Sync canonical snapshot: keep only remaining IDs in run.json.
    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    let remaining_set: std::collections::HashSet<&str> =
        remaining.iter().map(|s| s.as_str()).collect();
    snapshot
        .amendment_queue
        .pending
        .retain(|a| remaining_set.contains(a.amendment_id.as_str()));
    run_write_port.write_run_snapshot(base_dir, project_id, &snapshot)?;

    if !remaining.is_empty() {
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

    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    let events = journal_port.read_journal(base_dir, project_id)?;
    let mut seq = journal::last_sequence(&events);
    let mut staged_ids = Vec::new();

    for amendment in amendments {
        // Dedup check against pending queue (disk).
        let pending = amendment_queue.list_pending_amendments(base_dir, project_id)?;
        if pending.iter().any(|a| a.dedup_key == amendment.dedup_key) {
            continue;
        }

        // Write durable amendment file.
        amendment_queue.write_amendment(base_dir, project_id, amendment)?;

        // Emit journal event.
        seq += 1;
        let journal_event = journal::amendment_queued_manual_event(
            seq,
            amendment.created_at,
            &amendment.amendment_id,
            &amendment.body,
            amendment.source.as_str(),
            &amendment.dedup_key,
        );
        let line = journal::serialize_event(&journal_event)?;
        if let Err(journal_err) = journal_port.append_event(base_dir, project_id, &line) {
            // Roll back the amendment file on journal failure.
            let _ = amendment_queue.remove_amendment(base_dir, project_id, &amendment.amendment_id);
            return Err(journal_err);
        }

        // Sync canonical snapshot.
        snapshot.amendment_queue.pending.push(amendment.clone());
        staged_ids.push(amendment.amendment_id.clone());
    }

    if staged_ids.is_empty() {
        return Ok(Vec::new());
    }

    // If the project is completed, reopen it with pending amendments.
    if snapshot.status == RunStatus::Completed {
        reopen_completed_project_with_snapshot(
            run_write_port,
            project_store,
            base_dir,
            project_id,
            &mut snapshot,
        )?;
    } else {
        // Persist the updated snapshot.
        run_write_port.write_run_snapshot(base_dir, project_id, &snapshot)?;
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

    snapshot.interrupted_run = Some(ActiveRun {
        run_id: format!("reopen-{}", project_id.as_str()),
        stage_cursor: StageCursor::new(planning_stage, current_cycle, 1, completion_round)?,
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
