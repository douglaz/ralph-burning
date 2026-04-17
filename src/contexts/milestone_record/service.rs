#![allow(clippy::too_many_arguments)]
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};
use tracing::Instrument;

use crate::adapters::br_health::{beads_health_failure_details, check_beads_health};
use crate::adapters::br_models::{BeadDetail, BeadStatus, BeadSummary, BeadType, DependencyKind};
use crate::adapters::br_process::{
    BrAdapter, BrCommand, BrMutationAdapter, ProcessRunner, SyncIfDirtyHealthError,
};
use crate::adapters::fs::FileSystem;
use crate::contexts::project_run_record::service::ProjectStorePort;
use crate::contexts::workflow_composition::review_classification::Severity;
use crate::shared::error::{AppError, AppResult};

use super::bundle::{
    bead_matches_implicit_slot, explicit_id_hints, normalize_bead_reference,
    progress_shape_signature, progress_shape_signature_with_explicit_id_hints, render_plan_json,
    render_plan_md_checked, MilestoneBundle,
};
use super::model::{
    collapse_task_run_attempts, latest_task_runs_per_bead, CompletionJournalDetails,
    MilestoneEventType, MilestoneId, MilestoneJournalEvent, MilestoneProgress, MilestoneRecord,
    MilestoneSnapshot, MilestoneStatus, PendingLineageReset, PlannedElsewhereMapping,
    StartJournalDetails, TaskRunEntry, TaskRunOutcome,
};
use super::queries::{
    BeadExecutionHistoryView, BeadLineageView, MilestoneTaskListView, MilestoneTaskView,
    TaskRunAttemptView,
};

// ── Ports ───────────────────────────────────────────────────────────────────

/// Port for reading and writing milestone records.
pub trait MilestoneStorePort {
    fn milestone_exists(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<bool>;
    fn read_milestone_record(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<MilestoneRecord>;
    fn write_milestone_record(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        record: &MilestoneRecord,
    ) -> AppResult<()>;
    fn list_milestone_ids(&self, base_dir: &Path) -> AppResult<Vec<MilestoneId>>;
    fn create_milestone_atomic(
        &self,
        base_dir: &Path,
        record: &MilestoneRecord,
        snapshot: &MilestoneSnapshot,
        initial_journal_line: &str,
    ) -> AppResult<()>;
}

/// Port for reading and writing milestone status snapshots.
pub trait MilestoneSnapshotPort {
    fn read_snapshot(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<MilestoneSnapshot>;
    fn write_snapshot(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        snapshot: &MilestoneSnapshot,
    ) -> AppResult<()>;
    /// Serialize multi-file milestone mutations for a single milestone.
    fn with_milestone_write_lock<T, F>(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        operation: F,
    ) -> AppResult<T>
    where
        F: FnOnce() -> AppResult<T>;
}

/// Port for appending milestone journal events.
pub trait MilestoneJournalPort {
    fn read_journal(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<MilestoneJournalEvent>>;
    fn append_event(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        line: &str,
    ) -> AppResult<()>;
    /// Atomically replace the milestone journal with an exact event set.
    fn replace_journal(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        events: &[MilestoneJournalEvent],
    ) -> AppResult<()>;
    /// Atomically append an event only when an identical entry is not present.
    fn append_event_if_missing(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        event: &MilestoneJournalEvent,
    ) -> AppResult<bool>;
    /// Explicitly repair a completion event for an already-known attempt.
    /// Unlike `append_event_if_missing`, this path may replace a stale
    /// completion row when a narrower repair flow has already established that
    /// the exact bead/project/run attempt should be corrected.
    fn repair_completion_event(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        event: &MilestoneJournalEvent,
    ) -> AppResult<bool> {
        self.append_event_if_missing(base_dir, milestone_id, event)
    }

    /// Atomically apply a sequence of journal mutations for a single milestone.
    ///
    /// The default implementation preserves legacy behavior for non-filesystem
    /// fakes, but concrete stores should override this when they can provide a
    /// single durable commit for the whole mutation set.
    fn commit_journal_ops(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        ops: &[JournalWriteOp],
    ) -> AppResult<usize> {
        if ops.is_empty() {
            return Ok(0);
        }

        let mut journal = self.read_journal(base_dir, milestone_id)?;
        let applied = apply_journal_write_ops(&mut journal, ops);
        if applied == 0 {
            return Ok(0);
        }

        self.replace_journal(base_dir, milestone_id, &journal)?;
        Ok(applied)
    }

    /// Atomically persist a snapshot update plus the associated journal ops.
    ///
    /// The default implementation preserves the previous write-then-rollback
    /// behavior. Concrete stores should override this when they can provide a
    /// crash-safe multi-file commit.
    fn commit_snapshot_and_journal_ops<S: MilestoneSnapshotPort>(
        &self,
        snapshot_store: &S,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        previous_snapshot: &MilestoneSnapshot,
        next_snapshot: &MilestoneSnapshot,
        ops: &[JournalWriteOp],
        error_context: &str,
    ) -> AppResult<usize> {
        snapshot_store.write_snapshot(base_dir, milestone_id, next_snapshot)?;
        match self.commit_journal_ops(base_dir, milestone_id, ops) {
            Ok(applied) => Ok(applied),
            Err(append_error) => {
                if let Err(restore_error) =
                    snapshot_store.write_snapshot(base_dir, milestone_id, previous_snapshot)
                {
                    return Err(AppError::CorruptRecord {
                        file: format!("milestones/{}/status.json", milestone_id),
                        details: format!(
                            "{error_context}: {append_error}; failed to restore the previous snapshot: {restore_error}"
                        ),
                    });
                }
                Err(append_error)
            }
        }
    }
}

/// Port for reading and writing task-run lineage.
pub trait TaskRunLineagePort {
    fn read_task_runs(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<TaskRunEntry>>;
    fn append_task_run(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        entry: &TaskRunEntry,
    ) -> AppResult<()>;
    /// Atomically reuse an existing running attempt or append a new start row.
    #[allow(clippy::too_many_arguments)]
    fn record_task_run_start(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
        project_id: &str,
        run_id: &str,
        plan_hash: &str,
        started_at: DateTime<Utc>,
    ) -> AppResult<TaskRunEntry>;
    /// Update an existing task run entry's outcome by matching bead_id + project_id + run_id.
    /// Implementations should reject ambiguous matches or stale started_at
    /// mismatches instead of rewriting an arbitrary historical attempt.
    #[allow(clippy::too_many_arguments)]
    fn update_task_run(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
        project_id: &str,
        run_id: &str,
        plan_hash: Option<&str>,
        started_at: DateTime<Utc>,
        outcome: TaskRunOutcome,
        outcome_detail: Option<String>,
        finished_at: DateTime<Utc>,
    ) -> AppResult<TaskRunEntry>;
    /// Explicitly repair the terminal state of an exact historical attempt.
    /// This is narrower than `update_task_run`: callers must already know they
    /// are correcting the same bead/project/run attempt instead of replaying a
    /// generic terminal completion.
    #[allow(clippy::too_many_arguments)]
    fn repair_task_run_terminal(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
        project_id: &str,
        run_id: &str,
        plan_hash: Option<&str>,
        started_at: DateTime<Utc>,
        outcome: TaskRunOutcome,
        outcome_detail: Option<String>,
        finished_at: DateTime<Utc>,
    ) -> AppResult<TaskRunEntry> {
        self.update_task_run(
            base_dir,
            milestone_id,
            bead_id,
            project_id,
            run_id,
            plan_hash,
            started_at,
            outcome,
            outcome_detail,
            finished_at,
        )
    }
    /// Find all task run entries for a specific bead, in chronological order.
    fn find_runs_for_bead(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        bead_id: &str,
    ) -> AppResult<Vec<TaskRunEntry>>;
}

/// Port for reading and writing plan artifacts.
pub trait MilestonePlanPort {
    fn read_plan_json(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<String>;
    fn write_plan_json(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        content: &str,
    ) -> AppResult<()>;
    fn read_plan_md(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<String>;
    fn write_plan_md(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        content: &str,
    ) -> AppResult<()>;
    fn read_plan_shape(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<String>;
    fn write_plan_shape(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        content: &str,
    ) -> AppResult<()>;
}

/// Port for reading and writing planned-elsewhere mappings.
pub trait PlannedElsewhereMappingPort {
    fn read_mappings(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<PlannedElsewhereMapping>>;
    fn append_mapping(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        mapping: &PlannedElsewhereMapping,
    ) -> AppResult<()>;
}

fn snapshot_corrupt_record(milestone_id: &MilestoneId, details: impl Into<String>) -> AppError {
    AppError::CorruptRecord {
        file: format!("milestones/{}/status.json", milestone_id),
        details: details.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredPlanShape {
    plan_hash: String,
    shape_signature: String,
    #[serde(default, skip_serializing_if = "is_false")]
    lineage_reset_required: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn hash_text(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn render_plan_shape_artifact(plan_hash: &str, shape_signature: &str) -> AppResult<String> {
    serde_json::to_string_pretty(&StoredPlanShape {
        plan_hash: plan_hash.to_owned(),
        shape_signature: shape_signature.to_owned(),
        lineage_reset_required: false,
    })
    .map_err(AppError::SerdeJson)
}

fn render_plan_shape_artifact_with_lineage_reset(
    plan_hash: &str,
    shape_signature: &str,
    lineage_reset: Option<&PendingLineageReset>,
) -> AppResult<String> {
    serde_json::to_string_pretty(&StoredPlanShape {
        plan_hash: plan_hash.to_owned(),
        shape_signature: shape_signature.to_owned(),
        lineage_reset_required: lineage_reset.is_some(),
    })
    .map_err(AppError::SerdeJson)
}

fn plan_json_path(base_dir: &Path, milestone_id: &MilestoneId) -> std::path::PathBuf {
    FileSystem::milestone_root(base_dir, milestone_id).join("plan.json")
}

fn plan_shape_path(base_dir: &Path, milestone_id: &MilestoneId) -> std::path::PathBuf {
    FileSystem::milestone_root(base_dir, milestone_id).join("plan.shape.json")
}

fn snapshot_has_pending_lineage_reset(snapshot: &MilestoneSnapshot) -> bool {
    snapshot.pending_lineage_reset.is_some()
}

fn pending_lineage_reset_for_snapshot(snapshot: &MilestoneSnapshot) -> Option<PendingLineageReset> {
    snapshot.pending_lineage_reset.clone()
}

fn render_committed_plan_shape_from_snapshot(
    snapshot: &MilestoneSnapshot,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<String> {
    let plan_hash = snapshot.plan_hash.as_deref().ok_or_else(|| {
        snapshot_corrupt_record(
            milestone_id,
            "pending lineage reset requires snapshot.plan_hash to be present",
        )
    })?;
    let plan_json = std::fs::read_to_string(plan_json_path(base_dir, milestone_id))?;
    let actual_plan_hash = hash_text(&plan_json);
    if actual_plan_hash != plan_hash {
        return Err(snapshot_corrupt_record(
            milestone_id,
            format!(
                "pending lineage reset expected committed plan hash '{plan_hash}' but plan.json hashes to '{actual_plan_hash}'"
            ),
        ));
    }
    let bundle: MilestoneBundle = serde_json::from_str(&plan_json).map_err(|error| {
        snapshot_corrupt_record(
            milestone_id,
            format!("pending lineage reset could not parse committed plan.json: {error}"),
        )
    })?;
    let shape_signature = progress_shape_signature(&bundle)
        .map_err(|errors| snapshot_corrupt_record(milestone_id, errors.join("; ")))?;
    render_plan_shape_artifact(plan_hash, &shape_signature)
}

fn clear_pending_lineage_reset_locked(
    snapshot_store: &impl MilestoneSnapshotPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    snapshot: &mut MilestoneSnapshot,
) -> AppResult<()> {
    if snapshot.pending_lineage_reset.is_none() {
        return Ok(());
    }

    let committed_plan_shape =
        render_committed_plan_shape_from_snapshot(snapshot, base_dir, milestone_id)?;
    clear_task_run_lineage(base_dir, milestone_id)?;
    FileSystem::write_atomic(
        &plan_shape_path(base_dir, milestone_id),
        &committed_plan_shape,
    )?;

    snapshot.pending_lineage_reset = None;
    validate_snapshot(snapshot, milestone_id)?;
    snapshot_store.write_snapshot(base_dir, milestone_id, snapshot)?;
    Ok(())
}

fn validate_snapshot(snapshot: &MilestoneSnapshot, milestone_id: &MilestoneId) -> AppResult<()> {
    snapshot
        .validate_semantics()
        .map_err(|details| snapshot_corrupt_record(milestone_id, details))
}

struct LifecycleTransitionCommit {
    snapshot: MilestoneSnapshot,
    event: MilestoneJournalEvent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionMilestoneDisposition {
    ReconcileFromLineage,
    MarkMilestoneFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalWriteOpKind {
    AppendIfMissing,
    RepairCompletion,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JournalWriteOp {
    pub event: MilestoneJournalEvent,
    pub kind: JournalWriteOpKind,
}

impl JournalWriteOp {
    fn append_if_missing(event: MilestoneJournalEvent) -> Self {
        Self {
            event,
            kind: JournalWriteOpKind::AppendIfMissing,
        }
    }

    fn repair_completion(event: MilestoneJournalEvent) -> Self {
        Self {
            event,
            kind: JournalWriteOpKind::RepairCompletion,
        }
    }
}

fn format_allowed_transition_targets(status: MilestoneStatus) -> String {
    let targets = status
        .allowed_transition_targets()
        .iter()
        .map(|status| status.as_str())
        .collect::<Vec<_>>();
    if targets.is_empty() {
        "none".to_owned()
    } else {
        targets.join(", ")
    }
}

fn invalid_transition_error(
    milestone_id: &MilestoneId,
    from_status: MilestoneStatus,
    to_status: MilestoneStatus,
) -> AppError {
    AppError::InvalidConfigValue {
        key: "milestone_transition".to_owned(),
        value: format!("{from_status} -> {to_status}"),
        reason: format!(
            "milestone '{milestone_id}' allows transitions from '{from_status}' only to: {}",
            format_allowed_transition_targets(from_status)
        ),
    }
}

fn has_finalized_plan(snapshot: &MilestoneSnapshot) -> bool {
    snapshot.plan_version > 0 && snapshot.plan_hash.is_some()
}

fn validate_transition_prerequisites(
    snapshot: &MilestoneSnapshot,
    milestone_id: &MilestoneId,
    to_status: MilestoneStatus,
) -> AppResult<()> {
    if matches!(
        to_status,
        MilestoneStatus::Ready | MilestoneStatus::Running | MilestoneStatus::Completed
    ) && !has_finalized_plan(snapshot)
    {
        return Err(AppError::InvalidConfigValue {
            key: "milestone_transition".to_owned(),
            value: format!("{} -> {}", snapshot.status, to_status),
            reason: format!(
                "milestone '{milestone_id}' cannot move to '{to_status}' before a plan is finalized and exported"
            ),
        });
    }

    if to_status == MilestoneStatus::Completed {
        let closed_beads = snapshot
            .progress
            .completed_beads
            .saturating_add(snapshot.progress.skipped_beads);
        let all_beads_closed =
            snapshot.progress.total_beads == 0 || closed_beads >= snapshot.progress.total_beads;
        if snapshot.progress.in_progress_beads > 0
            || snapshot.progress.failed_beads > 0
            || !all_beads_closed
        {
            let failed_beads_hint = if snapshot.progress.failed_beads > 0 {
                "; re-run or skip failed beads to unblock completion"
            } else {
                ""
            };
            return Err(AppError::InvalidConfigValue {
                key: "milestone_transition".to_owned(),
                value: format!("{} -> {}", snapshot.status, to_status),
                reason: format!(
                    "milestone '{milestone_id}' cannot move to '{to_status}' until all beads are closed (total={}, completed={}, skipped={}, in_progress={}, failed={}){failed_beads_hint}",
                    snapshot.progress.total_beads,
                    snapshot.progress.completed_beads,
                    snapshot.progress.skipped_beads,
                    snapshot.progress.in_progress_beads,
                    snapshot.progress.failed_beads,
                ),
            });
        }
    }

    Ok(())
}

fn insert_metadata_u32(metadata: &mut JsonMap<String, JsonValue>, key: &str, value: u32) {
    metadata.insert(key.to_owned(), JsonValue::from(u64::from(value)));
}

fn insert_metadata_string(
    metadata: &mut JsonMap<String, JsonValue>,
    key: &str,
    value: Option<&str>,
) {
    if let Some(value) = value {
        metadata.insert(key.to_owned(), JsonValue::from(value));
    }
}

fn event_plan_version(event: &MilestoneJournalEvent) -> Option<u32> {
    event
        .metadata
        .as_ref()?
        .get("plan_version")?
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
}

fn event_matches_plan_version(event: &MilestoneJournalEvent, plan_version: u32) -> bool {
    match event_plan_version(event) {
        Some(event_plan_version) => event_plan_version == plan_version,
        None => plan_version <= 1,
    }
}

fn accumulated_running_duration_seconds(
    journal: &[MilestoneJournalEvent],
    from_status: MilestoneStatus,
    plan_version: u32,
    running_started_fallback: DateTime<Utc>,
    now: DateTime<Utc>,
) -> i64 {
    let mut total_seconds = 0_i64;
    let mut running_started_at = None;
    let mut lifecycle_events: Vec<_> = journal
        .iter()
        .enumerate()
        .filter(|(_, event)| {
            event.event_type == MilestoneEventType::StatusChanged
                && event_matches_plan_version(event, plan_version)
        })
        .collect();
    lifecycle_events.sort_by_key(|(index, event)| (event.timestamp, *index));

    for (_, event) in lifecycle_events {
        match (event.from_state, event.to_state) {
            (_, Some(MilestoneStatus::Running)) => {
                running_started_at.get_or_insert(event.timestamp);
            }
            (
                Some(MilestoneStatus::Running),
                Some(
                    MilestoneStatus::Paused | MilestoneStatus::Completed | MilestoneStatus::Failed,
                ),
            ) => {
                if let Some(started_at) = running_started_at.take() {
                    total_seconds += (event.timestamp - started_at).num_seconds().max(0);
                }
            }
            _ => {}
        }
    }

    if from_status == MilestoneStatus::Running {
        let started_at = running_started_at.unwrap_or(running_started_fallback);
        total_seconds += (now - started_at).num_seconds().max(0);
    }

    total_seconds
}

fn build_transition_snapshot(
    snapshot: &MilestoneSnapshot,
    milestone_id: &MilestoneId,
    to_status: MilestoneStatus,
    now: DateTime<Utc>,
) -> AppResult<MilestoneSnapshot> {
    let from_status = snapshot.status;
    if !from_status.allows_transition_to(to_status) {
        return Err(invalid_transition_error(
            milestone_id,
            from_status,
            to_status,
        ));
    }
    validate_transition_prerequisites(snapshot, milestone_id, to_status)?;

    let mut next_snapshot = snapshot.clone();
    next_snapshot.status = to_status;
    if matches!(
        to_status,
        MilestoneStatus::Planning
            | MilestoneStatus::Ready
            | MilestoneStatus::Paused
            | MilestoneStatus::Completed
            | MilestoneStatus::Failed
    ) {
        next_snapshot.active_bead = None;
    }
    next_snapshot.updated_at = now;
    validate_snapshot(&next_snapshot, milestone_id)?;
    Ok(next_snapshot)
}

fn lifecycle_transition_metadata(
    snapshot: &MilestoneSnapshot,
    from_status: MilestoneStatus,
    to_status: MilestoneStatus,
    journal: &[MilestoneJournalEvent],
    running_started_fallback: DateTime<Utc>,
    now: DateTime<Utc>,
) -> JsonMap<String, JsonValue> {
    let mut metadata = JsonMap::new();
    insert_metadata_u32(&mut metadata, "plan_version", snapshot.plan_version);
    insert_metadata_u32(&mut metadata, "total_beads", snapshot.progress.total_beads);
    insert_metadata_u32(
        &mut metadata,
        "completed_beads",
        snapshot.progress.completed_beads,
    );
    insert_metadata_u32(
        &mut metadata,
        "failed_beads",
        snapshot.progress.failed_beads,
    );
    insert_metadata_u32(
        &mut metadata,
        "skipped_beads",
        snapshot.progress.skipped_beads,
    );
    insert_metadata_u32(
        &mut metadata,
        "in_progress_beads",
        snapshot.progress.in_progress_beads,
    );
    insert_metadata_string(&mut metadata, "plan_hash", snapshot.plan_hash.as_deref());
    insert_metadata_string(
        &mut metadata,
        "active_bead",
        snapshot.active_bead.as_deref(),
    );

    if matches!(
        to_status,
        MilestoneStatus::Completed | MilestoneStatus::Failed
    ) {
        let duration_seconds = accumulated_running_duration_seconds(
            journal,
            from_status,
            snapshot.plan_version,
            running_started_fallback,
            now,
        );
        metadata.insert(
            "duration_seconds".to_owned(),
            JsonValue::from(duration_seconds),
        );
        if to_status == MilestoneStatus::Completed {
            metadata.insert(
                "bead_count".to_owned(),
                JsonValue::from(u64::from(snapshot.progress.total_beads)),
            );
        }
    }

    if from_status == MilestoneStatus::Paused && to_status == MilestoneStatus::Running {
        metadata.insert("resumed".to_owned(), JsonValue::from(true));
    }

    metadata
}

fn completion_status_reason(status: MilestoneStatus) -> &'static str {
    match status {
        MilestoneStatus::Ready => "milestone ready for execution",
        MilestoneStatus::Running => "execution continued",
        MilestoneStatus::Paused => "execution paused",
        MilestoneStatus::Completed => "all beads closed",
        MilestoneStatus::Failed => "unrecoverable error requires operator intervention",
        MilestoneStatus::Planning => "planning resumed",
    }
}

fn apply_completion_milestone_disposition(
    snapshot: &mut MilestoneSnapshot,
    disposition: CompletionMilestoneDisposition,
) {
    if disposition == CompletionMilestoneDisposition::MarkMilestoneFailed
        && snapshot.progress.failed_beads > 0
    {
        snapshot.status = MilestoneStatus::Failed;
        snapshot.active_bead = None;
        snapshot.progress.in_progress_beads = 0;
    }
}

fn build_synthetic_transition_snapshot(
    current_snapshot: &MilestoneSnapshot,
    final_snapshot: &MilestoneSnapshot,
    to_status: MilestoneStatus,
    event_timestamp: DateTime<Utc>,
) -> MilestoneSnapshot {
    let mut synthetic_snapshot = current_snapshot.clone();
    synthetic_snapshot.status = to_status;
    synthetic_snapshot.plan_hash = final_snapshot.plan_hash.clone();
    synthetic_snapshot.plan_version = final_snapshot.plan_version;
    synthetic_snapshot.progress.total_beads = final_snapshot.progress.total_beads;
    synthetic_snapshot.updated_at = event_timestamp;

    match to_status {
        MilestoneStatus::Ready => {
            synthetic_snapshot.active_bead = None;
            synthetic_snapshot.progress.in_progress_beads = 0;
        }
        MilestoneStatus::Running => {
            synthetic_snapshot.active_bead = synthetic_snapshot
                .active_bead
                .clone()
                .or_else(|| final_snapshot.active_bead.clone());
            if synthetic_snapshot.progress.in_progress_beads == 0 {
                synthetic_snapshot.progress.in_progress_beads = 1;
            }
        }
        MilestoneStatus::Planning
        | MilestoneStatus::Paused
        | MilestoneStatus::Completed
        | MilestoneStatus::Failed => {
            synthetic_snapshot.active_bead = None;
            synthetic_snapshot.progress.in_progress_beads = 0;
        }
    }

    synthetic_snapshot
}

fn build_reconciled_transition_events(
    milestone_id: &MilestoneId,
    previous_snapshot: &MilestoneSnapshot,
    snapshot: &MilestoneSnapshot,
    journal: &[MilestoneJournalEvent],
    now: DateTime<Utc>,
    running_started_at: Option<DateTime<Utc>>,
    actor: &str,
    reason: &str,
) -> AppResult<Vec<MilestoneJournalEvent>> {
    let previous_status = previous_snapshot.status;
    if previous_status == snapshot.status {
        return Ok(Vec::new());
    }
    let transition_path: Vec<_> = if previous_status.allows_transition_to(snapshot.status) {
        vec![snapshot.status]
    } else {
        match (previous_status, snapshot.status) {
            (MilestoneStatus::Planning, MilestoneStatus::Running) => {
                vec![MilestoneStatus::Ready, MilestoneStatus::Running]
            }
            (
                MilestoneStatus::Planning,
                MilestoneStatus::Paused | MilestoneStatus::Completed | MilestoneStatus::Failed,
            ) => vec![
                MilestoneStatus::Ready,
                MilestoneStatus::Running,
                snapshot.status,
            ],
            (
                MilestoneStatus::Ready,
                MilestoneStatus::Paused | MilestoneStatus::Completed | MilestoneStatus::Failed,
            ) => vec![MilestoneStatus::Running, snapshot.status],
            (MilestoneStatus::Paused, MilestoneStatus::Completed | MilestoneStatus::Failed) => {
                vec![MilestoneStatus::Running, snapshot.status]
            }
            _ => {
                return Err(invalid_transition_error(
                    milestone_id,
                    previous_status,
                    snapshot.status,
                ))
            }
        }
    };

    let mut events = Vec::with_capacity(transition_path.len());
    let mut from_status = previous_status;
    let mut current_snapshot = previous_snapshot.clone();
    let mut event_journal = journal.to_vec();
    for (index, to_status) in transition_path.iter().copied().enumerate() {
        let is_final = index + 1 == transition_path.len();
        let event_timestamp = if is_final {
            now
        } else if to_status == MilestoneStatus::Ready {
            running_started_at.unwrap_or(now)
        } else if to_status == MilestoneStatus::Running {
            if from_status == MilestoneStatus::Paused {
                running_started_at
                    .filter(|started_at| *started_at >= previous_snapshot.updated_at)
                    .unwrap_or(now)
            } else {
                running_started_at.unwrap_or(now)
            }
        } else {
            now
        };
        let event_snapshot = if is_final {
            snapshot.clone()
        } else {
            build_synthetic_transition_snapshot(
                &current_snapshot,
                snapshot,
                to_status,
                event_timestamp,
            )
        };
        let event_reason = if is_final {
            reason
        } else {
            match to_status {
                MilestoneStatus::Ready => "plan finalized and beads exported",
                MilestoneStatus::Running => {
                    if from_status == MilestoneStatus::Paused {
                        "execution resumed"
                    } else {
                        "execution started"
                    }
                }
                MilestoneStatus::Paused => "execution paused",
                MilestoneStatus::Completed => "all beads closed",
                MilestoneStatus::Failed => "unrecoverable error requires operator intervention",
                MilestoneStatus::Planning => "planning resumed",
            }
        };
        events.push(MilestoneJournalEvent::lifecycle_transition(
            event_timestamp,
            from_status,
            to_status,
            actor,
            event_reason,
            lifecycle_transition_metadata(
                &event_snapshot,
                from_status,
                to_status,
                &event_journal,
                running_started_at.unwrap_or(previous_snapshot.updated_at),
                event_timestamp,
            ),
        ));
        if let Some(event) = events.last() {
            event_journal.push(event.clone());
        }
        current_snapshot = event_snapshot;
        from_status = to_status;
    }

    Ok(events)
}

fn write_snapshot_with_atomic_transition(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    previous_snapshot: &MilestoneSnapshot,
    transition: LifecycleTransitionCommit,
) -> AppResult<()> {
    journal_store.commit_snapshot_and_journal_ops(
        snapshot_store,
        base_dir,
        milestone_id,
        previous_snapshot,
        &transition.snapshot,
        &[JournalWriteOp::append_if_missing(transition.event)],
        "lifecycle journal append failed after snapshot write",
    )?;
    Ok(())
}

fn commit_snapshot_and_journal_ops(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    previous_snapshot: &MilestoneSnapshot,
    next_snapshot: &MilestoneSnapshot,
    journal_ops: &[JournalWriteOp],
    error_context: &str,
) -> AppResult<()> {
    journal_store.commit_snapshot_and_journal_ops(
        snapshot_store,
        base_dir,
        milestone_id,
        previous_snapshot,
        next_snapshot,
        journal_ops,
        error_context,
    )?;
    Ok(())
}

fn reconcile_snapshot_from_lineage(
    snapshot: &mut MilestoneSnapshot,
    milestone_id: &MilestoneId,
    task_runs: Vec<TaskRunEntry>,
) -> AppResult<()> {
    let task_runs = collapse_task_run_attempts(task_runs);
    let current_bead_runs = latest_task_runs_per_bead(&task_runs);
    let has_any_task_runs = !current_bead_runs.is_empty();
    let active_beads: BTreeSet<String> = current_bead_runs
        .iter()
        .filter(|entry| !entry.outcome.is_terminal())
        .map(|entry| entry.bead_id.clone())
        .collect();

    snapshot.active_bead = match active_beads.len() {
        0 => None,
        1 => active_beads.iter().next().cloned(),
        _ => {
            let bead_list = active_beads.into_iter().collect::<Vec<_>>().join(", ");
            return Err(snapshot_corrupt_record(
                milestone_id,
                format!("multiple active beads present in lineage: {bead_list}"),
            ));
        }
    };

    snapshot.progress.in_progress_beads = current_bead_runs
        .iter()
        .filter(|entry| !entry.outcome.is_terminal())
        .count() as u32;
    snapshot.progress.completed_beads = current_bead_runs
        .iter()
        .filter(|entry| entry.outcome == TaskRunOutcome::Succeeded)
        .count() as u32;
    snapshot.progress.failed_beads = current_bead_runs
        .iter()
        .filter(|entry| entry.outcome == TaskRunOutcome::Failed)
        .count() as u32;
    snapshot.progress.skipped_beads = current_bead_runs
        .iter()
        .filter(|entry| entry.outcome == TaskRunOutcome::Skipped)
        .count() as u32;

    if snapshot.active_bead.is_some() {
        if snapshot.status.is_terminal() {
            return Err(snapshot_corrupt_record(
                milestone_id,
                format!(
                    "status is '{}' but lineage still has an active bead",
                    snapshot.status
                ),
            ));
        }
        snapshot.status = MilestoneStatus::Running;
    } else if snapshot.progress.total_beads > 0
        && snapshot.progress.completed_beads + snapshot.progress.skipped_beads
            >= snapshot.progress.total_beads
        && snapshot.progress.failed_beads == 0
    {
        snapshot.status = MilestoneStatus::Completed;
        snapshot.active_bead = None;
    } else if snapshot.status == MilestoneStatus::Paused {
        snapshot.active_bead = None;
    } else if has_any_task_runs && snapshot.status == MilestoneStatus::Running {
        snapshot.status = MilestoneStatus::Paused;
        snapshot.active_bead = None;
    } else if has_any_task_runs && !snapshot.status.is_terminal() {
        snapshot.status = if snapshot.progress.total_beads > 0 {
            MilestoneStatus::Paused
        } else {
            MilestoneStatus::Ready
        };
        snapshot.active_bead = None;
    }

    Ok(())
}

fn event_type_for_outcome(outcome: TaskRunOutcome) -> MilestoneEventType {
    match outcome {
        TaskRunOutcome::Succeeded => MilestoneEventType::BeadCompleted,
        TaskRunOutcome::Failed => MilestoneEventType::BeadFailed,
        TaskRunOutcome::Skipped => MilestoneEventType::BeadSkipped,
        TaskRunOutcome::Pending | TaskRunOutcome::Running => {
            unreachable!("non-terminal outcomes are rejected before journal construction")
        }
    }
}

fn option_conflicts(existing: Option<&str>, requested: Option<&str>) -> bool {
    matches!((existing, requested), (Some(existing), Some(requested)) if existing != requested)
}

fn merge_start_journal_details(
    existing: &StartJournalDetails,
    requested: &StartJournalDetails,
) -> Option<StartJournalDetails> {
    if existing.project_id != requested.project_id
        || option_conflicts(existing.run_id.as_deref(), requested.run_id.as_deref())
        || option_conflicts(
            existing.plan_hash.as_deref(),
            requested.plan_hash.as_deref(),
        )
    {
        return None;
    }

    let mut merged = existing.clone();
    if merged.run_id.is_none() {
        merged.run_id = requested.run_id.clone();
    }
    if merged.plan_hash.is_none() {
        merged.plan_hash = requested.plan_hash.clone();
    }
    Some(merged)
}

fn merge_completion_journal_details(
    existing: &CompletionJournalDetails,
    requested: &CompletionJournalDetails,
) -> Option<CompletionJournalDetails> {
    if existing.project_id != requested.project_id
        || existing.started_at != requested.started_at
        || existing.outcome != requested.outcome
        || option_conflicts(existing.run_id.as_deref(), requested.run_id.as_deref())
        || option_conflicts(
            existing.plan_hash.as_deref(),
            requested.plan_hash.as_deref(),
        )
        || option_conflicts(
            existing.outcome_detail.as_deref(),
            requested.outcome_detail.as_deref(),
        )
        || option_conflicts(existing.task_id.as_deref(), requested.task_id.as_deref())
    {
        return None;
    }

    let mut merged = existing.clone();
    if merged.run_id.is_none() {
        merged.run_id = requested.run_id.clone();
    }
    if merged.plan_hash.is_none() {
        merged.plan_hash = requested.plan_hash.clone();
    }
    if merged.outcome_detail.is_none() {
        merged.outcome_detail = requested.outcome_detail.clone();
    }
    if merged.task_id.is_none() {
        merged.task_id = requested.task_id.clone();
    }
    Some(merged)
}

fn render_journal_details<T: Serialize>(details: &T) -> Option<String> {
    serde_json::to_string(details).ok()
}

fn is_completion_event(event: &MilestoneJournalEvent) -> bool {
    matches!(
        event.event_type,
        MilestoneEventType::BeadCompleted
            | MilestoneEventType::BeadFailed
            | MilestoneEventType::BeadSkipped
    )
}

fn repairable_start_event(
    existing: &MilestoneJournalEvent,
    requested: &MilestoneJournalEvent,
) -> Option<MilestoneJournalEvent> {
    if existing.event_type != MilestoneEventType::BeadStarted
        || requested.event_type != MilestoneEventType::BeadStarted
        || existing.timestamp != requested.timestamp
        || existing.bead_id != requested.bead_id
    {
        return None;
    }

    let existing_details: StartJournalDetails =
        serde_json::from_str(existing.details.as_deref()?).ok()?;
    let requested_details: StartJournalDetails =
        serde_json::from_str(requested.details.as_deref()?).ok()?;
    let merged_details = merge_start_journal_details(&existing_details, &requested_details)?;

    let mut repaired = existing.clone();
    repaired.details = render_journal_details(&merged_details);
    Some(repaired)
}

fn repairable_completion_event(
    existing: &MilestoneJournalEvent,
    requested: &MilestoneJournalEvent,
) -> Option<MilestoneJournalEvent> {
    if !is_completion_event(existing)
        || !is_completion_event(requested)
        || existing.event_type != requested.event_type
        || existing.bead_id != requested.bead_id
    {
        return None;
    }

    let existing_details: CompletionJournalDetails =
        serde_json::from_str(existing.details.as_deref()?).ok()?;
    let requested_details: CompletionJournalDetails =
        serde_json::from_str(requested.details.as_deref()?).ok()?;
    if existing_details.project_id != requested_details.project_id
        || existing_details.started_at != requested_details.started_at
        || option_conflicts(
            existing_details.run_id.as_deref(),
            requested_details.run_id.as_deref(),
        )
        || option_conflicts(
            existing_details.plan_hash.as_deref(),
            requested_details.plan_hash.as_deref(),
        )
    {
        return None;
    }

    let merged_details = merge_completion_journal_details(&existing_details, &requested_details)?;
    let mut repaired = existing.clone();
    repaired.details = render_journal_details(&merged_details);
    Some(repaired)
}

fn explicitly_repaired_completion_event(
    existing: &MilestoneJournalEvent,
    requested: &MilestoneJournalEvent,
) -> Option<MilestoneJournalEvent> {
    if !is_completion_event(existing)
        || !is_completion_event(requested)
        || existing.bead_id != requested.bead_id
    {
        return None;
    }

    let existing_details: CompletionJournalDetails =
        serde_json::from_str(existing.details.as_deref()?).ok()?;
    let requested_details: CompletionJournalDetails =
        serde_json::from_str(requested.details.as_deref()?).ok()?;
    if existing_details.project_id != requested_details.project_id
        || existing_details.started_at != requested_details.started_at
        || option_conflicts(
            existing_details.run_id.as_deref(),
            requested_details.run_id.as_deref(),
        )
        || option_conflicts(
            existing_details.plan_hash.as_deref(),
            requested_details.plan_hash.as_deref(),
        )
    {
        return None;
    }

    let mut repaired_details = requested_details.clone();
    if repaired_details.run_id.is_none() {
        repaired_details.run_id = existing_details.run_id.clone();
    }
    if repaired_details.plan_hash.is_none() {
        repaired_details.plan_hash = existing_details.plan_hash.clone();
    }
    if repaired_details.outcome == existing_details.outcome
        && repaired_details.outcome_detail.is_none()
    {
        repaired_details.outcome_detail = existing_details.outcome_detail.clone();
    }

    let mut repaired = requested.clone();
    repaired.details = render_journal_details(&repaired_details);
    Some(repaired)
}

fn apply_append_if_missing(
    journal: &mut Vec<MilestoneJournalEvent>,
    event: &MilestoneJournalEvent,
) -> bool {
    if journal.iter().any(|existing| existing == event) {
        return false;
    }

    if let Some((existing_index, repaired_event)) =
        journal.iter().enumerate().find_map(|(index, existing)| {
            repairable_start_event(existing, event)
                .or_else(|| repairable_completion_event(existing, event))
                .map(|repaired| (index, repaired))
        })
    {
        if journal[existing_index].details == repaired_event.details {
            return false;
        }

        journal[existing_index] = repaired_event;
        return true;
    }

    journal.push(event.clone());
    true
}

fn apply_repair_completion(
    journal: &mut Vec<MilestoneJournalEvent>,
    event: &MilestoneJournalEvent,
) -> bool {
    let exact_match_index = journal.iter().position(|existing| existing == event);

    if let Some((existing_index, repaired_event)) =
        journal.iter().enumerate().find_map(|(index, existing)| {
            if existing == event {
                return None;
            }
            explicitly_repaired_completion_event(existing, event).map(|repaired| (index, repaired))
        })
    {
        if exact_match_index.is_some() {
            journal.remove(existing_index);
        } else {
            journal[existing_index] = repaired_event;
        }
        return true;
    }

    if exact_match_index.is_some() {
        return false;
    }

    journal.push(event.clone());
    true
}

fn apply_journal_write_ops(
    journal: &mut Vec<MilestoneJournalEvent>,
    ops: &[JournalWriteOp],
) -> usize {
    let mut applied = 0;
    for op in ops {
        let changed = match op.kind {
            JournalWriteOpKind::AppendIfMissing => apply_append_if_missing(journal, &op.event),
            JournalWriteOpKind::RepairCompletion => apply_repair_completion(journal, &op.event),
        };
        if changed {
            applied += 1;
        }
    }
    applied
}

// ── Service use cases ───────────────────────────────────────────────────────

/// Input for creating a new milestone.
pub struct CreateMilestoneInput {
    pub id: String,
    pub name: String,
    pub description: String,
}

/// Create a new milestone with initial snapshot and journal event.
#[tracing::instrument(skip_all, fields(milestone_id = %input.id))]
pub fn create_milestone(
    store: &impl MilestoneStorePort,
    base_dir: &Path,
    input: CreateMilestoneInput,
    now: DateTime<Utc>,
) -> AppResult<MilestoneRecord> {
    let milestone_id = MilestoneId::new(&input.id)?;

    if store.milestone_exists(base_dir, &milestone_id)? {
        return Err(AppError::DuplicateProject {
            project_id: input.id,
        });
    }

    let record = MilestoneRecord::new(milestone_id, input.name, input.description, now);
    let snapshot = MilestoneSnapshot::initial(now);
    let event = MilestoneJournalEvent::new(MilestoneEventType::Created, now)
        .with_details("Milestone created");
    let journal_line = event.to_ndjson_line().map_err(AppError::SerdeJson)?;

    store.create_milestone_atomic(base_dir, &record, &snapshot, &journal_line)?;
    tracing::info!(
        operation = "create_milestone",
        outcome = "success",
        "created milestone"
    );
    Ok(record)
}

/// Materialize a milestone bundle into milestone record + plan files.
///
/// This is retry-safe for planning/ready milestones: an existing milestone with
/// the same ID can be re-used to finish a partial handoff or refresh the plan.
pub fn materialize_bundle(
    store: &impl MilestoneStorePort,
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    plan_store: &impl MilestonePlanPort,
    base_dir: &Path,
    bundle: &MilestoneBundle,
    now: DateTime<Utc>,
) -> AppResult<MilestoneRecord> {
    let milestone_id = MilestoneId::new(bundle.identity.id.clone())?;
    let expected_plan_json = render_plan_json(bundle).map_err(AppError::SerdeJson)?;
    let expected_plan_md = render_plan_md_checked(bundle)
        .map_err(|errors| snapshot_corrupt_record(&milestone_id, errors.join("; ")))?;
    let expected_plan_shape_signature = progress_shape_signature(bundle)
        .map_err(|errors| snapshot_corrupt_record(&milestone_id, errors.join("; ")))?;
    let expected_plan_hash = hash_text(&expected_plan_json);
    let expected_plan_shape =
        render_plan_shape_artifact(&expected_plan_hash, &expected_plan_shape_signature)?;

    let mut record = if store.milestone_exists(base_dir, &milestone_id)? {
        let existing_record = store.read_milestone_record(base_dir, &milestone_id)?;
        if existing_record.name != bundle.identity.name {
            return Err(AppError::CorruptRecord {
                file: format!("milestones/{}/milestone.toml", milestone_id),
                details: format!(
                    "existing milestone name '{}' does not match bundle identity '{}'",
                    existing_record.name, bundle.identity.name
                ),
            });
        }

        existing_record
    } else {
        create_milestone(
            store,
            base_dir,
            CreateMilestoneInput {
                id: milestone_id.to_string(),
                name: bundle.identity.name.clone(),
                description: bundle.executive_summary.clone(),
            },
            now,
        )?
    };

    snapshot_store.with_milestone_write_lock(base_dir, &milestone_id, || {
        let mut snapshot = snapshot_store.read_snapshot(base_dir, &milestone_id)?;
        if snapshot.status == MilestoneStatus::Running {
            return Err(AppError::InvalidConfigValue {
                key: "milestone_status".to_owned(),
                value: snapshot.status.to_string(),
                reason: format!(
                    "cannot materialize milestone bundle into milestone '{}'",
                    milestone_id
                ),
            });
        }

        let should_refresh_plan = snapshot.plan_hash.as_deref()
            != Some(expected_plan_hash.as_str())
            || snapshot_has_pending_lineage_reset(&snapshot)
            || plan_artifacts_need_refresh(
                plan_store,
                base_dir,
                &milestone_id,
                &expected_plan_json,
                &expected_plan_md,
                &expected_plan_shape,
            )?;
        if should_refresh_plan {
            persist_plan_locked(
                snapshot_store,
                journal_store,
                plan_store,
                base_dir,
                &milestone_id,
                bundle,
                &mut snapshot,
                now,
            )?;
        }

        reconcile_record_description(
            store,
            base_dir,
            &milestone_id,
            &mut record,
            &bundle.executive_summary,
        )?;

        if snapshot.status == MilestoneStatus::Planning {
            update_status_locked(
                snapshot_store,
                journal_store,
                base_dir,
                &milestone_id,
                &mut snapshot,
                MilestoneStatus::Ready,
                now,
            )?;
        }

        Ok(record)
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeadMaterializationReport {
    pub root_epic_id: String,
    pub workstream_epic_ids: Vec<String>,
    pub task_bead_ids: Vec<String>,
    pub created_beads: usize,
    pub reused_beads: usize,
}

#[derive(Debug, Clone)]
struct MaterializedBeadState {
    id: String,
    dependencies: HashSet<MaterializedDependency>,
    comments: HashSet<String>,
    created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MaterializedBeadRole {
    RootEpic,
    WorkstreamEpic,
    Task,
}

#[derive(Debug, Clone)]
struct MaterializeBeadInput {
    title: String,
    bead_type: String,
    priority: String,
    labels: Vec<String>,
    description: Option<String>,
    role: MaterializedBeadRole,
    proposal_label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MaterializedDependency {
    id: String,
    kind: DependencyKind,
}

#[derive(Debug, Clone)]
struct MaterializedProposal {
    actual_id: String,
    depends_on: Vec<String>,
    dependencies: HashSet<MaterializedDependency>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum BrListSummariesResponse {
    Envelope { issues: Vec<BeadSummary> },
    Many(Vec<BeadSummary>),
}

impl BrListSummariesResponse {
    fn into_issues(self) -> Vec<BeadSummary> {
        match self {
            Self::Envelope { issues } => issues,
            Self::Many(issues) => issues,
        }
    }
}

/// Materialize a milestone bundle into `br` beads without altering plan files.
pub async fn materialize_beads<R: ProcessRunner>(
    bundle: &MilestoneBundle,
    base_dir: &Path,
    br_mutation: &BrMutationAdapter<R>,
) -> AppResult<BeadMaterializationReport> {
    bundle
        .validate()
        .map_err(|errors| AppError::MilestoneOperationFailed {
            milestone_id: bundle.identity.id.clone(),
            action: "export beads".to_owned(),
            details: errors.join("; "),
        })?;
    ensure_bead_export_preflight(bundle, base_dir, br_mutation).await?;
    ensure_beads_mutation_health(
        base_dir,
        &MilestoneId::new(bundle.identity.id.clone())?,
        "export beads",
    )?;

    let milestone_scope_label = milestone_export_label(&bundle.identity.id);
    let existing_summaries = br_mutation
        .inner()
        .exec_json::<BrListSummariesResponse>(&BrCommand::list_all())
        .await
        .map(BrListSummariesResponse::into_issues)
        .map_err(|error| milestone_bead_export_error(bundle, "list existing beads", error))?;
    let mut existing_by_title = existing_summaries.into_iter().fold(
        HashMap::<String, Vec<BeadSummary>>::new(),
        |mut acc, summary| {
            if summary
                .labels
                .iter()
                .any(|label| label == &milestone_scope_label)
            {
                acc.entry(normalize_bead_match_text(&summary.title))
                    .or_default()
                    .push(summary);
            }
            acc
        },
    );
    let acceptance_lookup = bundle
        .acceptance_map
        .iter()
        .map(|criterion| (criterion.id.as_str(), criterion.description.as_str()))
        .collect::<HashMap<_, _>>();

    let mut proposal_id_map = HashMap::new();
    let mut proposals = Vec::new();
    let mut next_implicit_bead = 1usize;

    let root = materialize_bead_entry(
        bundle,
        br_mutation,
        &mut existing_by_title,
        MaterializeBeadInput {
            title: bundle.identity.name.clone(),
            bead_type: "epic".to_owned(),
            priority: "P1".to_owned(),
            labels: materialized_bead_labels(
                &[String::from("milestone-root")],
                &milestone_scope_label,
                None,
            ),
            description: None,
            role: MaterializedBeadRole::RootEpic,
            proposal_label: None,
        },
    )
    .await?;
    let mut created_beads = usize::from(root.created);
    let mut reused_beads = usize::from(!root.created);
    let mut workstream_epic_ids = Vec::new();
    let mut task_bead_ids = Vec::new();

    for workstream in &bundle.workstreams {
        let mut workstream_epic = materialize_bead_entry(
            bundle,
            br_mutation,
            &mut existing_by_title,
            MaterializeBeadInput {
                title: workstream.name.clone(),
                bead_type: "epic".to_owned(),
                priority: "P1".to_owned(),
                labels: materialized_bead_labels(
                    &Vec::<String>::new(),
                    &milestone_scope_label,
                    None,
                ),
                description: None,
                role: MaterializedBeadRole::WorkstreamEpic,
                proposal_label: None,
            },
        )
        .await?;
        created_beads += usize::from(workstream_epic.created);
        reused_beads += usize::from(!workstream_epic.created);
        workstream_epic_ids.push(workstream_epic.id.clone());

        ensure_dependency(
            bundle,
            br_mutation,
            &mut workstream_epic,
            root.id.as_str(),
            DependencyKind::ParentChild,
            "link workstream epic to milestone root",
        )
        .await?;

        if let Some(description) = workstream.description.as_deref() {
            ensure_bead_comment(
                bundle,
                br_mutation,
                &mut workstream_epic,
                description,
                &format!("comment workstream epic '{}'", workstream.name),
            )
            .await?;
        }

        for proposal in &workstream.beads {
            let canonical_proposal_id = canonical_export_proposal_id(
                bundle,
                proposal.bead_id.as_deref(),
                next_implicit_bead,
            )?;
            next_implicit_bead += 1;
            let bead_type = proposal.bead_type.as_deref().unwrap_or("task");
            let proposal_label = proposal_export_label(&canonical_proposal_id);
            let mut bead = materialize_bead_entry(
                bundle,
                br_mutation,
                &mut existing_by_title,
                MaterializeBeadInput {
                    title: proposal.title.clone(),
                    bead_type: bead_type.to_owned(),
                    priority: format!("P{}", proposal.priority.unwrap_or(2)),
                    labels: materialized_bead_labels(
                        &proposal.labels,
                        &milestone_scope_label,
                        Some(proposal_label.as_str()),
                    ),
                    description: proposal.description.clone(),
                    role: MaterializedBeadRole::Task,
                    proposal_label: Some(proposal_label),
                },
            )
            .await?;
            created_beads += usize::from(bead.created);
            reused_beads += usize::from(!bead.created);
            task_bead_ids.push(bead.id.clone());

            ensure_dependency(
                bundle,
                br_mutation,
                &mut bead,
                workstream_epic.id.as_str(),
                DependencyKind::ParentChild,
                &format!("link bead '{}' to workstream epic", proposal.title),
            )
            .await?;

            if let Some(comment) =
                render_bead_planning_comment(workstream.name.as_str(), proposal, &acceptance_lookup)
            {
                ensure_bead_comment(
                    bundle,
                    br_mutation,
                    &mut bead,
                    &comment,
                    &format!("comment planning rationale for '{}'", proposal.title),
                )
                .await?;
            }

            proposal_id_map.insert(canonical_proposal_id, bead.id.clone());
            proposals.push(MaterializedProposal {
                actual_id: bead.id.clone(),
                depends_on: proposal.depends_on.clone(),
                dependencies: bead.dependencies.clone(),
            });
        }
    }

    for proposal in proposals {
        let mut bead_state = MaterializedBeadState {
            id: proposal.actual_id.clone(),
            dependencies: proposal.dependencies,
            comments: HashSet::new(),
            created: false,
        };
        for dependency in proposal.depends_on {
            let normalized_dependency = normalize_bead_reference(&bundle.identity.id, &dependency)
                .map_err(|reason| AppError::MilestoneOperationFailed {
                    milestone_id: bundle.identity.id.clone(),
                    action: "resolve exported dependency".to_owned(),
                    details: format!(
                        "bead '{}' has invalid dependency reference '{}': {}",
                        bead_state.id, dependency, reason
                    ),
                })?;
            let actual_dependency_id = proposal_id_map
                .get(&normalized_dependency)
                .cloned()
                .ok_or_else(|| AppError::MilestoneOperationFailed {
                    milestone_id: bundle.identity.id.clone(),
                    action: "resolve exported dependency".to_owned(),
                    details: format!(
                        "bead '{}' depends on unknown proposal id '{}'",
                        bead_state.id, dependency
                    ),
                })?;
            let action = format!("link bead '{}' dependency", bead_state.id);
            ensure_dependency(
                bundle,
                br_mutation,
                &mut bead_state,
                actual_dependency_id.as_str(),
                DependencyKind::Blocks,
                &action,
            )
            .await?;
        }
    }

    match br_mutation.sync_own_dirty_if_beads_healthy(base_dir).await {
        Ok(_) => {}
        Err(SyncIfDirtyHealthError::UnsafeBeadsState { details }) => {
            return Err(AppError::MilestoneOperationFailed {
                milestone_id: bundle.identity.id.clone(),
                action: "sync exported beads".to_owned(),
                details,
            });
        }
        Err(SyncIfDirtyHealthError::Br(error)) => {
            return Err(milestone_bead_export_error(
                bundle,
                "sync exported beads",
                error,
            ));
        }
    }

    Ok(BeadMaterializationReport {
        root_epic_id: root.id,
        workstream_epic_ids,
        task_bead_ids,
        created_beads,
        reused_beads,
    })
}

async fn materialize_bead_entry<R: ProcessRunner>(
    bundle: &MilestoneBundle,
    br_mutation: &BrMutationAdapter<R>,
    existing_by_title: &mut HashMap<String, Vec<BeadSummary>>,
    input: MaterializeBeadInput,
) -> AppResult<MaterializedBeadState> {
    if let Some(existing) =
        find_existing_materialized_bead(bundle, br_mutation.inner(), existing_by_title, &input)
            .await?
    {
        return Ok(existing);
    }

    let output = br_mutation
        .create_bead(
            &input.title,
            &input.bead_type,
            &input.priority,
            &input.labels,
            input.description.as_deref(),
        )
        .await
        .map_err(|error| {
            milestone_bead_export_error(bundle, &format!("create bead '{}'", input.title), error)
        })?;
    let created_id =
        resolve_exported_bead_id(bundle, br_mutation.inner(), &input, &output.stdout).await?;
    Ok(MaterializedBeadState {
        id: created_id,
        dependencies: HashSet::new(),
        comments: HashSet::new(),
        created: true,
    })
}

async fn resolve_exported_bead_id<R: ProcessRunner>(
    bundle: &MilestoneBundle,
    br_read: &BrAdapter<R>,
    input: &MaterializeBeadInput,
    create_stdout: &str,
) -> AppResult<String> {
    for candidate in candidate_bead_ids_from_create_stdout(create_stdout) {
        if let Ok(detail) = br_read
            .exec_json::<BeadDetail>(&BrCommand::show(candidate.clone()))
            .await
        {
            if detail_matches_materialized_bead(&detail, input) {
                return Ok(detail.id);
            }
        }
    }

    let candidates = list_matching_beads_by_title(br_read, &input.title)
        .await
        .map_err(|error| milestone_bead_export_error(bundle, "query created bead id", error))?;
    let mut matched_ids = Vec::new();
    for candidate in candidates {
        let detail = br_read
            .exec_json::<BeadDetail>(&BrCommand::show(candidate.id.clone()))
            .await
            .map_err(|error| {
                milestone_bead_export_error(
                    bundle,
                    &format!("inspect created bead fallback candidate '{}'", candidate.id),
                    error,
                )
            })?;
        if detail_matches_materialized_bead(&detail, input) {
            matched_ids.push(detail.id);
        }
    }

    match matched_ids.len() {
        1 => Ok(matched_ids.remove(0)),
        0 => Err(AppError::MilestoneOperationFailed {
            milestone_id: bundle.identity.id.clone(),
            action: "resolve created bead id".to_owned(),
            details: format!(
                "br create succeeded but no bead id could be resolved for title '{}'",
                input.title
            ),
        }),
        _ => Err(AppError::MilestoneOperationFailed {
            milestone_id: bundle.identity.id.clone(),
            action: "resolve created bead id".to_owned(),
            details: format!(
                "br create for '{}' matched multiple milestone-scoped beads",
                input.title
            ),
        }),
    }
}

async fn ensure_bead_export_preflight<R: ProcessRunner>(
    bundle: &MilestoneBundle,
    base_dir: &Path,
    br_mutation: &BrMutationAdapter<R>,
) -> AppResult<()> {
    if br_mutation.working_dir().is_none() {
        return Ok(());
    }
    match br_mutation.sync_own_dirty_if_beads_healthy(base_dir).await {
        Ok(_) => Ok(()),
        Err(SyncIfDirtyHealthError::UnsafeBeadsState { details }) => {
            Err(AppError::MilestoneOperationFailed {
                milestone_id: bundle.identity.id.clone(),
                action: "prepare bead export".to_owned(),
                details,
            })
        }
        Err(SyncIfDirtyHealthError::Br(error)) => Err(milestone_bead_export_error(
            bundle,
            "prepare bead export",
            error,
        )),
    }
}

fn milestone_export_label(milestone_id: &str) -> String {
    format!("milestone:{milestone_id}")
}

fn proposal_export_label(proposal_id: &str) -> String {
    format!("proposal:{proposal_id}")
}

fn is_proposal_export_label(label: &str) -> bool {
    label.starts_with("proposal:")
}

fn proposal_label_matches(candidate_labels: &[String], proposal_label: Option<&String>) -> bool {
    let Some(proposal_label) = proposal_label else {
        return true;
    };
    if candidate_labels
        .iter()
        .any(|existing| existing == proposal_label)
    {
        return true;
    }

    !candidate_labels
        .iter()
        .any(|label| is_proposal_export_label(label))
}

fn materialized_bead_labels(
    labels: &[String],
    milestone_scope_label: &str,
    proposal_label: Option<&str>,
) -> Vec<String> {
    let mut merged = Vec::with_capacity(labels.len() + 1);
    merged.push(milestone_scope_label.to_owned());
    if let Some(proposal_label) = proposal_label {
        merged.push(proposal_label.to_owned());
    }
    for label in labels {
        if !merged.iter().any(|existing| existing == label) {
            merged.push(label.clone());
        }
    }
    merged
}

fn canonical_export_proposal_id(
    bundle: &MilestoneBundle,
    explicit_id: Option<&str>,
    implicit_index: usize,
) -> AppResult<String> {
    explicit_id
        .map(|raw| {
            normalize_bead_reference(&bundle.identity.id, raw).map_err(|reason| {
                AppError::MilestoneOperationFailed {
                    milestone_id: bundle.identity.id.clone(),
                    action: "resolve exported proposal id".to_owned(),
                    details: reason,
                }
            })
        })
        .transpose()?
        .map_or_else(|| Ok(format!("bead-{implicit_index}")), Ok)
}

fn summary_matches_materialized_bead(summary: &BeadSummary, input: &MaterializeBeadInput) -> bool {
    summary_matches_materialized_bead_except_role(summary, input)
        && proposal_label_matches(&summary.labels, input.proposal_label.as_ref())
        && materialized_role_matches(&summary.labels, &input.role)
}

fn summary_matches_materialized_bead_except_role(
    summary: &BeadSummary,
    input: &MaterializeBeadInput,
) -> bool {
    normalize_bead_match_text(&summary.title) == normalize_bead_match_text(&input.title)
        && bead_type_matches(summary.bead_type.clone(), &input.bead_type)
        && shared_materialized_labels(&input.labels)
            .all(|label| summary.labels.iter().any(|existing| existing == label))
}

fn detail_matches_materialized_bead(detail: &BeadDetail, input: &MaterializeBeadInput) -> bool {
    normalize_bead_match_text(&detail.title) == normalize_bead_match_text(&input.title)
        && bead_type_matches(detail.bead_type.clone(), &input.bead_type)
        && materialized_role_matches(&detail.labels, &input.role)
        && proposal_label_matches(&detail.labels, input.proposal_label.as_ref())
        && shared_materialized_labels(&input.labels)
            .all(|label| detail.labels.iter().any(|existing| existing == label))
}

fn bead_status_is_active(status: &BeadStatus) -> bool {
    matches!(status, BeadStatus::Open | BeadStatus::InProgress)
}

fn bead_type_matches(bead_type: BeadType, expected: &str) -> bool {
    bead_type.to_string() == expected
}

fn materialized_role_matches(labels: &[String], role: &MaterializedBeadRole) -> bool {
    let has_root_label = labels.iter().any(|label| label == "milestone-root");
    match role {
        MaterializedBeadRole::RootEpic => has_root_label,
        MaterializedBeadRole::WorkstreamEpic | MaterializedBeadRole::Task => !has_root_label,
    }
}

fn shared_materialized_labels(labels: &[String]) -> impl Iterator<Item = &String> {
    labels.iter().filter(|label| {
        let label = label.as_str();
        label != "milestone-root" && !is_proposal_export_label(label)
    })
}

async fn find_existing_materialized_bead<R: ProcessRunner>(
    bundle: &MilestoneBundle,
    br_read: &BrAdapter<R>,
    existing_by_title: &mut HashMap<String, Vec<BeadSummary>>,
    input: &MaterializeBeadInput,
) -> AppResult<Option<MaterializedBeadState>> {
    let normalized_title = normalize_bead_match_text(&input.title);
    let Some(matches) = existing_by_title.get_mut(&normalized_title) else {
        return Ok(None);
    };

    let eligible_indices = matches
        .iter()
        .enumerate()
        .filter_map(|(index, summary)| {
            summary_matches_materialized_bead(summary, input).then_some(index)
        })
        .collect::<Vec<_>>();
    let active_eligible_indices = eligible_indices
        .iter()
        .copied()
        .filter(|index| bead_status_is_active(&matches[*index].status))
        .collect::<Vec<_>>();
    if active_eligible_indices.len() > 1 {
        return Err(AppError::MilestoneOperationFailed {
            milestone_id: bundle.identity.id.clone(),
            action: "reuse exported bead".to_owned(),
            details: format!(
                "multiple active milestone-scoped beads already match title '{}' and type '{}'",
                input.title, input.bead_type
            ),
        });
    }
    let selected_index = if let Some(index) = active_eligible_indices.first().copied() {
        index
    } else if eligible_indices.len() == 1 {
        eligible_indices[0]
    } else if eligible_indices.len() > 1 {
        let conflict_ids = eligible_indices
            .iter()
            .map(|index| matches[*index].id.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(AppError::MilestoneOperationFailed {
            milestone_id: bundle.identity.id.clone(),
            action: "reuse exported bead".to_owned(),
            details: format!(
                "milestone-scoped bead title '{}' has multiple historical matches for type '{}' ({conflict_ids})",
                input.title, input.bead_type
            ),
        });
    } else {
        let active_conflict_ids = matches
            .iter()
            .filter(|summary| bead_status_is_active(&summary.status))
            .map(|summary| summary.id.as_str())
            .collect::<Vec<_>>();
        if active_conflict_ids.is_empty() {
            return Ok(None);
        }
        return Err(AppError::MilestoneOperationFailed {
            milestone_id: bundle.identity.id.clone(),
            action: "reuse exported bead".to_owned(),
            details: format!(
                "milestone-scoped bead title '{}' already exists with incompatible active matches ({})",
                input.title,
                active_conflict_ids.join(", ")
            ),
        });
    };
    let conflicting_matches = matches
        .iter()
        .enumerate()
        .filter_map(|(index, summary)| {
            (index != selected_index && bead_status_is_active(&summary.status)).then_some(summary)
        })
        .collect::<Vec<_>>();
    if !conflicting_matches.is_empty()
        && !conflicting_matches
            .iter()
            .all(|summary| summary_matches_materialized_bead_except_role(summary, input))
    {
        let conflict_ids = matches
            .iter()
            .enumerate()
            .filter_map(|(index, summary)| (index != selected_index).then_some(summary.id.as_str()))
            .collect::<Vec<_>>()
            .join(", ");
        return Err(AppError::MilestoneOperationFailed {
            milestone_id: bundle.identity.id.clone(),
            action: "reuse exported bead".to_owned(),
            details: format!(
                "milestone-scoped bead title '{}' is ambiguous because conflicting matches also exist ({conflict_ids})",
                input.title
            ),
        });
    }

    let existing = matches.swap_remove(selected_index);
    if matches.is_empty() {
        existing_by_title.remove(&normalized_title);
    }
    let detail = br_read
        .exec_json::<BeadDetail>(&BrCommand::show(existing.id.clone()))
        .await
        .map_err(|error| {
            milestone_bead_export_error(
                bundle,
                &format!("inspect existing bead '{}'", existing.id),
                error,
            )
        })?;
    if !detail_matches_materialized_bead(&detail, input) {
        return Err(AppError::MilestoneOperationFailed {
            milestone_id: bundle.identity.id.clone(),
            action: "reuse exported bead".to_owned(),
            details: format!(
                "milestone-scoped bead '{}' no longer matches title '{}' and type '{}'",
                detail.id, input.title, input.bead_type
            ),
        });
    }

    Ok(Some(MaterializedBeadState {
        id: detail.id,
        dependencies: detail
            .dependencies
            .into_iter()
            .map(|dependency| MaterializedDependency {
                id: dependency.id,
                kind: dependency.kind,
            })
            .collect(),
        comments: detail
            .comments
            .into_iter()
            .map(|comment| normalize_bead_match_text(&comment.text))
            .collect(),
        created: false,
    }))
}

async fn ensure_dependency<R: ProcessRunner>(
    bundle: &MilestoneBundle,
    br_mutation: &BrMutationAdapter<R>,
    from: &mut MaterializedBeadState,
    depends_on_id: &str,
    kind: DependencyKind,
    action: &str,
) -> AppResult<()> {
    let dependency = MaterializedDependency {
        id: depends_on_id.to_owned(),
        kind: kind.clone(),
    };
    if from.dependencies.contains(&dependency) {
        return Ok(());
    }

    br_mutation
        .add_dependency_with_kind(&from.id, depends_on_id, kind)
        .await
        .map_err(|error| milestone_bead_export_error(bundle, action, error))?;
    from.dependencies.insert(dependency);
    Ok(())
}

async fn ensure_bead_comment<R: ProcessRunner>(
    bundle: &MilestoneBundle,
    br_mutation: &BrMutationAdapter<R>,
    bead: &mut MaterializedBeadState,
    comment: &str,
    action: &str,
) -> AppResult<()> {
    let normalized_comment = normalize_bead_match_text(comment);
    if bead.comments.contains(&normalized_comment) {
        return Ok(());
    }

    br_mutation
        .comment_bead(&bead.id, comment)
        .await
        .map_err(|error| milestone_bead_export_error(bundle, action, error))?;
    bead.comments.insert(normalized_comment);
    Ok(())
}

fn render_bead_planning_comment(
    workstream_name: &str,
    proposal: &super::bundle::BeadProposal,
    acceptance_lookup: &HashMap<&str, &str>,
) -> Option<String> {
    let mut sections = vec![format!(
        "Planning rationale for workstream '{}'.",
        workstream_name
    )];

    if let Some(description) = proposal.description.as_deref() {
        sections.push(format!("Scope: {description}"));
    }

    let covered_criteria = proposal
        .acceptance_criteria
        .iter()
        .map(|criterion_id| {
            acceptance_lookup
                .get(criterion_id.as_str())
                .map(|description| format!("{criterion_id}: {description}"))
                .unwrap_or_else(|| criterion_id.clone())
        })
        .collect::<Vec<_>>();
    if !covered_criteria.is_empty() {
        sections.push(format!(
            "Acceptance coverage:\n- {}",
            covered_criteria.join("\n- ")
        ));
    }

    if sections.len() == 1 {
        return None;
    }

    Some(sections.join("\n\n"))
}

fn milestone_bead_export_error(
    bundle: &MilestoneBundle,
    action: &str,
    error: impl ToString,
) -> AppError {
    AppError::MilestoneOperationFailed {
        milestone_id: bundle.identity.id.clone(),
        action: action.to_owned(),
        details: error.to_string(),
    }
}

fn reconcile_record_description(
    store: &impl MilestoneStorePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    record: &mut MilestoneRecord,
    expected_description: &str,
) -> AppResult<()> {
    if record.description == expected_description {
        return Ok(());
    }

    let mut updated_record = record.clone();
    updated_record.description = expected_description.to_owned();
    store.write_milestone_record(base_dir, milestone_id, &updated_record)?;
    *record = updated_record;
    Ok(())
}

/// Load a milestone record by ID.
#[tracing::instrument(skip_all, level = "debug", fields(milestone_id = %milestone_id))]
pub fn load_milestone(
    store: &impl MilestoneStorePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneRecord> {
    let milestone = store.read_milestone_record(base_dir, milestone_id)?;
    tracing::debug!(
        operation = "load_milestone",
        outcome = "success",
        "loaded milestone record"
    );
    Ok(milestone)
}

/// Load a milestone's current status snapshot.
#[tracing::instrument(skip_all, level = "debug", fields(milestone_id = %milestone_id))]
pub fn load_snapshot(
    snapshot_store: &impl MilestoneSnapshotPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneSnapshot> {
    let snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
    tracing::debug!(
        operation = "load_snapshot",
        outcome = "success",
        "loaded milestone snapshot"
    );
    Ok(snapshot)
}

/// List all milestone IDs in the workspace.
#[tracing::instrument(skip_all, level = "debug")]
pub fn list_milestones(
    store: &impl MilestoneStorePort,
    base_dir: &Path,
) -> AppResult<Vec<MilestoneId>> {
    let milestones = store.list_milestone_ids(base_dir)?;
    tracing::debug!(
        operation = "list_milestones",
        outcome = "success",
        milestone_count = milestones.len(),
        "listed milestones"
    );
    Ok(milestones)
}

/// Update the milestone status and append a journal event.
#[tracing::instrument(skip_all, fields(milestone_id = %milestone_id))]
pub fn update_status(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    new_status: MilestoneStatus,
    now: DateTime<Utc>,
) -> AppResult<MilestoneSnapshot> {
    let snapshot = snapshot_store.with_milestone_write_lock(base_dir, milestone_id, || {
        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        let previous_status = snapshot.status;
        update_status_locked(
            snapshot_store,
            journal_store,
            base_dir,
            milestone_id,
            &mut snapshot,
            new_status,
            now,
        )?;
        tracing::info!(
            operation = "update_status",
            outcome = "success",
            from_status = %previous_status,
            to_status = %snapshot.status,
            "updated milestone status"
        );
        Ok(snapshot)
    })?;
    Ok(snapshot)
}

/// Persist a plan bundle: writes plan.json, plan.md, and updates the snapshot.
pub fn persist_plan(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    plan_store: &impl MilestonePlanPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bundle: &MilestoneBundle,
    now: DateTime<Utc>,
) -> AppResult<MilestoneSnapshot> {
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, move || {
        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        persist_plan_locked(
            snapshot_store,
            journal_store,
            plan_store,
            base_dir,
            milestone_id,
            bundle,
            &mut snapshot,
            now,
        )?;
        Ok(snapshot)
    })
}

fn update_status_locked(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    snapshot: &mut MilestoneSnapshot,
    new_status: MilestoneStatus,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let previous_snapshot = snapshot.clone();
    let existing_journal = journal_store.read_journal(base_dir, milestone_id)?;
    let next_snapshot = build_transition_snapshot(snapshot, milestone_id, new_status, now)?;
    let metadata = lifecycle_transition_metadata(
        &next_snapshot,
        snapshot.status,
        new_status,
        &existing_journal,
        previous_snapshot.updated_at,
        now,
    );
    let reason = match new_status {
        MilestoneStatus::Ready => "plan finalized and beads exported",
        MilestoneStatus::Running => {
            if previous_snapshot.status == MilestoneStatus::Paused {
                "execution resumed"
            } else {
                "execution started"
            }
        }
        MilestoneStatus::Paused => "execution paused",
        MilestoneStatus::Completed => "all beads closed",
        MilestoneStatus::Failed => "unrecoverable error requires operator intervention",
        MilestoneStatus::Planning => "planning resumed",
    };
    let transition = LifecycleTransitionCommit {
        snapshot: next_snapshot,
        event: MilestoneJournalEvent::lifecycle_transition(
            now,
            previous_snapshot.status,
            new_status,
            "system",
            reason,
            metadata,
        ),
    };
    write_snapshot_with_atomic_transition(
        snapshot_store,
        journal_store,
        base_dir,
        milestone_id,
        &previous_snapshot,
        transition,
    )?;
    *snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
    Ok(())
}

fn persist_plan_locked(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    plan_store: &impl MilestonePlanPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bundle: &MilestoneBundle,
    snapshot: &mut MilestoneSnapshot,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let plan_json = render_plan_json(bundle).map_err(AppError::SerdeJson)?;
    let plan_md = render_plan_md_checked(bundle)
        .map_err(|errors| snapshot_corrupt_record(milestone_id, errors.join("; ")))?;
    let plan_shape_signature = progress_shape_signature(bundle)
        .map_err(|errors| snapshot_corrupt_record(milestone_id, errors.join("; ")))?;
    let plan_hash = hash_text(&plan_json);
    let plan_hash_changed = snapshot.plan_hash.as_deref() != Some(plan_hash.as_str());
    let shape_matches = if plan_hash_changed {
        plan_shape_matches(
            plan_store,
            base_dir,
            milestone_id,
            snapshot.plan_hash.as_deref(),
            bundle,
            &plan_shape_signature,
        )?
    } else {
        true
    };
    let lineage_reset_required = !shape_matches || snapshot_has_pending_lineage_reset(snapshot);
    let next_plan_version = if plan_hash_changed {
        snapshot.plan_version.saturating_add(1)
    } else {
        snapshot.plan_version
    };
    let pending_lineage_reset = lineage_reset_required.then(|| PendingLineageReset {
        plan_hash: plan_hash.clone(),
        plan_version: next_plan_version,
    });
    let plan_shape = render_plan_shape_artifact_with_lineage_reset(
        &plan_hash,
        &plan_shape_signature,
        pending_lineage_reset.as_ref(),
    )?;

    let mut progress = reconcile_progress_for_new_plan(shape_matches, bundle, snapshot);
    progress.total_beads = bundle.bead_count() as u32;
    let reopen_terminal_status = match snapshot.status {
        MilestoneStatus::Completed => {
            (plan_hash_changed || lineage_reset_required)
                && (progress.completed_beads + progress.skipped_beads < progress.total_beads
                    || progress.failed_beads > 0)
        }
        MilestoneStatus::Failed => {
            (plan_hash_changed || lineage_reset_required) && progress.failed_beads == 0
        }
        _ => false,
    };

    plan_store.write_plan_json(base_dir, milestone_id, &plan_json)?;
    plan_store.write_plan_md(base_dir, milestone_id, &plan_md)?;
    plan_store.write_plan_shape(base_dir, milestone_id, &plan_shape)?;

    let previous_snapshot = snapshot.clone();
    let pending_lineage_reset_before = pending_lineage_reset_for_snapshot(snapshot);

    if plan_hash_changed {
        snapshot.plan_hash = Some(plan_hash.clone());
        snapshot.plan_version = next_plan_version;
        snapshot.progress = progress;
        snapshot.updated_at = now;
    }
    snapshot.pending_lineage_reset = pending_lineage_reset.clone();
    if reopen_terminal_status {
        snapshot.status = MilestoneStatus::Planning;
        snapshot.active_bead = None;
    }

    let needs_ready_transition = snapshot.status == MilestoneStatus::Planning;
    let mut existing_journal = if reopen_terminal_status || needs_ready_transition {
        Some(journal_store.read_journal(base_dir, milestone_id)?)
    } else {
        None
    };
    let mut journal_ops = Vec::new();
    if reopen_terminal_status {
        let reopen_event = MilestoneJournalEvent::lifecycle_transition(
            now,
            previous_snapshot.status,
            MilestoneStatus::Planning,
            "system",
            "plan changed, milestone reopened",
            lifecycle_transition_metadata(
                snapshot,
                previous_snapshot.status,
                MilestoneStatus::Planning,
                existing_journal
                    .as_deref()
                    .expect("lifecycle journal should be loaded for reopen events"),
                previous_snapshot.updated_at,
                now,
            ),
        );
        journal_ops.push(JournalWriteOp::append_if_missing(reopen_event.clone()));
        if let Some(journal) = existing_journal.as_mut() {
            journal.push(reopen_event);
        }
    }

    if plan_hash_changed {
        let event_type = if snapshot.plan_version == 1 {
            MilestoneEventType::PlanDrafted
        } else {
            MilestoneEventType::PlanUpdated
        };
        journal_ops.push(JournalWriteOp::append_if_missing(
            MilestoneJournalEvent::new(event_type, now).with_details(format!(
                "Plan v{} with {} beads",
                snapshot.plan_version,
                bundle.bead_count()
            )),
        ));
    }

    if needs_ready_transition {
        let planning_snapshot = snapshot.clone();
        let ready_snapshot = build_transition_snapshot(
            &planning_snapshot,
            milestone_id,
            MilestoneStatus::Ready,
            now,
        )?;
        let ready_event = MilestoneJournalEvent::lifecycle_transition(
            now,
            MilestoneStatus::Planning,
            MilestoneStatus::Ready,
            "system",
            "plan finalized and beads exported",
            lifecycle_transition_metadata(
                &ready_snapshot,
                MilestoneStatus::Planning,
                MilestoneStatus::Ready,
                existing_journal
                    .as_deref()
                    .expect("lifecycle journal should be loaded for ready transitions"),
                planning_snapshot.updated_at,
                now,
            ),
        );
        journal_ops.push(JournalWriteOp::append_if_missing(ready_event.clone()));
        if let Some(journal) = existing_journal.as_mut() {
            journal.push(ready_event);
        }
        *snapshot = ready_snapshot;
    }

    if plan_hash_changed {
        validate_snapshot(snapshot, milestone_id)?;
        commit_snapshot_and_journal_ops(
            snapshot_store,
            journal_store,
            base_dir,
            milestone_id,
            &previous_snapshot,
            snapshot,
            &journal_ops,
            "milestone journal commit failed after plan update snapshot write",
        )?;
    } else if pending_lineage_reset_before != pending_lineage_reset || !journal_ops.is_empty() {
        validate_snapshot(snapshot, milestone_id)?;
        if journal_ops.is_empty() {
            snapshot_store.write_snapshot(base_dir, milestone_id, snapshot)?;
        } else {
            commit_snapshot_and_journal_ops(
                snapshot_store,
                journal_store,
                base_dir,
                milestone_id,
                &previous_snapshot,
                snapshot,
                &journal_ops,
                "milestone journal commit failed after milestone reopen snapshot write",
            )?;
        }
    }

    if lineage_reset_required {
        clear_pending_lineage_reset_locked(snapshot_store, base_dir, milestone_id, snapshot)?;
    }
    Ok(())
}

fn reconcile_progress_for_new_plan(
    shape_matches: bool,
    bundle: &MilestoneBundle,
    snapshot: &MilestoneSnapshot,
) -> MilestoneProgress {
    let mut progress = if shape_matches {
        snapshot.progress.clone()
    } else {
        MilestoneProgress::default()
    };

    let completed_or_terminal = progress
        .completed_beads
        .saturating_add(progress.failed_beads)
        .saturating_add(progress.skipped_beads)
        .saturating_add(progress.in_progress_beads);
    let total_beads = bundle.bead_count() as u32;
    if completed_or_terminal > total_beads {
        progress = MilestoneProgress::default();
    }

    progress
}

fn plan_shape_matches(
    plan_store: &impl MilestonePlanPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    committed_plan_hash: Option<&str>,
    current_bundle: &MilestoneBundle,
    current_shape: &str,
) -> AppResult<bool> {
    let Some(committed_plan_hash) = committed_plan_hash else {
        return Ok(false);
    };

    let explicit_id_hints = explicit_id_hints(current_bundle)
        .map_err(|errors| snapshot_corrupt_record(milestone_id, errors.join("; ")))?;

    if let Ok(existing_plan_json) = plan_store.read_plan_json(base_dir, milestone_id) {
        let existing_plan_hash = hash_text(&existing_plan_json);
        if existing_plan_hash == committed_plan_hash {
            let Ok(existing_bundle) = serde_json::from_str::<MilestoneBundle>(&existing_plan_json)
            else {
                return Ok(false);
            };
            let Ok(existing_shape_signature) = progress_shape_signature_with_explicit_id_hints(
                &existing_bundle,
                Some(&explicit_id_hints),
            ) else {
                return Ok(false);
            };
            return Ok(existing_shape_signature == current_shape);
        }
    }

    let Ok(stored_plan_shape) = plan_store.read_plan_shape(base_dir, milestone_id) else {
        return Ok(false);
    };
    let Ok(stored_plan_shape) = serde_json::from_str::<StoredPlanShape>(&stored_plan_shape) else {
        return Ok(false);
    };
    if stored_plan_shape.plan_hash != committed_plan_hash {
        return Ok(false);
    }

    Ok(stored_plan_shape.shape_signature == current_shape)
}

fn plan_artifacts_need_refresh(
    plan_store: &impl MilestonePlanPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    expected_plan_json: &str,
    expected_plan_md: &str,
    expected_plan_shape: &str,
) -> AppResult<bool> {
    match plan_store.read_plan_json(base_dir, milestone_id) {
        Ok(existing_plan_json) if existing_plan_json == expected_plan_json => {}
        Ok(_) | Err(_) => return Ok(true),
    }

    match plan_store.read_plan_md(base_dir, milestone_id) {
        Ok(existing_plan_md) if existing_plan_md == expected_plan_md => {}
        Ok(_) | Err(_) => return Ok(true),
    }

    match plan_store.read_plan_shape(base_dir, milestone_id) {
        Ok(existing_plan_shape) if existing_plan_shape == expected_plan_shape => Ok(false),
        Ok(_) | Err(_) => Ok(true),
    }
}

fn clear_task_run_lineage(base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<()> {
    let task_runs_path =
        FileSystem::milestone_root(base_dir, milestone_id).join("task-runs.ndjson");
    if !task_runs_path.exists() {
        return Ok(());
    }

    FileSystem::write_atomic(&task_runs_path, "")
}

/// Record the start of a bead task run.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    skip_all,
    fields(
        milestone_id = %milestone_id,
        bead_id = %bead_id,
        task_id = %project_id
    )
)]
pub fn record_bead_start(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: &str,
    plan_hash: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, || {
        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        clear_pending_lineage_reset_locked(snapshot_store, base_dir, milestone_id, &mut snapshot)?;
        // Capture the on-disk snapshot before lineage reconciliation so rollback
        // restores the exact durable state. Any transient reconcile-only
        // correction (for example running -> paused with no active bead) is not
        // itself journaled unless the start mutation commits successfully.
        let previous_snapshot = snapshot.clone();
        let previous_status = previous_snapshot.status;
        reconcile_snapshot_from_lineage(
            &mut snapshot,
            milestone_id,
            lineage_store.read_task_runs(base_dir, milestone_id)?,
        )?;
        if snapshot.status.is_terminal() {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "cannot start bead '{bead_id}': milestone '{milestone_id}' is already {}",
                    snapshot.status,
                ),
            });
        }
        validate_transition_prerequisites(&snapshot, milestone_id, MilestoneStatus::Running)?;
        let existing_journal = journal_store.read_journal(base_dir, milestone_id)?;

        let started_entry = lineage_store.record_task_run_start(
            base_dir,
            milestone_id,
            bead_id,
            project_id,
            run_id,
            plan_hash,
            now,
        )?;

        reconcile_snapshot_from_lineage(
            &mut snapshot,
            milestone_id,
            lineage_store.read_task_runs(base_dir, milestone_id)?,
        )?;
        snapshot.updated_at = snapshot.updated_at.max(now).max(started_entry.started_at);
        validate_snapshot(&snapshot, milestone_id)?;
        let bead_event =
            MilestoneJournalEvent::new(MilestoneEventType::BeadStarted, started_entry.started_at)
                .with_bead(bead_id)
                .with_details(started_entry.start_journal_details());
        let mut journal_ops: Vec<_> = build_reconciled_transition_events(
            milestone_id,
            &previous_snapshot,
            &snapshot,
            &existing_journal,
            now,
            Some(started_entry.started_at),
            "controller",
            if previous_status == MilestoneStatus::Paused {
                "execution resumed"
            } else {
                "execution started"
            },
        )?
        .into_iter()
        .map(JournalWriteOp::append_if_missing)
        .collect();
        journal_ops.push(JournalWriteOp::append_if_missing(bead_event));

        commit_snapshot_and_journal_ops(
            snapshot_store,
            journal_store,
            base_dir,
            milestone_id,
            &previous_snapshot,
            &snapshot,
            &journal_ops,
            "milestone journal commit failed after bead start snapshot write",
        )?;

        tracing::info!(
            operation = "record_bead_start",
            outcome = "success",
            run_id = run_id,
            "recorded bead start"
        );
        Ok(())
    })
}

/// Record the completion of a bead task run.
///
/// This finalizes the existing lineage row created by [`record_bead_start`] and
/// updates snapshot + journal state in the same flow.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    skip_all,
    fields(
        milestone_id = %milestone_id,
        bead_id = %bead_id,
        task_id = %project_id
    )
)]
pub fn record_bead_completion(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: &str,
    plan_hash: Option<&str>,
    outcome: TaskRunOutcome,
    outcome_detail: Option<&str>,
    started_at: DateTime<Utc>,
    now: DateTime<Utc>,
) -> AppResult<()> {
    record_bead_completion_with_disposition(
        snapshot_store,
        journal_store,
        lineage_store,
        base_dir,
        milestone_id,
        bead_id,
        project_id,
        run_id,
        plan_hash,
        outcome,
        outcome_detail,
        started_at,
        now,
        CompletionMilestoneDisposition::ReconcileFromLineage,
    )?;
    tracing::info!(
        operation = "record_bead_completion",
        outcome = "success",
        run_id = run_id,
        "recorded bead completion"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    skip_all,
    fields(
        milestone_id = %milestone_id,
        bead_id = %bead_id,
        task_id = %project_id
    )
)]
pub fn record_bead_completion_with_disposition(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: &str,
    plan_hash: Option<&str>,
    outcome: TaskRunOutcome,
    outcome_detail: Option<&str>,
    started_at: DateTime<Utc>,
    now: DateTime<Utc>,
    disposition: CompletionMilestoneDisposition,
) -> AppResult<()> {
    update_task_run_with_disposition(
        snapshot_store,
        journal_store,
        lineage_store,
        base_dir,
        milestone_id,
        bead_id,
        project_id,
        run_id,
        plan_hash,
        started_at,
        outcome,
        outcome_detail.map(str::to_owned),
        now,
        disposition,
    )?;
    tracing::info!(
        operation = "record_bead_completion_with_disposition",
        outcome = "success",
        run_id = run_id,
        "recorded bead completion with disposition"
    );
    Ok(())
}

/// Read the journal for a milestone.
#[tracing::instrument(skip_all, level = "debug", fields(milestone_id = %milestone_id))]
pub fn read_journal(
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<Vec<MilestoneJournalEvent>> {
    let journal = journal_store.read_journal(base_dir, milestone_id)?;
    tracing::debug!(
        operation = "read_journal",
        outcome = "success",
        event_count = journal.len(),
        "read milestone journal"
    );
    Ok(journal)
}

pub(crate) fn load_plan_bundle(
    plan_store: &(impl MilestonePlanPort + ?Sized),
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<(MilestoneBundle, String)> {
    let plan_json = plan_store.read_plan_json(base_dir, milestone_id)?;
    let plan_hash = hash_text(&plan_json);
    let mut bundle: MilestoneBundle =
        serde_json::from_str(&plan_json).map_err(|error| AppError::CorruptRecord {
            file: format!("milestones/{}/plan.json", milestone_id),
            details: error.to_string(),
        })?;
    if bundle.identity.id != milestone_id.as_str() {
        return Err(AppError::CorruptRecord {
            file: format!("milestones/{}/plan.json", milestone_id),
            details: format!(
                "bundle identity '{}' does not match milestone '{}'",
                bundle.identity.id, milestone_id
            ),
        });
    }
    backfill_legacy_explicit_bead_flags(&mut bundle, milestone_id);
    bundle
        .validate()
        .map_err(|errors| AppError::CorruptRecord {
            file: format!("milestones/{}/plan.json", milestone_id),
            details: errors.join("; "),
        })?;
    Ok((bundle, plan_hash))
}

fn load_plan_bundle_for_lineage(
    plan_store: &(impl MilestonePlanPort + ?Sized),
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<Option<(MilestoneBundle, String)>> {
    match load_plan_bundle(plan_store, base_dir, milestone_id) {
        Ok(bundle) => Ok(Some(bundle)),
        Err(error) if should_fallback_for_lineage_plan_error(&error, milestone_id) => Ok(None),
        Err(error) => Err(error),
    }
}

fn should_fallback_for_lineage_plan_error(error: &AppError, milestone_id: &MilestoneId) -> bool {
    match error {
        AppError::Io(io_error) => io_error.kind() == std::io::ErrorKind::NotFound,
        AppError::CorruptRecord { file, .. } => {
            file.ends_with(&format!("milestones/{}/plan.json", milestone_id))
        }
        _ => false,
    }
}

fn backfill_legacy_explicit_bead_flags(bundle: &mut MilestoneBundle, milestone_id: &MilestoneId) {
    let mut next_implicit_bead = 1usize;

    for workstream in &mut bundle.workstreams {
        for proposal in &mut workstream.beads {
            let implicit_bead_id = format!("{}.bead-{}", milestone_id.as_str(), next_implicit_bead);
            next_implicit_bead += 1;

            if proposal.explicit_id.is_some() {
                continue;
            }

            if let Some(candidate) = proposal.bead_id.as_deref() {
                proposal.explicit_id = Some(!bead_matches_implicit_slot(
                    candidate,
                    milestone_id.as_str(),
                    &implicit_bead_id,
                ));
            }
        }
    }
}

fn find_bead_plan_details(
    bundle: &MilestoneBundle,
    bead_id: &str,
) -> Option<(String, Vec<String>)> {
    let milestone_prefix = format!("{}.", bundle.identity.id);

    for workstream in &bundle.workstreams {
        for bead in &workstream.beads {
            let Some(planned_bead_id) = bead.bead_id.as_deref() else {
                continue;
            };
            let short_id = planned_bead_id
                .strip_prefix(milestone_prefix.as_str())
                .unwrap_or(planned_bead_id);
            if planned_bead_id != bead_id && short_id != bead_id {
                continue;
            }

            let acceptance_criteria = bead
                .acceptance_criteria
                .iter()
                .map(|criterion_id| {
                    bundle
                        .acceptance_map
                        .iter()
                        .find(|criterion| criterion.id == *criterion_id)
                        .map(|criterion| criterion.description.clone())
                        .unwrap_or_else(|| criterion_id.clone())
                })
                .collect();
            return Some((bead.title.clone(), acceptance_criteria));
        }
    }

    None
}

fn resolve_bead_plan_details(
    bundle: &MilestoneBundle,
    bead_id: &str,
) -> AppResult<(String, Vec<String>)> {
    find_bead_plan_details(bundle, bead_id).ok_or_else(|| AppError::CorruptRecord {
        file: format!("milestones/{}/plan.json", bundle.identity.id),
        details: format!("bead '{bead_id}' was not found in the validated milestone plan"),
    })
}

fn build_bead_lineage_view(
    milestone: &MilestoneRecord,
    bundle: &MilestoneBundle,
    bead_id: &str,
) -> AppResult<BeadLineageView> {
    let (bead_title, acceptance_criteria) = resolve_bead_plan_details(bundle, bead_id)?;
    Ok(BeadLineageView {
        milestone_id: milestone.id.to_string(),
        milestone_name: milestone.name.clone(),
        bead_id: bead_id.to_owned(),
        bead_title: Some(bead_title),
        acceptance_criteria,
    })
}

fn build_fallback_bead_lineage_view(milestone: &MilestoneRecord, bead_id: &str) -> BeadLineageView {
    BeadLineageView {
        milestone_id: milestone.id.to_string(),
        milestone_name: milestone.name.clone(),
        bead_id: bead_id.to_owned(),
        bead_title: None,
        acceptance_criteria: Vec::new(),
    }
}

fn build_bead_lineage_from_current_plan(
    milestone: &MilestoneRecord,
    current_plan: Option<(&MilestoneBundle, &str)>,
    bead_id: &str,
    expected_plan_hash: Option<&str>,
) -> AppResult<BeadLineageView> {
    let Some(expected_plan_hash) = expected_plan_hash else {
        return Ok(build_fallback_bead_lineage_view(milestone, bead_id));
    };
    let Some((bundle, current_plan_hash)) = current_plan else {
        return Ok(build_fallback_bead_lineage_view(milestone, bead_id));
    };
    if expected_plan_hash != current_plan_hash {
        return Ok(build_fallback_bead_lineage_view(milestone, bead_id));
    }

    build_bead_lineage_view(milestone, bundle, bead_id)
}

fn shared_plan_hash_for_runs(runs: &[TaskRunEntry]) -> Result<Option<&str>, ()> {
    if runs.is_empty() {
        return Ok(None);
    }

    let mut shared_plan_hash = None;
    for entry in runs {
        let Some(plan_hash) = entry.plan_hash.as_deref() else {
            return Err(());
        };
        match shared_plan_hash {
            Some(existing) if existing != plan_hash => return Err(()),
            Some(_) => {}
            None => shared_plan_hash = Some(plan_hash),
        }
    }
    Ok(shared_plan_hash)
}

/// Read milestone/bead lineage metadata, only projecting plan details when the
/// current plan matches the persisted plan provenance for the task/run.
#[tracing::instrument(
    skip_all,
    level = "debug",
    fields(milestone_id = %milestone_id, bead_id = %bead_id)
)]
pub fn read_bead_lineage(
    store: &(impl MilestoneStorePort + ?Sized),
    plan_store: &(impl MilestonePlanPort + ?Sized),
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    expected_plan_hash: Option<&str>,
) -> AppResult<BeadLineageView> {
    if !store.milestone_exists(base_dir, milestone_id)? {
        return Err(AppError::MilestoneNotFound {
            milestone_id: milestone_id.to_string(),
        });
    }

    let milestone = store.read_milestone_record(base_dir, milestone_id)?;
    let current_plan = load_plan_bundle_for_lineage(plan_store, base_dir, milestone_id)?;
    let current_plan = current_plan
        .as_ref()
        .map(|(bundle, plan_hash)| (bundle, plan_hash.as_str()));

    let lineage = build_bead_lineage_from_current_plan(
        &milestone,
        current_plan,
        bead_id,
        expected_plan_hash,
    )?;
    tracing::debug!(
        operation = "read_bead_lineage",
        outcome = "success",
        "read bead lineage"
    );
    Ok(lineage)
}

/// Read the task-run lineage for a milestone.
pub fn read_task_runs(
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<Vec<TaskRunEntry>> {
    lineage_store.read_task_runs(base_dir, milestone_id)
}

/// Find all task runs for a specific bead in chronological order.
pub fn find_runs_for_bead(
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> AppResult<Vec<TaskRunEntry>> {
    lineage_store.find_runs_for_bead(base_dir, milestone_id, bead_id)
}

/// Read all execution attempts for a bead, including retries and durations.
pub fn bead_execution_history(
    store: &impl MilestoneStorePort,
    plan_store: &impl MilestonePlanPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
) -> AppResult<BeadExecutionHistoryView> {
    let runs = find_runs_for_bead(lineage_store, base_dir, milestone_id, bead_id)?;
    let lineage = if runs.is_empty() {
        let milestone = store.read_milestone_record(base_dir, milestone_id)?;
        match load_plan_bundle_for_lineage(plan_store, base_dir, milestone_id)? {
            Some((bundle, _)) => build_bead_lineage_view(&milestone, &bundle, bead_id)?,
            None => build_fallback_bead_lineage_view(&milestone, bead_id),
        }
    } else {
        match shared_plan_hash_for_runs(&runs) {
            Ok(expected_plan_hash) => read_bead_lineage(
                store,
                plan_store,
                base_dir,
                milestone_id,
                bead_id,
                expected_plan_hash,
            )?,
            Err(()) => {
                let milestone = store.read_milestone_record(base_dir, milestone_id)?;
                build_fallback_bead_lineage_view(&milestone, bead_id)
            }
        }
    };
    let runs = runs
        .into_iter()
        .map(|entry| {
            let TaskRunEntry {
                milestone_id,
                bead_id,
                project_id,
                run_id,
                plan_hash,
                outcome,
                outcome_detail,
                started_at,
                finished_at,
                task_id,
            } = entry;
            let duration_ms = finished_at
                .as_ref()
                .and_then(|finished_at| finished_at.signed_duration_since(started_at).to_std().ok())
                .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64);
            TaskRunAttemptView {
                milestone_id,
                bead_id,
                project_id,
                run_id,
                plan_hash,
                outcome,
                outcome_detail,
                started_at,
                finished_at,
                duration_ms,
                task_id,
            }
        })
        .collect();

    Ok(BeadExecutionHistoryView { lineage, runs })
}

/// List Ralph tasks linked to a milestone.
#[tracing::instrument(skip_all, level = "debug", fields(milestone_id = %milestone_id))]
pub fn list_tasks_for_milestone(
    store: &impl MilestoneStorePort,
    plan_store: &impl MilestonePlanPort,
    project_store: &impl ProjectStorePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneTaskListView> {
    let milestone = store.read_milestone_record(base_dir, milestone_id)?;
    let current_plan = load_plan_bundle_for_lineage(plan_store, base_dir, milestone_id)?;

    let mut tasks = Vec::new();
    for project_id in project_store.list_project_ids(base_dir)? {
        let record = project_store.read_project_record(base_dir, &project_id)?;
        let Some(task_source) = record.task_source.as_ref() else {
            continue;
        };
        if task_source.milestone_id != milestone_id.as_str() {
            continue;
        }

        tasks.push(MilestoneTaskView {
            project_id: record.id.to_string(),
            project_name: record.name,
            flow: record.flow,
            status_summary: record.status_summary,
            created_at: record.created_at,
            bead_id: task_source.bead_id.clone(),
            bead_title: if task_source.plan_hash.is_some() {
                match (
                    task_source.plan_hash.as_deref(),
                    current_plan
                        .as_ref()
                        .map(|(bundle, plan_hash)| (bundle, plan_hash.as_str())),
                ) {
                    (Some(expected_plan_hash), Some((bundle, current_plan_hash)))
                        if expected_plan_hash == current_plan_hash =>
                    {
                        find_bead_plan_details(bundle, &task_source.bead_id)
                            .map(|(bead_title, _)| bead_title)
                    }
                    _ => None,
                }
            } else {
                None
            },
        });
    }
    tasks.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.project_id.cmp(&right.project_id))
    });

    let view = MilestoneTaskListView {
        milestone_id: milestone.id.to_string(),
        milestone_name: milestone.name,
        tasks,
    };
    tracing::debug!(
        operation = "list_tasks_for_milestone",
        outcome = "success",
        task_count = view.tasks.len(),
        "listed milestone tasks"
    );
    Ok(view)
}

/// Update an existing task run's outcome after completion.
///
/// The lineage row remains the durable source of truth. Snapshot counters and
/// journal events are repaired from canonical lineage state, so replaying the
/// same terminal completion can finish a partially failed write without
/// duplicating counters or events.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    skip_all,
    fields(
        milestone_id = %milestone_id,
        bead_id = %bead_id,
        task_id = %project_id
    )
)]
pub fn update_task_run(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: &str,
    plan_hash: Option<&str>,
    started_at: DateTime<Utc>,
    outcome: TaskRunOutcome,
    outcome_detail: Option<String>,
    finished_at: DateTime<Utc>,
) -> AppResult<()> {
    update_task_run_with_disposition(
        snapshot_store,
        journal_store,
        lineage_store,
        base_dir,
        milestone_id,
        bead_id,
        project_id,
        run_id,
        plan_hash,
        started_at,
        outcome,
        outcome_detail,
        finished_at,
        CompletionMilestoneDisposition::ReconcileFromLineage,
    )?;
    tracing::info!(
        operation = "update_task_run",
        outcome = "success",
        run_id = run_id,
        "updated task run"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn update_task_run_with_disposition(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: &str,
    plan_hash: Option<&str>,
    started_at: DateTime<Utc>,
    outcome: TaskRunOutcome,
    outcome_detail: Option<String>,
    finished_at: DateTime<Utc>,
    disposition: CompletionMilestoneDisposition,
) -> AppResult<()> {
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, move || {
        if !outcome.is_terminal() {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "cannot finalize bead '{bead_id}' with non-terminal outcome '{outcome}'"
                ),
            });
        }

        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        clear_pending_lineage_reset_locked(snapshot_store, base_dir, milestone_id, &mut snapshot)?;
        let previous_snapshot = snapshot.clone();
        let existing_journal = journal_store.read_journal(base_dir, milestone_id)?;
        let finalized_run = lineage_store.update_task_run(
            base_dir,
            milestone_id,
            bead_id,
            project_id,
            run_id,
            plan_hash,
            started_at,
            outcome,
            outcome_detail,
            finished_at,
        )?;

        reconcile_snapshot_from_lineage(
            &mut snapshot,
            milestone_id,
            lineage_store.read_task_runs(base_dir, milestone_id)?,
        )?;
        apply_completion_milestone_disposition(&mut snapshot, disposition);
        let event_timestamp = finalized_run.finished_at.unwrap_or(finished_at);
        snapshot.updated_at = snapshot.updated_at.max(finished_at).max(event_timestamp);
        validate_snapshot(&snapshot, milestone_id)?;
        let bead_event = MilestoneJournalEvent::new(
            event_type_for_outcome(finalized_run.outcome),
            event_timestamp,
        )
        .with_bead(&finalized_run.bead_id)
        .with_details(finalized_run.completion_journal_details());
        let mut journal_ops: Vec<_> = build_reconciled_transition_events(
            milestone_id,
            &previous_snapshot,
            &snapshot,
            &existing_journal,
            event_timestamp,
            Some(finalized_run.started_at),
            "controller",
            completion_status_reason(snapshot.status),
        )?
        .into_iter()
        .map(JournalWriteOp::append_if_missing)
        .collect();
        journal_ops.push(JournalWriteOp::append_if_missing(bead_event));

        commit_snapshot_and_journal_ops(
            snapshot_store,
            journal_store,
            base_dir,
            milestone_id,
            &previous_snapshot,
            &snapshot,
            &journal_ops,
            "milestone journal commit failed after bead completion snapshot write",
        )?;

        Ok(())
    })
}

/// Explicitly repair an already-terminal task run for an exact attempt.
///
/// This narrow path is reserved for milestone reconciliation when the project
/// journal proves the canonical terminal outcome and earlier sync logic wrote a
/// stale terminal row. Generic duplicate finalization continues to be rejected
/// by [`update_task_run`].
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(
    skip_all,
    fields(
        milestone_id = %milestone_id,
        bead_id = %bead_id,
        task_id = %project_id
    )
)]
pub fn repair_task_run(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: &str,
    plan_hash: Option<&str>,
    started_at: DateTime<Utc>,
    outcome: TaskRunOutcome,
    outcome_detail: Option<String>,
    finished_at: DateTime<Utc>,
) -> AppResult<()> {
    repair_task_run_with_disposition(
        snapshot_store,
        journal_store,
        lineage_store,
        base_dir,
        milestone_id,
        bead_id,
        project_id,
        run_id,
        plan_hash,
        started_at,
        outcome,
        outcome_detail,
        finished_at,
        CompletionMilestoneDisposition::ReconcileFromLineage,
    )?;
    tracing::info!(
        operation = "repair_task_run",
        outcome = "success",
        run_id = run_id,
        "repaired task run"
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn repair_task_run_with_disposition(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    lineage_store: &impl TaskRunLineagePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    project_id: &str,
    run_id: &str,
    plan_hash: Option<&str>,
    started_at: DateTime<Utc>,
    outcome: TaskRunOutcome,
    outcome_detail: Option<String>,
    finished_at: DateTime<Utc>,
    disposition: CompletionMilestoneDisposition,
) -> AppResult<()> {
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, move || {
        if !outcome.is_terminal() {
            return Err(AppError::RunStartFailed {
                reason: format!(
                    "cannot repair bead '{bead_id}' with non-terminal outcome '{outcome}'"
                ),
            });
        }

        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        clear_pending_lineage_reset_locked(snapshot_store, base_dir, milestone_id, &mut snapshot)?;
        let previous_snapshot = snapshot.clone();
        let existing_journal = journal_store.read_journal(base_dir, milestone_id)?;
        let repaired_run = lineage_store.repair_task_run_terminal(
            base_dir,
            milestone_id,
            bead_id,
            project_id,
            run_id,
            plan_hash,
            started_at,
            outcome,
            outcome_detail,
            finished_at,
        )?;

        reconcile_snapshot_from_lineage(
            &mut snapshot,
            milestone_id,
            lineage_store.read_task_runs(base_dir, milestone_id)?,
        )?;
        apply_completion_milestone_disposition(&mut snapshot, disposition);
        let event_timestamp = repaired_run.finished_at.unwrap_or(finished_at);
        snapshot.updated_at = snapshot.updated_at.max(finished_at).max(event_timestamp);
        validate_snapshot(&snapshot, milestone_id)?;
        let bead_start_event =
            MilestoneJournalEvent::new(MilestoneEventType::BeadStarted, repaired_run.started_at)
                .with_bead(&repaired_run.bead_id)
                .with_details(repaired_run.start_journal_details());
        let bead_event = MilestoneJournalEvent::new(
            event_type_for_outcome(repaired_run.outcome),
            event_timestamp,
        )
        .with_bead(&repaired_run.bead_id)
        .with_details(repaired_run.completion_journal_details());
        let mut journal_ops = vec![JournalWriteOp::append_if_missing(bead_start_event)];
        journal_ops.extend(
            build_reconciled_transition_events(
                milestone_id,
                &previous_snapshot,
                &snapshot,
                &existing_journal,
                event_timestamp,
                Some(repaired_run.started_at),
                "controller",
                completion_status_reason(snapshot.status),
            )?
            .into_iter()
            .map(JournalWriteOp::append_if_missing),
        );
        journal_ops.push(JournalWriteOp::repair_completion(bead_event));

        commit_snapshot_and_journal_ops(
            snapshot_store,
            journal_store,
            base_dir,
            milestone_id,
            &previous_snapshot,
            &snapshot,
            &journal_ops,
            "milestone journal commit failed after bead repair snapshot write",
        )?;

        Ok(())
    })
}

// ── Planned-elsewhere mapping ────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposeNewBeadInput {
    pub active_bead_id: String,
    pub finding_summary: String,
    pub proposed_title: String,
    pub proposed_scope: String,
    pub severity: Severity,
    pub rationale: String,
    pub run_id: Option<String>,
    pub completion_round: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProposeNewBeadOutcome {
    Created { bead_id: String },
    ReclassifiedAsPlannedElsewhere { bead_id: String },
}

fn normalize_bead_match_text(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn proposed_bead_priority(severity: Severity) -> &'static str {
    match severity {
        Severity::Critical => "0",
        Severity::High => "1",
        Severity::Medium => "2",
        Severity::Low => "3",
    }
}

fn render_proposed_bead_description(input: &ProposeNewBeadInput) -> String {
    format!(
        "## Finding Summary\n{}\n\n## Proposed Scope\n{}\n\n## Rationale\n{}",
        input.finding_summary, input.proposed_scope, input.rationale
    )
}

fn candidate_bead_ids_from_create_stdout(stdout: &str) -> Vec<String> {
    let mut candidates = Vec::new();
    for token in stdout.split_whitespace().rev() {
        let cleaned = token.trim_matches(|ch: char| {
            !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.')
        });
        if cleaned.is_empty()
            || cleaned.eq_ignore_ascii_case("created")
            || cleaned.eq_ignore_ascii_case("bead")
            || cleaned.eq_ignore_ascii_case("issue")
        {
            continue;
        }
        if !candidates.iter().any(|candidate| candidate == cleaned) {
            candidates.push(cleaned.to_owned());
        }
    }
    candidates
}

async fn list_matching_beads_by_title<R: ProcessRunner>(
    br_read: &BrAdapter<R>,
    proposed_title: &str,
) -> Result<Vec<BeadSummary>, crate::adapters::br_process::BrError> {
    let summaries = br_read
        .exec_json::<BrListSummariesResponse>(&BrCommand::list_all())
        .await?
        .into_issues();
    let normalized_title = normalize_bead_match_text(proposed_title);
    Ok(summaries
        .into_iter()
        .filter(|summary| normalize_bead_match_text(&summary.title) == normalized_title)
        .collect())
}

fn eligible_existing_proposed_bead_match(summary: &BeadSummary, active_bead_id: &str) -> bool {
    summary.id != active_bead_id
        && matches!(
            summary.status,
            BeadStatus::Open | BeadStatus::InProgress | BeadStatus::Deferred
        )
}

fn proposed_bead_depends_on_active_bead(detail: &BeadDetail, active_bead_id: &str) -> bool {
    detail
        .dependencies
        .iter()
        .any(|dependency| dependency.id == active_bead_id)
}

fn existing_bead_matches_replayed_proposed_creation(
    detail: &BeadDetail,
    input: &ProposeNewBeadInput,
) -> bool {
    detail.description.as_deref() == Some(render_proposed_bead_description(input).as_str())
}

fn existing_bead_semantically_matches_proposed_work(
    detail: &BeadDetail,
    input: &ProposeNewBeadInput,
) -> bool {
    let Some(description) = detail.description.as_deref() else {
        return false;
    };
    let normalized_description = normalize_bead_match_text(description);
    [
        input.finding_summary.as_str(),
        input.proposed_scope.as_str(),
        input.rationale.as_str(),
    ]
    .into_iter()
    .map(normalize_bead_match_text)
    .all(|needle| normalized_description.contains(&needle))
}

fn is_missing_bead_error(error: &crate::adapters::br_process::BrError) -> bool {
    match error {
        crate::adapters::br_process::BrError::BrExitError { stdout, stderr, .. } => {
            let stdout = normalize_bead_match_text(stdout);
            let stderr = normalize_bead_match_text(stderr);
            stdout.contains("not found") || stderr.contains("not found")
        }
        _ => false,
    }
}

fn matching_proposed_bead_created_event(
    event: &MilestoneJournalEvent,
    input: &ProposeNewBeadInput,
) -> Option<String> {
    if event.event_type != MilestoneEventType::ProposedBeadCreated {
        return None;
    }

    let metadata = event.metadata.as_ref()?;
    if metadata
        .get("active_bead_id")
        .and_then(|value| value.as_str())
        != Some(input.active_bead_id.as_str())
    {
        return None;
    }
    if metadata
        .get("proposed_title")
        .and_then(|value| value.as_str())
        != Some(input.proposed_title.as_str())
    {
        return None;
    }
    if event.details.as_deref() != Some(input.finding_summary.as_str()) {
        return None;
    }
    if let Some(run_id) = input.run_id.as_deref() {
        if metadata.get("run_id").and_then(|value| value.as_str()) != Some(run_id) {
            return None;
        }
    }
    if let Some(completion_round) = input.completion_round {
        if metadata
            .get("completion_round")
            .and_then(|value| value.as_u64())
            .and_then(|value| u32::try_from(value).ok())
            != Some(completion_round)
        {
            return None;
        }
    }

    metadata
        .get("created_bead_id")
        .and_then(|value| value.as_str())
        .map(str::to_owned)
}

fn load_existing_proposed_bead_creation(
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    input: &ProposeNewBeadInput,
) -> AppResult<Option<String>> {
    let journal = journal_store.read_journal(base_dir, milestone_id)?;
    Ok(journal
        .iter()
        .rev()
        .find_map(|event| matching_proposed_bead_created_event(event, input)))
}

async fn resolve_created_bead_id<R: ProcessRunner>(
    br_read: &BrAdapter<R>,
    input: &ProposeNewBeadInput,
    create_stdout: &str,
) -> AppResult<String> {
    for candidate in candidate_bead_ids_from_create_stdout(create_stdout) {
        if let Ok(detail) = br_read
            .exec_json::<BeadDetail>(&BrCommand::show(candidate.clone()))
            .await
        {
            if existing_bead_matches_replayed_proposed_creation(&detail, input) {
                return Ok(candidate);
            }
        }
    }

    for existing in list_matching_beads_by_title(br_read, &input.proposed_title)
        .await
        .map_err(|error| AppError::MilestoneOperationFailed {
            milestone_id: "unknown".to_owned(),
            action: "query existing beads".to_owned(),
            details: error.to_string(),
        })?
        .into_iter()
        .filter(|summary| eligible_existing_proposed_bead_match(summary, &input.active_bead_id))
    {
        let detail = br_read
            .exec_json::<BeadDetail>(&BrCommand::show(existing.id.clone()))
            .await
            .map_err(|error| AppError::MilestoneOperationFailed {
                milestone_id: "unknown".to_owned(),
                action: "inspect created bead fallback candidate".to_owned(),
                details: error.to_string(),
            })?;
        if existing_bead_matches_replayed_proposed_creation(&detail, input) {
            return Ok(existing.id);
        }
    }

    Err(AppError::MilestoneOperationFailed {
        milestone_id: "unknown".to_owned(),
        action: "resolve created bead id".to_owned(),
        details: format!(
            "br create succeeded but the created bead id could not be determined for title '{}'",
            input.proposed_title
        ),
    })
}

fn record_proposed_bead_created_event(
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    input: &ProposeNewBeadInput,
    bead_id: &str,
    no_existing_match_reason: &str,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let mut metadata = JsonMap::new();
    metadata.insert(
        "active_bead_id".to_owned(),
        JsonValue::String(input.active_bead_id.clone()),
    );
    metadata.insert(
        "created_bead_id".to_owned(),
        JsonValue::String(bead_id.to_owned()),
    );
    metadata.insert(
        "proposed_title".to_owned(),
        JsonValue::String(input.proposed_title.clone()),
    );
    metadata.insert(
        "proposed_scope".to_owned(),
        JsonValue::String(input.proposed_scope.clone()),
    );
    metadata.insert(
        "severity".to_owned(),
        JsonValue::String(input.severity.as_str().to_owned()),
    );
    metadata.insert(
        "rationale".to_owned(),
        JsonValue::String(input.rationale.clone()),
    );
    metadata.insert(
        "no_existing_match_reason".to_owned(),
        JsonValue::String(no_existing_match_reason.to_owned()),
    );
    metadata.insert(
        "placement".to_owned(),
        JsonValue::String("created_bead_depends_on_active_bead".to_owned()),
    );
    metadata.insert(
        "dependency_from_bead_id".to_owned(),
        JsonValue::String(bead_id.to_owned()),
    );
    metadata.insert(
        "dependency_depends_on_bead_id".to_owned(),
        JsonValue::String(input.active_bead_id.clone()),
    );
    if let Some(ref run_id) = input.run_id {
        metadata.insert("run_id".to_owned(), JsonValue::String(run_id.clone()));
    }
    if let Some(completion_round) = input.completion_round {
        metadata.insert(
            "completion_round".to_owned(),
            JsonValue::Number(serde_json::Number::from(completion_round)),
        );
    }

    let mut event = MilestoneJournalEvent::new(MilestoneEventType::ProposedBeadCreated, now)
        .with_bead(input.active_bead_id.clone())
        .with_details(input.finding_summary.clone());
    event.metadata = Some(metadata);
    let line = event.to_ndjson_line()?;
    journal_store.append_event(base_dir, milestone_id, &line)
}

pub fn record_beads_exported_event(
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bundle_hash: &str,
    report: &BeadMaterializationReport,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let mut metadata = JsonMap::new();
    metadata.insert(
        "sub_type".to_owned(),
        JsonValue::String("beads_exported".to_owned()),
    );
    metadata.insert(
        "bundle_hash".to_owned(),
        JsonValue::String(bundle_hash.to_owned()),
    );
    metadata.insert(
        "root_epic_id".to_owned(),
        JsonValue::String(report.root_epic_id.clone()),
    );
    metadata.insert(
        "created_beads".to_owned(),
        JsonValue::Number(serde_json::Number::from(report.created_beads)),
    );
    metadata.insert(
        "reused_beads".to_owned(),
        JsonValue::Number(serde_json::Number::from(report.reused_beads)),
    );
    metadata.insert(
        "workstream_epic_ids".to_owned(),
        JsonValue::Array(
            report
                .workstream_epic_ids
                .iter()
                .cloned()
                .map(JsonValue::String)
                .collect(),
        ),
    );
    metadata.insert(
        "task_bead_ids".to_owned(),
        JsonValue::Array(
            report
                .task_bead_ids
                .iter()
                .cloned()
                .map(JsonValue::String)
                .collect(),
        ),
    );

    let mut event = MilestoneJournalEvent::new(MilestoneEventType::ProgressUpdated, now)
        .with_bead(report.root_epic_id.clone())
        .with_details("milestone beads exported".to_owned());
    event.metadata = Some(metadata);
    let line = event.to_ndjson_line()?;
    journal_store.append_event(base_dir, milestone_id, &line)
}

fn ensure_beads_mutation_health(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    action: &str,
) -> AppResult<()> {
    if let Some(details) = beads_health_failure_details(&check_beads_health(base_dir)) {
        return Err(AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: action.to_owned(),
            details,
        });
    }

    Ok(())
}

async fn sync_recovered_proposed_bead_replay_if_dirty<R: ProcessRunner>(
    br_mutation: &BrMutationAdapter<R>,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    action: &str,
) -> AppResult<()> {
    let outcome = match br_mutation.sync_own_dirty_if_beads_healthy(base_dir).await {
        Ok(outcome) => outcome,
        Err(SyncIfDirtyHealthError::UnsafeBeadsState { details }) => {
            return Err(AppError::MilestoneOperationFailed {
                milestone_id: milestone_id.to_string(),
                action: action.to_owned(),
                details,
            });
        }
        Err(SyncIfDirtyHealthError::Br(error)) => {
            return Err(AppError::MilestoneOperationFailed {
                milestone_id: milestone_id.to_string(),
                action: action.to_owned(),
                details: error.to_string(),
            });
        }
    };

    if !outcome.is_clean() {
        ensure_beads_mutation_health(base_dir, milestone_id, action)?;
    }

    Ok(())
}

async fn sync_beads_mutation_if_dirty_when_healthy<R: ProcessRunner>(
    br_mutation: &BrMutationAdapter<R>,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    action: &str,
) -> AppResult<()> {
    match br_mutation.sync_own_dirty_if_beads_healthy(base_dir).await {
        Ok(_) => Ok(()),
        Err(SyncIfDirtyHealthError::UnsafeBeadsState { details }) => {
            Err(AppError::MilestoneOperationFailed {
                milestone_id: milestone_id.to_string(),
                action: action.to_owned(),
                details,
            })
        }
        Err(SyncIfDirtyHealthError::Br(error)) => Err(AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: action.to_owned(),
            details: error.to_string(),
        }),
    }
}

pub async fn handle_propose_new_bead<R: ProcessRunner>(
    journal_store: &impl MilestoneJournalPort,
    mapping_store: &impl PlannedElsewhereMappingPort,
    br_mutation: &BrMutationAdapter<R>,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    input: &ProposeNewBeadInput,
    created_in_pass: &mut usize,
    now: DateTime<Utc>,
) -> AppResult<ProposeNewBeadOutcome> {
    async move {
        let br_read = br_mutation.inner();

        if let Some(existing_bead_id) =
            load_existing_proposed_bead_creation(journal_store, base_dir, milestone_id, input)?
        {
            match br_read
                .exec_json::<BeadDetail>(&BrCommand::show(existing_bead_id.clone()))
                .await
            {
                Ok(detail) => {
                    if !proposed_bead_depends_on_active_bead(&detail, &input.active_bead_id) {
                        ensure_beads_mutation_health(
                            base_dir,
                            milestone_id,
                            "repair journaled proposed bead dependency",
                        )?;
                        br_mutation
                            .add_dependency(&detail.id, &input.active_bead_id)
                            .await
                            .map_err(|error| AppError::MilestoneOperationFailed {
                                milestone_id: milestone_id.to_string(),
                                action: "repair journaled proposed bead dependency".to_owned(),
                                details: error.to_string(),
                            })?;
                        sync_beads_mutation_if_dirty_when_healthy(
                            br_mutation,
                            base_dir,
                            milestone_id,
                            "sync journaled proposed bead dependency repair",
                        )
                        .await?;
                    }

                    sync_recovered_proposed_bead_replay_if_dirty(
                        br_mutation,
                        base_dir,
                        milestone_id,
                        "sync recovered journaled proposed bead replay",
                    )
                    .await?;

                    return Ok(ProposeNewBeadOutcome::Created { bead_id: detail.id });
                }
                Err(error) if is_missing_bead_error(&error) => {
                    tracing::warn!(
                        active_bead_id = input.active_bead_id.as_str(),
                        created_bead_id = existing_bead_id.as_str(),
                        error = %error,
                        "journaled proposed bead no longer exists; re-running defensive lookup and creation flow"
                    );
                }
                Err(error) => {
                    return Err(AppError::MilestoneOperationFailed {
                        milestone_id: milestone_id.to_string(),
                        action: "verify journaled proposed bead".to_owned(),
                        details: error.to_string(),
                    });
                }
            }
        }

        let matching_beads = list_matching_beads_by_title(br_read, &input.proposed_title)
            .await
            .map_err(|error| AppError::MilestoneOperationFailed {
                milestone_id: milestone_id.to_string(),
                action: "query existing beads".to_owned(),
                details: error.to_string(),
            })?;

        for existing in matching_beads
        .iter()
        .filter(|existing| eligible_existing_proposed_bead_match(existing, &input.active_bead_id))
        {
            let detail = br_read
                .exec_json::<BeadDetail>(&BrCommand::show(existing.id.clone()))
                .await
                .map_err(|error| AppError::MilestoneOperationFailed {
                    milestone_id: milestone_id.to_string(),
                    action: "inspect existing proposed bead".to_owned(),
                    details: error.to_string(),
                })?;

            if !existing_bead_semantically_matches_proposed_work(&detail, input) {
                continue;
            }

            if !proposed_bead_depends_on_active_bead(&detail, &input.active_bead_id) {
                ensure_beads_mutation_health(
                    base_dir,
                    milestone_id,
                    "place recovered proposed bead dependency",
                )?;
                br_mutation
                    .add_dependency(&detail.id, &input.active_bead_id)
                    .await
                    .map_err(|error| AppError::MilestoneOperationFailed {
                        milestone_id: milestone_id.to_string(),
                        action: "place recovered proposed bead dependency".to_owned(),
                        details: error.to_string(),
                    })?;
                sync_beads_mutation_if_dirty_when_healthy(
                    br_mutation,
                    base_dir,
                    milestone_id,
                    "sync recovered proposed bead placement",
                )
                .await?;
            }

            sync_recovered_proposed_bead_replay_if_dirty(
                br_mutation,
                base_dir,
                milestone_id,
                "sync recovered proposed bead replay",
            )
            .await?;

            record_proposed_bead_created_event(
                journal_store,
                base_dir,
                milestone_id,
                input,
                &detail.id,
                "recovered previously created bead by matching title and rendered proposal payload",
                now,
            )?;
            return Ok(ProposeNewBeadOutcome::Created { bead_id: detail.id });
        }

        for existing in matching_beads
        .into_iter()
        .filter(|existing| eligible_existing_proposed_bead_match(existing, &input.active_bead_id))
        {
            let detail = br_read
                .exec_json::<BeadDetail>(&BrCommand::show(existing.id.clone()))
                .await
            .map_err(|error| AppError::MilestoneOperationFailed {
                milestone_id: milestone_id.to_string(),
                action: "inspect planned-elsewhere candidate".to_owned(),
                details: error.to_string(),
            })?;
        if !existing_bead_matches_replayed_proposed_creation(&detail, input) {
            continue;
        }

        let mapping = PlannedElsewhereMapping {
            active_bead_id: input.active_bead_id.clone(),
            finding_summary: input.finding_summary.clone(),
            mapped_to_bead_id: existing.id.clone(),
            recorded_at: now,
            mapped_bead_verified: true,
            run_id: input.run_id.clone(),
            completion_round: input.completion_round,
        };
        record_planned_elsewhere_mapping(
            journal_store,
            mapping_store,
            base_dir,
            milestone_id,
            &mapping,
        )?;
        return Ok(ProposeNewBeadOutcome::ReclassifiedAsPlannedElsewhere {
            bead_id: existing.id,
        });
    }

    let labels = match br_read
        .exec_json::<BeadDetail>(&BrCommand::show(input.active_bead_id.clone()))
        .await
    {
        Ok(detail) => detail.labels,
        Err(error) => {
            tracing::warn!(
                active_bead_id = input.active_bead_id.as_str(),
                error = %error,
                "failed to load active bead labels for propose-new-bead creation; creating without labels"
            );
            Vec::new()
        }
    };

    ensure_beads_mutation_health(base_dir, milestone_id, "prepare bead mutation")?;
    let create_output = br_mutation
        .create_bead(
            &input.proposed_title,
            "task",
            proposed_bead_priority(input.severity),
            &labels,
            Some(&render_proposed_bead_description(input)),
        )
        .await
        .map_err(|error| AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: "create proposed bead".to_owned(),
            details: error.to_string(),
        })?;

    let bead_id = resolve_created_bead_id(br_read, input, &create_output.stdout)
        .await
        .map_err(|error| AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: "resolve created bead".to_owned(),
            details: error.to_string(),
        })?;

    ensure_beads_mutation_health(base_dir, milestone_id, "place proposed bead dependency")?;
    br_mutation
        .add_dependency(&bead_id, &input.active_bead_id)
        .await
        .map_err(|error| AppError::MilestoneOperationFailed {
            milestone_id: milestone_id.to_string(),
            action: "place proposed bead dependency".to_owned(),
            details: error.to_string(),
        })?;

    sync_beads_mutation_if_dirty_when_healthy(
        br_mutation,
        base_dir,
        milestone_id,
        "sync proposed bead creation",
    )
    .await?;

    let no_existing_match_reason = format!(
        "no existing bead matched proposed title '{}'",
        input.proposed_title.trim()
    );
    record_proposed_bead_created_event(
        journal_store,
        base_dir,
        milestone_id,
        input,
        &bead_id,
        &no_existing_match_reason,
        now,
    )?;

    *created_in_pass += 1;
    tracing::warn!(
        active_bead_id = input.active_bead_id.as_str(),
        created_bead_id = bead_id.as_str(),
        created_in_pass = *created_in_pass,
        severity = input.severity.as_str(),
        "created new bead from propose-new-bead amendment"
    );
    if *created_in_pass > 2 {
        tracing::error!(
            active_bead_id = input.active_bead_id.as_str(),
            created_in_pass = *created_in_pass,
            "propose-new-bead created more than two beads in one reconciliation pass; review scope may be wrong"
        );
    }

        Ok(ProposeNewBeadOutcome::Created { bead_id })
    }
    .instrument(tracing::info_span!(
        "handle_propose_new_bead",
        milestone_id = %milestone_id,
        bead_id = input.active_bead_id.as_str()
    ))
    .await
}

/// Record a planned-elsewhere mapping: the finding in `active_bead_id` is
/// already covered by `mapped_to_bead_id`. Persists to both the milestone
/// journal (authoritative audit record) and the dedicated NDJSON file.
///
/// # Write ordering
///
/// The journal event is written first because it is the authoritative audit
/// record. If the secondary NDJSON write fails, the journal still contains
/// the mapping for audit purposes, and the NDJSON file can be rebuilt from
/// the journal. The reverse ordering (NDJSON first) would leave a mapping
/// with no audit trail on journal failure.
pub fn record_planned_elsewhere_mapping(
    journal_store: &impl MilestoneJournalPort,
    mapping_store: &impl PlannedElsewhereMappingPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    mapping: &PlannedElsewhereMapping,
) -> AppResult<()> {
    // 1. Build the journal event (authoritative audit record).
    let mut metadata = JsonMap::new();
    metadata.insert(
        "active_bead_id".to_owned(),
        JsonValue::String(mapping.active_bead_id.clone()),
    );
    metadata.insert(
        "mapped_to_bead_id".to_owned(),
        JsonValue::String(mapping.mapped_to_bead_id.clone()),
    );
    metadata.insert(
        "mapped_bead_verified".to_owned(),
        JsonValue::Bool(mapping.mapped_bead_verified),
    );
    if let Some(ref rid) = mapping.run_id {
        metadata.insert("run_id".to_owned(), JsonValue::String(rid.clone()));
    }
    if let Some(cr) = mapping.completion_round {
        metadata.insert(
            "completion_round".to_owned(),
            JsonValue::Number(serde_json::Number::from(cr)),
        );
    }
    // Write as ProgressUpdated with a sub_type discriminator so that
    // rolled-back code (which does not know PlannedElsewhereMapped) can
    // still parse the journal line.  The rebuild reader recognises both
    // the legacy PlannedElsewhereMapped event type and ProgressUpdated
    // events with sub_type == "planned_elsewhere_mapped".
    metadata.insert(
        "sub_type".to_owned(),
        JsonValue::String("planned_elsewhere_mapped".to_owned()),
    );

    let mut event =
        MilestoneJournalEvent::new(MilestoneEventType::ProgressUpdated, mapping.recorded_at)
            .with_bead(mapping.active_bead_id.clone())
            .with_details(mapping.finding_summary.clone());
    event.metadata = Some(metadata);

    let line = event.to_ndjson_line()?;

    // 2. Write journal first — this is the authoritative record.
    journal_store.append_event(base_dir, milestone_id, &line)?;

    // 3. Write to dedicated NDJSON file (secondary, write-through projection).
    // A failure here is non-critical: the journal is authoritative and
    // load_planned_elsewhere_mappings rebuilds from it. Log and continue.
    if let Err(e) = mapping_store.append_mapping(base_dir, milestone_id, mapping) {
        tracing::warn!(
            milestone_id = %milestone_id,
            error = %e,
            "failed to write planned-elsewhere NDJSON sidecar (journal record is intact)"
        );
    }

    Ok(())
}

/// Write a PE round sentinel to the milestone journal.
///
/// A sentinel marks that a given `(active_bead_id, run_id, completion_round)`
/// tuple was processed — even if zero PE mappings were produced.  This allows
/// [`rebuild_planned_elsewhere_from_journal`] to compute the authoritative max
/// round correctly so that a later round with no PE findings can still
/// supersede an earlier round that did have PE findings.
pub fn record_planned_elsewhere_round_sentinel(
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    active_bead_id: &str,
    run_id: &str,
    completion_round: u32,
    now: DateTime<Utc>,
) -> AppResult<()> {
    let mut metadata = JsonMap::new();
    metadata.insert(
        "active_bead_id".to_owned(),
        JsonValue::String(active_bead_id.to_owned()),
    );
    metadata.insert("run_id".to_owned(), JsonValue::String(run_id.to_owned()));
    metadata.insert(
        "completion_round".to_owned(),
        JsonValue::Number(serde_json::Number::from(completion_round)),
    );
    metadata.insert(
        "sub_type".to_owned(),
        JsonValue::String("pe_round_sentinel".to_owned()),
    );

    let mut event = MilestoneJournalEvent::new(MilestoneEventType::ProgressUpdated, now)
        .with_bead(active_bead_id.to_owned())
        .with_details("planned-elsewhere round sentinel".to_owned());
    event.metadata = Some(metadata);

    let line = event.to_ndjson_line()?;
    journal_store.append_event(base_dir, milestone_id, &line)?;
    Ok(())
}

/// Load all planned-elsewhere mappings for a milestone.
///
/// Rebuilds authoritative state from the journal `PlannedElsewhereMapped`
/// events, collapsed by identity `(active_bead_id, finding_summary,
/// mapped_to_bead_id)` so that later verification events (with
/// `mapped_bead_verified=true`) supersede earlier unverified records.
///
/// The journal is always the source of truth. The NDJSON sidecar is a
/// write-through projection for convenience; it is not consulted during reads
/// because it can diverge from the journal when `append_mapping()` fails
/// after a successful `append_event()`.
pub fn load_planned_elsewhere_mappings(
    _mapping_store: &impl PlannedElsewhereMappingPort,
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<Vec<PlannedElsewhereMapping>> {
    rebuild_planned_elsewhere_from_journal(journal_store, base_dir, milestone_id)
}

/// Rebuild planned-elsewhere mappings from journal `PlannedElsewhereMapped`
/// events. Collapses rows by identity key `(active_bead_id, finding_summary,
/// mapped_to_bead_id)`, keeping the last journal entry for each key (relies
/// on append-only chronological ordering of the journal). This ensures that
/// a verification event appended after the original unverified record
/// supersedes it.
fn rebuild_planned_elsewhere_from_journal(
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<Vec<PlannedElsewhereMapping>> {
    use std::collections::HashMap;

    let events = journal_store.read_journal(base_dir, milestone_id)?;
    // Key: (active_bead_id, finding_summary, mapped_to_bead_id) → latest mapping.
    let mut by_identity: HashMap<(String, String, String), PlannedElsewhereMapping> =
        HashMap::new();

    // Also track max completion_round from PE round sentinels so that a
    // later round with zero PE mappings can still supersede earlier rounds.
    let mut sentinel_max_round: HashMap<(String, String), u32> = HashMap::new();
    // Track the latest (most recent journal entry) run_id per active_bead_id
    // from sentinel events.  Since the journal is append-only, the last
    // sentinel for a bead carries the authoritative run_id.  Mappings from
    // earlier runs are superseded by the latest run.
    let mut latest_run_by_bead: HashMap<String, String> = HashMap::new();

    for event in &events {
        let sub_type = event
            .metadata
            .as_ref()
            .and_then(|m| m.get("sub_type"))
            .and_then(|v| v.as_str());

        // Collect PE round sentinels for max-round computation.
        if event.event_type == MilestoneEventType::ProgressUpdated
            && sub_type == Some("pe_round_sentinel")
        {
            if let Some(m) = &event.metadata {
                let bead = m.get("active_bead_id").and_then(|v| v.as_str());
                let rid = m.get("run_id").and_then(|v| v.as_str());
                let cr = m
                    .get("completion_round")
                    .and_then(|v| v.as_u64())
                    .and_then(|v| u32::try_from(v).ok());
                if let (Some(bead), Some(rid), Some(cr)) = (bead, rid, cr) {
                    let key = (bead.to_owned(), rid.to_owned());
                    let entry = sentinel_max_round.entry(key).or_insert(cr);
                    if cr > *entry {
                        *entry = cr;
                    }
                    // Append-only: last sentinel wins, giving us the
                    // authoritative run for cross-run supersession.
                    latest_run_by_bead.insert(bead.to_owned(), rid.to_owned());
                }
            }
            continue;
        }

        // Recognise both the legacy PlannedElsewhereMapped event type and
        // the newer ProgressUpdated events that carry a sub_type
        // discriminator of "planned_elsewhere_mapped" (write-compatible
        // with older code that does not know PlannedElsewhereMapped).
        let is_pe = event.event_type == MilestoneEventType::PlannedElsewhereMapped
            || (event.event_type == MilestoneEventType::ProgressUpdated
                && sub_type == Some("planned_elsewhere_mapped"));
        if !is_pe {
            continue;
        }
        let metadata = match &event.metadata {
            Some(m) => m,
            None => continue,
        };
        let active_bead_id = match metadata.get("active_bead_id").and_then(|v| v.as_str()) {
            Some(s) => s.to_owned(),
            None => continue,
        };
        let mapped_to_bead_id = match metadata.get("mapped_to_bead_id").and_then(|v| v.as_str()) {
            Some(s) => s.to_owned(),
            None => continue,
        };
        let mapped_bead_verified = metadata
            .get("mapped_bead_verified")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let run_id = metadata
            .get("run_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_owned());
        let completion_round = metadata
            .get("completion_round")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok());
        let finding_summary = event.details.clone().unwrap_or_default();

        let key = (
            active_bead_id.clone(),
            finding_summary.clone(),
            mapped_to_bead_id.clone(),
        );
        let mapping = PlannedElsewhereMapping {
            active_bead_id,
            finding_summary,
            mapped_to_bead_id,
            recorded_at: event.timestamp,
            mapped_bead_verified,
            run_id,
            completion_round,
        };
        // Later journal events always supersede earlier ones.
        by_identity.insert(key, mapping);
    }

    // Apply run/round supersession: for each (active_bead_id, run_id) group
    // where run_id is Some, find the maximum completion_round and keep only
    // mappings from that round.  This ensures that if a later round fixes or
    // remaps a finding, the earlier round's PE mapping is discarded.
    // Mappings with run_id=None (legacy) or completion_round=None are kept
    // unconditionally since they lack provenance for round filtering.
    let mappings: Vec<_> = by_identity.into_values().collect();
    // Seed max-round from sentinel events (which cover zero-PE rounds),
    // then update from the mapping-derived rounds.
    let mut max_round_by_bead_run: HashMap<(String, String), u32> = sentinel_max_round;
    for m in &mappings {
        if let (Some(rid), Some(cr)) = (&m.run_id, m.completion_round) {
            let key = (m.active_bead_id.clone(), rid.clone());
            let entry = max_round_by_bead_run.entry(key).or_insert(cr);
            if cr > *entry {
                *entry = cr;
            }
        }
    }
    let result: Vec<_> = mappings
        .into_iter()
        .filter(|m| {
            match (&m.run_id, m.completion_round) {
                (Some(rid), Some(cr)) => {
                    // Cross-run supersession: if a sentinel identifies the
                    // authoritative run for this bead, discard mappings from
                    // any other (abandoned/earlier) run.
                    if let Some(auth_run) = latest_run_by_bead.get(&m.active_bead_id) {
                        if rid != auth_run {
                            return false;
                        }
                    }
                    // Within-run round supersession: keep only the max round.
                    let key = (m.active_bead_id.clone(), rid.clone());
                    max_round_by_bead_run.get(&key) == Some(&cr)
                }
                // Legacy mappings without provenance are kept unconditionally.
                _ => true,
            }
        })
        .collect();

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::Duration;

    use crate::adapters::br_process::{BrError, BrOutput, ProcessRunner};
    use crate::adapters::fs::{
        FsMilestoneJournalStore, FsMilestonePlanStore, FsMilestoneSnapshotStore, FsMilestoneStore,
        FsPlannedElsewhereMappingStore, FsTaskRunLineageStore,
    };
    use crate::contexts::milestone_record::model::render_completion_journal_details;
    use crate::test_support::br::{MockBrAdapter, MockBrResponse};
    use crate::test_support::logging::log_capture;

    struct FailSecondJournalAppend {
        append_calls: Cell<u32>,
    }

    impl MilestoneJournalPort for FailSecondJournalAppend {
        fn read_journal(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<Vec<MilestoneJournalEvent>> {
            FsMilestoneJournalStore.read_journal(base_dir, milestone_id)
        }

        fn append_event(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            line: &str,
        ) -> AppResult<()> {
            FsMilestoneJournalStore.append_event(base_dir, milestone_id, line)
        }

        fn replace_journal(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            events: &[MilestoneJournalEvent],
        ) -> AppResult<()> {
            let next_call = self.append_calls.get() + 1;
            self.append_calls.set(next_call);
            if next_call == 2 {
                return Err(std::io::Error::other("simulated completion journal failure").into());
            }

            FsMilestoneJournalStore.replace_journal(base_dir, milestone_id, events)
        }

        fn append_event_if_missing(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            event: &MilestoneJournalEvent,
        ) -> AppResult<bool> {
            FsMilestoneJournalStore.append_event_if_missing(base_dir, milestone_id, event)
        }
    }

    struct FailFirstJournalAppend {
        append_calls: Cell<u32>,
    }

    impl MilestoneJournalPort for FailFirstJournalAppend {
        fn read_journal(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<Vec<MilestoneJournalEvent>> {
            FsMilestoneJournalStore.read_journal(base_dir, milestone_id)
        }

        fn append_event(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            line: &str,
        ) -> AppResult<()> {
            FsMilestoneJournalStore.append_event(base_dir, milestone_id, line)
        }

        fn replace_journal(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            events: &[MilestoneJournalEvent],
        ) -> AppResult<()> {
            let next_call = self.append_calls.get() + 1;
            self.append_calls.set(next_call);
            if next_call == 1 {
                return Err(std::io::Error::other("simulated start journal failure").into());
            }

            FsMilestoneJournalStore.replace_journal(base_dir, milestone_id, events)
        }

        fn append_event_if_missing(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            event: &MilestoneJournalEvent,
        ) -> AppResult<bool> {
            FsMilestoneJournalStore.append_event_if_missing(base_dir, milestone_id, event)
        }
    }

    struct MockBrRunner {
        responses: Mutex<Vec<Result<BrOutput, BrError>>>,
        commands: Arc<Mutex<Vec<Vec<String>>>>,
    }

    impl MockBrRunner {
        fn new(responses: Vec<Result<BrOutput, BrError>>) -> Self {
            Self {
                responses: Mutex::new(responses),
                commands: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn success(stdout: &str) -> Result<BrOutput, BrError> {
            Ok(BrOutput {
                stdout: stdout.to_owned(),
                stderr: String::new(),
                exit_code: 0,
            })
        }

        fn error(exit_code: i32, stderr: &str) -> Result<BrOutput, BrError> {
            Err(BrError::BrExitError {
                exit_code,
                stdout: String::new(),
                stderr: stderr.to_owned(),
                command: "br test".to_owned(),
            })
        }

        fn command_log(&self) -> Arc<Mutex<Vec<Vec<String>>>> {
            Arc::clone(&self.commands)
        }
    }

    impl ProcessRunner for MockBrRunner {
        async fn run(
            &self,
            args: Vec<String>,
            _timeout: Duration,
            _working_dir: Option<&Path>,
        ) -> Result<BrOutput, BrError> {
            self.commands
                .lock()
                .expect("mock br runner command log poisoned")
                .push(args);
            self.responses
                .lock()
                .expect("mock br runner lock poisoned")
                .pop()
                .unwrap_or_else(|| panic!("MockBrRunner: no more responses"))
        }
    }

    type ScriptedBrResponder = dyn FnMut(&[String]) -> Result<BrOutput, BrError> + Send + 'static;

    struct ScriptedBrRunner {
        responder: Mutex<Box<ScriptedBrResponder>>,
        commands: Arc<Mutex<Vec<Vec<String>>>>,
    }

    impl ScriptedBrRunner {
        fn new<F>(responder: F) -> Self
        where
            F: FnMut(&[String]) -> Result<BrOutput, BrError> + Send + 'static,
        {
            Self {
                responder: Mutex::new(Box::new(responder)),
                commands: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn command_log(&self) -> Arc<Mutex<Vec<Vec<String>>>> {
            Arc::clone(&self.commands)
        }
    }

    impl ProcessRunner for ScriptedBrRunner {
        async fn run(
            &self,
            args: Vec<String>,
            _timeout: Duration,
            _working_dir: Option<&Path>,
        ) -> Result<BrOutput, BrError> {
            self.commands
                .lock()
                .expect("scripted br runner command log poisoned")
                .push(args.clone());
            let mut responder = self
                .responder
                .lock()
                .expect("scripted br runner responder poisoned");
            responder(&args)
        }
    }

    struct BlockingSnapshotStore {
        entered_tx: Mutex<Option<mpsc::Sender<()>>>,
        release_rx: Mutex<Option<mpsc::Receiver<()>>>,
    }

    impl BlockingSnapshotStore {
        fn new(entered_tx: mpsc::Sender<()>, release_rx: mpsc::Receiver<()>) -> Self {
            Self {
                entered_tx: Mutex::new(Some(entered_tx)),
                release_rx: Mutex::new(Some(release_rx)),
            }
        }
    }

    /// Single-use test helper that coordinates exactly one contended lock
    /// acquisition. A second use indicates the test setup is no longer matching
    /// its intended one-shot synchronization contract.
    struct LockAttemptSnapshotStore {
        attempted_tx: Mutex<Option<mpsc::Sender<()>>>,
        proceed_rx: Mutex<Option<mpsc::Receiver<()>>>,
    }

    impl LockAttemptSnapshotStore {
        fn new(attempted_tx: mpsc::Sender<()>, proceed_rx: mpsc::Receiver<()>) -> Self {
            Self {
                attempted_tx: Mutex::new(Some(attempted_tx)),
                proceed_rx: Mutex::new(Some(proceed_rx)),
            }
        }
    }

    impl MilestoneSnapshotPort for LockAttemptSnapshotStore {
        fn read_snapshot(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<MilestoneSnapshot> {
            FsMilestoneSnapshotStore.read_snapshot(base_dir, milestone_id)
        }

        fn write_snapshot(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            snapshot: &MilestoneSnapshot,
        ) -> AppResult<()> {
            FsMilestoneSnapshotStore.write_snapshot(base_dir, milestone_id, snapshot)
        }

        fn with_milestone_write_lock<T, F>(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            operation: F,
        ) -> AppResult<T>
        where
            F: FnOnce() -> AppResult<T>,
        {
            let attempted_tx = self
                .attempted_tx
                .lock()
                .expect("lock attempted_tx")
                .take()
                .expect("LockAttemptSnapshotStore is single-use");
            attempted_tx
                .send(())
                .expect("signal lock attempt before contention");

            let proceed_rx = self
                .proceed_rx
                .lock()
                .expect("lock proceed_rx")
                .take()
                .expect("LockAttemptSnapshotStore is single-use");
            proceed_rx.recv().expect("allow lock contention");

            FsMilestoneSnapshotStore.with_milestone_write_lock(base_dir, milestone_id, operation)
        }
    }

    impl MilestoneSnapshotPort for BlockingSnapshotStore {
        fn read_snapshot(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<MilestoneSnapshot> {
            FsMilestoneSnapshotStore.read_snapshot(base_dir, milestone_id)
        }

        fn write_snapshot(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            snapshot: &MilestoneSnapshot,
        ) -> AppResult<()> {
            if snapshot.status != MilestoneStatus::Running && snapshot.active_bead.is_none() {
                if let Some(entered_tx) = self.entered_tx.lock().expect("lock entered_tx").take() {
                    entered_tx.send(()).expect("signal blocked snapshot write");
                }
                if let Some(release_rx) = self.release_rx.lock().expect("lock release_rx").take() {
                    release_rx.recv().expect("release blocked snapshot write");
                }
            }

            FsMilestoneSnapshotStore.write_snapshot(base_dir, milestone_id, snapshot)
        }

        fn with_milestone_write_lock<T, F>(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            operation: F,
        ) -> AppResult<T>
        where
            F: FnOnce() -> AppResult<T>,
        {
            FsMilestoneSnapshotStore.with_milestone_write_lock(base_dir, milestone_id, operation)
        }
    }

    struct FailPlanVersionSnapshotWrite {
        blocked_plan_version: u32,
    }

    impl MilestoneSnapshotPort for FailPlanVersionSnapshotWrite {
        fn read_snapshot(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<MilestoneSnapshot> {
            FsMilestoneSnapshotStore.read_snapshot(base_dir, milestone_id)
        }

        fn write_snapshot(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            snapshot: &MilestoneSnapshot,
        ) -> AppResult<()> {
            if snapshot.plan_version == self.blocked_plan_version {
                return Err(std::io::Error::other("simulated snapshot write failure").into());
            }

            FsMilestoneSnapshotStore.write_snapshot(base_dir, milestone_id, snapshot)
        }

        fn with_milestone_write_lock<T, F>(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            operation: F,
        ) -> AppResult<T>
        where
            F: FnOnce() -> AppResult<T>,
        {
            FsMilestoneSnapshotStore.with_milestone_write_lock(base_dir, milestone_id, operation)
        }
    }

    struct CountingSnapshotStore {
        write_calls: Cell<u32>,
    }

    impl CountingSnapshotStore {
        fn new() -> Self {
            Self {
                write_calls: Cell::new(0),
            }
        }
    }

    impl MilestoneSnapshotPort for CountingSnapshotStore {
        fn read_snapshot(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<MilestoneSnapshot> {
            FsMilestoneSnapshotStore.read_snapshot(base_dir, milestone_id)
        }

        fn write_snapshot(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            snapshot: &MilestoneSnapshot,
        ) -> AppResult<()> {
            self.write_calls.set(self.write_calls.get() + 1);
            FsMilestoneSnapshotStore.write_snapshot(base_dir, milestone_id, snapshot)
        }

        fn with_milestone_write_lock<T, F>(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            operation: F,
        ) -> AppResult<T>
        where
            F: FnOnce() -> AppResult<T>,
        {
            FsMilestoneSnapshotStore.with_milestone_write_lock(base_dir, milestone_id, operation)
        }
    }

    struct FailingPlanReadStore {
        error_kind: std::io::ErrorKind,
    }

    impl MilestonePlanPort for FailingPlanReadStore {
        fn read_plan_json(
            &self,
            _base_dir: &Path,
            _milestone_id: &MilestoneId,
        ) -> AppResult<String> {
            Err(std::io::Error::new(self.error_kind, "simulated plan read failure").into())
        }

        fn write_plan_json(
            &self,
            _base_dir: &Path,
            _milestone_id: &MilestoneId,
            _content: &str,
        ) -> AppResult<()> {
            unreachable!("write_plan_json is not used by this test helper")
        }

        fn read_plan_md(&self, _base_dir: &Path, _milestone_id: &MilestoneId) -> AppResult<String> {
            unreachable!("read_plan_md is not used by this test helper")
        }

        fn write_plan_md(
            &self,
            _base_dir: &Path,
            _milestone_id: &MilestoneId,
            _content: &str,
        ) -> AppResult<()> {
            unreachable!("write_plan_md is not used by this test helper")
        }

        fn read_plan_shape(
            &self,
            _base_dir: &Path,
            _milestone_id: &MilestoneId,
        ) -> AppResult<String> {
            unreachable!("read_plan_shape is not used by this test helper")
        }

        fn write_plan_shape(
            &self,
            _base_dir: &Path,
            _milestone_id: &MilestoneId,
            _content: &str,
        ) -> AppResult<()> {
            unreachable!("write_plan_shape is not used by this test helper")
        }
    }

    fn setup_workspace(dir: &Path) {
        std::fs::create_dir_all(dir.join(".beads")).unwrap();
        std::fs::create_dir_all(dir.join(".ralph-burning/milestones")).unwrap();
        std::fs::write(dir.join(".beads/issues.jsonl"), "").unwrap();
    }

    fn sample_bundle(
        id: &str,
        name: &str,
    ) -> crate::contexts::milestone_record::bundle::MilestoneBundle {
        use crate::contexts::milestone_record::bundle::{
            AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
        };

        MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: id.to_owned(),
                name: name.to_owned(),
            },
            executive_summary: "Test plan.".to_owned(),
            goals: vec!["Goal 1".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Tests pass".to_owned(),
                covered_by: vec!["bead-1".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: Some("Fixture description.".to_owned()),
                beads: vec![BeadProposal {
                    bead_id: None,
                    explicit_id: None,
                    title: "Implement feature".to_owned(),
                    description: Some("Deliver the scoped milestone behavior.".to_owned()),
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec!["backend".to_owned()],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::QuickDev,
            agents_guidance: None,
        }
    }

    fn create_milestone_with_plan(
        store: &FsMilestoneStore,
        snapshot_store: &FsMilestoneSnapshotStore,
        journal_store: &FsMilestoneJournalStore,
        plan_store: &FsMilestonePlanStore,
        base: &Path,
        id: &str,
        name: &str,
        now: DateTime<Utc>,
    ) -> Result<MilestoneRecord, Box<dyn std::error::Error>> {
        use crate::contexts::milestone_record::bundle::BeadProposal;

        let record = create_milestone(
            store,
            base,
            CreateMilestoneInput {
                id: id.to_owned(),
                name: name.to_owned(),
                description: format!("testing {id}"),
            },
            now,
        )?;
        let mut bundle = sample_bundle(id, name);
        bundle.acceptance_map[0]
            .covered_by
            .push("bead-2".to_owned());
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            explicit_id: None,
            title: "Follow-up feature".to_owned(),
            description: Some("Fixture description.".to_owned()),
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec!["fixture".to_owned()],
            depends_on: vec![],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        persist_plan(
            snapshot_store,
            journal_store,
            plan_store,
            base,
            &record.id,
            &bundle,
            now + chrono::Duration::milliseconds(1),
        )?;
        Ok(record)
    }

    fn bead_export_bundle(id: &str, name: &str) -> MilestoneBundle {
        use crate::contexts::milestone_record::bundle::{
            AcceptanceCriterion, BeadProposal, MilestoneIdentity, Workstream,
        };
        let bead_ref = |suffix: &str| format!("{id}.{suffix}");

        MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: id.to_owned(),
                name: name.to_owned(),
            },
            executive_summary: "Export the milestone plan into beads.".to_owned(),
            goals: vec!["Ship milestone bead export".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![
                AcceptanceCriterion {
                    id: "AC-1".to_owned(),
                    description: "Runtime scaffold exists".to_owned(),
                    covered_by: vec![bead_ref("bead-1"), bead_ref("bead-2")],
                },
                AcceptanceCriterion {
                    id: "AC-2".to_owned(),
                    description: "Validation coverage is exported".to_owned(),
                    covered_by: vec![bead_ref("bead-5"), bead_ref("bead-6")],
                },
            ],
            workstreams: vec![
                Workstream {
                    name: "Core".to_owned(),
                    description: Some("Core milestone delivery work.".to_owned()),
                    beads: vec![
                        BeadProposal {
                            bead_id: Some(bead_ref("bead-1")),
                            explicit_id: Some(true),
                            title: "Create runtime scaffold".to_owned(),
                            description: Some("Set up the exporter runtime.".to_owned()),
                            bead_type: Some("task".to_owned()),
                            priority: Some(1),
                            labels: vec!["backend".to_owned()],
                            depends_on: vec![],
                            acceptance_criteria: vec!["AC-1".to_owned()],
                            flow_override: None,
                        },
                        BeadProposal {
                            bead_id: Some(bead_ref("bead-2")),
                            explicit_id: Some(true),
                            title: "Wire milestone exporter".to_owned(),
                            description: Some("Invoke the exporter from the CLI.".to_owned()),
                            bead_type: Some("feature".to_owned()),
                            priority: Some(1),
                            labels: vec!["backend".to_owned(), "cli".to_owned()],
                            depends_on: vec![bead_ref("bead-1")],
                            acceptance_criteria: vec!["AC-1".to_owned()],
                            flow_override: None,
                        },
                        BeadProposal {
                            bead_id: Some(bead_ref("bead-3")),
                            explicit_id: Some(true),
                            title: "Persist export journal".to_owned(),
                            description: Some("Record export completion details.".to_owned()),
                            bead_type: Some("task".to_owned()),
                            priority: Some(2),
                            labels: vec!["backend".to_owned()],
                            depends_on: vec![bead_ref("bead-2")],
                            acceptance_criteria: vec![],
                            flow_override: None,
                        },
                    ],
                },
                Workstream {
                    name: "Validation".to_owned(),
                    description: Some("Validation and recovery coverage.".to_owned()),
                    beads: vec![
                        BeadProposal {
                            bead_id: Some(bead_ref("bead-4")),
                            explicit_id: Some(true),
                            title: "Add exporter smoke test".to_owned(),
                            description: Some("Cover the happy path.".to_owned()),
                            bead_type: Some("task".to_owned()),
                            priority: Some(2),
                            labels: vec!["test".to_owned()],
                            depends_on: vec![],
                            acceptance_criteria: vec![],
                            flow_override: None,
                        },
                        BeadProposal {
                            bead_id: Some(bead_ref("bead-5")),
                            explicit_id: Some(true),
                            title: "Verify idempotent export".to_owned(),
                            description: Some("Ensure reruns reuse existing beads.".to_owned()),
                            bead_type: Some("task".to_owned()),
                            priority: Some(2),
                            labels: vec!["test".to_owned()],
                            depends_on: vec![bead_ref("bead-2")],
                            acceptance_criteria: vec!["AC-2".to_owned()],
                            flow_override: None,
                        },
                        BeadProposal {
                            bead_id: Some(bead_ref("bead-6")),
                            explicit_id: Some(true),
                            title: "Handle partial export failure".to_owned(),
                            description: Some("Recover cleanly after mid-run failure.".to_owned()),
                            bead_type: Some("bug".to_owned()),
                            priority: Some(2),
                            labels: vec!["test".to_owned(), "reliability".to_owned()],
                            depends_on: vec![bead_ref("bead-5")],
                            acceptance_criteria: vec!["AC-2".to_owned()],
                            flow_override: None,
                        },
                    ],
                },
            ],
            default_flow: crate::shared::domain::FlowPreset::QuickDev,
            agents_guidance: None,
        }
    }

    fn bead_summary_value(
        id: &str,
        title: &str,
        bead_type: &str,
        status: &str,
        labels: &[&str],
    ) -> serde_json::Value {
        serde_json::json!({
            "id": id,
            "title": title,
            "status": status,
            "priority": 1,
            "bead_type": bead_type,
            "labels": labels
        })
    }

    fn list_all_stdout(summaries: Vec<serde_json::Value>) -> String {
        serde_json::json!({ "issues": summaries }).to_string()
    }

    fn bead_detail_stdout(
        id: &str,
        title: &str,
        bead_type: &str,
        status: &str,
        labels: &[&str],
        dependencies: &[&str],
    ) -> String {
        bead_detail_with_comments_stdout(id, title, bead_type, status, labels, dependencies, &[])
    }

    fn bead_detail_with_comments_stdout(
        id: &str,
        title: &str,
        bead_type: &str,
        status: &str,
        labels: &[&str],
        dependencies: &[&str],
        comments: &[&str],
    ) -> String {
        bead_detail_with_dependency_kinds_stdout(
            id,
            title,
            bead_type,
            status,
            labels,
            &dependencies
                .iter()
                .map(|dependency_id| (*dependency_id, "blocks"))
                .collect::<Vec<_>>(),
            comments,
        )
    }

    fn bead_detail_with_dependency_kinds_stdout(
        id: &str,
        title: &str,
        bead_type: &str,
        status: &str,
        labels: &[&str],
        dependencies: &[(&str, &str)],
        comments: &[&str],
    ) -> String {
        serde_json::json!([{
            "id": id,
            "title": title,
            "status": status,
            "priority": 1,
            "bead_type": bead_type,
            "labels": labels,
            "description": serde_json::Value::Null,
            "dependencies": dependencies.iter().enumerate().map(|(index, (dependency_id, kind))| serde_json::json!({
                "id": dependency_id,
                "kind": kind,
                "title": format!("dep-{index}")
            })).collect::<Vec<_>>(),
            "dependents": [],
            "comments": comments.iter().enumerate().map(|(index, text)| serde_json::json!({
                "id": index + 1,
                "issue_id": id,
                "author": "planner",
                "text": text,
                "created_at": "2026-04-17T12:00:00Z"
            })).collect::<Vec<_>>()
        }])
        .to_string()
    }

    fn setup_pending_lineage_reset_state(
        test_id: &str,
        test_name: &str,
    ) -> Result<
        (
            tempfile::TempDir,
            MilestoneRecord,
            crate::contexts::milestone_record::bundle::MilestoneBundle,
            DateTime<Utc>,
        ),
        Box<dyn std::error::Error>,
    > {
        use crate::contexts::milestone_record::bundle::BeadProposal;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle(test_id, test_name);
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            explicit_id: None,
            title: "Replacement bead".to_owned(),
            description: Some("Fixture description.".to_owned()),
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec!["fixture".to_owned()],
            depends_on: vec![],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned(), "bead-2".to_owned()];

        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("first bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        let mut updated_bundle = bundle.clone();
        updated_bundle.workstreams[0].beads.remove(0);
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        updated_bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned()];

        let milestone_root = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str());
        let task_runs_path = milestone_root.join("task-runs.ndjson");
        let preserved_lineage = std::fs::read_to_string(&task_runs_path)?;

        std::fs::remove_file(&task_runs_path)?;
        std::fs::create_dir(&task_runs_path)?;
        let error = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(3),
        )
        .expect_err("lineage truncation failure should leave a pending reset");
        assert!(!error.to_string().is_empty());

        std::fs::remove_dir(&task_runs_path)?;
        std::fs::write(&task_runs_path, preserved_lineage)?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.plan_version, 2);
        assert_eq!(
            snapshot.pending_lineage_reset,
            Some(PendingLineageReset {
                plan_hash: snapshot
                    .plan_hash
                    .clone()
                    .expect("plan hash must be present after rematerialize"),
                plan_version: 2,
            })
        );

        Ok((tmp, record, updated_bundle, now))
    }

    #[test]
    fn create_and_load_milestone() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "test-ms".to_owned(),
                name: "Test Milestone".to_owned(),
                description: "A test milestone".to_owned(),
            },
            now,
        )?;

        assert_eq!(record.id.as_str(), "test-ms");

        let loaded = load_milestone(&store, base, &record.id)?;
        assert_eq!(loaded.name, "Test Milestone");

        let ids = list_milestones(&store, base)?;
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].as_str(), "test-ms");
        Ok(())
    }

    #[test]
    fn create_duplicate_fails() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "dup".to_owned(),
                name: "First".to_owned(),
                description: "first".to_owned(),
            },
            now,
        )?;

        let result = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "dup".to_owned(),
                name: "Second".to_owned(),
                description: "second".to_owned(),
            },
            now,
        );
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn update_status_transitions() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "status-test",
            "Status Test",
            now,
        )?;

        let snapshot = update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Running,
            now,
        )?;
        assert_eq!(snapshot.status, MilestoneStatus::Running);

        let journal = read_journal(&journal_store, base, &record.id)?;
        assert!(journal.len() >= 2);
        Ok(())
    }

    #[test]
    fn update_status_rejects_invalid_transition_with_allowed_targets(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "invalid-status-transition",
            "Invalid Status Transition",
            now,
        )?;

        let error = update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Planning,
            now + chrono::Duration::seconds(1),
        )
        .expect_err("ready -> planning must be rejected");
        assert!(error.to_string().contains("ready -> planning"));
        assert!(error.to_string().contains("only to: running"));
        Ok(())
    }

    #[test]
    fn pausing_clears_active_bead_and_pause_event_metadata(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "pause-metadata-test",
            "Pause Metadata Test",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(2),
        )?;

        let snapshot = update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Paused,
            now + chrono::Duration::seconds(3),
        )?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Paused);
        assert_eq!(snapshot.active_bead, None);

        let journal = read_journal(&journal_store, base, &record.id)?;
        let paused_event = journal
            .iter()
            .rev()
            .find(|event| event.to_state == Some(MilestoneStatus::Paused))
            .expect("paused lifecycle event should be recorded");
        let metadata = paused_event
            .metadata
            .as_ref()
            .expect("paused lifecycle event should include metadata");
        assert!(!metadata.contains_key("active_bead"));
        assert_eq!(
            metadata.get("in_progress_beads"),
            Some(&serde_json::json!(1))
        );
        Ok(())
    }

    #[test]
    fn completed_transition_accumulates_runtime_across_resume(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "runtime-accumulation-test",
            "Runtime Accumulation Test",
            now,
        )?;

        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Running,
            now + chrono::Duration::seconds(2),
        )?;
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Paused,
            now + chrono::Duration::seconds(12),
        )?;
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Running,
            now + chrono::Duration::seconds(20),
        )?;
        let mut snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot.progress.completed_beads = snapshot.progress.total_beads;
        snapshot_store.write_snapshot(base, &record.id, &snapshot)?;
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Completed,
            now + chrono::Duration::seconds(25),
        )?;

        let journal = read_journal(&journal_store, base, &record.id)?;
        let completed_event = journal
            .iter()
            .rev()
            .find(|event| event.to_state == Some(MilestoneStatus::Completed))
            .expect("completed lifecycle event should be recorded");
        let metadata = completed_event
            .metadata
            .as_ref()
            .expect("completed lifecycle event should include metadata");
        assert_eq!(
            metadata.get("duration_seconds"),
            Some(&serde_json::json!(15))
        );
        Ok(())
    }

    #[test]
    fn update_status_paused_to_running_uses_resume_reason() -> Result<(), Box<dyn std::error::Error>>
    {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "resume-reason-test",
            "Resume Reason Test",
            now,
        )?;

        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Running,
            now + chrono::Duration::seconds(2),
        )?;
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Paused,
            now + chrono::Duration::seconds(3),
        )?;
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Running,
            now + chrono::Duration::seconds(4),
        )?;

        let journal = read_journal(&journal_store, base, &record.id)?;
        let resumed_event = journal
            .iter()
            .rev()
            .find(|event| {
                event.from_state == Some(MilestoneStatus::Paused)
                    && event.to_state == Some(MilestoneStatus::Running)
            })
            .expect("paused -> running event should be recorded");
        assert_eq!(resumed_event.reason.as_deref(), Some("execution resumed"));
        Ok(())
    }

    #[test]
    fn completed_transition_requires_all_beads_closed() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "completed-prereq-test",
            "Completed Prereq Test",
            now,
        )?;

        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Running,
            now + chrono::Duration::seconds(2),
        )?;

        let error = update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Completed,
            now + chrono::Duration::seconds(3),
        )
        .expect_err("completed transition must fail while beads remain open");
        assert!(error.to_string().contains("until all beads are closed"));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Running);
        Ok(())
    }

    #[test]
    fn completed_transition_error_mentions_failed_bead_recovery_hint() {
        let now = Utc::now();
        let mut snapshot = MilestoneSnapshot::initial(now);
        snapshot.status = MilestoneStatus::Running;
        snapshot.plan_hash = Some("plan-v1".to_owned());
        snapshot.plan_version = 1;
        snapshot.progress.total_beads = 2;
        snapshot.progress.completed_beads = 1;
        snapshot.progress.failed_beads = 1;

        let milestone_id = MilestoneId::new("completed-hint-test").expect("milestone id");
        let error =
            validate_transition_prerequisites(&snapshot, &milestone_id, MilestoneStatus::Completed)
                .expect_err("failed beads should block completion");
        let message = error.to_string();
        assert!(message.contains("until all beads are closed"));
        assert!(message.contains("re-run or skip failed beads to unblock completion"));
    }

    #[test]
    fn update_status_requires_finalized_plan_before_ready() -> Result<(), Box<dyn std::error::Error>>
    {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "ready-without-plan".to_owned(),
                name: "Ready Without Plan".to_owned(),
                description: "reject ready transition without plan export".to_owned(),
            },
            now,
        )?;

        let error = update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Ready,
            now + chrono::Duration::seconds(1),
        )
        .expect_err("ready transition without a plan must fail");
        assert!(error
            .to_string()
            .contains("before a plan is finalized and exported"));
        Ok(())
    }

    #[test]
    fn terminal_transition_duration_uses_pre_transition_timestamp_when_running_entry_is_missing(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "legacy-running-duration-test",
            "Legacy Running Duration Test",
            now,
        )?;

        let mut running_snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        running_snapshot.status = MilestoneStatus::Running;
        running_snapshot.progress.completed_beads = running_snapshot.progress.total_beads;
        running_snapshot.updated_at = now + chrono::Duration::seconds(7);
        snapshot_store.write_snapshot(base, &record.id, &running_snapshot)?;

        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Completed,
            now + chrono::Duration::seconds(13),
        )?;

        let journal = read_journal(&journal_store, base, &record.id)?;
        let completed_event = journal
            .iter()
            .rev()
            .find(|event| event.to_state == Some(MilestoneStatus::Completed))
            .expect("completed lifecycle event should be recorded");
        let metadata = completed_event
            .metadata
            .as_ref()
            .expect("completed lifecycle event should include metadata");
        assert_eq!(
            metadata.get("duration_seconds"),
            Some(&serde_json::json!(6))
        );
        Ok(())
    }

    #[test]
    fn persist_plan_updates_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "plan-test".to_owned(),
                name: "Plan Test".to_owned(),
                description: "testing plans".to_owned(),
            },
            now,
        )?;

        let bundle = sample_bundle("plan-test", "Plan Test");

        let snapshot = persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &bundle,
            now,
        )?;

        assert_eq!(snapshot.plan_version, 1);
        assert!(snapshot.plan_hash.is_some());
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.progress.total_beads, 1);

        let plan_json = plan_store.read_plan_json(base, &record.id)?;
        assert!(plan_json.contains("Plan Test"));

        let plan_md = plan_store.read_plan_md(base, &record.id)?;
        assert!(plan_md.contains("# Plan Test"));

        let journal = read_journal(&journal_store, base, &record.id)?;
        assert!(journal.iter().any(|event| {
            event.event_type == MilestoneEventType::StatusChanged
                && event.from_state == Some(MilestoneStatus::Planning)
                && event.to_state == Some(MilestoneStatus::Ready)
                && event.reason.as_deref() == Some("plan finalized and beads exported")
        }));
        Ok(())
    }

    #[test]
    fn persist_plan_rolls_back_snapshot_when_journal_commit_fails(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = CountingSnapshotStore::new();
        let journal_store = FailFirstJournalAppend {
            append_calls: Cell::new(0),
        };
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "single-plan-write".to_owned(),
                name: "Single Plan Write".to_owned(),
                description: "ensure plan commits write the snapshot once".to_owned(),
            },
            now,
        )?;

        let error = persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &sample_bundle("single-plan-write", "Single Plan Write"),
            now + chrono::Duration::seconds(1),
        )
        .expect_err("journal commit failure should roll back the snapshot");

        assert!(error
            .to_string()
            .contains("simulated start journal failure"));
        assert_eq!(snapshot_store.write_calls.get(), 2);
        Ok(())
    }

    #[test]
    fn persist_plan_missing_milestone_does_not_block_future_create(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();
        let missing_id = MilestoneId::new("missing-plan-test")?;

        let error = persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &missing_id,
            &sample_bundle("missing-plan-test", "Missing Plan Test"),
            now,
        )
        .expect_err("persisting a plan for a missing milestone must fail");
        assert!(
            error.to_string().contains("No such file") || error.to_string().contains("status.json")
        );

        let created = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: missing_id.to_string(),
                name: "Missing Plan Test".to_owned(),
                description: "creation should still work after a typoed persist_plan".to_owned(),
            },
            now,
        )?;
        assert_eq!(created.id, missing_id);
        Ok(())
    }

    #[test]
    fn materialize_bundle_refreshes_record_description_on_rematerialize(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let bundle = sample_bundle("materialize-refresh", "Materialize Refresh");
        let initial = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;
        assert_eq!(initial.description, "Test plan.");

        let mut updated_bundle = bundle.clone();
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        let refreshed = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(1),
        )?;

        assert_eq!(refreshed.description, "Updated milestone summary.");
        let loaded = load_milestone(&store, base, &initial.id)?;
        assert_eq!(loaded.description, "Updated milestone summary.");
        Ok(())
    }

    struct FailOnceRecordWriteStore {
        fail_next_record_write: AtomicBool,
    }

    impl FailOnceRecordWriteStore {
        fn new() -> Self {
            Self {
                fail_next_record_write: AtomicBool::new(true),
            }
        }
    }

    struct BlockingRecordWriteStore {
        entered_tx: Mutex<Option<mpsc::Sender<()>>>,
        release_rx: Mutex<Option<mpsc::Receiver<()>>>,
    }

    impl BlockingRecordWriteStore {
        fn new(entered_tx: mpsc::Sender<()>, release_rx: mpsc::Receiver<()>) -> Self {
            Self {
                entered_tx: Mutex::new(Some(entered_tx)),
                release_rx: Mutex::new(Some(release_rx)),
            }
        }
    }

    impl MilestoneStorePort for BlockingRecordWriteStore {
        fn milestone_exists(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<bool> {
            FsMilestoneStore.milestone_exists(base_dir, milestone_id)
        }

        fn read_milestone_record(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<MilestoneRecord> {
            FsMilestoneStore.read_milestone_record(base_dir, milestone_id)
        }

        fn write_milestone_record(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            record: &MilestoneRecord,
        ) -> AppResult<()> {
            if let Some(entered_tx) = self.entered_tx.lock().expect("lock entered_tx").take() {
                entered_tx.send(()).expect("signal blocked record write");
            }
            if let Some(release_rx) = self.release_rx.lock().expect("lock release_rx").take() {
                release_rx.recv().expect("release blocked record write");
            }

            FsMilestoneStore.write_milestone_record(base_dir, milestone_id, record)
        }

        fn list_milestone_ids(&self, base_dir: &Path) -> AppResult<Vec<MilestoneId>> {
            FsMilestoneStore.list_milestone_ids(base_dir)
        }

        fn create_milestone_atomic(
            &self,
            base_dir: &Path,
            record: &MilestoneRecord,
            snapshot: &MilestoneSnapshot,
            initial_journal_line: &str,
        ) -> AppResult<()> {
            FsMilestoneStore.create_milestone_atomic(
                base_dir,
                record,
                snapshot,
                initial_journal_line,
            )
        }
    }

    impl MilestoneStorePort for FailOnceRecordWriteStore {
        fn milestone_exists(&self, base_dir: &Path, milestone_id: &MilestoneId) -> AppResult<bool> {
            FsMilestoneStore.milestone_exists(base_dir, milestone_id)
        }

        fn read_milestone_record(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
        ) -> AppResult<MilestoneRecord> {
            FsMilestoneStore.read_milestone_record(base_dir, milestone_id)
        }

        fn write_milestone_record(
            &self,
            base_dir: &Path,
            milestone_id: &MilestoneId,
            record: &MilestoneRecord,
        ) -> AppResult<()> {
            if self.fail_next_record_write.swap(false, Ordering::SeqCst) {
                return Err(AppError::Io(std::io::Error::other(
                    "simulated milestone record write failure",
                )));
            }

            FsMilestoneStore.write_milestone_record(base_dir, milestone_id, record)
        }

        fn list_milestone_ids(&self, base_dir: &Path) -> AppResult<Vec<MilestoneId>> {
            FsMilestoneStore.list_milestone_ids(base_dir)
        }

        fn create_milestone_atomic(
            &self,
            base_dir: &Path,
            record: &MilestoneRecord,
            snapshot: &MilestoneSnapshot,
            initial_journal_line: &str,
        ) -> AppResult<()> {
            FsMilestoneStore.create_milestone_atomic(
                base_dir,
                record,
                snapshot,
                initial_journal_line,
            )
        }
    }

    #[test]
    fn materialize_bundle_retry_repairs_description_after_record_write_failure(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let failing_store = FailOnceRecordWriteStore::new();
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let bundle = sample_bundle("materialize-retry", "Materialize Retry");
        let initial = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;
        assert_eq!(initial.description, "Test plan.");

        let mut updated_bundle = bundle.clone();
        updated_bundle.executive_summary = "Retried summary repair.".to_owned();
        let error = materialize_bundle(
            &failing_store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(1),
        )
        .expect_err("first rematerialize should fail during milestone.toml rewrite");
        assert!(error
            .to_string()
            .contains("simulated milestone record write failure"));

        let stale_record = load_milestone(&store, base, &initial.id)?;
        assert_eq!(stale_record.description, "Test plan.");

        let repaired = materialize_bundle(
            &failing_store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(3),
        )?;
        assert_eq!(repaired.description, "Retried summary repair.");

        let loaded = load_milestone(&store, base, &initial.id)?;
        assert_eq!(loaded.description, "Retried summary repair.");

        let snapshot = load_snapshot(&snapshot_store, base, &initial.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        Ok(())
    }

    #[test]
    fn materialize_bundle_holds_write_lock_while_repairing_record_description(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let bundle = sample_bundle("materialize-lock", "Materialize Lock");
        let initial = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;
        assert_eq!(initial.description, "Test plan.");

        let mut first_update = bundle.clone();
        first_update.executive_summary = "First rematerialized summary.".to_owned();
        let mut second_update = bundle.clone();
        second_update.executive_summary = "Second rematerialized summary.".to_owned();

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let blocking_store = Arc::new(BlockingRecordWriteStore::new(entered_tx, release_rx));
        let base_dir = base.to_path_buf();

        let first_store = Arc::clone(&blocking_store);
        let first_snapshot_store = snapshot_store;
        let first_journal_store = journal_store;
        let first_plan_store = plan_store;
        let first_handle = std::thread::spawn(move || -> AppResult<MilestoneRecord> {
            materialize_bundle(
                first_store.as_ref(),
                &first_snapshot_store,
                &first_journal_store,
                &first_plan_store,
                &base_dir,
                &first_update,
                now + chrono::Duration::seconds(1),
            )
        });

        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first rematerialization should block while rewriting milestone.toml");

        let base_dir = base.to_path_buf();
        let (finished_tx, finished_rx) = mpsc::channel();
        let second_handle = std::thread::spawn(move || -> AppResult<MilestoneRecord> {
            let result = materialize_bundle(
                &FsMilestoneStore,
                &FsMilestoneSnapshotStore,
                &FsMilestoneJournalStore,
                &FsMilestonePlanStore,
                &base_dir,
                &second_update,
                now + chrono::Duration::seconds(2),
            );
            let _ = finished_tx.send(());
            result
        });

        assert!(
            finished_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "concurrent rematerialization should wait for the in-flight record repair lock"
        );

        release_tx
            .send(())
            .expect("release blocked record write under the lock");

        let first_record = first_handle
            .join()
            .expect("first rematerialization thread should join")?;
        assert_eq!(first_record.description, "First rematerialized summary.");

        let second_record = second_handle
            .join()
            .expect("second rematerialization thread should join")?;
        assert_eq!(second_record.description, "Second rematerialized summary.");

        let final_record = load_milestone(&store, base, &initial.id)?;
        assert_eq!(final_record.description, "Second rematerialized summary.");
        Ok(())
    }

    #[test]
    fn materialize_bundle_preserves_lineage_progress_on_rematerialize(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::contexts::milestone_record::bundle::BeadProposal;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle("materialize-progress", "Materialize Progress");
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            explicit_id: None,
            title: "Handle retry flow".to_owned(),
            description: Some("Fixture description.".to_owned()),
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec!["fixture".to_owned()],
            depends_on: vec![],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            explicit_id: None,
            title: "Document skip path".to_owned(),
            description: Some("Fixture description.".to_owned()),
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec!["fixture".to_owned()],
            depends_on: vec![],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        bundle.acceptance_map[0].covered_by = vec![
            "bead-1".to_owned(),
            "bead-2".to_owned(),
            "bead-3".to_owned(),
        ];

        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("first bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-2",
            "project-2",
            "run-2",
            "plan-v1",
            now + chrono::Duration::seconds(3),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-2",
            "project-2",
            "run-2",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("second bead failed"),
            now + chrono::Duration::seconds(3),
            now + chrono::Duration::seconds(4),
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-3",
            "project-3",
            "run-3",
            "plan-v1",
            now + chrono::Duration::seconds(5),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-3",
            "project-3",
            "run-3",
            Some("plan-v1"),
            TaskRunOutcome::Skipped,
            Some("third bead skipped"),
            now + chrono::Duration::seconds(5),
            now + chrono::Duration::seconds(6),
        )?;

        let snapshot_before = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_before.progress.total_beads, 3);
        assert_eq!(snapshot_before.progress.completed_beads, 1);
        assert_eq!(snapshot_before.progress.failed_beads, 1);
        assert_eq!(snapshot_before.progress.skipped_beads, 1);

        let mut updated_bundle = bundle.clone();
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(7),
        )?;

        let snapshot_after = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after.status, MilestoneStatus::Paused);
        assert_eq!(snapshot_after.progress.total_beads, 3);
        assert_eq!(snapshot_after.progress.completed_beads, 1);
        assert_eq!(snapshot_after.progress.failed_beads, 1);
        assert_eq!(snapshot_after.progress.skipped_beads, 1);
        Ok(())
    }

    #[test]
    fn materialize_bundle_preserves_progress_for_explicit_id_metadata_edits(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle("materialize-explicit-progress", "Explicit Progress");
        bundle.workstreams[0].beads[0].bead_id = Some("bead-1".to_owned());
        bundle.workstreams[0].beads[0].explicit_id = Some(true);
        bundle.workstreams[0].beads[0].description =
            Some("Original description for explicit bead.".to_owned());
        bundle.workstreams[0].description = Some("Original workstream copy.".to_owned());

        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("explicit bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        let mut updated_bundle = bundle.clone();
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        updated_bundle.workstreams[0].name = "Renamed workstream".to_owned();
        updated_bundle.workstreams[0].description = Some("Updated workstream copy.".to_owned());
        updated_bundle.workstreams[0].beads[0].title = "Renamed explicit bead".to_owned();
        updated_bundle.workstreams[0].beads[0].description =
            Some("Updated explicit bead copy.".to_owned());
        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(3),
        )?;

        let snapshot_after = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after.progress.total_beads, 1);
        assert_eq!(snapshot_after.progress.completed_beads, 1);
        assert_eq!(snapshot_after.progress.failed_beads, 0);
        assert_eq!(snapshot_after.progress.skipped_beads, 0);
        Ok(())
    }

    #[test]
    fn materialize_bundle_preserves_progress_for_legacy_plan_json_missing_explicit_id(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let bundle = sample_bundle("legacy-explicit-id", "Legacy Explicit Id");
        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("implicit bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        let milestone_root = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str());
        let mut legacy_plan_json: serde_json::Value =
            serde_json::from_str(&plan_store.read_plan_json(base, &record.id)?)?;
        let legacy_workstreams = legacy_plan_json["workstreams"]
            .as_array_mut()
            .expect("plan.json workstreams should be an array");
        for workstream in legacy_workstreams {
            let legacy_beads = workstream["beads"]
                .as_array_mut()
                .expect("plan.json workstream beads should be an array");
            for bead in legacy_beads {
                bead.as_object_mut()
                    .expect("plan.json bead should be an object")
                    .remove("explicit_id");
            }
        }
        std::fs::write(
            milestone_root.join("plan.json"),
            serde_json::to_string_pretty(&legacy_plan_json)?,
        )?;

        let mut updated_bundle = bundle.clone();
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(3),
        )?;

        let snapshot_after = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after.plan_version, 2);
        assert_eq!(snapshot_after.progress.total_beads, 1);
        assert_eq!(snapshot_after.progress.completed_beads, 1);
        assert_eq!(snapshot_after.progress.failed_beads, 0);
        assert_eq!(snapshot_after.progress.skipped_beads, 0);

        let persisted_bundle: crate::contexts::milestone_record::bundle::MilestoneBundle =
            serde_json::from_str(&plan_store.read_plan_json(base, &record.id)?)?;
        assert_eq!(
            persisted_bundle.workstreams[0].beads[0].explicit_id,
            Some(false)
        );
        Ok(())
    }

    #[test]
    fn materialize_bundle_preserves_progress_for_qualified_explicit_ids_with_flag(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle("qualified-explicit-id", "Qualified Explicit Id");
        bundle.workstreams[0].beads[0].bead_id = Some("qualified-explicit-id.bead-1".to_owned());
        bundle.workstreams[0].beads[0].explicit_id = Some(true);
        bundle.workstreams[0].beads[0].description =
            Some("Original explicit bead copy.".to_owned());
        bundle.workstreams[0].description = Some("Original workstream copy.".to_owned());

        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("qualified explicit bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        let mut updated_bundle = bundle.clone();
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        updated_bundle.workstreams[0].name = "Renamed workstream".to_owned();
        updated_bundle.workstreams[0].description = Some("Updated workstream copy.".to_owned());
        updated_bundle.workstreams[0].beads[0].title = "Renamed explicit bead".to_owned();
        updated_bundle.workstreams[0].beads[0].description =
            Some("Updated explicit bead copy.".to_owned());

        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(3),
        )?;

        let snapshot_after = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after.progress.total_beads, 1);
        assert_eq!(snapshot_after.progress.completed_beads, 1);
        assert_eq!(snapshot_after.progress.failed_beads, 0);
        assert_eq!(snapshot_after.progress.skipped_beads, 0);

        let persisted_bundle: crate::contexts::milestone_record::bundle::MilestoneBundle =
            serde_json::from_str(&plan_store.read_plan_json(base, &record.id)?)?;
        assert_eq!(
            persisted_bundle.workstreams[0].beads[0].explicit_id,
            Some(true)
        );
        Ok(())
    }

    #[test]
    fn materialize_bundle_resets_progress_when_plan_shape_changes_and_clears_old_lineage(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::contexts::milestone_record::bundle::BeadProposal;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle("materialize-progress-reset", "Materialize Progress Reset");
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            explicit_id: None,
            title: "Handle retry flow".to_owned(),
            description: Some("Fixture description.".to_owned()),
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec!["fixture".to_owned()],
            depends_on: vec![],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned(), "bead-2".to_owned()];

        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("first bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        let mut updated_bundle = bundle.clone();
        updated_bundle.workstreams[0].beads.remove(0);
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        updated_bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned()];
        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(3),
        )?;

        let snapshot_after = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after.status, MilestoneStatus::Paused);
        assert_eq!(snapshot_after.progress.total_beads, 1);
        assert_eq!(snapshot_after.progress.completed_beads, 0);
        assert_eq!(snapshot_after.progress.failed_beads, 0);
        assert_eq!(snapshot_after.progress.skipped_beads, 0);

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-2",
            "run-2",
            "plan-v2",
            now + chrono::Duration::seconds(4),
        )?;
        let snapshot_during_retry = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_during_retry.progress.completed_beads, 0);
        assert_eq!(snapshot_during_retry.progress.in_progress_beads, 1);

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-2",
            "run-2",
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("replacement bead completed"),
            now + chrono::Duration::seconds(4),
            now + chrono::Duration::seconds(5),
        )?;

        let snapshot_after_retry = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after_retry.progress.total_beads, 1);
        assert_eq!(snapshot_after_retry.progress.completed_beads, 1);
        assert_eq!(snapshot_after_retry.progress.failed_beads, 0);
        assert_eq!(snapshot_after_retry.progress.skipped_beads, 0);
        Ok(())
    }

    #[test]
    fn materialize_bundle_preserves_lineage_until_snapshot_update_succeeds_after_shape_change(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::contexts::milestone_record::bundle::BeadProposal;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let failing_snapshot_store = FailPlanVersionSnapshotWrite {
            blocked_plan_version: 2,
        };
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle("materialize-lineage-retry", "Materialize Lineage Retry");
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            explicit_id: None,
            title: "Handle retry flow".to_owned(),
            description: Some("Fixture description.".to_owned()),
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec!["fixture".to_owned()],
            depends_on: vec![],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned(), "bead-2".to_owned()];

        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("first bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        let mut updated_bundle = bundle.clone();
        updated_bundle.workstreams[0].beads.remove(0);
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        updated_bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned()];

        let error = materialize_bundle(
            &store,
            &failing_snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(3),
        )
        .expect_err("snapshot write failure should abort rematerialize");

        assert!(error
            .to_string()
            .contains("simulated snapshot write failure"));
        assert!(!lineage_store.read_task_runs(base, &record.id)?.is_empty());

        let snapshot_after_failure = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after_failure.plan_version, 1);
        assert_eq!(snapshot_after_failure.progress.completed_beads, 1);

        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(4),
        )?;

        let snapshot_after_retry = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after_retry.plan_version, 2);
        assert_eq!(snapshot_after_retry.progress.total_beads, 1);
        assert_eq!(snapshot_after_retry.progress.completed_beads, 0);
        assert_eq!(lineage_store.read_task_runs(base, &record.id)?.len(), 0);
        Ok(())
    }

    #[test]
    fn materialize_bundle_retries_lineage_clear_after_post_commit_truncation_failure(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::contexts::milestone_record::bundle::BeadProposal;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle("lineage-clear-retry", "Lineage Clear Retry");
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            explicit_id: None,
            title: "Replacement bead".to_owned(),
            description: Some("Fixture description.".to_owned()),
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec!["fixture".to_owned()],
            depends_on: vec![],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned(), "bead-2".to_owned()];
        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("first bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        let mut updated_bundle = bundle.clone();
        updated_bundle.workstreams[0].beads.remove(0);
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        updated_bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned()];

        let milestone_root = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str());
        let task_runs_path = milestone_root.join("task-runs.ndjson");
        let preserved_lineage = std::fs::read_to_string(&task_runs_path)?;

        std::fs::remove_file(&task_runs_path)?;
        std::fs::create_dir(&task_runs_path)?;

        let error = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(3),
        )
        .expect_err("lineage truncation failure should abort rematerialize");

        assert!(!error.to_string().is_empty());
        let snapshot_after_failure = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after_failure.plan_version, 2);
        assert_eq!(snapshot_after_failure.progress.total_beads, 1);
        assert_eq!(snapshot_after_failure.progress.completed_beads, 0);
        assert_eq!(
            snapshot_after_failure.pending_lineage_reset,
            Some(PendingLineageReset {
                plan_hash: snapshot_after_failure
                    .plan_hash
                    .clone()
                    .expect("plan hash must remain committed"),
                plan_version: 2,
            })
        );

        let stored_plan_shape: StoredPlanShape =
            serde_json::from_str(&plan_store.read_plan_shape(base, &record.id)?)?;
        assert!(stored_plan_shape.lineage_reset_required);
        let journal_after_failure = read_journal(&journal_store, base, &record.id)?;

        std::fs::remove_dir(&task_runs_path)?;
        std::fs::write(&task_runs_path, preserved_lineage)?;

        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(4),
        )?;

        let snapshot_after_retry = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after_retry.plan_version, 2);
        assert_eq!(snapshot_after_retry.progress.total_beads, 1);
        assert_eq!(snapshot_after_retry.progress.completed_beads, 0);
        assert_eq!(snapshot_after_retry.pending_lineage_reset, None);
        assert!(lineage_store.read_task_runs(base, &record.id)?.is_empty());

        let stored_plan_shape: StoredPlanShape =
            serde_json::from_str(&plan_store.read_plan_shape(base, &record.id)?)?;
        assert!(!stored_plan_shape.lineage_reset_required);
        assert_eq!(
            read_journal(&journal_store, base, &record.id)?.len(),
            journal_after_failure.len()
        );
        Ok(())
    }

    #[test]
    fn materialize_bundle_retries_pending_lineage_clear_with_missing_sidecar(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (tmp, record, updated_bundle, now) = setup_pending_lineage_reset_state(
            "lineage-clear-missing-sidecar",
            "Lineage Clear Missing Sidecar",
        )?;
        let base = tmp.path();
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;

        std::fs::remove_file(plan_shape_path(base, &record.id))?;

        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(4),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.plan_version, 2);
        assert_eq!(snapshot.pending_lineage_reset, None);
        assert_eq!(snapshot.progress.completed_beads, 0);
        assert!(lineage_store.read_task_runs(base, &record.id)?.is_empty());

        let stored_plan_shape: StoredPlanShape =
            serde_json::from_str(&plan_store.read_plan_shape(base, &record.id)?)?;
        assert!(!stored_plan_shape.lineage_reset_required);
        Ok(())
    }

    #[test]
    fn record_bead_start_clears_pending_lineage_reset_without_sidecar(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (tmp, record, _, now) = setup_pending_lineage_reset_state(
            "lineage-start-self-heal",
            "Lineage Start Self Heal",
        )?;
        let base = tmp.path();
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;

        std::fs::remove_file(plan_shape_path(base, &record.id))?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-2",
            "run-2",
            "plan-v2",
            now + chrono::Duration::seconds(4),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.pending_lineage_reset, None);
        assert_eq!(snapshot.progress.completed_beads, 0);
        assert_eq!(snapshot.progress.in_progress_beads, 1);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].project_id, "project-2");
        assert_eq!(runs[0].run_id.as_deref(), Some("run-2"));

        let stored_plan_shape: StoredPlanShape =
            serde_json::from_str(&std::fs::read_to_string(plan_shape_path(base, &record.id))?)?;
        assert!(!stored_plan_shape.lineage_reset_required);
        Ok(())
    }

    #[test]
    fn update_task_run_clears_pending_lineage_reset_before_rejecting_stale_completion(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (tmp, record, _, now) = setup_pending_lineage_reset_state(
            "lineage-update-self-heal",
            "Lineage Update Self Heal",
        )?;
        let base = tmp.path();
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;

        std::fs::write(plan_shape_path(base, &record.id), "{not valid json")?;

        let error = update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            now + chrono::Duration::seconds(1),
            TaskRunOutcome::Succeeded,
            Some("stale completion".to_owned()),
            now + chrono::Duration::seconds(4),
        )
        .expect_err("stale completions should be rejected after pending reset self-heals");
        assert!(!error.to_string().is_empty());

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.pending_lineage_reset, None);
        assert_eq!(snapshot.progress.completed_beads, 0);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert!(read_task_runs(&lineage_store, base, &record.id)?.is_empty());

        let stored_plan_shape: StoredPlanShape =
            serde_json::from_str(&std::fs::read_to_string(plan_shape_path(base, &record.id))?)?;
        assert!(!stored_plan_shape.lineage_reset_required);
        Ok(())
    }

    #[test]
    fn repair_task_run_clears_pending_lineage_reset_before_rejecting_stale_repair(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (tmp, record, _, now) = setup_pending_lineage_reset_state(
            "lineage-repair-self-heal",
            "Lineage Repair Self Heal",
        )?;
        let base = tmp.path();
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;

        std::fs::remove_file(plan_shape_path(base, &record.id))?;

        let error = repair_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            now + chrono::Duration::seconds(1),
            TaskRunOutcome::Succeeded,
            Some("stale repair".to_owned()),
            now + chrono::Duration::seconds(4),
        )
        .expect_err("stale repairs should be rejected after pending reset self-heals");
        assert!(!error.to_string().is_empty());

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.pending_lineage_reset, None);
        assert_eq!(snapshot.progress.completed_beads, 0);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert!(read_task_runs(&lineage_store, base, &record.id)?.is_empty());

        let stored_plan_shape: StoredPlanShape =
            serde_json::from_str(&std::fs::read_to_string(plan_shape_path(base, &record.id))?)?;
        assert!(!stored_plan_shape.lineage_reset_required);
        Ok(())
    }

    #[test]
    fn materialize_bundle_resets_progress_when_implicit_beads_reorder(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::contexts::milestone_record::bundle::BeadProposal;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle(
            "materialize-progress-reorder",
            "Materialize Progress Reorder",
        );
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            explicit_id: None,
            title: "Second implicit bead".to_owned(),
            description: Some("Runs after the first bead.".to_owned()),
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec!["second".to_owned()],
            depends_on: vec!["bead-1".to_owned()],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned(), "bead-2".to_owned()];

        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("first implicit bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        let mut reordered_bundle = bundle.clone();
        reordered_bundle.workstreams[0].beads.swap(0, 1);
        reordered_bundle.workstreams[0].beads[0].depends_on.clear();
        reordered_bundle.workstreams[0].beads[1].depends_on = vec!["bead-1".to_owned()];
        reordered_bundle.executive_summary = "Reordered milestone summary.".to_owned();
        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &reordered_bundle,
            now + chrono::Duration::seconds(3),
        )?;

        let snapshot_after = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot_after.progress.total_beads, 2);
        assert_eq!(snapshot_after.progress.completed_beads, 0);
        assert_eq!(snapshot_after.progress.failed_beads, 0);
        assert_eq!(snapshot_after.progress.skipped_beads, 0);
        Ok(())
    }

    #[test]
    fn materialize_bundle_rejects_invalid_bundle_before_writing_plan_files(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc::now();

        let mut bundle = sample_bundle("materialize-invalid", "Materialize Invalid");
        bundle.workstreams[0]
            .beads
            .push(crate::contexts::milestone_record::bundle::BeadProposal {
                bead_id: None,
                explicit_id: None,
                title: "Implicit duplicate".to_owned(),
                description: Some("Fixture description.".to_owned()),
                bead_type: Some("task".to_owned()),
                priority: Some(1),
                labels: vec!["fixture".to_owned()],
                depends_on: vec![],
                acceptance_criteria: vec!["AC-1".to_owned()],
                flow_override: None,
            });
        bundle.workstreams[0].beads[0].bead_id = Some("bead-2".to_owned());
        bundle.acceptance_map[0].covered_by = vec!["bead-2".to_owned(), "bead-2".to_owned()];

        let error = materialize_bundle(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            &bundle,
            now,
        )
        .unwrap_err();

        let rendered = format!("{error:?}");
        assert!(
            rendered.contains("duplicate bead identifier") || rendered.contains("duplicates bead")
        );
        assert!(!base
            .join(".ralph-burning/milestones/materialize-invalid")
            .exists());
        Ok(())
    }

    #[test]
    fn materialize_bundle_backfills_legacy_missing_covered_by_entries(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let mut bundle = sample_bundle("legacy-covered-by", "Legacy Covered By");
        bundle.acceptance_map[0].covered_by.clear();
        bundle.workstreams[0].beads[0].description = None;
        bundle.workstreams[0].beads[0].bead_type = None;
        bundle.workstreams[0].beads[0].priority = Some(0);
        bundle.workstreams[0].beads[0].labels.clear();

        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )
        .expect("legacy bundle should still materialize");

        let plan: MilestoneBundle =
            serde_json::from_str(&plan_store.read_plan_json(base, &record.id)?)?;
        assert_eq!(
            plan.acceptance_map[0].covered_by,
            vec!["legacy-covered-by.bead-1".to_owned()]
        );
        assert_eq!(plan.workstreams[0].beads[0].priority, Some(0));
        Ok(())
    }

    #[test]
    fn materialize_bundle_backfills_missing_plan_artifacts_when_hash_is_unchanged(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let bundle = sample_bundle("artifact-backfill", "Artifact Backfill");
        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;
        let snapshot_before = load_snapshot(&snapshot_store, base, &record.id)?;
        let journal_before = read_journal(&journal_store, base, &record.id)?;

        let milestone_root = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str());
        std::fs::remove_file(milestone_root.join("plan.md"))?;
        std::fs::remove_file(milestone_root.join("plan.shape.json"))?;

        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now + chrono::Duration::seconds(1),
        )?;
        let snapshot_after = load_snapshot(&snapshot_store, base, &record.id)?;
        let journal_after = read_journal(&journal_store, base, &record.id)?;

        let plan_md = plan_store.read_plan_md(base, &record.id)?;
        assert!(plan_md.contains("## Acceptance Criteria"));
        assert!(milestone_root.join("plan.shape.json").is_file());
        assert_eq!(snapshot_after.plan_version, snapshot_before.plan_version);
        assert_eq!(snapshot_after.updated_at, snapshot_before.updated_at);
        assert_eq!(journal_after.len(), journal_before.len());
        Ok(())
    }

    #[test]
    fn materialize_bundle_recomputes_shape_from_plan_json_when_sidecar_signature_is_corrupt(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle("shape-signature-corrupt", "Shape Signature Corrupt");
        bundle.workstreams[0].beads[0].bead_id = Some("bead-1".to_owned());
        bundle.workstreams[0].beads[0].explicit_id = Some(true);
        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("explicit bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        let milestone_root = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str());
        let existing_plan_hash = hash_text(&plan_store.read_plan_json(base, &record.id)?);
        let corrupt_shape =
            render_plan_shape_artifact(existing_plan_hash.as_str(), "{\"beads\":[]}")?;
        std::fs::write(milestone_root.join("plan.shape.json"), corrupt_shape)?;

        let mut updated_bundle = bundle.clone();
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        updated_bundle.workstreams[0].name = "Renamed workstream".to_owned();
        updated_bundle.workstreams[0].beads[0].title = "Renamed explicit bead".to_owned();

        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(3),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.plan_version, 2);
        assert_eq!(snapshot.progress.completed_beads, 1);
        let expected_plan_json = render_plan_json(&updated_bundle)?;
        let expected_plan_hash = hash_text(&expected_plan_json);
        let expected_shape = progress_shape_signature(&updated_bundle)
            .map_err(|errors| std::io::Error::other(errors.join("; ")))?;
        let expected_plan_shape =
            render_plan_shape_artifact(expected_plan_hash.as_str(), expected_shape.as_str())?;
        assert_eq!(
            plan_store.read_plan_shape(base, &record.id)?,
            expected_plan_shape
        );
        Ok(())
    }

    #[test]
    fn materialize_bundle_repairs_stale_plan_shape_sidecar_after_partial_plan_write(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle("shape-sidecar-repair", "Shape Sidecar Repair");
        bundle.workstreams[0].beads[0].bead_id = Some("bead-1".to_owned());
        bundle.workstreams[0].beads[0].explicit_id = Some(true);
        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("first bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        let mut updated_bundle = bundle.clone();
        updated_bundle.executive_summary = "Updated milestone summary.".to_owned();
        updated_bundle.workstreams[0].name = "Renamed workstream".to_owned();
        updated_bundle.workstreams[0].beads[0].title = "Renamed explicit bead".to_owned();

        let milestone_root = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str());
        let expected_plan_json = render_plan_json(&updated_bundle)?;
        let expected_plan_md = render_plan_md_checked(&updated_bundle)
            .map_err(|errors| std::io::Error::other(errors.join("; ")))?;
        std::fs::write(milestone_root.join("plan.json"), &expected_plan_json)?;
        std::fs::write(milestone_root.join("plan.md"), &expected_plan_md)?;

        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &updated_bundle,
            now + chrono::Duration::seconds(3),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.plan_version, 2);
        assert_eq!(snapshot.progress.completed_beads, 1);
        let expected_plan_hash = hash_text(&expected_plan_json);
        let expected_shape = progress_shape_signature(&updated_bundle)
            .map_err(|errors| std::io::Error::other(errors.join("; ")))?;
        let expected_plan_shape =
            render_plan_shape_artifact(expected_plan_hash.as_str(), expected_shape.as_str())?;
        assert_eq!(
            plan_store.read_plan_shape(base, &record.id)?,
            expected_plan_shape
        );
        Ok(())
    }

    #[test]
    fn materialize_bundle_clears_progress_when_only_uncommitted_plan_shape_matches(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::contexts::milestone_record::bundle::BeadProposal;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let mut bundle = sample_bundle("uncommitted-shape", "Uncommitted Shape");
        bundle.workstreams[0].beads[0].bead_id = Some("bead-1".to_owned());
        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("original bead completed"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(2),
        )?;

        let mut staged_bundle = bundle.clone();
        staged_bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            explicit_id: None,
            title: "Uncommitted extra bead".to_owned(),
            description: Some("Only written during a crashed persist.".to_owned()),
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec!["staged".to_owned()],
            depends_on: vec!["bead-1".to_owned()],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        staged_bundle.acceptance_map[0].covered_by = vec!["bead-1".to_owned(), "bead-2".to_owned()];
        let staged_plan_json = render_plan_json(&staged_bundle)?;
        let staged_plan_hash = hash_text(&staged_plan_json);
        let staged_plan_shape = render_plan_shape_artifact(
            staged_plan_hash.as_str(),
            progress_shape_signature(&staged_bundle)
                .map_err(|errors| std::io::Error::other(errors.join("; ")))?
                .as_str(),
        )?;

        let milestone_root = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str());
        std::fs::write(milestone_root.join("plan.json"), staged_plan_json)?;
        std::fs::write(milestone_root.join("plan.shape.json"), staged_plan_shape)?;

        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &staged_bundle,
            now + chrono::Duration::seconds(3),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.plan_version, 2);
        assert_eq!(snapshot.progress.total_beads, 2);
        assert_eq!(snapshot.progress.completed_beads, 0);
        assert_eq!(lineage_store.read_task_runs(base, &record.id)?.len(), 0);
        Ok(())
    }

    #[test]
    fn bead_start_and_completion_tracking() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "bead-track",
            "Bead Tracking",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "abc123",
            now,
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Running);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(snapshot.progress.in_progress_beads, 1);

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("abc123"),
            TaskRunOutcome::Succeeded,
            Some("All tests passed"),
            now,
            now,
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Paused);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);
        assert_eq!(snapshot.progress.skipped_beads, 0);

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].milestone_id, record.id.to_string());
        assert_eq!(runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(runs[0].plan_hash.as_deref(), Some("abc123"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(runs[0].outcome_detail.as_deref(), Some("All tests passed"));

        let journal = read_journal(&journal_store, base, &record.id)?;
        assert_eq!(
            journal.last().map(|event| event.event_type),
            Some(MilestoneEventType::BeadCompleted)
        );
        Ok(())
    }

    #[test]
    fn paused_milestone_completion_reconciles_to_completed_with_bridge_events(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();
        let bundle = sample_bundle("paused-completion-test", "Paused Completion Test");

        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now,
        )?;
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Paused,
            now + chrono::Duration::seconds(1),
        )?;

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("completed while paused"),
            now,
            now + chrono::Duration::seconds(2),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Completed);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);

        let transitions: Vec<_> = read_journal(&journal_store, base, &record.id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::StatusChanged)
            .collect();
        assert!(transitions.iter().any(|event| {
            event.from_state == Some(MilestoneStatus::Paused)
                && event.to_state == Some(MilestoneStatus::Running)
                && event.timestamp == now + chrono::Duration::seconds(2)
        }));
        assert!(transitions.iter().any(|event| {
            event.from_state == Some(MilestoneStatus::Running)
                && event.to_state == Some(MilestoneStatus::Completed)
                && event.timestamp == now + chrono::Duration::seconds(2)
        }));
        Ok(())
    }

    #[test]
    fn unrecoverable_completion_marks_milestone_failed_and_emits_failed_transition(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let started_at = Utc::now();
        let failed_at = started_at + chrono::Duration::seconds(5);

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "unrecoverable-completion-test",
            "Unrecoverable Completion Test",
            started_at,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            started_at,
        )?;
        record_bead_completion_with_disposition(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("fatal controller failure"),
            started_at,
            failed_at,
            CompletionMilestoneDisposition::MarkMilestoneFailed,
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Failed);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.failed_beads, 1);

        let journal = read_journal(&journal_store, base, &record.id)?;
        assert!(journal.iter().any(|event| {
            event.event_type == MilestoneEventType::StatusChanged
                && event.from_state == Some(MilestoneStatus::Running)
                && event.to_state == Some(MilestoneStatus::Failed)
                && event.timestamp == failed_at
        }));
        assert!(journal.iter().any(|event| {
            event.event_type == MilestoneEventType::BeadFailed && event.timestamp == failed_at
        }));
        Ok(())
    }

    #[test]
    fn repairing_failed_completion_can_promote_paused_snapshot_to_failed(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let started_at = Utc::now();
        let failed_at = started_at + chrono::Duration::seconds(5);
        let repaired_at = failed_at + chrono::Duration::seconds(2);

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "failed-repair-promotion-test",
            "Failed Repair Promotion Test",
            started_at,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            started_at,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("retryable failure"),
            started_at,
            failed_at,
        )?;

        let paused_snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(paused_snapshot.status, MilestoneStatus::Paused);

        repair_task_run_with_disposition(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            started_at,
            TaskRunOutcome::Failed,
            Some("fatal controller failure".to_owned()),
            repaired_at,
            CompletionMilestoneDisposition::MarkMilestoneFailed,
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Failed);

        let journal = read_journal(&journal_store, base, &record.id)?;
        let resumed_event = journal
            .iter()
            .find(|event| {
                event.event_type == MilestoneEventType::StatusChanged
                    && event.from_state == Some(MilestoneStatus::Paused)
                    && event.to_state == Some(MilestoneStatus::Running)
            })
            .expect("repair should synthesize a paused -> running bridge");
        assert_eq!(resumed_event.timestamp, repaired_at);

        let failed_event = journal
            .iter()
            .find(|event| {
                event.event_type == MilestoneEventType::StatusChanged
                    && event.from_state == Some(MilestoneStatus::Running)
                    && event.to_state == Some(MilestoneStatus::Failed)
                    && event.timestamp == repaired_at
            })
            .expect("repair should record the terminal failed transition");
        let metadata = failed_event
            .metadata
            .as_ref()
            .expect("failed transition should carry metadata");
        assert_eq!(
            metadata.get("duration_seconds"),
            Some(&serde_json::json!(5))
        );
        Ok(())
    }

    #[test]
    fn terminal_milestone_rejects_bead_start_without_mutating_lineage(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "terminal-start-test",
            "Terminal Start Test",
            now,
        )?;

        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Running,
            now + chrono::Duration::seconds(2),
        )?;
        let mut snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot.progress.completed_beads = snapshot.progress.total_beads;
        snapshot_store.write_snapshot(base, &record.id, &snapshot)?;
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Completed,
            now + chrono::Duration::seconds(3),
        )?;

        let error = record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(4),
        )
        .expect_err("terminal milestones must reject new bead starts");
        assert!(error.to_string().contains("already completed"));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Completed);
        assert_eq!(snapshot.active_bead, None);

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert!(runs.is_empty());

        let journal = read_journal(&journal_store, base, &record.id)?;
        assert!(journal
            .iter()
            .all(|event| event.event_type != MilestoneEventType::BeadStarted));
        Ok(())
    }

    #[test]
    fn bead_start_requires_finalized_plan() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "start-without-plan".to_owned(),
                name: "Start Without Plan".to_owned(),
                description: "reject bead starts before plan export".to_owned(),
            },
            now,
        )?;

        let error = record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )
        .expect_err("bead start without a finalized plan must fail");
        assert!(error
            .to_string()
            .contains("before a plan is finalized and exported"));
        assert!(read_task_runs(&lineage_store, base, &record.id)?.is_empty());
        Ok(())
    }

    #[test]
    fn materialize_bundle_preserves_terminal_status() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let bundle = sample_bundle("terminal-materialize", "Terminal Materialize");
        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Running,
            now + chrono::Duration::seconds(1),
        )?;
        let mut snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot.progress.completed_beads = snapshot.progress.total_beads;
        snapshot_store.write_snapshot(base, &record.id, &snapshot)?;
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Completed,
            now + chrono::Duration::seconds(2),
        )?;

        let refreshed = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now + chrono::Duration::seconds(3),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Completed);
        assert_eq!(refreshed.id, record.id);
        Ok(())
    }

    #[test]
    fn materialize_bundle_reopens_terminal_milestone_when_plan_changes(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::contexts::milestone_record::bundle::BeadProposal;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let bundle = sample_bundle("terminal-plan-change", "Terminal Plan Change");
        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Running,
            now + chrono::Duration::seconds(1),
        )?;
        let mut snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot.progress.completed_beads = snapshot.progress.total_beads;
        snapshot_store.write_snapshot(base, &record.id, &snapshot)?;
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Completed,
            now + chrono::Duration::seconds(2),
        )?;

        let mut changed_bundle = bundle.clone();
        changed_bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            explicit_id: None,
            title: "Second bead after rematerialize".to_owned(),
            description: Some("Reopens the terminal milestone.".to_owned()),
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec!["fixture".to_owned()],
            depends_on: vec!["bead-1".to_owned()],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        changed_bundle.acceptance_map[0].covered_by =
            vec!["bead-1".to_owned(), "bead-2".to_owned()];
        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &changed_bundle,
            now + chrono::Duration::seconds(3),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.plan_version, 2);
        assert_eq!(snapshot.progress.total_beads, 2);
        assert_eq!(snapshot.progress.completed_beads, 0);

        let journal = read_journal(&journal_store, base, &record.id)?;
        let reopen_event = journal
            .iter()
            .find(|event| {
                event.from_state == Some(MilestoneStatus::Completed)
                    && event.to_state == Some(MilestoneStatus::Planning)
            })
            .expect("terminal rematerialization should emit a reopen transition");
        assert_eq!(
            reopen_event.reason.as_deref(),
            Some("plan changed, milestone reopened")
        );
        Ok(())
    }

    #[test]
    fn completed_transition_after_reopen_ignores_prior_plan_runtime(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use crate::contexts::milestone_record::bundle::BeadProposal;

        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let now = Utc::now();

        let bundle = sample_bundle("reopen-runtime-reset", "Reopen Runtime Reset");
        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Running,
            now + chrono::Duration::seconds(1),
        )?;
        let mut snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot.progress.completed_beads = snapshot.progress.total_beads;
        snapshot_store.write_snapshot(base, &record.id, &snapshot)?;
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Completed,
            now + chrono::Duration::seconds(11),
        )?;

        let mut changed_bundle = bundle.clone();
        changed_bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            explicit_id: None,
            title: "Second bead after reopen".to_owned(),
            description: Some("Forces a new execution plan.".to_owned()),
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec!["fixture".to_owned()],
            depends_on: vec!["bead-1".to_owned()],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        changed_bundle.acceptance_map[0].covered_by =
            vec!["bead-1".to_owned(), "bead-2".to_owned()];
        materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &changed_bundle,
            now + chrono::Duration::seconds(20),
        )?;

        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Running,
            now + chrono::Duration::seconds(25),
        )?;
        let mut reopened_snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        reopened_snapshot.progress.completed_beads = reopened_snapshot.progress.total_beads;
        snapshot_store.write_snapshot(base, &record.id, &reopened_snapshot)?;
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Completed,
            now + chrono::Duration::seconds(31),
        )?;

        let completed_event = read_journal(&journal_store, base, &record.id)?
            .into_iter()
            .rev()
            .find(|event| event.to_state == Some(MilestoneStatus::Completed))
            .expect("reopened milestone should emit a completed transition");
        let metadata = completed_event
            .metadata
            .as_ref()
            .expect("completed transition should carry metadata");
        assert_eq!(metadata.get("plan_version"), Some(&serde_json::json!(2)));
        assert_eq!(
            metadata.get("duration_seconds"),
            Some(&serde_json::json!(6))
        );
        Ok(())
    }

    #[test]
    fn task_run_entry_serialization_with_new_fields() -> Result<(), Box<dyn std::error::Error>> {
        let now = Utc::now();
        let entry = TaskRunEntry {
            milestone_id: "ms-1".to_owned(),
            bead_id: "bead-1".to_owned(),
            project_id: "proj-1".to_owned(),
            run_id: Some("run-42".to_owned()),
            plan_hash: Some("sha256-abc".to_owned()),
            outcome: TaskRunOutcome::Succeeded,
            outcome_detail: Some("Completed with 3 warnings".to_owned()),
            started_at: now,
            finished_at: Some(now),
            task_id: None,
        };
        let json = serde_json::to_string(&entry)?;
        let parsed: TaskRunEntry = serde_json::from_str(&json)?;
        assert_eq!(parsed.milestone_id, "ms-1");
        assert_eq!(parsed.run_id.as_deref(), Some("run-42"));
        assert_eq!(parsed.plan_hash.as_deref(), Some("sha256-abc"));
        assert_eq!(
            parsed.outcome_detail.as_deref(),
            Some("Completed with 3 warnings")
        );
        Ok(())
    }

    #[test]
    fn append_task_run_persists_milestone_id_for_new_entries(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "append-run-milestone-id-test".to_owned(),
                name: "Append Run Milestone ID Test".to_owned(),
                description: "testing append_task_run milestone_id persistence".to_owned(),
            },
            now,
        )?;

        lineage_store.append_task_run(
            base,
            &record.id,
            &TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now,
                finished_at: None,
                task_id: None,
            },
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        let raw = std::fs::read_to_string(task_runs_path)?;
        assert!(raw.contains(&format!(r#""milestone_id":"{}""#, record.id)));
        Ok(())
    }

    #[test]
    fn misfiled_task_run_rows_are_rejected_by_queries_and_reconciliation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "misfiled-task-run-test".to_owned(),
                name: "Misfiled Task Run Test".to_owned(),
                description: "reject rows whose milestone_id does not match the file path"
                    .to_owned(),
            },
            now,
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        std::fs::write(
            &task_runs_path,
            format!(
                "{}\n",
                serde_json::to_string(&TaskRunEntry {
                    milestone_id: "other-milestone".to_owned(),
                    bead_id: "bead-1".to_owned(),
                    project_id: "project-1".to_owned(),
                    run_id: None,
                    plan_hash: None,
                    outcome: TaskRunOutcome::Running,
                    outcome_detail: None,
                    started_at: now,
                    finished_at: None,
                    task_id: None,
                })?
            ),
        )?;

        let query_error = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")
            .expect_err("misfiled rows must fail bead queries");
        assert!(query_error
            .to_string()
            .contains("does not match milestone path"));

        let start_error = record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-2",
            "project-2",
            "run-2",
            "plan-v2",
            now + chrono::Duration::seconds(1),
        )
        .expect_err("snapshot reconciliation must reject misfiled lineage rows");
        assert!(start_error
            .to_string()
            .contains("does not match milestone path"));
        Ok(())
    }

    #[test]
    fn find_runs_for_bead_filters_correctly() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "find-bead-test",
            "Find Bead Test",
            now,
        )?;

        // Start and complete bead-1
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-hash",
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            None,
            TaskRunOutcome::Failed,
            Some("Build error"),
            now,
            now,
        )?;

        // Start and complete bead-2 (different bead)
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-2",
            "project-2",
            "run-id",
            "plan-hash",
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-2",
            "project-2",
            "run-id",
            None,
            TaskRunOutcome::Succeeded,
            None,
            now,
            now,
        )?;

        // Retry bead-1 (second attempt)
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-3",
            "run-2",
            "plan-hash",
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-3",
            "run-2",
            None,
            TaskRunOutcome::Succeeded,
            Some("Passed on retry"),
            now,
            now,
        )?;

        // find_runs_for_bead should return only bead-1 entries
        let bead1_runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(bead1_runs.len(), 2);
        for run in &bead1_runs {
            assert_eq!(run.milestone_id, record.id.to_string());
            assert_eq!(run.bead_id, "bead-1");
        }
        assert_eq!(bead1_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(bead1_runs[1].outcome, TaskRunOutcome::Succeeded);

        // bead-2 should have exactly 1 entry
        let bead2_runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-2")?;
        assert_eq!(bead2_runs.len(), 1);
        assert_eq!(bead2_runs[0].outcome, TaskRunOutcome::Succeeded);

        // Non-existent bead should return empty
        let empty_runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-999")?;
        assert!(empty_runs.is_empty());

        Ok(())
    }

    #[test]
    fn find_runs_for_bead_collapses_legacy_start_completion_pairs(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "legacy-query-test".to_owned(),
                name: "Legacy Query Test".to_owned(),
                description: "testing legacy task-run query collapse".to_owned(),
            },
            now,
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        let first_started = now;
        let second_started = now + chrono::Duration::seconds(10);
        let legacy_lines = [
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: first_started,
                finished_at: None,
                task_id: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("legacy failure".to_owned()),
                started_at: first_started,
                finished_at: Some(first_started + chrono::Duration::seconds(5)),
                task_id: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-2".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: second_started,
                finished_at: None,
                task_id: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-2".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: Some("legacy success".to_owned()),
                started_at: second_started,
                finished_at: Some(second_started + chrono::Duration::seconds(5)),
                task_id: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-2".to_owned(),
                project_id: "other-project".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now + chrono::Duration::seconds(20),
                finished_at: None,
                task_id: None,
            })?,
        ]
        .join("\n");
        std::fs::write(&task_runs_path, format!("{legacy_lines}\n"))?;

        let runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(runs[0].outcome_detail.as_deref(), Some("legacy failure"));
        assert_eq!(runs[1].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(runs[1].outcome_detail.as_deref(), Some("legacy success"));
        assert!(runs.iter().all(|run| run.outcome.is_terminal()));
        assert!(runs
            .iter()
            .all(|run| run.milestone_id == record.id.to_string()));
        Ok(())
    }

    #[test]
    fn find_runs_for_bead_deduplicates_replayed_terminal_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "duplicate-terminal-query-test".to_owned(),
                name: "Duplicate Terminal Query Test".to_owned(),
                description: "dedupe replayed completion rows in bead audit queries".to_owned(),
            },
            now,
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        let started_at = now;
        let finished_at = now + chrono::Duration::seconds(5);
        let duplicated_lines = [
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
                task_id: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: None,
                started_at,
                finished_at: Some(finished_at),
                task_id: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: None,
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: Some("replayed completion".to_owned()),
                started_at,
                finished_at: Some(finished_at),
                task_id: None,
            })?,
        ]
        .join("\n");
        std::fs::write(&task_runs_path, format!("{duplicated_lines}\n"))?;

        let runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(
            runs[0].outcome_detail.as_deref(),
            Some("replayed completion")
        );
        Ok(())
    }

    #[test]
    fn find_runs_for_bead_collapses_short_and_qualified_refs_for_same_attempt(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "9ni".to_owned(),
                name: "Mixed Bead Ref Query Test".to_owned(),
                description: "short and qualified bead refs should collapse to one attempt"
                    .to_owned(),
            },
            now,
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        let finished_at = now + chrono::Duration::seconds(5);
        let raw_lines = [
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "8.5.3".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now,
                finished_at: None,
                task_id: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: format!("{}.8.5.3", record.id),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: Some("completed".to_owned()),
                started_at: now,
                finished_at: Some(finished_at),
                task_id: None,
            })?,
        ]
        .join("\n");
        std::fs::write(&task_runs_path, format!("{raw_lines}\n"))?;

        let runs = find_runs_for_bead(&lineage_store, base, &record.id, "8.5.3")?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].bead_id, format!("{}.8.5.3", record.id));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(runs[0].outcome_detail.as_deref(), Some("completed"));
        Ok(())
    }

    #[test]
    fn find_runs_for_bead_preserves_same_timestamp_legacy_retries(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "same-timestamp-legacy-query-test".to_owned(),
                name: "Same Timestamp Legacy Query Test".to_owned(),
                description: "same-timestamp legacy retries stay visible in bead audit queries"
                    .to_owned(),
            },
            now,
        )?;

        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        let started_at = now;
        let raw_lines = [
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
                task_id: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("first retry".to_owned()),
                started_at,
                finished_at: Some(started_at + chrono::Duration::seconds(1)),
                task_id: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
                task_id: None,
            })?,
            serde_json::to_string(&TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("second retry".to_owned()),
                started_at,
                finished_at: Some(started_at + chrono::Duration::seconds(2)),
                task_id: None,
            })?,
        ]
        .join("\n");
        std::fs::write(&task_runs_path, format!("{raw_lines}\n"))?;

        let runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].outcome_detail.as_deref(), Some("first retry"));
        assert_eq!(runs[1].outcome_detail.as_deref(), Some("second retry"));
        Ok(())
    }

    #[test]
    fn update_task_run_modifies_outcome() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "update-run-test",
            "Update Run Test",
            now,
        )?;

        // Start bead-1 (appends a Running entry)
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now,
        )?;

        // Update the running entry via update_task_run
        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            now,
            TaskRunOutcome::Succeeded,
            Some("All checks passed".to_owned()),
            now,
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Paused);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].milestone_id, record.id.to_string());
        assert_eq!(runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(runs[0].outcome_detail.as_deref(), Some("All checks passed"));
        assert!(runs[0].finished_at.is_some());

        let journal = read_journal(&journal_store, base, &record.id)?;
        assert_eq!(
            journal.last().map(|event| event.event_type),
            Some(MilestoneEventType::BeadCompleted)
        );
        Ok(())
    }

    #[test]
    fn record_bead_start_reopens_failed_attempt_across_short_and_qualified_bead_refs(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "mixed-ref-reopen-test",
            "Mixed Ref Reopen Test",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("first attempt failed"),
            now,
            now + chrono::Duration::seconds(5),
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            &format!("{}.bead-1", record.id),
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(10),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 2);
        assert!(runs.iter().any(|entry| {
            entry.outcome == TaskRunOutcome::Failed
                && entry.run_id.as_deref() == Some("run-1")
                && entry.started_at == now
        }));
        assert!(runs.iter().any(|entry| {
            entry.outcome == TaskRunOutcome::Running
                && entry.run_id.as_deref() == Some("run-1")
                && entry.started_at == now + chrono::Duration::seconds(10)
        }));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Running);
        assert_eq!(snapshot.progress.in_progress_beads, 1);
        assert_eq!(snapshot.progress.failed_beads, 0);
        assert!(snapshot.active_bead.is_some());
        Ok(())
    }

    #[test]
    fn update_task_run_missing_milestone_does_not_block_future_create(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();
        let missing_id = MilestoneId::new("missing-update-test")?;

        let error = update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &missing_id,
            "bead-1",
            "project-1",
            "run-id",
            None,
            now,
            TaskRunOutcome::Succeeded,
            Some("missing milestone".to_owned()),
            now,
        )
        .expect_err("updating a missing milestone must fail");
        assert!(
            error.to_string().contains("No such file") || error.to_string().contains("status.json")
        );

        let created = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: missing_id.to_string(),
                name: "Missing Update Test".to_owned(),
                description: "creation should still work after a typoed update_task_run".to_owned(),
            },
            now,
        )?;
        assert_eq!(created.id, missing_id);
        Ok(())
    }

    #[test]
    fn completion_rejects_conflicting_plan_hash() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "completion-plan-conflict-test",
            "Completion Plan Conflict Test",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now,
        )?;

        let error = record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("should fail"),
            now,
            now + chrono::Duration::seconds(5),
        )
        .expect_err("conflicting plan hashes must be rejected");
        assert!(error.to_string().contains("conflicting plan_hash"));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Running);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(snapshot.progress.in_progress_beads, 1);
        assert_eq!(snapshot.progress.completed_beads, 0);

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Running);
        assert!(runs[0].finished_at.is_none());

        let journal = read_journal(&journal_store, base, &record.id)?;
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| {
                matches!(
                    event.event_type,
                    MilestoneEventType::BeadCompleted
                        | MilestoneEventType::BeadFailed
                        | MilestoneEventType::BeadSkipped
                )
            })
            .collect();
        assert!(completion_events.is_empty());
        Ok(())
    }

    #[test]
    fn multiple_retries_visible_for_same_bead() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "retry-test",
            "Retry Test",
            now,
        )?;

        // Attempt 1: fail
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-a",
            "run-1",
            "plan-v1",
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-a",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("Compile error"),
            now,
            now,
        )?;

        // Attempt 2: succeed
        let retry_started = now + chrono::Duration::seconds(10);
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-b",
            "run-2",
            "plan-v2",
            retry_started,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-b",
            "run-2",
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("All green"),
            retry_started,
            retry_started + chrono::Duration::seconds(1),
        )?;

        let bead1_runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(bead1_runs.len(), 2);
        assert_eq!(bead1_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(bead1_runs[0].plan_hash.as_deref(), Some("plan-v1"));
        // Second attempt succeeded
        assert_eq!(bead1_runs[1].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(bead1_runs[1].plan_hash.as_deref(), Some("plan-v2"));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Paused);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);
        assert_eq!(snapshot.progress.failed_beads, 0);
        assert_eq!(snapshot.progress.skipped_beads, 0);
        Ok(())
    }

    #[test]
    fn same_timestamp_retry_keeps_snapshot_active() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "same-timestamp-retry-state-test",
            "Same Timestamp Retry State Test",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-a",
            "run-1",
            "plan-v1",
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-a",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("first attempt failed"),
            now,
            now + chrono::Duration::seconds(1),
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-b",
            "run-2",
            "plan-v2",
            now,
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Running);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(snapshot.progress.in_progress_beads, 1);
        assert_eq!(snapshot.progress.failed_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 0);

        let bead_runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(bead_runs.len(), 2);
        assert!(bead_runs
            .iter()
            .any(|run| run.run_id.as_deref() == Some("run-1")
                && run.outcome == TaskRunOutcome::Failed));
        assert!(bead_runs
            .iter()
            .any(|run| run.run_id.as_deref() == Some("run-2")
                && run.outcome == TaskRunOutcome::Running));
        Ok(())
    }

    #[test]
    fn completion_repair_moves_stale_planning_snapshot_to_ready(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "planning-repair-test".to_owned(),
                name: "Planning Repair Test".to_owned(),
                description: "testing stale planning snapshot repair after completion".to_owned(),
            },
            now,
        )?;

        lineage_store.append_task_run(
            base,
            &record.id,
            &TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: now,
                finished_at: None,
                task_id: None,
            },
        )?;

        let stale_snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(stale_snapshot.status, MilestoneStatus::Planning);

        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            now,
            TaskRunOutcome::Succeeded,
            Some("planning snapshot repaired".to_owned()),
            now + chrono::Duration::seconds(5),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Ready);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);
        Ok(())
    }

    #[test]
    fn skipped_runs_are_tracked_without_counting_as_failures(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "skip-test",
            "Skip Test",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-skip",
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-skip"),
            TaskRunOutcome::Skipped,
            Some("Handled by another milestone"),
            now,
            now,
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Paused);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.failed_beads, 0);
        assert_eq!(snapshot.progress.skipped_beads, 1);

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Skipped);

        let journal = read_journal(&journal_store, base, &record.id)?;
        assert_eq!(
            journal.last().map(|event| event.event_type),
            Some(MilestoneEventType::BeadSkipped)
        );
        Ok(())
    }

    #[test]
    fn completion_flow_rejects_duplicate_finalization() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "duplicate-finalize-test",
            "Duplicate Finalize Test",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-hash",
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            None,
            TaskRunOutcome::Succeeded,
            Some("first completion"),
            now,
            now,
        )?;

        let error = update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            None,
            now,
            TaskRunOutcome::Failed,
            Some("second completion".to_owned()),
            now,
        )
        .expect_err("duplicate terminal updates should fail");
        assert!(error.to_string().contains("already finalized"));
        assert!(error.to_string().contains("run=run-1"));
        Ok(())
    }

    #[test]
    fn update_task_run_accepts_short_and_qualified_bead_refs_for_same_attempt(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "mixed-ref-finalize-test",
            "Mixed Ref Finalize Test",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now,
        )?;

        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            &format!("{}.bead-1", record.id),
            "project-1",
            "run-1",
            Some("plan-v1"),
            now,
            TaskRunOutcome::Succeeded,
            Some("completed through qualified ref".to_owned()),
            now + chrono::Duration::seconds(5),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(
            runs[0].outcome_detail.as_deref(),
            Some("completed through qualified ref")
        );

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.progress.completed_beads, 1);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.active_bead, None);
        Ok(())
    }

    #[test]
    fn update_task_run_rejects_stale_same_run_id_completion_after_retry_reopen(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let first_started_at = Utc::now();
        let second_started_at = first_started_at + chrono::Duration::seconds(30);

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "stale-same-run-id-finalize-test",
            "Stale Same Run Id Finalize Test",
            first_started_at,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            first_started_at,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("first failure"),
            first_started_at,
            first_started_at + chrono::Duration::seconds(5),
        )?;
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            second_started_at,
        )?;

        let error = update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            first_started_at,
            TaskRunOutcome::Failed,
            Some("stale replay".to_owned()),
            second_started_at + chrono::Duration::seconds(5),
        )
        .expect_err("stale same-run-id completion should be rejected");
        assert!(error.to_string().contains("stale task run update"));
        assert!(error.to_string().contains("run=run-1"));

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 2);
        assert!(runs.iter().any(|entry| {
            entry.outcome == TaskRunOutcome::Failed
                && entry.started_at == first_started_at
                && entry.run_id.as_deref() == Some("run-1")
        }));
        assert!(runs.iter().any(|entry| {
            entry.outcome == TaskRunOutcome::Running
                && entry.started_at == second_started_at
                && entry.run_id.as_deref() == Some("run-1")
        }));

        Ok(())
    }

    #[test]
    fn reconcile_snapshot_from_lineage_collapses_mixed_bead_refs(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let now = Utc::now();
        let milestone_id = MilestoneId::new("ms-alpha")?;
        let mut snapshot = MilestoneSnapshot::initial(now);
        snapshot.status = MilestoneStatus::Running;
        snapshot.progress.total_beads = 1;

        reconcile_snapshot_from_lineage(
            &mut snapshot,
            &milestone_id,
            vec![
                TaskRunEntry {
                    milestone_id: milestone_id.to_string(),
                    bead_id: "bead-1".to_owned(),
                    project_id: "project-1".to_owned(),
                    run_id: Some("run-1".to_owned()),
                    plan_hash: Some("plan-v1".to_owned()),
                    outcome: TaskRunOutcome::Running,
                    outcome_detail: None,
                    started_at: now,
                    finished_at: None,
                    task_id: None,
                },
                TaskRunEntry {
                    milestone_id: milestone_id.to_string(),
                    bead_id: format!("{}.bead-1", milestone_id),
                    project_id: "project-2".to_owned(),
                    run_id: Some("run-2".to_owned()),
                    plan_hash: Some("plan-v1".to_owned()),
                    outcome: TaskRunOutcome::Running,
                    outcome_detail: None,
                    started_at: now + chrono::Duration::seconds(10),
                    finished_at: None,
                    task_id: None,
                },
            ],
        )?;

        assert_eq!(snapshot.status, MilestoneStatus::Running);
        assert_eq!(snapshot.active_bead.as_deref(), Some("ms-alpha.bead-1"));
        assert_eq!(snapshot.progress.in_progress_beads, 1);
        assert_eq!(snapshot.progress.completed_beads, 0);
        assert_eq!(snapshot.progress.failed_beads, 0);
        Ok(())
    }

    #[test]
    fn completion_retry_repairs_snapshot_and_journal_without_double_counting(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let flaky_journal_store = FailSecondJournalAppend {
            append_calls: Cell::new(0),
        };
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "completion-retry-test",
            "Completion Retry Test",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &flaky_journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now,
        )?;

        let failure = record_bead_completion(
            &snapshot_store,
            &flaky_journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("journal failed once"),
            now,
            now,
        )
        .expect_err("completion should fail when the journal append fails");
        assert!(failure
            .to_string()
            .contains("simulated completion journal failure"));

        let runs_after_failure = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs_after_failure.len(), 1);
        assert_eq!(runs_after_failure[0].outcome, TaskRunOutcome::Succeeded);
        let journal_after_failure = read_journal(&journal_store, base, &record.id)?;
        assert!(journal_after_failure.iter().all(|event| {
            event.event_type != MilestoneEventType::BeadCompleted
                && event.to_state != Some(MilestoneStatus::Paused)
        }));

        let snapshot_after_failure = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot_after_failure
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot_after_failure.status, MilestoneStatus::Running);
        assert_eq!(
            snapshot_after_failure.active_bead.as_deref(),
            Some("bead-1")
        );
        assert_eq!(snapshot_after_failure.progress.in_progress_beads, 1);
        assert_eq!(snapshot_after_failure.progress.completed_beads, 0);
        let repaired_updated_at = now + chrono::Duration::seconds(10);
        let mut drifted_snapshot = snapshot_after_failure.clone();
        drifted_snapshot.updated_at = repaired_updated_at;
        snapshot_store.write_snapshot(base, &record.id, &drifted_snapshot)?;

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("journal failed once"),
            now,
            now + chrono::Duration::seconds(5),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Paused);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);
        assert_eq!(snapshot.updated_at, repaired_updated_at);

        let journal = read_journal(&journal_store, base, &record.id)?;
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadCompleted)
            .collect();
        assert_eq!(completion_events.len(), 1);
        assert_eq!(completion_events[0].timestamp, now);
        let details: serde_json::Value =
            serde_json::from_str(completion_events[0].details.as_deref().unwrap())?;
        assert_eq!(
            details,
            serde_json::json!({
                "project_id": "project-1",
                "run_id": "run-1",
                "plan_hash": "plan-v1",
                "started_at": now,
                "outcome": "succeeded",
                "outcome_detail": "journal failed once",
            })
        );
        Ok(())
    }

    #[test]
    fn completion_replay_backfills_existing_journal_event_without_duplicate(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "completion-backfill-test",
            "Completion Backfill Test",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-hash",
            now,
        )?;

        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            None,
            now,
            TaskRunOutcome::Succeeded,
            None,
            now + chrono::Duration::seconds(1),
        )?;
        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            None,
            now,
            TaskRunOutcome::Succeeded,
            Some("backfilled detail".to_owned()),
            now + chrono::Duration::seconds(2),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-hash"));
        assert_eq!(runs[0].outcome_detail.as_deref(), Some("backfilled detail"));

        let journal = read_journal(&journal_store, base, &record.id)?;
        let completion_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadCompleted)
            .collect();
        assert_eq!(completion_events.len(), 1);
        assert_eq!(
            completion_events[0].timestamp,
            now + chrono::Duration::seconds(1)
        );
        let details: serde_json::Value =
            serde_json::from_str(completion_events[0].details.as_deref().unwrap())?;
        assert_eq!(
            details,
            serde_json::json!({
                "project_id": "project-1",
                "run_id": "run-1",
                "plan_hash": "plan-hash",
                "started_at": now,
                "outcome": "succeeded",
                "outcome_detail": "backfilled detail",
            })
        );
        Ok(())
    }

    #[test]
    fn completion_journal_dedupe_keeps_same_timestamp_attempts_separate(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let journal_store = FsMilestoneJournalStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "completion-journal-dedupe-test".to_owned(),
                name: "Completion Journal Dedupe Test".to_owned(),
                description: "same-timestamp completion events keep distinct attempts".to_owned(),
            },
            now,
        )?;

        let shared_finished_at = now + chrono::Duration::seconds(30);
        let first_attempt = TaskRunEntry {
            milestone_id: record.id.to_string(),
            bead_id: "bead-1".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: None,
            plan_hash: Some("plan-v1".to_owned()),
            outcome: TaskRunOutcome::Succeeded,
            outcome_detail: Some("legacy completion".to_owned()),
            started_at: now,
            finished_at: Some(shared_finished_at),
            task_id: None,
        };
        let second_attempt = TaskRunEntry {
            milestone_id: record.id.to_string(),
            bead_id: "bead-1".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: Some("run-2".to_owned()),
            plan_hash: Some("plan-v2".to_owned()),
            outcome: TaskRunOutcome::Succeeded,
            outcome_detail: Some("retry completion".to_owned()),
            started_at: now + chrono::Duration::seconds(10),
            finished_at: Some(shared_finished_at),
            task_id: None,
        };

        let first_event =
            MilestoneJournalEvent::new(MilestoneEventType::BeadCompleted, shared_finished_at)
                .with_bead("bead-1")
                .with_details(first_attempt.completion_journal_details());
        let second_event =
            MilestoneJournalEvent::new(MilestoneEventType::BeadCompleted, shared_finished_at)
                .with_bead("bead-1")
                .with_details(second_attempt.completion_journal_details());

        assert!(journal_store.append_event_if_missing(base, &record.id, &first_event)?);
        assert!(journal_store.append_event_if_missing(base, &record.id, &second_event)?);

        let completion_events: Vec<_> = read_journal(&journal_store, base, &record.id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadCompleted)
            .collect();
        assert_eq!(completion_events.len(), 2);
        assert!(completion_events
            .iter()
            .any(|event| event.details.as_deref() == first_event.details.as_deref()));
        assert!(completion_events
            .iter()
            .any(|event| event.details.as_deref() == second_event.details.as_deref()));
        Ok(())
    }

    #[test]
    fn start_retry_with_same_run_id_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let flaky_journal_store = FailFirstJournalAppend {
            append_calls: Cell::new(0),
        };
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "start-retry-test",
            "Start Retry Test",
            now,
        )?;

        let failure = record_bead_start(
            &snapshot_store,
            &flaky_journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now,
        )
        .expect_err("start should fail when the journal append fails");
        assert!(failure
            .to_string()
            .contains("simulated start journal failure"));

        let runs_after_failure = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs_after_failure.len(), 1);
        assert_eq!(runs_after_failure[0].outcome, TaskRunOutcome::Running);
        let journal_after_failure = read_journal(&journal_store, base, &record.id)?;
        assert!(journal_after_failure
            .iter()
            .all(|event| event.event_type != MilestoneEventType::BeadStarted));

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(5),
        )?;

        let runs_after_retry = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs_after_retry.len(), 1);
        assert_eq!(runs_after_retry[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(runs_after_retry[0].started_at, now);

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Running);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(snapshot.progress.in_progress_beads, 1);
        assert_eq!(snapshot.updated_at, now + chrono::Duration::seconds(5));

        let journal = read_journal(&journal_store, base, &record.id)?;
        let start_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadStarted)
            .collect();
        assert_eq!(start_events.len(), 1);
        assert_eq!(start_events[0].timestamp, now);

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("completed after retry"),
            now,
            now + chrono::Duration::seconds(6),
        )?;

        let finalized_runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(finalized_runs.len(), 1);
        assert_eq!(finalized_runs[0].outcome, TaskRunOutcome::Succeeded);
        Ok(())
    }

    #[test]
    fn start_retry_rejects_conflicting_plan_hash() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "start-plan-conflict-test",
            "Start Plan Conflict Test",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now,
        )?;

        let error = record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v2",
            now + chrono::Duration::seconds(5),
        )
        .expect_err("conflicting plan hashes must be rejected");
        assert!(error.to_string().contains("conflicting plan_hash"));

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Running);

        let journal = read_journal(&journal_store, base, &record.id)?;
        let start_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadStarted)
            .collect();
        assert_eq!(start_events.len(), 1);
        Ok(())
    }

    #[test]
    fn start_retry_reopens_failed_attempt_for_same_run_id() -> Result<(), Box<dyn std::error::Error>>
    {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "start-reopen-failed-run",
            "Start Reopen Failed Run",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("interrupted before resume"),
            now,
            now + chrono::Duration::seconds(5),
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(10),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 2);
        let failed_attempt = runs
            .iter()
            .find(|entry| {
                entry.run_id.as_deref() == Some("run-1")
                    && entry.started_at == now
                    && entry.outcome == TaskRunOutcome::Failed
            })
            .expect("historical failed attempt should be preserved");
        assert_eq!(failed_attempt.plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(
            failed_attempt.outcome_detail.as_deref(),
            Some("interrupted before resume")
        );
        assert_eq!(
            failed_attempt.finished_at,
            Some(now + chrono::Duration::seconds(5))
        );

        let retried_attempt = runs
            .iter()
            .find(|entry| {
                entry.run_id.as_deref() == Some("run-1")
                    && entry.started_at == now + chrono::Duration::seconds(10)
                    && entry.outcome == TaskRunOutcome::Running
            })
            .expect("retried running attempt should be appended");
        assert_eq!(retried_attempt.plan_hash.as_deref(), Some("plan-v1"));
        assert!(retried_attempt.finished_at.is_none());
        assert!(retried_attempt.outcome_detail.is_none());

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Running);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        Ok(())
    }

    #[test]
    fn bead_execution_history_preserves_same_run_failed_attempt_after_success(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();
        let retry_started_at = now + chrono::Duration::seconds(10);
        let retry_finished_at = retry_started_at + chrono::Duration::seconds(5);

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "same-run-history-preserved",
            "Same Run History Preserved",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("first failure"),
            now,
            now + chrono::Duration::seconds(5),
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            retry_started_at,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("retry succeeded"),
            retry_started_at,
            retry_finished_at,
        )?;

        let runs = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].started_at, now);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(runs[1].started_at, retry_started_at);
        assert_eq!(runs[1].outcome, TaskRunOutcome::Succeeded);

        let history = bead_execution_history(
            &store,
            &plan_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
        )?;
        assert_eq!(history.runs.len(), 2);
        assert_eq!(history.runs[0].started_at, now);
        assert_eq!(history.runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(history.runs[1].started_at, retry_started_at);
        assert_eq!(history.runs[1].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(history.runs[1].duration_ms, Some(5_000));

        Ok(())
    }

    #[test]
    fn resume_transition_uses_resume_time_instead_of_original_start_time(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let started_at = Utc::now();
        let paused_at = started_at + chrono::Duration::seconds(5);
        let resumed_at = started_at + chrono::Duration::seconds(15);

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "resume-transition-timestamp",
            "Resume Transition Timestamp",
            started_at,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            started_at,
        )?;
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &record.id,
            MilestoneStatus::Paused,
            paused_at,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            resumed_at,
        )?;

        let journal = read_journal(&journal_store, base, &record.id)?;
        let resume_event = journal
            .iter()
            .find(|event| {
                event.event_type == MilestoneEventType::StatusChanged
                    && event.from_state == Some(MilestoneStatus::Paused)
                    && event.to_state == Some(MilestoneStatus::Running)
            })
            .expect("paused milestone should record a resumed running transition");
        assert_eq!(resume_event.timestamp, resumed_at);

        let bead_start_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadStarted)
            .collect();
        assert_eq!(bead_start_events.len(), 1);
        assert_eq!(bead_start_events[0].timestamp, started_at);
        Ok(())
    }

    #[test]
    fn retry_start_on_same_bead_supersedes_running_attempt_from_other_project(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "same-bead-cross-project-retry-test",
            "Same Bead Cross Project Retry Test",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now,
        )?;

        let retry_started_at = now + chrono::Duration::seconds(10);
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-2",
            "run-2",
            "plan-v2",
            retry_started_at,
        )?;

        let runs_after_retry = find_runs_for_bead(&lineage_store, base, &record.id, "bead-1")?;
        assert_eq!(runs_after_retry.len(), 2);
        assert_eq!(runs_after_retry[0].project_id, "project-1");
        assert_eq!(runs_after_retry[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(runs_after_retry[0].finished_at, Some(retry_started_at));
        assert!(runs_after_retry[0]
            .outcome_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("superseded by retry")));
        assert_eq!(runs_after_retry[1].project_id, "project-2");
        assert_eq!(runs_after_retry[1].run_id.as_deref(), Some("run-2"));
        assert_eq!(runs_after_retry[1].outcome, TaskRunOutcome::Running);

        let active_snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        active_snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(active_snapshot.status, MilestoneStatus::Running);
        assert_eq!(active_snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(active_snapshot.progress.in_progress_beads, 1);
        assert_eq!(active_snapshot.progress.failed_beads, 0);

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-2",
            "run-2",
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("retry completed"),
            retry_started_at,
            retry_started_at + chrono::Duration::seconds(5),
        )?;

        let final_snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        final_snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(final_snapshot.status, MilestoneStatus::Paused);
        assert_eq!(final_snapshot.active_bead, None);
        assert_eq!(final_snapshot.progress.in_progress_beads, 0);
        assert_eq!(final_snapshot.progress.completed_beads, 1);
        assert_eq!(final_snapshot.progress.failed_beads, 0);
        Ok(())
    }

    #[test]
    fn distinct_named_run_with_same_started_at_as_finalized_attempt_can_start(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "same-started-at-named-retry-test",
            "Same Started At Named Retry Test",
            now,
        )?;

        lineage_store.append_task_run(
            base,
            &record.id,
            &TaskRunEntry {
                milestone_id: record.id.to_string(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-2".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: Some("first attempt finished".to_owned()),
                started_at: now,
                finished_at: Some(now + chrono::Duration::seconds(5)),
                task_id: None,
            },
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-3",
            "plan-v2",
            now,
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 2);
        assert!(runs.iter().any(|entry| {
            entry.run_id.as_deref() == Some("run-2") && entry.outcome == TaskRunOutcome::Succeeded
        }));
        assert!(runs.iter().any(|entry| {
            entry.run_id.as_deref() == Some("run-3")
                && entry.started_at == now
                && entry.outcome == TaskRunOutcome::Running
        }));

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Running);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        Ok(())
    }

    #[test]
    fn start_journal_dedupe_keeps_same_timestamp_named_attempts_separate(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "same-started-at-start-journal-test",
            "Same Started At Start Journal Test",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-2",
            "plan-v1",
            now,
        )?;
        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-2",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("first run finished"),
            now,
            now + chrono::Duration::seconds(5),
        )?;
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-3",
            "plan-v2",
            now,
        )?;

        let start_events: Vec<_> = read_journal(&journal_store, base, &record.id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadStarted)
            .collect();
        assert_eq!(start_events.len(), 2);

        let start_details: Vec<serde_json::Value> = start_events
            .iter()
            .map(|event| {
                serde_json::from_str(
                    event
                        .details
                        .as_deref()
                        .expect("start event should carry details"),
                )
            })
            .collect::<Result<_, _>>()?;
        assert!(start_details.iter().any(|details| {
            details
                == &serde_json::json!({
                    "project_id": "project-1",
                    "run_id": "run-2",
                    "plan_hash": "plan-v1",
                })
        }));
        assert!(start_details.iter().any(|details| {
            details
                == &serde_json::json!({
                    "project_id": "project-1",
                    "run_id": "run-3",
                    "plan_hash": "plan-v2",
                })
        }));
        Ok(())
    }

    #[test]
    fn synthetic_ready_event_uses_ready_shaped_metadata() -> Result<(), Box<dyn std::error::Error>>
    {
        let now = Utc::now();
        let milestone_id = MilestoneId::new("synthetic-ready-test")?;
        let mut running_snapshot = MilestoneSnapshot::initial(now);
        running_snapshot.status = MilestoneStatus::Running;
        running_snapshot.plan_hash = Some("plan-v1".to_owned());
        running_snapshot.plan_version = 1;
        running_snapshot.active_bead = Some("bead-1".to_owned());
        running_snapshot.progress.total_beads = 2;
        running_snapshot.progress.in_progress_beads = 1;

        let events = build_reconciled_transition_events(
            &milestone_id,
            &MilestoneSnapshot::initial(now),
            &running_snapshot,
            &[],
            now + chrono::Duration::seconds(5),
            Some(now + chrono::Duration::seconds(5)),
            "controller",
            "execution started",
        )?;

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].from_state, Some(MilestoneStatus::Planning));
        assert_eq!(events[0].to_state, Some(MilestoneStatus::Ready));
        assert_eq!(events[1].from_state, Some(MilestoneStatus::Ready));
        assert_eq!(events[1].to_state, Some(MilestoneStatus::Running));

        let ready_metadata = events[0]
            .metadata
            .as_ref()
            .expect("synthetic ready event should carry metadata");
        assert!(!ready_metadata.contains_key("active_bead"));
        assert_eq!(
            ready_metadata.get("in_progress_beads"),
            Some(&serde_json::json!(0))
        );

        let running_metadata = events[1]
            .metadata
            .as_ref()
            .expect("synthetic running event should carry metadata");
        assert_eq!(
            running_metadata.get("active_bead"),
            Some(&serde_json::json!("bead-1"))
        );
        assert_eq!(
            running_metadata.get("in_progress_beads"),
            Some(&serde_json::json!(1))
        );
        Ok(())
    }

    #[test]
    fn synthetic_bridge_events_preserve_pre_transition_progress_metadata(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let now = Utc::now();
        let milestone_id = MilestoneId::new("synthetic-bridge-progress-test")?;
        let previous_snapshot = MilestoneSnapshot::initial(now);
        let mut completed_snapshot = previous_snapshot.clone();
        completed_snapshot.status = MilestoneStatus::Completed;
        completed_snapshot.plan_hash = Some("plan-v1".to_owned());
        completed_snapshot.plan_version = 1;
        completed_snapshot.progress.total_beads = 2;
        completed_snapshot.progress.completed_beads = 2;

        let events = build_reconciled_transition_events(
            &milestone_id,
            &previous_snapshot,
            &completed_snapshot,
            &[],
            now + chrono::Duration::seconds(10),
            Some(now + chrono::Duration::seconds(1)),
            "controller",
            "all beads closed",
        )?;

        assert_eq!(events.len(), 3);
        for event in events.iter().take(2) {
            let metadata = event
                .metadata
                .as_ref()
                .expect("synthetic bridge events should carry metadata");
            assert_eq!(metadata.get("completed_beads"), Some(&serde_json::json!(0)));
            assert_eq!(metadata.get("failed_beads"), Some(&serde_json::json!(0)));
            assert_eq!(metadata.get("skipped_beads"), Some(&serde_json::json!(0)));
        }
        assert_eq!(
            events[0]
                .metadata
                .as_ref()
                .and_then(|metadata| metadata.get("total_beads")),
            Some(&serde_json::json!(2))
        );
        Ok(())
    }

    #[test]
    fn synthetic_paused_resume_bridge_uses_resumed_start_when_it_postdates_pause(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let paused_at = Utc::now();
        let resumed_at = paused_at + chrono::Duration::minutes(10);
        let completed_at = resumed_at + chrono::Duration::minutes(10);
        let milestone_id = MilestoneId::new("synthetic-paused-resume-bridge-test")?;
        let mut previous_snapshot = MilestoneSnapshot::initial(paused_at);
        previous_snapshot.status = MilestoneStatus::Paused;
        previous_snapshot.updated_at = paused_at;
        previous_snapshot.plan_hash = Some("plan-v1".to_owned());
        previous_snapshot.plan_version = 1;
        previous_snapshot.progress.total_beads = 1;

        let mut completed_snapshot = previous_snapshot.clone();
        completed_snapshot.status = MilestoneStatus::Completed;
        completed_snapshot.updated_at = completed_at;
        completed_snapshot.progress.completed_beads = 1;

        let events = build_reconciled_transition_events(
            &milestone_id,
            &previous_snapshot,
            &completed_snapshot,
            &[],
            completed_at,
            Some(resumed_at),
            "controller",
            "all beads closed",
        )?;

        assert_eq!(events.len(), 2);
        let resumed_event = &events[0];
        assert_eq!(resumed_event.from_state, Some(MilestoneStatus::Paused));
        assert_eq!(resumed_event.to_state, Some(MilestoneStatus::Running));
        assert_eq!(resumed_event.timestamp, resumed_at);

        let completed_event = &events[1];
        assert_eq!(completed_event.from_state, Some(MilestoneStatus::Running));
        assert_eq!(completed_event.to_state, Some(MilestoneStatus::Completed));
        let metadata = completed_event
            .metadata
            .as_ref()
            .expect("completed bridge should carry metadata");
        assert_eq!(
            metadata.get("duration_seconds"),
            Some(&serde_json::json!(600))
        );
        Ok(())
    }

    #[test]
    fn completion_after_missed_start_reconstructs_running_bridge(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let flaky_journal_store = FailFirstJournalAppend {
            append_calls: Cell::new(0),
        };
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let bundle = sample_bundle("missed-running-bridge", "Missed Running Bridge");
        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &flaky_journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now + chrono::Duration::seconds(1),
        )
        .expect_err("start journal failure should leave lineage ahead of the snapshot");

        record_bead_completion(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Succeeded,
            Some("bridged completion"),
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(10),
        )?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Completed);

        let transitions: Vec<_> = read_journal(&journal_store, base, &record.id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::StatusChanged)
            .collect();
        let running_event = transitions
            .iter()
            .find(|event| {
                event.from_state == Some(MilestoneStatus::Ready)
                    && event.to_state == Some(MilestoneStatus::Running)
            })
            .expect("missed start should synthesize a running bridge");
        assert_eq!(running_event.timestamp, now + chrono::Duration::seconds(1));

        let completed_event = transitions
            .iter()
            .find(|event| {
                event.from_state == Some(MilestoneStatus::Running)
                    && event.to_state == Some(MilestoneStatus::Completed)
            })
            .expect("completion transition should be recorded");
        let metadata = completed_event
            .metadata
            .as_ref()
            .expect("completed transition should carry metadata");
        assert_eq!(
            metadata.get("duration_seconds"),
            Some(&serde_json::json!(9))
        );
        Ok(())
    }

    #[test]
    fn mark_failed_disposition_preserves_completed_snapshots_without_failed_beads(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let now = Utc::now();
        let mut snapshot = MilestoneSnapshot::initial(now);
        snapshot.status = MilestoneStatus::Completed;
        snapshot.progress.total_beads = 1;
        snapshot.progress.completed_beads = 1;

        apply_completion_milestone_disposition(
            &mut snapshot,
            CompletionMilestoneDisposition::MarkMilestoneFailed,
        );

        assert_eq!(snapshot.status, MilestoneStatus::Completed);
        assert_eq!(snapshot.progress.failed_beads, 0);
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        Ok(())
    }

    #[test]
    fn repairable_completion_event_rejects_mismatched_event_types() {
        let started_at = Utc::now();
        let existing = MilestoneJournalEvent::new(
            MilestoneEventType::BeadCompleted,
            started_at + chrono::Duration::seconds(10),
        )
        .with_bead("bead-1")
        .with_details(render_completion_journal_details(
            "proj-1",
            Some("run-1"),
            Some("plan-v1"),
            started_at,
            "succeeded",
            None,
            None,
        ));
        let requested = MilestoneJournalEvent::new(
            MilestoneEventType::BeadFailed,
            started_at + chrono::Duration::seconds(12),
        )
        .with_bead("bead-1")
        .with_details(render_completion_journal_details(
            "proj-1",
            Some("run-1"),
            Some("plan-v1"),
            started_at,
            "failed",
            Some("different outcome"),
            None,
        ));

        assert!(repairable_completion_event(&existing, &requested).is_none());
    }

    #[test]
    fn concurrent_start_retries_share_one_lineage_row_and_start_event(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "concurrent-start-test",
            "Concurrent Start Test",
            now,
        )?;

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let base_dir = base.to_path_buf();
        let milestone_id = record.id.clone();
        let mut handles = Vec::new();

        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            let base_dir = base_dir.clone();
            let milestone_id = milestone_id.clone();
            handles.push(std::thread::spawn(move || -> AppResult<()> {
                let snapshot_store = FsMilestoneSnapshotStore;
                let journal_store = FsMilestoneJournalStore;
                let lineage_store = FsTaskRunLineageStore;

                barrier.wait();
                record_bead_start(
                    &snapshot_store,
                    &journal_store,
                    &lineage_store,
                    &base_dir,
                    &milestone_id,
                    "bead-1",
                    "project-1",
                    "run-1",
                    "plan-v1",
                    now,
                )
            }));
        }

        for handle in handles {
            handle.join().expect("start worker panicked")?;
        }

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Running);

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Running);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
        assert_eq!(snapshot.progress.in_progress_beads, 1);

        let journal = read_journal(&journal_store, base, &record.id)?;
        let start_events: Vec<_> = journal
            .iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadStarted)
            .collect();
        assert_eq!(start_events.len(), 1);
        assert_eq!(start_events[0].timestamp, now);
        Ok(())
    }

    #[test]
    fn start_and_completion_serialize_milestone_snapshot_updates(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone_with_plan(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            "serialized-mutation-test",
            "Serialized Mutation Test",
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "plan-v1",
            now,
        )?;

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (start_attempted_tx, start_attempted_rx) = mpsc::channel();
        let (start_proceed_tx, start_proceed_rx) = mpsc::channel();
        let (start_finished_tx, start_finished_rx) = mpsc::channel();
        let blocking_snapshot_store = Arc::new(BlockingSnapshotStore::new(entered_tx, release_rx));
        let start_snapshot_store = Arc::new(LockAttemptSnapshotStore::new(
            start_attempted_tx,
            start_proceed_rx,
        ));
        let base_dir = base.to_path_buf();
        let milestone_id = record.id.clone();

        let completion_store = Arc::clone(&blocking_snapshot_store);
        let completion_handle = std::thread::spawn(move || -> AppResult<()> {
            let journal_store = FsMilestoneJournalStore;
            let lineage_store = FsTaskRunLineageStore;
            update_task_run(
                completion_store.as_ref(),
                &journal_store,
                &lineage_store,
                &base_dir,
                &milestone_id,
                "bead-1",
                "project-1",
                "run-1",
                Some("plan-v1"),
                now,
                TaskRunOutcome::Succeeded,
                Some("finished bead-1".to_owned()),
                now + chrono::Duration::seconds(5),
            )
        });

        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("completion should block on the ready snapshot write");

        let start_store = Arc::clone(&start_snapshot_store);
        let base_dir = base.to_path_buf();
        let milestone_id = record.id.clone();
        let start_handle = std::thread::spawn(move || -> AppResult<()> {
            let journal_store = FsMilestoneJournalStore;
            let lineage_store = FsTaskRunLineageStore;
            let result = record_bead_start(
                start_store.as_ref(),
                &journal_store,
                &lineage_store,
                &base_dir,
                &milestone_id,
                "bead-2",
                "project-2",
                "run-2",
                "plan-v2",
                now + chrono::Duration::seconds(10),
            );
            let _ = start_finished_tx.send(());
            result
        });

        start_attempted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("new bead start should reach the write-lock contention point");
        start_proceed_tx
            .send(())
            .expect("allow new bead start to contend on the write lock");

        assert!(
            start_finished_rx
                .recv_timeout(Duration::from_millis(100))
                .is_err(),
            "new bead start should wait for the in-flight completion write lock",
        );

        let runs_while_blocked = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs_while_blocked.len(), 1);
        assert_eq!(runs_while_blocked[0].bead_id, "bead-1");
        assert_eq!(runs_while_blocked[0].outcome, TaskRunOutcome::Succeeded);

        release_tx
            .send(())
            .expect("release blocked completion snapshot write");

        completion_handle
            .join()
            .expect("completion worker panicked")?;
        start_handle.join().expect("start worker panicked")?;

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Running);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-2"));
        assert_eq!(snapshot.progress.completed_beads, 1);
        assert_eq!(snapshot.progress.in_progress_beads, 1);

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 2);
        assert!(runs
            .iter()
            .any(|run| { run.bead_id == "bead-1" && run.outcome == TaskRunOutcome::Succeeded }));
        assert!(runs
            .iter()
            .any(|run| { run.bead_id == "bead-2" && run.outcome == TaskRunOutcome::Running }));
        Ok(())
    }

    #[test]
    fn duplicate_running_rows_with_same_run_id_can_be_finalized(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "duplicate-run-id-test".to_owned(),
                name: "Duplicate Run ID Test".to_owned(),
                description: "testing duplicate running row repair".to_owned(),
            },
            now,
        )?;

        for started_at in [now, now + chrono::Duration::seconds(1)] {
            lineage_store.append_task_run(
                base,
                &record.id,
                &TaskRunEntry {
                    milestone_id: record.id.to_string(),
                    bead_id: "bead-1".to_owned(),
                    project_id: "project-1".to_owned(),
                    run_id: Some("run-1".to_owned()),
                    plan_hash: Some("plan-v1".to_owned()),
                    outcome: TaskRunOutcome::Running,
                    outcome_detail: None,
                    started_at,
                    finished_at: None,
                    task_id: None,
                },
            )?;
        }

        let mut stale_snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        stale_snapshot.status = MilestoneStatus::Running;
        stale_snapshot.active_bead = Some("bead-1".to_owned());
        stale_snapshot.progress.in_progress_beads = 2;
        stale_snapshot.updated_at = now + chrono::Duration::seconds(1);
        snapshot_store.write_snapshot(base, &record.id, &stale_snapshot)?;

        update_task_run(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            Some("plan-v1"),
            now,
            TaskRunOutcome::Succeeded,
            Some("deduped finalization".to_owned()),
            now + chrono::Duration::seconds(10),
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(runs[0].started_at, now);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Succeeded);

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        snapshot
            .validate_semantics()
            .map_err(Box::<dyn std::error::Error>::from)?;
        assert_eq!(snapshot.status, MilestoneStatus::Paused);
        assert_eq!(snapshot.active_bead, None);
        assert_eq!(snapshot.progress.in_progress_beads, 0);
        assert_eq!(snapshot.progress.completed_beads, 1);
        Ok(())
    }

    /// When callers provide an explicit plan_hash, it should
    /// take precedence over whatever the snapshot may contain.
    #[test]
    fn explicit_plan_hash_takes_precedence_over_snapshot() -> Result<(), Box<dyn std::error::Error>>
    {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "plan-hash-explicit".to_owned(),
                name: "Plan Hash Explicit".to_owned(),
                description: "test explicit plan_hash precedence".to_owned(),
            },
            now,
        )?;

        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &sample_bundle("plan-hash-explicit", "Plan Hash Explicit"),
            now,
        )?;

        // Start bead WITH explicit plan_hash
        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "run-1",
            "caller-provided-hash",
            now,
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(
            runs[0].plan_hash.as_deref(),
            Some("caller-provided-hash"),
            "explicit plan_hash must take precedence over snapshot"
        );
        Ok(())
    }

    /// Required run_id and plan_hash are stored in the lineage entry.
    #[test]
    fn required_run_id_populates_lineage_entry() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "run-id-required".to_owned(),
                name: "Run ID Required".to_owned(),
                description: "test required run_id".to_owned(),
            },
            now,
        )?;

        persist_plan(
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &record.id,
            &sample_bundle("run-id-required", "Run ID Required"),
            now,
        )?;

        record_bead_start(
            &snapshot_store,
            &journal_store,
            &lineage_store,
            base,
            &record.id,
            "bead-1",
            "project-1",
            "my-run-42",
            "hash-abc",
            now,
        )?;

        let runs = read_task_runs(&lineage_store, base, &record.id)?;
        assert_eq!(runs.len(), 1);
        assert_eq!(
            runs[0].run_id.as_deref(),
            Some("my-run-42"),
            "run_id must be stored in the lineage entry"
        );
        assert_eq!(
            runs[0].plan_hash.as_deref(),
            Some("hash-abc"),
            "plan_hash must be stored in the lineage entry"
        );
        Ok(())
    }

    // ── Planned-elsewhere rebuild / collapse tests ──────────────────────

    #[test]
    fn planned_elsewhere_rebuild_from_journal_when_ndjson_missing(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pe-rebuild".to_owned(),
                name: "PE rebuild".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        // Write a mapping via the service (writes to both journal and NDJSON).
        let mapping = PlannedElsewhereMapping {
            active_bead_id: "bead-A".to_owned(),
            finding_summary: "concern about X".to_owned(),
            mapped_to_bead_id: "bead-B".to_owned(),
            recorded_at: now,
            mapped_bead_verified: false,
            run_id: None,
            completion_round: None,
        };
        record_planned_elsewhere_mapping(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            base,
            &record.id,
            &mapping,
        )?;

        // Delete the NDJSON sidecar to simulate loss.
        let ndjson_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("planned_elsewhere.ndjson");
        std::fs::remove_file(&ndjson_path)?;

        // Load should rebuild from journal.
        let loaded = load_planned_elsewhere_mappings(
            &FsPlannedElsewhereMappingStore,
            &FsMilestoneJournalStore,
            base,
            &record.id,
        )?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].active_bead_id, "bead-A");
        assert_eq!(loaded[0].finding_summary, "concern about X");
        assert_eq!(loaded[0].mapped_to_bead_id, "bead-B");
        assert!(!loaded[0].mapped_bead_verified);
        Ok(())
    }

    #[test]
    fn planned_elsewhere_collapse_verified_supersedes_unverified(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pe-collapse".to_owned(),
                name: "PE collapse".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        // Write an unverified mapping.
        let unverified = PlannedElsewhereMapping {
            active_bead_id: "bead-A".to_owned(),
            finding_summary: "concern about X".to_owned(),
            mapped_to_bead_id: "bead-B".to_owned(),
            recorded_at: now,
            mapped_bead_verified: false,
            run_id: None,
            completion_round: None,
        };
        record_planned_elsewhere_mapping(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            base,
            &record.id,
            &unverified,
        )?;

        // Write a verified mapping for the same identity.
        let verified = PlannedElsewhereMapping {
            active_bead_id: "bead-A".to_owned(),
            finding_summary: "concern about X".to_owned(),
            mapped_to_bead_id: "bead-B".to_owned(),
            recorded_at: now + chrono::Duration::seconds(10),
            mapped_bead_verified: true,
            run_id: None,
            completion_round: None,
        };
        record_planned_elsewhere_mapping(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            base,
            &record.id,
            &verified,
        )?;

        // Load should return only the verified record (collapsed).
        let loaded = load_planned_elsewhere_mappings(
            &FsPlannedElsewhereMappingStore,
            &FsMilestoneJournalStore,
            base,
            &record.id,
        )?;
        assert_eq!(loaded.len(), 1, "should be collapsed to one record");
        assert!(loaded[0].mapped_bead_verified, "verified record should win");
        Ok(())
    }

    #[test]
    fn planned_elsewhere_journal_events_with_missing_metadata_skipped(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pe-missing".to_owned(),
                name: "PE missing".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        // Write a valid mapping first so the journal has at least one good event.
        let mapping = PlannedElsewhereMapping {
            active_bead_id: "bead-A".to_owned(),
            finding_summary: "valid concern".to_owned(),
            mapped_to_bead_id: "bead-B".to_owned(),
            recorded_at: now,
            mapped_bead_verified: false,
            run_id: None,
            completion_round: None,
        };
        record_planned_elsewhere_mapping(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            base,
            &record.id,
            &mapping,
        )?;

        // Manually append a malformed PlannedElsewhereMapped event (missing mapped_to_bead_id).
        let mut bad_metadata = serde_json::Map::new();
        bad_metadata.insert(
            "active_bead_id".to_owned(),
            serde_json::Value::String("bead-C".to_owned()),
        );
        // No mapped_to_bead_id — should be skipped.
        let mut bad_event =
            MilestoneJournalEvent::new(MilestoneEventType::PlannedElsewhereMapped, now)
                .with_bead("bead-C".to_owned())
                .with_details("bad concern".to_owned());
        bad_event.metadata = Some(bad_metadata);
        let line = bad_event.to_ndjson_line()?;
        FsMilestoneJournalStore.append_event(base, &record.id, &line)?;

        // Also append an event with no metadata at all.
        let no_meta_event =
            MilestoneJournalEvent::new(MilestoneEventType::PlannedElsewhereMapped, now);
        let line2 = no_meta_event.to_ndjson_line()?;
        FsMilestoneJournalStore.append_event(base, &record.id, &line2)?;

        // Load should only return the valid mapping, skipping the malformed ones.
        let loaded = load_planned_elsewhere_mappings(
            &FsPlannedElsewhereMappingStore,
            &FsMilestoneJournalStore,
            base,
            &record.id,
        )?;
        assert_eq!(loaded.len(), 1, "only valid mapping should be returned");
        assert_eq!(loaded[0].active_bead_id, "bead-A");
        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_rejects_conflicted_beads_jsonl(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        std::fs::write(
            base.join(".beads/issues.jsonl"),
            "<<<<<<< HEAD\n{\"id\":\"bead-a\"}\n=======\n{\"id\":\"bead-b\"}\n>>>>>>> branch\n",
        )?;
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-conflict".to_owned(),
                name: "PN conflict".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let runner = MockBrRunner::new(vec![
            MockBrRunner::success(
                r#"{"id":"active-bead","title":"Active bead","status":"open","priority":1,"bead_type":"task","labels":["backend"]}"#,
            ),
            MockBrRunner::success(&list_all_stdout(vec![])),
        ]);
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));
        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Conflict should block bead mutation".to_owned(),
            run_id: Some("run-conflict".to_owned()),
            completion_round: Some(1),
        };

        let mut created_in_pass = 0usize;
        let error = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await
        .expect_err("conflicted issues.jsonl should block mutation");

        match error {
            AppError::MilestoneOperationFailed {
                action, details, ..
            } => {
                assert_eq!(action, "prepare bead mutation");
                assert!(details.contains("resolve the conflict"));
            }
            other => panic!("unexpected error: {other}"),
        }

        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.as_slice(),
            &[
                vec![
                    "list".to_owned(),
                    "--all".to_owned(),
                    "--deferred".to_owned(),
                    "--limit=0".to_owned(),
                    "--json".to_owned(),
                ],
                vec!["show".to_owned(), "active-bead".to_owned(), "--json".to_owned()],
            ],
            "health failure should still allow read-only idempotent checks before blocking mutation"
        );
        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_rejects_malformed_beads_jsonl(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        std::fs::write(
            base.join(".beads/issues.jsonl"),
            "{\"id\":\"bead-a\"}\n{\"id\": }\n",
        )?;
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-malformed".to_owned(),
                name: "PN malformed".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let runner = MockBrRunner::new(vec![
            MockBrRunner::success(
                r#"{"id":"active-bead","title":"Active bead","status":"open","priority":1,"bead_type":"task","labels":["backend"]}"#,
            ),
            MockBrRunner::success(&list_all_stdout(vec![])),
        ]);
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));
        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Malformed JSONL should block bead mutation".to_owned(),
            run_id: Some("run-malformed".to_owned()),
            completion_round: Some(1),
        };

        let mut created_in_pass = 0usize;
        let error = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await
        .expect_err("malformed issues.jsonl should block mutation");

        match error {
            AppError::MilestoneOperationFailed {
                action, details, ..
            } => {
                assert_eq!(action, "prepare bead mutation");
                assert!(details.contains("malformed .beads/issues.jsonl line 2"));
            }
            other => panic!("unexpected error: {other}"),
        }

        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.as_slice(),
            &[
                vec![
                    "list".to_owned(),
                    "--all".to_owned(),
                    "--deferred".to_owned(),
                    "--limit=0".to_owned(),
                    "--json".to_owned(),
                ],
                vec!["show".to_owned(), "active-bead".to_owned(), "--json".to_owned()],
            ],
            "health failure should still allow read-only idempotent checks before blocking mutation"
        );
        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_rechecks_health_before_dependency_after_create(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-create-conflict-before-dependency".to_owned(),
                name: "PN create conflict before dependency".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let issues_path = base.join(".beads/issues.jsonl");
        let runner = ScriptedBrRunner::new({
            let issues_path = issues_path.clone();
            move |args| match args {
                [command, ..] if command == "list" => {
                    MockBrRunner::success(&list_all_stdout(vec![]))
                }
                [command, bead_id, ..] if command == "show" && bead_id == "active-bead" => {
                    MockBrRunner::success(
                        r#"{"id":"active-bead","title":"Active bead","status":"open","priority":1,"bead_type":"task","labels":["backend"]}"#,
                    )
                }
                [command, ..] if command == "create" => {
                    std::fs::write(
                        &issues_path,
                        "<<<<<<< HEAD\n{\"id\":\"bead-a\"}\n=======\n{\"id\":\"bead-b\"}\n>>>>>>> branch\n",
                    )?;
                    MockBrRunner::success("created bead-new")
                }
                [command, bead_id, ..] if command == "show" && bead_id == "bead-new" => {
                    MockBrRunner::success(
                        r###"{"id":"bead-new","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"],"description":"## Finding Summary\nRetry paths lack telemetry\n\n## Proposed Scope\nInstrument retry loops with counters and histograms\n\n## Rationale\nNeed a new bead","dependencies":[],"dependents":[]}"###,
                    )
                }
                other => panic!("unexpected args: {other:?}"),
            }
        });
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));
        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Need a new bead".to_owned(),
            run_id: Some("run-create-conflict-before-dependency".to_owned()),
            completion_round: Some(11),
        };

        let mut created_in_pass = 0usize;
        let error = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await
        .expect_err("conflicted issues.jsonl should block dependency placement after create");

        match error {
            AppError::MilestoneOperationFailed {
                action, details, ..
            } => {
                assert_eq!(action, "place proposed bead dependency");
                assert!(details.contains("resolve the conflict"));
            }
            other => panic!("unexpected error: {other}"),
        }

        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.as_slice(),
            &[
                vec![
                    "list".to_owned(),
                    "--all".to_owned(),
                    "--deferred".to_owned(),
                    "--limit=0".to_owned(),
                    "--json".to_owned(),
                ],
                vec![
                    "show".to_owned(),
                    "active-bead".to_owned(),
                    "--json".to_owned()
                ],
                vec![
                    "create".to_owned(),
                    "--title=Add retry telemetry".to_owned(),
                    "--type=task".to_owned(),
                    "--priority=2".to_owned(),
                    "--labels=backend".to_owned(),
                    format!("--description={}", render_proposed_bead_description(&input)),
                ],
                vec![
                    "show".to_owned(),
                    "bead-new".to_owned(),
                    "--json".to_owned()
                ],
            ],
            "the second health check should stop before add_dependency runs"
        );
        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_rechecks_health_before_sync_after_dependency(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-create-conflict-before-sync".to_owned(),
                name: "PN create conflict before sync".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let issues_path = base.join(".beads/issues.jsonl");
        let runner = ScriptedBrRunner::new({
            let issues_path = issues_path.clone();
            move |args| match args {
                [command, ..] if command == "list" => {
                    MockBrRunner::success(&list_all_stdout(vec![]))
                }
                [command, bead_id, ..] if command == "show" && bead_id == "active-bead" => {
                    MockBrRunner::success(
                        r#"{"id":"active-bead","title":"Active bead","status":"open","priority":1,"bead_type":"task","labels":["backend"]}"#,
                    )
                }
                [command, ..] if command == "create" => MockBrRunner::success("created bead-new"),
                [command, bead_id, ..] if command == "show" && bead_id == "bead-new" => {
                    MockBrRunner::success(
                        r###"{"id":"bead-new","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"],"description":"## Finding Summary\nRetry paths lack telemetry\n\n## Proposed Scope\nInstrument retry loops with counters and histograms\n\n## Rationale\nNeed a new bead","dependencies":[],"dependents":[]}"###,
                    )
                }
                [command, subcommand, ..] if command == "dep" && subcommand == "add" => {
                    std::fs::write(&issues_path, "{\"id\":\"bead-new\"}\n{\"id\": }\n")?;
                    MockBrRunner::success("dependency added")
                }
                other => panic!("unexpected args: {other:?}"),
            }
        });
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));
        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Need a new bead".to_owned(),
            run_id: Some("run-create-conflict-before-sync".to_owned()),
            completion_round: Some(12),
        };

        let mut created_in_pass = 0usize;
        let error = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await
        .expect_err("malformed issues.jsonl should block sync after dependency placement");

        match error {
            AppError::MilestoneOperationFailed {
                action, details, ..
            } => {
                assert_eq!(action, "sync proposed bead creation");
                assert!(details.contains("malformed .beads/issues.jsonl line 2"));
            }
            other => panic!("unexpected error: {other}"),
        }

        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.as_slice(),
            &[
                vec![
                    "list".to_owned(),
                    "--all".to_owned(),
                    "--deferred".to_owned(),
                    "--limit=0".to_owned(),
                    "--json".to_owned(),
                ],
                vec![
                    "show".to_owned(),
                    "active-bead".to_owned(),
                    "--json".to_owned()
                ],
                vec![
                    "create".to_owned(),
                    "--title=Add retry telemetry".to_owned(),
                    "--type=task".to_owned(),
                    "--priority=2".to_owned(),
                    "--labels=backend".to_owned(),
                    format!("--description={}", render_proposed_bead_description(&input)),
                ],
                vec![
                    "show".to_owned(),
                    "bead-new".to_owned(),
                    "--json".to_owned()
                ],
                vec![
                    "dep".to_owned(),
                    "add".to_owned(),
                    "bead-new".to_owned(),
                    "active-bead".to_owned()
                ],
            ],
            "the final health check should stop before sync_flush runs"
        );
        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_creates_bead_with_expected_fields(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-create".to_owned(),
                name: "PN create".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let runner = MockBrRunner::new(vec![
            MockBrRunner::success("synced"),
            MockBrRunner::success("dependency added"),
            MockBrRunner::success(
                r###"{"id":"bead-new","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend","observability"],"description":"## Finding Summary\nRetry paths lack telemetry\n\n## Proposed Scope\nInstrument retry loops with counters and histograms\n\n## Rationale\nNo existing bead covers retry observability","dependencies":[],"dependents":[]}"###,
            ),
            MockBrRunner::success("created bead-new"),
            MockBrRunner::success(
                r#"{"id":"active-bead","title":"Active bead","status":"open","priority":1,"bead_type":"task","labels":["backend","observability"]}"#,
            ),
            MockBrRunner::success(&list_all_stdout(vec![])),
        ]);
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "No existing bead covers retry observability".to_owned(),
            run_id: Some("run-123".to_owned()),
            completion_round: Some(4),
        };

        let mut created_in_pass = 0usize;
        let outcome = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await?;

        assert_eq!(
            outcome,
            ProposeNewBeadOutcome::Created {
                bead_id: "bead-new".to_owned()
            }
        );
        assert_eq!(created_in_pass, 1);

        let commands = command_log.lock().expect("command log");
        assert!(commands.iter().any(|args| {
            args.iter().any(|arg| arg == "--title=Add retry telemetry")
                && args.iter().any(|arg| arg == "--priority=2")
                && args.iter().any(|arg| arg == "--labels=backend,observability")
                && args.iter().any(|arg| {
                    arg == "--description=## Finding Summary\nRetry paths lack telemetry\n\n## Proposed Scope\nInstrument retry loops with counters and histograms\n\n## Rationale\nNo existing bead covers retry observability"
                })
        }));
        assert!(commands
            .iter()
            .any(|args| args == &["dep", "add", "bead-new", "active-bead"]));

        let journal = FsMilestoneJournalStore.read_journal(base, &record.id)?;
        let created_event = journal
            .iter()
            .find(|event| event.event_type == MilestoneEventType::ProposedBeadCreated)
            .expect("proposed bead event");
        assert_eq!(
            created_event.details.as_deref(),
            Some("Retry paths lack telemetry")
        );
        let metadata = created_event.metadata.as_ref().expect("metadata");
        assert_eq!(metadata["created_bead_id"], "bead-new");
        assert_eq!(metadata["proposed_title"], "Add retry telemetry");
        assert_eq!(
            metadata["no_existing_match_reason"],
            "no existing bead matched proposed title 'Add retry telemetry'"
        );
        assert_eq!(metadata["placement"], "created_bead_depends_on_active_bead");
        assert_eq!(metadata["dependency_from_bead_id"], "bead-new");
        assert_eq!(metadata["dependency_depends_on_bead_id"], "active-bead");

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_reclassifies_existing_title_as_planned_elsewhere(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-reclassify".to_owned(),
                name: "PN reclassify".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "No existing bead covers retry observability".to_owned(),
            run_id: Some("run-456".to_owned()),
            completion_round: Some(2),
        };

        // MockBrRunner::pop() consumes from the END, so responses are in
        // REVERSE call order (last element = first call consumed).
        // Call order:
        // 1. list_matching_beads_by_title → br list --all (array)
        // 2. br show existing-bead (first loop — semantic match succeeds)
        // 3. br update (add dependency on active-bead)
        // 4. br sync --flush-only
        let rendered_description = format!(
            "## Finding Summary\n{}\n\n## Proposed Scope\n{}\n\n## Rationale\n{}",
            input.finding_summary, input.proposed_scope, input.rationale
        );
        let runner = MockBrRunner::new(vec![
            // 4. br sync --flush-only (consumed first from end = last call)
            MockBrRunner::success("Synced"),
            // 3. br update (add dependency)
            MockBrRunner::success("Updated existing-bead"),
            // 2. br show existing-bead (semantic match succeeds)
            MockBrRunner::success(
                &serde_json::json!({
                    "id": "existing-bead",
                    "title": "Add retry telemetry",
                    "status": "open",
                    "priority": 2,
                    "bead_type": "task",
                    "labels": ["backend"],
                    "description": rendered_description,
                    "dependencies": [],
                    "dependents": []
                })
                .to_string(),
            ),
            // 1. br list --all (consumed last from end = first call)
            MockBrRunner::success(
                r#"[{"id":"existing-bead","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"]}]"#,
            ),
        ]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));

        let mut created_in_pass = 0usize;
        let outcome = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await?;

        // When an existing bead with matching title semantically matches the proposed
        // work, the handler recovers it as a previously created bead (idempotent path).
        assert_eq!(
            outcome,
            ProposeNewBeadOutcome::Created {
                bead_id: "existing-bead".to_owned()
            }
        );
        assert_eq!(created_in_pass, 0);

        // The journal should record a ProposedBeadCreated event for the recovered bead.
        let journal = FsMilestoneJournalStore.read_journal(base, &record.id)?;
        assert!(journal
            .iter()
            .any(|event| event.event_type == MilestoneEventType::ProposedBeadCreated));

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_reclassifies_deferred_title_as_planned_elsewhere(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-reclassify-deferred".to_owned(),
                name: "PN reclassify deferred".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Deferred work should still suppress duplicate creation".to_owned(),
            run_id: Some("run-deferred".to_owned()),
            completion_round: Some(2),
        };

        // MockBrRunner::pop() consumes from the END (reverse order).
        let rendered_description = format!(
            "## Finding Summary\n{}\n\n## Proposed Scope\n{}\n\n## Rationale\n{}",
            input.finding_summary, input.proposed_scope, input.rationale
        );
        let runner = MockBrRunner::new(vec![
            // 4. br sync --flush-only
            MockBrRunner::success("Synced"),
            // 3. br update (add dependency)
            MockBrRunner::success("Updated existing-deferred"),
            // 2. br show existing-deferred (semantic match succeeds)
            MockBrRunner::success(
                &serde_json::json!({
                    "id": "existing-deferred",
                    "title": "Add retry telemetry",
                    "status": "deferred",
                    "priority": 2,
                    "bead_type": "task",
                    "labels": ["backend"],
                    "description": rendered_description,
                    "dependencies": [],
                    "dependents": []
                })
                .to_string(),
            ),
            // 1. br list --all
            MockBrRunner::success(
                r#"[{"id":"existing-deferred","title":"Add retry telemetry","status":"deferred","priority":2,"bead_type":"task","labels":["backend"]}]"#,
            ),
        ]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));

        let mut created_in_pass = 0usize;
        let outcome = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await?;

        assert_eq!(
            outcome,
            ProposeNewBeadOutcome::Created {
                bead_id: "existing-deferred".to_owned()
            }
        );
        assert_eq!(created_in_pass, 0);

        // Recovered bead is recorded in journal, not as planned-elsewhere mapping.
        let journal = FsMilestoneJournalStore.read_journal(base, &record.id)?;
        assert!(journal
            .iter()
            .any(|event| event.event_type == MilestoneEventType::ProposedBeadCreated));

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_ignores_closed_duplicate_match(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-ignore-closed".to_owned(),
                name: "PN ignore closed".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let runner = MockBrRunner::new(vec![
            MockBrRunner::success("synced"),
            MockBrRunner::success("dependency added"),
            MockBrRunner::success(
                r###"{"id":"bead-new","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"],"description":"## Finding Summary\nRetry paths lack telemetry\n\n## Proposed Scope\nInstrument retry loops with counters and histograms\n\n## Rationale\nClosed work should not suppress follow-up creation","dependencies":[],"dependents":[]}"###,
            ),
            MockBrRunner::success("created bead-new"),
            MockBrRunner::success(
                r#"{"id":"active-bead","title":"Active bead","status":"open","priority":1,"bead_type":"task","labels":["backend"]}"#,
            ),
            MockBrRunner::success(
                r#"[{"id":"closed-bead","title":"Add retry telemetry","status":"closed","priority":2,"bead_type":"task","labels":["backend"]}]"#,
            ),
        ]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Closed work should not suppress follow-up creation".to_owned(),
            run_id: Some("run-closed".to_owned()),
            completion_round: Some(3),
        };

        let mut created_in_pass = 0usize;
        let outcome = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await?;

        assert_eq!(
            outcome,
            ProposeNewBeadOutcome::Created {
                bead_id: "bead-new".to_owned()
            }
        );
        assert_eq!(created_in_pass, 1);

        let mappings = load_planned_elsewhere_mappings(
            &FsPlannedElsewhereMappingStore,
            &FsMilestoneJournalStore,
            base,
            &record.id,
        )?;
        assert!(mappings.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_ignores_active_bead_title_match(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-ignore-self".to_owned(),
                name: "PN ignore self".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let runner = MockBrRunner::new(vec![
            MockBrRunner::success("synced"),
            MockBrRunner::success("dependency added"),
            MockBrRunner::success(
                r###"{"id":"bead-newer","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"],"description":"## Finding Summary\nNeed a separate follow-up bead\n\n## Proposed Scope\nTrack work separately from the active bead\n\n## Rationale\nMatching the active bead itself should not count as planned elsewhere","dependencies":[],"dependents":[]}"###,
            ),
            MockBrRunner::success("created bead-newer"),
            MockBrRunner::success(
                r#"{"id":"active-bead","title":"Add retry telemetry","status":"open","priority":1,"bead_type":"task","labels":["backend"]}"#,
            ),
            MockBrRunner::success(
                r#"[{"id":"active-bead","title":"Add retry telemetry","status":"open","priority":1,"bead_type":"task","labels":["backend"]}]"#,
            ),
        ]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Need a separate follow-up bead".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Track work separately from the active bead".to_owned(),
            severity: Severity::Low,
            rationale: "Matching the active bead itself should not count as planned elsewhere"
                .to_owned(),
            run_id: Some("run-self".to_owned()),
            completion_round: Some(6),
        };

        let mut created_in_pass = 0usize;
        let outcome = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await?;

        assert_eq!(
            outcome,
            ProposeNewBeadOutcome::Created {
                bead_id: "bead-newer".to_owned()
            }
        );
        assert_eq!(created_in_pass, 1);

        let mappings = load_planned_elsewhere_mappings(
            &FsPlannedElsewhereMappingStore,
            &FsMilestoneJournalStore,
            base,
            &record.id,
        )?;
        assert!(mappings.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_ignores_title_collision_without_semantic_match(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-ignore-title-collision".to_owned(),
                name: "PN ignore title collision".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let runner = MockBrRunner::new(vec![
            MockBrRunner::success("synced"),
            MockBrRunner::success("dependency added"),
            MockBrRunner::success(
                r###"{"id":"bead-new-collision","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"],"description":"## Finding Summary\nRetry paths lack telemetry\n\n## Proposed Scope\nInstrument retry loops with counters and histograms\n\n## Rationale\nA generic title collision must not suppress new work","dependencies":[],"dependents":[]}"###,
            ),
            MockBrRunner::success("created bead-new-collision"),
            MockBrRunner::success(
                r#"{"id":"active-bead","title":"Active bead","status":"open","priority":1,"bead_type":"task","labels":["backend"]}"#,
            ),
            MockBrRunner::success(
                r#"{"id":"existing-bead","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"],"description":"Some other follow-up with the same title","dependencies":[],"dependents":[]}"#,
            ),
            MockBrRunner::success(
                r#"{"id":"existing-bead","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"],"description":"Some other follow-up with the same title","dependencies":[],"dependents":[]}"#,
            ),
            MockBrRunner::success(
                r#"[{"id":"existing-bead","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"]}]"#,
            ),
        ]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "A generic title collision must not suppress new work".to_owned(),
            run_id: Some("run-title-collision".to_owned()),
            completion_round: Some(4),
        };

        let mut created_in_pass = 0usize;
        let outcome = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await?;

        assert_eq!(
            outcome,
            ProposeNewBeadOutcome::Created {
                bead_id: "bead-new-collision".to_owned()
            }
        );
        assert_eq!(created_in_pass, 1);
        assert!(load_planned_elsewhere_mappings(
            &FsPlannedElsewhereMappingStore,
            &FsMilestoneJournalStore,
            base,
            &record.id,
        )?
        .is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_logs_threshold_warning_after_third_creation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-threshold".to_owned(),
                name: "PN threshold".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let runner = MockBrRunner::new(vec![
            MockBrRunner::success("synced"),
            MockBrRunner::success("dependency added"),
            MockBrRunner::success(
                r###"{"id":"bead-third","title":"Add third follow-up","status":"open","priority":3,"bead_type":"task","labels":["backend"],"description":"## Finding Summary\nThird missing follow-up\n\n## Proposed Scope\nCreate a third scoped follow-up bead\n\n## Rationale\nConservative threshold should still allow creation","dependencies":[],"dependents":[]}"###,
            ),
            MockBrRunner::success("created bead-third"),
            MockBrRunner::success(
                r#"{"id":"active-bead","title":"Active bead","status":"open","priority":1,"bead_type":"task","labels":["backend"]}"#,
            ),
            MockBrRunner::success(&list_all_stdout(vec![])),
        ]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));
        let capture = log_capture();
        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Third missing follow-up".to_owned(),
            proposed_title: "Add third follow-up".to_owned(),
            proposed_scope: "Create a third scoped follow-up bead".to_owned(),
            severity: Severity::Low,
            rationale: "Conservative threshold should still allow creation".to_owned(),
            run_id: Some("run-789".to_owned()),
            completion_round: Some(5),
        };

        let mut created_in_pass = 2usize;
        capture
            .in_scope_async(async {
                handle_propose_new_bead(
                    &FsMilestoneJournalStore,
                    &FsPlannedElsewhereMappingStore,
                    &br_mutation,
                    base,
                    &record.id,
                    &input,
                    &mut created_in_pass,
                    now,
                )
                .await
            })
            .await?;

        capture.assert_event_has_fields(&[("level", "ERROR"), ("created_in_pass", "3")]);

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_repairs_existing_journaled_creation_before_reuse(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-journal-replay".to_owned(),
                name: "PN journal replay".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "No existing bead covers retry observability".to_owned(),
            run_id: Some("run-replay".to_owned()),
            completion_round: Some(7),
        };
        record_proposed_bead_created_event(
            &FsMilestoneJournalStore,
            base,
            &record.id,
            &input,
            "bead-replayed",
            "replayed from prior success reconciliation",
            now,
        )?;

        let runner = MockBrRunner::new(vec![
            MockBrRunner::success("synced"),
            MockBrRunner::success("dependency added"),
            MockBrRunner::success(
                r###"{"id":"bead-replayed","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"],"description":"## Finding Summary\nRetry paths lack telemetry\n\n## Proposed Scope\nInstrument retry loops with counters and histograms\n\n## Rationale\nNo existing bead covers retry observability","dependencies":[],"dependents":[]}"###,
            ),
        ]);
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));

        let mut created_in_pass = 0usize;
        let outcome = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await?;

        assert_eq!(
            outcome,
            ProposeNewBeadOutcome::Created {
                bead_id: "bead-replayed".to_owned()
            }
        );
        assert_eq!(created_in_pass, 0);
        let commands = command_log.lock().expect("command log");
        assert!(commands
            .iter()
            .any(|args| args == &["show", "bead-replayed", "--json"]));
        assert!(commands
            .iter()
            .any(|args| args == &["dep", "add", "bead-replayed", "active-bead"]));
        let journal = FsMilestoneJournalStore.read_journal(base, &record.id)?;
        assert_eq!(
            journal
                .iter()
                .filter(|event| event.event_type == MilestoneEventType::ProposedBeadCreated)
                .count(),
            1
        );

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_reuses_existing_journaled_bead_without_mutation_health_gate(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        std::fs::write(
            base.join(".beads/issues.jsonl"),
            "<<<<<<< HEAD\n{\"id\":\"bead-a\"}\n=======\n{\"id\":\"bead-b\"}\n>>>>>>> branch\n",
        )?;
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-journal-reuse-conflict".to_owned(),
                name: "PN journal reuse conflict".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Replays should reuse the recorded bead without a new mutation".to_owned(),
            run_id: Some("run-journal-reuse-conflict".to_owned()),
            completion_round: Some(10),
        };
        record_proposed_bead_created_event(
            &FsMilestoneJournalStore,
            base,
            &record.id,
            &input,
            "bead-replayed",
            "replayed from prior success reconciliation",
            now,
        )?;

        let runner = MockBrRunner::new(vec![MockBrRunner::success(
            &serde_json::json!({
                "id": "bead-replayed",
                "title": "Add retry telemetry",
                "status": "open",
                "priority": 2,
                "bead_type": "task",
                "labels": ["backend"],
                "description": render_proposed_bead_description(&input),
                "dependencies": [{
                    "id": "active-bead",
                    "kind": "blocks",
                    "title": "Active bead",
                    "status": "open"
                }],
                "dependents": []
            })
            .to_string(),
        )]);
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));

        let mut created_in_pass = 0usize;
        let outcome = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await?;

        assert_eq!(
            outcome,
            ProposeNewBeadOutcome::Created {
                bead_id: "bead-replayed".to_owned()
            }
        );
        assert_eq!(created_in_pass, 0);
        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.as_slice(),
            &[vec!["show".to_owned(), "bead-replayed".to_owned(), "--json".to_owned()]],
            "idempotent replay should not be blocked by mutation health or issue any new br mutation"
        );

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_flushes_recovered_dirty_state_before_reusing_journaled_bead(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let adapter_id = "proposal-replay-owner";
        let own_record = base.join(format!(".beads/.br-unsynced-mutations.d/{adapter_id}.json"));
        std::fs::create_dir_all(
            own_record
                .parent()
                .expect("own pending record must have a parent dir"),
        )?;
        std::fs::write(
            &own_record,
            r#"{"adapter_id":"proposal-replay-owner","operation":"create_bead","bead_id":"bead-replayed","status":null}"#,
        )?;
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-journal-reuse-dirty".to_owned(),
                name: "PN journal reuse dirty".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Recovered reuse must replay pending br sync state".to_owned(),
            run_id: Some("run-journal-reuse-dirty".to_owned()),
            completion_round: Some(11),
        };
        record_proposed_bead_created_event(
            &FsMilestoneJournalStore,
            base,
            &record.id,
            &input,
            "bead-replayed",
            "replayed from prior success reconciliation",
            now,
        )?;

        let runner = MockBrRunner::new(vec![
            MockBrRunner::success("synced"),
            MockBrRunner::success(
                &serde_json::json!({
                    "id": "bead-replayed",
                    "title": "Add retry telemetry",
                    "status": "open",
                    "priority": 2,
                    "bead_type": "task",
                    "labels": ["backend"],
                    "description": render_proposed_bead_description(&input),
                    "dependencies": [{
                        "id": "active-bead",
                        "kind": "blocks",
                        "title": "Active bead",
                        "status": "open"
                    }],
                    "dependents": []
                })
                .to_string(),
            ),
        ]);
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter_id(
            BrAdapter::with_runner(runner).with_working_dir(base.to_path_buf()),
            adapter_id,
        );

        let mut created_in_pass = 0usize;
        let outcome = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await?;

        assert_eq!(
            outcome,
            ProposeNewBeadOutcome::Created {
                bead_id: "bead-replayed".to_owned()
            }
        );
        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.as_slice(),
            &[
                vec![
                    "show".to_owned(),
                    "bead-replayed".to_owned(),
                    "--json".to_owned()
                ],
                vec!["sync".to_owned(), "--flush-only".to_owned()],
            ],
            "replay should flush recovered pending mutations before returning success"
        );
        assert!(
            !own_record.exists(),
            "successful recovered flush should clear the owned pending record"
        );

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_blocks_recovered_dirty_replay_for_journaled_bead_when_beads_export_is_missing(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        std::fs::write(base.join(".beads/.br-unsynced-mutations"), "pending\n")?;
        std::fs::remove_file(base.join(".beads/issues.jsonl"))?;
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-journal-reuse-missing-beads".to_owned(),
                name: "PN journal reuse missing beads".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Recovered replay must refuse missing bead exports".to_owned(),
            run_id: Some("run-journal-reuse-missing-beads".to_owned()),
            completion_round: Some(13),
        };
        record_proposed_bead_created_event(
            &FsMilestoneJournalStore,
            base,
            &record.id,
            &input,
            "bead-replayed",
            "replayed from prior success reconciliation",
            now,
        )?;

        let runner = MockBrRunner::new(vec![MockBrRunner::success(
            &serde_json::json!({
                "id": "bead-replayed",
                "title": "Add retry telemetry",
                "status": "open",
                "priority": 2,
                "bead_type": "task",
                "labels": ["backend"],
                "description": render_proposed_bead_description(&input),
                "dependencies": [{
                    "id": "active-bead",
                    "kind": "blocks",
                    "title": "Active bead",
                    "status": "open"
                }],
                "dependents": []
            })
            .to_string(),
        )]);
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter(
            BrAdapter::with_runner(runner).with_working_dir(base.to_path_buf()),
        );

        let mut created_in_pass = 0usize;
        let error = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await
        .expect_err("missing issues.jsonl should block recovered replay sync");

        match error {
            AppError::MilestoneOperationFailed {
                action, details, ..
            } => {
                assert_eq!(action, "sync recovered journaled proposed bead replay");
                assert!(details.contains("missing .beads/issues.jsonl"));
            }
            other => panic!("unexpected error: {other}"),
        }

        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.as_slice(),
            &[vec![
                "show".to_owned(),
                "bead-replayed".to_owned(),
                "--json".to_owned()
            ]],
            "recovered replay should stop before br sync when the beads export is missing"
        );
        assert!(
            base.join(".beads/.br-unsynced-mutations").exists(),
            "blocked replay must leave the pending marker in place for later recovery"
        );

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_rechecks_repo_pending_state_before_recovered_replay_sync(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-journal-reuse-late-dirty".to_owned(),
                name: "PN journal reuse late dirty".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Recovered replay must recheck repo pending state under the sync lock"
                .to_owned(),
            run_id: Some("run-journal-reuse-late-dirty".to_owned()),
            completion_round: Some(15),
        };
        record_proposed_bead_created_event(
            &FsMilestoneJournalStore,
            base,
            &record.id,
            &input,
            "bead-replayed",
            "replayed from prior success reconciliation",
            now,
        )?;

        let runner = MockBrRunner::new(vec![MockBrRunner::success(
            &serde_json::json!({
                "id": "bead-replayed",
                "title": "Add retry telemetry",
                "status": "open",
                "priority": 2,
                "bead_type": "task",
                "labels": ["backend"],
                "description": render_proposed_bead_description(&input),
                "dependencies": [{
                    "id": "active-bead",
                    "kind": "blocks",
                    "title": "Active bead",
                    "status": "open"
                }],
                "dependents": []
            })
            .to_string(),
        )]);
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter(
            BrAdapter::with_runner(runner).with_working_dir(base.to_path_buf()),
        );

        std::fs::write(base.join(".beads/.br-unsynced-mutations"), "pending\n")?;
        std::fs::write(
            base.join(".beads/issues.jsonl"),
            "<<<<<<< HEAD\n{\"id\":\"bead-a\"}\n=======\n{\"id\":\"bead-b\"}\n>>>>>>> branch\n",
        )?;

        let mut created_in_pass = 0usize;
        let error = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await
        .expect_err("late recovered dirtiness should still be health-gated before replay sync");

        match error {
            AppError::MilestoneOperationFailed {
                action, details, ..
            } => {
                assert_eq!(action, "sync recovered journaled proposed bead replay");
                assert!(
                    details.contains("resolve the conflict"),
                    "error should direct the operator to resolve the conflicted export: {details}"
                );
            }
            other => panic!("unexpected error: {other}"),
        }

        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.as_slice(),
            &[vec![
                "show".to_owned(),
                "bead-replayed".to_owned(),
                "--json".to_owned()
            ]],
            "late recovered replay should stop before br sync when the export becomes unsafe"
        );
        assert!(
            base.join(".beads/.br-unsynced-mutations").exists(),
            "blocked replay must preserve the recovered pending marker"
        );

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_recreates_missing_journaled_bead(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-journal-missing".to_owned(),
                name: "PN journal missing".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "No existing bead covers retry observability".to_owned(),
            run_id: Some("run-journal-missing".to_owned()),
            completion_round: Some(9),
        };
        record_proposed_bead_created_event(
            &FsMilestoneJournalStore,
            base,
            &record.id,
            &input,
            "bead-missing",
            "stale journal entry",
            now,
        )?;

        let runner = MockBrRunner::new(vec![
            MockBrRunner::success("synced"),
            MockBrRunner::success("dependency added"),
            MockBrRunner::success(
                r###"{"id":"bead-new","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"],"description":"## Finding Summary\nRetry paths lack telemetry\n\n## Proposed Scope\nInstrument retry loops with counters and histograms\n\n## Rationale\nNo existing bead covers retry observability","dependencies":[],"dependents":[]}"###,
            ),
            MockBrRunner::success("created bead-new"),
            MockBrRunner::success(
                r#"{"id":"active-bead","title":"Active bead","status":"open","priority":1,"bead_type":"task","labels":["backend"]}"#,
            ),
            MockBrRunner::success(&list_all_stdout(vec![])),
            MockBrRunner::error(1, "bead not found"),
        ]);
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));

        let mut created_in_pass = 0usize;
        let outcome = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await?;

        assert_eq!(
            outcome,
            ProposeNewBeadOutcome::Created {
                bead_id: "bead-new".to_owned()
            }
        );
        assert_eq!(created_in_pass, 1);

        let journal = FsMilestoneJournalStore.read_journal(base, &record.id)?;
        let latest_created = journal
            .iter()
            .rev()
            .find(|event| event.event_type == MilestoneEventType::ProposedBeadCreated)
            .expect("latest created event");
        let metadata = latest_created.metadata.as_ref().expect("metadata");
        assert_eq!(metadata["created_bead_id"], "bead-new");

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_recovers_existing_created_bead_and_repairs_dependency(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-recover-existing".to_owned(),
                name: "PN recover existing".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "No existing bead covers retry observability".to_owned(),
            run_id: Some("run-recover".to_owned()),
            completion_round: Some(8),
        };
        let recovered_detail = serde_json::json!({
            "id": "bead-recovered",
            "title": "Add retry telemetry",
            "status": "open",
            "priority": 2,
            "bead_type": "task",
            "labels": ["backend"],
            "description": render_proposed_bead_description(&input),
            "dependencies": [],
            "dependents": []
        })
        .to_string();
        let runner = MockBrRunner::new(vec![
            MockBrRunner::success("synced"),
            MockBrRunner::success("dependency added"),
            MockBrRunner::success(&recovered_detail),
            MockBrRunner::success(
                r#"[{"id":"bead-recovered","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"]}]"#,
            ),
        ]);
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter(BrAdapter::with_runner(runner));

        let mut created_in_pass = 0usize;
        let outcome = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await?;

        assert_eq!(
            outcome,
            ProposeNewBeadOutcome::Created {
                bead_id: "bead-recovered".to_owned()
            }
        );
        assert_eq!(created_in_pass, 0);
        assert!(load_planned_elsewhere_mappings(
            &FsPlannedElsewhereMappingStore,
            &FsMilestoneJournalStore,
            base,
            &record.id,
        )?
        .is_empty());
        let commands = command_log.lock().expect("command log");
        assert!(commands
            .iter()
            .any(|args| args == &["dep", "add", "bead-recovered", "active-bead"]));

        let journal = FsMilestoneJournalStore.read_journal(base, &record.id)?;
        let created_event = journal
            .iter()
            .find(|event| event.event_type == MilestoneEventType::ProposedBeadCreated)
            .expect("created event");
        let metadata = created_event.metadata.as_ref().expect("metadata");
        assert_eq!(metadata["created_bead_id"], "bead-recovered");
        assert_eq!(
            metadata["no_existing_match_reason"],
            "recovered previously created bead by matching title and rendered proposal payload"
        );

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_rechecks_health_before_sync_after_journaled_dependency_repair(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-journal-repair-conflict-before-sync".to_owned(),
                name: "PN journal repair conflict before sync".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Journaled dependency repair should recheck bead health under the sync lock"
                .to_owned(),
            run_id: Some("run-journal-repair-conflict-before-sync".to_owned()),
            completion_round: Some(16),
        };
        record_proposed_bead_created_event(
            &FsMilestoneJournalStore,
            base,
            &record.id,
            &input,
            "bead-replayed",
            "replayed from prior success reconciliation",
            now,
        )?;

        let issues_path = base.join(".beads/issues.jsonl");
        let runner = ScriptedBrRunner::new({
            let issues_path = issues_path.clone();
            let expected_detail = serde_json::json!({
                "id": "bead-replayed",
                "title": "Add retry telemetry",
                "status": "open",
                "priority": 2,
                "bead_type": "task",
                "labels": ["backend"],
                "description": render_proposed_bead_description(&input),
                "dependencies": [],
                "dependents": []
            })
            .to_string();
            move |args| match args {
                [command, bead_id, ..] if command == "show" && bead_id == "bead-replayed" => {
                    MockBrRunner::success(&expected_detail)
                }
                [command, subcommand, ..] if command == "dep" && subcommand == "add" => {
                    std::fs::write(
                        &issues_path,
                        "<<<<<<< HEAD\n{\"id\":\"bead-a\"}\n=======\n{\"id\":\"bead-b\"}\n>>>>>>> branch\n",
                    )?;
                    MockBrRunner::success("dependency added")
                }
                other => panic!("unexpected args: {other:?}"),
            }
        });
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter(
            BrAdapter::with_runner(runner).with_working_dir(base.to_path_buf()),
        );

        let mut created_in_pass = 0usize;
        let error = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await
        .expect_err("conflicted export should block the repaired journaled dependency flush");

        match error {
            AppError::MilestoneOperationFailed {
                action, details, ..
            } => {
                assert_eq!(action, "sync journaled proposed bead dependency repair");
                assert!(
                    details.contains("resolve the conflict"),
                    "error should direct the operator to resolve the conflicted export: {details}"
                );
            }
            other => panic!("unexpected error: {other}"),
        }

        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.as_slice(),
            &[
                vec![
                    "show".to_owned(),
                    "bead-replayed".to_owned(),
                    "--json".to_owned()
                ],
                vec![
                    "dep".to_owned(),
                    "add".to_owned(),
                    "bead-replayed".to_owned(),
                    "active-bead".to_owned()
                ],
            ],
            "the guarded sync should stop before br sync --flush-only runs"
        );

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_rejects_foreign_pending_dependency_repair_flush(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-journal-repair-foreign-pending".to_owned(),
                name: "PN journal repair foreign pending".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Dependency repair must not flush another workflow's pending bead changes"
                .to_owned(),
            run_id: Some("run-journal-repair-foreign-pending".to_owned()),
            completion_round: Some(18),
        };
        record_proposed_bead_created_event(
            &FsMilestoneJournalStore,
            base,
            &record.id,
            &input,
            "bead-replayed",
            "replayed from prior success reconciliation",
            now,
        )?;

        let foreign_record = base.join(".beads/.br-unsynced-mutations.d/foreign.json");
        let runner = ScriptedBrRunner::new({
            let foreign_record = foreign_record.clone();
            let expected_detail = serde_json::json!({
                "id": "bead-replayed",
                "title": "Add retry telemetry",
                "status": "open",
                "priority": 2,
                "bead_type": "task",
                "labels": ["backend"],
                "description": render_proposed_bead_description(&input),
                "dependencies": [],
                "dependents": []
            })
            .to_string();
            move |args| match args {
                [command, bead_id, ..] if command == "show" && bead_id == "bead-replayed" => {
                    MockBrRunner::success(&expected_detail)
                }
                [command, subcommand, ..] if command == "dep" && subcommand == "add" => {
                    std::fs::create_dir_all(
                        foreign_record
                            .parent()
                            .expect("foreign record path must have a parent dir"),
                    )?;
                    std::fs::write(
                        &foreign_record,
                        r#"{"adapter_id":"other-workflow","operation":"create_bead","bead_id":"bead-foreign","status":null}"#,
                    )?;
                    MockBrRunner::success("dependency added")
                }
                other => panic!("unexpected args: {other:?}"),
            }
        });
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter_id(
            BrAdapter::with_runner(runner).with_working_dir(base.to_path_buf()),
            "proposal-owner",
        );

        let mut created_in_pass = 0usize;
        let error = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await
        .expect_err("foreign pending bead work must block the dependency repair flush");

        match error {
            AppError::MilestoneOperationFailed {
                action, details, ..
            } => {
                assert_eq!(action, "sync journaled proposed bead dependency repair");
                assert!(
                    details.contains("another local bead workflow still has pending `create_bead`"),
                    "error should explain the foreign pending mutation: {details}"
                );
            }
            other => panic!("unexpected error: {other}"),
        }

        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.as_slice(),
            &[
                vec![
                    "show".to_owned(),
                    "bead-replayed".to_owned(),
                    "--json".to_owned()
                ],
                vec![
                    "dep".to_owned(),
                    "add".to_owned(),
                    "bead-replayed".to_owned(),
                    "active-bead".to_owned()
                ],
            ],
            "owned-only sync should stop before br sync --flush-only runs"
        );
        assert!(
            foreign_record.exists(),
            "blocking the flush must preserve the foreign pending journal"
        );

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_rechecks_health_before_sync_after_recovered_dependency_repair(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-recover-existing-conflict-before-sync".to_owned(),
                name: "PN recover existing conflict before sync".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Recovered dependency repair should recheck bead health under the sync lock"
                .to_owned(),
            run_id: Some("run-recover-existing-conflict-before-sync".to_owned()),
            completion_round: Some(17),
        };

        let issues_path = base.join(".beads/issues.jsonl");
        let runner = ScriptedBrRunner::new({
            let issues_path = issues_path.clone();
            let recovered_detail = serde_json::json!({
                "id": "bead-recovered",
                "title": "Add retry telemetry",
                "status": "open",
                "priority": 2,
                "bead_type": "task",
                "labels": ["backend"],
                "description": render_proposed_bead_description(&input),
                "dependencies": [],
                "dependents": []
            })
            .to_string();
            move |args| match args {
                [command, ..] if command == "list" => MockBrRunner::success(
                    r#"[{"id":"bead-recovered","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"]}]"#,
                ),
                [command, bead_id, ..] if command == "show" && bead_id == "bead-recovered" => {
                    MockBrRunner::success(&recovered_detail)
                }
                [command, subcommand, ..] if command == "dep" && subcommand == "add" => {
                    std::fs::write(&issues_path, "{\"id\":\"bead-recovered\"}\n{\"id\": }\n")?;
                    MockBrRunner::success("dependency added")
                }
                other => panic!("unexpected args: {other:?}"),
            }
        });
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter(
            BrAdapter::with_runner(runner).with_working_dir(base.to_path_buf()),
        );

        let mut created_in_pass = 0usize;
        let error = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await
        .expect_err("malformed export should block the recovered dependency repair flush");

        match error {
            AppError::MilestoneOperationFailed {
                action, details, ..
            } => {
                assert_eq!(action, "sync recovered proposed bead placement");
                assert!(
                    details.contains("malformed .beads/issues.jsonl line 2"),
                    "error should report the malformed export: {details}"
                );
            }
            other => panic!("unexpected error: {other}"),
        }

        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.as_slice(),
            &[
                vec![
                    "list".to_owned(),
                    "--all".to_owned(),
                    "--deferred".to_owned(),
                    "--limit=0".to_owned(),
                    "--json".to_owned(),
                ],
                vec![
                    "show".to_owned(),
                    "bead-recovered".to_owned(),
                    "--json".to_owned()
                ],
                vec![
                    "dep".to_owned(),
                    "add".to_owned(),
                    "bead-recovered".to_owned(),
                    "active-bead".to_owned()
                ],
            ],
            "the guarded sync should stop before br sync --flush-only runs"
        );

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_flushes_recovered_dirty_state_before_reusing_matching_created_bead(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let adapter_id = "proposal-replay-owner";
        let own_record = base.join(format!(".beads/.br-unsynced-mutations.d/{adapter_id}.json"));
        std::fs::create_dir_all(
            own_record
                .parent()
                .expect("own pending record must have a parent dir"),
        )?;
        std::fs::write(
            &own_record,
            r#"{"adapter_id":"proposal-replay-owner","operation":"create_bead","bead_id":"bead-recovered","status":null}"#,
        )?;
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-recover-existing-dirty".to_owned(),
                name: "PN recover existing dirty".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Recovered replay must flush pending create sync".to_owned(),
            run_id: Some("run-recover-existing-dirty".to_owned()),
            completion_round: Some(12),
        };
        let recovered_detail = serde_json::json!({
            "id": "bead-recovered",
            "title": "Add retry telemetry",
            "status": "open",
            "priority": 2,
            "bead_type": "task",
            "labels": ["backend"],
            "description": render_proposed_bead_description(&input),
            "dependencies": [{
                "id": "active-bead",
                "kind": "blocks",
                "title": "Active bead",
                "status": "open"
            }],
            "dependents": []
        })
        .to_string();
        let runner = MockBrRunner::new(vec![
            MockBrRunner::success("synced"),
            MockBrRunner::success(&recovered_detail),
            MockBrRunner::success(
                r#"[{"id":"bead-recovered","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"]}]"#,
            ),
        ]);
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter_id(
            BrAdapter::with_runner(runner).with_working_dir(base.to_path_buf()),
            adapter_id,
        );

        let mut created_in_pass = 0usize;
        let outcome = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await?;

        assert_eq!(
            outcome,
            ProposeNewBeadOutcome::Created {
                bead_id: "bead-recovered".to_owned()
            }
        );
        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.as_slice(),
            &[
                vec![
                    "list".to_owned(),
                    "--all".to_owned(),
                    "--deferred".to_owned(),
                    "--limit=0".to_owned(),
                    "--json".to_owned(),
                ],
                vec![
                    "show".to_owned(),
                    "bead-recovered".to_owned(),
                    "--json".to_owned()
                ],
                vec!["sync".to_owned(), "--flush-only".to_owned()],
            ],
            "recovered created-bead replay should flush pending local mutations before success"
        );
        assert!(
            !own_record.exists(),
            "successful recovered flush should clear the owned pending record"
        );

        Ok(())
    }

    #[tokio::test]
    async fn handle_propose_new_bead_blocks_recovered_dirty_replay_for_matching_created_bead_when_beads_export_is_conflicted(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        std::fs::write(base.join(".beads/.br-unsynced-mutations"), "pending\n")?;
        std::fs::write(
            base.join(".beads/issues.jsonl"),
            "<<<<<<< HEAD\n{\"id\":\"bead-a\"}\n=======\n{\"id\":\"bead-b\"}\n>>>>>>> branch\n",
        )?;
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pn-recover-existing-conflicted-beads".to_owned(),
                name: "PN recover existing conflicted beads".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "Recovered replay must refuse conflicted bead exports".to_owned(),
            run_id: Some("run-recover-existing-conflicted-beads".to_owned()),
            completion_round: Some(14),
        };
        let recovered_detail = serde_json::json!({
            "id": "bead-recovered",
            "title": "Add retry telemetry",
            "status": "open",
            "priority": 2,
            "bead_type": "task",
            "labels": ["backend"],
            "description": render_proposed_bead_description(&input),
            "dependencies": [{
                "id": "active-bead",
                "kind": "blocks",
                "title": "Active bead",
                "status": "open"
            }],
            "dependents": []
        })
        .to_string();
        let runner = MockBrRunner::new(vec![
            MockBrRunner::success(&recovered_detail),
            MockBrRunner::success(
                r#"[{"id":"bead-recovered","title":"Add retry telemetry","status":"open","priority":2,"bead_type":"task","labels":["backend"]}]"#,
            ),
        ]);
        let command_log = runner.command_log();
        let br_mutation = BrMutationAdapter::with_adapter(
            BrAdapter::with_runner(runner).with_working_dir(base.to_path_buf()),
        );

        let mut created_in_pass = 0usize;
        let error = handle_propose_new_bead(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            &br_mutation,
            base,
            &record.id,
            &input,
            &mut created_in_pass,
            now,
        )
        .await
        .expect_err("conflicted issues.jsonl should block recovered replay sync");

        match error {
            AppError::MilestoneOperationFailed {
                action, details, ..
            } => {
                assert_eq!(action, "sync recovered proposed bead replay");
                assert!(details.contains("resolve the conflict"));
            }
            other => panic!("unexpected error: {other}"),
        }

        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.as_slice(),
            &[
                vec![
                    "list".to_owned(),
                    "--all".to_owned(),
                    "--deferred".to_owned(),
                    "--limit=0".to_owned(),
                    "--json".to_owned(),
                ],
                vec![
                    "show".to_owned(),
                    "bead-recovered".to_owned(),
                    "--json".to_owned()
                ],
            ],
            "recovered replay should stop before br sync when the beads export is conflicted"
        );
        assert!(
            base.join(".beads/.br-unsynced-mutations").exists(),
            "blocked replay must leave the pending marker in place for later recovery"
        );

        Ok(())
    }

    #[tokio::test]
    async fn resolve_created_bead_id_fallback_ignores_closed_and_active_matches(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockBrRunner::new(vec![MockBrRunner::success(
            r#"[{"id":"closed-bead","title":"Add retry telemetry","status":"closed","priority":2,"bead_type":"task","labels":["backend"]},{"id":"active-bead","title":"Add retry telemetry","status":"open","priority":1,"bead_type":"task","labels":["backend"]}]"#,
        )]);
        let br_read = BrAdapter::with_runner(runner);
        let input = ProposeNewBeadInput {
            active_bead_id: "active-bead".to_owned(),
            finding_summary: "Retry paths lack telemetry".to_owned(),
            proposed_title: "Add retry telemetry".to_owned(),
            proposed_scope: "Instrument retry loops with counters and histograms".to_owned(),
            severity: Severity::Medium,
            rationale: "No existing bead covers retry observability".to_owned(),
            run_id: Some("run-resolve".to_owned()),
            completion_round: Some(10),
        };

        let error = resolve_created_bead_id(&br_read, &input, "")
            .await
            .expect_err("closed/self-only fallback should fail");

        assert!(
            error
                .to_string()
                .contains("created bead id could not be determined"),
            "unexpected error: {error}"
        );

        Ok(())
    }

    #[test]
    fn planned_elsewhere_sidecar_divergence_resolved_by_journal(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let store = FsMilestoneStore;
        let now = Utc::now();

        let record = create_milestone(
            &store,
            base,
            CreateMilestoneInput {
                id: "pe-diverge".to_owned(),
                name: "PE diverge".to_owned(),
                description: "test".to_owned(),
            },
            now,
        )?;

        // Write a mapping normally (both journal + NDJSON).
        let mapping1 = PlannedElsewhereMapping {
            active_bead_id: "bead-A".to_owned(),
            finding_summary: "first concern".to_owned(),
            mapped_to_bead_id: "bead-B".to_owned(),
            recorded_at: now,
            mapped_bead_verified: false,
            run_id: None,
            completion_round: None,
        };
        record_planned_elsewhere_mapping(
            &FsMilestoneJournalStore,
            &FsPlannedElsewhereMappingStore,
            base,
            &record.id,
            &mapping1,
        )?;

        // Simulate: journal append succeeds but NDJSON append fails for a second mapping.
        // Write only to journal (not NDJSON).
        let mapping2 = PlannedElsewhereMapping {
            active_bead_id: "bead-A".to_owned(),
            finding_summary: "second concern".to_owned(),
            mapped_to_bead_id: "bead-C".to_owned(),
            recorded_at: now + chrono::Duration::seconds(5),
            mapped_bead_verified: false,
            run_id: None,
            completion_round: None,
        };
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "active_bead_id".to_owned(),
            serde_json::Value::String(mapping2.active_bead_id.clone()),
        );
        metadata.insert(
            "mapped_to_bead_id".to_owned(),
            serde_json::Value::String(mapping2.mapped_to_bead_id.clone()),
        );
        metadata.insert(
            "mapped_bead_verified".to_owned(),
            serde_json::Value::Bool(false),
        );
        let mut event = MilestoneJournalEvent::new(
            MilestoneEventType::PlannedElsewhereMapped,
            mapping2.recorded_at,
        )
        .with_bead(mapping2.active_bead_id.clone())
        .with_details(mapping2.finding_summary.clone());
        event.metadata = Some(metadata);
        let line = event.to_ndjson_line()?;
        FsMilestoneJournalStore.append_event(base, &record.id, &line)?;
        // Deliberately skip NDJSON write — simulating sidecar failure.

        // Load should see both mappings (from journal), not just the one in NDJSON.
        let loaded = load_planned_elsewhere_mappings(
            &FsPlannedElsewhereMappingStore,
            &FsMilestoneJournalStore,
            base,
            &record.id,
        )?;
        assert_eq!(loaded.len(), 2, "journal-only mapping must be visible");
        let bead_ids: Vec<&str> = loaded
            .iter()
            .map(|m| m.mapped_to_bead_id.as_str())
            .collect();
        assert!(bead_ids.contains(&"bead-B"), "first mapping present");
        assert!(bead_ids.contains(&"bead-C"), "journal-only mapping present");
        Ok(())
    }

    #[test]
    fn bead_execution_history_returns_retries_with_duration(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 12, 0, 0)
            .single()
            .unwrap();
        let record = create_milestone_with_plan(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            "ms-alpha",
            "Alpha",
            now,
        )?;
        let current_plan_hash = hash_text(&FsMilestonePlanStore.read_plan_json(base, &record.id)?);

        let first_started = now + chrono::Duration::minutes(1);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
            "task-alpha",
            "run-1",
            &current_plan_hash,
            first_started,
        )?;
        record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
            "task-alpha",
            "run-1",
            Some(&current_plan_hash),
            TaskRunOutcome::Failed,
            Some("first attempt failed"),
            first_started,
            first_started + chrono::Duration::seconds(30),
        )?;

        let second_started = now + chrono::Duration::minutes(3);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
            "task-alpha",
            "run-2",
            &current_plan_hash,
            second_started,
        )?;
        record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
            "task-alpha",
            "run-2",
            Some(&current_plan_hash),
            TaskRunOutcome::Succeeded,
            Some("second attempt passed"),
            second_started,
            second_started + chrono::Duration::seconds(45),
        )?;

        let history = bead_execution_history(
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
        )?;

        assert_eq!(history.lineage.milestone_id, "ms-alpha");
        assert_eq!(history.lineage.milestone_name, "Alpha");
        assert_eq!(history.lineage.bead_id, "bead-1");
        assert_eq!(
            history.lineage.bead_title.as_deref(),
            Some("Implement feature")
        );
        assert_eq!(history.lineage.acceptance_criteria, vec!["Tests pass"]);
        assert_eq!(history.runs.len(), 2);
        assert_eq!(history.runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(history.runs[0].duration_ms, Some(30_000));
        assert_eq!(history.runs[1].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(history.runs[1].duration_ms, Some(45_000));
        Ok(())
    }

    #[test]
    fn bead_execution_history_omits_stale_plan_details_for_mixed_plan_hashes(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 12, 30, 0)
            .single()
            .unwrap();
        let record = create_milestone_with_plan(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            "ms-alpha",
            "Alpha",
            now,
        )?;

        let first_started = now + chrono::Duration::minutes(1);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
            "task-alpha",
            "run-1",
            "plan-v1",
            first_started,
        )?;
        record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
            "task-alpha",
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("first attempt failed"),
            first_started,
            first_started + chrono::Duration::seconds(30),
        )?;

        let second_started = now + chrono::Duration::minutes(3);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
            "task-alpha",
            "run-2",
            "plan-v2",
            second_started,
        )?;
        record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
            "task-alpha",
            "run-2",
            Some("plan-v2"),
            TaskRunOutcome::Succeeded,
            Some("second attempt passed"),
            second_started,
            second_started + chrono::Duration::seconds(45),
        )?;

        let history = bead_execution_history(
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
        )?;

        assert_eq!(history.lineage.milestone_id, "ms-alpha");
        assert_eq!(history.lineage.milestone_name, "Alpha");
        assert_eq!(history.lineage.bead_id, "bead-1");
        assert_eq!(history.lineage.bead_title, None);
        assert!(history.lineage.acceptance_criteria.is_empty());
        Ok(())
    }

    #[test]
    fn bead_execution_history_omits_plan_details_when_any_run_lacks_plan_hash(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 12, 45, 0)
            .single()
            .unwrap();
        let record = create_milestone_with_plan(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            "ms-alpha",
            "Alpha",
            now,
        )?;
        let current_plan_hash = hash_text(&FsMilestonePlanStore.read_plan_json(base, &record.id)?);

        let first_started = now + chrono::Duration::minutes(1);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
            "task-alpha",
            "run-1",
            &current_plan_hash,
            first_started,
        )?;
        record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
            "task-alpha",
            "run-1",
            Some(&current_plan_hash),
            TaskRunOutcome::Failed,
            Some("first attempt failed"),
            first_started,
            first_started + chrono::Duration::seconds(15),
        )?;

        let second_started = now + chrono::Duration::minutes(3);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
            "task-alpha",
            "run-2",
            &current_plan_hash,
            second_started,
        )?;
        let task_runs_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("task-runs.ndjson");
        let task_runs = std::fs::read_to_string(&task_runs_path)?.replace(
            &format!(r#""plan_hash":"{current_plan_hash}""#),
            r#""plan_hash":null"#,
        );
        std::fs::write(&task_runs_path, task_runs)?;

        let history = bead_execution_history(
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
        )?;

        assert_eq!(history.runs.len(), 2);
        assert_eq!(history.lineage.milestone_id, "ms-alpha");
        assert_eq!(history.lineage.bead_id, "bead-1");
        assert_eq!(history.lineage.bead_title, None);
        assert!(history.lineage.acceptance_criteria.is_empty());
        Ok(())
    }

    #[test]
    fn bead_execution_history_matches_short_and_qualified_bead_ids(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 12, 50, 0)
            .single()
            .unwrap();
        let record = create_milestone_with_plan(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            "ms-alpha",
            "Alpha",
            now,
        )?;
        let current_plan_hash = hash_text(&FsMilestonePlanStore.read_plan_json(base, &record.id)?);

        let first_started = now + chrono::Duration::minutes(1);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
            "task-alpha",
            "run-1",
            &current_plan_hash,
            first_started,
        )?;
        record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
            "task-alpha",
            "run-1",
            Some(&current_plan_hash),
            TaskRunOutcome::Succeeded,
            Some("completed"),
            first_started,
            first_started + chrono::Duration::seconds(30),
        )?;

        let short_history = bead_execution_history(
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
        )?;
        let qualified_history = bead_execution_history(
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "ms-alpha.bead-1",
        )?;

        assert_eq!(short_history.runs.len(), 1);
        assert_eq!(qualified_history.runs.len(), 1);
        assert_eq!(short_history.runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(qualified_history.runs[0].run_id.as_deref(), Some("run-1"));
        Ok(())
    }

    #[test]
    fn list_tasks_for_milestone_returns_only_linked_projects(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 13, 0, 0)
            .single()
            .unwrap();
        let record = create_milestone_with_plan(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            "ms-alpha",
            "Alpha",
            now,
        )?;
        let current_plan_hash = hash_text(&FsMilestonePlanStore.read_plan_json(base, &record.id)?);

        let create_linked_project = |project_id: &str,
                                     bead_id: &str,
                                     created_at: DateTime<Utc>|
         -> Result<(), Box<dyn std::error::Error>> {
            crate::contexts::project_run_record::service::create_project(
                &crate::adapters::fs::FsProjectStore,
                &crate::adapters::fs::FsJournalStore,
                base,
                crate::contexts::project_run_record::service::CreateProjectInput {
                    id: crate::shared::domain::ProjectId::new(project_id)?,
                    name: format!("Project {project_id}"),
                    flow: crate::shared::domain::FlowPreset::Standard,
                    prompt_path: "prompt.md".to_owned(),
                    prompt_contents: "# Prompt".to_owned(),
                    prompt_hash: "hash".to_owned(),
                    created_at,
                    task_source: Some(crate::contexts::project_run_record::model::TaskSource {
                        milestone_id: record.id.to_string(),
                        bead_id: bead_id.to_owned(),
                        parent_epic_id: None,
                        origin: crate::contexts::project_run_record::model::TaskOrigin::Milestone,
                        plan_hash: Some(current_plan_hash.clone()),
                        plan_version: Some(1),
                    }),
                },
            )?;
            Ok(())
        };

        create_linked_project("task-a", "bead-1", now + chrono::Duration::seconds(10))?;
        create_linked_project("task-b", "bead-2", now + chrono::Duration::seconds(20))?;
        crate::contexts::project_run_record::service::create_project(
            &crate::adapters::fs::FsProjectStore,
            &crate::adapters::fs::FsJournalStore,
            base,
            crate::contexts::project_run_record::service::CreateProjectInput {
                id: crate::shared::domain::ProjectId::new("standalone")?,
                name: "Standalone".to_owned(),
                flow: crate::shared::domain::FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: "hash".to_owned(),
                created_at: now + chrono::Duration::seconds(30),
                task_source: None,
            },
        )?;

        let listing = list_tasks_for_milestone(
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            &crate::adapters::fs::FsProjectStore,
            base,
            &record.id,
        )?;

        assert_eq!(listing.milestone_id, "ms-alpha");
        assert_eq!(listing.milestone_name, "Alpha");
        assert_eq!(listing.tasks.len(), 2);
        assert_eq!(listing.tasks[0].project_id, "task-a");
        assert_eq!(listing.tasks[0].bead_id, "bead-1");
        assert_eq!(
            listing.tasks[0].bead_title.as_deref(),
            Some("Implement feature")
        );
        assert_eq!(listing.tasks[1].project_id, "task-b");
        assert_eq!(listing.tasks[1].bead_id, "bead-2");
        assert_eq!(
            listing.tasks[1].bead_title.as_deref(),
            Some("Follow-up feature")
        );
        Ok(())
    }

    #[test]
    fn list_tasks_for_milestone_omits_stale_plan_titles() -> Result<(), Box<dyn std::error::Error>>
    {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 13, 30, 0)
            .single()
            .unwrap();
        let record = create_milestone_with_plan(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            "ms-alpha",
            "Alpha",
            now,
        )?;
        let current_plan_hash = hash_text(&FsMilestonePlanStore.read_plan_json(base, &record.id)?);

        for (project_id, bead_id, plan_hash, created_at) in [
            (
                "task-current",
                "bead-1",
                Some(current_plan_hash.as_str()),
                now + chrono::Duration::seconds(10),
            ),
            (
                "task-stale",
                "bead-2",
                Some("stale-plan-hash"),
                now + chrono::Duration::seconds(20),
            ),
        ] {
            crate::contexts::project_run_record::service::create_project(
                &crate::adapters::fs::FsProjectStore,
                &crate::adapters::fs::FsJournalStore,
                base,
                crate::contexts::project_run_record::service::CreateProjectInput {
                    id: crate::shared::domain::ProjectId::new(project_id)?,
                    name: format!("Project {project_id}"),
                    flow: crate::shared::domain::FlowPreset::Standard,
                    prompt_path: "prompt.md".to_owned(),
                    prompt_contents: "# Prompt".to_owned(),
                    prompt_hash: "hash".to_owned(),
                    created_at,
                    task_source: Some(crate::contexts::project_run_record::model::TaskSource {
                        milestone_id: record.id.to_string(),
                        bead_id: bead_id.to_owned(),
                        parent_epic_id: None,
                        origin: crate::contexts::project_run_record::model::TaskOrigin::Milestone,
                        plan_hash: plan_hash.map(str::to_owned),
                        plan_version: Some(1),
                    }),
                },
            )?;
        }

        let listing = list_tasks_for_milestone(
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            &crate::adapters::fs::FsProjectStore,
            base,
            &record.id,
        )?;

        assert_eq!(listing.tasks.len(), 2);
        assert_eq!(listing.tasks[0].project_id, "task-current");
        assert_eq!(
            listing.tasks[0].bead_title.as_deref(),
            Some("Implement feature")
        );
        assert_eq!(listing.tasks[1].project_id, "task-stale");
        assert_eq!(listing.tasks[1].bead_title, None);
        Ok(())
    }

    #[test]
    fn list_tasks_for_milestone_omits_titles_when_task_plan_hash_is_missing(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 13, 45, 0)
            .single()
            .unwrap();
        let record = create_milestone_with_plan(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            "ms-alpha",
            "Alpha",
            now,
        )?;

        crate::contexts::project_run_record::service::create_project(
            &crate::adapters::fs::FsProjectStore,
            &crate::adapters::fs::FsJournalStore,
            base,
            crate::contexts::project_run_record::service::CreateProjectInput {
                id: crate::shared::domain::ProjectId::new("task-missing-hash")?,
                name: "Project task-missing-hash".to_owned(),
                flow: crate::shared::domain::FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: "hash".to_owned(),
                created_at: now + chrono::Duration::seconds(10),
                task_source: Some(crate::contexts::project_run_record::model::TaskSource {
                    milestone_id: record.id.to_string(),
                    bead_id: "bead-1".to_owned(),
                    parent_epic_id: None,
                    origin: crate::contexts::project_run_record::model::TaskOrigin::Milestone,
                    plan_hash: None,
                    plan_version: Some(1),
                }),
            },
        )?;

        let listing = list_tasks_for_milestone(
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            &crate::adapters::fs::FsProjectStore,
            base,
            &record.id,
        )?;

        assert_eq!(listing.tasks.len(), 1);
        assert_eq!(listing.tasks[0].project_id, "task-missing-hash");
        assert_eq!(listing.tasks[0].bead_title, None);
        Ok(())
    }

    #[test]
    fn read_bead_lineage_falls_back_when_plan_bundle_identity_is_wrong(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 14, 0, 0)
            .single()
            .unwrap();
        let record = create_milestone_with_plan(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            "ms-alpha",
            "Alpha",
            now,
        )?;
        let plan_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("plan.json");
        let mut bundle: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&plan_path)?).expect("parse plan json");
        bundle["identity"]["id"] = serde_json::json!("ms-other");
        let raw = serde_json::to_string_pretty(&bundle)?;
        let expected_hash = hash_text(&raw);
        std::fs::write(&plan_path, raw)?;

        let lineage = read_bead_lineage(
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            base,
            &record.id,
            "bead-1",
            Some(&expected_hash),
        )?;

        assert_eq!(lineage.milestone_id, "ms-alpha");
        assert_eq!(lineage.milestone_name, "Alpha");
        assert_eq!(lineage.bead_id, "bead-1");
        assert_eq!(lineage.bead_title, None);
        assert!(lineage.acceptance_criteria.is_empty());
        Ok(())
    }

    #[test]
    fn read_bead_lineage_falls_back_when_plan_bundle_is_semantically_invalid(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 14, 5, 0)
            .single()
            .unwrap();
        let record = create_milestone_with_plan(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            "ms-alpha",
            "Alpha",
            now,
        )?;
        let plan_path = base
            .join(".ralph-burning/milestones")
            .join(record.id.as_str())
            .join("plan.json");
        let mut bundle: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&plan_path)?).expect("parse plan json");
        bundle["workstreams"][0]["name"] = serde_json::json!("");
        let raw = serde_json::to_string_pretty(&bundle)?;
        let expected_hash = hash_text(&raw);
        std::fs::write(&plan_path, raw)?;

        let lineage = read_bead_lineage(
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            base,
            &record.id,
            "bead-1",
            Some(&expected_hash),
        )?;

        assert_eq!(lineage.milestone_id, "ms-alpha");
        assert_eq!(lineage.milestone_name, "Alpha");
        assert_eq!(lineage.bead_id, "bead-1");
        assert_eq!(lineage.bead_title, None);
        assert!(lineage.acceptance_criteria.is_empty());
        Ok(())
    }

    #[test]
    fn read_bead_lineage_errors_for_unknown_bead_in_valid_plan(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 14, 10, 0)
            .single()
            .unwrap();
        let record = create_milestone_with_plan(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            "ms-alpha",
            "Alpha",
            now,
        )?;
        let expected_hash = hash_text(&FsMilestonePlanStore.read_plan_json(base, &record.id)?);

        let error = read_bead_lineage(
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            base,
            &record.id,
            "bead-missing",
            Some(&expected_hash),
        )
        .expect_err("unknown bead should fail against a valid plan");

        match error {
            AppError::CorruptRecord { file, details } => {
                assert!(file.ends_with("milestones/ms-alpha/plan.json"));
                assert!(details.contains("bead 'bead-missing' was not found"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn bead_execution_history_errors_for_unknown_bead_without_runs(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 14, 15, 0)
            .single()
            .unwrap();
        let record = create_milestone_with_plan(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            "ms-alpha",
            "Alpha",
            now,
        )?;

        let error = bead_execution_history(
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-missing",
        )
        .expect_err("unknown bead should fail against a valid plan");

        match error {
            AppError::CorruptRecord { file, details } => {
                assert!(file.ends_with("milestones/ms-alpha/plan.json"));
                assert!(details.contains("bead 'bead-missing' was not found"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn read_bead_lineage_propagates_non_not_found_plan_store_errors(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 14, 20, 0)
            .single()
            .unwrap();
        let record = create_milestone(
            &FsMilestoneStore,
            base,
            CreateMilestoneInput {
                id: "ms-alpha".to_owned(),
                name: "Alpha".to_owned(),
                description: "testing".to_owned(),
            },
            now,
        )?;

        let error = read_bead_lineage(
            &FsMilestoneStore,
            &FailingPlanReadStore {
                error_kind: std::io::ErrorKind::PermissionDenied,
            },
            base,
            &record.id,
            "bead-1",
            Some("plan-hash"),
        )
        .expect_err("permission failures should propagate");

        assert!(
            matches!(error, AppError::Io(io_error) if io_error.kind() == std::io::ErrorKind::PermissionDenied)
        );
        Ok(())
    }

    #[test]
    fn bead_execution_history_propagates_non_not_found_plan_store_errors(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 14, 25, 0)
            .single()
            .unwrap();
        let record = create_milestone(
            &FsMilestoneStore,
            base,
            CreateMilestoneInput {
                id: "ms-alpha".to_owned(),
                name: "Alpha".to_owned(),
                description: "testing".to_owned(),
            },
            now,
        )?;

        let error = bead_execution_history(
            &FsMilestoneStore,
            &FailingPlanReadStore {
                error_kind: std::io::ErrorKind::PermissionDenied,
            },
            &FsTaskRunLineageStore,
            base,
            &record.id,
            "bead-1",
        )
        .expect_err("permission failures should propagate");

        assert!(
            matches!(error, AppError::Io(io_error) if io_error.kind() == std::io::ErrorKind::PermissionDenied)
        );
        Ok(())
    }

    #[test]
    fn list_tasks_for_milestone_propagates_non_not_found_plan_store_errors(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 14, 30, 0)
            .single()
            .unwrap();
        let record = create_milestone(
            &FsMilestoneStore,
            base,
            CreateMilestoneInput {
                id: "ms-alpha".to_owned(),
                name: "Alpha".to_owned(),
                description: "testing".to_owned(),
            },
            now,
        )?;

        let error = list_tasks_for_milestone(
            &FsMilestoneStore,
            &FailingPlanReadStore {
                error_kind: std::io::ErrorKind::PermissionDenied,
            },
            &crate::adapters::fs::FsProjectStore,
            base,
            &record.id,
        )
        .expect_err("permission failures should propagate");

        assert!(
            matches!(error, AppError::Io(io_error) if io_error.kind() == std::io::ErrorKind::PermissionDenied)
        );
        Ok(())
    }

    #[tokio::test]
    async fn materialize_beads_creates_epics_tasks_and_resolved_dependencies(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        setup_workspace(tmp.path());
        let bundle = bead_export_bundle("ms-alpha", "Alpha");
        let responses = vec![
            MockBrResponse::success(list_all_stdout(vec![])),
            MockBrResponse::success("Created bead root-epic"),
            MockBrResponse::success(bead_detail_stdout(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
                &[],
            )),
            MockBrResponse::success("Created bead ws-core"),
            MockBrResponse::success(bead_detail_stdout(
                "ws-core",
                "Core",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-1"),
            MockBrResponse::success(bead_detail_stdout(
                "task-1",
                "Create runtime scaffold",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-2"),
            MockBrResponse::success(bead_detail_stdout(
                "task-2",
                "Wire milestone exporter",
                "feature",
                "open",
                &["milestone:ms-alpha", "backend", "cli"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-3"),
            MockBrResponse::success(bead_detail_stdout(
                "task-3",
                "Persist export journal",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead ws-validation"),
            MockBrResponse::success(bead_detail_stdout(
                "ws-validation",
                "Validation",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-4"),
            MockBrResponse::success(bead_detail_stdout(
                "task-4",
                "Add exporter smoke test",
                "task",
                "open",
                &["milestone:ms-alpha", "test"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-5"),
            MockBrResponse::success(bead_detail_stdout(
                "task-5",
                "Verify idempotent export",
                "task",
                "open",
                &["milestone:ms-alpha", "test"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-6"),
            MockBrResponse::success(bead_detail_stdout(
                "task-6",
                "Handle partial export failure",
                "bug",
                "open",
                &["milestone:ms-alpha", "test", "reliability"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("synced"),
        ];

        let mock = MockBrAdapter::from_responses(responses);
        let report = materialize_beads(&bundle, tmp.path(), &mock.as_mutation_adapter()).await?;
        let calls = mock.calls();

        assert_eq!(report.root_epic_id, "root-epic");
        assert_eq!(report.workstream_epic_ids, vec!["ws-core", "ws-validation"]);
        assert_eq!(
            report.task_bead_ids,
            vec!["task-1", "task-2", "task-3", "task-4", "task-5", "task-6"]
        );
        assert_eq!(report.created_beads, 9);
        assert_eq!(report.reused_beads, 0);
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.args.first().map(String::as_str) == Some("create"))
                .count(),
            9
        );
        assert_eq!(
            calls
                .iter()
                .filter(|call| {
                    call.args.first().map(String::as_str) == Some("dep")
                        && call.args.get(1).map(String::as_str) == Some("add")
                })
                .count(),
            12
        );
        assert!(calls.iter().any(|call| {
            call.args
                == vec![
                    "dep".to_owned(),
                    "add".to_owned(),
                    "ws-core".to_owned(),
                    "root-epic".to_owned(),
                    "--type=parent-child".to_owned(),
                ]
        }));
        assert!(calls.iter().any(|call| {
            call.args
                == vec![
                    "dep".to_owned(),
                    "add".to_owned(),
                    "task-1".to_owned(),
                    "ws-core".to_owned(),
                    "--type=parent-child".to_owned(),
                ]
        }));
        assert!(calls.iter().any(|call| {
            call.args
                == vec![
                    "dep".to_owned(),
                    "add".to_owned(),
                    "task-5".to_owned(),
                    "task-2".to_owned(),
                ]
        }));
        let rationale_call = calls
            .iter()
            .find(|call| {
                call.args.first().map(String::as_str) == Some("comments")
                    && call.args.get(2).map(String::as_str) == Some("task-2")
            })
            .expect("task rationale comment");
        assert!(rationale_call.args[3].contains("Planning rationale for workstream 'Core'."));
        assert!(rationale_call.args[3].contains("AC-1: Runtime scaffold exists"));
        let root_create = calls
            .iter()
            .find(|call| {
                call.args.first().map(String::as_str) == Some("create")
                    && call.args.iter().any(|arg| arg == "--title=Alpha")
            })
            .expect("root create call");
        assert!(root_create
            .args
            .iter()
            .any(|arg| arg == "--labels=milestone:ms-alpha,milestone-root"));
        assert!(root_create
            .args
            .iter()
            .all(|arg| !arg.starts_with("--label=")));
        assert_eq!(
            calls.last().map(|call| call.args.clone()),
            Some(vec!["sync".to_owned(), "--flush-only".to_owned()])
        );

        Ok(())
    }

    #[tokio::test]
    async fn materialize_beads_is_idempotent_for_existing_titles(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        setup_workspace(tmp.path());
        let bundle = bead_export_bundle("ms-alpha", "Alpha");
        let acceptance_lookup = bundle
            .acceptance_map
            .iter()
            .map(|criterion| (criterion.id.as_str(), criterion.description.as_str()))
            .collect::<HashMap<_, _>>();
        let core_description = bundle.workstreams[0]
            .description
            .as_deref()
            .expect("core description");
        let validation_description = bundle.workstreams[1]
            .description
            .as_deref()
            .expect("validation description");
        let core_comment_1 = render_bead_planning_comment(
            bundle.workstreams[0].name.as_str(),
            &bundle.workstreams[0].beads[0],
            &acceptance_lookup,
        )
        .expect("core comment 1");
        let core_comment_2 = render_bead_planning_comment(
            bundle.workstreams[0].name.as_str(),
            &bundle.workstreams[0].beads[1],
            &acceptance_lookup,
        )
        .expect("core comment 2");
        let core_comment_3 = render_bead_planning_comment(
            bundle.workstreams[0].name.as_str(),
            &bundle.workstreams[0].beads[2],
            &acceptance_lookup,
        )
        .expect("core comment 3");
        let validation_comment_1 = render_bead_planning_comment(
            bundle.workstreams[1].name.as_str(),
            &bundle.workstreams[1].beads[0],
            &acceptance_lookup,
        )
        .expect("validation comment 1");
        let validation_comment_2 = render_bead_planning_comment(
            bundle.workstreams[1].name.as_str(),
            &bundle.workstreams[1].beads[1],
            &acceptance_lookup,
        )
        .expect("validation comment 2");
        let validation_comment_3 = render_bead_planning_comment(
            bundle.workstreams[1].name.as_str(),
            &bundle.workstreams[1].beads[2],
            &acceptance_lookup,
        )
        .expect("validation comment 3");
        let list_all = list_all_stdout(vec![
            bead_summary_value(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
            ),
            bead_summary_value("ws-core", "Core", "epic", "open", &["milestone:ms-alpha"]),
            bead_summary_value(
                "task-1",
                "Create runtime scaffold",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
            ),
            bead_summary_value(
                "task-2",
                "Wire milestone exporter",
                "feature",
                "open",
                &["milestone:ms-alpha", "backend", "cli"],
            ),
            bead_summary_value(
                "task-3",
                "Persist export journal",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
            ),
            bead_summary_value(
                "ws-validation",
                "Validation",
                "epic",
                "open",
                &["milestone:ms-alpha"],
            ),
            bead_summary_value(
                "task-4",
                "Add exporter smoke test",
                "task",
                "open",
                &["milestone:ms-alpha", "test"],
            ),
            bead_summary_value(
                "task-5",
                "Verify idempotent export",
                "task",
                "open",
                &["milestone:ms-alpha", "test"],
            ),
            bead_summary_value(
                "task-6",
                "Handle partial export failure",
                "bug",
                "open",
                &["milestone:ms-alpha", "test", "reliability"],
            ),
        ]);
        let mock = MockBrAdapter::from_responses([
            MockBrResponse::success(list_all),
            MockBrResponse::success(bead_detail_stdout(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
                &[],
            )),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "ws-core",
                "Core",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[],
                &[core_description],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "task-1",
                "Create runtime scaffold",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
                &[],
                &[core_comment_1.as_str()],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "task-2",
                "Wire milestone exporter",
                "feature",
                "open",
                &["milestone:ms-alpha", "backend", "cli"],
                &["task-1"],
                &[core_comment_2.as_str()],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "task-3",
                "Persist export journal",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
                &["task-2"],
                &[core_comment_3.as_str()],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "ws-validation",
                "Validation",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[],
                &[validation_description],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "task-4",
                "Add exporter smoke test",
                "task",
                "open",
                &["milestone:ms-alpha", "test"],
                &[],
                &[validation_comment_1.as_str()],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "task-5",
                "Verify idempotent export",
                "task",
                "open",
                &["milestone:ms-alpha", "test"],
                &["task-2"],
                &[validation_comment_2.as_str()],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "task-6",
                "Handle partial export failure",
                "bug",
                "open",
                &["milestone:ms-alpha", "test", "reliability"],
                &["task-5"],
                &[validation_comment_3.as_str()],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("synced"),
        ]);

        let report = materialize_beads(&bundle, tmp.path(), &mock.as_mutation_adapter()).await?;
        let calls = mock.calls();

        assert_eq!(report.created_beads, 0);
        assert_eq!(report.reused_beads, 9);
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.args.first().map(String::as_str) == Some("create"))
                .count(),
            0
        );
        assert_eq!(
            calls
                .iter()
                .filter(|call| {
                    call.args.first().map(String::as_str) == Some("dep")
                        && call.args.get(1).map(String::as_str) == Some("add")
                })
                .count(),
            8
        );
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.args.first().map(String::as_str) == Some("comments"))
                .count(),
            0
        );
        assert!(calls.iter().any(|call| {
            call.args
                == vec![
                    "dep".to_owned(),
                    "add".to_owned(),
                    "ws-core".to_owned(),
                    "root-epic".to_owned(),
                    "--type=parent-child".to_owned(),
                ]
        }));

        Ok(())
    }

    #[tokio::test]
    async fn materialize_beads_reuses_same_title_explicit_proposals_by_proposal_label(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        setup_workspace(tmp.path());
        let mut bundle = sample_bundle("ms-alpha", "Alpha");
        bundle.acceptance_map[0].covered_by =
            vec!["build-api".to_owned(), "validate-api".to_owned()];
        bundle.workstreams[0].beads[0].bead_id = Some("build-api".to_owned());
        bundle.workstreams[0].beads[0].explicit_id = Some(true);
        bundle.workstreams[0].beads[0].title = "Shared title".to_owned();
        bundle.workstreams[0]
            .beads
            .push(crate::contexts::milestone_record::bundle::BeadProposal {
                bead_id: Some("validate-api".to_owned()),
                explicit_id: Some(true),
                title: "Shared title".to_owned(),
                description: Some("Validate the exported API surface.".to_owned()),
                bead_type: Some("task".to_owned()),
                priority: Some(1),
                labels: vec!["backend".to_owned()],
                depends_on: vec!["build-api".to_owned()],
                acceptance_criteria: vec!["AC-1".to_owned()],
                flow_override: None,
            });

        let acceptance_lookup = bundle
            .acceptance_map
            .iter()
            .map(|criterion| (criterion.id.as_str(), criterion.description.as_str()))
            .collect::<HashMap<_, _>>();
        let workstream_comment = bundle.workstreams[0]
            .description
            .as_deref()
            .expect("workstream description")
            .to_owned();
        let first_comment = render_bead_planning_comment(
            bundle.workstreams[0].name.as_str(),
            &bundle.workstreams[0].beads[0],
            &acceptance_lookup,
        )
        .expect("first planning comment");
        let second_comment = render_bead_planning_comment(
            bundle.workstreams[0].name.as_str(),
            &bundle.workstreams[0].beads[1],
            &acceptance_lookup,
        )
        .expect("second planning comment");

        let list_all = list_all_stdout(vec![
            bead_summary_value(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
            ),
            bead_summary_value("ws-core", "Core", "epic", "open", &["milestone:ms-alpha"]),
            bead_summary_value(
                "task-build",
                "Shared title",
                "task",
                "open",
                &["milestone:ms-alpha", "backend", "proposal:build-api"],
            ),
            bead_summary_value(
                "task-validate",
                "Shared title",
                "task",
                "open",
                &["milestone:ms-alpha", "backend", "proposal:validate-api"],
            ),
        ]);
        let mock = MockBrAdapter::from_responses([
            MockBrResponse::success(list_all),
            MockBrResponse::success(bead_detail_stdout(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
                &[],
            )),
            MockBrResponse::success(bead_detail_with_dependency_kinds_stdout(
                "ws-core",
                "Core",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[("root-epic", "parent-child")],
                &[workstream_comment.as_str()],
            )),
            MockBrResponse::success(bead_detail_with_dependency_kinds_stdout(
                "task-build",
                "Shared title",
                "task",
                "open",
                &["milestone:ms-alpha", "backend", "proposal:build-api"],
                &[("ws-core", "parent-child")],
                &[first_comment.as_str()],
            )),
            MockBrResponse::success(bead_detail_with_dependency_kinds_stdout(
                "task-validate",
                "Shared title",
                "task",
                "open",
                &["milestone:ms-alpha", "backend", "proposal:validate-api"],
                &[("ws-core", "parent-child"), ("task-build", "blocks")],
                &[second_comment.as_str()],
            )),
        ]);

        let report = materialize_beads(&bundle, tmp.path(), &mock.as_mutation_adapter()).await?;
        let calls = mock.calls();

        assert_eq!(report.created_beads, 0);
        assert_eq!(report.reused_beads, 4);
        assert_eq!(report.task_bead_ids, vec!["task-build", "task-validate"]);
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.args.first().map(String::as_str) == Some("create"))
                .count(),
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn materialize_beads_reuses_closed_and_deferred_history_without_duplication(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        setup_workspace(tmp.path());
        let bundle = bead_export_bundle("ms-alpha", "Alpha");
        let acceptance_lookup = bundle
            .acceptance_map
            .iter()
            .map(|criterion| (criterion.id.as_str(), criterion.description.as_str()))
            .collect::<HashMap<_, _>>();
        let core_description = bundle.workstreams[0]
            .description
            .as_deref()
            .expect("core description");
        let validation_description = bundle.workstreams[1]
            .description
            .as_deref()
            .expect("validation description");
        let core_comment_1 = render_bead_planning_comment(
            bundle.workstreams[0].name.as_str(),
            &bundle.workstreams[0].beads[0],
            &acceptance_lookup,
        )
        .expect("core comment 1");
        let core_comment_2 = render_bead_planning_comment(
            bundle.workstreams[0].name.as_str(),
            &bundle.workstreams[0].beads[1],
            &acceptance_lookup,
        )
        .expect("core comment 2");
        let core_comment_3 = render_bead_planning_comment(
            bundle.workstreams[0].name.as_str(),
            &bundle.workstreams[0].beads[2],
            &acceptance_lookup,
        )
        .expect("core comment 3");
        let validation_comment_1 = render_bead_planning_comment(
            bundle.workstreams[1].name.as_str(),
            &bundle.workstreams[1].beads[0],
            &acceptance_lookup,
        )
        .expect("validation comment 1");
        let validation_comment_2 = render_bead_planning_comment(
            bundle.workstreams[1].name.as_str(),
            &bundle.workstreams[1].beads[1],
            &acceptance_lookup,
        )
        .expect("validation comment 2");
        let validation_comment_3 = render_bead_planning_comment(
            bundle.workstreams[1].name.as_str(),
            &bundle.workstreams[1].beads[2],
            &acceptance_lookup,
        )
        .expect("validation comment 3");
        let list_all = list_all_stdout(vec![
            bead_summary_value(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
            ),
            bead_summary_value("ws-core", "Core", "epic", "open", &["milestone:ms-alpha"]),
            bead_summary_value(
                "task-1",
                "Create runtime scaffold",
                "task",
                "closed",
                &["milestone:ms-alpha", "backend"],
            ),
            bead_summary_value(
                "task-2",
                "Wire milestone exporter",
                "feature",
                "deferred",
                &["milestone:ms-alpha", "backend", "cli"],
            ),
            bead_summary_value(
                "task-3",
                "Persist export journal",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
            ),
            bead_summary_value(
                "ws-validation",
                "Validation",
                "epic",
                "open",
                &["milestone:ms-alpha"],
            ),
            bead_summary_value(
                "task-4",
                "Add exporter smoke test",
                "task",
                "open",
                &["milestone:ms-alpha", "test"],
            ),
            bead_summary_value(
                "task-5",
                "Verify idempotent export",
                "task",
                "open",
                &["milestone:ms-alpha", "test"],
            ),
            bead_summary_value(
                "task-6",
                "Handle partial export failure",
                "bug",
                "open",
                &["milestone:ms-alpha", "test", "reliability"],
            ),
        ]);
        let mock = MockBrAdapter::from_responses([
            MockBrResponse::success(list_all),
            MockBrResponse::success(bead_detail_stdout(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
                &[],
            )),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "ws-core",
                "Core",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[],
                &[core_description],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "task-1",
                "Create runtime scaffold",
                "task",
                "closed",
                &["milestone:ms-alpha", "backend"],
                &[],
                &[core_comment_1.as_str()],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "task-2",
                "Wire milestone exporter",
                "feature",
                "deferred",
                &["milestone:ms-alpha", "backend", "cli"],
                &["task-1"],
                &[core_comment_2.as_str()],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "task-3",
                "Persist export journal",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
                &["task-2"],
                &[core_comment_3.as_str()],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "ws-validation",
                "Validation",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[],
                &[validation_description],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "task-4",
                "Add exporter smoke test",
                "task",
                "open",
                &["milestone:ms-alpha", "test"],
                &[],
                &[validation_comment_1.as_str()],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "task-5",
                "Verify idempotent export",
                "task",
                "open",
                &["milestone:ms-alpha", "test"],
                &["task-2"],
                &[validation_comment_2.as_str()],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success(bead_detail_with_comments_stdout(
                "task-6",
                "Handle partial export failure",
                "bug",
                "open",
                &["milestone:ms-alpha", "test", "reliability"],
                &["task-5"],
                &[validation_comment_3.as_str()],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("synced"),
        ]);

        let report = materialize_beads(&bundle, tmp.path(), &mock.as_mutation_adapter()).await?;
        let calls = mock.calls();

        assert_eq!(report.created_beads, 0);
        assert_eq!(report.reused_beads, 9);
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.args.first().map(String::as_str) == Some("create"))
                .count(),
            0
        );
        assert!(calls.iter().any(|call| {
            call.args == vec!["show".to_owned(), "task-1".to_owned(), "--json".to_owned()]
        }));
        assert!(calls.iter().any(|call| {
            call.args == vec!["show".to_owned(), "task-2".to_owned(), "--json".to_owned()]
        }));

        Ok(())
    }

    #[tokio::test]
    async fn materialize_beads_ignores_other_milestones_with_same_titles(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        setup_workspace(tmp.path());
        let bundle = bead_export_bundle("ms-alpha", "Alpha");
        let responses = vec![
            MockBrResponse::success(list_all_stdout(vec![
                bead_summary_value(
                    "other-root",
                    "Alpha",
                    "epic",
                    "open",
                    &["milestone:ms-beta", "milestone-root"],
                ),
                bead_summary_value("other-core", "Core", "epic", "open", &["milestone:ms-beta"]),
            ])),
            MockBrResponse::success("Created bead root-epic"),
            MockBrResponse::success(bead_detail_stdout(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
                &[],
            )),
            MockBrResponse::success("Created bead ws-core"),
            MockBrResponse::success(bead_detail_stdout(
                "ws-core",
                "Core",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-1"),
            MockBrResponse::success(bead_detail_stdout(
                "task-1",
                "Create runtime scaffold",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-2"),
            MockBrResponse::success(bead_detail_stdout(
                "task-2",
                "Wire milestone exporter",
                "feature",
                "open",
                &["milestone:ms-alpha", "backend", "cli"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-3"),
            MockBrResponse::success(bead_detail_stdout(
                "task-3",
                "Persist export journal",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead ws-validation"),
            MockBrResponse::success(bead_detail_stdout(
                "ws-validation",
                "Validation",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-4"),
            MockBrResponse::success(bead_detail_stdout(
                "task-4",
                "Add exporter smoke test",
                "task",
                "open",
                &["milestone:ms-alpha", "test"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-5"),
            MockBrResponse::success(bead_detail_stdout(
                "task-5",
                "Verify idempotent export",
                "task",
                "open",
                &["milestone:ms-alpha", "test"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-6"),
            MockBrResponse::success(bead_detail_stdout(
                "task-6",
                "Handle partial export failure",
                "bug",
                "open",
                &["milestone:ms-alpha", "test", "reliability"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("synced"),
        ];

        let mock = MockBrAdapter::from_responses(responses);
        let report = materialize_beads(&bundle, tmp.path(), &mock.as_mutation_adapter()).await?;

        assert_eq!(report.created_beads, 9);
        assert_eq!(report.reused_beads, 0);
        Ok(())
    }

    #[tokio::test]
    async fn materialize_beads_resolves_implicit_dependency_ids(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        setup_workspace(tmp.path());
        let mut bundle = sample_bundle("ms-alpha", "Alpha");
        bundle.workstreams[0].beads[0].title = "First implicit task".to_owned();
        bundle.workstreams[0]
            .beads
            .push(crate::contexts::milestone_record::bundle::BeadProposal {
                bead_id: None,
                explicit_id: None,
                title: "Second implicit task".to_owned(),
                description: Some("Depends on the implicit first bead.".to_owned()),
                bead_type: Some("task".to_owned()),
                priority: Some(1),
                labels: vec!["backend".to_owned()],
                depends_on: vec!["ms-alpha.bead-1".to_owned()],
                acceptance_criteria: vec![],
                flow_override: None,
            });
        let responses = vec![
            MockBrResponse::success(list_all_stdout(vec![])),
            MockBrResponse::success("Created bead root-epic"),
            MockBrResponse::success(bead_detail_stdout(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
                &[],
            )),
            MockBrResponse::success("Created bead ws-core"),
            MockBrResponse::success(bead_detail_stdout(
                "ws-core",
                "Core",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-1"),
            MockBrResponse::success(bead_detail_stdout(
                "task-1",
                "First implicit task",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-2"),
            MockBrResponse::success(bead_detail_stdout(
                "task-2",
                "Second implicit task",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("synced"),
        ];

        let mock = MockBrAdapter::from_responses(responses);
        let report = materialize_beads(&bundle, tmp.path(), &mock.as_mutation_adapter()).await?;
        let calls = mock.calls();

        assert_eq!(report.task_bead_ids, vec!["task-1", "task-2"]);
        assert!(calls.iter().any(|call| {
            call.args
                == vec![
                    "dep".to_owned(),
                    "add".to_owned(),
                    "task-2".to_owned(),
                    "task-1".to_owned(),
                ]
        }));
        Ok(())
    }

    #[tokio::test]
    async fn materialize_beads_restores_missing_comments_for_reused_beads(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        setup_workspace(tmp.path());
        let bundle = sample_bundle("ms-alpha", "Alpha");
        let list_all = list_all_stdout(vec![
            bead_summary_value(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
            ),
            bead_summary_value("ws-core", "Core", "epic", "open", &["milestone:ms-alpha"]),
            bead_summary_value(
                "task-1",
                "Implement feature",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
            ),
        ]);
        let acceptance_lookup = bundle
            .acceptance_map
            .iter()
            .map(|criterion| (criterion.id.as_str(), criterion.description.as_str()))
            .collect::<HashMap<_, _>>();
        let planning_comment = render_bead_planning_comment(
            bundle.workstreams[0].name.as_str(),
            &bundle.workstreams[0].beads[0],
            &acceptance_lookup,
        )
        .expect("planning comment");
        let mock = MockBrAdapter::from_responses([
            MockBrResponse::success(list_all),
            MockBrResponse::success(bead_detail_stdout(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
                &[],
            )),
            MockBrResponse::success(bead_detail_with_dependency_kinds_stdout(
                "ws-core",
                "Core",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[("root-epic", "parent-child")],
                &[],
            )),
            MockBrResponse::success("comment added"),
            MockBrResponse::success(bead_detail_with_dependency_kinds_stdout(
                "task-1",
                "Implement feature",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
                &[("ws-core", "parent-child")],
                &[],
            )),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("synced"),
        ]);

        let report = materialize_beads(&bundle, tmp.path(), &mock.as_mutation_adapter()).await?;
        let calls = mock.calls();

        assert_eq!(report.created_beads, 0);
        assert_eq!(report.reused_beads, 3);
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.args.first().map(String::as_str) == Some("comments"))
                .count(),
            2
        );
        assert!(calls.iter().any(|call| {
            call.args.first().map(String::as_str) == Some("comments")
                && call.args.get(2).map(String::as_str) == Some("ws-core")
                && call.args.get(3).map(String::as_str)
                    == bundle.workstreams[0].description.as_deref()
        }));
        assert!(calls.iter().any(|call| {
            call.args.first().map(String::as_str) == Some("comments")
                && call.args.get(2).map(String::as_str) == Some("task-1")
                && call.args.get(3).map(String::as_str) == Some(planning_comment.as_str())
        }));

        Ok(())
    }

    #[tokio::test]
    async fn materialize_beads_replays_own_pending_sync_before_retry(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        setup_workspace(tmp.path());
        let bundle = sample_bundle("ms-alpha", "Alpha");
        let adapter_id = "milestone-export-beads-ms-alpha";
        let own_record = tmp
            .path()
            .join(format!(".beads/.br-unsynced-mutations.d/{adapter_id}.json"));
        std::fs::create_dir_all(
            own_record
                .parent()
                .expect("pending record path should have a parent"),
        )?;
        std::fs::write(
            &own_record,
            r#"{"adapter_id":"milestone-export-beads-ms-alpha","operation":"create_bead","bead_id":"task-1","status":null}"#,
        )?;
        let mock = MockBrAdapter::from_responses([
            MockBrResponse::success("synced"),
            MockBrResponse::success(list_all_stdout(vec![])),
            MockBrResponse::success("Created bead root-epic"),
            MockBrResponse::success(bead_detail_stdout(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
                &[],
            )),
            MockBrResponse::success("Created bead ws-core"),
            MockBrResponse::success(bead_detail_stdout(
                "ws-core",
                "Core",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("Created bead task-1"),
            MockBrResponse::success(bead_detail_stdout(
                "task-1",
                "Implement feature",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
                &[],
            )),
            MockBrResponse::success("dependency added"),
            MockBrResponse::success("comment added"),
            MockBrResponse::success("synced"),
        ]);
        let br_mutation = BrMutationAdapter::with_adapter_id(
            BrAdapter::with_runner(mock.clone()).with_working_dir(tmp.path().to_path_buf()),
            adapter_id,
        );

        let report = materialize_beads(&bundle, tmp.path(), &br_mutation).await?;
        let calls = mock.calls();

        assert_eq!(report.created_beads, 3);
        assert_eq!(
            calls.first().map(|call| call.args.clone()),
            Some(vec!["sync".to_owned(), "--flush-only".to_owned()])
        );
        assert_eq!(
            calls.last().map(|call| call.args.clone()),
            Some(vec!["sync".to_owned(), "--flush-only".to_owned()])
        );
        Ok(())
    }

    #[tokio::test]
    async fn materialize_beads_reuses_same_title_root_and_workstream_without_ambiguity(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        setup_workspace(tmp.path());
        let mut bundle = sample_bundle("ms-alpha", "Alpha");
        bundle.workstreams[0].name = "Alpha".to_owned();
        let acceptance_lookup = bundle
            .acceptance_map
            .iter()
            .map(|criterion| (criterion.id.as_str(), criterion.description.as_str()))
            .collect::<HashMap<_, _>>();
        let planning_comment = render_bead_planning_comment(
            bundle.workstreams[0].name.as_str(),
            &bundle.workstreams[0].beads[0],
            &acceptance_lookup,
        )
        .expect("planning comment");
        let list_all = list_all_stdout(vec![
            bead_summary_value(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
            ),
            bead_summary_value(
                "workstream-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha"],
            ),
            bead_summary_value(
                "task-1",
                "Implement feature",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
            ),
        ]);
        let mock = MockBrAdapter::from_responses([
            MockBrResponse::success(list_all),
            MockBrResponse::success(bead_detail_stdout(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
                &[],
            )),
            MockBrResponse::success(bead_detail_with_dependency_kinds_stdout(
                "workstream-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[("root-epic", "parent-child")],
                &[bundle.workstreams[0]
                    .description
                    .as_deref()
                    .expect("workstream description")],
            )),
            MockBrResponse::success(bead_detail_with_dependency_kinds_stdout(
                "task-1",
                "Implement feature",
                "task",
                "open",
                &["milestone:ms-alpha", "backend"],
                &[("workstream-epic", "parent-child")],
                &[planning_comment.as_str()],
            )),
            MockBrResponse::success("synced"),
        ]);

        let report = materialize_beads(&bundle, tmp.path(), &mock.as_mutation_adapter()).await?;
        let calls = mock.calls();

        assert_eq!(report.root_epic_id, "root-epic");
        assert_eq!(report.workstream_epic_ids, vec!["workstream-epic"]);
        assert_eq!(report.created_beads, 0);
        assert_eq!(report.reused_beads, 3);
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.args.first().map(String::as_str) == Some("create"))
                .count(),
            0
        );

        Ok(())
    }

    #[test]
    fn record_beads_exported_event_appends_bundle_hash_and_ids(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base);
        let milestone_id = MilestoneId::new("ms-alpha")?;
        let journal_store = FsMilestoneJournalStore;
        let now = Utc
            .with_ymd_and_hms(2026, 4, 17, 12, 0, 0)
            .single()
            .unwrap();
        create_milestone(
            &FsMilestoneStore,
            base,
            CreateMilestoneInput {
                id: milestone_id.to_string(),
                name: "Alpha".to_owned(),
                description: "testing".to_owned(),
            },
            now,
        )?;

        let report = BeadMaterializationReport {
            root_epic_id: "root-epic".to_owned(),
            workstream_epic_ids: vec!["ws-core".to_owned(), "ws-validation".to_owned()],
            task_bead_ids: vec!["task-1".to_owned(), "task-2".to_owned()],
            created_beads: 4,
            reused_beads: 1,
        };
        record_beads_exported_event(
            &journal_store,
            base,
            &milestone_id,
            "bundle-hash-123",
            &report,
            now + chrono::Duration::seconds(1),
        )?;

        let journal = read_journal(&journal_store, base, &milestone_id)?;
        let event = journal.last().expect("export event");
        assert_eq!(event.event_type, MilestoneEventType::ProgressUpdated);
        assert_eq!(event.bead_id.as_deref(), Some("root-epic"));
        assert_eq!(event.details.as_deref(), Some("milestone beads exported"));
        let metadata = event.metadata.as_ref().expect("event metadata");
        assert_eq!(
            metadata.get("sub_type").and_then(|value| value.as_str()),
            Some("beads_exported")
        );
        assert_eq!(
            metadata.get("bundle_hash").and_then(|value| value.as_str()),
            Some("bundle-hash-123")
        );
        assert_eq!(
            metadata
                .get("task_bead_ids")
                .and_then(|value| value.as_array())
                .map(|values| values.len()),
            Some(2)
        );

        Ok(())
    }

    #[tokio::test]
    async fn materialize_beads_returns_error_without_sync_when_mutation_fails(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        setup_workspace(tmp.path());
        let bundle = sample_bundle("ms-alpha", "Alpha");
        let mock = MockBrAdapter::from_responses([
            MockBrResponse::success(list_all_stdout(vec![])),
            MockBrResponse::success("Created bead root-epic"),
            MockBrResponse::success(bead_detail_stdout(
                "root-epic",
                "Alpha",
                "epic",
                "open",
                &["milestone:ms-alpha", "milestone-root"],
                &[],
            )),
            MockBrResponse::success("Created bead ws-core"),
            MockBrResponse::success(bead_detail_stdout(
                "ws-core",
                "Core",
                "epic",
                "open",
                &["milestone:ms-alpha"],
                &[],
            )),
            MockBrResponse::exit_failure(1, "dependency failed"),
        ]);

        let error = materialize_beads(&bundle, tmp.path(), &mock.as_mutation_adapter())
            .await
            .expect_err("dependency failure should surface");
        let calls = mock.calls();

        assert!(matches!(
            error,
            AppError::MilestoneOperationFailed { ref action, .. }
                if action == "link workstream epic to milestone root"
        ));
        assert!(!calls.iter().any(|call| {
            call.args.first().map(String::as_str) == Some("sync")
                && call.args.get(1).map(String::as_str) == Some("--flush-only")
        }));

        Ok(())
    }
}
