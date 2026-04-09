use std::collections::BTreeSet;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sha2::{Digest, Sha256};

use crate::adapters::fs::FileSystem;
use crate::shared::error::{AppError, AppResult};

use super::bundle::{
    explicit_id_hints, progress_shape_signature, progress_shape_signature_with_explicit_id_hints,
    render_plan_json, render_plan_md_checked, MilestoneBundle,
};
use super::model::{
    collapse_task_run_attempts, latest_task_runs_per_bead, CompletionJournalDetails,
    MilestoneEventType, MilestoneId, MilestoneJournalEvent, MilestoneProgress, MilestoneRecord,
    MilestoneSnapshot, MilestoneStatus, PendingLineageReset, PlannedElsewhereMapping,
    StartJournalDetails, TaskRunEntry, TaskRunOutcome,
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
    /// Implementations should reject ambiguous matches instead of rewriting an
    /// arbitrary historical attempt.
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
pub fn load_milestone(
    store: &impl MilestoneStorePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneRecord> {
    store.read_milestone_record(base_dir, milestone_id)
}

/// Load a milestone's current status snapshot.
pub fn load_snapshot(
    snapshot_store: &impl MilestoneSnapshotPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<MilestoneSnapshot> {
    snapshot_store.read_snapshot(base_dir, milestone_id)
}

/// List all milestone IDs in the workspace.
pub fn list_milestones(
    store: &impl MilestoneStorePort,
    base_dir: &Path,
) -> AppResult<Vec<MilestoneId>> {
    store.list_milestone_ids(base_dir)
}

/// Update the milestone status and append a journal event.
pub fn update_status(
    snapshot_store: &impl MilestoneSnapshotPort,
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    new_status: MilestoneStatus,
    now: DateTime<Utc>,
) -> AppResult<MilestoneSnapshot> {
    snapshot_store.with_milestone_write_lock(base_dir, milestone_id, || {
        let mut snapshot = snapshot_store.read_snapshot(base_dir, milestone_id)?;
        update_status_locked(
            snapshot_store,
            journal_store,
            base_dir,
            milestone_id,
            &mut snapshot,
            new_status,
            now,
        )?;
        Ok(snapshot)
    })
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

        Ok(())
    })
}

/// Record the completion of a bead task run.
///
/// This finalizes the existing lineage row created by [`record_bead_start`] and
/// updates snapshot + journal state in the same flow.
#[allow(clippy::too_many_arguments)]
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
    )
}

#[allow(clippy::too_many_arguments)]
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
    )
}

/// Read the journal for a milestone.
pub fn read_journal(
    journal_store: &impl MilestoneJournalPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<Vec<MilestoneJournalEvent>> {
    journal_store.read_journal(base_dir, milestone_id)
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

/// Update an existing task run's outcome after completion.
///
/// The lineage row remains the durable source of truth. Snapshot counters and
/// journal events are repaired from canonical lineage state, so replaying the
/// same terminal completion can finish a partially failed write without
/// duplicating counters or events.
#[allow(clippy::too_many_arguments)]
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
    )
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
    )
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
/// mapped_to_bead_id)`, keeping the latest (newest `recorded_at`) row for
/// each key. This ensures that a verification event appended after the
/// original unverified record supersedes it.
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

    for event in &events {
        // Recognise both the legacy PlannedElsewhereMapped event type and
        // the newer ProgressUpdated events that carry a sub_type
        // discriminator of "planned_elsewhere_mapped" (write-compatible
        // with older code that does not know PlannedElsewhereMapped).
        let is_pe = event.event_type == MilestoneEventType::PlannedElsewhereMapped
            || (event.event_type == MilestoneEventType::ProgressUpdated
                && event
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get("sub_type"))
                    .and_then(|v| v.as_str())
                    == Some("planned_elsewhere_mapped"));
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

    Ok(by_identity.into_values().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc, Arc, Mutex};
    use std::time::Duration;

    use crate::adapters::fs::{
        FsMilestoneJournalStore, FsMilestonePlanStore, FsMilestoneSnapshotStore, FsMilestoneStore,
        FsPlannedElsewhereMappingStore, FsTaskRunLineageStore,
    };
    use crate::contexts::milestone_record::model::render_completion_journal_details;

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

    fn setup_workspace(dir: &Path) {
        std::fs::create_dir_all(dir.join(".ralph-burning/milestones")).unwrap();
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
                description: None,
                beads: vec![BeadProposal {
                    bead_id: None,
                    explicit_id: None,
                    title: "Implement feature".to_owned(),
                    description: None,
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec![],
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
            description: None,
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec![],
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
            description: None,
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec![],
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
            description: None,
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec![],
            depends_on: vec![],
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        bundle.workstreams[0].beads.push(BeadProposal {
            bead_id: None,
            explicit_id: None,
            title: "Document skip path".to_owned(),
            description: None,
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec![],
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
            description: None,
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec![],
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
            description: None,
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec![],
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
            description: None,
            bead_type: Some("task".to_owned()),
            priority: Some(1),
            labels: vec![],
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
                description: None,
                bead_type: Some("task".to_owned()),
                priority: Some(1),
                labels: vec![],
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
    fn materialize_bundle_accepts_legacy_missing_covered_by_and_backfills_plan_json(
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

        let record = materialize_bundle(
            &store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &bundle,
            now,
        )?;

        let persisted_bundle: MilestoneBundle =
            serde_json::from_str(&plan_store.read_plan_json(base, &record.id)?)?;
        assert_eq!(
            persisted_bundle.acceptance_map[0].covered_by,
            vec!["legacy-covered-by.bead-1".to_owned()]
        );
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
            labels: vec![],
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
            labels: vec![],
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
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(runs[0].outcome, TaskRunOutcome::Running);
        assert_eq!(runs[0].plan_hash.as_deref(), Some("plan-v1"));
        assert!(runs[0].finished_at.is_none());
        assert!(runs[0].outcome_detail.is_none());

        let snapshot = load_snapshot(&snapshot_store, base, &record.id)?;
        assert_eq!(snapshot.status, MilestoneStatus::Running);
        assert_eq!(snapshot.active_bead.as_deref(), Some("bead-1"));
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
}
