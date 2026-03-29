use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::shared::error::{AppError, AppResult};

/// Current milestone record schema version.
pub const MILESTONE_SCHEMA_VERSION: u32 = 1;

// ── Identity ──────────────────────────────────────────────────────────

/// Validated milestone identifier. Must be a non-empty single path segment.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MilestoneId(String);

impl MilestoneId {
    pub fn new(value: impl Into<String>) -> AppResult<Self> {
        let normalized = value.into().trim().to_owned();
        if normalized.is_empty()
            || normalized.starts_with('.')
            || normalized == "."
            || normalized == ".."
            || normalized.contains('/')
            || normalized.contains('\\')
        {
            return Err(AppError::InvalidIdentifier { value: normalized });
        }
        Ok(Self(normalized))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MilestoneId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ── Status ────────────────────────────────────────────────────────────

/// Top-level lifecycle status of a milestone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MilestoneStatus {
    /// Planning phase — plan is being drafted or refined.
    Planning,
    /// Plan is finalized; bead execution has not started.
    Ready,
    /// At least one bead task is running.
    Active,
    /// Execution paused (e.g., waiting for external input).
    Paused,
    /// All acceptance criteria met.
    Completed,
    /// Milestone abandoned.
    Abandoned,
}

impl MilestoneStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::Ready => "ready",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Abandoned => "abandoned",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Abandoned)
    }
}

impl fmt::Display for MilestoneStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for MilestoneStatus {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "planning" => Ok(Self::Planning),
            "ready" => Ok(Self::Ready),
            "active" => Ok(Self::Active),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "abandoned" => Ok(Self::Abandoned),
            _ => Err(AppError::InvalidConfigValue {
                key: "milestone_status".to_owned(),
                value: value.to_owned(),
                reason: "expected one of planning, ready, active, paused, completed, abandoned"
                    .to_owned(),
            }),
        }
    }
}

// ── Immutable Record ──────────────────────────────────────────────────

/// Immutable milestone metadata persisted in `milestone.toml`.
/// Created once at milestone initialization and never modified.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MilestoneRecord {
    /// Schema version for forward compatibility.
    pub schema_version: u32,
    /// Unique identifier for this milestone.
    pub id: MilestoneId,
    /// Human-readable name.
    pub name: String,
    /// One-line description of what this milestone delivers.
    pub description: String,
    /// When this milestone was created.
    pub created_at: DateTime<Utc>,
}

impl MilestoneRecord {
    pub fn new(id: MilestoneId, name: String, description: String, now: DateTime<Utc>) -> Self {
        Self {
            schema_version: MILESTONE_SCHEMA_VERSION,
            id,
            name,
            description,
            created_at: now,
        }
    }
}

// ── Mutable Status Snapshot ───────────────────────────────────────────

/// Mutable milestone state persisted in `status.json`.
/// Updated as milestone planning and execution progresses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MilestoneSnapshot {
    /// Current lifecycle status.
    pub status: MilestoneStatus,
    /// SHA-256 hash of the current plan.json content (if planned).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,
    /// Monotonically increasing plan version counter.
    pub plan_version: u32,
    /// Bead ID currently being executed (if any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_bead: Option<String>,
    /// Execution progress summary.
    pub progress: MilestoneProgress,
    /// Last time this snapshot was updated.
    pub updated_at: DateTime<Utc>,
}

impl MilestoneSnapshot {
    pub fn initial(now: DateTime<Utc>) -> Self {
        Self {
            status: MilestoneStatus::Planning,
            plan_hash: None,
            plan_version: 0,
            active_bead: None,
            progress: MilestoneProgress::default(),
            updated_at: now,
        }
    }

    pub fn validate_semantics(&self) -> Result<(), String> {
        if self.status == MilestoneStatus::Active && self.active_bead.is_none() {
            return Err(
                "status is 'active' but active_bead is null — inconsistent state".to_owned(),
            );
        }
        if self.status.is_terminal() && self.active_bead.is_some() {
            return Err(format!(
                "status is '{}' but active_bead is set — inconsistent state",
                self.status
            ));
        }
        Ok(())
    }
}

// ── Progress Tracking ─────────────────────────────────────────────────

/// Aggregate progress counters for a milestone.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct MilestoneProgress {
    /// Total number of beads in the plan.
    pub total_beads: u32,
    /// Beads whose latest task attempt has completed successfully.
    pub completed_beads: u32,
    /// Beads whose latest task attempt is currently in progress.
    pub in_progress_beads: u32,
    /// Beads whose latest task attempt failed and still need attention.
    pub failed_beads: u32,
    /// Beads whose latest task attempt was intentionally skipped.
    pub skipped_beads: u32,
    /// Beads that are blocked by dependencies.
    pub blocked_beads: u32,
}

impl MilestoneProgress {
    pub fn remaining(&self) -> u32 {
        self.total_beads
            .saturating_sub(self.completed_beads)
            .saturating_sub(self.failed_beads)
            .saturating_sub(self.skipped_beads)
    }
}

// ── Journal Events ────────────────────────────────────────────────────

/// Events recorded to `journal.ndjson` for milestone activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MilestoneJournalEvent {
    pub timestamp: DateTime<Utc>,
    pub event_type: MilestoneEventType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MilestoneEventType {
    Created,
    PlanDrafted,
    PlanUpdated,
    StatusChanged,
    BeadStarted,
    BeadCompleted,
    BeadFailed,
    BeadSkipped,
    ProgressUpdated,
}

impl MilestoneJournalEvent {
    pub fn new(event_type: MilestoneEventType, now: DateTime<Utc>) -> Self {
        Self {
            timestamp: now,
            event_type,
            bead_id: None,
            details: None,
        }
    }

    pub fn with_bead(mut self, bead_id: impl Into<String>) -> Self {
        self.bead_id = Some(bead_id.into());
        self
    }

    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    pub fn to_ndjson_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

// ── Task-Run Lineage ──────────────────────────────────────────────────

/// Maps a bead to the Ralph project/run that executed it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskRunEntry {
    /// Milestone ID for self-contained lineage queries.
    pub milestone_id: String,
    /// Bead ID from the `.beads/` graph.
    pub bead_id: String,
    /// Ralph project ID for this bead's execution.
    pub project_id: String,
    /// Specific run ID within the project.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// Plan version/hash at time of execution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,
    /// Outcome of the task run.
    pub outcome: TaskRunOutcome,
    /// Human-readable outcome summary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_detail: Option<String>,
    /// When this task run was started.
    pub started_at: DateTime<Utc>,
    /// When this task run finished (if complete).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
}

impl TaskRunEntry {
    pub fn same_attempt(left: &Self, right: &Self) -> bool {
        if left.bead_id != right.bead_id || left.project_id != right.project_id {
            return false;
        }

        match (left.run_id.as_deref(), right.run_id.as_deref()) {
            (Some(left_run_id), Some(right_run_id)) => left_run_id == right_run_id,
            _ => {
                left.started_at == right.started_at
                    && (!left.outcome.is_terminal() || !right.outcome.is_terminal())
            }
        }
    }

    pub fn merge_attempt_entries(primary: &Self, secondary: &Self) -> Self {
        let mut merged = primary.clone();
        if merged.run_id.is_none() {
            merged.run_id = secondary.run_id.clone();
        }
        if merged.plan_hash.is_none() {
            merged.plan_hash = secondary.plan_hash.clone();
        }
        if merged.outcome_detail.is_none() {
            merged.outcome_detail = secondary.outcome_detail.clone();
        }
        merged.started_at = merged.started_at.min(secondary.started_at);
        match (merged.finished_at, secondary.finished_at) {
            (None, Some(finished_at)) => merged.finished_at = Some(finished_at),
            (Some(current), Some(other)) if other < current => merged.finished_at = Some(other),
            _ => {}
        }
        merged
    }

    pub fn completion_journal_details(&self) -> String {
        render_completion_journal_details(
            &self.project_id,
            self.run_id.as_deref(),
            self.plan_hash.as_deref(),
            self.outcome,
            self.outcome_detail.as_deref(),
        )
    }
}

#[derive(Serialize)]
struct CompletionJournalDetailsPayload<'a> {
    project_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    plan_hash: Option<&'a str>,
    outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    outcome_detail: Option<&'a str>,
}

pub fn render_completion_journal_details(
    project_id: &str,
    run_id: Option<&str>,
    plan_hash: Option<&str>,
    outcome: impl fmt::Display,
    outcome_detail: Option<&str>,
) -> String {
    serde_json::to_string(&CompletionJournalDetailsPayload {
        project_id,
        run_id,
        plan_hash,
        outcome: outcome.to_string(),
        outcome_detail,
    })
    .expect("completion journal details serialization should not fail")
}

fn compare_task_run_recency(left: &TaskRunEntry, right: &TaskRunEntry) -> Ordering {
    left.started_at.cmp(&right.started_at)
}

/// Collapse attempt history down to each bead's latest observed state.
/// When retries share the same `started_at`, later entries in the slice win so
/// append order still reflects the newest observed attempt.
pub fn latest_task_runs_per_bead(entries: &[TaskRunEntry]) -> Vec<TaskRunEntry> {
    let mut latest_by_bead = BTreeMap::new();

    for (index, entry) in entries.iter().enumerate() {
        latest_by_bead
            .entry(entry.bead_id.clone())
            .and_modify(|current: &mut (usize, TaskRunEntry)| {
                let ordering = compare_task_run_recency(entry, &current.1);
                if ordering != Ordering::Less {
                    current.0 = index;
                    current.1 = entry.clone();
                }
            })
            .or_insert_with(|| (index, entry.clone()));
    }

    latest_by_bead
        .into_values()
        .map(|(_, entry)| entry)
        .collect()
}

pub fn active_bead_ids(entries: &[TaskRunEntry]) -> BTreeSet<String> {
    latest_task_runs_per_bead(entries)
        .into_iter()
        .filter(|entry| !entry.outcome.is_terminal())
        .map(|entry| entry.bead_id)
        .collect()
}

pub fn find_matching_running_task_run(
    entries: &[TaskRunEntry],
    bead_id: &str,
    project_id: &str,
    run_id: Option<&str>,
    started_at: DateTime<Utc>,
) -> Option<TaskRunEntry> {
    let matching_running_entries: Vec<TaskRunEntry> = entries
        .iter()
        .filter(|entry| {
            entry.bead_id == bead_id
                && entry.project_id == project_id
                && !entry.outcome.is_terminal()
        })
        .cloned()
        .collect();

    if let Some(run_id) = run_id {
        matching_running_entries
            .iter()
            .find(|entry| entry.run_id.as_deref() == Some(run_id))
            .cloned()
            .or_else(|| match matching_running_entries.as_slice() {
                [entry] if entry.run_id.is_none() && entry.started_at == started_at => {
                    Some(entry.clone())
                }
                _ => None,
            })
    } else {
        match matching_running_entries.as_slice() {
            [entry] if entry.run_id.is_none() && entry.started_at == started_at => {
                Some(entry.clone())
            }
            _ => None,
        }
    }
}

pub fn has_finalized_task_run(
    entries: &[TaskRunEntry],
    bead_id: &str,
    project_id: &str,
    run_id: &str,
) -> bool {
    entries.iter().any(|entry| {
        entry.bead_id == bead_id
            && entry.project_id == project_id
            && entry.run_id.as_deref() == Some(run_id)
            && entry.outcome.is_terminal()
    })
}

pub fn collapse_task_run_attempts(entries: Vec<TaskRunEntry>) -> Vec<TaskRunEntry> {
    let mut collapsed_groups: Vec<Vec<TaskRunEntry>> = Vec::new();

    for entry in entries {
        if let Some(group_index) = find_collapse_group_index(&collapsed_groups, &entry) {
            collapsed_groups[group_index].push(entry);
        } else {
            collapsed_groups.push(vec![entry]);
        }
    }

    collapsed_groups
        .into_iter()
        .map(collapse_task_run_group)
        .collect()
}

fn find_collapse_group_index(groups: &[Vec<TaskRunEntry>], entry: &TaskRunEntry) -> Option<usize> {
    if let Some(run_id) = entry.run_id.as_deref() {
        if let Some(group_index) = groups.iter().position(|group| {
            group_matches_named_attempt(group, entry, run_id) && group_accepts_entry(group, entry)
        }) {
            return Some(group_index);
        }
    }

    if entry.outcome.is_terminal() {
        return unique_open_legacy_group_index(groups, entry);
    }

    None
}

fn group_matches_named_attempt(group: &[TaskRunEntry], entry: &TaskRunEntry, run_id: &str) -> bool {
    group.iter().any(|existing| {
        existing.bead_id == entry.bead_id
            && existing.project_id == entry.project_id
            && existing.run_id.as_deref() == Some(run_id)
    })
}

fn group_accepts_entry(group: &[TaskRunEntry], entry: &TaskRunEntry) -> bool {
    group.iter().all(|existing| {
        !existing.outcome.is_terminal()
            || !entry.outcome.is_terminal()
            || existing.outcome == entry.outcome
    })
}

fn unique_open_legacy_group_index(
    groups: &[Vec<TaskRunEntry>],
    entry: &TaskRunEntry,
) -> Option<usize> {
    let matching_group_indices: Vec<usize> = groups
        .iter()
        .enumerate()
        .filter_map(|(index, group)| legacy_group_accepts_completion(group, entry).then_some(index))
        .collect();

    match matching_group_indices.as_slice() {
        [index] => Some(*index),
        _ => None,
    }
}

fn legacy_group_accepts_completion(group: &[TaskRunEntry], entry: &TaskRunEntry) -> bool {
    !group.iter().any(|existing| existing.outcome.is_terminal())
        && group.iter().any(|existing| {
            existing.bead_id == entry.bead_id
                && existing.project_id == entry.project_id
                && existing.run_id.is_none()
                && !existing.outcome.is_terminal()
                && existing.started_at == entry.started_at
        })
}

fn collapse_task_run_group(group: Vec<TaskRunEntry>) -> TaskRunEntry {
    let primary_index = group
        .iter()
        .position(|entry| entry.outcome.is_terminal())
        .unwrap_or(0);
    let mut merged = group[primary_index].clone();

    for (index, entry) in group.iter().enumerate() {
        if index == primary_index {
            continue;
        }
        merged = TaskRunEntry::merge_attempt_entries(&merged, entry);
    }

    merged
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskRunOutcome {
    Pending,
    Running,
    Succeeded,
    Failed,
    Skipped,
}

impl TaskRunOutcome {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Skipped)
    }
}

impl fmt::Display for TaskRunOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Running => f.write_str("running"),
            Self::Succeeded => f.write_str("succeeded"),
            Self::Failed => f.write_str("failed"),
            Self::Skipped => f.write_str("skipped"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn milestone_id_rejects_empty() -> Result<(), Box<dyn std::error::Error>> {
        assert!(MilestoneId::new("").is_err());
        assert!(MilestoneId::new("  ").is_err());
        assert!(MilestoneId::new(".").is_err());
        assert!(MilestoneId::new("..").is_err());
        assert!(MilestoneId::new(".locks").is_err());
        assert!(MilestoneId::new(".hidden").is_err());
        assert!(MilestoneId::new("a/b").is_err());
        Ok(())
    }

    #[test]
    fn milestone_id_accepts_valid() -> Result<(), Box<dyn std::error::Error>> {
        let id = MilestoneId::new("my-milestone-v1")?;
        assert_eq!(id.as_str(), "my-milestone-v1");
        Ok(())
    }

    #[test]
    fn milestone_status_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        for status in [
            MilestoneStatus::Planning,
            MilestoneStatus::Ready,
            MilestoneStatus::Active,
            MilestoneStatus::Paused,
            MilestoneStatus::Completed,
            MilestoneStatus::Abandoned,
        ] {
            let parsed: MilestoneStatus = status.as_str().parse()?;
            assert_eq!(parsed, status);
        }
        Ok(())
    }

    #[test]
    fn snapshot_initial_validates() -> Result<(), Box<dyn std::error::Error>> {
        let now = Utc::now();
        let snapshot = MilestoneSnapshot::initial(now);
        snapshot.validate_semantics().map_err(|e| e.into())
    }

    #[test]
    fn snapshot_active_without_bead_is_invalid() -> Result<(), Box<dyn std::error::Error>> {
        let now = Utc::now();
        let mut snapshot = MilestoneSnapshot::initial(now);
        snapshot.status = MilestoneStatus::Active;
        assert!(snapshot.validate_semantics().is_err());
        Ok(())
    }

    #[test]
    fn snapshot_terminal_with_bead_is_invalid() -> Result<(), Box<dyn std::error::Error>> {
        let now = Utc::now();
        let mut snapshot = MilestoneSnapshot::initial(now);
        snapshot.status = MilestoneStatus::Completed;
        snapshot.active_bead = Some("bead-1".to_owned());
        assert!(snapshot.validate_semantics().is_err());
        Ok(())
    }

    #[test]
    fn progress_remaining_saturates() -> Result<(), Box<dyn std::error::Error>> {
        let progress = MilestoneProgress {
            total_beads: 5,
            completed_beads: 2,
            in_progress_beads: 1,
            failed_beads: 1,
            skipped_beads: 1,
            blocked_beads: 0,
        };
        assert_eq!(progress.remaining(), 1);
        Ok(())
    }

    #[test]
    fn progress_deserialization_requires_skipped_beads() {
        let error = serde_json::from_str::<MilestoneProgress>(
            r#"{"total_beads":5,"completed_beads":2,"in_progress_beads":1,"failed_beads":1,"blocked_beads":0}"#,
        )
        .expect_err("missing skipped_beads should fail deserialization");
        assert!(error.to_string().contains("skipped_beads"));
    }

    #[test]
    fn task_run_entry_deserialization_requires_milestone_id() {
        let error = serde_json::from_str::<TaskRunEntry>(
            r#"{"bead_id":"b1","project_id":"p1","outcome":"running","started_at":"2025-01-01T00:00:00Z"}"#,
        )
        .expect_err("missing milestone_id should fail deserialization");
        assert!(error.to_string().contains("milestone_id"));
    }

    #[test]
    fn journal_event_serializes_to_ndjson() -> Result<(), Box<dyn std::error::Error>> {
        let now = Utc::now();
        let event = MilestoneJournalEvent::new(MilestoneEventType::Created, now)
            .with_details("Initial milestone creation");
        let line = event.to_ndjson_line()?;
        let parsed: MilestoneJournalEvent = serde_json::from_str(&line)?;
        assert_eq!(parsed.event_type, MilestoneEventType::Created);
        assert!(parsed.details.is_some());
        Ok(())
    }

    #[test]
    fn milestone_record_toml_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let now = Utc::now();
        let record = MilestoneRecord::new(
            MilestoneId::new("test-ms")?,
            "Test Milestone".to_owned(),
            "A test milestone for round-trip serialization".to_owned(),
            now,
        );
        let toml_str = toml::to_string_pretty(&record)?;
        let parsed: MilestoneRecord = toml::from_str(&toml_str)?;
        assert_eq!(parsed.id, record.id);
        assert_eq!(parsed.schema_version, MILESTONE_SCHEMA_VERSION);
        Ok(())
    }

    #[test]
    fn milestone_snapshot_json_round_trip() -> Result<(), Box<dyn std::error::Error>> {
        let now = Utc::now();
        let snapshot = MilestoneSnapshot::initial(now);
        let json = serde_json::to_string_pretty(&snapshot)?;
        let parsed: MilestoneSnapshot = serde_json::from_str(&json)?;
        assert_eq!(parsed.status, MilestoneStatus::Planning);
        assert_eq!(parsed.plan_version, 0);
        Ok(())
    }

    #[test]
    fn task_run_outcome_terminal_check() -> Result<(), Box<dyn std::error::Error>> {
        assert!(!TaskRunOutcome::Pending.is_terminal());
        assert!(!TaskRunOutcome::Running.is_terminal());
        assert!(TaskRunOutcome::Succeeded.is_terminal());
        assert!(TaskRunOutcome::Failed.is_terminal());
        assert!(TaskRunOutcome::Skipped.is_terminal());
        Ok(())
    }

    #[test]
    fn collapse_task_run_attempts_merges_legacy_rows() -> Result<(), Box<dyn std::error::Error>> {
        let started_at = Utc::now();
        let collapsed = collapse_task_run_attempts(vec![
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: Some("plan-a".to_owned()),
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            },
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: None,
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: Some("done".to_owned()),
                started_at,
                finished_at: Some(started_at),
            },
        ]);

        assert_eq!(collapsed.len(), 1);
        assert_eq!(collapsed[0].milestone_id, "ms-1");
        assert_eq!(collapsed[0].run_id.as_deref(), Some("run-1"));
        assert_eq!(collapsed[0].plan_hash.as_deref(), Some("plan-a"));
        assert_eq!(collapsed[0].outcome_detail.as_deref(), Some("done"));
        Ok(())
    }

    #[test]
    fn collapse_task_run_attempts_deduplicates_terminal_replays(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let started_at = Utc::now();
        let finished_at = started_at + chrono::Duration::seconds(5);
        let collapsed = collapse_task_run_attempts(vec![
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            },
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: None,
                started_at,
                finished_at: Some(finished_at),
            },
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: None,
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: Some("replayed".to_owned()),
                started_at,
                finished_at: Some(finished_at),
            },
        ]);

        assert_eq!(collapsed.len(), 1);
        assert_eq!(collapsed[0].outcome, TaskRunOutcome::Succeeded);
        assert_eq!(collapsed[0].plan_hash.as_deref(), Some("plan-v1"));
        assert_eq!(collapsed[0].outcome_detail.as_deref(), Some("replayed"));
        Ok(())
    }

    #[test]
    fn same_attempt_prefers_run_id_when_present() -> Result<(), Box<dyn std::error::Error>> {
        let started_at = Utc::now();
        let first = TaskRunEntry {
            milestone_id: "ms-1".to_owned(),
            bead_id: "bead-1".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: Some("run-1".to_owned()),
            plan_hash: None,
            outcome: TaskRunOutcome::Running,
            outcome_detail: None,
            started_at,
            finished_at: None,
        };
        let second = TaskRunEntry {
            run_id: Some("run-2".to_owned()),
            ..first.clone()
        };

        assert!(!TaskRunEntry::same_attempt(&first, &second));
        Ok(())
    }

    #[test]
    fn latest_task_runs_per_bead_prefers_newest_retry_state(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let started_at = Utc::now();
        let entries = vec![
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("first attempt failed".to_owned()),
                started_at,
                finished_at: Some(started_at + chrono::Duration::seconds(5)),
            },
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-2".to_owned(),
                run_id: Some("run-2".to_owned()),
                plan_hash: Some("plan-v2".to_owned()),
                outcome: TaskRunOutcome::Succeeded,
                outcome_detail: Some("retry passed".to_owned()),
                started_at: started_at + chrono::Duration::seconds(10),
                finished_at: Some(started_at + chrono::Duration::seconds(20)),
            },
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-2".to_owned(),
                project_id: "project-3".to_owned(),
                run_id: Some("run-3".to_owned()),
                plan_hash: Some("plan-v3".to_owned()),
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: started_at + chrono::Duration::seconds(30),
                finished_at: None,
            },
        ];

        let latest = latest_task_runs_per_bead(&entries);
        assert_eq!(latest.len(), 2);
        assert!(latest.iter().any(|entry| {
            entry.bead_id == "bead-1" && entry.outcome == TaskRunOutcome::Succeeded
        }));
        assert!(latest.iter().any(|entry| {
            entry.bead_id == "bead-2" && entry.outcome == TaskRunOutcome::Running
        }));

        let active_beads = active_bead_ids(&entries);
        assert_eq!(active_beads.len(), 1);
        assert!(active_beads.contains("bead-2"));
        Ok(())
    }

    #[test]
    fn latest_task_runs_per_bead_prefers_later_same_timestamp_retry_state(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let started_at = Utc::now();
        let entries = vec![
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-1".to_owned()),
                plan_hash: Some("plan-v1".to_owned()),
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("first attempt failed".to_owned()),
                started_at,
                finished_at: Some(started_at + chrono::Duration::seconds(1)),
            },
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-2".to_owned(),
                run_id: Some("run-2".to_owned()),
                plan_hash: Some("plan-v2".to_owned()),
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            },
        ];

        let latest = latest_task_runs_per_bead(&entries);
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].run_id.as_deref(), Some("run-2"));
        assert_eq!(latest[0].outcome, TaskRunOutcome::Running);

        let active_beads = active_bead_ids(&entries);
        assert_eq!(active_beads.len(), 1);
        assert!(active_beads.contains("bead-1"));
        Ok(())
    }

    #[test]
    fn find_matching_running_task_run_backfills_named_replay_with_matching_started_at(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let started_at = Utc::now();
        let entries = vec![TaskRunEntry {
            milestone_id: "ms-1".to_owned(),
            bead_id: "bead-1".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: None,
            plan_hash: None,
            outcome: TaskRunOutcome::Running,
            outcome_detail: None,
            started_at,
            finished_at: None,
        }];

        let matched = find_matching_running_task_run(
            &entries,
            "bead-1",
            "project-1",
            Some("run-3"),
            started_at,
        )
        .expect("matching started_at should let a replay backfill a runless row");
        assert_eq!(matched.started_at, started_at);
        assert_eq!(matched.run_id, None);
        Ok(())
    }

    #[test]
    fn find_matching_running_task_run_reuses_sole_runless_attempt_for_matching_started_at(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let started_at = Utc::now();
        let entries = vec![TaskRunEntry {
            milestone_id: "ms-1".to_owned(),
            bead_id: "bead-1".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: None,
            plan_hash: Some("plan-v1".to_owned()),
            outcome: TaskRunOutcome::Running,
            outcome_detail: None,
            started_at,
            finished_at: None,
        }];

        let matched =
            find_matching_running_task_run(&entries, "bead-1", "project-1", None, started_at)
                .expect("matching started_at should reuse the same runless attempt");
        assert_eq!(matched.started_at, started_at);
        assert_eq!(matched.plan_hash.as_deref(), Some("plan-v1"));
        Ok(())
    }

    #[test]
    fn find_matching_running_task_run_treats_new_started_at_as_new_attempt(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let started_at = Utc::now();
        let entries = vec![TaskRunEntry {
            milestone_id: "ms-1".to_owned(),
            bead_id: "bead-1".to_owned(),
            project_id: "project-1".to_owned(),
            run_id: None,
            plan_hash: Some("plan-v1".to_owned()),
            outcome: TaskRunOutcome::Running,
            outcome_detail: None,
            started_at,
            finished_at: None,
        }];

        assert!(find_matching_running_task_run(
            &entries,
            "bead-1",
            "project-1",
            None,
            started_at + chrono::Duration::seconds(30),
        )
        .is_none());
        assert!(find_matching_running_task_run(
            &entries,
            "bead-1",
            "project-1",
            Some("run-3"),
            started_at + chrono::Duration::seconds(30),
        )
        .is_none());
        Ok(())
    }

    #[test]
    fn find_matching_running_task_run_rejects_mixed_legacy_and_named_open_attempts(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let started_at = Utc::now();
        let entries = vec![
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            },
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: Some("run-2".to_owned()),
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at: started_at + chrono::Duration::seconds(1),
                finished_at: None,
            },
        ];

        assert!(find_matching_running_task_run(
            &entries,
            "bead-1",
            "project-1",
            Some("run-3"),
            started_at + chrono::Duration::seconds(2),
        )
        .is_none());
        Ok(())
    }

    #[test]
    fn collapse_task_run_attempts_preserves_same_timestamp_legacy_retries(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let started_at = Utc::now();
        let collapsed = collapse_task_run_attempts(vec![
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            },
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("first retry".to_owned()),
                started_at,
                finished_at: Some(started_at + chrono::Duration::seconds(1)),
            },
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Running,
                outcome_detail: None,
                started_at,
                finished_at: None,
            },
            TaskRunEntry {
                milestone_id: "ms-1".to_owned(),
                bead_id: "bead-1".to_owned(),
                project_id: "project-1".to_owned(),
                run_id: None,
                plan_hash: None,
                outcome: TaskRunOutcome::Failed,
                outcome_detail: Some("second retry".to_owned()),
                started_at,
                finished_at: Some(started_at + chrono::Duration::seconds(2)),
            },
        ]);

        assert_eq!(collapsed.len(), 2);
        assert_eq!(collapsed[0].outcome_detail.as_deref(), Some("first retry"));
        assert_eq!(collapsed[1].outcome_detail.as_deref(), Some("second retry"));
        Ok(())
    }

    #[test]
    fn completion_journal_details_support_delimited_identifiers(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let details = render_completion_journal_details(
            "project, one",
            Some("run, 1"),
            Some("plan, v2"),
            TaskRunOutcome::Succeeded,
            Some("detail payload"),
        );

        let parsed: serde_json::Value = serde_json::from_str(&details)?;
        assert_eq!(
            parsed,
            serde_json::json!({
                "project_id": "project, one",
                "run_id": "run, 1",
                "plan_hash": "plan, v2",
                "outcome": "succeeded",
                "outcome_detail": "detail payload",
            })
        );
        Ok(())
    }
}
