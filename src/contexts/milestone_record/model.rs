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
    /// Beads that have been completed.
    pub completed_beads: u32,
    /// Beads currently in progress.
    pub in_progress_beads: u32,
    /// Beads that failed and need attention.
    pub failed_beads: u32,
    /// Beads that are blocked by dependencies.
    pub blocked_beads: u32,
}

impl MilestoneProgress {
    pub fn remaining(&self) -> u32 {
        self.total_beads
            .saturating_sub(self.completed_beads)
            .saturating_sub(self.failed_beads)
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
    /// Bead ID from the `.beads/` graph.
    pub bead_id: String,
    /// Ralph project ID for this bead's execution.
    pub project_id: String,
    /// Outcome of the task run.
    pub outcome: TaskRunOutcome,
    /// When this task run was started.
    pub started_at: DateTime<Utc>,
    /// When this task run finished (if complete).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
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
            completed_beads: 3,
            in_progress_beads: 1,
            failed_beads: 1,
            blocked_beads: 0,
        };
        assert_eq!(progress.remaining(), 1);
        Ok(())
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
}
