use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::shared::domain::{FlowPreset, ProjectId, StageCursor, StageId};

/// Immutable project metadata persisted in `project.toml`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectRecord {
    pub id: ProjectId,
    pub name: String,
    pub flow: FlowPreset,
    pub prompt_reference: String,
    pub prompt_hash: String,
    pub created_at: DateTime<Utc>,
    pub status_summary: ProjectStatusSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatusSummary {
    Created,
    Active,
    Completed,
    Failed,
}

/// Canonical run state persisted in `run.json`.
/// This is the single source of truth for run progression - never inferred from artifacts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSnapshot {
    pub active_run: Option<ActiveRun>,
    pub status: RunStatus,
    pub status_summary: String,
}

impl RunSnapshot {
    pub fn initial() -> Self {
        Self {
            active_run: None,
            status: RunStatus::NotStarted,
            status_summary: "not started".to_owned(),
        }
    }

    pub fn has_active_run(&self) -> bool {
        self.active_run.is_some()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActiveRun {
    pub run_id: String,
    pub stage_cursor: StageCursor,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    NotStarted,
    Running,
    Paused,
    Completed,
    Failed,
}

/// A single journal event stored in `journal.ndjson`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JournalEvent {
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
    pub event_type: JournalEventType,
    pub details: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalEventType {
    ProjectCreated,
    RunStarted,
    StageEntered,
    StageCompleted,
    CycleAdvanced,
    CompletionRoundAdvanced,
    RunCompleted,
    RunFailed,
    RollbackCreated,
    AmendmentQueued,
}

/// A durable history payload record stored in `history/payloads/`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PayloadRecord {
    pub payload_id: String,
    pub stage_id: StageId,
    pub cycle: u32,
    pub attempt: u32,
    pub created_at: DateTime<Utc>,
    pub payload: serde_json::Value,
}

/// A durable history artifact record stored in `history/artifacts/`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub artifact_id: String,
    pub payload_id: String,
    pub stage_id: StageId,
    pub created_at: DateTime<Utc>,
    pub content: String,
}

/// A runtime log entry stored in `runtime/logs/`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeLogEntry {
    pub timestamp: DateTime<Utc>,
    pub level: LogLevel,
    pub source: String,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

/// Rollback metadata stored in `rollback/`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RollbackPoint {
    pub rollback_id: String,
    pub created_at: DateTime<Utc>,
    pub stage_id: StageId,
    pub cycle: u32,
    pub run_snapshot: RunSnapshot,
}

/// Empty session store for `sessions.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionStore {
    pub sessions: Vec<serde_json::Value>,
}

impl SessionStore {
    pub fn empty() -> Self {
        Self {
            sessions: Vec::new(),
        }
    }
}

/// Summary used in `project list` output.
#[derive(Debug, Clone)]
pub struct ProjectListEntry {
    pub id: ProjectId,
    pub name: String,
    pub flow: FlowPreset,
    pub status_summary: ProjectStatusSummary,
    pub created_at: DateTime<Utc>,
    pub is_active: bool,
}

/// Detailed project view used in `project show` output.
#[derive(Debug, Clone)]
pub struct ProjectDetail {
    pub record: ProjectRecord,
    pub run_snapshot: RunSnapshot,
    pub journal_event_count: u64,
    pub rollback_count: usize,
    pub is_active: bool,
}
