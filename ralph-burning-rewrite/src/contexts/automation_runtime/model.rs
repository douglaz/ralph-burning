use std::path::PathBuf;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::shared::domain::FlowPreset;
use crate::shared::error::{AppError, AppResult};

/// Dispatch mode for a daemon task — determines whether the task enters
/// workflow execution directly or goes through requirements drafting first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchMode {
    Workflow,
    RequirementsDraft,
    RequirementsQuick,
}

impl DispatchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Workflow => "workflow",
            Self::RequirementsDraft => "requirements_draft",
            Self::RequirementsQuick => "requirements_quick",
        }
    }
}

impl std::fmt::Display for DispatchMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Metadata captured from a watched issue source for idempotent ingestion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchedIssueMeta {
    pub issue_ref: String,
    pub source_revision: String,
    pub title: String,
    pub body: String,
    pub labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_command: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonTask {
    pub task_id: String,
    pub issue_ref: String,
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_command: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routing_labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_flow: Option<FlowPreset>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_source: Option<RoutingSource>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routing_warnings: Vec<String>,
    pub status: TaskStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub attempt_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_message: Option<String>,
    /// Dispatch mode for this task (workflow vs requirements handoff).
    #[serde(default = "default_dispatch_mode")]
    pub dispatch_mode: DispatchMode,
    /// Stable source revision for idempotent re-polling.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_revision: Option<String>,
    /// Linked requirements run ID (set during requirements_draft or requirements_quick dispatch).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requirements_run_id: Option<String>,
}

fn default_dispatch_mode() -> DispatchMode {
    DispatchMode::Workflow
}

impl DaemonTask {
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }

    pub fn transition_to(&mut self, next: TaskStatus, now: DateTime<Utc>) -> AppResult<()> {
        if !self.status.can_transition_to(next) {
            return Err(AppError::TaskStateTransitionInvalid {
                task_id: self.task_id.clone(),
                from: self.status.as_str().to_owned(),
                to: next.as_str().to_owned(),
            });
        }

        self.status = next;
        self.updated_at = now;
        Ok(())
    }

    pub fn attach_lease(&mut self, lease_id: impl Into<String>) {
        self.lease_id = Some(lease_id.into());
    }

    pub fn clear_lease(&mut self) {
        self.lease_id = None;
    }

    pub fn clear_failure(&mut self) {
        self.failure_class = None;
        self.failure_message = None;
    }

    pub fn set_failure(
        &mut self,
        failure_class: impl Into<String>,
        failure_message: impl Into<String>,
    ) {
        self.failure_class = Some(failure_class.into());
        self.failure_message = Some(failure_message.into());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Claimed,
    Active,
    /// Waiting for external input (e.g. requirements answers). Holds no lease,
    /// no writer lock, no active workflow run.
    WaitingForRequirements,
    Completed,
    Failed,
    Aborted,
}

impl TaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Claimed => "claimed",
            Self::Active => "active",
            Self::WaitingForRequirements => "waiting_for_requirements",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Aborted => "aborted",
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Aborted)
    }

    /// Whether this status is non-terminal and counts as an active task for
    /// duplicate-issue detection purposes.
    pub fn is_active_for_issue(self) -> bool {
        !self.is_terminal()
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        match (self, next) {
            (Self::Pending, Self::Claimed | Self::Failed) => true,
            (Self::Claimed, Self::Active | Self::Failed | Self::Aborted) => true,
            (Self::Active, Self::Pending | Self::Completed | Self::Failed | Self::Aborted | Self::WaitingForRequirements) => true,
            (Self::WaitingForRequirements, Self::Pending | Self::Failed | Self::Aborted) => true,
            (Self::Failed, Self::Pending) => true,
            _ if self == next => true,
            _ => false,
        }
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorktreeLease {
    pub lease_id: String,
    pub task_id: String,
    pub project_id: String,
    pub worktree_path: PathBuf,
    pub branch_name: String,
    pub acquired_at: DateTime<Utc>,
    pub ttl_seconds: u64,
    pub last_heartbeat: DateTime<Utc>,
}

impl WorktreeLease {
    pub fn heartbeat_deadline(&self) -> DateTime<Utc> {
        self.last_heartbeat + Duration::seconds(self.ttl_seconds.min(i64::MAX as u64) as i64)
    }

    pub fn is_stale_at(&self, now: DateTime<Utc>) -> bool {
        now > self.heartbeat_deadline()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutingSource {
    Command,
    Label,
    DefaultFlow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingResolution {
    pub flow: FlowPreset,
    pub source: RoutingSource,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DaemonJournalEvent {
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
    pub event_type: DaemonJournalEventType,
    pub details: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DaemonJournalEventType {
    TaskCreated,
    TaskClaimed,
    TaskCompleted,
    TaskFailed,
    TaskAborted,
    LeaseAcquired,
    LeaseReleased,
    ReconciliationRun,
    WatcherIngestion,
    RequirementsHandoff,
    RequirementsWaiting,
    RequirementsResumed,
    RoutingWarning,
}
