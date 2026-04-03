use std::path::PathBuf;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::shared::domain::{FlowPreset, ReviewWhitelistConfig};
use crate::shared::error::{AppError, AppResult};

/// Dispatch mode for a daemon task — determines whether the task enters
/// workflow execution directly or goes through requirements drafting first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchMode {
    Workflow,
    RequirementsDraft,
    RequirementsQuick,
    RequirementsMilestone,
}

impl DispatchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Workflow => "workflow",
            Self::RequirementsDraft => "requirements_draft",
            Self::RequirementsQuick => "requirements_quick",
            Self::RequirementsMilestone => "requirements_milestone",
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
    /// GitHub repo slug (e.g. "owner/repo") for multi-repo daemon tasks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_slug: Option<String>,
    /// GitHub issue number for this task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issue_number: Option<u64>,
    /// GitHub PR URL associated with this task.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_url: Option<String>,
    /// Dedup cursor: last-seen comment ID for incremental comment ingestion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_comment_id: Option<u64>,
    /// Dedup cursor: last-seen review ID for incremental review ingestion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_review_id: Option<u64>,
    /// True when the GitHub status label is known to be out of sync with
    /// durable task state. Set on label-sync failure; cleared by reconcile
    /// or a successful subsequent sync.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub label_dirty: bool,
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

/// GitHub-specific task metadata for multi-repo daemon tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GithubTaskMeta {
    pub repo_slug: String,
    pub issue_number: u64,
    pub issue_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pr_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_comment_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen_review_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ReviewWhitelist {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_usernames: Vec<String>,
}

impl ReviewWhitelist {
    pub fn from_config(config: &ReviewWhitelistConfig) -> Self {
        let mut usernames = config
            .usernames()
            .iter()
            .map(|name| name.trim().to_ascii_lowercase())
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        usernames.sort();
        usernames.dedup();
        Self {
            allowed_usernames: usernames,
        }
    }

    pub fn allows(&self, username: &str) -> bool {
        if self.allowed_usernames.is_empty() {
            return true;
        }
        let normalized = username.trim().to_ascii_lowercase();
        self.allowed_usernames
            .iter()
            .any(|candidate| candidate == &normalized)
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
            (
                Self::Active,
                Self::Pending
                | Self::Completed
                | Self::Failed
                | Self::Aborted
                | Self::WaitingForRequirements,
            ) => true,
            (
                Self::WaitingForRequirements,
                Self::Pending | Self::Completed | Self::Failed | Self::Aborted,
            ) => true,
            (Self::Failed | Self::Aborted, Self::Pending) => true,
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

fn lease_heartbeat_deadline(last_heartbeat: DateTime<Utc>, ttl_seconds: u64) -> DateTime<Utc> {
    saturating_heartbeat_deadline(last_heartbeat, ttl_seconds)
}

/// Compute a heartbeat deadline with saturating semantics. If the TTL value
/// is too large for `chrono::Duration` or the resulting `DateTime` overflows,
/// returns `DateTime::MAX_UTC` so the lease is never considered stale.
pub fn saturating_heartbeat_deadline(
    last_heartbeat: DateTime<Utc>,
    ttl_seconds: u64,
) -> DateTime<Utc> {
    let ttl_i64 = ttl_seconds.min(i64::MAX as u64) as i64;
    match Duration::try_seconds(ttl_i64) {
        Some(dur) => last_heartbeat
            .checked_add_signed(dur)
            .unwrap_or(DateTime::<Utc>::MAX_UTC),
        None => DateTime::<Utc>::MAX_UTC,
    }
}

/// Convert a `u64` TTL to a bounded `i64` suitable for
/// `chrono::Duration::try_seconds()`. Values above `i64::MAX` are saturated
/// to `i64::MAX` rather than wrapping negative. Callers must still use
/// `try_seconds()` (not `seconds()`) because chrono's `TimeDelta` may reject
/// even `i64::MAX`.
pub fn saturating_ttl_seconds(ttl: u64) -> i64 {
    ttl.min(i64::MAX as u64) as i64
}

impl WorktreeLease {
    pub fn heartbeat_deadline(&self) -> DateTime<Utc> {
        lease_heartbeat_deadline(self.last_heartbeat, self.ttl_seconds)
    }

    pub fn is_stale_at(&self, now: DateTime<Utc>) -> bool {
        now > self.heartbeat_deadline()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CliWriterLease {
    pub lease_id: String,
    pub project_id: String,
    pub owner: String,
    pub acquired_at: DateTime<Utc>,
    pub ttl_seconds: u64,
    pub last_heartbeat: DateTime<Utc>,
}

impl CliWriterLease {
    pub fn heartbeat_deadline(&self) -> DateTime<Utc> {
        lease_heartbeat_deadline(self.last_heartbeat, self.ttl_seconds)
    }

    pub fn is_stale_at(&self, now: DateTime<Utc>) -> bool {
        now > self.heartbeat_deadline()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "LeaseRecordWire", into = "TaggedLeaseRecord")]
pub enum LeaseRecord {
    Worktree(WorktreeLease),
    CliWriter(CliWriterLease),
}

impl LeaseRecord {
    pub fn lease_id(&self) -> &str {
        match self {
            Self::Worktree(lease) => &lease.lease_id,
            Self::CliWriter(lease) => &lease.lease_id,
        }
    }

    pub fn project_id(&self) -> &str {
        match self {
            Self::Worktree(lease) => &lease.project_id,
            Self::CliWriter(lease) => &lease.project_id,
        }
    }

    pub fn acquired_at(&self) -> &DateTime<Utc> {
        match self {
            Self::Worktree(lease) => &lease.acquired_at,
            Self::CliWriter(lease) => &lease.acquired_at,
        }
    }

    pub fn is_stale_at(&self, now: DateTime<Utc>) -> bool {
        match self {
            Self::Worktree(lease) => lease.is_stale_at(now),
            Self::CliWriter(lease) => lease.is_stale_at(now),
        }
    }
}

impl From<WorktreeLease> for LeaseRecord {
    fn from(value: WorktreeLease) -> Self {
        Self::Worktree(value)
    }
}

impl From<CliWriterLease> for LeaseRecord {
    fn from(value: CliWriterLease) -> Self {
        Self::CliWriter(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
enum LeaseRecordWire {
    Tagged(TaggedLeaseRecord),
    LegacyWorktree(LegacyWorktreeLease),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "lease_kind", rename_all = "snake_case")]
enum TaggedLeaseRecord {
    Worktree(WorktreeLease),
    CliWriter(CliWriterLease),
}

impl From<LeaseRecord> for TaggedLeaseRecord {
    fn from(value: LeaseRecord) -> Self {
        match value {
            LeaseRecord::Worktree(lease) => Self::Worktree(lease),
            LeaseRecord::CliWriter(lease) => Self::CliWriter(lease),
        }
    }
}

impl From<TaggedLeaseRecord> for LeaseRecord {
    fn from(value: TaggedLeaseRecord) -> Self {
        match value {
            TaggedLeaseRecord::Worktree(lease) => Self::Worktree(lease),
            TaggedLeaseRecord::CliWriter(lease) => Self::CliWriter(lease),
        }
    }
}

impl From<LeaseRecordWire> for LeaseRecord {
    fn from(value: LeaseRecordWire) -> Self {
        match value {
            LeaseRecordWire::Tagged(record) => record.into(),
            LeaseRecordWire::LegacyWorktree(lease) => Self::Worktree(lease.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct LegacyWorktreeLease {
    lease_id: String,
    task_id: String,
    project_id: String,
    worktree_path: PathBuf,
    branch_name: String,
    acquired_at: DateTime<Utc>,
    ttl_seconds: u64,
    last_heartbeat: DateTime<Utc>,
}

impl From<LegacyWorktreeLease> for WorktreeLease {
    fn from(value: LegacyWorktreeLease) -> Self {
        Self {
            lease_id: value.lease_id,
            task_id: value.task_id,
            project_id: value.project_id,
            worktree_path: value.worktree_path,
            branch_name: value.branch_name,
            acquired_at: value.acquired_at,
            ttl_seconds: value.ttl_seconds,
            last_heartbeat: value.last_heartbeat,
        }
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
    ClaimRollback,
    WatcherIngestion,
    RequirementsHandoff,
    RequirementsWaiting,
    RequirementsResumed,
    RoutingWarning,
    RebaseStarted,
    RebaseCompleted,
    RebaseConflict,
    RebaseAgentResolution,
    DraftPrCreated,
    PrClosed,
    PrMarkedReady,
    ReviewsIngested,
    AmendmentsStaged,
    ProjectReopened,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebaseFailureClassification {
    Conflict,
    Timeout,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum RebaseOutcome {
    Success,
    AgentResolved {
        resolved_files: Vec<String>,
        summary: String,
    },
    Failed {
        classification: RebaseFailureClassification,
        details: String,
    },
}
