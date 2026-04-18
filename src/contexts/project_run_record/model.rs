use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::contexts::milestone_record::queries::BeadLineageView;
use crate::contexts::workflow_composition::panel_contracts::{RecordKind, RecordProducer};
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
    /// Milestone linkage: present when this project executes a bead within a milestone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_source: Option<TaskSource>,
}

/// Describes the origin of a project when it executes a milestone bead.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskSource {
    /// The milestone this task belongs to.
    pub milestone_id: String,
    /// The bead being executed.
    pub bead_id: String,
    /// Parent epic or root epic ID when useful for context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_epic_id: Option<String>,
    /// How this task was created.
    #[serde(default)]
    pub origin: TaskOrigin,
    /// Stable milestone plan hash captured when the task was created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,
    /// Plan version at the time this task was created.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_version: Option<u32>,
}

impl ProjectRecord {
    /// Returns true if this project is a milestone bead execution.
    pub fn is_milestone_task(&self) -> bool {
        self.task_source.is_some()
    }

    /// Returns the milestone ID if this is a milestone task.
    pub fn milestone_id(&self) -> Option<&str> {
        self.task_source.as_ref().map(|ts| ts.milestone_id.as_str())
    }

    /// Returns the bead ID if this is a milestone task.
    pub fn bead_id(&self) -> Option<&str> {
        self.task_source.as_ref().map(|ts| ts.bead_id.as_str())
    }
}

/// How a task-mode project was created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskOrigin {
    /// Created by the milestone controller.
    #[default]
    Milestone,
    /// Created manually by a user.
    Manual,
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
    /// Preserved across failure/pause/rollback so resume can recover the
    /// interrupted cycle baseline even after `active_run` is cleared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interrupted_run: Option<ActiveRun>,
    pub status: RunStatus,
    pub cycle_history: Vec<CycleHistoryEntry>,
    pub completion_rounds: u32,
    /// The configured maximum completion rounds for this run.
    /// Stored so operators can see the limit alongside the current count.
    /// `None` indicates a legacy snapshot written before this field existed,
    /// and must be preserved during read-only inspection so historical
    /// artifacts do not silently inherit present-day config.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_completion_rounds: Option<u32>,
    pub rollback_point_meta: RollbackPointMeta,
    pub amendment_queue: AmendmentQueueState,
    pub status_summary: String,
    /// Preserved across failure/pause so resume can detect drift.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_stage_resolution_snapshot: Option<StageResolutionSnapshot>,
}

impl RunSnapshot {
    pub fn initial(max_completion_rounds: u32) -> Self {
        Self {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::NotStarted,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            max_completion_rounds: Some(max_completion_rounds),
            rollback_point_meta: RollbackPointMeta::default(),
            amendment_queue: AmendmentQueueState::default(),
            status_summary: "not started".to_owned(),
            last_stage_resolution_snapshot: None,
        }
    }

    pub fn has_active_run(&self) -> bool {
        self.active_run.is_some()
    }

    /// Semantic validation of run snapshot consistency.
    /// Returns `Err` with a description if status and active_run are inconsistent.
    pub fn validate_semantics(&self) -> Result<(), String> {
        match (&self.status, &self.active_run) {
            // Running requires an active run cursor.
            (RunStatus::Running, None) => Err(
                "status is 'running' but active_run is null — inconsistent canonical state"
                    .to_owned(),
            ),
            (RunStatus::Paused, Some(_)) => Err(
                "status is 'paused' but active_run is present — inconsistent canonical state"
                    .to_owned(),
            ),
            // All non-running states must not retain an active run cursor.
            (RunStatus::NotStarted, Some(_)) => Err(
                "status is 'not_started' but active_run is present — inconsistent canonical state"
                    .to_owned(),
            ),
            (RunStatus::Completed, Some(_)) => Err(
                "status is 'completed' but active_run is present — inconsistent canonical state"
                    .to_owned(),
            ),
            (RunStatus::Failed, Some(_)) => Err(
                "status is 'failed' but active_run is present — inconsistent canonical state"
                    .to_owned(),
            ),
            // Valid combinations:
            // NotStarted/Paused/Completed/Failed + None, Running + Some
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActiveRun {
    pub run_id: String,
    pub stage_cursor: StageCursor,
    pub started_at: DateTime<Utc>,
    #[serde(default)]
    pub prompt_hash_at_cycle_start: String,
    #[serde(default)]
    pub prompt_hash_at_stage_start: String,
    #[serde(default)]
    pub qa_iterations_current_cycle: u32,
    #[serde(default)]
    pub review_iterations_current_cycle: u32,
    #[serde(default)]
    pub final_review_restart_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iterative_implementer_state: Option<IterativeImplementerState>,
    /// Resolution snapshot persisted at stage start before any agent invocation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_resolution_snapshot: Option<StageResolutionSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IterativeImplementerLoopPolicy {
    pub max_consecutive_implementer_rounds: u32,
    pub stable_rounds_required: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IterativeImplementerState {
    #[serde(default)]
    pub completed_iterations: u32,
    #[serde(default)]
    pub stable_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loop_policy: Option<IterativeImplementerLoopPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_target: Option<ResolvedTargetRecord>,
}

/// Records the exact resolved backend/model targets at stage start.
///
/// For single-target stages this contains one resolved target. For panel stages
/// it records the ordered members and any single-purpose panel targets used by
/// prompt-review, completion, and final-review.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageResolutionSnapshot {
    pub stage_id: StageId,
    pub resolved_at: DateTime<Utc>,
    /// The primary single-target resolution (for non-panel stages).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_target: Option<ResolvedTargetRecord>,
    /// Ordered panel members for prompt-review validators.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prompt_review_validators: Vec<ResolvedTargetRecord>,
    /// The prompt-review refiner target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_review_refiner: Option<ResolvedTargetRecord>,
    /// Ordered panel members for completion completers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completion_completers: Vec<ResolvedTargetRecord>,
    /// Ordered panel members for final-review reviewers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub final_review_reviewers: Vec<ResolvedTargetRecord>,
    /// The final-review arbiter target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_review_arbiter: Option<ResolvedTargetRecord>,
}

/// A serializable resolved target record for snapshot persistence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedTargetRecord {
    pub backend_family: String,
    pub model_id: String,
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

impl RunStatus {
    /// Human-readable status string for CLI output.
    pub fn display_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not started",
            Self::Running => "running",
            Self::Paused => "paused",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.display_str())
    }
}

/// A single entry in the cycle history tracking progression through work cycles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CycleHistoryEntry {
    pub cycle: u32,
    pub stage_id: StageId,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// Rollback-point metadata tracked in the canonical run snapshot.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RollbackPointMeta {
    pub last_rollback_id: Option<String>,
    pub rollback_count: u32,
}

/// Source of an amendment for metadata and dedup purposes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AmendmentSource {
    Manual,
    PrReview,
    IssueCommand,
    WorkflowStage,
}

impl AmendmentSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::PrReview => "pr_review",
            Self::IssueCommand => "issue_command",
            Self::WorkflowStage => "workflow_stage",
        }
    }
}

impl std::fmt::Display for AmendmentSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A typed queued amendment record for durable persistence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueuedAmendment {
    pub amendment_id: String,
    pub source_stage: StageId,
    pub source_cycle: u32,
    pub source_completion_round: u32,
    pub body: String,
    pub created_at: DateTime<Utc>,
    /// Stable ordering key within a batch. Amendments created in the same batch
    /// share a `created_at` timestamp; `batch_sequence` distinguishes their order.
    #[serde(default)]
    pub batch_sequence: u32,
    /// Tracks the origin of this amendment for dedup and metadata purposes.
    #[serde(default = "default_amendment_source")]
    pub source: AmendmentSource,
    /// Deterministic dedup key. Two amendments with the same dedup_key are considered
    /// duplicates. For manual amendments this is derived from normalized body + source.
    #[serde(default)]
    pub dedup_key: String,
}

fn default_amendment_source() -> AmendmentSource {
    AmendmentSource::WorkflowStage
}

impl QueuedAmendment {
    /// Compute a deterministic dedup key from source and normalized body.
    pub fn compute_dedup_key(source: &AmendmentSource, body: &str) -> String {
        use sha2::{Digest, Sha256};
        let normalized = body.split_whitespace().collect::<Vec<_>>().join(" ");
        let mut hasher = Sha256::new();
        hasher.update(source.as_str().as_bytes());
        hasher.update(b":");
        hasher.update(normalized.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

/// Amendment queue state tracked in the canonical run snapshot.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AmendmentQueueState {
    pub pending: Vec<QueuedAmendment>,
    pub processed_count: u32,
    /// Snapshot-only follow-ups captured from conditional approvals on flows
    /// that do not use durable completion-round amendments.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recorded_follow_ups: Vec<QueuedAmendment>,
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
    RunResumed,
    StageEntered,
    StageFailed,
    StageCompleted,
    StageSkipped,
    CycleAdvanced,
    CompletionRoundAdvanced,
    RunCompleted,
    RunFailed,
    RollbackCreated,
    RollbackPerformed,
    ReviewerStarted,
    ReviewerCompleted,
    ImplementerIterationStarted,
    ImplementerIterationCompleted,
    ImplementerLoopExited,
    AmendmentQueued,
    DurableWarning,
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
    /// Discriminates primary, supporting, and aggregate records.
    #[serde(default = "default_record_kind")]
    pub record_kind: RecordKind,
    /// Who produced this record (agent, local validation, or system).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<RecordProducer>,
    /// The completion round during which this record was produced.
    #[serde(default = "default_completion_round")]
    pub completion_round: u32,
}

/// A durable history artifact record stored in `history/artifacts/`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRecord {
    pub artifact_id: String,
    pub payload_id: String,
    pub stage_id: StageId,
    pub created_at: DateTime<Utc>,
    pub content: String,
    /// Discriminates primary, supporting, and aggregate records.
    #[serde(default = "default_record_kind")]
    pub record_kind: RecordKind,
    /// Who produced this record (agent, local validation, or system).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer: Option<RecordProducer>,
    /// The completion round during which this record was produced.
    #[serde(default = "default_completion_round")]
    pub completion_round: u32,
}

fn default_record_kind() -> RecordKind {
    RecordKind::StagePrimary
}

fn default_completion_round() -> u32 {
    1
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_sha: Option<String>,
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

/// Summary of a failed stage for run snapshot persistence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FailedStageSummary {
    pub stage_id: StageId,
    pub cycle: u32,
    pub attempt: u32,
    pub failure_class: String,
    pub message: String,
    pub failed_at: DateTime<Utc>,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectDetail {
    pub record: ProjectRecord,
    pub run_snapshot: RunSnapshot,
    pub journal_event_count: u64,
    pub rollback_count: usize,
    pub is_active: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_lineage: Option<BeadLineageView>,
}
