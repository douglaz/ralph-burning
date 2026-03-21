use std::path::PathBuf;

use thiserror::Error;

use crate::shared::domain::{FailureClass, StageId};

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("workspace already initialized at {path}")]
    AlreadyInitialized { path: PathBuf },
    #[error(
        "cannot initialize workspace because {path} already exists without a workspace configuration"
    )]
    WorkspaceConflict { path: PathBuf },
    #[error("invalid {field}: must be greater than zero")]
    InvalidStageCursorField { field: &'static str },
    #[error("stage cursor overflow for {field}: cannot increment {value}")]
    StageCursorOverflow { field: &'static str, value: u32 },
    #[error("invalid identifier '{value}': identifiers must be non-empty single path segments")]
    InvalidIdentifier { value: String },
    #[error(
        "unknown flow preset '{flow_id}'. supported presets: standard, quick_dev, docs_change, ci_improvement"
    )]
    InvalidFlowPreset { flow_id: String },
    #[error("unsupported workspace version {version}; supported version is {supported}")]
    UnsupportedWorkspaceVersion { version: u32, supported: u32 },
    #[error("unknown config key '{key}'")]
    UnknownConfigKey { key: String },
    #[error("invalid value '{value}' for config key '{key}': {reason}")]
    InvalidConfigValue {
        key: String,
        value: String,
        reason: String,
    },
    #[error("no active project selected; run `ralph-burning project select <id>`")]
    NoActiveProject,
    #[error(
        "project '{project_id}' was not found under .ralph-burning/projects/ or is missing project.toml"
    )]
    ProjectNotFound { project_id: String },
    #[error("editor '{editor}' failed{details}")]
    EditorFailed { editor: String, details: String },
    #[error("project '{project_id}' already exists")]
    DuplicateProject { project_id: String },
    #[error("corrupt record in {file}: {details}")]
    CorruptRecord { file: String, details: String },
    #[error("invalid prompt file '{path}': {reason}")]
    InvalidPrompt { path: String, reason: String },
    #[error("cannot delete project '{project_id}': it has an active run")]
    ActiveRunDelete { project_id: String },
    #[error("journal sequence error: {details}")]
    JournalSequence { details: String },
    #[error("cannot start run: {reason}")]
    RunStartFailed { reason: String },
    #[error("cannot resume run: {reason}")]
    ResumeFailed { reason: String },
    #[error(
        "cannot rollback project '{project_id}' while run status is '{status}'; rollback is only allowed from failed or paused snapshots"
    )]
    RollbackInvalidStatus { project_id: String, status: String },
    #[error(
        "cannot rollback project '{project_id}' to stage '{stage_id}': stage is not part of flow '{flow}'"
    )]
    RollbackStageNotInFlow {
        project_id: String,
        stage_id: String,
        flow: String,
    },
    #[error(
        "cannot rollback project '{project_id}' to stage '{stage_id}': no persisted rollback point exists"
    )]
    RollbackPointNotFound {
        project_id: String,
        stage_id: String,
    },
    #[error(
        "hard rollback for project '{project_id}' failed after the logical rollback was committed at rollback point '{rollback_id}': {details}"
    )]
    RollbackGitResetFailed {
        project_id: String,
        rollback_id: String,
        details: String,
    },
    #[error("preflight check failed for stage '{stage_id}': {details}")]
    PreflightFailed { stage_id: StageId, details: String },
    #[error("stage commit failed for stage '{stage_id}': {details}")]
    StageCommitFailed { stage_id: StageId, details: String },
    #[error("{command} is not yet implemented")]
    NotYetImplemented { command: String },
    #[error("backend '{backend}' is unavailable: {details}")]
    BackendUnavailable { backend: String, details: String },
    #[error("backend '{backend}' cannot satisfy contract '{contract_id}': {details}")]
    CapabilityMismatch {
        backend: String,
        contract_id: String,
        details: String,
    },
    #[error("backend invocation failed for contract '{contract_id}' via '{backend}': {details}")]
    InvocationFailed {
        backend: String,
        contract_id: String,
        failure_class: FailureClass,
        details: String,
    },
    #[error(
        "backend invocation timed out for contract '{contract_id}' via '{backend}' after {timeout_ms} ms"
    )]
    InvocationTimeout {
        backend: String,
        contract_id: String,
        timeout_ms: u64,
    },
    #[error("backend invocation cancelled for contract '{contract_id}' via '{backend}'")]
    InvocationCancelled {
        backend: String,
        contract_id: String,
    },
    #[error("invalid requirements state for run '{run_id}': {details}")]
    InvalidRequirementsState { run_id: String, details: String },
    #[error("answer validation failed for run '{run_id}': {details}")]
    AnswerValidationFailed { run_id: String, details: String },
    #[error("seed persistence failed for run '{run_id}': {details}")]
    SeedPersistenceFailed { run_id: String, details: String },
    #[error("remediation exhausted at cycle {cycle}; maximum supported cycles is {max}")]
    RemediationExhausted { cycle: u32, max: u32 },
    #[error("amendment queue error: {details}")]
    AmendmentQueueError { details: String },
    #[error("duplicate amendment: existing amendment '{amendment_id}' has the same content")]
    DuplicateAmendment { amendment_id: String },
    #[error("amendment not found: '{amendment_id}'")]
    AmendmentNotFound { amendment_id: String },
    #[error("payload not found: '{payload_id}'")]
    PayloadNotFound { payload_id: String },
    #[error("artifact not found: '{artifact_id}'")]
    ArtifactNotFound { artifact_id: String },
    #[error(
        "cannot modify amendments for project '{project_id}': a writer lease is currently held"
    )]
    AmendmentLeaseConflict { project_id: String },
    #[error("amendment clear partially failed: removed {removed_count} of {total} amendments")]
    AmendmentClearPartial {
        removed: Vec<String>,
        remaining: Vec<String>,
        removed_count: usize,
        total: usize,
    },
    #[error("completion blocked: {details}")]
    CompletionBlocked { details: String },
    #[error("completion guard snapshot commit failed: {details}")]
    CompletionGuardSnapshotFailed { details: String },
    #[error("routing resolution failed for '{input}': {details}")]
    RoutingResolutionFailed { input: String, details: String },
    #[error("ambiguous routing labels resolved to multiple flows: {labels:?}")]
    AmbiguousRouting { labels: Vec<String> },
    #[error("task for issue '{issue_ref}' already exists with non-terminal status")]
    DuplicateTaskForIssue { issue_ref: String },
    #[error("project writer lock is already held for '{project_id}'")]
    ProjectWriterLockHeld { project_id: String },
    #[error("failed to create worktree for task '{task_id}': {details}")]
    WorktreeCreationFailed { task_id: String, details: String },
    #[error("failed to remove worktree for task '{task_id}': {details}")]
    WorktreeRemovalFailed { task_id: String, details: String },
    #[error("lease '{lease_id}' is stale")]
    LeaseStale { lease_id: String },
    #[error("invalid task state transition for '{task_id}': {from} -> {to}")]
    TaskStateTransitionInvalid {
        task_id: String,
        from: String,
        to: String,
    },
    #[error("rebase conflict for branch '{branch_name}': {details}")]
    RebaseConflict {
        branch_name: String,
        details: String,
    },
    #[error("watcher ingestion failed for issue '{issue_ref}': {details}")]
    WatcherIngestionFailed { issue_ref: String, details: String },
    #[error("requirements handoff failed for task '{task_id}': {details}")]
    RequirementsHandoffFailed { task_id: String, details: String },
    #[error("duplicate watched issue '{issue_ref}' with source_revision '{source_revision}'")]
    DuplicateWatchedIssue {
        issue_ref: String,
        source_revision: String,
    },
    #[error(
        "reconcile cleanup failed: {failed_count} stale lease(s) could not be fully cleaned up"
    )]
    ReconcileCleanupFailed { failed_count: usize },
    #[error(
        "lease cleanup partially failed for task '{task_id}': some resources could not be released"
    )]
    LeaseCleanupPartialFailure { task_id: String },
    #[error("CLI writer-lease guard close failed at step '{step}': {details}")]
    GuardCloseFailed { step: String, details: String },
    #[error("acquisition rollback failed: {trigger}; rollback: {rollback_details}")]
    AcquisitionRollbackFailed {
        trigger: String,
        rollback_details: String,
    },
    #[error("insufficient panel members for '{panel}': resolved {resolved}, minimum {minimum}")]
    InsufficientPanelMembers {
        panel: String,
        resolved: usize,
        minimum: usize,
    },
    #[error("prompt review rejected: {details}")]
    PromptReviewRejected { details: String },
    #[error("resume drift failure for stage '{stage_id}': {details}")]
    ResumeDriftFailure { stage_id: StageId, details: String },
    #[error("stage resolution snapshot failed for stage '{stage_id}': {details}")]
    SnapshotPersistFailed { stage_id: StageId, details: String },
    #[error("prompt replacement failed: {details}")]
    PromptReplacementFailed { details: String },
    #[error("conformance parse error in {file} line {line}: {details}")]
    ConformanceParseFailed {
        file: String,
        line: usize,
        details: String,
    },
    #[error("conformance discovery failed: {details}")]
    ConformanceDiscoveryFailed { details: String },
    #[error("conformance registry drift: {details}")]
    ConformanceRegistryDrift { details: String },
    #[error("conformance filter failed: unknown scenario ID '{scenario_id}'")]
    ConformanceFilterFailed { scenario_id: String },
    #[error("cutover guard violation in {file} line {line}: {pattern}")]
    ConformanceCutoverViolation {
        file: String,
        line: usize,
        pattern: String,
    },
    #[error("conformance scenario '{scenario_id}' failed: {details}")]
    ConformanceScenarioFailed {
        scenario_id: String,
        details: String,
    },
    #[error("backend diagnostics check failed: {failure_count} failure(s)")]
    BackendCheckFailed { failure_count: usize },
    #[error("malformed template override at '{path}': {reason}")]
    MalformedTemplate { path: String, reason: String },
    #[error("conformance run failed (see above for details)")]
    ConformanceRunFailed,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    TomlDeserialize(#[from] toml::de::Error),
    #[error(transparent)]
    TomlSerialize(#[from] toml::ser::Error),
    #[error(transparent)]
    TomlEdit(#[from] toml_edit::TomlError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

/// Errors specific to stage contract evaluation.
///
/// Each variant maps to a distinct [`FailureClass`] for retry/terminal policy.
/// The `stage_id` field is a domain-neutral string identifier, supporting both
/// workflow stage IDs and requirements stage IDs.
#[derive(Debug, Error)]
pub enum ContractError {
    #[error("schema validation failed for stage '{stage_id}': {details}")]
    SchemaValidation { stage_id: String, details: String },

    #[error("domain validation failed for stage '{stage_id}': {details}")]
    DomainValidation { stage_id: String, details: String },

    #[error("rendering failed for stage '{stage_id}': {details}")]
    RenderError { stage_id: String, details: String },

    #[error("non-passing QA/review outcome for stage '{stage_id}': {outcome}")]
    QaReviewOutcome { stage_id: String, outcome: String },
}

impl ContractError {
    pub fn failure_class(&self) -> FailureClass {
        match self {
            Self::SchemaValidation { .. } => FailureClass::SchemaValidationFailure,
            Self::DomainValidation { .. } | Self::RenderError { .. } => {
                FailureClass::DomainValidationFailure
            }
            Self::QaReviewOutcome { .. } => FailureClass::QaReviewOutcomeFailure,
        }
    }

    pub fn stage_id(&self) -> &str {
        match self {
            Self::SchemaValidation { stage_id, .. }
            | Self::DomainValidation { stage_id, .. }
            | Self::RenderError { stage_id, .. }
            | Self::QaReviewOutcome { stage_id, .. } => stage_id,
        }
    }
}

impl AppError {
    pub fn failure_class(&self) -> Option<FailureClass> {
        match self {
            Self::BackendUnavailable { .. } => Some(FailureClass::TransportFailure),
            Self::CapabilityMismatch { .. } => Some(FailureClass::DomainValidationFailure),
            Self::InvocationFailed { failure_class, .. } => Some(*failure_class),
            Self::InvocationTimeout { .. } => Some(FailureClass::Timeout),
            Self::InvocationCancelled { .. } => Some(FailureClass::Cancellation),
            _ => None,
        }
    }
}
