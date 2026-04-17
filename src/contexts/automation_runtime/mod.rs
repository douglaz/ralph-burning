use std::path::{Path, PathBuf};

use crate::shared::domain::ProjectId;
use crate::shared::error::{AppError, AppResult};

pub mod cli_writer_lease;
pub mod daemon_loop;
pub mod failure_reconciliation;
pub mod github_intake;
pub mod lease_service;
pub mod model;
pub mod planned_elsewhere;
pub mod pr_review;
pub mod pr_runtime;
pub mod repo_registry;
pub mod routing;
pub mod success_reconciliation;
pub mod task_service;
pub mod watcher;

pub const CONTEXT_NAME: &str = "automation_runtime";

pub use cli_writer_lease::CliWriterLeaseGuard;
pub use daemon_loop::{DaemonLoop, DaemonLoopConfig};
pub use failure_reconciliation::{
    reconcile_failure, FailureReconciliationError, FailureReconciliationOutcome,
    MAX_FAILURE_RETRIES,
};
pub use lease_service::{
    LeaseCleanupFailure, LeaseService, ReconcileReport, ReleaseMode, ReleaseResult,
};
pub use model::{
    CliWriterLease, DaemonJournalEvent, DaemonJournalEventType, DaemonTask, DispatchMode,
    GithubTaskMeta, LeaseRecord, RebaseFailureClassification, RebaseOutcome, ReviewWhitelist,
    RoutingResolution, RoutingSource, TaskStatus, WatchedIssueMeta, WorktreeLease,
};
pub use pr_review::{IngestedReviewBatch, PrReviewIngestionService};
pub use pr_runtime::{CompletionPrAction, PrRuntimeService};
pub use repo_registry::{DataDirLayout, RepoRegistration, RepoRegistryPort};
pub use routing::RoutingEngine;
pub use task_service::{CreateTaskInput, DaemonTaskService};
pub use watcher::IssueWatcherPort;

/// Distinguishes a worktree that was actively removed from one that was
/// already absent when cleanup was attempted. Callers use this to enforce
/// policy (e.g. reconcile treats `AlreadyAbsent` as a cleanup failure).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorktreeCleanupOutcome {
    /// The worktree directory existed and was successfully removed.
    Removed,
    /// The worktree directory was not present at cleanup time.
    AlreadyAbsent,
}

/// Outcome of removing a durable resource (lease file, writer lock).
/// Distinguishes positive removal from a no-op on already-absent state so
/// callers like reconcile can enforce explicit cleanup accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceCleanupOutcome {
    /// The resource file existed and was successfully deleted.
    Removed,
    /// The resource file was not present at deletion time.
    AlreadyAbsent,
}

/// Outcome of an owner-aware writer-lock release. Distinguishes positive
/// removal from absence and ownership mismatch so callers can enforce
/// strict cleanup accounting and avoid deleting a lock owned by another writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriterLockReleaseOutcome {
    /// The lock file contents matched the expected owner and was removed.
    Released,
    /// The lock file was not present at release time.
    AlreadyAbsent,
    /// The lock file exists but contains a different owner token.
    OwnerMismatch { actual_owner: String },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RebaseConflictFile {
    pub path: String,
    pub contents: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RebaseConflictRequest {
    pub branch_name: String,
    pub upstream: String,
    pub failure_details: String,
    pub conflicted_files: Vec<RebaseConflictFile>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RebaseResolutionFile {
    pub path: String,
    pub content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RebaseConflictResolution {
    pub summary: String,
    pub resolved_files: Vec<RebaseResolutionFile>,
}

pub trait RebaseConflictResolver {
    fn resolve_conflicts(
        &self,
        request: &RebaseConflictRequest,
    ) -> AppResult<RebaseConflictResolution>;
}

pub trait DaemonStorePort {
    fn list_tasks(&self, base_dir: &Path) -> AppResult<Vec<DaemonTask>>;
    fn read_task(&self, base_dir: &Path, task_id: &str) -> AppResult<DaemonTask>;
    fn create_task(&self, base_dir: &Path, task: &DaemonTask) -> AppResult<()>;
    fn write_task(&self, base_dir: &Path, task: &DaemonTask) -> AppResult<()>;

    fn list_leases(&self, base_dir: &Path) -> AppResult<Vec<WorktreeLease>>;
    fn read_lease(&self, base_dir: &Path, lease_id: &str) -> AppResult<WorktreeLease>;
    fn write_lease(&self, base_dir: &Path, lease: &WorktreeLease) -> AppResult<()>;
    fn list_lease_records(&self, base_dir: &Path) -> AppResult<Vec<LeaseRecord>>;
    fn read_lease_record(&self, base_dir: &Path, lease_id: &str) -> AppResult<LeaseRecord>;
    fn write_lease_record(&self, base_dir: &Path, lease: &LeaseRecord) -> AppResult<()>;
    fn remove_lease(&self, base_dir: &Path, lease_id: &str) -> AppResult<ResourceCleanupOutcome>;

    fn read_daemon_journal(&self, base_dir: &Path) -> AppResult<Vec<DaemonJournalEvent>>;
    fn append_daemon_journal_event(
        &self,
        base_dir: &Path,
        event: &DaemonJournalEvent,
    ) -> AppResult<()>;

    fn acquire_writer_lock(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        lease_id: &str,
    ) -> AppResult<()>;
    fn release_writer_lock(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        expected_owner: &str,
    ) -> AppResult<WriterLockReleaseOutcome>;
}

pub trait WorktreePort {
    fn worktree_path(&self, base_dir: &Path, task_id: &str) -> PathBuf;
    fn branch_name(&self, task_id: &str) -> String;
    fn create_worktree(
        &self,
        repo_root: &Path,
        worktree_path: &Path,
        branch_name: &str,
        task_id: &str,
    ) -> AppResult<()>;
    fn remove_worktree(
        &self,
        repo_root: &Path,
        worktree_path: &Path,
        task_id: &str,
    ) -> AppResult<WorktreeCleanupOutcome>;
    fn rebase_onto_default_branch(
        &self,
        repo_root: &Path,
        worktree_path: &Path,
        branch_name: &str,
    ) -> AppResult<()>;
    fn default_branch_name(&self, _repo_root: &Path) -> AppResult<String> {
        Ok("main".to_owned())
    }
    /// Push a branch to the remote with `--set-upstream`. Used for normal
    /// PR-branch publication. Does not force-push — will fail if the remote
    /// branch has diverged, which is the safe default for successful runs.
    fn push_branch(
        &self,
        _repo_root: &Path,
        _worktree_path: &Path,
        _branch_name: &str,
    ) -> AppResult<()> {
        Ok(())
    }
    /// Force-push a branch to the remote, using `--force-with-lease` to avoid
    /// clobbering concurrent changes. Used only for preserving checkpoint
    /// commits from failed runs — not for normal PR publication.
    fn force_push_branch(
        &self,
        _repo_root: &Path,
        _worktree_path: &Path,
        _branch_name: &str,
    ) -> AppResult<()> {
        Ok(())
    }
    /// Returns true if the worktree branch contains checkpoint commits from
    /// the implementation stage or later (excludes prompt_review, planning,
    /// docs_plan, ci_plan). Used to gate branch preservation on task failure.
    /// `repo_root` is used to resolve the default branch for scoping the log
    /// to branch-local commits only.
    fn has_checkpoint_commits(&self, _repo_root: &Path, _worktree_path: &Path) -> bool {
        false
    }
    /// Best-effort fetch of a remote branch and reset to the latest checkpoint
    /// commit. Returns true if the remote branch existed and the worktree was
    /// reset. After fetching, the implementation locates the newest checkpoint
    /// commit and resets to that SHA rather than the branch tip.
    fn try_resume_from_remote(
        &self,
        _repo_root: &Path,
        _worktree_path: &Path,
        _branch_name: &str,
    ) -> AppResult<bool> {
        Ok(false)
    }
    fn rebase_with_agent_resolution(
        &self,
        repo_root: &Path,
        worktree_path: &Path,
        branch_name: &str,
        _policy: &crate::shared::domain::EffectiveRebasePolicy,
        _resolver: Option<&dyn RebaseConflictResolver>,
    ) -> AppResult<RebaseOutcome> {
        match self.rebase_onto_default_branch(repo_root, worktree_path, branch_name) {
            Ok(()) => Ok(RebaseOutcome::Success),
            Err(AppError::RebaseConflict { details, .. }) => Ok(RebaseOutcome::Failed {
                classification: RebaseFailureClassification::Conflict,
                details,
            }),
            Err(error) => Err(error),
        }
    }
}
