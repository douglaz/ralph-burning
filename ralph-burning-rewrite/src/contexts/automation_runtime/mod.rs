use std::path::{Path, PathBuf};

use crate::shared::domain::ProjectId;
use crate::shared::error::AppResult;

pub mod daemon_loop;
pub mod lease_service;
pub mod model;
pub mod routing;
pub mod task_service;
pub mod watcher;

pub const CONTEXT_NAME: &str = "automation_runtime";

pub use daemon_loop::{DaemonLoop, DaemonLoopConfig};
pub use lease_service::{
    LeaseCleanupFailure, LeaseService, ReconcileReport, ReleaseMode, ReleaseResult,
};
pub use model::{
    CliWriterLease, DaemonJournalEvent, DaemonJournalEventType, DaemonTask, DispatchMode,
    LeaseRecord, RoutingResolution, RoutingSource, TaskStatus, WatchedIssueMeta, WorktreeLease,
};
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
    ) -> AppResult<ResourceCleanupOutcome>;
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
}
