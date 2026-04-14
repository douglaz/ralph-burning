//! CLI writer-lease guard: wraps the project writer lock with a durable
//! `CliWriterLease` record and a periodic heartbeat so that `daemon reconcile`
//! can discover and clean stale CLI-held locks after a crash.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::adapters::fs::{FileSystem, PidRecordLiveState, RunPidOwner, RunPidRecord};
use crate::contexts::automation_runtime::lease_service::{
    try_preserve_failed_branch, LeaseService, ReleaseMode,
};
use crate::contexts::automation_runtime::model::{
    CliWriterCleanupHandoff, CliWriterLease, LeaseRecord,
};
use crate::contexts::automation_runtime::task_service::DaemonTaskService;
use crate::contexts::automation_runtime::{
    DaemonStorePort, ResourceCleanupOutcome, WorktreeCleanupOutcome, WriterLockReleaseOutcome,
};
use crate::shared::domain::ProjectId;
use crate::shared::error::{AppError, AppResult};

/// Default TTL for CLI writer leases (seconds).
pub const CLI_LEASE_TTL_SECONDS: u64 = 300;

/// Default heartbeat cadence (seconds).
pub const CLI_LEASE_HEARTBEAT_CADENCE_SECONDS: u64 = 30;

/// Serializes in-process CLI lease record mutations so the signal-cleanup
/// handoff cannot be lost to a concurrent heartbeat read-modify-write cycle.
static CLI_LEASE_RECORD_UPDATE_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// RAII guard that owns a project writer lock, a matching CLI lease record,
/// and a background heartbeat task. On drop, the heartbeat is stopped
/// deterministically before cleanup begins.
pub struct CliWriterLeaseGuard {
    store: Arc<dyn DaemonStorePort + Send + Sync>,
    base_dir: PathBuf,
    project_id: ProjectId,
    lease_id: String,
    shutdown: Arc<Notify>,
    /// Set to `true` by drop before cleanup; the heartbeat task checks this
    /// before each tick so no file I/O can race with lease deletion.
    closed: Arc<AtomicBool>,
    /// Held by the heartbeat task during each tick; drop acquires this to wait
    /// for any in-flight tick to finish before cleaning up resources.
    tick_lock: Arc<std::sync::Mutex<()>>,
    heartbeat_handle: Option<JoinHandle<()>>,
}

fn writer_lock_path(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
    crate::adapters::fs::FileSystem::daemon_root(base_dir)
        .join("leases")
        .join(format!("writer-{}.lock", project_id.as_str()))
}

pub(crate) fn read_project_writer_lock_owner(
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<Option<String>> {
    match fs::read_to_string(writer_lock_path(base_dir, project_id)) {
        Ok(owner) => Ok(Some(owner)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn read_lease_record_if_present(
    store: &dyn DaemonStorePort,
    base_dir: &Path,
    lease_id: &str,
) -> AppResult<Option<LeaseRecord>> {
    match store.read_lease_record(base_dir, lease_id) {
        Ok(record) => Ok(Some(record)),
        Err(AppError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(AppError::CorruptRecord { details, .. }) if details == "canonical file is missing" => {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn strict_lease_cleanup_failure(task_id: &str) -> AppError {
    AppError::LeaseCleanupPartialFailure {
        task_id: task_id.to_owned(),
    }
}

fn task_referencing_project_lease(
    store: &dyn DaemonStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    expected_owner: &str,
) -> AppResult<Option<crate::contexts::automation_runtime::model::DaemonTask>> {
    Ok(store.list_tasks(base_dir)?.into_iter().find(|task| {
        task.project_id == project_id.as_str() && task.lease_id.as_deref() == Some(expected_owner)
    }))
}

fn require_removed_lease_record(outcome: ResourceCleanupOutcome, task_id: &str) -> AppResult<()> {
    match outcome {
        ResourceCleanupOutcome::Removed => Ok(()),
        ResourceCleanupOutcome::AlreadyAbsent => Err(strict_lease_cleanup_failure(task_id)),
    }
}

fn require_removed_worktree(outcome: WorktreeCleanupOutcome, task_id: &str) -> AppResult<()> {
    match outcome {
        WorktreeCleanupOutcome::Removed => Ok(()),
        WorktreeCleanupOutcome::AlreadyAbsent => Err(strict_lease_cleanup_failure(task_id)),
    }
}

pub(crate) fn remove_owned_run_pid_file(
    base_dir: &Path,
    repo_root: &Path,
    project_id: &str,
    expected_writer_owner: Option<&str>,
    task_id: &str,
) -> AppResult<()> {
    let cleanup_failure = || strict_lease_cleanup_failure(task_id);
    let project_id = ProjectId::new(project_id.to_owned()).map_err(|_| cleanup_failure())?;

    for _ in 0..2 {
        // Remove the matching PID record while this writer owner still holds
        // the project lock; releasing first lets a successor rewrite
        // `run.pid` in the compare/unlink window.
        let Some(pid_record) =
            FileSystem::read_pid_file(repo_root, &project_id).map_err(|_| cleanup_failure())?
        else {
            return Ok(());
        };
        if run_pid_record_belongs_to_successor_owner(
            base_dir,
            &project_id,
            &pid_record,
            expected_writer_owner,
        )
        .map_err(|_| cleanup_failure())?
        {
            return Ok(());
        }
        if !run_pid_record_is_reclaimable(&pid_record, expected_writer_owner) {
            return Err(cleanup_failure());
        }
        if FileSystem::remove_pid_file_if_matches(repo_root, &project_id, &pid_record)
            .map_err(|_| cleanup_failure())?
        {
            return Ok(());
        }
    }

    match FileSystem::read_pid_file(repo_root, &project_id).map_err(|_| cleanup_failure())? {
        None => Ok(()),
        Some(pid_record)
            if run_pid_record_belongs_to_successor_owner(
                base_dir,
                &project_id,
                &pid_record,
                expected_writer_owner,
            )
            .map_err(|_| cleanup_failure())? =>
        {
            Ok(())
        }
        Some(_) => Err(cleanup_failure()),
    }
}

fn run_pid_record_is_reclaimable(
    pid_record: &RunPidRecord,
    expected_writer_owner: Option<&str>,
) -> bool {
    let live_state = FileSystem::pid_record_live_state(pid_record);
    pid_record.owner == RunPidOwner::Daemon
        && (matches!(live_state, PidRecordLiveState::Stale)
            || (pid_record.pid == std::process::id()
                && pid_record.writer_owner.as_deref() == expected_writer_owner
                && expected_writer_owner.is_some()))
}

fn run_pid_record_belongs_to_successor_owner(
    base_dir: &Path,
    project_id: &ProjectId,
    pid_record: &RunPidRecord,
    expected_writer_owner: Option<&str>,
) -> AppResult<bool> {
    let Some(expected_writer_owner) = expected_writer_owner else {
        return Ok(false);
    };
    let Some(pid_writer_owner) = pid_record.writer_owner.as_deref() else {
        return Ok(false);
    };
    if pid_writer_owner == expected_writer_owner {
        return Ok(false);
    }

    Ok(read_project_writer_lock_owner(base_dir, project_id)?.as_deref() == Some(pid_writer_owner))
}

pub fn read_project_writer_lease_record(
    store: &dyn DaemonStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<Option<(String, LeaseRecord)>> {
    let Some(owner) = read_project_writer_lock_owner(base_dir, project_id)? else {
        return Ok(None);
    };
    Ok(read_lease_record_if_present(store, base_dir, &owner)?.map(|record| (owner, record)))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DetachedProjectWriterOwner {
    None,
    Single(String),
    Ambiguous(Vec<String>),
}

pub fn reclaim_specific_cli_writer_lease(
    store: &dyn DaemonStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    expected_owner: &str,
) -> AppResult<bool> {
    let Some(current_owner) = read_project_writer_lock_owner(base_dir, project_id)? else {
        return Ok(false);
    };
    if current_owner != expected_owner {
        return Ok(false);
    }

    let Some(record) = read_lease_record_if_present(store, base_dir, expected_owner)? else {
        return Ok(false);
    };
    let LeaseRecord::CliWriter(lease) = record else {
        return Ok(false);
    };
    if lease.project_id != project_id.as_str() {
        return Ok(false);
    }

    match store.release_writer_lock(base_dir, project_id, expected_owner)? {
        WriterLockReleaseOutcome::Released | WriterLockReleaseOutcome::AlreadyAbsent => {}
        WriterLockReleaseOutcome::OwnerMismatch { .. } => return Ok(false),
    }
    require_removed_lease_record(
        store.remove_lease(base_dir, expected_owner)?,
        expected_owner,
    )?;
    Ok(true)
}

pub fn persist_cli_writer_cleanup_handoff(
    store: &dyn DaemonStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    expected_owner: &str,
    cleanup_handoff: CliWriterCleanupHandoff,
) -> AppResult<bool> {
    let _record_update_guard = CLI_LEASE_RECORD_UPDATE_MUTEX
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let Some(current_owner) = read_project_writer_lock_owner(base_dir, project_id)? else {
        return Ok(false);
    };
    if current_owner != expected_owner {
        return Ok(false);
    }

    let Some(record) = read_lease_record_if_present(store, base_dir, expected_owner)? else {
        return Ok(false);
    };
    let LeaseRecord::CliWriter(mut lease) = record else {
        return Ok(false);
    };
    if lease.project_id != project_id.as_str() {
        return Ok(false);
    }

    lease.cleanup_handoff = Some(cleanup_handoff);
    store.write_lease_record(base_dir, &LeaseRecord::CliWriter(lease))?;
    Ok(true)
}

/// Release the exact observed writer-lock owner for a project when recovery has
/// already proven the owner stale. This is used by `run resume` / `run stop`
/// so they never tear down a replacement owner that won the lock later.
pub fn reclaim_specific_project_writer_owner(
    store: &dyn DaemonStorePort,
    worktree: &dyn crate::contexts::automation_runtime::WorktreePort,
    base_dir: &Path,
    repo_root: &Path,
    project_id: &ProjectId,
    expected_owner: &str,
) -> AppResult<bool> {
    let Some(current_owner) = read_project_writer_lock_owner(base_dir, project_id)? else {
        return Ok(false);
    };
    if current_owner != expected_owner {
        return Ok(false);
    }

    let Some(record) = read_lease_record_if_present(store, base_dir, expected_owner)? else {
        match store.release_writer_lock(base_dir, project_id, expected_owner)? {
            WriterLockReleaseOutcome::Released | WriterLockReleaseOutcome::AlreadyAbsent => {
                if let Some(task) =
                    task_referencing_project_lease(store, base_dir, project_id, expected_owner)?
                {
                    return Err(AppError::LeaseCleanupPartialFailure {
                        task_id: task.task_id,
                    });
                }
                return Ok(true);
            }
            WriterLockReleaseOutcome::OwnerMismatch { .. } => return Ok(false),
        }
    };
    let owns_project = match &record {
        LeaseRecord::CliWriter(lease) => lease.project_id == project_id.as_str(),
        LeaseRecord::Worktree(lease) => lease.project_id == project_id.as_str(),
    };
    if !owns_project {
        return Ok(false);
    }

    match record {
        LeaseRecord::CliWriter(_) => {
            match store.release_writer_lock(base_dir, project_id, expected_owner)? {
                WriterLockReleaseOutcome::Released | WriterLockReleaseOutcome::AlreadyAbsent => {}
                WriterLockReleaseOutcome::OwnerMismatch { .. } => return Ok(false),
            }
            require_removed_lease_record(
                store.remove_lease(base_dir, expected_owner)?,
                expected_owner,
            )?;
            Ok(true)
        }
        LeaseRecord::Worktree(lease) => {
            if let Ok(task) = store.read_task(base_dir, &lease.task_id) {
                if matches!(
                    task.status,
                    crate::contexts::automation_runtime::model::TaskStatus::Claimed
                        | crate::contexts::automation_runtime::model::TaskStatus::Active
                ) {
                    let _ = DaemonTaskService::mark_failed(
                        store,
                        base_dir,
                        &task.task_id,
                        "stale_writer_owner_reclaimed",
                        "CLI stale-run recovery reclaimed the daemon worktree lease",
                    )?;
                }
            }

            if let Ok(task) = store.read_task(base_dir, &lease.task_id) {
                if task.status == crate::contexts::automation_runtime::model::TaskStatus::Failed {
                    try_preserve_failed_branch(worktree, repo_root, &lease);
                }
            }

            remove_owned_run_pid_file(
                base_dir,
                repo_root,
                &lease.project_id,
                Some(lease.lease_id.as_str()),
                &lease.task_id,
            )?;

            let release_result = LeaseService::release(
                store,
                worktree,
                base_dir,
                repo_root,
                &lease,
                ReleaseMode::Idempotent,
            )?;
            if release_result.writer_lock_owner_mismatch {
                return Ok(false);
            }
            if release_result.has_cleanup_failures() {
                return Err(AppError::LeaseCleanupPartialFailure {
                    task_id: lease.task_id.clone(),
                });
            }

            if store
                .read_task(base_dir, &lease.task_id)
                .ok()
                .and_then(|task| task.lease_id)
                .as_deref()
                == Some(lease.lease_id.as_str())
            {
                let _ = DaemonTaskService::clear_lease_reference(store, base_dir, &lease.task_id)?;
            }
            Ok(true)
        }
    }
}

pub fn find_detached_project_writer_owner(
    store: &dyn DaemonStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<DetachedProjectWriterOwner> {
    let project_key = project_id.as_str();
    let now = Utc::now();
    let lease_records = store.list_lease_records(base_dir)?;
    let known_lease_ids = lease_records
        .iter()
        .map(|record| record.lease_id().to_owned())
        .collect::<BTreeSet<_>>();
    let mut matching_owners = lease_records
        .into_iter()
        .filter_map(|record| match record {
            LeaseRecord::CliWriter(lease)
                if lease.project_id == project_key && lease.is_stale_at(now) =>
            {
                Some(lease.lease_id)
            }
            LeaseRecord::Worktree(lease) if lease.project_id == project_key => {
                let task_failed_or_aborted = store
                    .read_task(base_dir, &lease.task_id)
                    .ok()
                    .filter(|task| task.project_id == project_key)
                    .filter(|task| task.lease_id.as_deref() == Some(lease.lease_id.as_str()))
                    .is_some_and(|task| {
                        matches!(
                            task.status,
                            crate::contexts::automation_runtime::model::TaskStatus::Failed
                                | crate::contexts::automation_runtime::model::TaskStatus::Aborted
                        )
                    });
                if task_failed_or_aborted || lease.is_stale_at(now) {
                    Some(lease.lease_id)
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    matching_owners.extend(
        store
            .list_tasks(base_dir)?
            .into_iter()
            .filter(|task| task.project_id == project_key)
            .filter_map(|task| task.lease_id)
            .filter(|lease_id| !known_lease_ids.contains(lease_id)),
    );
    matching_owners.sort();
    matching_owners.dedup();
    Ok(match matching_owners.len() {
        0 => DetachedProjectWriterOwner::None,
        1 => DetachedProjectWriterOwner::Single(
            matching_owners
                .pop()
                .expect("single detached project writer owner must exist"),
        ),
        _ => DetachedProjectWriterOwner::Ambiguous(matching_owners),
    })
}

pub fn cleanup_detached_project_writer_owner(
    store: &dyn DaemonStorePort,
    worktree: &dyn crate::contexts::automation_runtime::WorktreePort,
    base_dir: &Path,
    repo_root: &Path,
    project_id: &ProjectId,
    expected_owner: &str,
) -> AppResult<bool> {
    if read_project_writer_lock_owner(base_dir, project_id)?
        .as_deref()
        .is_some_and(|owner| owner == expected_owner)
    {
        return Ok(false);
    }

    let detached_task =
        task_referencing_project_lease(store, base_dir, project_id, expected_owner)?;

    let Some(record) = read_lease_record_if_present(store, base_dir, expected_owner)? else {
        return match detached_task {
            Some(task) => Err(AppError::LeaseCleanupPartialFailure {
                task_id: task.task_id,
            }),
            None => Ok(false),
        };
    };

    let owns_project = match &record {
        LeaseRecord::CliWriter(lease) => lease.project_id == project_id.as_str(),
        LeaseRecord::Worktree(lease) => lease.project_id == project_id.as_str(),
    };
    if !owns_project {
        return Ok(false);
    }

    match record {
        LeaseRecord::CliWriter(_) => {
            require_removed_lease_record(
                store.remove_lease(base_dir, expected_owner)?,
                expected_owner,
            )?;
            Ok(true)
        }
        LeaseRecord::Worktree(lease) => {
            if let Ok(task) = store.read_task(base_dir, &lease.task_id) {
                if matches!(
                    task.status,
                    crate::contexts::automation_runtime::model::TaskStatus::Claimed
                        | crate::contexts::automation_runtime::model::TaskStatus::Active
                ) {
                    let _ = DaemonTaskService::mark_failed(
                        store,
                        base_dir,
                        &task.task_id,
                        "stale_writer_owner_reclaimed",
                        "CLI stale-run recovery reclaimed the detached daemon worktree lease",
                    )?;
                }
            }

            if let Ok(task) = store.read_task(base_dir, &lease.task_id) {
                if task.status == crate::contexts::automation_runtime::model::TaskStatus::Failed {
                    try_preserve_failed_branch(worktree, repo_root, &lease);
                }
            }

            remove_owned_run_pid_file(
                base_dir,
                repo_root,
                &lease.project_id,
                Some(lease.lease_id.as_str()),
                &lease.task_id,
            )?;

            let worktree_outcome = worktree
                .remove_worktree(repo_root, &lease.worktree_path, &lease.task_id)
                .map_err(|_| strict_lease_cleanup_failure(&lease.task_id))?;
            require_removed_worktree(worktree_outcome, &lease.task_id)?;
            let lease_outcome = store
                .remove_lease(base_dir, &lease.lease_id)
                .map_err(|_| strict_lease_cleanup_failure(&lease.task_id))?;
            require_removed_lease_record(lease_outcome, &lease.task_id)?;

            if store
                .read_task(base_dir, &lease.task_id)
                .ok()
                .and_then(|task| task.lease_id)
                .as_deref()
                == Some(lease.lease_id.as_str())
            {
                DaemonTaskService::clear_lease_reference(store, base_dir, &lease.task_id).map_err(
                    |_| AppError::LeaseCleanupPartialFailure {
                        task_id: lease.task_id.clone(),
                    },
                )?;
            }
            Ok(true)
        }
    }
}

/// Best-effort stale CLI lease cleanup for a project after the owning CLI
/// process has died. Releases the stranded writer lock using the observed lock
/// owner token, then prunes any remaining CLI lease records for the project.
pub fn reclaim_stale_project_writer_lease(
    store: &dyn DaemonStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<()> {
    let cli_lease_ids = store
        .list_lease_records(base_dir)?
        .into_iter()
        .filter_map(|record| match record {
            LeaseRecord::CliWriter(lease) if lease.project_id == project_id.as_str() => {
                Some(lease.lease_id)
            }
            _ => None,
        })
        .collect::<Vec<_>>();

    if let Some(owner) = read_project_writer_lock_owner(base_dir, project_id)? {
        if cli_lease_ids.iter().any(|lease_id| lease_id == &owner)
            && !reclaim_specific_cli_writer_lease(store, base_dir, project_id, &owner)?
        {
            return Ok(());
        }
    }

    for lease_id in cli_lease_ids {
        let _ = store.remove_lease(base_dir, &lease_id)?;
    }

    Ok(())
}

impl CliWriterLeaseGuard {
    /// Returns the lease_id assigned to this guard.
    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }

    /// Explicit fallible shutdown for normal completion paths.
    ///
    /// Stops the heartbeat, performs owner-aware writer-lock release, and
    /// deletes the CLI lease record only after the lock release positively
    /// succeeds. Returns an error describing the failed sub-step so the
    /// caller can exit non-zero without rolling back committed artifacts.
    ///
    /// After a successful `close()`, the subsequent `Drop` is a no-op.
    pub fn close(mut self) -> AppResult<()> {
        self.close_inner()
    }

    /// Shared shutdown logic used by both `close()` and `Drop`.
    /// Returns `Ok(())` only when all sub-steps succeed.
    fn close_inner(&mut self) -> AppResult<()> {
        // Already closed (idempotent).
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }

        // 1. Signal heartbeat shutdown.
        self.shutdown.notify_one();
        // 2. Mark closed so the heartbeat task will not start a new tick.
        self.closed.store(true, Ordering::Release);
        // 3. Wait for any in-flight heartbeat tick to finish.
        let _tick_guard = self.tick_lock.lock().unwrap_or_else(|e| e.into_inner());
        // 4. Abort the heartbeat task.
        if let Some(handle) = self.heartbeat_handle.take() {
            handle.abort();
        }

        // 5. Owner-aware writer-lock release.
        let lock_outcome =
            self.store
                .release_writer_lock(&self.base_dir, &self.project_id, &self.lease_id);

        match lock_outcome {
            Ok(WriterLockReleaseOutcome::Released) => {
                // 6. Lock released — delete the CLI lease record.
                if let Err(e) = self.store.remove_lease(&self.base_dir, &self.lease_id) {
                    // Lease file delete failed after successful lock release.
                    // Lock stays released; lease record stays durable.
                    return Err(AppError::GuardCloseFailed {
                        step: "lease_file_delete".to_owned(),
                        details: e.to_string(),
                    });
                }
                Ok(())
            }
            Ok(WriterLockReleaseOutcome::AlreadyAbsent) => {
                // Writer lock absent — keep lease record durable.
                Err(AppError::GuardCloseFailed {
                    step: "writer_lock_absent".to_owned(),
                    details: "writer lock file was already absent at close time".to_owned(),
                })
            }
            Ok(WriterLockReleaseOutcome::OwnerMismatch { actual_owner }) => {
                // Lock owned by someone else — keep lease record durable.
                Err(AppError::GuardCloseFailed {
                    step: "writer_lock_owner_mismatch".to_owned(),
                    details: format!("lock owned by '{actual_owner}', not this guard"),
                })
            }
            Err(e) => {
                // I/O error — keep lease record durable.
                Err(AppError::GuardCloseFailed {
                    step: "writer_lock_io_error".to_owned(),
                    details: e.to_string(),
                })
            }
        }
    }

    /// Persist a `CliWriterLease` record, acquire the project writer lock, and
    /// spawn a heartbeat task.
    ///
    /// **Crash-safety invariant:** the durable CLI lease record is written
    /// _before_ the writer lock is acquired, so a crash after persistence
    /// but before `acquire()` returns can never strand a writer lock without
    /// a matching reconcile-visible lease record.
    ///
    /// On contention (writer lock already held), the prewritten lease is
    /// deleted.  If cleanup succeeds the original `ProjectWriterLockHeld`
    /// error is returned with no leftover lease.  If cleanup fails
    /// (`AlreadyAbsent` or I/O error), `AcquisitionRollbackFailed` is
    /// returned preserving both failure causes.
    pub fn acquire(
        store: Arc<dyn DaemonStorePort + Send + Sync>,
        base_dir: &Path,
        project_id: ProjectId,
        ttl_seconds: u64,
        heartbeat_cadence_seconds: u64,
    ) -> AppResult<Self> {
        let lease_id = format!("cli-{}", uuid::Uuid::new_v4());

        // Step 1: persist the CLI lease record BEFORE acquiring the writer
        // lock. This guarantees a reconcile-visible record exists whenever a
        // CLI-held writer lock can be stranded.
        let now = Utc::now();
        let lease = CliWriterLease {
            lease_id: lease_id.clone(),
            project_id: project_id.to_string(),
            owner: "cli".to_owned(),
            acquired_at: now,
            ttl_seconds,
            last_heartbeat: now,
            cleanup_handoff: None,
        };
        store.write_lease_record(base_dir, &LeaseRecord::CliWriter(lease))?;

        // Step 2: acquire the writer lock with our lease_id as content.
        if let Err(e) = store.acquire_writer_lock(base_dir, &project_id, &lease_id) {
            // Contention: roll back the prewritten lease record.
            match store.remove_lease(base_dir, &lease_id) {
                Ok(ResourceCleanupOutcome::Removed) => {
                    // Lease cleaned up — return the original contention error.
                    return Err(e);
                }
                Ok(ResourceCleanupOutcome::AlreadyAbsent) => {
                    // The prewritten lease vanished unexpectedly — treat as a
                    // combined acquisition/cleanup failure so the caller knows
                    // the rollback state is uncertain.
                    return Err(AppError::AcquisitionRollbackFailed {
                        trigger: e.to_string(),
                        rollback_details:
                            "prewritten CLI lease was already absent at rollback time".to_owned(),
                    });
                }
                Err(cleanup_err) => {
                    return Err(AppError::AcquisitionRollbackFailed {
                        trigger: e.to_string(),
                        rollback_details: format!(
                            "prewritten CLI lease cleanup failed: {cleanup_err}"
                        ),
                    });
                }
            }
        }

        // Step 3: spawn heartbeat task.
        let shutdown = Arc::new(Notify::new());
        let closed = Arc::new(AtomicBool::new(false));
        let tick_lock = Arc::new(std::sync::Mutex::new(()));
        let heartbeat_handle = {
            let store = Arc::clone(&store);
            let base_dir = base_dir.to_path_buf();
            let lease_id = lease_id.clone();
            let shutdown = Arc::clone(&shutdown);
            let closed = Arc::clone(&closed);
            let tick_lock = Arc::clone(&tick_lock);
            tokio::spawn(async move {
                let interval = tokio::time::Duration::from_secs(heartbeat_cadence_seconds);
                loop {
                    tokio::select! {
                        _ = shutdown.notified() => break,
                        _ = tokio::time::sleep(interval) => {}
                    }
                    // Check the closed flag before acquiring the tick lock so
                    // no file I/O can race with drop cleanup.
                    if closed.load(Ordering::Acquire) {
                        break;
                    }
                    let _tick_guard = tick_lock.lock().unwrap_or_else(|e| e.into_inner());
                    // Re-check after acquiring the lock — drop may have set
                    // the flag while we were waiting.
                    if closed.load(Ordering::Acquire) {
                        break;
                    }
                    // Best-effort heartbeat: failure leaves lease intact for
                    // later recovery rather than partially cleaning resources.
                    let _ = heartbeat_tick(&*store, &base_dir, &lease_id);
                }
            })
        };

        Ok(Self {
            store,
            base_dir: base_dir.to_path_buf(),
            project_id,
            lease_id,
            shutdown,
            closed,
            tick_lock,
            heartbeat_handle: Some(heartbeat_handle),
        })
    }
}

/// Update `last_heartbeat` on an existing CLI lease record.
fn heartbeat_tick(store: &dyn DaemonStorePort, base_dir: &Path, lease_id: &str) -> AppResult<()> {
    let _record_update_guard = CLI_LEASE_RECORD_UPDATE_MUTEX
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let record = store.read_lease_record(base_dir, lease_id)?;
    match record {
        LeaseRecord::CliWriter(mut cli) => {
            cli.last_heartbeat = Utc::now();
            store.write_lease_record(base_dir, &LeaseRecord::CliWriter(cli))
        }
        LeaseRecord::Worktree(_) => Ok(()), // unexpected, ignore
    }
}

impl Drop for CliWriterLeaseGuard {
    fn drop(&mut self) {
        // Best-effort cleanup for unwind/error paths.
        // After a successful explicit close(), this is a no-op.
        let _ = self.close_inner();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Duration;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use tempfile::tempdir;

    use crate::adapters::fs::{
        FileSystem, FsDaemonStore, PidRecordLiveState, RunPidOwner, RunPidRecord,
    };
    use crate::contexts::automation_runtime::model::{
        DaemonJournalEvent, DaemonTask, DispatchMode, RoutingSource, TaskStatus, WorktreeLease,
    };
    use crate::contexts::automation_runtime::{WorktreeCleanupOutcome, WorktreePort};
    use crate::shared::domain::FlowPreset;

    fn store() -> Arc<dyn DaemonStorePort + Send + Sync> {
        Arc::new(FsDaemonStore)
    }

    enum StubWorktreeCleanup {
        Removed,
        AlreadyAbsent,
    }

    struct StubWorktreePort {
        cleanup: StubWorktreeCleanup,
    }

    impl WorktreePort for StubWorktreePort {
        fn worktree_path(&self, base_dir: &Path, task_id: &str) -> PathBuf {
            base_dir.join("worktrees").join(task_id)
        }

        fn branch_name(&self, task_id: &str) -> String {
            format!("stub-{task_id}")
        }

        fn create_worktree(
            &self,
            _repo_root: &Path,
            _worktree_path: &Path,
            _branch_name: &str,
            _task_id: &str,
        ) -> AppResult<()> {
            Ok(())
        }

        fn remove_worktree(
            &self,
            _repo_root: &Path,
            _worktree_path: &Path,
            _task_id: &str,
        ) -> AppResult<WorktreeCleanupOutcome> {
            Ok(match self.cleanup {
                StubWorktreeCleanup::Removed => WorktreeCleanupOutcome::Removed,
                StubWorktreeCleanup::AlreadyAbsent => WorktreeCleanupOutcome::AlreadyAbsent,
            })
        }

        fn rebase_onto_default_branch(
            &self,
            _repo_root: &Path,
            _worktree_path: &Path,
            _branch_name: &str,
        ) -> AppResult<()> {
            Ok(())
        }
    }

    struct FailOnceLeaseReferenceClearStore {
        task_id: String,
        fail_next_clear_lease_write: AtomicBool,
    }

    impl FailOnceLeaseReferenceClearStore {
        fn new(task_id: &str) -> Self {
            Self {
                task_id: task_id.to_owned(),
                fail_next_clear_lease_write: AtomicBool::new(true),
            }
        }
    }

    impl DaemonStorePort for FailOnceLeaseReferenceClearStore {
        fn list_tasks(&self, base_dir: &Path) -> AppResult<Vec<DaemonTask>> {
            FsDaemonStore.list_tasks(base_dir)
        }

        fn read_task(&self, base_dir: &Path, task_id: &str) -> AppResult<DaemonTask> {
            FsDaemonStore.read_task(base_dir, task_id)
        }

        fn create_task(&self, base_dir: &Path, task: &DaemonTask) -> AppResult<()> {
            FsDaemonStore.create_task(base_dir, task)
        }

        fn write_task(&self, base_dir: &Path, task: &DaemonTask) -> AppResult<()> {
            if task.task_id == self.task_id
                && task.lease_id.is_none()
                && self
                    .fail_next_clear_lease_write
                    .swap(false, Ordering::SeqCst)
            {
                return Err(AppError::Io(std::io::Error::other(
                    "simulated clear_lease_reference write failure",
                )));
            }

            FsDaemonStore.write_task(base_dir, task)
        }

        fn list_leases(&self, base_dir: &Path) -> AppResult<Vec<WorktreeLease>> {
            FsDaemonStore.list_leases(base_dir)
        }

        fn read_lease(&self, base_dir: &Path, lease_id: &str) -> AppResult<WorktreeLease> {
            FsDaemonStore.read_lease(base_dir, lease_id)
        }

        fn write_lease(&self, base_dir: &Path, lease: &WorktreeLease) -> AppResult<()> {
            FsDaemonStore.write_lease(base_dir, lease)
        }

        fn list_lease_records(&self, base_dir: &Path) -> AppResult<Vec<LeaseRecord>> {
            FsDaemonStore.list_lease_records(base_dir)
        }

        fn read_lease_record(&self, base_dir: &Path, lease_id: &str) -> AppResult<LeaseRecord> {
            FsDaemonStore.read_lease_record(base_dir, lease_id)
        }

        fn write_lease_record(&self, base_dir: &Path, lease: &LeaseRecord) -> AppResult<()> {
            FsDaemonStore.write_lease_record(base_dir, lease)
        }

        fn remove_lease(
            &self,
            base_dir: &Path,
            lease_id: &str,
        ) -> AppResult<ResourceCleanupOutcome> {
            FsDaemonStore.remove_lease(base_dir, lease_id)
        }

        fn read_daemon_journal(&self, base_dir: &Path) -> AppResult<Vec<DaemonJournalEvent>> {
            FsDaemonStore.read_daemon_journal(base_dir)
        }

        fn append_daemon_journal_event(
            &self,
            base_dir: &Path,
            event: &DaemonJournalEvent,
        ) -> AppResult<()> {
            FsDaemonStore.append_daemon_journal_event(base_dir, event)
        }

        fn acquire_writer_lock(
            &self,
            base_dir: &Path,
            project_id: &ProjectId,
            lease_id: &str,
        ) -> AppResult<()> {
            FsDaemonStore.acquire_writer_lock(base_dir, project_id, lease_id)
        }

        fn release_writer_lock(
            &self,
            base_dir: &Path,
            project_id: &ProjectId,
            expected_owner: &str,
        ) -> AppResult<WriterLockReleaseOutcome> {
            FsDaemonStore.release_writer_lock(base_dir, project_id, expected_owner)
        }
    }

    #[tokio::test]
    async fn acquire_creates_lease_record_and_writer_lock() {
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("guard-test".to_owned()).expect("valid id");

        let guard = CliWriterLeaseGuard::acquire(
            store(),
            temp.path(),
            project_id.clone(),
            CLI_LEASE_TTL_SECONDS,
            CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
        )
        .expect("acquire");

        // Lease record should be visible
        let records = FsDaemonStore.list_lease_records(temp.path()).expect("list");
        assert_eq!(1, records.len());
        assert!(
            matches!(&records[0], LeaseRecord::CliWriter(cli) if cli.lease_id == guard.lease_id)
        );

        // Writer lock should be held
        let err = FsDaemonStore
            .acquire_writer_lock(temp.path(), &project_id, "other")
            .expect_err("lock should be held");
        assert!(matches!(
            err,
            crate::shared::error::AppError::ProjectWriterLockHeld { .. }
        ));

        drop(guard);

        // After drop, both should be cleaned up
        let records_after = FsDaemonStore
            .list_lease_records(temp.path())
            .expect("list after");
        assert!(records_after.is_empty(), "lease record should be removed");
        FsDaemonStore
            .acquire_writer_lock(temp.path(), &project_id, "after-drop")
            .expect("lock should be available after drop");
        FsDaemonStore
            .release_writer_lock(temp.path(), &project_id, "after-drop")
            .expect("cleanup");
    }

    #[tokio::test]
    async fn heartbeat_advances_last_heartbeat() {
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("hb-test".to_owned()).expect("valid id");

        let guard = CliWriterLeaseGuard::acquire(
            store(),
            temp.path(),
            project_id.clone(),
            CLI_LEASE_TTL_SECONDS,
            1, // 1-second heartbeat cadence for testing
        )
        .expect("acquire");

        let record_before = FsDaemonStore
            .read_lease_record(temp.path(), &guard.lease_id)
            .expect("read before");
        let hb_before = match &record_before {
            LeaseRecord::CliWriter(cli) => cli.last_heartbeat,
            _ => panic!("expected CliWriter"),
        };

        // Wait for at least one heartbeat tick
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

        let record_after = FsDaemonStore
            .read_lease_record(temp.path(), &guard.lease_id)
            .expect("read after");
        let hb_after = match &record_after {
            LeaseRecord::CliWriter(cli) => cli.last_heartbeat,
            _ => panic!("expected CliWriter"),
        };

        assert!(
            hb_after > hb_before,
            "heartbeat should advance last_heartbeat"
        );

        drop(guard);
    }

    #[test]
    fn heartbeat_tick_preserves_cleanup_handoff() {
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("hb-handoff-test".to_owned()).expect("valid id");
        let acquired_at = Utc::now() - Duration::seconds(5);
        let initial_heartbeat = Utc::now() - Duration::seconds(2);
        let cleanup_handoff = CliWriterCleanupHandoff {
            pid: 4242,
            recorded_at: Some(Utc::now()),
            run_id: Some("run-handoff".to_owned()),
            run_started_at: Some(Utc::now() - Duration::minutes(1)),
            proc_start_ticks: Some(99),
            proc_start_marker: Some("marker-99".to_owned()),
        };
        let lease = CliWriterLease {
            lease_id: "cli-heartbeat-handoff".to_owned(),
            project_id: project_id.to_string(),
            owner: "cli".to_owned(),
            acquired_at,
            ttl_seconds: CLI_LEASE_TTL_SECONDS,
            last_heartbeat: initial_heartbeat,
            cleanup_handoff: Some(cleanup_handoff.clone()),
        };
        FsDaemonStore
            .write_lease_record(temp.path(), &LeaseRecord::CliWriter(lease))
            .expect("write cli lease");

        heartbeat_tick(&FsDaemonStore, temp.path(), "cli-heartbeat-handoff")
            .expect("heartbeat tick should succeed");

        let record = FsDaemonStore
            .read_lease_record(temp.path(), "cli-heartbeat-handoff")
            .expect("read updated lease");
        let LeaseRecord::CliWriter(cli) = record else {
            panic!("expected cli lease record");
        };
        assert!(
            cli.last_heartbeat > initial_heartbeat,
            "heartbeat should still advance last_heartbeat"
        );
        assert_eq!(
            cli.cleanup_handoff,
            Some(cleanup_handoff),
            "heartbeat updates must preserve a persisted cleanup handoff"
        );
    }

    #[cfg(unix)]
    #[test]
    fn run_pid_record_is_not_reclaimable_while_legacy_daemon_owner_may_still_be_running() {
        let pid_record = RunPidRecord {
            pid: std::process::id(),
            started_at: Utc::now(),
            owner: RunPidOwner::Daemon,
            writer_owner: Some("writer-alpha".to_owned()),
            run_id: None,
            run_started_at: None,
            proc_start_ticks: None,
            proc_start_marker: None,
        };

        assert!(
            !matches!(
                FileSystem::pid_record_live_state(&pid_record),
                PidRecordLiveState::Stale
            ),
            "test fixture must exercise a legacy pid record that still points at a running process"
        );
        assert!(
            !run_pid_record_is_reclaimable(&pid_record, Some("writer-beta")),
            "live or unverified legacy daemon pid records must block reclaim until they are verified stale"
        );
    }

    #[tokio::test]
    async fn drop_cleans_up_on_simulated_error() {
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("err-test".to_owned()).expect("valid id");

        let guard = CliWriterLeaseGuard::acquire(
            store(),
            temp.path(),
            project_id.clone(),
            CLI_LEASE_TTL_SECONDS,
            CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
        )
        .expect("acquire");

        let lease_id = guard.lease_id.clone();
        // Simulate error path: drop guard explicitly
        drop(guard);

        // Lease record should be gone
        let records = FsDaemonStore.list_lease_records(temp.path()).expect("list");
        assert!(records.is_empty(), "lease record should be removed on drop");

        // Writer lock should be released
        FsDaemonStore
            .acquire_writer_lock(temp.path(), &project_id, "post-error")
            .expect("lock should be available after error drop");
        FsDaemonStore
            .release_writer_lock(temp.path(), &project_id, "post-error")
            .expect("cleanup");

        // Double-check by lease_id
        assert!(
            FsDaemonStore
                .read_lease_record(temp.path(), &lease_id)
                .is_err(),
            "lease record should not exist after cleanup"
        );
    }

    #[tokio::test]
    async fn failed_lock_acquisition_leaves_no_lease_record() {
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("contention-test".to_owned()).expect("valid id");

        // Pre-hold the writer lock
        FsDaemonStore
            .acquire_writer_lock(temp.path(), &project_id, "blocker")
            .expect("pre-acquire");

        let result = CliWriterLeaseGuard::acquire(
            store(),
            temp.path(),
            project_id.clone(),
            CLI_LEASE_TTL_SECONDS,
            CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
        );
        assert!(result.is_err(), "should fail when lock is held");

        // No lease record should exist
        let records = FsDaemonStore.list_lease_records(temp.path()).expect("list");
        assert!(
            records.is_empty(),
            "no lease record should be written on failed acquisition"
        );

        FsDaemonStore
            .release_writer_lock(temp.path(), &project_id, "blocker")
            .expect("cleanup blocker");
    }

    #[tokio::test]
    async fn lease_is_reconcile_visible_and_stale_detectable() {
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("reconcile-vis-test".to_owned()).expect("valid id");

        let guard = CliWriterLeaseGuard::acquire(
            store(),
            temp.path(),
            project_id.clone(),
            10, // very short TTL
            CLI_LEASE_HEARTBEAT_CADENCE_SECONDS,
        )
        .expect("acquire");

        let records = FsDaemonStore.list_lease_records(temp.path()).expect("list");
        assert_eq!(1, records.len());

        // The record should be stale-detectable after TTL
        let future_time = Utc::now() + Duration::seconds(11);
        assert!(
            records[0].is_stale_at(future_time),
            "lease should be stale after TTL"
        );

        drop(guard);
    }

    #[test]
    fn reclaim_stale_project_writer_lease_releases_lock_and_prunes_cli_leases() {
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("stale-cli-cleanup".to_owned()).expect("valid id");
        let lease = CliWriterLease {
            lease_id: "cli-stale-cleanup".to_owned(),
            project_id: project_id.to_string(),
            owner: "cli".to_owned(),
            acquired_at: Utc::now(),
            ttl_seconds: CLI_LEASE_TTL_SECONDS,
            last_heartbeat: Utc::now(),
            cleanup_handoff: None,
        };
        FsDaemonStore
            .write_lease_record(temp.path(), &LeaseRecord::CliWriter(lease.clone()))
            .expect("write stale cli lease");
        FsDaemonStore
            .acquire_writer_lock(temp.path(), &project_id, &lease.lease_id)
            .expect("acquire writer lock");

        reclaim_stale_project_writer_lease(&FsDaemonStore, temp.path(), &project_id)
            .expect("reclaim stale lease");

        assert!(
            FsDaemonStore
                .list_lease_records(temp.path())
                .expect("list leases")
                .is_empty(),
            "stale cli lease record should be removed"
        );
        FsDaemonStore
            .acquire_writer_lock(temp.path(), &project_id, "after-stale-cleanup")
            .expect("writer lock should be available after cleanup");
        FsDaemonStore
            .release_writer_lock(temp.path(), &project_id, "after-stale-cleanup")
            .expect("cleanup post-test writer lock");
    }

    #[test]
    fn reclaim_specific_cli_writer_lease_preserves_replaced_owner() {
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("stale-cli-race".to_owned()).expect("valid id");
        let stale_lease = CliWriterLease {
            lease_id: "cli-stale-race".to_owned(),
            project_id: project_id.to_string(),
            owner: "cli".to_owned(),
            acquired_at: Utc::now(),
            ttl_seconds: CLI_LEASE_TTL_SECONDS,
            last_heartbeat: Utc::now(),
            cleanup_handoff: None,
        };
        let fresh_lease = CliWriterLease {
            lease_id: "cli-fresh-race".to_owned(),
            project_id: project_id.to_string(),
            owner: "cli".to_owned(),
            acquired_at: Utc::now(),
            ttl_seconds: CLI_LEASE_TTL_SECONDS,
            last_heartbeat: Utc::now(),
            cleanup_handoff: None,
        };
        FsDaemonStore
            .write_lease_record(temp.path(), &LeaseRecord::CliWriter(stale_lease.clone()))
            .expect("write stale cli lease");
        FsDaemonStore
            .write_lease_record(temp.path(), &LeaseRecord::CliWriter(fresh_lease.clone()))
            .expect("write fresh cli lease");
        FsDaemonStore
            .acquire_writer_lock(temp.path(), &project_id, &fresh_lease.lease_id)
            .expect("acquire fresh writer lock");

        let reclaimed = reclaim_specific_cli_writer_lease(
            &FsDaemonStore,
            temp.path(),
            &project_id,
            &stale_lease.lease_id,
        )
        .expect("reclaim should not fail");

        assert!(
            !reclaimed,
            "reclaim should refuse to remove a replaced owner"
        );
        assert!(
            FsDaemonStore
                .read_lease_record(temp.path(), &fresh_lease.lease_id)
                .is_ok(),
            "fresh cli lease must remain intact"
        );
        FsDaemonStore
            .release_writer_lock(temp.path(), &project_id, &fresh_lease.lease_id)
            .expect("cleanup fresh writer lock");
    }

    #[test]
    fn reclaim_specific_project_writer_owner_releases_observed_worktree_owner() {
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("stale-daemon-owner".to_owned()).expect("valid id");
        let now = Utc::now();
        let worktree_lease = WorktreeLease {
            lease_id: "lease-stale-daemon-owner".to_owned(),
            task_id: "task-stale-daemon-owner".to_owned(),
            project_id: project_id.to_string(),
            worktree_path: temp.path().join("worktrees/task-stale-daemon-owner"),
            branch_name: "task-stale-daemon-owner".to_owned(),
            acquired_at: now,
            ttl_seconds: CLI_LEASE_TTL_SECONDS,
            last_heartbeat: now,
            cleanup_handoff: None,
        };
        FsDaemonStore
            .write_task(
                temp.path(),
                &DaemonTask {
                    task_id: worktree_lease.task_id.clone(),
                    issue_ref: "repo#1".to_owned(),
                    project_id: project_id.to_string(),
                    project_name: Some("stale daemon".to_owned()),
                    prompt: Some("prompt".to_owned()),
                    routing_command: None,
                    routing_labels: vec![],
                    resolved_flow: Some(FlowPreset::Standard),
                    routing_source: Some(RoutingSource::DefaultFlow),
                    routing_warnings: vec![],
                    status: TaskStatus::Active,
                    created_at: now,
                    updated_at: now,
                    attempt_count: 0,
                    lease_id: Some(worktree_lease.lease_id.clone()),
                    failure_class: None,
                    failure_message: None,
                    dispatch_mode: DispatchMode::Workflow,
                    source_revision: None,
                    requirements_run_id: None,
                    workflow_run_id: None,
                    repo_slug: None,
                    issue_number: None,
                    pr_url: None,
                    last_seen_comment_id: None,
                    last_seen_review_id: None,
                    label_dirty: false,
                },
            )
            .expect("write daemon task");
        FsDaemonStore
            .write_lease_record(temp.path(), &LeaseRecord::Worktree(worktree_lease.clone()))
            .expect("write worktree lease");
        FsDaemonStore
            .acquire_writer_lock(temp.path(), &project_id, &worktree_lease.lease_id)
            .expect("acquire writer lock");

        let reclaimed = reclaim_specific_project_writer_owner(
            &FsDaemonStore,
            &StubWorktreePort {
                cleanup: StubWorktreeCleanup::Removed,
            },
            temp.path(),
            temp.path(),
            &project_id,
            &worktree_lease.lease_id,
        )
        .expect("reclaim observed worktree owner");

        assert!(reclaimed, "observed worktree owner should be reclaimed");
        assert!(
            read_project_writer_lock_owner(temp.path(), &project_id)
                .expect("read writer owner")
                .is_none(),
            "writer lock should be released"
        );
        assert!(
            FsDaemonStore
                .read_lease_record(temp.path(), &worktree_lease.lease_id)
                .is_err(),
            "worktree lease record should be removed"
        );
        let task = FsDaemonStore
            .read_task(temp.path(), &worktree_lease.task_id)
            .expect("read daemon task");
        assert_eq!(task.status, TaskStatus::Failed);
        assert!(
            task.lease_id.is_none(),
            "stale daemon recovery must clear the task lease reference"
        );
    }

    #[test]
    fn reclaim_specific_project_writer_owner_surfaces_partial_cleanup_failure() {
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("stale-daemon-owner-partial".to_owned()).expect("valid id");
        let now = Utc::now();
        let worktree_lease = WorktreeLease {
            lease_id: "lease-stale-daemon-owner-partial".to_owned(),
            task_id: "task-stale-daemon-owner-partial".to_owned(),
            project_id: project_id.to_string(),
            worktree_path: temp
                .path()
                .join("worktrees/missing-stale-daemon-owner-partial"),
            branch_name: "task-stale-daemon-owner-partial".to_owned(),
            acquired_at: now,
            ttl_seconds: CLI_LEASE_TTL_SECONDS,
            last_heartbeat: now,
            cleanup_handoff: None,
        };
        FsDaemonStore
            .write_task(
                temp.path(),
                &DaemonTask {
                    task_id: worktree_lease.task_id.clone(),
                    issue_ref: "repo#2".to_owned(),
                    project_id: project_id.to_string(),
                    project_name: Some("stale daemon partial".to_owned()),
                    prompt: Some("prompt".to_owned()),
                    routing_command: None,
                    routing_labels: vec![],
                    resolved_flow: Some(FlowPreset::Standard),
                    routing_source: Some(RoutingSource::DefaultFlow),
                    routing_warnings: vec![],
                    status: TaskStatus::Active,
                    created_at: now,
                    updated_at: now,
                    attempt_count: 0,
                    lease_id: Some(worktree_lease.lease_id.clone()),
                    failure_class: None,
                    failure_message: None,
                    dispatch_mode: DispatchMode::Workflow,
                    source_revision: None,
                    requirements_run_id: None,
                    workflow_run_id: None,
                    repo_slug: None,
                    issue_number: None,
                    pr_url: None,
                    last_seen_comment_id: None,
                    last_seen_review_id: None,
                    label_dirty: false,
                },
            )
            .expect("write daemon task");
        FsDaemonStore
            .write_lease_record(temp.path(), &LeaseRecord::Worktree(worktree_lease.clone()))
            .expect("write worktree lease");
        FsDaemonStore
            .acquire_writer_lock(temp.path(), &project_id, &worktree_lease.lease_id)
            .expect("acquire writer lock");

        let error = reclaim_specific_project_writer_owner(
            &FsDaemonStore,
            &StubWorktreePort {
                cleanup: StubWorktreeCleanup::AlreadyAbsent,
            },
            temp.path(),
            temp.path(),
            &project_id,
            &worktree_lease.lease_id,
        )
        .expect_err("partial cleanup should surface an error");

        assert!(
            matches!(error, AppError::LeaseCleanupPartialFailure { .. }),
            "expected partial cleanup error, got: {error:?}"
        );
        assert!(
            read_project_writer_lock_owner(temp.path(), &project_id)
                .expect("read writer owner")
                .is_none(),
            "writer lock release may already have succeeded before the partial cleanup surfaced"
        );
        assert!(
            FsDaemonStore
                .read_lease_record(temp.path(), &worktree_lease.lease_id)
                .is_ok(),
            "worktree lease record should remain for operator recovery"
        );
        let task = FsDaemonStore
            .read_task(temp.path(), &worktree_lease.task_id)
            .expect("read daemon task");
        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(
            task.lease_id.as_deref(),
            Some(worktree_lease.lease_id.as_str()),
            "partial cleanup must preserve the task lease reference"
        );
    }

    #[test]
    fn reclaim_specific_project_writer_owner_blocks_missing_lease_when_task_still_references_owner()
    {
        let temp = tempdir().expect("tempdir");
        let project_id =
            ProjectId::new("stale-daemon-owner-missing-lease".to_owned()).expect("valid id");
        let now = Utc::now();
        let stale_owner = "lease-stale-daemon-owner-missing-lease";
        FsDaemonStore
            .write_task(
                temp.path(),
                &DaemonTask {
                    task_id: "task-stale-daemon-owner-missing-lease".to_owned(),
                    issue_ref: "repo#2b".to_owned(),
                    project_id: project_id.to_string(),
                    project_name: Some("stale daemon missing lease".to_owned()),
                    prompt: Some("prompt".to_owned()),
                    routing_command: None,
                    routing_labels: vec![],
                    resolved_flow: Some(FlowPreset::Standard),
                    routing_source: Some(RoutingSource::DefaultFlow),
                    routing_warnings: vec![],
                    status: TaskStatus::Failed,
                    created_at: now,
                    updated_at: now,
                    attempt_count: 0,
                    lease_id: Some(stale_owner.to_owned()),
                    failure_class: Some("stale_writer_owner_reclaimed".to_owned()),
                    failure_message: Some("cleanup metadata write failed".to_owned()),
                    dispatch_mode: DispatchMode::Workflow,
                    source_revision: None,
                    requirements_run_id: None,
                    workflow_run_id: None,
                    repo_slug: None,
                    issue_number: None,
                    pr_url: None,
                    last_seen_comment_id: None,
                    last_seen_review_id: None,
                    label_dirty: false,
                },
            )
            .expect("write daemon task");
        FsDaemonStore
            .acquire_writer_lock(temp.path(), &project_id, stale_owner)
            .expect("acquire writer lock");

        let error = reclaim_specific_project_writer_owner(
            &FsDaemonStore,
            &StubWorktreePort {
                cleanup: StubWorktreeCleanup::Removed,
            },
            temp.path(),
            temp.path(),
            &project_id,
            stale_owner,
        )
        .expect_err("orphaned task metadata should block reclaim success");

        assert!(
            matches!(error, AppError::LeaseCleanupPartialFailure { .. }),
            "expected partial cleanup error, got: {error:?}"
        );
        assert!(
            read_project_writer_lock_owner(temp.path(), &project_id)
                .expect("read writer owner")
                .is_none(),
            "stale writer lock may already be released before the partial cleanup surfaces"
        );
        let task = FsDaemonStore
            .read_task(temp.path(), "task-stale-daemon-owner-missing-lease")
            .expect("read daemon task");
        assert_eq!(
            task.lease_id.as_deref(),
            Some(stale_owner),
            "missing-lease reclaim must preserve the orphaned task lease reference"
        );
    }

    #[test]
    fn reclaim_specific_project_writer_owner_removes_matching_daemon_run_pid() {
        let temp = tempdir().expect("tempdir");
        let project_id = ProjectId::new("stale-daemon-owner-run-pid".to_owned()).expect("valid id");
        let now = Utc::now();
        let worktree_lease = WorktreeLease {
            lease_id: "lease-stale-daemon-owner-run-pid".to_owned(),
            task_id: "task-stale-daemon-owner-run-pid".to_owned(),
            project_id: project_id.to_string(),
            worktree_path: temp
                .path()
                .join("worktrees/task-stale-daemon-owner-run-pid"),
            branch_name: "task-stale-daemon-owner-run-pid".to_owned(),
            acquired_at: now,
            ttl_seconds: CLI_LEASE_TTL_SECONDS,
            last_heartbeat: now,
            cleanup_handoff: None,
        };
        FsDaemonStore
            .write_task(
                temp.path(),
                &DaemonTask {
                    task_id: worktree_lease.task_id.clone(),
                    issue_ref: "repo#2c".to_owned(),
                    project_id: project_id.to_string(),
                    project_name: Some("stale daemon pid".to_owned()),
                    prompt: Some("prompt".to_owned()),
                    routing_command: None,
                    routing_labels: vec![],
                    resolved_flow: Some(FlowPreset::Standard),
                    routing_source: Some(RoutingSource::DefaultFlow),
                    routing_warnings: vec![],
                    status: TaskStatus::Active,
                    created_at: now,
                    updated_at: now,
                    attempt_count: 0,
                    lease_id: Some(worktree_lease.lease_id.clone()),
                    failure_class: None,
                    failure_message: None,
                    dispatch_mode: DispatchMode::Workflow,
                    source_revision: None,
                    requirements_run_id: None,
                    workflow_run_id: None,
                    repo_slug: None,
                    issue_number: None,
                    pr_url: None,
                    last_seen_comment_id: None,
                    last_seen_review_id: None,
                    label_dirty: false,
                },
            )
            .expect("write daemon task");
        FsDaemonStore
            .write_lease_record(temp.path(), &LeaseRecord::Worktree(worktree_lease.clone()))
            .expect("write worktree lease");
        FsDaemonStore
            .acquire_writer_lock(temp.path(), &project_id, &worktree_lease.lease_id)
            .expect("acquire writer lock");
        FileSystem::write_pid_file(
            temp.path(),
            &project_id,
            RunPidOwner::Daemon,
            Some(worktree_lease.lease_id.as_str()),
            Some("run-stale-daemon-owner-run-pid"),
            Some(now),
        )
        .expect("write daemon run.pid");

        let reclaimed = reclaim_specific_project_writer_owner(
            &FsDaemonStore,
            &StubWorktreePort {
                cleanup: StubWorktreeCleanup::Removed,
            },
            temp.path(),
            temp.path(),
            &project_id,
            &worktree_lease.lease_id,
        )
        .expect("reclaim observed worktree owner");

        assert!(reclaimed, "observed worktree owner should be reclaimed");
        assert!(
            FileSystem::read_pid_file(temp.path(), &project_id)
                .expect("read run.pid")
                .is_none(),
            "stale daemon-owner reclaim must remove the matching daemon run.pid"
        );
    }

    #[test]
    fn cleanup_detached_project_writer_owner_surfaces_partial_cleanup_after_lock_loss() {
        let temp = tempdir().expect("tempdir");
        let project_id =
            ProjectId::new("stale-daemon-owner-detached".to_owned()).expect("valid id");
        let now = Utc::now();
        let worktree_lease = WorktreeLease {
            lease_id: "lease-stale-daemon-owner-detached".to_owned(),
            task_id: "task-stale-daemon-owner-detached".to_owned(),
            project_id: project_id.to_string(),
            worktree_path: temp
                .path()
                .join("worktrees/missing-stale-daemon-owner-detached"),
            branch_name: "task-stale-daemon-owner-detached".to_owned(),
            acquired_at: now,
            ttl_seconds: CLI_LEASE_TTL_SECONDS,
            last_heartbeat: now,
            cleanup_handoff: None,
        };
        FsDaemonStore
            .write_task(
                temp.path(),
                &DaemonTask {
                    task_id: worktree_lease.task_id.clone(),
                    issue_ref: "repo#3".to_owned(),
                    project_id: project_id.to_string(),
                    project_name: Some("stale daemon detached".to_owned()),
                    prompt: Some("prompt".to_owned()),
                    routing_command: None,
                    routing_labels: vec![],
                    resolved_flow: Some(FlowPreset::Standard),
                    routing_source: Some(RoutingSource::DefaultFlow),
                    routing_warnings: vec![],
                    status: TaskStatus::Failed,
                    created_at: now,
                    updated_at: now,
                    attempt_count: 0,
                    lease_id: Some(worktree_lease.lease_id.clone()),
                    failure_class: Some("stale_writer_owner_reclaimed".to_owned()),
                    failure_message: Some("partial cleanup preserved lease state".to_owned()),
                    dispatch_mode: DispatchMode::Workflow,
                    source_revision: None,
                    requirements_run_id: None,
                    workflow_run_id: None,
                    repo_slug: None,
                    issue_number: None,
                    pr_url: None,
                    last_seen_comment_id: None,
                    last_seen_review_id: None,
                    label_dirty: false,
                },
            )
            .expect("write daemon task");
        FsDaemonStore
            .write_lease_record(temp.path(), &LeaseRecord::Worktree(worktree_lease.clone()))
            .expect("write detached worktree lease");

        let error = cleanup_detached_project_writer_owner(
            &FsDaemonStore,
            &StubWorktreePort {
                cleanup: StubWorktreeCleanup::AlreadyAbsent,
            },
            temp.path(),
            temp.path(),
            &project_id,
            &worktree_lease.lease_id,
        )
        .expect_err("partial detached cleanup should surface an error");

        assert!(
            matches!(error, AppError::LeaseCleanupPartialFailure { .. }),
            "expected partial cleanup error, got: {error:?}"
        );
        assert!(
            FsDaemonStore
                .read_lease_record(temp.path(), &worktree_lease.lease_id)
                .is_ok(),
            "detached lease record should remain durable after partial cleanup"
        );
        let task = FsDaemonStore
            .read_task(temp.path(), &worktree_lease.task_id)
            .expect("read daemon task");
        assert_eq!(task.status, TaskStatus::Failed);
        assert_eq!(
            task.lease_id.as_deref(),
            Some(worktree_lease.lease_id.as_str()),
            "detached cleanup must preserve the stale task lease reference on partial failure"
        );
    }

    #[test]
    fn cleanup_detached_project_writer_owner_removes_matching_daemon_run_pid() {
        let temp = tempdir().expect("tempdir");
        let project_id =
            ProjectId::new("stale-daemon-owner-detached-run-pid".to_owned()).expect("valid id");
        let now = Utc::now();
        let worktree_lease = WorktreeLease {
            lease_id: "lease-stale-daemon-owner-detached-run-pid".to_owned(),
            task_id: "task-stale-daemon-owner-detached-run-pid".to_owned(),
            project_id: project_id.to_string(),
            worktree_path: temp
                .path()
                .join("worktrees/task-stale-daemon-owner-detached-run-pid"),
            branch_name: "task-stale-daemon-owner-detached-run-pid".to_owned(),
            acquired_at: now,
            ttl_seconds: CLI_LEASE_TTL_SECONDS,
            last_heartbeat: now,
            cleanup_handoff: None,
        };
        FsDaemonStore
            .write_task(
                temp.path(),
                &DaemonTask {
                    task_id: worktree_lease.task_id.clone(),
                    issue_ref: "repo#3b".to_owned(),
                    project_id: project_id.to_string(),
                    project_name: Some("stale daemon detached pid".to_owned()),
                    prompt: Some("prompt".to_owned()),
                    routing_command: None,
                    routing_labels: vec![],
                    resolved_flow: Some(FlowPreset::Standard),
                    routing_source: Some(RoutingSource::DefaultFlow),
                    routing_warnings: vec![],
                    status: TaskStatus::Failed,
                    created_at: now,
                    updated_at: now,
                    attempt_count: 0,
                    lease_id: Some(worktree_lease.lease_id.clone()),
                    failure_class: Some("stale_writer_owner_reclaimed".to_owned()),
                    failure_message: Some("stale daemon owner detached".to_owned()),
                    dispatch_mode: DispatchMode::Workflow,
                    source_revision: None,
                    requirements_run_id: None,
                    workflow_run_id: None,
                    repo_slug: None,
                    issue_number: None,
                    pr_url: None,
                    last_seen_comment_id: None,
                    last_seen_review_id: None,
                    label_dirty: false,
                },
            )
            .expect("write daemon task");
        FsDaemonStore
            .write_lease_record(temp.path(), &LeaseRecord::Worktree(worktree_lease.clone()))
            .expect("write detached worktree lease");
        FileSystem::write_pid_file(
            temp.path(),
            &project_id,
            RunPidOwner::Daemon,
            Some(worktree_lease.lease_id.as_str()),
            Some("run-stale-daemon-owner-detached-run-pid"),
            Some(now),
        )
        .expect("write daemon run.pid");

        let reclaimed = cleanup_detached_project_writer_owner(
            &FsDaemonStore,
            &StubWorktreePort {
                cleanup: StubWorktreeCleanup::Removed,
            },
            temp.path(),
            temp.path(),
            &project_id,
            &worktree_lease.lease_id,
        )
        .expect("cleanup detached worktree owner");

        assert!(reclaimed, "detached stale owner should be reclaimed");
        assert!(
            FileSystem::read_pid_file(temp.path(), &project_id)
                .expect("read run.pid")
                .is_none(),
            "detached stale-owner cleanup must remove the matching daemon run.pid"
        );
    }

    #[test]
    fn detached_owner_discovery_falls_back_to_orphaned_task_lease_reference() {
        let temp = tempdir().expect("tempdir");
        let project_id =
            ProjectId::new("stale-daemon-owner-orphaned".to_owned()).expect("valid id");
        let now = Utc::now();
        let worktree_lease = WorktreeLease {
            lease_id: "lease-stale-daemon-owner-orphaned".to_owned(),
            task_id: "task-stale-daemon-owner-orphaned".to_owned(),
            project_id: project_id.to_string(),
            worktree_path: temp
                .path()
                .join("worktrees/task-stale-daemon-owner-orphaned"),
            branch_name: "task-stale-daemon-owner-orphaned".to_owned(),
            acquired_at: now,
            ttl_seconds: CLI_LEASE_TTL_SECONDS,
            last_heartbeat: now,
            cleanup_handoff: None,
        };
        FsDaemonStore
            .write_task(
                temp.path(),
                &DaemonTask {
                    task_id: worktree_lease.task_id.clone(),
                    issue_ref: "repo#4".to_owned(),
                    project_id: project_id.to_string(),
                    project_name: Some("stale daemon orphaned".to_owned()),
                    prompt: Some("prompt".to_owned()),
                    routing_command: None,
                    routing_labels: vec![],
                    resolved_flow: Some(FlowPreset::Standard),
                    routing_source: Some(RoutingSource::DefaultFlow),
                    routing_warnings: vec![],
                    status: TaskStatus::Failed,
                    created_at: now,
                    updated_at: now,
                    attempt_count: 0,
                    lease_id: Some(worktree_lease.lease_id.clone()),
                    failure_class: Some("daemon_failed".to_owned()),
                    failure_message: Some("cleanup metadata write failed".to_owned()),
                    dispatch_mode: DispatchMode::Workflow,
                    source_revision: None,
                    requirements_run_id: None,
                    workflow_run_id: None,
                    repo_slug: None,
                    issue_number: None,
                    pr_url: None,
                    last_seen_comment_id: None,
                    last_seen_review_id: None,
                    label_dirty: false,
                },
            )
            .expect("write daemon task");
        FsDaemonStore
            .write_lease_record(temp.path(), &LeaseRecord::Worktree(worktree_lease.clone()))
            .expect("write detached worktree lease");

        let failing_store = FailOnceLeaseReferenceClearStore::new(&worktree_lease.task_id);
        let error = cleanup_detached_project_writer_owner(
            &failing_store,
            &StubWorktreePort {
                cleanup: StubWorktreeCleanup::Removed,
            },
            temp.path(),
            temp.path(),
            &project_id,
            &worktree_lease.lease_id,
        )
        .expect_err("metadata write failure should surface as partial cleanup");
        assert!(
            matches!(error, AppError::LeaseCleanupPartialFailure { .. }),
            "expected partial cleanup error, got: {error:?}"
        );
        assert!(
            FsDaemonStore
                .read_lease_record(temp.path(), &worktree_lease.lease_id)
                .is_err(),
            "the first partial cleanup attempt should have already removed the lease record"
        );

        let observed_owner =
            find_detached_project_writer_owner(&FsDaemonStore, temp.path(), &project_id)
                .expect("find orphaned detached owner");
        assert_eq!(
            observed_owner,
            DetachedProjectWriterOwner::Single(worktree_lease.lease_id.clone()),
            "task-side orphaned lease references must stay discoverable for follow-up recovery"
        );

        let retry_error = cleanup_detached_project_writer_owner(
            &FsDaemonStore,
            &StubWorktreePort {
                cleanup: StubWorktreeCleanup::Removed,
            },
            temp.path(),
            temp.path(),
            &project_id,
            &worktree_lease.lease_id,
        )
        .expect_err(
            "follow-up recovery should stay blocked until the task lease reference is cleared",
        );
        assert!(
            matches!(retry_error, AppError::LeaseCleanupPartialFailure { .. }),
            "expected retry to surface partial cleanup, got: {retry_error:?}"
        );
    }

    #[test]
    fn find_detached_project_writer_owner_reports_ambiguous_detached_state() {
        let temp = tempfile::tempdir().expect("tempdir");
        let project_id = ProjectId::new("ambiguous-detached-owner").expect("project id");
        let now = Utc::now();

        let stale_cli_lease = CliWriterLease {
            lease_id: "cli-detached-owner".to_owned(),
            project_id: project_id.to_string(),
            owner: "cli".to_owned(),
            acquired_at: now,
            ttl_seconds: 1,
            last_heartbeat: now - chrono::Duration::minutes(10),
            cleanup_handoff: None,
        };
        FsDaemonStore
            .write_lease_record(
                temp.path(),
                &LeaseRecord::CliWriter(stale_cli_lease.clone()),
            )
            .expect("write stale cli lease");

        let orphaned_worktree_lease = WorktreeLease {
            lease_id: "lease-detached-owner".to_owned(),
            task_id: "task-detached-owner".to_owned(),
            project_id: project_id.to_string(),
            worktree_path: temp.path().join("worktrees/orphaned-detached-owner"),
            branch_name: "task-detached-owner".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
            cleanup_handoff: None,
        };
        FsDaemonStore
            .write_task(
                temp.path(),
                &DaemonTask {
                    task_id: orphaned_worktree_lease.task_id.clone(),
                    issue_ref: "repo#44".to_owned(),
                    project_id: project_id.to_string(),
                    project_name: Some("ambiguous detached owner".to_owned()),
                    prompt: Some("prompt".to_owned()),
                    routing_command: None,
                    routing_labels: vec![],
                    resolved_flow: Some(FlowPreset::Standard),
                    routing_source: Some(RoutingSource::DefaultFlow),
                    routing_warnings: vec![],
                    status: TaskStatus::Failed,
                    created_at: now,
                    updated_at: now,
                    attempt_count: 0,
                    lease_id: Some(orphaned_worktree_lease.lease_id.clone()),
                    failure_class: Some("stale_writer_owner_reclaimed".to_owned()),
                    failure_message: Some("partial cleanup preserved lease state".to_owned()),
                    dispatch_mode: DispatchMode::Workflow,
                    source_revision: None,
                    requirements_run_id: None,
                    workflow_run_id: None,
                    repo_slug: None,
                    issue_number: None,
                    pr_url: None,
                    last_seen_comment_id: None,
                    last_seen_review_id: None,
                    label_dirty: false,
                },
            )
            .expect("write orphaned detached task");

        let observed_owner =
            find_detached_project_writer_owner(&FsDaemonStore, temp.path(), &project_id)
                .expect("find detached owners");
        assert_eq!(
            observed_owner,
            DetachedProjectWriterOwner::Ambiguous(vec![
                stale_cli_lease.lease_id,
                orphaned_worktree_lease.lease_id,
            ]),
            "ambiguous detached-owner state must stay visible instead of collapsing to None"
        );
    }
}
