use std::path::Path;

use chrono::{DateTime, Utc};
use serde_json::json;

use crate::contexts::automation_runtime::model::{
    saturating_heartbeat_deadline, LeaseRecord, TaskStatus, WorktreeLease,
};
use crate::contexts::automation_runtime::task_service::DaemonTaskService;
use crate::contexts::automation_runtime::{
    DaemonStorePort, ResourceCleanupOutcome, WorktreeCleanupOutcome, WorktreePort,
    WriterLockReleaseOutcome,
};
use crate::shared::domain::ProjectId;
use crate::shared::error::{AppError, AppResult};

/// Best-effort force-push of the worktree branch to preserve checkpoint
/// commits from a failed task run. Only pushes when the worktree contains
/// checkpoint commits from the implementation stage or later.
///
/// This function is the single entry point for branch preservation and
/// should be called before any `LeaseService::release()` that disposes of
/// a failed task's worktree.
pub fn try_preserve_failed_branch(
    worktree: &dyn WorktreePort,
    repo_root: &Path,
    lease: &WorktreeLease,
) {
    if !worktree.has_checkpoint_commits(&lease.worktree_path) {
        return;
    }
    if let Err(e) = worktree.force_push_branch(repo_root, &lease.worktree_path, &lease.branch_name)
    {
        eprintln!(
            "daemon: best-effort push of branch '{}' failed: {e}",
            lease.branch_name
        );
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileReport {
    pub stale_lease_ids: Vec<String>,
    pub failed_task_ids: Vec<String>,
    pub released_lease_ids: Vec<String>,
    pub removed_worktrees: Vec<String>,
    pub cleanup_failures: Vec<LeaseCleanupFailure>,
}

impl ReconcileReport {
    pub fn has_cleanup_failures(&self) -> bool {
        !self.cleanup_failures.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseCleanupFailure {
    pub lease_id: String,
    pub task_id: Option<String>,
    pub details: String,
}

/// Controls how `release()` reports `AlreadyAbsent` outcomes from cleanup
/// sub-steps. Both modes enforce the same fail-closed `resources_released`
/// rule: true only when worktree removal, writer-lock release, and
/// lease-file deletion all positively succeed. The difference is error
/// reporting: `Strict` treats already-absent as a reportable cleanup
/// failure, while `Idempotent` returns `Ok` without surfacing it.
///
/// Release order: worktree removal → writer-lock release → lease-file
/// deletion.  The lease file is preserved when writer-lock release fails
/// so the durable record remains visible for recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseMode {
    /// `AlreadyAbsent` sub-step outcomes do not produce an error, but
    /// `resources_released` is still `false` — callers must not clear
    /// durable lease references or emit `LeaseReleased` journal entries.
    Idempotent,
    /// `AlreadyAbsent` sub-step outcomes are treated as reportable cleanup
    /// failures. Used by `reconcile()` where the strict pre-check already
    /// ensures resources should still exist.
    Strict,
}

/// Result of a lease release operation. Distinguishes physical cleanup
/// (worktree, writer lock, lease file) from post-cleanup journal append,
/// and tracks per-sub-step outcomes so callers can enforce strict cleanup
/// accounting.
#[derive(Debug, Clone)]
pub struct ReleaseResult {
    /// Whether physical cleanup succeeded according to the chosen
    /// `ReleaseMode`. True only when worktree removal, writer-lock release,
    /// and lease-file deletion all positively succeeded. Callers must only
    /// clear durable lease references when this is true.
    pub resources_released: bool,
    /// If the LeaseReleased journal append failed after physical cleanup,
    /// this contains the error description. Only set when `resources_released`
    /// is true, since journal append is skipped on partial failure.
    pub journal_error: Option<String>,
    /// Whether the worktree was already absent when removal was attempted.
    pub worktree_already_absent: bool,
    /// If worktree removal returned a real I/O error, this contains the error
    /// description. When set, writer-lock release and lease-file deletion are
    /// not attempted.
    pub worktree_error: Option<String>,
    /// Whether the lease file was already absent when deletion was attempted.
    /// Only meaningful when writer-lock release returned `Released`.
    pub lease_file_already_absent: bool,
    /// Whether the writer lock was already absent when release was attempted.
    pub writer_lock_already_absent: bool,
    /// Whether the writer lock was owned by a different writer at release time.
    pub writer_lock_owner_mismatch: bool,
    /// If lease-file deletion returned a real I/O error (not `AlreadyAbsent`),
    /// this contains the error description so callers can report the specific
    /// failing sub-step.
    pub lease_file_error: Option<String>,
    /// If writer-lock release returned a real I/O error (not `AlreadyAbsent`),
    /// this contains the error description so callers can report the specific
    /// failing sub-step.
    pub writer_lock_error: Option<String>,
}

impl ReleaseResult {
    /// Returns true if any cleanup sub-step failed or found already-absent
    /// state. Callers should not clear durable lease references when this
    /// returns true.
    pub fn has_cleanup_failures(&self) -> bool {
        !self.resources_released
    }
}

pub struct LeaseService;

impl LeaseService {
    #[allow(clippy::too_many_arguments)]
    pub fn acquire(
        store: &dyn DaemonStorePort,
        worktree: &dyn WorktreePort,
        base_dir: &Path,
        repo_root: &Path,
        task_id: &str,
        project_id: &ProjectId,
        ttl_seconds: u64,
        worktree_path_override: Option<std::path::PathBuf>,
        branch_name_override: Option<String>,
        is_retry: bool,
    ) -> AppResult<WorktreeLease> {
        if store
            .list_leases(base_dir)?
            .iter()
            .any(|lease| lease.task_id == task_id)
        {
            return Err(AppError::TaskStateTransitionInvalid {
                task_id: task_id.to_owned(),
                from: "lease_exists".to_owned(),
                to: "lease_acquire".to_owned(),
            });
        }

        let lease_id = format!("lease-{task_id}");
        store.acquire_writer_lock(base_dir, project_id, &lease_id)?;

        let worktree_path =
            worktree_path_override.unwrap_or_else(|| worktree.worktree_path(base_dir, task_id));
        let branch_name = branch_name_override.unwrap_or_else(|| worktree.branch_name(task_id));
        if let Err(error) =
            worktree.create_worktree(repo_root, &worktree_path, &branch_name, task_id)
        {
            // Rollback: attempt every applicable cleanup step and aggregate
            // failures so the caller knows which resources may be orphaned.
            let mut rollback_failures = Vec::new();

            // Step 1: remove any partially created worktree path.
            if worktree_path.exists() {
                if let Err(e) = worktree.remove_worktree(repo_root, &worktree_path, task_id) {
                    rollback_failures.push(format!("worktree removal: {e}"));
                }
            }

            // Step 2: release writer lock.
            let lock_may_be_held = match store.release_writer_lock(base_dir, project_id, &lease_id)
            {
                Ok(WriterLockReleaseOutcome::Released)
                | Ok(WriterLockReleaseOutcome::AlreadyAbsent) => false,
                Ok(WriterLockReleaseOutcome::OwnerMismatch { actual_owner }) => {
                    rollback_failures.push(format!(
                        "writer lock owner mismatch (actual: {actual_owner})"
                    ));
                    true
                }
                Err(e) => {
                    rollback_failures.push(format!("writer lock release: {e}"));
                    true
                }
            };

            if rollback_failures.is_empty() {
                return Err(error);
            }
            let mut details = rollback_failures.join("; ");
            if lock_may_be_held {
                details.push_str("; project writer lock may still be held");
            }
            return Err(AppError::AcquisitionRollbackFailed {
                trigger: error.to_string(),
                rollback_details: details,
            });
        }

        // Best-effort: on retries only, fetch a previously-preserved branch
        // and reset to the latest implementation-stage checkpoint. New tasks
        // (attempt_count == 0) always start fresh from the default branch.
        if is_retry {
            let _ = worktree.try_resume_from_remote(repo_root, &worktree_path, &branch_name);
        }

        let now = Utc::now();
        let lease = WorktreeLease {
            lease_id,
            task_id: task_id.to_owned(),
            project_id: project_id.to_string(),
            worktree_path,
            branch_name,
            acquired_at: now,
            ttl_seconds,
            last_heartbeat: now,
        };
        if let Err(error) = store.write_lease(base_dir, &lease) {
            // Rollback: attempt every applicable cleanup step and aggregate
            // failures so the caller knows which resources may be orphaned.
            let mut rollback_failures = Vec::new();
            if let Err(e) = worktree.remove_worktree(repo_root, &lease.worktree_path, task_id) {
                rollback_failures.push(format!("worktree removal: {e}"));
            }
            let lock_may_be_held =
                match store.release_writer_lock(base_dir, project_id, &lease.lease_id) {
                    Ok(WriterLockReleaseOutcome::Released)
                    | Ok(WriterLockReleaseOutcome::AlreadyAbsent) => false,
                    Ok(WriterLockReleaseOutcome::OwnerMismatch { actual_owner }) => {
                        rollback_failures.push(format!(
                            "writer lock owner mismatch (actual: {actual_owner})"
                        ));
                        true
                    }
                    Err(e) => {
                        rollback_failures.push(format!("writer lock release: {e}"));
                        true
                    }
                };
            if rollback_failures.is_empty() {
                return Err(error);
            }
            let mut details = rollback_failures.join("; ");
            if lock_may_be_held {
                details.push_str("; project writer lock may still be held");
            }
            return Err(AppError::AcquisitionRollbackFailed {
                trigger: error.to_string(),
                rollback_details: details,
            });
        }

        Ok(lease)
    }

    pub fn heartbeat(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        lease_id: &str,
    ) -> AppResult<WorktreeLease> {
        let mut lease = store.read_lease(base_dir, lease_id)?;
        let now = Utc::now();
        if lease.is_stale_at(now) {
            return Err(AppError::LeaseStale {
                lease_id: lease_id.to_owned(),
            });
        }
        lease.last_heartbeat = now;
        store.write_lease(base_dir, &lease)?;
        Ok(lease)
    }

    pub fn release(
        store: &dyn DaemonStorePort,
        worktree: &dyn WorktreePort,
        base_dir: &Path,
        repo_root: &Path,
        lease: &WorktreeLease,
        mode: ReleaseMode,
    ) -> AppResult<ReleaseResult> {
        let project_id = ProjectId::new(lease.project_id.clone())?;

        // PHASE 1: Worktree removal. If it fails, keep all durable lease
        // state (writer lock, lease file) intact so a later reconcile can retry.
        let worktree_already_absent =
            match worktree.remove_worktree(repo_root, &lease.worktree_path, &lease.task_id) {
                Ok(WorktreeCleanupOutcome::Removed) => false,
                Ok(WorktreeCleanupOutcome::AlreadyAbsent) => true,
                Err(e) => {
                    // Worktree removal failed — do not attempt writer-lock
                    // release or lease-file deletion.
                    return Ok(ReleaseResult {
                        resources_released: false,
                        journal_error: None,
                        worktree_already_absent: false,
                        worktree_error: Some(e.to_string()),
                        lease_file_already_absent: false,
                        writer_lock_already_absent: false,
                        writer_lock_owner_mismatch: false,
                        lease_file_error: None,
                        writer_lock_error: None,
                    });
                }
            };

        // PHASE 2: Owner-aware writer-lock release. Must succeed before
        // the lease file is deleted so the durable lease record remains
        // visible for recovery when the lock cannot be released.
        let writer_lock_released;
        let (writer_lock_already_absent, writer_lock_owner_mismatch, writer_lock_error) =
            match store.release_writer_lock(base_dir, &project_id, &lease.lease_id) {
                Ok(WriterLockReleaseOutcome::Released) => {
                    writer_lock_released = true;
                    (false, false, None)
                }
                Ok(WriterLockReleaseOutcome::AlreadyAbsent) => {
                    writer_lock_released = false;
                    (true, false, None)
                }
                Ok(WriterLockReleaseOutcome::OwnerMismatch { .. }) => {
                    writer_lock_released = false;
                    (false, true, None)
                }
                Err(e) => {
                    writer_lock_released = false;
                    (false, false, Some(e.to_string()))
                }
            };

        // PHASE 3: Lease-file deletion — only attempted when ALL prior
        // sub-steps positively succeeded (worktree removed AND writer
        // lock released). If the worktree was only AlreadyAbsent or
        // the lock was not released, the lease file stays durable so
        // callers can report the incomplete cleanup and subsequent
        // lookups via find_lease_for_task() still discover the lease.
        let (lease_file_already_absent, lease_file_error) =
            if writer_lock_released && !worktree_already_absent {
                match store.remove_lease(base_dir, &lease.lease_id) {
                    Ok(ResourceCleanupOutcome::Removed) => (false, None),
                    Ok(ResourceCleanupOutcome::AlreadyAbsent) => (true, None),
                    Err(e) => (false, Some(e.to_string())),
                }
            } else {
                // Lease file preserved — prior sub-steps did not all
                // positively succeed (worktree absent or lock not released).
                (false, None)
            };

        // Determine whether cleanup succeeded. True only when all three
        // sub-steps positively succeeded, regardless of ReleaseMode.
        // Idempotent vs Strict only affects error reporting, not the
        // resources_released flag.
        let resources_released = !worktree_already_absent
            && writer_lock_released
            && !lease_file_already_absent
            && lease_file_error.is_none();
        let _ = mode; // mode governs caller error reporting, not this flag

        // Only emit LeaseReleased when all sub-steps succeeded. Partial
        // cleanup must not record a release event — the lease state remains
        // visible for operator recovery.
        let journal_error = if resources_released {
            DaemonTaskService::append_journal_event(
                store,
                base_dir,
                crate::contexts::automation_runtime::model::DaemonJournalEventType::LeaseReleased,
                json!({
                    "task_id": lease.task_id,
                    "lease_id": lease.lease_id,
                    "project_id": lease.project_id,
                }),
            )
            .err()
            .map(|e| e.to_string())
        } else {
            None
        };

        Ok(ReleaseResult {
            resources_released,
            journal_error,
            worktree_already_absent,
            worktree_error: None,
            lease_file_already_absent,
            writer_lock_already_absent,
            writer_lock_owner_mismatch,
            lease_file_error,
            writer_lock_error,
        })
    }

    pub fn find_lease_for_task(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        task_id: &str,
    ) -> AppResult<Option<WorktreeLease>> {
        Ok(store
            .list_leases(base_dir)?
            .into_iter()
            .find(|lease| lease.task_id == task_id))
    }

    pub fn reconcile(
        store: &dyn DaemonStorePort,
        worktree: &dyn WorktreePort,
        base_dir: &Path,
        repo_root: &Path,
        ttl_override_seconds: Option<u64>,
        now: DateTime<Utc>,
    ) -> AppResult<ReconcileReport> {
        let mut report = ReconcileReport::default();
        let leases = store.list_leases(base_dir)?;

        for lease in leases {
            let is_stale = ttl_override_seconds
                .map(|ttl| now > saturating_heartbeat_deadline(lease.last_heartbeat, ttl))
                .unwrap_or_else(|| lease.is_stale_at(now));
            if !is_stale {
                continue;
            }

            report.stale_lease_ids.push(lease.lease_id.clone());
            let task = store.read_task(base_dir, &lease.task_id)?;
            if matches!(task.status, TaskStatus::Claimed | TaskStatus::Active) {
                DaemonTaskService::mark_failed(
                    store,
                    base_dir,
                    &task.task_id,
                    "reconciliation_timeout",
                    "stale lease heartbeat exceeded ttl",
                )?;
                report.failed_task_ids.push(task.task_id.clone());
            }

            // Reconcile enforces strict cleanup: a stale lease whose worktree is
            // already absent cannot be positively cleaned up.  Leave the durable
            // lease state visible for operator recovery instead of silently clearing
            // it.  This distinguishes "already absent" from "removed successfully".
            if !lease.worktree_path.exists() {
                report.cleanup_failures.push(LeaseCleanupFailure {
                    lease_id: lease.lease_id.clone(),
                    task_id: Some(task.task_id.clone()),
                    details: format!(
                        "worktree_absent: referenced worktree path '{}' does not exist",
                        lease.worktree_path.display()
                    ),
                });
                continue;
            }

            // Preserve checkpoint commits for failed tasks before cleanup.
            // The task was either already Failed/Aborted or was just marked
            // Failed above due to stale heartbeat.
            let task = store.read_task(base_dir, &lease.task_id)?;
            if task.status == TaskStatus::Failed {
                try_preserve_failed_branch(worktree, repo_root, &lease);
            }

            // Release order: worktree removal → writer-lock release → lease-file
            // deletion → journal. The lease file is preserved when writer-lock
            // release fails so the durable record remains visible for recovery.
            // If physical release fails (e.g. worktree removal), the lease remains
            // durable for a later reconcile pass. The task is already terminal.
            let release_result = Self::release(
                store,
                worktree,
                base_dir,
                repo_root,
                &lease,
                ReleaseMode::Strict,
            );
            match release_result {
                Ok(outcome) => {
                    // Check for sub-step anomalies: resources that were already
                    // absent cannot be positively cleaned up, and real I/O errors
                    // on sub-steps are recorded with the specific step name.
                    let mut has_sub_step_failure = false;

                    if let Some(ref err) = outcome.worktree_error {
                        report.cleanup_failures.push(LeaseCleanupFailure {
                            lease_id: lease.lease_id.clone(),
                            task_id: Some(task.task_id.clone()),
                            details: format!("worktree_remove: {err}"),
                        });
                        has_sub_step_failure = true;
                    }
                    if outcome.worktree_already_absent {
                        report.cleanup_failures.push(LeaseCleanupFailure {
                            lease_id: lease.lease_id.clone(),
                            task_id: Some(task.task_id.clone()),
                            details: "worktree_absent_during_release: worktree disappeared between pre-check and release".to_owned(),
                        });
                        has_sub_step_failure = true;
                    }
                    if outcome.lease_file_already_absent {
                        report.cleanup_failures.push(LeaseCleanupFailure {
                            lease_id: lease.lease_id.clone(),
                            task_id: Some(task.task_id.clone()),
                            details:
                                "lease_file_absent: lease file was already missing during cleanup"
                                    .to_owned(),
                        });
                        has_sub_step_failure = true;
                    }
                    if let Some(ref err) = outcome.lease_file_error {
                        report.cleanup_failures.push(LeaseCleanupFailure {
                            lease_id: lease.lease_id.clone(),
                            task_id: Some(task.task_id.clone()),
                            details: format!("lease_file_delete: {err}"),
                        });
                        has_sub_step_failure = true;
                    }
                    if outcome.writer_lock_already_absent {
                        report.cleanup_failures.push(LeaseCleanupFailure {
                            lease_id: lease.lease_id.clone(),
                            task_id: Some(task.task_id.clone()),
                            details:
                                "writer_lock_absent: writer lock was already missing during cleanup"
                                    .to_owned(),
                        });
                        has_sub_step_failure = true;
                    }
                    if outcome.writer_lock_owner_mismatch {
                        report.cleanup_failures.push(LeaseCleanupFailure {
                            lease_id: lease.lease_id.clone(),
                            task_id: Some(task.task_id.clone()),
                            details:
                                "writer_lock_owner_mismatch: lock is owned by a different writer"
                                    .to_owned(),
                        });
                        has_sub_step_failure = true;
                    }
                    if let Some(ref err) = outcome.writer_lock_error {
                        report.cleanup_failures.push(LeaseCleanupFailure {
                            lease_id: lease.lease_id.clone(),
                            task_id: Some(task.task_id.clone()),
                            details: format!("writer_lock_release: {err}"),
                        });
                        has_sub_step_failure = true;
                    }

                    if !has_sub_step_failure {
                        // All physical sub-steps positively succeeded — clear
                        // the task's lease reference.
                        match DaemonTaskService::clear_lease_reference(
                            store,
                            base_dir,
                            &task.task_id,
                        ) {
                            Ok(_) => {
                                report.released_lease_ids.push(lease.lease_id.clone());
                                report
                                    .removed_worktrees
                                    .push(lease.worktree_path.display().to_string());
                            }
                            Err(e) => {
                                // Lease removed but task reference not cleared — do NOT
                                // report as released; the task remains visibly inconsistent
                                // for operator repair.
                                report.cleanup_failures.push(LeaseCleanupFailure {
                                    lease_id: lease.lease_id.clone(),
                                    task_id: Some(task.task_id.clone()),
                                    details: format!("clear_lease_ref: {e}"),
                                });
                            }
                        }
                    }
                    // else: sub-step failure — do NOT count as released; leave task
                    // lease reference intact so inconsistent state stays visible for
                    // operator recovery.

                    if let Some(je) = outcome.journal_error {
                        report.cleanup_failures.push(LeaseCleanupFailure {
                            lease_id: lease.lease_id.clone(),
                            task_id: Some(task.task_id.clone()),
                            details: format!("release_journal: {je}"),
                        });
                    }
                }
                Err(e) => {
                    // Release setup/validation failure (e.g. corrupt project_id),
                    // not physical worktree removal — lease remains durable and
                    // the task remains terminal but recoverable for later.
                    report.cleanup_failures.push(LeaseCleanupFailure {
                        lease_id: lease.lease_id.clone(),
                        task_id: Some(task.task_id.clone()),
                        details: format!("release_setup: {e}"),
                    });
                }
            }
        }

        // --- Pass 2: stale CLI writer leases ---
        // CLI leases have no associated task or worktree. Cleanup order:
        // 1. Validate project_id (before any side effects).
        // 2. Owner-aware writer-lock release (before lease deletion).
        // 3. Delete CLI lease record only after lock release succeeds.
        let all_records = store.list_lease_records(base_dir)?;
        for record in all_records {
            let cli_lease = match record {
                LeaseRecord::CliWriter(ref cli) => cli,
                LeaseRecord::Worktree(_) => continue,
            };

            let is_stale = ttl_override_seconds
                .map(|ttl| now > saturating_heartbeat_deadline(cli_lease.last_heartbeat, ttl))
                .unwrap_or_else(|| cli_lease.is_stale_at(now));
            if !is_stale {
                continue;
            }

            report.stale_lease_ids.push(cli_lease.lease_id.clone());

            // No task to mark failed — CLI leases are task-independent.

            // Validate project_id before any cleanup side effects.
            let project_id = match ProjectId::new(cli_lease.project_id.clone()) {
                Ok(pid) => pid,
                Err(e) => {
                    report.cleanup_failures.push(LeaseCleanupFailure {
                        lease_id: cli_lease.lease_id.clone(),
                        task_id: None,
                        details: format!("invalid_project_id: {e}"),
                    });
                    continue;
                }
            };

            // Sub-step 1: owner-aware writer-lock release BEFORE lease deletion.
            let mut has_sub_step_failure = false;
            // Track whether the lock was positively released vs already absent.
            // When the lock is already absent the cleanup is still a failure,
            // but we still attempt lease-file deletion to prune the stale
            // record so later reconcile runs do not rediscover it.
            let mut writer_lock_absent = false;
            match store.release_writer_lock(base_dir, &project_id, &cli_lease.lease_id) {
                Ok(WriterLockReleaseOutcome::Released) => {}
                Ok(WriterLockReleaseOutcome::AlreadyAbsent) => {
                    report.cleanup_failures.push(LeaseCleanupFailure {
                        lease_id: cli_lease.lease_id.clone(),
                        task_id: None,
                        details:
                            "writer_lock_absent: writer lock was already missing during cleanup"
                                .to_owned(),
                    });
                    has_sub_step_failure = true;
                    writer_lock_absent = true;
                }
                Ok(WriterLockReleaseOutcome::OwnerMismatch { actual_owner }) => {
                    report.cleanup_failures.push(LeaseCleanupFailure {
                        lease_id: cli_lease.lease_id.clone(),
                        task_id: None,
                        details: format!(
                            "writer_lock_owner_mismatch: expected '{}', found '{actual_owner}'",
                            cli_lease.lease_id
                        ),
                    });
                    has_sub_step_failure = true;
                }
                Err(e) => {
                    report.cleanup_failures.push(LeaseCleanupFailure {
                        lease_id: cli_lease.lease_id.clone(),
                        task_id: None,
                        details: format!("writer_lock_release: {e}"),
                    });
                    has_sub_step_failure = true;
                }
            }

            // If lock release failed for a reason other than already-absent,
            // keep the CLI lease record durable for later reconcile visibility.
            // When the lock was already absent, proceed to lease-file deletion
            // to prune the stale record.
            if has_sub_step_failure && !writer_lock_absent {
                continue;
            }

            // Sub-step 2: delete CLI lease record. Attempted after positive
            // lock release OR after writer_lock_absent (to prune the stale
            // record). In the writer_lock_absent case the overall pass remains
            // a cleanup failure regardless of deletion outcome.
            match store.remove_lease(base_dir, &cli_lease.lease_id) {
                Ok(ResourceCleanupOutcome::Removed) => {}
                Ok(ResourceCleanupOutcome::AlreadyAbsent) => {
                    report.cleanup_failures.push(LeaseCleanupFailure {
                        lease_id: cli_lease.lease_id.clone(),
                        task_id: None,
                        details: "lease_file_absent: lease file was already missing during cleanup"
                            .to_owned(),
                    });
                    has_sub_step_failure = true;
                }
                Err(e) => {
                    report.cleanup_failures.push(LeaseCleanupFailure {
                        lease_id: cli_lease.lease_id.clone(),
                        task_id: None,
                        details: format!("lease_file_delete: {e}"),
                    });
                    has_sub_step_failure = true;
                }
            }

            if !has_sub_step_failure {
                report.released_lease_ids.push(cli_lease.lease_id.clone());
            }
        }

        DaemonTaskService::append_journal_event(
            store,
            base_dir,
            crate::contexts::automation_runtime::model::DaemonJournalEventType::ReconciliationRun,
            json!({
                "stale_lease_ids": report.stale_lease_ids,
                "failed_task_ids": report.failed_task_ids,
                "released_lease_ids": report.released_lease_ids,
            }),
        )?;

        Ok(report)
    }
}
