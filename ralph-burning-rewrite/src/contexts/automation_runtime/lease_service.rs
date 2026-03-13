use std::path::Path;

use chrono::{DateTime, Utc};
use serde_json::json;

use crate::contexts::automation_runtime::model::{TaskStatus, WorktreeLease};
use crate::contexts::automation_runtime::task_service::DaemonTaskService;
use crate::contexts::automation_runtime::{
    DaemonStorePort, ResourceCleanupOutcome, WorktreeCleanupOutcome, WorktreePort,
};
use crate::shared::domain::ProjectId;
use crate::shared::error::{AppError, AppResult};

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
    pub task_id: String,
    pub details: String,
}

/// Result of a lease release operation. Distinguishes physical cleanup
/// (worktree, lease file, writer lock) from post-cleanup journal append,
/// and tracks per-sub-step outcomes so callers can enforce strict cleanup
/// accounting.
#[derive(Debug, Clone)]
pub struct ReleaseResult {
    /// True only when every physical cleanup sub-step positively succeeded:
    /// worktree removed (not already absent), lease file deleted (not already
    /// absent, no error), and writer lock released (not already absent, no
    /// error). Callers must only clear durable lease references when this is
    /// true.
    pub resources_released: bool,
    /// If the LeaseReleased journal append failed after physical cleanup,
    /// this contains the error description. Only set when `resources_released`
    /// is true, since journal append is skipped on partial failure.
    pub journal_error: Option<String>,
    /// Whether the worktree was already absent when removal was attempted.
    pub worktree_already_absent: bool,
    /// Whether the lease file was already absent when deletion was attempted.
    pub lease_file_already_absent: bool,
    /// Whether the writer lock was already absent when release was attempted.
    pub writer_lock_already_absent: bool,
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
    pub fn acquire(
        store: &dyn DaemonStorePort,
        worktree: &dyn WorktreePort,
        base_dir: &Path,
        repo_root: &Path,
        task_id: &str,
        project_id: &ProjectId,
        ttl_seconds: u64,
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

        let worktree_path = worktree.worktree_path(base_dir, task_id);
        let branch_name = worktree.branch_name(task_id);
        if let Err(error) =
            worktree.create_worktree(repo_root, &worktree_path, &branch_name, task_id)
        {
            let _ = store.release_writer_lock(base_dir, project_id);
            return Err(error);
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
            let _ = worktree.remove_worktree(repo_root, &lease.worktree_path, task_id);
            let _ = store.release_writer_lock(base_dir, project_id);
            return Err(error);
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
    ) -> AppResult<ReleaseResult> {
        let project_id = ProjectId::new(lease.project_id.clone())?;

        // Attempt worktree removal first. If it fails, keep all durable lease
        // state (lease file, writer lock) intact so a later reconcile can retry.
        let worktree_outcome =
            worktree.remove_worktree(repo_root, &lease.worktree_path, &lease.task_id)?;
        let worktree_already_absent = worktree_outcome == WorktreeCleanupOutcome::AlreadyAbsent;

        // Worktree removal returned Ok — proceed with lease file + lock cleanup.
        // Capture each sub-step outcome individually instead of propagating
        // errors immediately, so callers can report the specific failing step.
        let (lease_file_already_absent, lease_file_error) =
            match store.remove_lease(base_dir, &lease.lease_id) {
                Ok(ResourceCleanupOutcome::Removed) => (false, None),
                Ok(ResourceCleanupOutcome::AlreadyAbsent) => (true, None),
                Err(e) => (false, Some(e.to_string())),
            };

        // Always attempt writer-lock release even if lease-file deletion failed,
        // so the operator sees the full picture of what succeeded and what didn't.
        let (writer_lock_already_absent, writer_lock_error) =
            match store.release_writer_lock(base_dir, &project_id) {
                Ok(ResourceCleanupOutcome::Removed) => (false, None),
                Ok(ResourceCleanupOutcome::AlreadyAbsent) => (true, None),
                Err(e) => (false, Some(e.to_string())),
            };

        // Determine whether every sub-step positively succeeded. Only then
        // is the lease considered fully released and safe for callers to clear
        // durable references.
        let resources_released = !worktree_already_absent
            && !lease_file_already_absent
            && !writer_lock_already_absent
            && lease_file_error.is_none()
            && writer_lock_error.is_none();

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
            lease_file_already_absent,
            writer_lock_already_absent,
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
                .map(|ttl| now > lease.last_heartbeat + chrono::Duration::seconds(ttl as i64))
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
                    task_id: task.task_id.clone(),
                    details: format!(
                        "worktree_absent: referenced worktree path '{}' does not exist",
                        lease.worktree_path.display()
                    ),
                });
                continue;
            }

            // Release order: worktree removal → lease file → writer lock → journal.
            // If physical release fails (e.g. worktree removal), the lease remains
            // durable for a later reconcile pass. The task is already terminal.
            let release_result = Self::release(store, worktree, base_dir, repo_root, &lease);
            match release_result {
                Ok(outcome) => {
                    // Check for sub-step anomalies: resources that were already
                    // absent cannot be positively cleaned up, and real I/O errors
                    // on sub-steps are recorded with the specific step name.
                    // Also check worktree_already_absent to handle the race where
                    // the worktree disappears between the pre-check and release().
                    let mut has_sub_step_failure = false;

                    if outcome.worktree_already_absent {
                        report.cleanup_failures.push(LeaseCleanupFailure {
                            lease_id: lease.lease_id.clone(),
                            task_id: task.task_id.clone(),
                            details: "worktree_absent_during_release: worktree disappeared between pre-check and release".to_owned(),
                        });
                        has_sub_step_failure = true;
                    }
                    if outcome.lease_file_already_absent {
                        report.cleanup_failures.push(LeaseCleanupFailure {
                            lease_id: lease.lease_id.clone(),
                            task_id: task.task_id.clone(),
                            details: "lease_file_absent: lease file was already missing during cleanup".to_owned(),
                        });
                        has_sub_step_failure = true;
                    }
                    if let Some(ref err) = outcome.lease_file_error {
                        report.cleanup_failures.push(LeaseCleanupFailure {
                            lease_id: lease.lease_id.clone(),
                            task_id: task.task_id.clone(),
                            details: format!("lease_file_delete: {err}"),
                        });
                        has_sub_step_failure = true;
                    }
                    if outcome.writer_lock_already_absent {
                        report.cleanup_failures.push(LeaseCleanupFailure {
                            lease_id: lease.lease_id.clone(),
                            task_id: task.task_id.clone(),
                            details: "writer_lock_absent: writer lock was already missing during cleanup".to_owned(),
                        });
                        has_sub_step_failure = true;
                    }
                    if let Some(ref err) = outcome.writer_lock_error {
                        report.cleanup_failures.push(LeaseCleanupFailure {
                            lease_id: lease.lease_id.clone(),
                            task_id: task.task_id.clone(),
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
                                    task_id: task.task_id.clone(),
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
                            task_id: task.task_id.clone(),
                            details: format!("release_journal: {je}"),
                        });
                    }
                }
                Err(e) => {
                    // Physical release failed (e.g. worktree removal) — lease remains
                    // durable and the task remains terminal but recoverable for later.
                    report.cleanup_failures.push(LeaseCleanupFailure {
                        lease_id: lease.lease_id.clone(),
                        task_id: task.task_id.clone(),
                        details: format!("worktree_remove: {e}"),
                    });
                }
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
