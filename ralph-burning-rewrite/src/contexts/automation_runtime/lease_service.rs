use std::path::Path;

use chrono::{DateTime, Utc};
use serde_json::json;

use crate::contexts::automation_runtime::model::{TaskStatus, WorktreeLease};
use crate::contexts::automation_runtime::task_service::DaemonTaskService;
use crate::contexts::automation_runtime::{DaemonStorePort, WorktreePort};
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
    ) -> AppResult<()> {
        let project_id = ProjectId::new(lease.project_id.clone())?;
        let remove_error = worktree
            .remove_worktree(repo_root, &lease.worktree_path, &lease.task_id)
            .err();
        store.remove_lease(base_dir, &lease.lease_id)?;
        store.release_writer_lock(base_dir, &project_id)?;
        DaemonTaskService::append_journal_event(
            store,
            base_dir,
            crate::contexts::automation_runtime::model::DaemonJournalEventType::LeaseReleased,
            json!({
                "task_id": lease.task_id,
                "lease_id": lease.lease_id,
                "project_id": lease.project_id,
            }),
        )?;

        if let Some(error) = remove_error {
            return Err(error);
        }

        Ok(())
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

            let release_result = Self::release(store, worktree, base_dir, repo_root, &lease);
            let clear_result =
                DaemonTaskService::clear_lease_reference(store, base_dir, &task.task_id);

            match (&release_result, &clear_result) {
                (Ok(()), Ok(_)) => {
                    report.released_lease_ids.push(lease.lease_id.clone());
                    report
                        .removed_worktrees
                        .push(lease.worktree_path.display().to_string());
                }
                _ => {
                    let mut details = Vec::new();
                    if let Err(ref e) = release_result {
                        details.push(format!("release: {e}"));
                    }
                    if let Err(ref e) = clear_result {
                        details.push(format!("clear_lease_ref: {e}"));
                    }
                    report.cleanup_failures.push(LeaseCleanupFailure {
                        lease_id: lease.lease_id.clone(),
                        task_id: task.task_id.clone(),
                        details: details.join("; "),
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
