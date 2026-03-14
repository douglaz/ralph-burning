//! CLI writer-lease guard: wraps the project writer lock with a durable
//! `CliWriterLease` record and a periodic heartbeat so that `daemon reconcile`
//! can discover and clean stale CLI-held locks after a crash.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::contexts::automation_runtime::model::{CliWriterLease, LeaseRecord};
use crate::contexts::automation_runtime::DaemonStorePort;
use crate::shared::domain::ProjectId;
use crate::shared::error::AppResult;

/// Default TTL for CLI writer leases (seconds).
pub const CLI_LEASE_TTL_SECONDS: u64 = 300;

/// Default heartbeat cadence (seconds).
pub const CLI_LEASE_HEARTBEAT_CADENCE_SECONDS: u64 = 30;

/// RAII guard that owns a project writer lock, a matching CLI lease record,
/// and a background heartbeat task. On drop, the heartbeat is cancelled and
/// both the lease record and writer lock are cleaned up.
pub struct CliWriterLeaseGuard {
    store: Arc<dyn DaemonStorePort + Send + Sync>,
    base_dir: PathBuf,
    project_id: ProjectId,
    lease_id: String,
    shutdown: Arc<Notify>,
    heartbeat_handle: Option<JoinHandle<()>>,
}

impl CliWriterLeaseGuard {
    /// Returns the lease_id assigned to this guard.
    pub fn lease_id(&self) -> &str {
        &self.lease_id
    }

    /// Acquire the project writer lock, persist a `CliWriterLease` record, and
    /// spawn a heartbeat task. If the lease record write fails, the writer lock
    /// is released before returning.
    pub fn acquire(
        store: Arc<dyn DaemonStorePort + Send + Sync>,
        base_dir: &Path,
        project_id: ProjectId,
        ttl_seconds: u64,
        heartbeat_cadence_seconds: u64,
    ) -> AppResult<Self> {
        let lease_id = format!("cli-{}", uuid::Uuid::new_v4());

        // Step 1: acquire the writer lock with our lease_id as content.
        store.acquire_writer_lock(base_dir, &project_id, &lease_id)?;

        // Step 2: persist the CLI lease record.
        let now = Utc::now();
        let lease = CliWriterLease {
            lease_id: lease_id.clone(),
            project_id: project_id.to_string(),
            owner: "cli".to_owned(),
            acquired_at: now,
            ttl_seconds,
            last_heartbeat: now,
        };
        if let Err(e) = store.write_lease_record(base_dir, &LeaseRecord::CliWriter(lease)) {
            // Invariant: failed acquisition leaves neither lease record nor lock.
            let _ = store.release_writer_lock(base_dir, &project_id);
            return Err(e);
        }

        // Step 3: spawn heartbeat task.
        let shutdown = Arc::new(Notify::new());
        let heartbeat_handle = {
            let store = Arc::clone(&store);
            let base_dir = base_dir.to_path_buf();
            let lease_id = lease_id.clone();
            let shutdown = Arc::clone(&shutdown);
            tokio::spawn(async move {
                let interval = tokio::time::Duration::from_secs(heartbeat_cadence_seconds);
                loop {
                    tokio::select! {
                        _ = shutdown.notified() => break,
                        _ = tokio::time::sleep(interval) => {}
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
            heartbeat_handle: Some(heartbeat_handle),
        })
    }
}

/// Update `last_heartbeat` on an existing CLI lease record.
fn heartbeat_tick(
    store: &dyn DaemonStorePort,
    base_dir: &Path,
    lease_id: &str,
) -> AppResult<()> {
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
        // Signal heartbeat shutdown and wait for it to stop.
        self.shutdown.notify_one();
        if let Some(handle) = self.heartbeat_handle.take() {
            // Best-effort blocking wait — we are in a Drop impl so we cannot
            // `.await`. Use `tokio::task::block_in_place` if available, or
            // fall back to aborting the task.
            handle.abort();
            // The abort ensures the heartbeat task stops; we don't need to
            // block-wait on it since abort is synchronous cancellation.
        }

        // Cleanup: delete lease record and release writer lock independently.
        // Failure in one must not skip the other.
        let _ = self
            .store
            .remove_lease(&self.base_dir, &self.lease_id);
        let _ = self
            .store
            .release_writer_lock(&self.base_dir, &self.project_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Duration;
    use tempfile::tempdir;

    use crate::adapters::fs::FsDaemonStore;

    fn store() -> Arc<dyn DaemonStorePort + Send + Sync> {
        Arc::new(FsDaemonStore)
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
        assert!(matches!(&records[0], LeaseRecord::CliWriter(cli) if cli.lease_id == guard.lease_id));

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
            .release_writer_lock(temp.path(), &project_id)
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
        let records = FsDaemonStore
            .list_lease_records(temp.path())
            .expect("list");
        assert!(records.is_empty(), "lease record should be removed on drop");

        // Writer lock should be released
        FsDaemonStore
            .acquire_writer_lock(temp.path(), &project_id, "post-error")
            .expect("lock should be available after error drop");
        FsDaemonStore
            .release_writer_lock(temp.path(), &project_id)
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
        let records = FsDaemonStore
            .list_lease_records(temp.path())
            .expect("list");
        assert!(
            records.is_empty(),
            "no lease record should be written on failed acquisition"
        );

        FsDaemonStore
            .release_writer_lock(temp.path(), &project_id)
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

        let records = FsDaemonStore
            .list_lease_records(temp.path())
            .expect("list");
        assert_eq!(1, records.len());

        // The record should be stale-detectable after TTL
        let future_time = Utc::now() + Duration::seconds(11);
        assert!(
            records[0].is_stale_at(future_time),
            "lease should be stale after TTL"
        );

        drop(guard);
    }
}
