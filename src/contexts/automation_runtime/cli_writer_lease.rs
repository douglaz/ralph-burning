//! CLI writer-lease guard: wraps the project writer lock with a durable
//! `CliWriterLease` record and a periodic heartbeat so that `daemon reconcile`
//! can discover and clean stale CLI-held locks after a crash.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::contexts::automation_runtime::model::{CliWriterLease, LeaseRecord};
use crate::contexts::automation_runtime::{
    DaemonStorePort, ResourceCleanupOutcome, WriterLockReleaseOutcome,
};
use crate::shared::domain::ProjectId;
use crate::shared::error::{AppError, AppResult};

/// Default TTL for CLI writer leases (seconds).
pub const CLI_LEASE_TTL_SECONDS: u64 = 300;

/// Default heartbeat cadence (seconds).
pub const CLI_LEASE_HEARTBEAT_CADENCE_SECONDS: u64 = 30;

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

fn read_writer_lock_owner(base_dir: &Path, project_id: &ProjectId) -> AppResult<Option<String>> {
    match fs::read_to_string(writer_lock_path(base_dir, project_id)) {
        Ok(owner) => Ok(Some(owner)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
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

    let mut expected_owner = read_writer_lock_owner(base_dir, project_id)?;
    let mut attempts = 0usize;
    while let Some(owner) = expected_owner.take() {
        attempts += 1;
        match store.release_writer_lock(base_dir, project_id, &owner)? {
            WriterLockReleaseOutcome::Released | WriterLockReleaseOutcome::AlreadyAbsent => break,
            WriterLockReleaseOutcome::OwnerMismatch { actual_owner } => {
                if attempts >= 3 {
                    return Err(AppError::Io(std::io::Error::other(format!(
                        "stale writer lock for project '{}' changed ownership during recovery; final owner '{actual_owner}'",
                        project_id
                    ))));
                }
                expected_owner = Some(actual_owner);
            }
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
}
