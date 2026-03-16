use clap::{Args, Subcommand};

use crate::adapters::fs::{
    FsAmendmentQueueStore, FsArtifactStore, FsDaemonStore, FsJournalStore,
    FsPayloadArtifactWriteStore, FsProjectStore, FsRequirementsStore, FsRunSnapshotStore,
    FsRunSnapshotWriteStore, FsRuntimeLogWriteStore,
};
use crate::adapters::issue_watcher::FileIssueWatcher;
use crate::adapters::worktree::WorktreeAdapter;
use crate::contexts::automation_runtime::daemon_loop::{DaemonLoop, DaemonLoopConfig};
use crate::contexts::automation_runtime::lease_service::{LeaseService, ReleaseMode};
use crate::contexts::automation_runtime::model::TaskStatus;
use crate::contexts::automation_runtime::task_service::DaemonTaskService;
use crate::contexts::automation_runtime::DaemonStorePort;
use crate::contexts::workspace_governance;
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::shared::error::{AppError, AppResult};

use crate::composition::agent_execution_builder::build_agent_execution_service;

#[derive(Debug, Args)]
pub struct DaemonCommand {
    #[command(subcommand)]
    pub command: DaemonSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum DaemonSubcommand {
    Start {
        #[arg(long, default_value_t = 10)]
        poll_seconds: u64,
        #[arg(long)]
        single_iteration: bool,
    },
    Status,
    Abort {
        task_id: String,
    },
    Retry {
        task_id: String,
    },
    Reconcile {
        #[arg(long)]
        ttl_seconds: Option<u64>,
    },
}

pub async fn handle(command: DaemonCommand) -> AppResult<()> {
    match command.command {
        DaemonSubcommand::Start {
            poll_seconds,
            single_iteration,
        } => handle_start(poll_seconds, single_iteration).await,
        DaemonSubcommand::Status => handle_status().await,
        DaemonSubcommand::Abort { task_id } => handle_abort(&task_id).await,
        DaemonSubcommand::Retry { task_id } => handle_retry(&task_id).await,
        DaemonSubcommand::Reconcile { ttl_seconds } => handle_reconcile(ttl_seconds).await,
    }
}

async fn handle_start(poll_seconds: u64, single_iteration: bool) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;
    let _ = EffectiveConfig::load(&current_dir)?;

    let agent_service = build_agent_execution_service()?;
    let daemon_store = FsDaemonStore;
    let worktree = WorktreeAdapter;
    let project_store = FsProjectStore;
    let run_snapshot_read = FsRunSnapshotStore;
    let run_snapshot_write = FsRunSnapshotWriteStore;
    let journal_store = FsJournalStore;
    let artifact_store = FsArtifactStore;
    let artifact_write = FsPayloadArtifactWriteStore;
    let log_write = FsRuntimeLogWriteStore;
    let amendment_queue = FsAmendmentQueueStore;

    let issue_watcher = FileIssueWatcher;
    let requirements_store = FsRequirementsStore;

    let daemon_loop = DaemonLoop::new(
        &daemon_store,
        &worktree,
        &project_store,
        &run_snapshot_read,
        &run_snapshot_write,
        &journal_store,
        &artifact_store,
        &artifact_write,
        &log_write,
        &amendment_queue,
        &agent_service,
    )
    .with_watcher(&issue_watcher)
    .with_requirements_store(&requirements_store);

    let loop_config = DaemonLoopConfig {
        poll_interval: std::time::Duration::from_secs(poll_seconds),
        single_iteration,
        ..DaemonLoopConfig::default()
    };

    daemon_loop.run(&current_dir, &loop_config).await
}

async fn handle_status() -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let store = FsDaemonStore;
    let tasks = DaemonTaskService::list_tasks(&store, &current_dir)?;
    let leases = store.list_leases(&current_dir)?;

    if tasks.is_empty() {
        println!("No daemon tasks found.");
        return Ok(());
    }

    for task in tasks {
        let lease = leases.iter().find(|lease| lease.task_id == task.task_id);
        let lease_id = lease
            .map(|lease| lease.lease_id.as_str())
            .or(task.lease_id.as_deref())
            .unwrap_or("-");
        let heartbeat = lease
            .map(|lease| lease.last_heartbeat.to_rfc3339())
            .unwrap_or_else(|| "-".to_owned());
        let req_run = task.requirements_run_id.as_deref().unwrap_or("-");
        println!(
            "{}  {}  dispatch={}  lease={}  heartbeat={}  issue={}  requirements_run={}",
            task.task_id,
            task.status,
            task.dispatch_mode,
            lease_id,
            heartbeat,
            task.issue_ref,
            req_run,
        );
    }

    Ok(())
}

async fn handle_abort(task_id: &str) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let store = FsDaemonStore;
    let worktree = WorktreeAdapter;
    let task = store.read_task(&current_dir, task_id)?;
    if task.status.is_terminal() {
        return Err(AppError::TaskStateTransitionInvalid {
            task_id: task.task_id,
            from: task.status.as_str().to_owned(),
            to: TaskStatus::Aborted.as_str().to_owned(),
        });
    }

    let original_status = task.status;
    DaemonTaskService::mark_aborted(&store, &current_dir, task_id)?;

    if matches!(original_status, TaskStatus::Claimed | TaskStatus::Active) {
        cleanup_aborted_task(&store, &worktree, &current_dir, task_id, original_status).await?;
    }

    println!("Aborted task {task_id}");
    Ok(())
}

async fn handle_retry(task_id: &str) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let store = FsDaemonStore;
    let task = DaemonTaskService::retry_task(&store, &current_dir, task_id)?;
    println!(
        "Retried task {} (attempt_count={})",
        task.task_id, task.attempt_count
    );
    Ok(())
}

async fn handle_reconcile(ttl_seconds: Option<u64>) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = workspace_governance::load_workspace_config(&current_dir)?;
    workspace_governance::ensure_supported_workspace_version(&config)?;

    let store = FsDaemonStore;
    let worktree = WorktreeAdapter;
    let report = LeaseService::reconcile(
        &store,
        &worktree,
        &current_dir,
        &current_dir,
        ttl_seconds,
        chrono::Utc::now(),
    )?;

    println!(
        "reconciled stale_leases={} failed_tasks={} released_leases={}",
        report.stale_lease_ids.len(),
        report.failed_task_ids.len(),
        report.released_lease_ids.len()
    );

    if report.has_cleanup_failures() {
        println!("--- Cleanup Failures ---");
        for failure in &report.cleanup_failures {
            println!(
                "  lease={} task={}: {}",
                failure.lease_id,
                failure.task_id.as_deref().unwrap_or("n/a"),
                failure.details
            );
        }
        return Err(AppError::ReconcileCleanupFailed {
            failed_count: report.cleanup_failures.len(),
        });
    }
    Ok(())
}

async fn cleanup_aborted_task(
    store: &dyn DaemonStorePort,
    worktree: &WorktreeAdapter,
    base_dir: &std::path::Path,
    task_id: &str,
    original_status: TaskStatus,
) -> AppResult<()> {
    let grace_deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    let await_daemon_cleanup = original_status == TaskStatus::Active;

    loop {
        let lease = LeaseService::find_lease_for_task(store, base_dir, task_id)?;
        let Some(lease) = lease else {
            let task = store.read_task(base_dir, task_id)?;
            if task.lease_id.is_some() {
                DaemonTaskService::clear_lease_reference(store, base_dir, task_id)?;
            }
            return Ok(());
        };

        if await_daemon_cleanup && std::time::Instant::now() < grace_deadline {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        }

        let release_result = LeaseService::release(
            store,
            worktree,
            base_dir,
            base_dir,
            &lease,
            ReleaseMode::Idempotent,
        );
        return match release_result {
            Ok(ref r) if r.resources_released => {
                // All sub-steps succeeded — safe to clear durable lease reference.
                DaemonTaskService::clear_lease_reference(store, base_dir, task_id).map(|_| ())
            }
            Ok(_) => {
                // Partial cleanup: some resources remain. Do NOT clear lease
                // reference so inconsistent state stays visible for operator
                // recovery.
                Err(AppError::LeaseCleanupPartialFailure {
                    task_id: task_id.to_owned(),
                })
            }
            Err(error) => Err(error),
        };
    }
}
