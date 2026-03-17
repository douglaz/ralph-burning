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
use crate::contexts::automation_runtime::repo_registry::{self, DataDirLayout};
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
    /// Start the daemon loop. When --data-dir and --repo are provided, runs
    /// in multi-repo GitHub mode. Otherwise falls back to single-repo
    /// current-directory mode (file-watcher intake).
    Start {
        #[arg(long, default_value_t = 10)]
        poll_seconds: u64,
        #[arg(long)]
        single_iteration: bool,
        /// Root directory for multi-repo daemon state.
        #[arg(long)]
        data_dir: Option<String>,
        /// Repos to manage in owner/repo form. May be repeated.
        #[arg(long = "repo")]
        repos: Vec<String>,
        /// Enable verbose logging.
        #[arg(long)]
        verbose: bool,
    },
    /// Show status of daemon tasks.
    Status {
        /// Root directory for multi-repo daemon state.
        #[arg(long)]
        data_dir: Option<String>,
        /// Filter status to specific repos.
        #[arg(long = "repo")]
        repos: Vec<String>,
    },
    /// Abort a task by issue number (multi-repo) or task ID (single-repo).
    Abort {
        /// Issue number or task ID.
        identifier: String,
        /// Root directory for multi-repo daemon state.
        #[arg(long)]
        data_dir: Option<String>,
        /// Repo slug for issue-number resolution.
        #[arg(long = "repo")]
        repo: Option<String>,
    },
    /// Retry a failed task by issue number (multi-repo) or task ID (single-repo).
    Retry {
        /// Issue number or task ID.
        identifier: String,
        /// Root directory for multi-repo daemon state.
        #[arg(long)]
        data_dir: Option<String>,
        /// Repo slug for issue-number resolution.
        #[arg(long = "repo")]
        repo: Option<String>,
    },
    /// Reconcile stale leases across all repos.
    Reconcile {
        /// Root directory for multi-repo daemon state.
        #[arg(long)]
        data_dir: Option<String>,
        #[arg(long)]
        ttl_seconds: Option<u64>,
    },
}

pub async fn handle(command: DaemonCommand) -> AppResult<()> {
    match command.command {
        DaemonSubcommand::Start {
            poll_seconds,
            single_iteration,
            data_dir,
            repos,
            verbose,
        } => {
            if let Some(ref dd) = data_dir {
                handle_start_multi_repo(
                    dd,
                    &repos,
                    poll_seconds,
                    single_iteration,
                    verbose,
                )
                .await
            } else {
                handle_start_legacy(poll_seconds, single_iteration).await
            }
        }
        DaemonSubcommand::Status { data_dir, repos } => {
            if let Some(ref dd) = data_dir {
                handle_status_multi_repo(dd, &repos).await
            } else {
                handle_status_legacy().await
            }
        }
        DaemonSubcommand::Abort {
            identifier,
            data_dir,
            repo,
        } => {
            if let Some(ref dd) = data_dir {
                let repo_slug = repo.as_deref().ok_or_else(|| AppError::InvalidConfigValue {
                    key: "repo".to_owned(),
                    value: String::new(),
                    reason: "--repo is required with --data-dir for abort".to_owned(),
                })?;
                handle_abort_by_issue(dd, repo_slug, &identifier).await
            } else {
                handle_abort_legacy(&identifier).await
            }
        }
        DaemonSubcommand::Retry {
            identifier,
            data_dir,
            repo,
        } => {
            if let Some(ref dd) = data_dir {
                let repo_slug = repo.as_deref().ok_or_else(|| AppError::InvalidConfigValue {
                    key: "repo".to_owned(),
                    value: String::new(),
                    reason: "--repo is required with --data-dir for retry".to_owned(),
                })?;
                handle_retry_by_issue(dd, repo_slug, &identifier).await
            } else {
                handle_retry_legacy(&identifier).await
            }
        }
        DaemonSubcommand::Reconcile {
            data_dir,
            ttl_seconds,
        } => {
            if let Some(ref dd) = data_dir {
                handle_reconcile_multi_repo(dd, ttl_seconds).await
            } else {
                handle_reconcile_legacy(ttl_seconds).await
            }
        }
    }
}

// ===========================================================================
// Multi-repo (--data-dir) handlers
// ===========================================================================

async fn handle_start_multi_repo(
    data_dir: &str,
    repos: &[String],
    poll_seconds: u64,
    single_iteration: bool,
    verbose: bool,
) -> AppResult<()> {
    let data_dir_path = std::path::Path::new(data_dir);
    repo_registry::validate_data_dir(data_dir_path)?;

    if repos.is_empty() {
        return Err(AppError::InvalidConfigValue {
            key: "repo".to_owned(),
            value: String::new(),
            reason: "at least one --repo is required with --data-dir".to_owned(),
        });
    }

    // Validate and register all repos upfront
    let mut registrations = Vec::new();
    for slug in repos {
        let reg = repo_registry::register_repo(data_dir_path, slug)?;
        // Validate checkout if it exists
        if reg.repo_root.is_dir() {
            if let Err(e) = repo_registry::validate_repo_checkout(&reg.repo_root) {
                return Err(AppError::InvalidConfigValue {
                    key: "repo".to_owned(),
                    value: slug.clone(),
                    reason: format!("repo validation failed: {e}"),
                });
            }
        }
        registrations.push(reg);
    }

    if verbose {
        println!(
            "daemon: starting with data-dir={} repos={:?}",
            data_dir,
            repos
        );
    }

    // For now, multi-repo start validates and sets up the data-dir layout.
    // Full GitHub intake polling is wired through the daemon loop's
    // `with_registrations` path — the loop iterates registered repos
    // each cycle. This handler sets up the infrastructure and delegates
    // to the existing daemon loop with file-watcher disabled.
    let agent_service = build_agent_execution_service()?;

    // Use the first repo's checkout as the base_dir for the daemon loop.
    // Each repo gets its own daemon state under data-dir.
    let base_dir = registrations[0].repo_root.clone();

    // Ensure workspace exists in the checkout
    if base_dir.is_dir() {
        let _ = workspace_governance::load_workspace_config(&base_dir);
    }

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
    .with_requirements_store(&requirements_store)
    .with_registrations(registrations)
    .with_data_dir(data_dir_path.to_owned());

    let loop_config = DaemonLoopConfig {
        poll_interval: std::time::Duration::from_secs(poll_seconds),
        single_iteration,
        ..DaemonLoopConfig::default()
    };

    daemon_loop.run(&base_dir, &loop_config).await
}

async fn handle_status_multi_repo(data_dir: &str, repos: &[String]) -> AppResult<()> {
    let data_dir_path = std::path::Path::new(data_dir);
    repo_registry::validate_data_dir(data_dir_path)?;

    let store = FsDaemonStore;

    // Determine which repos to query
    let slugs: Vec<&str> = if repos.is_empty() {
        // List all repos under data-dir/repos/
        let repos_dir = data_dir_path.join("repos");
        if !repos_dir.is_dir() {
            println!("No repos registered.");
            return Ok(());
        }
        // Collect all owner/repo pairs from directory structure
        let mut found = Vec::new();
        if let Ok(owners) = std::fs::read_dir(&repos_dir) {
            for owner_entry in owners.flatten() {
                if !owner_entry.path().is_dir() {
                    continue;
                }
                let owner = owner_entry.file_name().to_string_lossy().to_string();
                if let Ok(repo_entries) = std::fs::read_dir(owner_entry.path()) {
                    for repo_entry in repo_entries.flatten() {
                        if repo_entry.path().is_dir() {
                            let repo_name = repo_entry.file_name().to_string_lossy().to_string();
                            found.push(format!("{owner}/{repo_name}"));
                        }
                    }
                }
            }
        }
        // We'll print tasks from all found repos below
        print_multi_repo_status(&store, data_dir_path, &found)?;
        return Ok(());
    } else {
        repos.iter().map(|s| s.as_str()).collect()
    };

    let slug_strings: Vec<String> = slugs.iter().map(|s| s.to_string()).collect();
    print_multi_repo_status(&store, data_dir_path, &slug_strings)?;
    Ok(())
}

fn print_multi_repo_status(
    store: &FsDaemonStore,
    data_dir: &std::path::Path,
    repo_slugs: &[String],
) -> AppResult<()> {
    let mut any_tasks = false;

    for slug in repo_slugs {
        let (owner, repo) = repo_registry::parse_repo_slug(slug)?;
        let daemon_dir = DataDirLayout::daemon_dir(data_dir, owner, repo);
        let tasks_dir = daemon_dir.join("tasks");
        if !tasks_dir.is_dir() {
            continue;
        }

        // Use the repo's daemon dir parent as the base_dir for the store
        // The FsDaemonStore expects base_dir such that daemon/ is under .ralph-burning/
        // For multi-repo, we need to point at the checkout path
        let checkout = DataDirLayout::checkout_path(data_dir, owner, repo);
        let tasks = match store.list_tasks(&checkout) {
            Ok(tasks) => tasks,
            Err(_) => continue,
        };

        for task in &tasks {
            any_tasks = true;
            let repo_label = task.repo_slug.as_deref().unwrap_or(slug);
            println!(
                "{}  {}  {}  dispatch={}  issue={}",
                repo_label,
                task.task_id,
                task.status,
                task.dispatch_mode,
                task.issue_ref,
            );
        }
    }

    if !any_tasks {
        println!("No daemon tasks found.");
    }

    Ok(())
}

async fn handle_abort_by_issue(
    data_dir: &str,
    repo_slug: &str,
    identifier: &str,
) -> AppResult<()> {
    let data_dir_path = std::path::Path::new(data_dir);
    let (owner, repo) = repo_registry::parse_repo_slug(repo_slug)?;
    let checkout = DataDirLayout::checkout_path(data_dir_path, owner, repo);
    let store = FsDaemonStore;
    let worktree = WorktreeAdapter;

    let issue_number: u64 = identifier.parse().map_err(|_| AppError::InvalidConfigValue {
        key: "issue-number".to_owned(),
        value: identifier.to_owned(),
        reason: "expected a numeric issue number".to_owned(),
    })?;

    let task = DaemonTaskService::find_task_by_issue(&store, &checkout, repo_slug, issue_number)?
        .ok_or_else(|| AppError::InvalidConfigValue {
            key: "issue-number".to_owned(),
            value: identifier.to_owned(),
            reason: format!("no task found for {repo_slug}#{issue_number}"),
        })?;

    if task.status.is_terminal() {
        return Err(AppError::TaskStateTransitionInvalid {
            task_id: task.task_id,
            from: task.status.as_str().to_owned(),
            to: TaskStatus::Aborted.as_str().to_owned(),
        });
    }

    let original_status = task.status;
    let task_id = task.task_id.clone();
    DaemonTaskService::mark_aborted(&store, &checkout, &task_id)?;

    if matches!(original_status, TaskStatus::Claimed | TaskStatus::Active) {
        cleanup_aborted_task(&store, &worktree, &checkout, &task_id, original_status).await?;
    }

    println!("Aborted {repo_slug}#{issue_number} (task {task_id})");
    Ok(())
}

async fn handle_retry_by_issue(
    data_dir: &str,
    repo_slug: &str,
    identifier: &str,
) -> AppResult<()> {
    let data_dir_path = std::path::Path::new(data_dir);
    let (owner, repo) = repo_registry::parse_repo_slug(repo_slug)?;
    let checkout = DataDirLayout::checkout_path(data_dir_path, owner, repo);
    let store = FsDaemonStore;

    let issue_number: u64 = identifier.parse().map_err(|_| AppError::InvalidConfigValue {
        key: "issue-number".to_owned(),
        value: identifier.to_owned(),
        reason: "expected a numeric issue number".to_owned(),
    })?;

    let task = DaemonTaskService::find_task_by_issue(&store, &checkout, repo_slug, issue_number)?
        .ok_or_else(|| AppError::InvalidConfigValue {
            key: "issue-number".to_owned(),
            value: identifier.to_owned(),
            reason: format!("no task found for {repo_slug}#{issue_number}"),
        })?;

    let task = DaemonTaskService::retry_task(&store, &checkout, &task.task_id)?;
    println!(
        "Retried {repo_slug}#{issue_number} (task {}, attempt_count={})",
        task.task_id, task.attempt_count
    );
    Ok(())
}

async fn handle_reconcile_multi_repo(data_dir: &str, ttl_seconds: Option<u64>) -> AppResult<()> {
    let data_dir_path = std::path::Path::new(data_dir);
    repo_registry::validate_data_dir(data_dir_path)?;

    let store = FsDaemonStore;
    let worktree = WorktreeAdapter;

    // Iterate all repos under data-dir
    let repos_dir = data_dir_path.join("repos");
    if !repos_dir.is_dir() {
        println!("No repos registered.");
        return Ok(());
    }

    let mut total_stale = 0usize;
    let mut total_failed = 0usize;
    let mut total_released = 0usize;
    let mut any_cleanup_failure = false;

    if let Ok(owners) = std::fs::read_dir(&repos_dir) {
        for owner_entry in owners.flatten() {
            if !owner_entry.path().is_dir() {
                continue;
            }
            let owner = owner_entry.file_name().to_string_lossy().to_string();
            if let Ok(repo_entries) = std::fs::read_dir(owner_entry.path()) {
                for repo_entry in repo_entries.flatten() {
                    if !repo_entry.path().is_dir() {
                        continue;
                    }
                    let repo_name = repo_entry.file_name().to_string_lossy().to_string();
                    let checkout = DataDirLayout::checkout_path(data_dir_path, &owner, &repo_name);

                    let report = LeaseService::reconcile(
                        &store,
                        &worktree,
                        &checkout,
                        &checkout,
                        ttl_seconds,
                        chrono::Utc::now(),
                    )?;

                    total_stale += report.stale_lease_ids.len();
                    total_failed += report.failed_task_ids.len();
                    total_released += report.released_lease_ids.len();

                    if report.has_cleanup_failures() {
                        any_cleanup_failure = true;
                        for failure in &report.cleanup_failures {
                            println!(
                                "  {owner}/{repo_name}: lease={} task={}: {}",
                                failure.lease_id,
                                failure.task_id.as_deref().unwrap_or("n/a"),
                                failure.details
                            );
                        }
                    }
                }
            }
        }
    }

    println!(
        "reconciled stale_leases={total_stale} failed_tasks={total_failed} released_leases={total_released}"
    );

    if any_cleanup_failure {
        return Err(AppError::ReconcileCleanupFailed {
            failed_count: total_failed,
        });
    }
    Ok(())
}

// ===========================================================================
// Legacy (current-dir) handlers
// ===========================================================================

async fn handle_start_legacy(poll_seconds: u64, single_iteration: bool) -> AppResult<()> {
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

async fn handle_status_legacy() -> AppResult<()> {
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

async fn handle_abort_legacy(task_id: &str) -> AppResult<()> {
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

async fn handle_retry_legacy(task_id: &str) -> AppResult<()> {
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

async fn handle_reconcile_legacy(ttl_seconds: Option<u64>) -> AppResult<()> {
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

// ===========================================================================
// Shared helpers
// ===========================================================================

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
                DaemonTaskService::clear_lease_reference(store, base_dir, task_id).map(|_| ())
            }
            Ok(_) => Err(AppError::LeaseCleanupPartialFailure {
                task_id: task_id.to_owned(),
            }),
            Err(error) => Err(error),
        };
    }
}
