use clap::{Args, Subcommand};

use crate::adapters::fs::{
    FsAmendmentQueueStore, FsArtifactStore, FsDataDirDaemonStore, FsJournalStore,
    FsPayloadArtifactWriteStore, FsProjectStore, FsRepoRegistryStore, FsRequirementsStore,
    FsRunSnapshotStore, FsRunSnapshotWriteStore, FsRuntimeLogWriteStore,
};
use crate::adapters::github::{GithubClient, GithubClientConfig};
use crate::adapters::worktree::WorktreeAdapter;
use crate::contexts::automation_runtime::daemon_loop::{DaemonLoop, DaemonLoopConfig};
use crate::contexts::automation_runtime::lease_service::{LeaseService, ReleaseMode};
use crate::contexts::automation_runtime::model::TaskStatus;
use crate::contexts::automation_runtime::repo_registry::{self, DataDirLayout, RepoRegistryPort};
use crate::contexts::automation_runtime::task_service::DaemonTaskService;
use crate::contexts::automation_runtime::DaemonStorePort;
use crate::shared::error::{AppError, AppResult};

use crate::composition::agent_execution_builder::{
    build_agent_execution_service, build_agent_execution_service_for_config,
};

#[derive(Debug, Args)]
pub struct DaemonCommand {
    /// GitHub personal access token for API and git HTTPS auth.
    #[arg(long, env = "GITHUB_TOKEN", global = true)]
    pub github_token: Option<String>,

    #[command(subcommand)]
    pub command: DaemonSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum DaemonSubcommand {
    /// Start the daemon loop. Requires --data-dir and at least one --repo.
    /// Runs in multi-repo GitHub intake mode.
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
    /// Show status of daemon tasks across registered repos.
    Status {
        /// Root directory for multi-repo daemon state.
        #[arg(long)]
        data_dir: String,
        /// Filter status to specific repos.
        #[arg(long = "repo")]
        repos: Vec<String>,
    },
    /// Abort a task by issue number.
    Abort {
        /// Issue number.
        identifier: String,
        /// Root directory for multi-repo daemon state.
        #[arg(long)]
        data_dir: String,
        /// Repo slug for issue-number resolution (owner/repo).
        #[arg(long = "repo")]
        repo: String,
    },
    /// Retry a failed or aborted task by issue number.
    Retry {
        /// Issue number.
        identifier: String,
        /// Root directory for multi-repo daemon state.
        #[arg(long)]
        data_dir: String,
        /// Repo slug for issue-number resolution (owner/repo).
        #[arg(long = "repo")]
        repo: String,
    },
    /// Reconcile stale leases across all repos.
    Reconcile {
        /// Root directory for multi-repo daemon state.
        #[arg(long)]
        data_dir: String,
        #[arg(long)]
        ttl_seconds: Option<u64>,
    },
}

pub async fn handle(command: DaemonCommand) -> AppResult<()> {
    let github_token = command.github_token;
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
                    github_token.as_deref(),
                )
                .await
            } else {
                Err(AppError::InvalidConfigValue {
                    key: "data-dir".to_owned(),
                    value: String::new(),
                    reason: "--data-dir is required for daemon start".to_owned(),
                })
            }
        }
        DaemonSubcommand::Status {
            ref data_dir,
            ref repos,
        } => handle_status_multi_repo(data_dir, repos).await,
        DaemonSubcommand::Abort {
            ref identifier,
            ref data_dir,
            ref repo,
        } => handle_abort_by_issue(data_dir, repo, identifier, github_token.as_deref()).await,
        DaemonSubcommand::Retry {
            ref identifier,
            ref data_dir,
            ref repo,
        } => handle_retry_by_issue(data_dir, repo, identifier, github_token.as_deref()).await,
        DaemonSubcommand::Reconcile {
            ref data_dir,
            ttl_seconds,
        } => handle_reconcile_multi_repo(data_dir, ttl_seconds, github_token.as_deref()).await,
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
    github_token: Option<&str>,
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

    // Resolve GitHub token: explicit arg > GITHUB_TOKEN env > `gh auth token`
    let token = resolve_github_token(github_token)?;

    // Configure git to use `gh` as credential helper for HTTPS operations.
    // This makes `git clone`/`git push` work transparently without manual
    // token injection into each git command.
    setup_gh_git_auth()?;

    // Create GitHub client early so we fail fast if token is invalid
    let github_config = GithubClientConfig {
        token: token.clone(),
        api_base_url: std::env::var("GITHUB_API_URL")
            .unwrap_or_else(|_| "https://api.github.com".to_owned()),
    };
    let github_client = GithubClient::new(github_config);

    // Register, bootstrap, and validate all repos upfront.
    // Bootstrap clones the repo if the checkout dir is empty; validate
    // ensures the result is a usable Git checkout with a workspace dir.
    let mut registrations = Vec::new();
    for slug in repos {
        let reg = repo_registry::register_repo(data_dir_path, slug)?;

        // Bootstrap: clone from GitHub if checkout is missing .git
        if let Err(e) = repo_registry::bootstrap_repo_checkout(data_dir_path, slug) {
            return Err(AppError::InvalidConfigValue {
                key: "repo".to_owned(),
                value: slug.clone(),
                reason: format!("checkout bootstrap failed: {e}"),
            });
        }

        // Validate the (possibly just-bootstrapped) checkout
        if let Err(e) = repo_registry::validate_repo_checkout(&reg.repo_root) {
            return Err(AppError::InvalidConfigValue {
                key: "repo".to_owned(),
                value: slug.clone(),
                reason: format!("repo validation failed: {e}"),
            });
        }

        registrations.push(reg);
    }

    if verbose {
        println!(
            "daemon: starting with data-dir={} repos={:?}",
            data_dir, repos
        );
    }

    // Persist registrations so status/reconcile can discover them later
    let registry_store = FsRepoRegistryStore;
    for reg in &registrations {
        registry_store.write_registration(data_dir_path, reg)?;
    }

    let agent_service = build_agent_execution_service()?;

    let daemon_store = FsDataDirDaemonStore;
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
    .with_configured_agent_service_builder(build_agent_execution_service_for_config)
    .with_requirements_store(&requirements_store)
    .with_registrations(registrations)
    .with_data_dir(data_dir_path.to_owned());

    let loop_config = DaemonLoopConfig {
        poll_interval: std::time::Duration::from_secs(poll_seconds),
        single_iteration,
        ..DaemonLoopConfig::default()
    };

    daemon_loop
        .run_multi_repo(&loop_config, &github_client)
        .await
}

async fn handle_status_multi_repo(data_dir: &str, repos: &[String]) -> AppResult<()> {
    let data_dir_path = std::path::Path::new(data_dir);
    repo_registry::validate_data_dir(data_dir_path)?;

    let store = FsDataDirDaemonStore;

    // Determine which repos to query
    let slug_strings: Vec<String> = if repos.is_empty() {
        let registry = FsRepoRegistryStore;
        let registrations = registry.list_registrations(data_dir_path)?;
        if registrations.is_empty() {
            println!("No repos registered.");
            return Ok(());
        }
        registrations.iter().map(|r| r.repo_slug.clone()).collect()
    } else {
        repos.to_vec()
    };

    print_multi_repo_status(&store, data_dir_path, &slug_strings)?;
    Ok(())
}

fn print_multi_repo_status(
    store: &FsDataDirDaemonStore,
    data_dir: &std::path::Path,
    repo_slugs: &[String],
) -> AppResult<()> {
    let mut any_tasks = false;

    for slug in repo_slugs {
        let (owner, repo) = repo_registry::parse_repo_slug(slug)?;
        let daemon_dir = DataDirLayout::daemon_dir(data_dir, owner, repo);
        let tasks = match store.list_tasks(&daemon_dir) {
            Ok(tasks) => tasks,
            Err(_) => continue,
        };

        for task in &tasks {
            any_tasks = true;
            let repo_label = task.repo_slug.as_deref().unwrap_or(slug);
            println!(
                "{}  {}  {}  dispatch={}  issue={}",
                repo_label, task.task_id, task.status, task.dispatch_mode, task.issue_ref,
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
    github_token: Option<&str>,
) -> AppResult<()> {
    let data_dir_path = std::path::Path::new(data_dir);
    let (owner, repo) = repo_registry::parse_repo_slug(repo_slug)?;
    let store = FsDataDirDaemonStore;
    let worktree = WorktreeAdapter;
    let daemon_dir = DataDirLayout::daemon_dir(data_dir_path, owner, repo);
    let checkout = DataDirLayout::checkout_path(data_dir_path, owner, repo);

    let issue_number: u64 = identifier
        .parse()
        .map_err(|_| AppError::InvalidConfigValue {
            key: "issue-number".to_owned(),
            value: identifier.to_owned(),
            reason: "expected a numeric issue number".to_owned(),
        })?;

    let task = DaemonTaskService::find_task_by_issue(&store, &daemon_dir, repo_slug, issue_number)?
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
    DaemonTaskService::mark_aborted(&store, &daemon_dir, &task_id)?;

    if matches!(original_status, TaskStatus::Claimed | TaskStatus::Active) {
        cleanup_aborted_task(
            &store,
            &worktree,
            &daemon_dir,
            &checkout,
            &task_id,
            original_status,
        )
        .await?;
    }

    // Sync GitHub label: Aborted → rb:failed
    let aborted_task = store.read_task(&daemon_dir, &task_id)?;
    match make_github_client(github_token) {
        Some(gh) => {
            if let Err(e) = crate::contexts::automation_runtime::github_intake::sync_label_for_task(
                &gh,
                &aborted_task,
            )
            .await
            {
                eprintln!("warning: failed to sync GitHub label after abort: {e}");
                let _ = DaemonTaskService::mark_label_dirty(&store, &daemon_dir, &task_id);
            }
        }
        None => {
            eprintln!(
                "warning: GitHub token not available — marking label_dirty for later reconcile"
            );
            let _ = DaemonTaskService::mark_label_dirty(&store, &daemon_dir, &task_id);
        }
    }

    println!("Aborted {repo_slug}#{issue_number} (task {task_id})");
    Ok(())
}

async fn handle_retry_by_issue(
    data_dir: &str,
    repo_slug: &str,
    identifier: &str,
    github_token: Option<&str>,
) -> AppResult<()> {
    let data_dir_path = std::path::Path::new(data_dir);
    let (owner, repo) = repo_registry::parse_repo_slug(repo_slug)?;
    let store = FsDataDirDaemonStore;
    let daemon_dir = DataDirLayout::daemon_dir(data_dir_path, owner, repo);

    let issue_number: u64 = identifier
        .parse()
        .map_err(|_| AppError::InvalidConfigValue {
            key: "issue-number".to_owned(),
            value: identifier.to_owned(),
            reason: "expected a numeric issue number".to_owned(),
        })?;

    let task = DaemonTaskService::find_task_by_issue(&store, &daemon_dir, repo_slug, issue_number)?
        .ok_or_else(|| AppError::InvalidConfigValue {
            key: "issue-number".to_owned(),
            value: identifier.to_owned(),
            reason: format!("no task found for {repo_slug}#{issue_number}"),
        })?;

    // If the task retains a lease from partial cleanup, attempt cleanup
    // before retry so retry_task() doesn't reject it.
    if let Some(ref lid) = task.lease_id {
        let worktree = WorktreeAdapter;
        let checkout = DataDirLayout::checkout_path(data_dir_path, owner, repo);
        if let Ok(lease) = store.read_lease(&daemon_dir, lid) {
            let result = LeaseService::release(
                &store,
                &worktree,
                &daemon_dir,
                &checkout,
                &lease,
                ReleaseMode::Idempotent,
            );
            if let Ok(ref r) = result {
                if r.resources_released {
                    let _ = DaemonTaskService::clear_lease_reference(
                        &store,
                        &daemon_dir,
                        &task.task_id,
                    );
                }
            }
            // If cleanup failed, retry_task() will reject with
            // LeaseCleanupPartialFailure — the user must reconcile first.
        }
    }

    let task = DaemonTaskService::retry_task(&store, &daemon_dir, &task.task_id)?;

    // Sync GitHub label: retried task is Pending → rb:ready
    match make_github_client(github_token) {
        Some(gh) => {
            if let Err(e) =
                crate::contexts::automation_runtime::github_intake::sync_label_for_task(&gh, &task)
                    .await
            {
                eprintln!("warning: failed to sync GitHub label after retry: {e}");
                let _ = DaemonTaskService::mark_label_dirty(&store, &daemon_dir, &task.task_id);
            }
        }
        None => {
            eprintln!(
                "warning: GitHub token not available — marking label_dirty for later reconcile"
            );
            let _ = DaemonTaskService::mark_label_dirty(&store, &daemon_dir, &task.task_id);
        }
    }

    println!(
        "Retried {repo_slug}#{issue_number} (task {}, attempt_count={})",
        task.task_id, task.attempt_count
    );
    Ok(())
}

async fn handle_reconcile_multi_repo(
    data_dir: &str,
    ttl_seconds: Option<u64>,
    github_token: Option<&str>,
) -> AppResult<()> {
    let data_dir_path = std::path::Path::new(data_dir);
    repo_registry::validate_data_dir(data_dir_path)?;

    let store = FsDataDirDaemonStore;
    let worktree = WorktreeAdapter;
    let registry = FsRepoRegistryStore;

    let registrations = match registry.list_registrations(data_dir_path) {
        Ok(regs) => regs,
        Err(_) => {
            // Fallback to directory scan
            let repos_dir = data_dir_path.join("repos");
            if !repos_dir.is_dir() {
                println!("No repos registered.");
                return Ok(());
            }
            let mut found = Vec::new();
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
                            found.push(format!("{owner}/{repo_name}"));
                        }
                    }
                }
            }
            if found.is_empty() {
                println!("No repos registered.");
                return Ok(());
            }
            // Convert to minimal registrations for iteration
            let mut regs = Vec::new();
            for slug in &found {
                let reg = repo_registry::register_repo(data_dir_path, slug)?;
                regs.push(reg);
            }
            regs
        }
    };

    if registrations.is_empty() {
        println!("No repos registered.");
        return Ok(());
    }

    let mut total_stale = 0usize;
    let mut total_failed = 0usize;
    let mut total_released = 0usize;
    let mut any_cleanup_failure = false;
    let mut total_label_repaired = 0usize;
    let mut total_label_repair_failed = 0usize;

    // Attempt GitHub label repair for tasks with label_dirty = true.
    // Best-effort: if GitHub token is unavailable, skip label repair.
    let github_client = make_github_client(github_token);

    for reg in &registrations {
        let (owner, repo_name) = repo_registry::parse_repo_slug(&reg.repo_slug)?;
        let daemon_dir = DataDirLayout::daemon_dir(data_dir_path, owner, repo_name);
        let checkout = DataDirLayout::checkout_path(data_dir_path, owner, repo_name);

        let report = LeaseService::reconcile(
            &store,
            &worktree,
            &daemon_dir,
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

        // Repair GitHub labels for tasks with label_dirty = true,
        // and also recover stuck tasks (revert Claimed/Active to Pending
        // so they re-enter the pending queue, or release worktrees for
        // terminal tasks).
        if let Some(ref gh) = github_client {
            if let Ok(tasks) = store.list_tasks(&daemon_dir) {
                for task in tasks.iter().filter(|t| t.label_dirty) {
                    match crate::contexts::automation_runtime::github_intake::sync_label_for_task(
                        gh, task,
                    )
                    .await
                    {
                        Ok(()) => {
                            let _ = DaemonTaskService::clear_label_dirty(
                                &store,
                                &daemon_dir,
                                &task.task_id,
                            );
                            // Phase-0 recovery: revert stuck Claimed/Active
                            // tasks back to Pending so daemon picks them up
                            // again on the next cycle.
                            if matches!(task.status.as_str(), "claimed" | "active") {
                                let _ = DaemonTaskService::revert_to_pending_for_recovery(
                                    &store,
                                    &worktree,
                                    &daemon_dir,
                                    &checkout,
                                    &task.task_id,
                                );
                            }
                            total_label_repaired += 1;
                            println!(
                                "  {owner}/{repo_name}: repaired label for task {}",
                                task.task_id
                            );
                        }
                        Err(e) => {
                            total_label_repair_failed += 1;
                            println!(
                                "  {owner}/{repo_name}: failed to repair label for task {}: {e}",
                                task.task_id
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
    if total_label_repaired > 0 || total_label_repair_failed > 0 {
        println!("label_repair repaired={total_label_repaired} failed={total_label_repair_failed}");
    }

    if any_cleanup_failure {
        println!("--- Cleanup Failures ---");
        return Err(AppError::ReconcileCleanupFailed {
            failed_count: total_failed,
        });
    }
    Ok(())
}

// ===========================================================================
// Shared helpers
// ===========================================================================

/// Resolve GitHub token from: explicit arg > `gh auth token` fallback.
fn resolve_github_token(explicit: Option<&str>) -> AppResult<String> {
    if let Some(t) = explicit {
        if !t.is_empty() {
            return Ok(t.to_owned());
        }
    }
    // Fallback: ask `gh` CLI for its stored token
    let output = std::process::Command::new("gh")
        .args(["auth", "token"])
        .output()
        .map_err(|e| AppError::InvalidConfigValue {
            key: "github-token".to_owned(),
            value: String::new(),
            reason: format!("failed to run `gh auth token`: {e}"),
        })?;
    if output.status.success() {
        let token = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if !token.is_empty() {
            return Ok(token);
        }
    }
    Err(AppError::InvalidConfigValue {
        key: "github-token".to_owned(),
        value: String::new(),
        reason: "no GitHub token: pass --github-token, set GITHUB_TOKEN, or run `gh auth login`"
            .to_owned(),
    })
}

/// Build a `GithubClient` from an optional explicit token, falling back to
/// `gh auth token`. Returns `None` if no token is available (best-effort).
fn make_github_client(explicit: Option<&str>) -> Option<GithubClient> {
    let token = resolve_github_token(explicit).ok()?;
    let config = GithubClientConfig {
        token,
        api_base_url: std::env::var("GITHUB_API_URL")
            .unwrap_or_else(|_| "https://api.github.com".to_owned()),
    };
    Some(GithubClient::new(config))
}

/// Run `gh auth setup-git` to configure the `gh` credential helper for
/// git HTTPS operations (clone, push, fetch).
fn setup_gh_git_auth() -> AppResult<()> {
    let output = std::process::Command::new("gh")
        .args(["auth", "setup-git"])
        .output()
        .map_err(|e| AppError::InvalidConfigValue {
            key: "gh".to_owned(),
            value: String::new(),
            reason: format!("failed to run `gh auth setup-git`: {e}"),
        })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(AppError::InvalidConfigValue {
            key: "gh".to_owned(),
            value: String::new(),
            reason: format!("gh auth setup-git failed: {}", stderr.trim()),
        });
    }
    Ok(())
}

/// Clean up lease and worktree resources for an aborted task.
///
/// `base_dir` is the directory containing daemon state (tasks/leases).
/// `repo_root` is the Git checkout root used for worktree operations.
/// In single-repo (legacy) mode these are the same; in multi-repo mode
/// `base_dir` is the daemon shard and `repo_root` is the checkout path.
async fn cleanup_aborted_task(
    store: &dyn DaemonStorePort,
    worktree: &WorktreeAdapter,
    base_dir: &std::path::Path,
    repo_root: &std::path::Path,
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
            repo_root,
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
