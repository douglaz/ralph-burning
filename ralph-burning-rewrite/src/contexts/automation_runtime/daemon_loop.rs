use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use crate::adapters::fs::FileSystem;
use crate::adapters::github::GithubPort;
use crate::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use crate::contexts::agent_execution::policy::BackendPolicyService;
use crate::contexts::agent_execution::service::{AgentExecutionPort, RawOutputPort};
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::AgentExecutionService;
use crate::contexts::automation_runtime::lease_service::LeaseService;
use crate::contexts::automation_runtime::model::{
    DaemonTask, DispatchMode, RebaseFailureClassification, RebaseOutcome, TaskStatus,
};
use crate::contexts::automation_runtime::pr_review::PrReviewIngestionService;
use crate::contexts::automation_runtime::pr_runtime::PrRuntimeService;
use crate::contexts::automation_runtime::repo_registry::{
    parse_repo_slug, DataDirLayout, RepoRegistration,
};
use crate::contexts::automation_runtime::routing::RoutingEngine;
use crate::contexts::automation_runtime::task_service::DaemonTaskService;
use crate::contexts::automation_runtime::watcher::{self, IssueWatcherPort};
use crate::contexts::automation_runtime::{
    github_intake, DaemonStorePort, RebaseConflictRequest, RebaseConflictResolution,
    RebaseConflictResolver, WorktreePort,
};
use crate::contexts::project_run_record::model::RunStatus;
use crate::contexts::project_run_record::service::{
    create_project, AmendmentQueuePort, ArtifactStorePort, JournalStorePort,
    PayloadArtifactWritePort, ProjectStorePort, RunSnapshotPort, RunSnapshotWritePort,
    RuntimeLogWritePort,
};
use crate::contexts::project_run_record::CreateProjectInput;
use crate::contexts::requirements_drafting::service::{self as req_service, RequirementsStorePort};
use crate::contexts::workflow_composition::engine;
use crate::contexts::workflow_composition::retry_policy::RetryPolicy;
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::contexts::workspace_governance::WORKSPACE_DIR;
use crate::shared::domain::{
    BackendPolicyRole, BackendRole, FlowPreset, ProjectId, ResolvedBackendTarget, SessionPolicy,
};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonLoopConfig {
    pub poll_interval: Duration,
    pub lease_ttl: Duration,
    pub heartbeat_interval: Duration,
    pub single_iteration: bool,
}

impl Default for DaemonLoopConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(10),
            lease_ttl: Duration::from_secs(300),
            heartbeat_interval: Duration::from_secs(30),
            single_iteration: false,
        }
    }
}

pub struct DaemonLoop<'a, A, R, S> {
    store: &'a dyn DaemonStorePort,
    worktree: &'a dyn WorktreePort,
    project_store: &'a dyn ProjectStorePort,
    run_snapshot_read: &'a dyn RunSnapshotPort,
    run_snapshot_write: &'a dyn RunSnapshotWritePort,
    journal_store: &'a dyn JournalStorePort,
    artifact_store: &'a dyn ArtifactStorePort,
    artifact_write: &'a dyn PayloadArtifactWritePort,
    log_write: &'a dyn RuntimeLogWritePort,
    amendment_queue: &'a dyn AmendmentQueuePort,
    agent_service: &'a AgentExecutionService<A, R, S>,
    routing_engine: RoutingEngine,
    watcher: Option<&'a dyn IssueWatcherPort>,
    requirements_store: Option<&'a dyn RequirementsStorePort>,
    /// Multi-repo registrations. When non-empty, the daemon iterates across
    /// registered repos each cycle instead of using the file watcher.
    registrations: Vec<RepoRegistration>,
    /// Data-dir root for multi-repo daemon state.
    data_dir: Option<std::path::PathBuf>,
}

impl<'a, A, R, S> DaemonLoop<'a, A, R, S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: &'a dyn DaemonStorePort,
        worktree: &'a dyn WorktreePort,
        project_store: &'a dyn ProjectStorePort,
        run_snapshot_read: &'a dyn RunSnapshotPort,
        run_snapshot_write: &'a dyn RunSnapshotWritePort,
        journal_store: &'a dyn JournalStorePort,
        artifact_store: &'a dyn ArtifactStorePort,
        artifact_write: &'a dyn PayloadArtifactWritePort,
        log_write: &'a dyn RuntimeLogWritePort,
        amendment_queue: &'a dyn AmendmentQueuePort,
        agent_service: &'a AgentExecutionService<A, R, S>,
    ) -> Self {
        Self {
            store,
            worktree,
            project_store,
            run_snapshot_read,
            run_snapshot_write,
            journal_store,
            artifact_store,
            artifact_write,
            log_write,
            amendment_queue,
            agent_service,
            routing_engine: RoutingEngine::new(),
            watcher: None,
            requirements_store: None,
            registrations: Vec::new(),
            data_dir: None,
        }
    }

    pub fn with_watcher(mut self, watcher: &'a dyn IssueWatcherPort) -> Self {
        self.watcher = Some(watcher);
        self
    }

    pub fn with_requirements_store(mut self, store: &'a dyn RequirementsStorePort) -> Self {
        self.requirements_store = Some(store);
        self
    }

    pub fn with_registrations(mut self, registrations: Vec<RepoRegistration>) -> Self {
        self.registrations = registrations;
        self
    }

    pub fn with_data_dir(mut self, data_dir: std::path::PathBuf) -> Self {
        self.data_dir = Some(data_dir);
        self
    }

    /// Whether this daemon loop is running in multi-repo mode.
    pub fn is_multi_repo(&self) -> bool {
        !self.registrations.is_empty()
    }
}

impl<A, R, S> DaemonLoop<'_, A, R, S>
where
    A: AgentExecutionPort + Sync,
    R: RawOutputPort + Sync,
    S: SessionStorePort + Sync,
{
    pub async fn run(&self, base_dir: &Path, config: &DaemonLoopConfig) -> AppResult<()> {
        let shutdown = CancellationToken::new();
        let shutdown_watcher = shutdown.clone();
        tokio::spawn(async move {
            let _ = wait_for_shutdown_signal().await;
            shutdown_watcher.cancel();
        });

        loop {
            if shutdown.is_cancelled() {
                self.cleanup_active_leases(base_dir, base_dir)?;
                break;
            }

            let processed = self
                .process_cycle(base_dir, config, shutdown.clone())
                .await?;
            if config.single_iteration {
                break;
            }

            let _ = processed;
            tokio::select! {
                _ = shutdown.cancelled() => {
                    self.cleanup_active_leases(base_dir, base_dir)?;
                    break;
                }
                _ = tokio::time::sleep(config.poll_interval) => {}
            }
        }

        Ok(())
    }

    /// Run the daemon loop in multi-repo GitHub mode. The GitHub adapter is
    /// passed by generic parameter to avoid dyn-compatibility issues with
    /// async trait methods. Each cycle: ensure labels once, poll GitHub
    /// for each registered repo, then process tasks per-repo with data-dir
    /// based state isolation.
    pub async fn run_multi_repo<G: GithubPort>(
        &self,
        config: &DaemonLoopConfig,
        github: &G,
    ) -> AppResult<()> {
        let data_dir = self
            .data_dir
            .as_deref()
            .ok_or_else(|| AppError::InvalidConfigValue {
                key: "data-dir".to_owned(),
                value: String::new(),
                reason: "data-dir is required for multi-repo mode".to_owned(),
            })?;

        let shutdown = CancellationToken::new();
        let shutdown_watcher = shutdown.clone();
        tokio::spawn(async move {
            let _ = wait_for_shutdown_signal().await;
            shutdown_watcher.cancel();
        });

        // Ensure labels on all registered repos at startup. During `daemon start`,
        // every requested repo must pass label ensure — silently excluding a
        // requested repo is a startup contract violation. If any repo fails,
        // surface the failure explicitly instead of quarantining.
        let active_registrations =
            github_intake::ensure_labels_on_repos(github, &self.registrations).await;
        if active_registrations.len() < self.registrations.len() {
            let quarantined: Vec<&str> = self
                .registrations
                .iter()
                .filter(|r| {
                    !active_registrations
                        .iter()
                        .any(|a| a.repo_slug == r.repo_slug)
                })
                .map(|r| r.repo_slug.as_str())
                .collect();
            return Err(AppError::InvalidConfigValue {
                key: "repos".to_owned(),
                value: quarantined.join(", "),
                reason: format!(
                    "startup label ensure failed for requested repo(s): {} — \
                     all repos must pass label ensure at daemon start",
                    quarantined.join(", ")
                ),
            });
        }

        loop {
            if shutdown.is_cancelled() {
                // Cleanup active leases across all repos using daemon shard for
                // store reads and checkout root for Git/worktree operations.
                for reg in &active_registrations {
                    if let Ok((owner, repo)) = parse_repo_slug(&reg.repo_slug) {
                        let daemon_dir = DataDirLayout::daemon_dir(data_dir, owner, repo);
                        let _ = self.cleanup_active_leases(&daemon_dir, &reg.repo_root);
                    }
                }
                break;
            }

            self.process_cycle_multi_repo(
                data_dir,
                config,
                github,
                shutdown.clone(),
                &active_registrations,
            )
            .await?;
            if config.single_iteration {
                break;
            }

            tokio::select! {
                _ = shutdown.cancelled() => {
                    for reg in &active_registrations {
                        if let Ok((owner, repo)) = parse_repo_slug(&reg.repo_slug) {
                            let daemon_dir = DataDirLayout::daemon_dir(data_dir, owner, repo);
                            let _ = self.cleanup_active_leases(&daemon_dir, &reg.repo_root);
                        }
                    }
                    break;
                }
                _ = tokio::time::sleep(config.poll_interval) => {}
            }
        }

        Ok(())
    }

    /// Process one multi-repo cycle: poll GitHub for each registration,
    /// check waiting tasks, and process pending tasks. Per-repo failures
    /// are isolated — a failure in one repo does not block others.
    async fn process_cycle_multi_repo<G: GithubPort>(
        &self,
        data_dir: &Path,
        config: &DaemonLoopConfig,
        github: &G,
        shutdown: CancellationToken,
        registrations: &[RepoRegistration],
    ) -> AppResult<()> {
        for reg in registrations {
            let (owner, repo) = match parse_repo_slug(&reg.repo_slug) {
                Ok(pair) => pair,
                Err(e) => {
                    eprintln!("daemon: invalid repo slug '{}': {e}", reg.repo_slug);
                    continue;
                }
            };

            let daemon_dir = DataDirLayout::daemon_dir(data_dir, owner, repo);
            let checkout = &reg.repo_root;

            // Phase 0: Attempt to repair label_dirty tasks from prior cycles.
            // A GitHub label failure during repair quarantines this repo for the
            // rest of the cycle, consistent with multi-repo failure isolation.
            //
            // After successful label repair AND successful cleanup/revert,
            // clear `label_dirty`. If label sync succeeds but cleanup fails,
            // keep `label_dirty` so Phase 0 retries on the next cycle (the
            // re-sync is idempotent and harmless).
            //
            // - Terminal tasks with unreleased leases get their leases released
            //   (deferred from the previous cycle's quarantine).
            // - Non-terminal tasks (Claimed/Active) are reverted to Pending so
            //   Phase 3 can re-process them in this cycle.
            let mut phase0_quarantined = false;
            if let Ok(tasks) = DaemonTaskService::list_tasks(self.store, &daemon_dir) {
                for dirty_task in tasks.iter().filter(|t| t.label_dirty) {
                    match github_intake::sync_label_for_task(github, dirty_task).await {
                        Ok(()) => {
                            if dirty_task.is_terminal() {
                                // Terminal tasks: release deferred lease, then clear dirty.
                                if let Some(ref lid) = dirty_task.lease_id {
                                    if let Ok(lease) = self.store.read_lease(&daemon_dir, lid) {
                                        match self.release_task_lease(
                                            &daemon_dir,
                                            checkout,
                                            &dirty_task.task_id,
                                            &lease,
                                        ) {
                                            Ok(()) => {
                                                let _ = DaemonTaskService::clear_label_dirty(
                                                    self.store,
                                                    &daemon_dir,
                                                    &dirty_task.task_id,
                                                );
                                            }
                                            Err(e) => {
                                                // Partial cleanup: keep label_dirty so Phase 0
                                                // retries cleanup on the next cycle.
                                                eprintln!(
                                                    "daemon: deferred lease release failed for terminal task '{}' in {}: {e}",
                                                    dirty_task.task_id, reg.repo_slug
                                                );
                                            }
                                        }
                                    } else {
                                        // Lease file not found — nothing to release.
                                        let _ = DaemonTaskService::clear_label_dirty(
                                            self.store,
                                            &daemon_dir,
                                            &dirty_task.task_id,
                                        );
                                    }
                                } else {
                                    // No lease reference — nothing to release.
                                    let _ = DaemonTaskService::clear_label_dirty(
                                        self.store,
                                        &daemon_dir,
                                        &dirty_task.task_id,
                                    );
                                }
                            } else {
                                // Non-terminal tasks (Claimed/Active): revert to Pending
                                // so Phase 3 can re-process them in this cycle.
                                match DaemonTaskService::revert_to_pending_for_recovery(
                                    self.store,
                                    self.worktree,
                                    &daemon_dir,
                                    checkout,
                                    &dirty_task.task_id,
                                ) {
                                    Ok(reverted) => {
                                        let _ = DaemonTaskService::clear_label_dirty(
                                            self.store,
                                            &daemon_dir,
                                            &reverted.task_id,
                                        );
                                        eprintln!(
                                            "daemon: reverted interrupted task '{}' to pending in {}",
                                            reverted.task_id, reg.repo_slug
                                        );
                                    }
                                    Err(e) => {
                                        // Revert failed (lease cleanup partial): keep
                                        // label_dirty for the next Phase 0 cycle.
                                        eprintln!(
                                            "daemon: revert failed for task '{}' in {}: {e}",
                                            dirty_task.task_id, reg.repo_slug
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "daemon: label repair failed for task '{}' in {}: {e} — quarantining repo for this cycle",
                                dirty_task.task_id, reg.repo_slug
                            );
                            phase0_quarantined = true;
                            break;
                        }
                    }
                }
            }
            if phase0_quarantined {
                continue; // Multi-repo failure isolation: skip to next repo
            }

            // Phase 1: Poll GitHub for new issue candidates
            // Handle daemon commands (/rb retry, /rb abort) inline during ingestion
            if let Err(e) = github_intake::poll_and_ingest_repo(
                github,
                self.store,
                self.worktree,
                &daemon_dir,
                reg,
                &self.routing_engine,
                EffectiveConfig::load(checkout)
                    .map(|c| c.default_flow())
                    .unwrap_or(FlowPreset::Standard),
            )
            .await
            {
                eprintln!("daemon: GitHub poll failed for {}: {e}", reg.repo_slug);
                continue; // Multi-repo failure isolation
            }

            // Phase 2: Check waiting tasks for completed requirements runs
            let resumed_task_ids = match self.check_waiting_tasks(&daemon_dir, checkout) {
                Ok(ids) => ids,
                Err(e) => {
                    eprintln!(
                        "daemon: check_waiting_tasks failed for {}: {e}",
                        reg.repo_slug
                    );
                    // Continue to other repos
                    continue;
                }
            };

            // Sync labels for resumed tasks: WaitingForRequirements -> Pending
            // means the issue should now be labeled rb:ready instead of rb:waiting-feedback.
            // A label sync failure quarantines this repo for the rest of the cycle
            // (multi-repo failure isolation: no further task/lease/worktree mutation).
            // Mark label_dirty so reconcile can repair the mismatch later.
            let mut label_sync_failed = false;
            for task_id in &resumed_task_ids {
                if let Ok(resumed_task) = self.store.read_task(&daemon_dir, task_id) {
                    if let Err(e) = github_intake::sync_label_for_task(github, &resumed_task).await
                    {
                        eprintln!(
                            "daemon: failed to sync label for resumed task '{}' in {}: {e}",
                            task_id, reg.repo_slug
                        );
                        let _ =
                            DaemonTaskService::mark_label_dirty(self.store, &daemon_dir, task_id);
                        label_sync_failed = true;
                        break;
                    }
                }
            }
            if label_sync_failed {
                continue; // Quarantine repo for this cycle
            }

            // Phase 2b: Ingest PR review feedback for active/completed tasks
            // with PRs before collecting pending tasks. Reopened completed
            // tasks become pending in time for the same cycle's dispatch.
            if let Err(e) = self
                .ingest_pr_feedback_for_repo(&daemon_dir, checkout, github, shutdown.clone())
                .await
            {
                eprintln!(
                    "daemon: PR review ingestion failed for {}: {e}",
                    reg.repo_slug
                );
                continue;
            }

            // Phase 3: Process pending tasks for this repo
            let pending_tasks: Vec<DaemonTask> =
                match DaemonTaskService::list_tasks(self.store, &daemon_dir) {
                    Ok(tasks) => tasks
                        .into_iter()
                        .filter(|t| t.status == TaskStatus::Pending)
                        .collect(),
                    Err(e) => {
                        eprintln!("daemon: list_tasks failed for {}: {e}", reg.repo_slug);
                        continue;
                    }
                };

            for task in &pending_tasks {
                if shutdown.is_cancelled() {
                    return Ok(());
                }

                // Compute data-dir-aware worktree path and branch name
                let worktree_path_override = Some(DataDirLayout::task_worktree_path(
                    data_dir,
                    owner,
                    repo,
                    &task.task_id,
                ));
                let branch_name_override = task
                    .issue_number
                    .map(|issue_num| DataDirLayout::branch_name(issue_num, &task.project_id));

                if let Err(error) = self
                    .process_task_multi_repo(
                        &daemon_dir,
                        checkout,
                        task,
                        config,
                        shutdown.clone(),
                        worktree_path_override,
                        branch_name_override,
                        github,
                    )
                    .await
                {
                    eprintln!(
                        "daemon: task {} failed for {}: {}",
                        task.task_id, reg.repo_slug, error
                    );
                    // Stop further task mutations for this repo in this cycle
                    // (multi-repo failure isolation: a label or GitHub failure
                    // quarantines the repo for the rest of the cycle).
                    break;
                }
            }
        }

        Ok(())
    }

    /// Process a single task in multi-repo mode with separate store_dir and
    /// repo_root paths, and data-dir-aware worktree overrides.
    async fn process_task_multi_repo<G: GithubPort>(
        &self,
        store_dir: &Path,
        repo_root: &Path,
        task: &DaemonTask,
        config: &DaemonLoopConfig,
        shutdown: CancellationToken,
        worktree_path_override: Option<std::path::PathBuf>,
        branch_name_override: Option<String>,
        github: &G,
    ) -> AppResult<()> {
        let effective_config = self.load_effective_config_for_task(repo_root, task)?;
        let default_flow = effective_config.default_flow();

        // Requirements dispatch before claiming lease/worktree
        match task.dispatch_mode {
            DispatchMode::RequirementsQuick => {
                self.handle_requirements_quick(store_dir, repo_root, task, &effective_config)
                    .await?;
            }
            DispatchMode::RequirementsDraft => {
                return self
                    .handle_requirements_draft(
                        store_dir,
                        repo_root,
                        task,
                        &effective_config,
                        github,
                    )
                    .await;
            }
            DispatchMode::Workflow => {}
        }

        let task = &self.store.read_task(store_dir, &task.task_id)?;

        let (claimed_task, lease) = match DaemonTaskService::claim_task(
            self.store,
            self.worktree,
            &self.routing_engine,
            store_dir,
            repo_root,
            &task.task_id,
            default_flow,
            config.lease_ttl.as_secs(),
            worktree_path_override,
            branch_name_override,
        ) {
            Ok(value) => value,
            Err(AppError::ProjectWriterLockHeld { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };

        println!("claimed task {}", claimed_task.task_id);
        // Sync label: Claimed → rb:in-progress. On failure, mark label_dirty and
        // quarantine this repo — no further task/lease/worktree mutations in this
        // cycle. The task remains Claimed with its lease; Phase 0 will repair
        // the label and revert the task to Pending for the next cycle.
        if let Err(e) = github_intake::sync_label_for_task(github, &claimed_task).await {
            let _ =
                DaemonTaskService::mark_label_dirty(self.store, store_dir, &claimed_task.task_id);
            eprintln!(
                "daemon: label sync failed for claimed task '{}', quarantining repo: {e}",
                claimed_task.task_id
            );
            return Err(e);
        }

        if let Err(error) = self.rebase_task_worktree(
            store_dir,
            repo_root,
            &claimed_task,
            &lease,
            &effective_config,
        ) {
            // Sync label: Failed → rb:failed. On failure, mark label_dirty and
            // quarantine this repo — no further mutations in this cycle.
            let failed_task = self.store.read_task(store_dir, &claimed_task.task_id).ok();
            if let Some(ref ft) = failed_task {
                if let Err(e) = github_intake::sync_label_for_task(github, ft).await {
                    let _ = DaemonTaskService::mark_label_dirty(
                        self.store,
                        store_dir,
                        &claimed_task.task_id,
                    );
                    eprintln!(
                        "daemon: label sync failed for failed task '{}', quarantining repo: {e}",
                        claimed_task.task_id
                    );
                    return Err(e);
                }
            }
            println!("failed task {}: {}", claimed_task.task_id, error);
            return Ok(());
        }

        if let Err(error) = self.ensure_project(repo_root, &claimed_task) {
            self.handle_post_claim_failure(store_dir, repo_root, &claimed_task, &lease, &error)?;
            // Sync label: Failed → rb:failed. On failure, mark label_dirty and
            // quarantine this repo — no further mutations in this cycle.
            let failed_task = self.store.read_task(store_dir, &claimed_task.task_id).ok();
            if let Some(ref ft) = failed_task {
                if let Err(e) = github_intake::sync_label_for_task(github, ft).await {
                    let _ = DaemonTaskService::mark_label_dirty(
                        self.store,
                        store_dir,
                        &claimed_task.task_id,
                    );
                    eprintln!(
                        "daemon: label sync failed for failed task '{}', quarantining repo: {e}",
                        claimed_task.task_id
                    );
                    return Err(e);
                }
            }
            println!("failed task {}: {}", claimed_task.task_id, error);
            return Ok(());
        }
        let task_on_disk = self.store.read_task(store_dir, &claimed_task.task_id)?;
        if task_on_disk.status == TaskStatus::Aborted {
            // Sync label: Aborted → rb:failed. On failure, mark label_dirty and
            // quarantine — do NOT release the lease in this cycle so no further
            // mutations occur. Phase 0 will repair the label and release the lease.
            if let Err(e) = github_intake::sync_label_for_task(github, &task_on_disk).await {
                let _ = DaemonTaskService::mark_label_dirty(
                    self.store,
                    store_dir,
                    &task_on_disk.task_id,
                );
                eprintln!(
                    "daemon: label sync failed for aborted task '{}', quarantining repo: {e}",
                    task_on_disk.task_id
                );
                return Err(e);
            }
            let _ = self.release_task_lease(store_dir, repo_root, &task_on_disk.task_id, &lease);
            return Ok(());
        }

        let active_task = match DaemonTaskService::mark_active(
            self.store,
            store_dir,
            &claimed_task.task_id,
        ) {
            Ok(task) => task,
            Err(error) => {
                self.handle_post_claim_failure(
                    store_dir,
                    repo_root,
                    &claimed_task,
                    &lease,
                    &error,
                )?;
                // Sync label: Failed → rb:failed. On failure, mark label_dirty
                // and quarantine this repo for the rest of the cycle.
                let failed_task = self.store.read_task(store_dir, &claimed_task.task_id).ok();
                if let Some(ref ft) = failed_task {
                    if let Err(e) = github_intake::sync_label_for_task(github, ft).await {
                        let _ = DaemonTaskService::mark_label_dirty(
                            self.store,
                            store_dir,
                            &claimed_task.task_id,
                        );
                        eprintln!(
                                "daemon: label sync failed for failed task '{}', quarantining repo: {e}",
                                claimed_task.task_id
                            );
                        return Err(e);
                    }
                }
                println!("failed task {}: {}", claimed_task.task_id, error);
                return Ok(());
            }
        };
        println!("active task {}", active_task.task_id);
        // Sync label: Active → rb:in-progress. On failure, mark label_dirty and
        // quarantine this repo — no further task/lease/worktree mutations in this
        // cycle. The task remains Active with its lease; Phase 0 will repair
        // the label and revert the task to Pending for the next cycle.
        if let Err(e) = github_intake::sync_label_for_task(github, &active_task).await {
            let _ =
                DaemonTaskService::mark_label_dirty(self.store, store_dir, &active_task.task_id);
            eprintln!(
                "daemon: label sync failed for active task '{}', quarantining repo: {e}",
                active_task.task_id
            );
            return Err(e);
        }

        let task_cancel = CancellationToken::new();
        let outcome = self
            .drive_dispatch_multi_repo(
                store_dir,
                repo_root,
                &active_task,
                &lease,
                &effective_config,
                config,
                shutdown.clone(),
                task_cancel.clone(),
                github,
            )
            .await;

        let latest_task = self.store.read_task(store_dir, &active_task.task_id)?;
        if latest_task.status == TaskStatus::Aborted {
            // Sync label: Aborted → rb:failed. On failure, mark label_dirty and
            // quarantine — do NOT release the lease in this cycle so no further
            // mutations occur. Phase 0 will repair the label and release the lease.
            if let Err(e) = github_intake::sync_label_for_task(github, &latest_task).await {
                let _ = DaemonTaskService::mark_label_dirty(
                    self.store,
                    store_dir,
                    &active_task.task_id,
                );
                eprintln!(
                    "daemon: label sync failed for aborted task '{}', quarantining repo: {e}",
                    active_task.task_id
                );
                return Err(e);
            }
            let _ = self.release_task_lease(store_dir, repo_root, &active_task.task_id, &lease);
            return Ok(());
        }

        match outcome {
            Ok(()) => {
                match self
                    .handle_completion_pr_with_cancellation(
                        store_dir,
                        repo_root,
                        &active_task,
                        &lease,
                        &effective_config,
                        shutdown.clone(),
                        task_cancel,
                        github,
                    )
                    .await
                {
                    Ok(Some(_)) => {}
                    Ok(None) => {
                        let aborted_task = self.store.read_task(store_dir, &active_task.task_id)?;
                        if let Err(e) =
                            github_intake::sync_label_for_task(github, &aborted_task).await
                        {
                            let _ = DaemonTaskService::mark_label_dirty(
                                self.store,
                                store_dir,
                                &active_task.task_id,
                            );
                            eprintln!(
                                "daemon: label sync failed for aborted task '{}', quarantining repo: {e}",
                                active_task.task_id
                            );
                            return Err(e);
                        }
                        let _ = self.release_task_lease(
                            store_dir,
                            repo_root,
                            &active_task.task_id,
                            &lease,
                        );
                        return Ok(());
                    }
                    Err(error) => {
                        let failed_task = DaemonTaskService::mark_failed(
                            self.store,
                            store_dir,
                            &active_task.task_id,
                            "pr_runtime_failed",
                            &error.to_string(),
                        )?;
                        if let Err(e) =
                            github_intake::sync_label_for_task(github, &failed_task).await
                        {
                            let _ = DaemonTaskService::mark_label_dirty(
                                self.store,
                                store_dir,
                                &active_task.task_id,
                            );
                            eprintln!(
                                "daemon: label sync failed for failed task '{}', quarantining repo: {e}",
                                active_task.task_id
                            );
                            return Err(e);
                        }
                        let _ = self.release_task_lease(
                            store_dir,
                            repo_root,
                            &active_task.task_id,
                            &lease,
                        );
                        println!("failed task {}: {}", active_task.task_id, error);
                        return Ok(());
                    }
                }
                let completed_task =
                    DaemonTaskService::mark_completed(self.store, store_dir, &active_task.task_id)?;
                // Sync label: Completed → rb:completed. On failure, mark label_dirty
                // and quarantine — do NOT release the lease in this cycle so no
                // further mutations occur. Phase 0 will repair the label and
                // release the lease in the next cycle.
                if let Err(e) = github_intake::sync_label_for_task(github, &completed_task).await {
                    let _ = DaemonTaskService::mark_label_dirty(
                        self.store,
                        store_dir,
                        &active_task.task_id,
                    );
                    eprintln!(
                        "daemon: label sync failed for completed task '{}', quarantining repo: {e}",
                        active_task.task_id
                    );
                    return Err(e);
                }
                let _ = self.release_task_lease(store_dir, repo_root, &active_task.task_id, &lease);
                println!("completed task {}", active_task.task_id);
            }
            Err(error) => {
                let failure_class = error
                    .failure_class()
                    .map(|class| class.as_str().to_owned())
                    .unwrap_or_else(|| "daemon_dispatch_failed".to_owned());
                let failed_task = DaemonTaskService::mark_failed(
                    self.store,
                    store_dir,
                    &active_task.task_id,
                    &failure_class,
                    &error.to_string(),
                )?;
                // Sync label: Failed → rb:failed. On failure, mark label_dirty
                // and quarantine — do NOT release the lease in this cycle so no
                // further mutations occur. Phase 0 will repair the label and
                // release the lease in the next cycle.
                if let Err(e) = github_intake::sync_label_for_task(github, &failed_task).await {
                    let _ = DaemonTaskService::mark_label_dirty(
                        self.store,
                        store_dir,
                        &active_task.task_id,
                    );
                    eprintln!(
                        "daemon: label sync failed for failed task '{}', quarantining repo: {e}",
                        active_task.task_id
                    );
                    return Err(e);
                }
                let _ = self.release_task_lease(store_dir, repo_root, &active_task.task_id, &lease);
                println!("failed task {}: {}", active_task.task_id, error);
            }
        }

        Ok(())
    }

    async fn process_cycle(
        &self,
        base_dir: &Path,
        config: &DaemonLoopConfig,
        shutdown: CancellationToken,
    ) -> AppResult<bool> {
        // Phase 1: Poll watchers for new issue candidates before task processing
        self.poll_watchers(base_dir)?;

        // Phase 2: Check waiting tasks for completed requirements runs
        // In single-repo mode, no GitHub labels to sync — just check and resume.
        let _resumed = self.check_waiting_tasks(base_dir, base_dir)?;

        // Phase 3: Process all pending tasks in this cycle. A per-task claim
        // failure or writer-lock contention does not stop the scan; the daemon
        // continues with remaining eligible tasks.
        let pending_tasks: Vec<DaemonTask> = DaemonTaskService::list_tasks(self.store, base_dir)?
            .into_iter()
            .filter(|task| task.status == TaskStatus::Pending)
            .collect();
        if pending_tasks.is_empty() {
            return Ok(false);
        }

        for task in &pending_tasks {
            if let Err(error) = self
                .process_task(base_dir, task, config, shutdown.clone())
                .await
            {
                println!("daemon: task {} failed: {}", task.task_id, error);
                // Continue scanning remaining pending tasks
            }
        }
        Ok(true)
    }

    /// Label sync after a task state mutation inside `handle_requirements_draft`.
    /// On failure, marks the task `label_dirty` so `reconcile` can repair, and
    /// returns the error so the caller can decide whether to propagate it (for
    /// repo quarantine) or swallow it (when the task is already terminal).
    async fn sync_label_after_mutation<G: GithubPort>(
        &self,
        github: &G,
        base_dir: &Path,
        task_id: &str,
    ) -> AppResult<()> {
        let t = match self.store.read_task(base_dir, task_id) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("daemon: failed to read task '{task_id}' for label sync: {e}");
                return Err(e);
            }
        };
        if let Err(e) = github_intake::sync_label_for_task(github, &t).await {
            let _ = DaemonTaskService::mark_label_dirty(self.store, base_dir, task_id);
            eprintln!("daemon: label sync failed for requirements_draft task '{task_id}': {e}");
            return Err(e);
        }
        Ok(())
    }

    /// Poll external watchers and ingest new issue candidates as daemon tasks.
    fn poll_watchers(&self, base_dir: &Path) -> AppResult<()> {
        let Some(watcher) = self.watcher else {
            return Ok(());
        };

        let effective_config = EffectiveConfig::load(base_dir)?;
        let default_flow = effective_config.default_flow();

        let candidates = watcher.poll(base_dir)?;
        for issue in &candidates {
            let dispatch_mode = match watcher::resolve_dispatch_mode(issue) {
                Ok(mode) => mode,
                Err(e) => {
                    println!("watcher: skipping issue '{}': {}", issue.issue_ref, e);
                    continue;
                }
            };

            match DaemonTaskService::create_task_from_watched_issue(
                self.store,
                base_dir,
                &self.routing_engine,
                default_flow,
                issue,
                dispatch_mode,
                None, // file-based watcher: no GitHub metadata
            ) {
                Ok(Some(task)) => {
                    println!(
                        "watcher: ingested issue '{}' as task '{}' (dispatch={})",
                        issue.issue_ref, task.task_id, task.dispatch_mode
                    );
                }
                Ok(None) => {
                    // Idempotent no-op
                }
                Err(e) => {
                    println!(
                        "watcher: failed to ingest issue '{}': {}",
                        issue.issue_ref, e
                    );
                }
            }
        }

        Ok(())
    }

    /// Check tasks in WaitingForRequirements state and resume them if their
    /// linked requirements run has completed. Before resuming, derive the seed
    /// handoff and populate the task's project metadata so the next workflow
    /// dispatch cycle can create/resume the project correctly.
    /// Check waiting-for-requirements tasks and resume any whose requirements
    /// run is complete. Returns a list of task IDs that were resumed to Pending
    /// so callers can sync GitHub labels.
    fn check_waiting_tasks(&self, base_dir: &Path, workspace_dir: &Path) -> AppResult<Vec<String>> {
        let Some(req_store) = self.requirements_store else {
            return Ok(vec![]);
        };

        let mut resumed_task_ids = Vec::new();
        let tasks = DaemonTaskService::list_tasks(self.store, base_dir)?;
        for task in tasks {
            if task.status != TaskStatus::WaitingForRequirements {
                continue;
            }
            let Some(ref run_id) = task.requirements_run_id else {
                continue;
            };

            match req_service::is_requirements_run_complete(req_store, workspace_dir, run_id) {
                Ok(true) => {
                    // Derive seed handoff before resuming so task has project metadata
                    match req_service::extract_seed_handoff(req_store, workspace_dir, run_id) {
                        Ok(handoff) => {
                            // Populate task with seed-derived project metadata.
                            // Guard: if any post-seed write fails, the task
                            // transitions to `failed` while the requirements
                            // run and seed remain addressable.
                            let routed_flow = task.resolved_flow.unwrap_or(handoff.flow);
                            let metadata_result: AppResult<()> = (|| {
                                let mut t = self.store.read_task(base_dir, &task.task_id)?;
                                if handoff.flow != routed_flow {
                                    let warning = format!(
                                        "seed suggests flow '{}' but routed flow '{}' is authoritative",
                                        handoff.flow.as_str(),
                                        routed_flow.as_str()
                                    );
                                    t.routing_warnings.push(warning.clone());
                                    // Journal append is best-effort for warnings
                                    if let Err(je) = DaemonTaskService::append_journal_event(
                                        self.store,
                                        base_dir,
                                        super::model::DaemonJournalEventType::RoutingWarning,
                                        json!({
                                            "task_id": task.task_id,
                                            "warning": warning,
                                        }),
                                    ) {
                                        eprintln!(
                                            "daemon: warning: failed to append RoutingWarning journal event for task '{}': {je}",
                                            task.task_id
                                        );
                                    }
                                    println!("daemon: {warning} for task '{}'", task.task_id);
                                }
                                t.project_id = handoff.project_id;
                                t.project_name = Some(handoff.project_name);
                                t.prompt = Some(handoff.prompt_body);
                                t.resolved_flow = Some(routed_flow);
                                self.store.write_task(base_dir, &t)?;
                                Ok(())
                            })();
                            if let Err(e) = metadata_result {
                                let _ = DaemonTaskService::mark_failed(
                                    self.store,
                                    base_dir,
                                    &task.task_id,
                                    "requirements_linking_failed",
                                    &format!("post-seed metadata update failed: {e}"),
                                );
                                println!(
                                    "daemon: post-seed metadata update failed for task '{}': {e}",
                                    task.task_id
                                );
                                continue;
                            }

                            match DaemonTaskService::resume_from_waiting(
                                self.store,
                                base_dir,
                                &task.task_id,
                            ) {
                                Ok(_) => {
                                    println!(
                                        "daemon: resumed task '{}' from waiting (requirements run '{}' complete)",
                                        task.task_id, run_id
                                    );
                                    resumed_task_ids.push(task.task_id.clone());
                                }
                                Err(e) => {
                                    println!(
                                        "daemon: failed to resume task '{}': {}",
                                        task.task_id, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            // Seed extraction failed — fail the task but preserve the requirements run
                            let _ = DaemonTaskService::mark_failed(
                                self.store,
                                base_dir,
                                &task.task_id,
                                "seed_handoff_failed",
                                &e.to_string(),
                            );
                            println!(
                                "daemon: seed handoff failed for task '{}': {}",
                                task.task_id, e
                            );
                        }
                    }
                }
                Ok(false) => {
                    // Still waiting — no action
                }
                Err(e) => {
                    println!(
                        "daemon: error checking requirements run '{}' for task '{}': {}",
                        run_id, task.task_id, e
                    );
                }
            }
        }

        Ok(resumed_task_ids)
    }

    async fn process_task(
        &self,
        base_dir: &Path,
        task: &DaemonTask,
        config: &DaemonLoopConfig,
        shutdown: CancellationToken,
    ) -> AppResult<()> {
        let repo_root = base_dir;
        let effective_config = self.load_effective_config_for_task(base_dir, task)?;
        let default_flow = effective_config.default_flow();

        // For requirements dispatch modes, handle before claiming lease/worktree.
        // requirements_quick completes the requirements run, derives the seed,
        // updates the task to Workflow mode, then falls through to the standard
        // claim/project/dispatch path in the same cycle.
        // requirements_draft enters WaitingForRequirements and returns immediately.
        match task.dispatch_mode {
            DispatchMode::RequirementsQuick => {
                self.handle_requirements_quick(base_dir, base_dir, task, &effective_config)
                    .await?;
                // Task is now Workflow mode with project metadata populated.
                // Fall through to standard claim/dispatch below.
            }
            DispatchMode::RequirementsDraft => {
                // Single-repo path is test-only; file-watcher tasks have no
                // repo_slug so sync_label_for_task will no-op. Use a no-op
                // in-memory GitHub client to satisfy the generic bound.
                let noop_gh = crate::adapters::github::InMemoryGithubClient::new();
                return self
                    .handle_requirements_draft(
                        base_dir,
                        base_dir,
                        task,
                        &effective_config,
                        &noop_gh,
                    )
                    .await;
            }
            DispatchMode::Workflow => {
                // Fall through to standard workflow dispatch
            }
        }

        // Re-read task from disk (may have been updated by requirements_quick)
        let task = &self.store.read_task(base_dir, &task.task_id)?;

        let (claimed_task, lease) = match DaemonTaskService::claim_task(
            self.store,
            self.worktree,
            &self.routing_engine,
            base_dir,
            repo_root,
            &task.task_id,
            default_flow,
            config.lease_ttl.as_secs(),
            None,
            None,
        ) {
            Ok(value) => value,
            Err(AppError::ProjectWriterLockHeld { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };

        println!("claimed task {}", claimed_task.task_id);

        if let Err(error) = self.rebase_task_worktree(
            base_dir,
            repo_root,
            &claimed_task,
            &lease,
            &effective_config,
        ) {
            println!("failed task {}: {}", claimed_task.task_id, error);
            return Ok(());
        }

        if let Err(error) = self.ensure_project(base_dir, &claimed_task) {
            self.handle_post_claim_failure(base_dir, repo_root, &claimed_task, &lease, &error)?;
            println!("failed task {}: {}", claimed_task.task_id, error);
            return Ok(());
        }
        let task_on_disk = self.store.read_task(base_dir, &claimed_task.task_id)?;
        if task_on_disk.status == TaskStatus::Aborted {
            let _ = self.release_task_lease(base_dir, repo_root, &task_on_disk.task_id, &lease);
            return Ok(());
        }

        let active_task =
            match DaemonTaskService::mark_active(self.store, base_dir, &claimed_task.task_id) {
                Ok(task) => task,
                Err(error) => {
                    self.handle_post_claim_failure(
                        base_dir,
                        repo_root,
                        &claimed_task,
                        &lease,
                        &error,
                    )?;
                    println!("failed task {}: {}", claimed_task.task_id, error);
                    return Ok(());
                }
            };
        println!("active task {}", active_task.task_id);

        let task_cancel = CancellationToken::new();
        let outcome = self
            .drive_dispatch(
                base_dir,
                base_dir,
                &active_task,
                &lease,
                &effective_config,
                config,
                shutdown.clone(),
                task_cancel.clone(),
            )
            .await;

        let latest_task = self.store.read_task(base_dir, &active_task.task_id)?;
        if latest_task.status == TaskStatus::Aborted {
            let _ = self.release_task_lease(base_dir, repo_root, &active_task.task_id, &lease);
            return Ok(());
        }

        match outcome {
            Ok(()) => {
                if task.repo_slug.is_some() {
                    let noop_gh = crate::adapters::github::InMemoryGithubClient::new();
                    if self
                        .handle_completion_pr_with_cancellation(
                            base_dir,
                            repo_root,
                            &active_task,
                            &lease,
                            &effective_config,
                            shutdown.clone(),
                            task_cancel,
                            &noop_gh,
                        )
                        .await?
                        .is_none()
                    {
                        let _ = self.release_task_lease(
                            base_dir,
                            repo_root,
                            &active_task.task_id,
                            &lease,
                        );
                        return Ok(());
                    }
                }
                let _ =
                    DaemonTaskService::mark_completed(self.store, base_dir, &active_task.task_id)?;
                let _ = self.release_task_lease(base_dir, repo_root, &active_task.task_id, &lease);
                println!("completed task {}", active_task.task_id);
            }
            Err(error) => {
                let failure_class = error
                    .failure_class()
                    .map(|class| class.as_str().to_owned())
                    .unwrap_or_else(|| "daemon_dispatch_failed".to_owned());
                let _ = DaemonTaskService::mark_failed(
                    self.store,
                    base_dir,
                    &active_task.task_id,
                    &failure_class,
                    &error.to_string(),
                )?;
                let _ = self.release_task_lease(base_dir, repo_root, &active_task.task_id, &lease);
                println!("failed task {}: {}", active_task.task_id, error);
            }
        }

        Ok(())
    }

    /// Handle requirements_quick dispatch: invoke requirements quick, link the
    /// run ID to the task, derive seed, and update the task with project metadata
    /// + Workflow mode so the caller can continue into the standard claim/project/
    /// dispatch path in the same daemon cycle.
    async fn handle_requirements_quick(
        &self,
        base_dir: &Path,
        workspace_dir: &Path,
        task: &DaemonTask,
        effective_config: &EffectiveConfig,
    ) -> AppResult<()> {
        let req_store =
            self.requirements_store
                .ok_or_else(|| AppError::RequirementsHandoffFailed {
                    task_id: task.task_id.clone(),
                    details: "no requirements store configured for daemon".to_owned(),
                })?;

        let idea = task
            .prompt
            .clone()
            .unwrap_or_else(|| format!("Automated task for issue {}", task.issue_ref));

        // Build a fresh requirements service with workspace defaults (same as CLI path)
        let req_svc = build_requirements_service_default(effective_config).map_err(|e| {
            let _ = DaemonTaskService::mark_failed(
                self.store,
                base_dir,
                &task.task_id,
                "requirements_quick_failed",
                &format!("failed to build requirements service: {e}"),
            );
            e
        })?;
        let run_id = match req_svc.quick(workspace_dir, &idea, Utc::now()).await {
            Ok(run_id) => run_id,
            Err(e) => {
                let _ = DaemonTaskService::mark_failed(
                    self.store,
                    base_dir,
                    &task.task_id,
                    "requirements_quick_failed",
                    &e.to_string(),
                );
                return Err(e);
            }
        };

        // Link requirements run to task. If the link write fails, the
        // requirements run remains addressable via `requirements show` but the
        // task must transition to `failed` with an explicit linking failure class.
        let link_result: AppResult<()> = (|| {
            let mut current = self.store.read_task(base_dir, &task.task_id)?;
            current.requirements_run_id = Some(run_id.clone());
            current.updated_at = Utc::now();
            self.store.write_task(base_dir, &current)?;

            DaemonTaskService::append_journal_event(
                self.store,
                base_dir,
                super::model::DaemonJournalEventType::RequirementsHandoff,
                json!({
                    "task_id": task.task_id,
                    "requirements_run_id": run_id,
                    "dispatch_mode": "requirements_quick",
                }),
            )?;
            Ok(())
        })();
        if let Err(e) = link_result {
            let _ = DaemonTaskService::mark_failed(
                self.store,
                base_dir,
                &task.task_id,
                "requirements_linking_failed",
                &e.to_string(),
            );
            return Err(e);
        }

        // Derive seed and create project from completed requirements run
        let handoff = match req_service::extract_seed_handoff(req_store, workspace_dir, &run_id) {
            Ok(h) => h,
            Err(e) => {
                let _ = DaemonTaskService::mark_failed(
                    self.store,
                    base_dir,
                    &task.task_id,
                    "seed_handoff_failed",
                    &e.to_string(),
                );
                return Err(e);
            }
        };

        // Use routed flow, not the seed's recommended flow (routed flow is authoritative)
        let routed_flow = task.resolved_flow.unwrap_or(handoff.flow);

        // Update task to workflow mode with seed-derived project metadata.
        // The caller will continue into the standard claim/project/dispatch path.
        // Guard: if any post-link write fails, the task transitions to `failed`
        // with an explicit failure class while the requirements run and seed
        // remain addressable.
        let metadata_result: AppResult<()> = (|| {
            let mut updated = self.store.read_task(base_dir, &task.task_id)?;
            if handoff.flow != routed_flow {
                let warning = format!(
                    "seed suggests flow '{}' but routed flow '{}' is authoritative",
                    handoff.flow.as_str(),
                    routed_flow.as_str()
                );
                updated.routing_warnings.push(warning.clone());
                // Journal append is best-effort for warnings
                if let Err(je) = DaemonTaskService::append_journal_event(
                    self.store,
                    base_dir,
                    super::model::DaemonJournalEventType::RoutingWarning,
                    json!({
                        "task_id": task.task_id,
                        "warning": warning,
                    }),
                ) {
                    eprintln!(
                        "daemon: warning: failed to append RoutingWarning journal event for task '{}': {je}",
                        task.task_id
                    );
                }
                println!("daemon: {warning}");
            }
            updated.dispatch_mode = DispatchMode::Workflow;
            updated.resolved_flow = Some(routed_flow);
            updated.project_id = handoff.project_id.clone();
            updated.project_name = Some(handoff.project_name.clone());
            updated.prompt = Some(handoff.prompt_body.clone());
            self.store.write_task(base_dir, &updated)?;
            Ok(())
        })();
        if let Err(e) = metadata_result {
            let _ = DaemonTaskService::mark_failed(
                self.store,
                base_dir,
                &task.task_id,
                "requirements_linking_failed",
                &format!("post-link metadata update failed: {e}"),
            );
            return Err(e);
        }

        println!(
            "daemon: requirements_quick seed ready for task '{}', run_id='{}', continuing to workflow",
            task.task_id, run_id
        );

        Ok(())
    }

    /// Handle requirements_draft dispatch: transition through Pending → Claimed
    /// → Active, invoke requirements draft to generate questions, then either
    /// transition to WaitingForRequirements (if questions need answers) or
    /// extract the seed and switch to Workflow mode (if the run completed
    /// directly with empty questions).
    async fn handle_requirements_draft<G: GithubPort>(
        &self,
        base_dir: &Path,
        workspace_dir: &Path,
        task: &DaemonTask,
        effective_config: &EffectiveConfig,
        github: &G,
    ) -> AppResult<()> {
        let req_store =
            self.requirements_store
                .ok_or_else(|| AppError::RequirementsHandoffFailed {
                    task_id: task.task_id.clone(),
                    details: "no requirements store configured for daemon".to_owned(),
                })?;

        // Transition through Pending → Claimed → Active without a worktree lease.
        // The draft path only needs the agent to generate questions — no project,
        // worktree, or writer lock is required.
        {
            let mut t = self.store.read_task(base_dir, &task.task_id)?;
            let now = Utc::now();
            t.transition_to(TaskStatus::Claimed, now)?;
            t.transition_to(TaskStatus::Active, now)?;
            self.store.write_task(base_dir, &t)?;

            // Sync label: Active → rb:in-progress immediately, so the issue
            // reflects truthful durable state during the draft run rather than
            // remaining on rb:ready until the draft completes.
            if let Err(e) = github_intake::sync_label_for_task(github, &t).await {
                let _ = DaemonTaskService::mark_label_dirty(self.store, base_dir, &task.task_id);
                eprintln!(
                    "daemon: label sync failed for requirements_draft task '{}', quarantining repo: {e}",
                    task.task_id
                );
                return Err(e);
            }
        }

        let idea = task
            .prompt
            .clone()
            .unwrap_or_else(|| format!("Automated task for issue {}", task.issue_ref));

        let req_svc = match build_requirements_service_default(effective_config) {
            Ok(svc) => svc,
            Err(e) => {
                let _ = DaemonTaskService::mark_failed(
                    self.store,
                    base_dir,
                    &task.task_id,
                    "requirements_draft_failed",
                    &format!("failed to build requirements service: {e}"),
                );
                let _ = self
                    .sync_label_after_mutation(github, base_dir, &task.task_id)
                    .await;
                return Err(e);
            }
        };
        let run_id = match req_svc.draft(workspace_dir, &idea, Utc::now()).await {
            Ok(run_id) => run_id,
            Err(e) => {
                let _ = DaemonTaskService::mark_failed(
                    self.store,
                    base_dir,
                    &task.task_id,
                    "requirements_draft_failed",
                    &e.to_string(),
                );
                let _ = self
                    .sync_label_after_mutation(github, base_dir, &task.task_id)
                    .await;
                return Err(e);
            }
        };

        // Check the requirements run status after draft() completes.
        // If the question set was empty, the run completes directly (no user
        // answers needed). Only enter WaitingForRequirements when answers are
        // actually pending.
        let run_complete =
            req_service::is_requirements_run_complete(req_store, workspace_dir, &run_id)?;

        if run_complete {
            // Empty-question draft: run already completed. Extract seed and
            // switch to Workflow mode so the caller continues into the standard
            // claim/project/dispatch path (same pattern as requirements_quick).
            //
            // Link write is guarded: if it fails, the task transitions to
            // `failed` with an explicit linking failure class while the
            // requirements run remains addressable via `requirements show`.
            let link_result: AppResult<()> = (|| {
                let mut current = self.store.read_task(base_dir, &task.task_id)?;
                current.requirements_run_id = Some(run_id.clone());
                current.updated_at = Utc::now();
                self.store.write_task(base_dir, &current)?;

                DaemonTaskService::append_journal_event(
                    self.store,
                    base_dir,
                    super::model::DaemonJournalEventType::RequirementsHandoff,
                    json!({
                        "task_id": task.task_id,
                        "requirements_run_id": run_id,
                        "dispatch_mode": "requirements_draft",
                        "empty_questions": true,
                    }),
                )?;
                Ok(())
            })();
            if let Err(e) = link_result {
                let _ = DaemonTaskService::mark_failed(
                    self.store,
                    base_dir,
                    &task.task_id,
                    "requirements_linking_failed",
                    &e.to_string(),
                );
                let _ = self
                    .sync_label_after_mutation(github, base_dir, &task.task_id)
                    .await;
                return Err(e);
            }

            let handoff = match req_service::extract_seed_handoff(req_store, workspace_dir, &run_id)
            {
                Ok(h) => h,
                Err(e) => {
                    let _ = DaemonTaskService::mark_failed(
                        self.store,
                        base_dir,
                        &task.task_id,
                        "seed_handoff_failed",
                        &e.to_string(),
                    );
                    let _ = self
                        .sync_label_after_mutation(github, base_dir, &task.task_id)
                        .await;
                    return Err(e);
                }
            };

            let routed_flow = task.resolved_flow.unwrap_or(handoff.flow);

            // Guard: if any post-link write fails, the task transitions to
            // `failed` with an explicit failure class while the requirements
            // run and seed remain addressable.
            let metadata_result: AppResult<()> = (|| {
                let mut updated = self.store.read_task(base_dir, &task.task_id)?;
                if handoff.flow != routed_flow {
                    let warning = format!(
                        "seed suggests flow '{}' but routed flow '{}' is authoritative",
                        handoff.flow.as_str(),
                        routed_flow.as_str()
                    );
                    updated.routing_warnings.push(warning.clone());
                    // Journal append is best-effort for warnings
                    if let Err(je) = DaemonTaskService::append_journal_event(
                        self.store,
                        base_dir,
                        super::model::DaemonJournalEventType::RoutingWarning,
                        json!({
                            "task_id": task.task_id,
                            "warning": warning,
                        }),
                    ) {
                        eprintln!(
                            "daemon: warning: failed to append RoutingWarning journal event for task '{}': {je}",
                            task.task_id
                        );
                    }
                    println!("daemon: {warning}");
                }
                updated.dispatch_mode = DispatchMode::Workflow;
                updated.resolved_flow = Some(routed_flow);
                updated.project_id = handoff.project_id.clone();
                updated.project_name = Some(handoff.project_name.clone());
                updated.prompt = Some(handoff.prompt_body.clone());
                self.store.write_task(base_dir, &updated)?;
                Ok(())
            })();
            if let Err(e) = metadata_result {
                let _ = DaemonTaskService::mark_failed(
                    self.store,
                    base_dir,
                    &task.task_id,
                    "requirements_linking_failed",
                    &format!("post-link metadata update failed: {e}"),
                );
                let _ = self
                    .sync_label_after_mutation(github, base_dir, &task.task_id)
                    .await;
                return Err(e);
            }

            // Transition Active → Pending so the next daemon cycle picks this
            // task up as a standard Workflow task with project metadata already
            // populated. We cannot fall through to claim/dispatch in the same
            // cycle because handle_requirements_draft returns (not falls through)
            // and the task has no lease or worktree yet.
            {
                let mut t = self.store.read_task(base_dir, &task.task_id)?;
                t.transition_to(TaskStatus::Pending, Utc::now())?;
                self.store.write_task(base_dir, &t)?;
            }

            // Sync label: Pending → rb:ready (requeued for workflow dispatch).
            // Propagate failure to quarantine the repo for the rest of the cycle.
            self.sync_label_after_mutation(github, base_dir, &task.task_id)
                .await?;

            println!(
                "daemon: requirements_draft completed directly (empty questions) for task '{}', run_id='{}', requeued for workflow",
                task.task_id, run_id
            );

            Ok(())
        } else {
            // Non-empty questions: transition Active → WaitingForRequirements
            match DaemonTaskService::mark_waiting_for_requirements(
                self.store,
                base_dir,
                &task.task_id,
                &run_id,
            ) {
                Ok(_) => {
                    // Journal append is supplementary — linking already
                    // succeeded inside mark_waiting_for_requirements.
                    // Log but do not propagate failure.
                    if let Err(e) = DaemonTaskService::append_journal_event(
                        self.store,
                        base_dir,
                        super::model::DaemonJournalEventType::RequirementsHandoff,
                        json!({
                            "task_id": task.task_id,
                            "requirements_run_id": run_id,
                            "dispatch_mode": "requirements_draft",
                        }),
                    ) {
                        eprintln!(
                            "daemon: warning: failed to append RequirementsHandoff journal event for task '{}': {e}",
                            task.task_id
                        );
                    }
                    // Sync label: WaitingForRequirements → rb:waiting-feedback.
                    // Propagate failure to quarantine the repo for the rest of the cycle.
                    self.sync_label_after_mutation(github, base_dir, &task.task_id)
                        .await?;

                    println!(
                        "daemon: requirements_draft started for task '{}', waiting for answers (run_id='{}')",
                        task.task_id, run_id
                    );
                }
                Err(e) => {
                    // Requirements run is still addressable via `requirements show`
                    let _ = DaemonTaskService::mark_failed(
                        self.store,
                        base_dir,
                        &task.task_id,
                        "requirements_linking_failed",
                        &e.to_string(),
                    );
                    let _ = self
                        .sync_label_after_mutation(github, base_dir, &task.task_id)
                        .await;
                    return Err(e);
                }
            }

            Ok(())
        }
    }

    async fn drive_dispatch_multi_repo<G: GithubPort>(
        &self,
        base_dir: &Path,
        workspace_dir: &Path,
        task: &DaemonTask,
        lease: &crate::contexts::automation_runtime::model::WorktreeLease,
        effective_config: &EffectiveConfig,
        config: &DaemonLoopConfig,
        shutdown: CancellationToken,
        task_cancel: CancellationToken,
        github: &G,
    ) -> AppResult<()> {
        let project_id = ProjectId::new(task.project_id.clone())?;
        let flow = task
            .resolved_flow
            .ok_or_else(|| AppError::RoutingResolutionFailed {
                input: task.task_id.clone(),
                details: "task has no resolved flow".to_owned(),
            })?;

        let run_snapshot = self
            .run_snapshot_read
            .read_run_snapshot(workspace_dir, &project_id)?;
        let dispatch_future = self.dispatch_in_worktree(
            workspace_dir,
            &project_id,
            flow,
            run_snapshot.status,
            effective_config,
            &lease.worktree_path,
            task_cancel.clone(),
        );
        tokio::pin!(dispatch_future);

        let heartbeat_interval = config.heartbeat_interval.max(Duration::from_secs(1));
        let mut heartbeat = tokio::time::interval(heartbeat_interval);
        let mut abort_poll = tokio::time::interval(Duration::from_millis(250));
        let mut draft_pr_poll =
            tokio::time::interval(heartbeat_interval.min(Duration::from_secs(5)));
        let pr_runtime = PrRuntimeService::new(self.store, self.worktree, github);

        loop {
            tokio::select! {
                result = &mut dispatch_future => break result,
                _ = heartbeat.tick() => {
                    let _ = LeaseService::heartbeat(self.store, base_dir, &lease.lease_id);
                }
                _ = draft_pr_poll.tick() => {
                    match pr_runtime
                        .ensure_draft_pr(base_dir, workspace_dir, &task.task_id, lease, &task_cancel)
                        .await
                    {
                        Ok(_) | Err(AppError::InvocationCancelled { .. }) => {}
                        Err(error) => break Err(error),
                    }
                }
                _ = abort_poll.tick() => {
                    let current = self.store.read_task(base_dir, &task.task_id)?;
                    if current.status == TaskStatus::Aborted {
                        task_cancel.cancel();
                    }
                }
                _ = shutdown.cancelled() => {
                    let _ = DaemonTaskService::mark_aborted(self.store, base_dir, &task.task_id);
                    task_cancel.cancel();
                }
            }
        }
    }

    async fn drive_dispatch(
        &self,
        base_dir: &Path,
        workspace_dir: &Path,
        task: &DaemonTask,
        lease: &crate::contexts::automation_runtime::model::WorktreeLease,
        effective_config: &EffectiveConfig,
        config: &DaemonLoopConfig,
        shutdown: CancellationToken,
        task_cancel: CancellationToken,
    ) -> AppResult<()> {
        let project_id = ProjectId::new(task.project_id.clone())?;
        let flow = task
            .resolved_flow
            .ok_or_else(|| AppError::RoutingResolutionFailed {
                input: task.task_id.clone(),
                details: "task has no resolved flow".to_owned(),
            })?;

        let run_snapshot = self
            .run_snapshot_read
            .read_run_snapshot(workspace_dir, &project_id)?;
        let dispatch_future = self.dispatch_in_worktree(
            workspace_dir,
            &project_id,
            flow,
            run_snapshot.status,
            effective_config,
            &lease.worktree_path,
            task_cancel.clone(),
        );
        tokio::pin!(dispatch_future);

        let heartbeat_interval = config.heartbeat_interval.max(Duration::from_secs(1));
        let mut heartbeat = tokio::time::interval(heartbeat_interval);
        let mut abort_poll = tokio::time::interval(Duration::from_millis(250));

        loop {
            tokio::select! {
                result = &mut dispatch_future => break result,
                _ = heartbeat.tick() => {
                    let _ = LeaseService::heartbeat(self.store, base_dir, &lease.lease_id);
                }
                _ = abort_poll.tick() => {
                    let current = self.store.read_task(base_dir, &task.task_id)?;
                    if current.status == TaskStatus::Aborted {
                        task_cancel.cancel();
                    }
                }
                _ = shutdown.cancelled() => {
                    let _ = DaemonTaskService::mark_aborted(self.store, base_dir, &task.task_id);
                    task_cancel.cancel();
                }
            }
        }
    }

    async fn dispatch_in_worktree(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        flow: FlowPreset,
        run_status: RunStatus,
        effective_config: &EffectiveConfig,
        worktree_path: &Path,
        cancellation_token: CancellationToken,
    ) -> AppResult<()> {
        match run_status {
            RunStatus::NotStarted => {
                engine::execute_run_with_retry(
                    self.agent_service,
                    self.run_snapshot_read,
                    self.run_snapshot_write,
                    self.journal_store,
                    self.artifact_write,
                    self.log_write,
                    self.amendment_queue,
                    base_dir,
                    Some(worktree_path),
                    project_id,
                    flow,
                    effective_config,
                    &RetryPolicy::default_policy(),
                    cancellation_token,
                )
                .await
            }
            RunStatus::Failed | RunStatus::Paused => {
                engine::resume_run_with_retry(
                    self.agent_service,
                    self.run_snapshot_read,
                    self.run_snapshot_write,
                    self.journal_store,
                    self.artifact_store,
                    self.artifact_write,
                    self.log_write,
                    self.amendment_queue,
                    base_dir,
                    Some(worktree_path),
                    project_id,
                    flow,
                    effective_config,
                    &RetryPolicy::default_policy(),
                    cancellation_token,
                )
                .await
            }
            RunStatus::Running => Err(AppError::TaskStateTransitionInvalid {
                task_id: project_id.to_string(),
                from: "run_running".to_owned(),
                to: "daemon_dispatch".to_owned(),
            }),
            RunStatus::Completed => Err(AppError::TaskStateTransitionInvalid {
                task_id: project_id.to_string(),
                from: "run_completed".to_owned(),
                to: "daemon_dispatch".to_owned(),
            }),
        }
    }

    fn ensure_project(&self, base_dir: &Path, task: &DaemonTask) -> AppResult<()> {
        let project_id = ProjectId::new(task.project_id.clone())?;
        if self.project_store.project_exists(base_dir, &project_id)? {
            let record = self
                .project_store
                .read_project_record(base_dir, &project_id)?;
            let flow = task
                .resolved_flow
                .ok_or_else(|| AppError::RoutingResolutionFailed {
                    input: task.task_id.clone(),
                    details: "task has no resolved flow".to_owned(),
                })?;
            if record.flow != flow {
                return Err(AppError::RoutingResolutionFailed {
                    input: task.task_id.clone(),
                    details: format!(
                        "existing project flow '{}' does not match routed flow '{}'",
                        record.flow.as_str(),
                        flow.as_str()
                    ),
                });
            }
            return Ok(());
        }

        let prompt_contents = task.prompt.clone().unwrap_or_else(|| {
            format!(
                "# Automated task {}\n\nIssue: {}\n",
                task.task_id, task.issue_ref
            )
        });
        let flow = task
            .resolved_flow
            .ok_or_else(|| AppError::RoutingResolutionFailed {
                input: task.task_id.clone(),
                details: "task has no resolved flow".to_owned(),
            })?;
        let name = task
            .project_name
            .clone()
            .unwrap_or_else(|| format!("Task {}", task.issue_ref));
        let input = CreateProjectInput {
            id: project_id,
            name,
            flow,
            prompt_path: "daemon".to_owned(),
            prompt_contents: prompt_contents.clone(),
            prompt_hash: FileSystem::prompt_hash(&prompt_contents),
            created_at: Utc::now(),
        };
        create_project(self.project_store, self.journal_store, base_dir, input)?;
        Ok(())
    }

    fn cleanup_active_leases(&self, store_dir: &Path, repo_root: &Path) -> AppResult<()> {
        let leases = self.store.list_leases(store_dir)?;
        for lease in &leases {
            let _ = DaemonTaskService::mark_aborted(self.store, store_dir, &lease.task_id);
            if let Err(e) = self.release_task_lease(store_dir, repo_root, &lease.task_id, lease) {
                eprintln!(
                    "daemon: cleanup failed for lease '{}' (task '{}'): {}",
                    lease.lease_id, lease.task_id, e
                );
            }
        }
        Ok(())
    }

    async fn ingest_pr_feedback_for_repo<G: GithubPort>(
        &self,
        store_dir: &Path,
        repo_root: &Path,
        github: &G,
        shutdown: CancellationToken,
    ) -> AppResult<()> {
        let service = PrReviewIngestionService::new(
            self.store,
            self.project_store,
            self.run_snapshot_read,
            self.run_snapshot_write,
            self.amendment_queue,
            github,
        );
        let tasks = DaemonTaskService::list_tasks(self.store, store_dir)?;
        for task in tasks.into_iter().filter(|task| {
            matches!(task.status, TaskStatus::Active | TaskStatus::Completed)
                && task.pr_url.is_some()
        }) {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            let effective_config = self.load_effective_config_for_task(repo_root, &task)?;
            let whitelist = super::model::ReviewWhitelist::from_config(
                &effective_config.daemon_pr_policy().review_whitelist,
            );
            let batch = service
                .ingest_reviews(store_dir, repo_root, &task.task_id, &whitelist, &shutdown)
                .await?;
            if batch.reopened_project {
                let reopened = self.store.read_task(store_dir, &task.task_id)?;
                if reopened.repo_slug.is_some() && reopened.issue_number.is_some() {
                    let _ = github_intake::sync_label_for_task(github, &reopened).await;
                }
            }
        }

        Ok(())
    }

    fn rebase_task_worktree(
        &self,
        base_dir: &Path,
        repo_root: &Path,
        task: &DaemonTask,
        lease: &crate::contexts::automation_runtime::model::WorktreeLease,
        effective_config: &EffectiveConfig,
    ) -> AppResult<()> {
        DaemonTaskService::append_journal_event(
            self.store,
            base_dir,
            super::model::DaemonJournalEventType::RebaseStarted,
            json!({
                "task_id": task.task_id,
                "branch_name": lease.branch_name,
            }),
        )?;

        let resolver = if effective_config.rebase_policy().agent_resolution_enabled {
            Some(self.build_rebase_conflict_resolver(
                repo_root,
                &lease.worktree_path,
                task,
                effective_config,
            )?)
        } else {
            None
        };
        let outcome = self.worktree.rebase_with_agent_resolution(
            repo_root,
            &lease.worktree_path,
            &lease.branch_name,
            effective_config.rebase_policy(),
            resolver
                .as_ref()
                .map(|resolver| resolver as &dyn RebaseConflictResolver),
        )?;

        match outcome {
            RebaseOutcome::Success => {
                DaemonTaskService::append_journal_event(
                    self.store,
                    base_dir,
                    super::model::DaemonJournalEventType::RebaseCompleted,
                    json!({
                        "task_id": task.task_id,
                        "branch_name": lease.branch_name,
                        "outcome": "success",
                    }),
                )?;
                Ok(())
            }
            RebaseOutcome::AgentResolved {
                resolved_files,
                summary,
            } => {
                DaemonTaskService::append_journal_event(
                    self.store,
                    base_dir,
                    super::model::DaemonJournalEventType::RebaseConflict,
                    json!({
                        "task_id": task.task_id,
                        "branch_name": lease.branch_name,
                        "classification": "conflict",
                    }),
                )?;
                DaemonTaskService::append_journal_event(
                    self.store,
                    base_dir,
                    super::model::DaemonJournalEventType::RebaseAgentResolution,
                    json!({
                        "task_id": task.task_id,
                        "branch_name": lease.branch_name,
                        "resolved_files": resolved_files,
                        "summary": summary,
                    }),
                )?;
                DaemonTaskService::append_journal_event(
                    self.store,
                    base_dir,
                    super::model::DaemonJournalEventType::RebaseCompleted,
                    json!({
                        "task_id": task.task_id,
                        "branch_name": lease.branch_name,
                        "outcome": "agent_resolved",
                    }),
                )?;
                Ok(())
            }
            RebaseOutcome::Failed {
                classification,
                details,
            } => {
                DaemonTaskService::append_journal_event(
                    self.store,
                    base_dir,
                    super::model::DaemonJournalEventType::RebaseConflict,
                    json!({
                        "task_id": task.task_id,
                        "branch_name": lease.branch_name,
                        "classification": match classification {
                            RebaseFailureClassification::Conflict => "conflict",
                            RebaseFailureClassification::Timeout => "timeout",
                            RebaseFailureClassification::Unknown => "unknown",
                        },
                        "details": details,
                    }),
                )?;
                self.fail_claimed_task_preserve_worktree(
                    base_dir,
                    &task.task_id,
                    match classification {
                        RebaseFailureClassification::Timeout => "rebase_timeout",
                        RebaseFailureClassification::Conflict => "rebase_conflict",
                        RebaseFailureClassification::Unknown => "rebase_failed",
                    },
                    &details,
                )?;
                Err(AppError::RebaseConflict {
                    branch_name: lease.branch_name.clone(),
                    details,
                })
            }
        }
    }

    fn load_effective_config_for_task(
        &self,
        repo_root: &Path,
        task: &DaemonTask,
    ) -> AppResult<EffectiveConfig> {
        let project_id = ProjectId::new(task.project_id.clone())?;
        EffectiveConfig::load_for_project(repo_root, Some(&project_id), Default::default())
    }

    async fn handle_completion_pr_with_cancellation<G: GithubPort>(
        &self,
        base_dir: &Path,
        repo_root: &Path,
        task: &DaemonTask,
        lease: &crate::contexts::automation_runtime::model::WorktreeLease,
        effective_config: &EffectiveConfig,
        shutdown: CancellationToken,
        task_cancel: CancellationToken,
        github: &G,
    ) -> AppResult<Option<crate::contexts::automation_runtime::CompletionPrAction>> {
        let pr_runtime = PrRuntimeService::new(self.store, self.worktree, github);
        let completion_future = pr_runtime.handle_completion_pr(
            base_dir,
            repo_root,
            &task.task_id,
            lease,
            effective_config.daemon_pr_policy(),
            &task_cancel,
        );
        tokio::pin!(completion_future);

        let mut abort_poll = tokio::time::interval(Duration::from_millis(250));
        let result = loop {
            tokio::select! {
                result = &mut completion_future => break result,
                _ = abort_poll.tick() => {
                    let current = self.store.read_task(base_dir, &task.task_id)?;
                    if current.status == TaskStatus::Aborted {
                        task_cancel.cancel();
                    }
                }
                _ = shutdown.cancelled() => {
                    let _ = DaemonTaskService::mark_aborted(self.store, base_dir, &task.task_id);
                    task_cancel.cancel();
                }
            }
        };

        match result {
            Ok(action) => Ok(Some(action)),
            Err(AppError::InvocationCancelled { .. }) => {
                let current = self.store.read_task(base_dir, &task.task_id)?;
                if current.status != TaskStatus::Aborted {
                    let _ = DaemonTaskService::mark_aborted(self.store, base_dir, &task.task_id);
                }
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    fn build_rebase_conflict_resolver(
        &self,
        repo_root: &Path,
        worktree_path: &Path,
        task: &DaemonTask,
        effective_config: &EffectiveConfig,
    ) -> AppResult<DaemonRebaseConflictResolver<'_, A, R, S>> {
        let project_id = ProjectId::new(task.project_id.clone())?;
        let cycle = self
            .run_snapshot_read
            .read_run_snapshot(repo_root, &project_id)
            .ok()
            .and_then(rebase_cycle_from_snapshot)
            .unwrap_or(1)
            .max(1);
        let policy = BackendPolicyService::new(effective_config);
        let target = policy.resolve_role_target(BackendPolicyRole::Implementer, cycle)?;

        Ok(DaemonRebaseConflictResolver {
            agent_service: self.agent_service,
            project_root: repo_root
                .join(WORKSPACE_DIR)
                .join("runtime")
                .join("rebase-agent")
                .join(&task.task_id),
            working_dir: worktree_path.to_path_buf(),
            target,
            timeout: Duration::from_secs(effective_config.rebase_policy().agent_timeout),
            task_id: task.task_id.clone(),
        })
    }

    fn handle_post_claim_failure(
        &self,
        base_dir: &Path,
        repo_root: &Path,
        task: &DaemonTask,
        lease: &crate::contexts::automation_runtime::model::WorktreeLease,
        error: &AppError,
    ) -> AppResult<()> {
        let latest_task = self.store.read_task(base_dir, &task.task_id)?;
        if latest_task.status == TaskStatus::Aborted {
            return self.release_task_lease(base_dir, repo_root, &task.task_id, lease);
        }

        let failure_class = error
            .failure_class()
            .map(|class| class.as_str().to_owned())
            .unwrap_or_else(|| "daemon_dispatch_failed".to_owned());
        self.fail_claimed_task(
            base_dir,
            repo_root,
            &task.task_id,
            lease,
            &failure_class,
            &error.to_string(),
        )
    }

    fn fail_claimed_task_preserve_worktree(
        &self,
        base_dir: &Path,
        task_id: &str,
        failure_class: &str,
        failure_message: &str,
    ) -> AppResult<()> {
        DaemonTaskService::mark_failed(
            self.store,
            base_dir,
            task_id,
            failure_class,
            failure_message,
        )
        .map(|_| ())
    }

    fn fail_claimed_task(
        &self,
        base_dir: &Path,
        repo_root: &Path,
        task_id: &str,
        lease: &crate::contexts::automation_runtime::model::WorktreeLease,
        failure_class: &str,
        failure_message: &str,
    ) -> AppResult<()> {
        let mark_result = DaemonTaskService::mark_failed(
            self.store,
            base_dir,
            task_id,
            failure_class,
            failure_message,
        )
        .map(|_| ());
        let cleanup_result = self.release_task_lease(base_dir, repo_root, task_id, lease);

        match (mark_result, cleanup_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Err(error), Err(_)) => Err(error),
        }
    }

    fn release_task_lease(
        &self,
        base_dir: &Path,
        repo_root: &Path,
        task_id: &str,
        lease: &crate::contexts::automation_runtime::model::WorktreeLease,
    ) -> AppResult<()> {
        let release_result = LeaseService::release(
            self.store,
            self.worktree,
            base_dir,
            repo_root,
            lease,
            crate::contexts::automation_runtime::lease_service::ReleaseMode::Idempotent,
        );
        match release_result {
            Ok(ref r) if r.resources_released => {
                // All sub-steps succeeded — safe to clear durable lease reference.
                DaemonTaskService::clear_lease_reference(self.store, base_dir, task_id).map(|_| ())
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
        }
    }
}

struct DaemonRebaseConflictResolver<'a, A, R, S> {
    agent_service: &'a AgentExecutionService<A, R, S>,
    project_root: PathBuf,
    working_dir: PathBuf,
    target: ResolvedBackendTarget,
    timeout: Duration,
    task_id: String,
}

impl<A, R, S> RebaseConflictResolver for DaemonRebaseConflictResolver<'_, A, R, S>
where
    A: AgentExecutionPort + Sync,
    R: RawOutputPort + Sync,
    S: SessionStorePort + Sync,
{
    fn resolve_conflicts(
        &self,
        request: &RebaseConflictRequest,
    ) -> AppResult<RebaseConflictResolution> {
        let context = json!({
            "branch_name": request.branch_name,
            "upstream": request.upstream,
            "failure_details": request.failure_details,
            "conflicted_files": request.conflicted_files,
        });
        let schema = json!({
            "type": "object",
            "required": ["summary", "resolved_files"],
            "properties": {
                "summary": { "type": "string" },
                "resolved_files": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["path", "content"],
                        "properties": {
                            "path": { "type": "string" },
                            "content": { "type": "string" }
                        }
                    }
                }
            }
        });
        let prompt = format!(
            "# Rebase Conflict Resolution\n\n\
Resolve the Git rebase conflicts described in the context JSON.\n\
Return full resolved file contents for every conflicted file.\n\
Do not omit any conflicted file and do not include extra files.\n\n\
## Output Schema\n\n```json\n{}\n```",
            serde_json::to_string_pretty(&schema)?
        );
        let request = InvocationRequest {
            invocation_id: format!("rebase-{}-{}", self.task_id, Utc::now().timestamp_millis()),
            project_root: self.project_root.clone(),
            working_dir: self.working_dir.clone(),
            contract: InvocationContract::Requirements {
                label: "daemon:rebase_resolution".to_owned(),
            },
            role: BackendRole::Implementer,
            resolved_target: self.target.clone(),
            payload: InvocationPayload { prompt, context },
            timeout: self.timeout,
            cancellation_token: CancellationToken::new(),
            session_policy: SessionPolicy::NewSession,
            prior_session: None,
            attempt_number: 1,
        };

        let envelope = thread::scope(|scope| {
            let agent_service = self.agent_service;
            scope
                .spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|error| {
                            AppError::Io(std::io::Error::other(format!(
                                "build rebase agent runtime: {error}"
                            )))
                        })?;
                    runtime.block_on(agent_service.invoke(request))
                })
                .join()
                .map_err(|_| {
                    AppError::Io(std::io::Error::other(
                        "rebase agent resolution thread panicked",
                    ))
                })?
        })?;

        serde_json::from_value(envelope.parsed_payload).map_err(|error| {
            AppError::InvocationFailed {
                backend: self.target.backend.family.to_string(),
                contract_id: "daemon:rebase_resolution".to_owned(),
                failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                details: format!("invalid rebase agent response: {error}"),
            }
        })
    }
}

fn rebase_cycle_from_snapshot(
    snapshot: crate::contexts::project_run_record::model::RunSnapshot,
) -> Option<u32> {
    snapshot
        .active_run
        .as_ref()
        .or(snapshot.interrupted_run.as_ref())
        .map(|run| run.stage_cursor.cycle)
        .or_else(|| snapshot.cycle_history.last().map(|entry| entry.cycle))
}

/// Build a requirements service for production daemon use via the shared builder.
/// Uses the same environment-driven adapter selection as the CLI requirements path.
fn build_requirements_service_default(
    effective_config: &EffectiveConfig,
) -> AppResult<crate::composition::agent_execution_builder::ProductionRequirementsService> {
    crate::composition::agent_execution_builder::build_requirements_service(effective_config)
}

/// Test-only seam: build a requirements service from an explicit
/// `StubBackendAdapter`.  Tests call this to inject custom stub payloads while
/// exercising the same workspace-default wiring the daemon uses.
#[cfg(feature = "test-stub")]
pub fn build_requirements_service_for_test(
    adapter: crate::adapters::stub_backend::StubBackendAdapter,
    effective_config: &EffectiveConfig,
) -> AppResult<
    crate::contexts::requirements_drafting::service::RequirementsService<
        crate::adapters::stub_backend::StubBackendAdapter,
        crate::adapters::fs::FsRawOutputStore,
        crate::adapters::fs::FsSessionStore,
        crate::adapters::fs::FsRequirementsStore,
    >,
> {
    use crate::adapters::fs::{FsRawOutputStore, FsRequirementsStore, FsSessionStore};
    use crate::contexts::agent_execution::service::BackendSelectionConfig;
    use crate::contexts::agent_execution::AgentExecutionService;
    use crate::contexts::requirements_drafting::service::RequirementsService;

    let workspace_defaults = BackendSelectionConfig::from_effective_config(effective_config)?;

    let raw_output_store = FsRawOutputStore;
    let session_store = FsSessionStore;
    let agent_service = AgentExecutionService::new(adapter, raw_output_store, session_store);
    let requirements_store = FsRequirementsStore;
    Ok(RequirementsService::new(agent_service, requirements_store)
        .with_workspace_defaults(workspace_defaults))
}

async fn wait_for_shutdown_signal() -> AppResult<()> {
    #[cfg(unix)]
    {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => Ok(()),
            _ = sigterm.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.map_err(AppError::from)
    }
}
