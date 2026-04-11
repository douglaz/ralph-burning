use std::future::Future;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use crate::adapters::fs::{
    FileSystem, FsMilestoneJournalStore, FsMilestonePlanStore, FsMilestoneSnapshotStore,
    FsMilestoneStore, FsTaskRunLineageStore,
};
use crate::adapters::github::GithubPort;
use crate::cli::run::cleanup_stale_backend_process_groups;
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
use crate::contexts::milestone_record::service as milestone_service;
use crate::contexts::project_run_record::model::{RunSnapshot, RunStatus};
use crate::contexts::project_run_record::service::{
    create_project, AmendmentQueuePort, ArtifactStorePort, JournalStorePort,
    PayloadArtifactWritePort, ProjectStorePort, RunSnapshotPort, RunSnapshotWritePort,
    RuntimeLogWritePort,
};
use crate::contexts::project_run_record::CreateProjectInput;
use crate::contexts::project_run_record::{journal, queries};
use crate::contexts::requirements_drafting::model::{RequirementsOutputKind, RequirementsStatus};
use crate::contexts::requirements_drafting::service::{self as req_service, RequirementsStorePort};
use crate::contexts::workflow_composition::engine;
use crate::contexts::workflow_composition::retry_policy::RetryPolicy;
use crate::contexts::workspace_governance::config::{EffectiveConfig, DEFAULT_FLOW_PRESET};
use crate::contexts::workspace_governance::WORKSPACE_DIR;
use crate::shared::domain::{
    BackendPolicyRole, BackendRole, FlowPreset, ProjectId, ResolvedBackendTarget, RunId,
    SessionPolicy,
};
use crate::shared::error::{AppError, AppResult};

type ConfiguredAgentServiceBuilder =
    fn(
        &EffectiveConfig,
    ) -> AppResult<crate::composition::agent_execution_builder::ProductionAgentService>;

type ConfiguredRequirementsServiceBuilder = Box<
    dyn Fn(
            &EffectiveConfig,
        )
            -> AppResult<crate::composition::agent_execution_builder::ProductionRequirementsService>
        + Send
        + Sync,
>;

fn allow_aborted_dispatch_fast_path(outcome: AppResult<()>) -> AppResult<()> {
    match outcome {
        Ok(()) | Err(AppError::InvocationCancelled { .. }) => Ok(()),
        Err(error) => Err(error),
    }
}

const DAEMON_TASK_CANCELLATION_STATUS_SUMMARY: &str =
    "failed (interrupted by daemon task cancellation)";
const DAEMON_TASK_CANCELLATION_LOG_MESSAGE: &str =
    "daemon task cancellation interrupted the orchestrator before graceful shutdown completed";
const DAEMON_SHUTDOWN_STATUS_SUMMARY: &str = "failed (interrupted by daemon shutdown)";
const DAEMON_SHUTDOWN_LOG_MESSAGE: &str =
    "daemon shutdown interrupted the orchestrator before graceful shutdown completed";

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

const DAEMON_DISPATCH_SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(5);

struct PreparedCancelledDispatchHandoff {
    expected_attempt: Option<engine::RunningAttemptIdentity>,
    interrupted_marker_persisted: bool,
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
    configured_agent_service_builder: Option<ConfiguredAgentServiceBuilder>,
    configured_requirements_service_builder: Option<ConfiguredRequirementsServiceBuilder>,
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
            configured_agent_service_builder: None,
            configured_requirements_service_builder: None,
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

    pub fn with_configured_agent_service_builder(
        mut self,
        builder: ConfiguredAgentServiceBuilder,
    ) -> Self {
        self.configured_agent_service_builder = Some(builder);
        self
    }

    pub fn with_configured_requirements_service_builder(
        mut self,
        builder: ConfiguredRequirementsServiceBuilder,
    ) -> Self {
        self.configured_requirements_service_builder = Some(builder);
        self
    }

    /// Build a requirements service using the configured builder callback,
    /// or fall back to `build_requirements_service_default` (which reads env).
    fn build_requirements_service(
        &self,
        effective_config: &EffectiveConfig,
    ) -> AppResult<crate::composition::agent_execution_builder::ProductionRequirementsService> {
        match self.configured_requirements_service_builder {
            Some(ref builder) => builder(effective_config),
            None => build_requirements_service_default(effective_config),
        }
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

impl<'a, A, R, S> DaemonLoop<'a, A, R, S>
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
                self.cleanup_registered_active_leases(data_dir, &active_registrations)?;
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
                    self.cleanup_registered_active_leases(data_dir, &active_registrations)?;
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
                                        // Preserve checkpoint commits for failed tasks
                                        // before the worktree is removed.
                                        if dirty_task.status == TaskStatus::Failed {
                                            self.try_push_failed_task_branch(checkout, &lease);
                                        }
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
                    .unwrap_or(DEFAULT_FLOW_PRESET),
            )
            .await
            {
                eprintln!("daemon: GitHub poll failed for {}: {e}", reg.repo_slug);
                continue; // Multi-repo failure isolation
            }

            // Phase 2: Check waiting tasks for completed requirements runs
            let changed_task_ids = match self.check_waiting_tasks(&daemon_dir, checkout) {
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

            // Sync labels for tasks changed by requirements completion.
            // A label sync failure quarantines this repo for the rest of the
            // cycle and marks the task label_dirty for later repair.
            let mut label_sync_failed = false;
            for task_id in &changed_task_ids {
                if let Ok(resumed_task) = self.store.read_task(&daemon_dir, task_id) {
                    if let Err(e) = github_intake::sync_label_for_task(github, &resumed_task).await
                    {
                        eprintln!(
                            "daemon: failed to sync label for updated task '{}' in {}: {e}",
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
    #[allow(clippy::too_many_arguments)]
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
                if let Err(e) = self
                    .handle_requirements_quick(store_dir, repo_root, task, &effective_config)
                    .await
                {
                    // Mark label dirty so daemon reconcile can repair the
                    // GitHub label — the task is already marked Failed by
                    // handle_requirements_quick, but the issue still shows
                    // rb:ready without this.
                    let _ =
                        DaemonTaskService::mark_label_dirty(self.store, store_dir, &task.task_id);
                    return Err(e);
                }
            }
            DispatchMode::RequirementsDraft | DispatchMode::RequirementsMilestone => {
                return self
                    .handle_requirements_full_mode(
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

        // On post-completion retries the run is already Completed and no
        // new code will execute. Skip rebase and ensure_project to avoid
        // unnecessary merge conflicts that would block reconciliation or
        // PR recovery. Gated on: (1) the pre-claim task had ANY failure
        // class (ruling out aborted retries — whose failure_class is None —
        // and fresh dispatches), AND (2) the run snapshot is Completed.
        // This covers reconciliation_* failures, pr_runtime_failed, and
        // any future post-completion failure class.
        // read_run_snapshot returns Err on first-run (no snapshot yet),
        // which is fine — we fall through to the normal path.
        let is_post_completion_retry = task.failure_class.is_some()
            && crate::shared::domain::ProjectId::new(claimed_task.project_id.clone())
                .ok()
                .and_then(|pid| {
                    self.run_snapshot_read
                        .read_run_snapshot(repo_root, &pid)
                        .ok()
                })
                .is_some_and(|snap| snap.status == RunStatus::Completed);

        if !is_post_completion_retry {
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
                self.handle_post_claim_failure(
                    store_dir,
                    repo_root,
                    &claimed_task,
                    &lease,
                    &error,
                )?;
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
            self.release_task_lease(store_dir, repo_root, &task_on_disk.task_id, &lease)?;
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
            allow_aborted_dispatch_fast_path(outcome)?;
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
            self.release_task_lease(store_dir, repo_root, &active_task.task_id, &lease)?;
            return Ok(());
        }

        match outcome {
            Ok(()) => {
                // Persist the workflow run_id on the task after dispatch
                // so reconciliation retries use the correct run rather
                // than re-reading the journal (which may have been
                // appended to by a manual re-run). Skip for
                // post-completion retries: no new RunStarted was written
                // (dispatch was a no-op for Completed), so the journal's
                // latest RunStarted may belong to a newer manual re-run.
                // Storing that would permanently bind the wrong run_id.
                // Tasks that already have workflow_run_id from the
                // original dispatch are unaffected (persist returns early).
                if !is_post_completion_retry {
                    self.persist_workflow_run_id(store_dir, repo_root, &active_task);
                }

                // Reconciliation-only retries (reconciliation_*) skip the PR
                // handler: the PR was already created/merged on the original
                // dispatch, and the retry only needs local bead close/sync/
                // milestone bookkeeping. Re-running the PR handler on a
                // reconciliation retry is unnecessary and can mutate or fail
                // PR state (e.g. close-or-skip when the worktree is not
                // ahead because no branch was resumed).
                //
                // NOTE: `task` here is the pre-claim snapshot (line ~647),
                // which shadows the outer `task` parameter. `claim_task`
                // calls `clear_failure()` on the claimed copy, so reading
                // `failure_class` from the post-claim `active_task` would
                // always be None. The pre-claim binding retains the original
                // failure_class needed for this gate.
                let is_reconciliation_only_retry = is_post_completion_retry
                    && task
                        .failure_class
                        .as_deref()
                        .is_some_and(|fc| fc.starts_with("reconciliation_"));

                if !is_reconciliation_only_retry {
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
                            let aborted_task =
                                self.store.read_task(store_dir, &active_task.task_id)?;
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
                            self.release_task_lease(
                                store_dir,
                                repo_root,
                                &active_task.task_id,
                                &lease,
                            )?;
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
                            self.try_push_failed_task_branch(repo_root, &lease);
                            self.release_task_lease(
                                store_dir,
                                repo_root,
                                &active_task.task_id,
                                &lease,
                            )?;
                            println!("failed task {}: {}", active_task.task_id, error);
                            return Ok(());
                        }
                    }
                } // end if !is_reconciliation_only_retry
                  // Reconcile bead BEFORE marking completed so a crash between
                  // reconciliation and mark_completed causes reprocessing on restart.
                if let Err((failure_class, failure_message)) =
                    self.try_reconcile_success(repo_root, &active_task).await
                {
                    match DaemonTaskService::mark_failed(
                        self.store,
                        store_dir,
                        &active_task.task_id,
                        &failure_class,
                        &failure_message,
                    ) {
                        Ok(failed_task) => {
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
                        }
                        Err(e) => {
                            // mark_failed failed: the task's Failed state was NOT
                            // durably written. Do NOT release the lease — the task
                            // is still Active/Claimed. Releasing it would strand the
                            // task with no lease and no scanner (Phase 3 only scans
                            // Pending; Phase 0 only scans label_dirty). Retaining the
                            // lease lets `daemon reconcile` detect it as stale and
                            // mark_failed with class reconciliation_timeout.
                            //
                            // If the failure is persistent (corrupt store), operator
                            // intervention is required: run `daemon reconcile` or
                            // manually repair the task store file.
                            eprintln!(
                                "daemon: CRITICAL: mark_failed itself failed for task '{}', \
                                 retaining lease for stale-lease recovery: {e}",
                                active_task.task_id
                            );
                            self.try_push_failed_task_branch(repo_root, &lease);
                            return Ok(());
                        }
                    }
                    self.try_push_failed_task_branch(repo_root, &lease);
                    self.release_task_lease(store_dir, repo_root, &active_task.task_id, &lease)?;
                    println!("failed task {}: {failure_class}", active_task.task_id);
                    return Ok(());
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
                self.release_task_lease(store_dir, repo_root, &active_task.task_id, &lease)?;
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
                self.try_push_failed_task_branch(repo_root, &lease);
                self.release_task_lease(store_dir, repo_root, &active_task.task_id, &lease)?;
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
        let _updated = self.check_waiting_tasks(base_dir, base_dir)?;

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

    /// Label sync after a task state mutation inside full-mode requirements handling.
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
            eprintln!("daemon: label sync failed for requirements task '{task_id}': {e}");
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

    /// Check tasks in WaitingForRequirements state and advance any whose linked
    /// requirements run has completed. Seed outputs resume into workflow;
    /// milestone outputs materialize a milestone plan and complete the task.
    /// Returns task IDs whose durable status changed so callers can sync labels.
    fn check_waiting_tasks(&self, base_dir: &Path, workspace_dir: &Path) -> AppResult<Vec<String>> {
        let Some(req_store) = self.requirements_store else {
            return Ok(vec![]);
        };

        let mut changed_task_ids = Vec::new();
        let tasks = DaemonTaskService::list_tasks(self.store, base_dir)?;
        for task in tasks {
            if task.status != TaskStatus::WaitingForRequirements {
                continue;
            }
            let Some(ref run_id) = task.requirements_run_id else {
                continue;
            };

            let run =
                match req_service::read_requirements_run_status(req_store, workspace_dir, run_id) {
                    Ok(run) => run,
                    Err(e) => {
                        println!(
                            "daemon: error checking requirements run '{}' for task '{}': {}",
                            run_id, task.task_id, e
                        );
                        continue;
                    }
                };
            if run.status != RequirementsStatus::Completed {
                continue;
            }

            match run.output_kind {
                RequirementsOutputKind::ProjectSeed => {
                    match req_service::extract_seed_handoff(req_store, workspace_dir, run_id) {
                        Ok(handoff) => {
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
                                if DaemonTaskService::mark_failed(
                                    self.store,
                                    base_dir,
                                    &task.task_id,
                                    "requirements_linking_failed",
                                    &format!("post-seed metadata update failed: {e}"),
                                )
                                .is_ok()
                                {
                                    changed_task_ids.push(task.task_id.clone());
                                }
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
                                    changed_task_ids.push(task.task_id.clone());
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
                            if DaemonTaskService::mark_failed(
                                self.store,
                                base_dir,
                                &task.task_id,
                                "seed_handoff_failed",
                                &e.to_string(),
                            )
                            .is_ok()
                            {
                                changed_task_ids.push(task.task_id.clone());
                            }
                            println!(
                                "daemon: seed handoff failed for task '{}': {}",
                                task.task_id, e
                            );
                        }
                    }
                }
                RequirementsOutputKind::MilestoneBundle => {
                    match req_service::extract_milestone_bundle_handoff(
                        req_store,
                        workspace_dir,
                        run_id,
                    ) {
                        Ok(handoff) => {
                            let materialize_result = milestone_service::materialize_bundle(
                                &FsMilestoneStore,
                                &FsMilestoneSnapshotStore,
                                &FsMilestoneJournalStore,
                                &FsMilestonePlanStore,
                                workspace_dir,
                                &handoff.bundle,
                                Utc::now(),
                            );
                            if let Err(e) = materialize_result {
                                if DaemonTaskService::mark_failed(
                                    self.store,
                                    base_dir,
                                    &task.task_id,
                                    "milestone_handoff_failed",
                                    &e.to_string(),
                                )
                                .is_ok()
                                {
                                    changed_task_ids.push(task.task_id.clone());
                                }
                                println!(
                                    "daemon: milestone handoff failed for task '{}': {}",
                                    task.task_id, e
                                );
                                continue;
                            }

                            match DaemonTaskService::mark_completed(
                                self.store,
                                base_dir,
                                &task.task_id,
                            ) {
                                Ok(_) => {
                                    println!(
                                        "daemon: completed milestone task '{}' from requirements run '{}'",
                                        task.task_id, run_id
                                    );
                                    changed_task_ids.push(task.task_id.clone());
                                }
                                Err(e) => {
                                    println!(
                                        "daemon: failed to complete milestone task '{}': {}",
                                        task.task_id, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            if DaemonTaskService::mark_failed(
                                self.store,
                                base_dir,
                                &task.task_id,
                                "milestone_handoff_failed",
                                &e.to_string(),
                            )
                            .is_ok()
                            {
                                changed_task_ids.push(task.task_id.clone());
                            }
                            println!(
                                "daemon: milestone handoff failed for task '{}': {}",
                                task.task_id, e
                            );
                        }
                    }
                }
            }
        }

        Ok(changed_task_ids)
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
        // full-mode requirements dispatch returns after waiting/completion handling.
        match task.dispatch_mode {
            DispatchMode::RequirementsQuick => {
                self.handle_requirements_quick(base_dir, base_dir, task, &effective_config)
                    .await?;
                // Task is now Workflow mode with project metadata populated.
                // Fall through to standard claim/dispatch below.
            }
            DispatchMode::RequirementsDraft | DispatchMode::RequirementsMilestone => {
                // Single-repo path is test-only; file-watcher tasks have no
                // repo_slug so sync_label_for_task will no-op. Use a no-op
                // in-memory GitHub client to satisfy the generic bound.
                let noop_gh = crate::adapters::github::InMemoryGithubClient::new();
                return self
                    .handle_requirements_full_mode(
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

        // On post-completion retries the run is already Completed and no
        // new code will execute. Skip rebase and ensure_project to avoid
        // unnecessary merge conflicts that would block reconciliation or
        // PR recovery. Gated on: (1) the pre-claim task had ANY failure
        // class (ruling out aborted retries — whose failure_class is None —
        // and fresh dispatches), AND (2) the run snapshot is Completed.
        // This covers reconciliation_* failures, pr_runtime_failed, and
        // any future post-completion failure class.
        let is_post_completion_retry = task.failure_class.is_some()
            && crate::shared::domain::ProjectId::new(claimed_task.project_id.clone())
                .ok()
                .and_then(|pid| {
                    self.run_snapshot_read
                        .read_run_snapshot(base_dir, &pid)
                        .ok()
                })
                .is_some_and(|snap| snap.status == RunStatus::Completed);

        if !is_post_completion_retry {
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
        }
        let task_on_disk = self.store.read_task(base_dir, &claimed_task.task_id)?;
        if task_on_disk.status == TaskStatus::Aborted {
            self.release_task_lease(base_dir, repo_root, &task_on_disk.task_id, &lease)?;
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
            allow_aborted_dispatch_fast_path(outcome)?;
            self.release_task_lease(base_dir, repo_root, &active_task.task_id, &lease)?;
            return Ok(());
        }

        match outcome {
            Ok(()) => {
                // Persist workflow run_id after dispatch — skip for
                // post-completion retries (see multi-repo path comment).
                if !is_post_completion_retry {
                    self.persist_workflow_run_id(base_dir, base_dir, &active_task);
                }

                // Skip PR handler for reconciliation-only retries — see
                // multi-repo path comment for rationale.
                //
                // NOTE: `task` is the pre-claim snapshot (line ~1445);
                // `claim_task` clears failure_class on the claimed copy.
                let is_reconciliation_only_retry = is_post_completion_retry
                    && task
                        .failure_class
                        .as_deref()
                        .is_some_and(|fc| fc.starts_with("reconciliation_"));

                if !is_reconciliation_only_retry && task.repo_slug.is_some() {
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
                        self.release_task_lease(base_dir, repo_root, &active_task.task_id, &lease)?;
                        return Ok(());
                    }
                }
                // Reconcile bead BEFORE marking completed so a crash between
                // reconciliation and mark_completed causes reprocessing on restart.
                if let Err((failure_class, failure_message)) =
                    self.try_reconcile_success(base_dir, &active_task).await
                {
                    if let Err(e) = DaemonTaskService::mark_failed(
                        self.store,
                        base_dir,
                        &active_task.task_id,
                        &failure_class,
                        &failure_message,
                    ) {
                        // Same as multi-repo: retain the lease for stale-lease
                        // recovery via `daemon reconcile`. See multi-repo handler
                        // comment for full rationale.
                        eprintln!(
                            "daemon: CRITICAL: mark_failed itself failed for task '{}', \
                             retaining lease for stale-lease recovery: {e}",
                            active_task.task_id
                        );
                        self.try_push_failed_task_branch(repo_root, &lease);
                        return Ok(());
                    }
                    self.try_push_failed_task_branch(repo_root, &lease);
                    self.release_task_lease(base_dir, repo_root, &active_task.task_id, &lease)?;
                    println!("failed task {}: {failure_class}", active_task.task_id);
                    return Ok(());
                }
                DaemonTaskService::mark_completed(self.store, base_dir, &active_task.task_id)?;
                self.release_task_lease(base_dir, repo_root, &active_task.task_id, &lease)?;
                println!("completed task {}", active_task.task_id);
            }
            Err(error) => {
                let failure_class = error
                    .failure_class()
                    .map(|class| class.as_str().to_owned())
                    .unwrap_or_else(|| "daemon_dispatch_failed".to_owned());
                DaemonTaskService::mark_failed(
                    self.store,
                    base_dir,
                    &active_task.task_id,
                    &failure_class,
                    &error.to_string(),
                )?;
                self.try_push_failed_task_branch(repo_root, &lease);
                self.release_task_lease(base_dir, repo_root, &active_task.task_id, &lease)?;
                println!("failed task {}: {}", active_task.task_id, error);
            }
        }

        Ok(())
    }

    /// Best-effort: persist the workflow run_id extracted from the journal
    /// onto the task so that reconciliation retries can identify the correct
    /// run without re-reading the journal. `store_dir` is the daemon store
    /// root; `project_dir` is where journal files live.
    fn persist_workflow_run_id(&self, store_dir: &Path, project_dir: &Path, task: &DaemonTask) {
        // Skip if already set (reconciliation retry of a task that
        // previously had its run_id persisted).
        if task.workflow_run_id.is_some() {
            return;
        }
        let Ok(project_id) = crate::shared::domain::ProjectId::new(&task.project_id) else {
            return;
        };
        let Ok(events) = self.journal_store.read_journal(project_dir, &project_id) else {
            return;
        };
        let run_id = events
            .iter()
            .rev()
            .find(|e| {
                e.event_type
                    == crate::contexts::project_run_record::model::JournalEventType::RunStarted
            })
            .and_then(|e| e.details.get("run_id").and_then(serde_json::Value::as_str));
        if let Some(run_id) = run_id {
            // The task had no workflow_run_id — we're binding it to the latest
            // RunStarted event. If another run was started on this project
            // between the original dispatch and this call, this may bind the
            // wrong run_id. Log so operators can correlate suspect bindings.
            tracing::debug!(
                task_id = %task.task_id,
                run_id,
                "persist_workflow_run_id: binding run_id from latest RunStarted journal event",
            );
            if let Err(e) =
                DaemonTaskService::set_workflow_run_id(self.store, store_dir, &task.task_id, run_id)
            {
                eprintln!(
                    "daemon: failed to persist workflow_run_id on task '{}' (non-blocking): {e}",
                    task.task_id
                );
            }
        }
    }

    /// Attempt success reconciliation for a completed milestone task.
    ///
    /// Closes the bead in `br`, syncs, updates milestone state, and captures
    /// next-step hints. Best-effort: failures are logged and transition the
    /// milestone to Failed (needs-operator) rather than blocking task completion.
    ///
    /// `project_dir` is the workspace root where project records, milestone
    /// files, and the `.beads/` graph live. In single-repo mode this is
    /// `base_dir`; in multi-repo mode it is `repo_root`.
    ///
    /// Returns `Ok(())` if callers should proceed with `mark_completed`.
    /// Returns `Err((failure_class, message))` if the task should be marked
    /// failed instead — covers metadata errors (invalid project_id, unreadable
    /// project record) and br close/sync failures.
    async fn try_reconcile_success(
        &self,
        project_dir: &Path,
        task: &DaemonTask,
    ) -> Result<(), (String, String)> {
        use crate::adapters::br_process::{BrAdapter, BrMutationAdapter, OsProcessRunner};
        use crate::adapters::bv_process::{BvAdapter, OsBvProcessRunner};
        use crate::contexts::automation_runtime::success_reconciliation::{
            reconcile_success, ReconciliationError,
        };
        use crate::contexts::project_run_record::model::JournalEventType;
        use crate::shared::domain::ProjectId;

        let project_id = match ProjectId::new(&task.project_id) {
            Ok(id) => id,
            Err(_) => {
                return Err((
                    "reconciliation_metadata_error".to_owned(),
                    "invalid project_id during success reconciliation".to_owned(),
                ));
            }
        };

        let project_record = match self
            .project_store
            .read_project_record(project_dir, &project_id)
        {
            Ok(record) => record,
            Err(e) => {
                eprintln!(
                    "daemon: could not read project record for reconciliation (task={}): {e}",
                    task.task_id
                );
                return Err((
                    "reconciliation_metadata_error".to_owned(),
                    format!("could not read project record during success reconciliation: {e}"),
                ));
            }
        };

        let task_source = match project_record.task_source.as_ref() {
            Some(ts) => ts,
            None => return Ok(()), // Not a milestone task, nothing to reconcile.
        };

        // NOTE: We intentionally do NOT transition the milestone to Failed on
        // reconciliation errors. MilestoneStatus::Failed is terminal — once set,
        // record_bead_start rejects all future bead starts, permanently wedging
        // the milestone with no automated recovery path. Instead, we leave the
        // milestone Running and return Err so the *task* is marked Failed. The
        // operator can then retry_task, which re-dispatches reconciliation against
        // the still-Running milestone. All reconciliation steps are idempotent:
        // br close checks bead status, sync is unconditional, and
        // record_bead_completion handles replays.

        // Extract run_id and started_at. Prefer the durable workflow_run_id
        // persisted on the task (set after dispatch completes) over scanning
        // the journal for the latest RunStarted event. The journal-based
        // fallback is kept for backwards compatibility with tasks dispatched
        // before workflow_run_id was introduced.
        let journal_events = match self.journal_store.read_journal(project_dir, &project_id) {
            Ok(events) => events,
            Err(e) => {
                return Err((
                    "reconciliation_metadata_error".to_owned(),
                    format!("could not read journal: {e}"),
                ));
            }
        };

        let run_id_from_task = task.workflow_run_id.as_deref();
        if run_id_from_task.is_none() {
            if task.attempt_count > 0 {
                // Fail-closed: on retries without a durable task→run
                // binding, the journal's latest RunStarted may belong to a
                // different manual re-run started between the original
                // failure and this retry. Guessing the wrong run would
                // close/sync the bead and rewrite milestone lineage for the
                // wrong attempt. Surface a needs-operator condition instead;
                // the operator can re-dispatch with a correct run binding.
                return Err((
                    "reconciliation_no_run_binding".to_owned(),
                    format!(
                        "task '{}' has no workflow_run_id on retry \
                         (attempt_count={}); cannot safely determine which \
                         run to reconcile — operator intervention required",
                        task.task_id, task.attempt_count,
                    ),
                ));
            }
            // First dispatch (attempt_count == 0): the journal's RunStarted
            // was written by this dispatch and is correct.
            // persist_workflow_run_id should have set the binding, but the
            // write may have failed transiently. Falling back to the latest
            // RunStarted is safe because no retry window exists for a manual
            // re-run to have overwritten it.
            eprintln!(
                "daemon: try_reconcile_success for task '{}' has no workflow_run_id — \
                 falling back to latest RunStarted journal event (first dispatch, safe)",
                task.task_id,
            );
        }
        let run_started = journal_events.iter().rev().find(|event| {
            if event.event_type != JournalEventType::RunStarted {
                return false;
            }
            match run_id_from_task {
                // When workflow_run_id is set, match the specific RunStarted
                // event rather than blindly taking the latest one.
                Some(target) => {
                    event
                        .details
                        .get("run_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(target)
                }
                None => true,
            }
        });

        let (run_id, started_at) = match run_started {
            Some(event) => {
                let run_id = event
                    .details
                    .get("run_id")
                    .and_then(serde_json::Value::as_str);
                match run_id {
                    Some(id) => {
                        // Use the durable bead-start timestamp from milestone
                        // lineage rather than journal events. The lineage entry
                        // was created by record_bead_start with the authoritative
                        // started_at. For resumed runs, journal RunResumed has a
                        // different timestamp, but the lineage entry retains the
                        // original bead-start value — and finalize_task_run_internal
                        // rejects started_at mismatches on terminal replays.
                        //
                        // Fallback chain (mirrors cli/run.rs effective_attempt_started_at):
                        // 1. Lineage entry started_at (authoritative)
                        // 2. RunResumed event timestamp (for resumed runs without lineage)
                        // 3. RunStarted event timestamp (original start)
                        let lineage_started_at = {
                            use crate::contexts::milestone_record::model::MilestoneId;
                            MilestoneId::new(&task_source.milestone_id)
                                .ok()
                                .and_then(|mid| {
                                    milestone_service::find_runs_for_bead(
                                        &FsTaskRunLineageStore,
                                        project_dir,
                                        &mid,
                                        &task_source.bead_id,
                                    )
                                    .ok()
                                })
                                .and_then(|entries| {
                                    entries
                                        .iter()
                                        .rev()
                                        .find(|e| e.run_id.as_deref() == Some(id))
                                        .map(|e| e.started_at)
                                })
                        };
                        // When lineage is missing, apply the same
                        // effective_attempt_started_at logic as cli/run.rs:
                        // prefer the latest RunResumed timestamp over the
                        // RunStarted timestamp so resumed-run reconciliation
                        // uses the correct started_at for lineage matching.
                        let effective_started_at = || {
                            journal_events
                                .iter()
                                .rev()
                                .find(|ev| {
                                    ev.event_type == JournalEventType::RunResumed
                                        && ev
                                            .details
                                            .get("run_id")
                                            .and_then(serde_json::Value::as_str)
                                            == Some(id)
                                })
                                .map(|ev| ev.timestamp)
                                .unwrap_or(event.timestamp)
                        };
                        let started_at = lineage_started_at.unwrap_or_else(effective_started_at);
                        (id.to_owned(), started_at)
                    }
                    None => {
                        return Err((
                            "reconciliation_metadata_error".to_owned(),
                            "RunStarted event missing run_id".to_owned(),
                        ));
                    }
                }
            }
            None => {
                return Err((
                    "reconciliation_metadata_error".to_owned(),
                    "no RunStarted event found in journal".to_owned(),
                ));
            }
        };

        // Use the authoritative RunCompleted journal event timestamp rather
        // than wall-clock time. Reconciliation may run after extra PR/cleanup
        // work, so Utc::now() would record a late, misordered completion time.
        // A missing RunCompleted event is a hard error — fabricating a
        // timestamp would silently corrupt the milestone record.
        let now = match journal_events
            .iter()
            .rev()
            .find(|event| {
                event.event_type == JournalEventType::RunCompleted
                    && event
                        .details
                        .get("run_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(&run_id)
            })
            .map(|event| event.timestamp)
        {
            Some(ts) => ts,
            None => {
                return Err((
                    "reconciliation_metadata_error".to_owned(),
                    format!("no RunCompleted journal event for run_id={run_id}"),
                ));
            }
        };
        // Anchor br/bv adapters to project_dir so commands target the
        // correct repo's .beads/ graph (matters in multi-repo mode).
        let br_mutation = BrMutationAdapter::with_adapter(
            BrAdapter::<OsProcessRunner>::new().with_working_dir(project_dir.to_path_buf()),
        );
        let br_read =
            BrAdapter::<OsProcessRunner>::new().with_working_dir(project_dir.to_path_buf());
        let bv = BvAdapter::<OsBvProcessRunner>::new().with_working_dir(project_dir.to_path_buf());

        match reconcile_success(
            &br_mutation,
            &br_read,
            Some(&bv),
            project_dir,
            &task_source.bead_id,
            &task.task_id,
            project_id.as_str(),
            &task_source.milestone_id,
            &run_id,
            task_source.plan_hash.as_deref(),
            started_at,
            now,
        )
        .await
        {
            Ok(outcome) => {
                if outcome.was_already_closed {
                    println!(
                        "reconciliation: bead {} already closed (idempotent)",
                        outcome.bead_id
                    );
                } else {
                    println!("reconciliation: closed bead {}", outcome.bead_id);
                }
                if let Some(hint) = &outcome.next_step_hint {
                    println!(
                        "reconciliation: next-step hint: {} (score={:.2})",
                        hint.id, hint.score
                    );
                }
                if let Some(warning) = &outcome.next_step_selection_warning {
                    println!("reconciliation: next-step selection warning: {warning}");
                }
            }
            Err(
                e @ (ReconciliationError::BrCloseFailed { .. }
                | ReconciliationError::BrSyncFailed { .. }),
            ) => {
                return Err(("reconciliation_br_failed".to_owned(), e.to_string()));
            }
            Err(ReconciliationError::MilestoneUpdateFailed {
                bead_id,
                task_id,
                details,
            }) => {
                // The critical bead mutation (close + sync) already succeeded,
                // but milestone state (snapshot progress, journal completion
                // entry, task-to-bead lineage) was NOT durably written.
                //
                // We must NOT swallow this error: completed tasks are terminal
                // and retry_task only accepts Failed/Aborted states. If we
                // proceed to mark_completed, the milestone state is permanently
                // lost with no automated recovery path. Instead, fail the task
                // so the operator can investigate and retry after fixing the
                // underlying milestone store issue. On retry, close_bead will
                // be idempotent (already closed) and record_bead_completion
                // handles replays (idempotent lineage upsert).
                return Err((
                    "reconciliation_milestone_update_failed".to_owned(),
                    format!(
                        "milestone update failed for bead={bead_id} task={task_id} \
                         after successful br close+sync: {details}"
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Handle requirements_quick dispatch: invoke requirements quick, link the
    /// run ID to the task, derive seed, and update the task with project metadata
    /// + Workflow mode so the caller can continue into the standard claim/project/
    ///   dispatch path in the same daemon cycle.
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

        // Build a fresh requirements service — use the configured builder if available,
        // otherwise fall back to the default (which reads RALPH_BURNING_BACKEND from env).
        let req_svc = self
            .build_requirements_service(effective_config)
            .map_err(|e| {
                let _ = DaemonTaskService::mark_failed(
                    self.store,
                    base_dir,
                    &task.task_id,
                    "requirements_quick_failed",
                    &format!("failed to build requirements service: {e}"),
                );
                e
            })?;
        let run_id = match req_svc
            .quick(workspace_dir, &idea, Utc::now(), None, true)
            .await
        {
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

    /// Handle full-mode requirements dispatch (`draft` and `milestone`).
    async fn handle_requirements_full_mode<G: GithubPort>(
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
        let failure_class = match task.dispatch_mode {
            DispatchMode::RequirementsDraft => "requirements_draft_failed",
            DispatchMode::RequirementsMilestone => "requirements_milestone_failed",
            _ => unreachable!("full-mode handler only supports draft/milestone dispatch"),
        };

        // Transition through Pending → Claimed → Active without a worktree lease.
        // Full-mode requirements only need the agent to generate requirements
        // artifacts, so no project, worktree, or writer lock is required.
        {
            let mut t = self.store.read_task(base_dir, &task.task_id)?;
            let now = Utc::now();
            t.transition_to(TaskStatus::Claimed, now)?;
            t.transition_to(TaskStatus::Active, now)?;
            self.store.write_task(base_dir, &t)?;

            // Sync label: Active → rb:in-progress immediately so durable task
            // state matches the issue while requirements are being generated.
            if let Err(e) = github_intake::sync_label_for_task(github, &t).await {
                let _ = DaemonTaskService::mark_label_dirty(self.store, base_dir, &task.task_id);
                eprintln!(
                    "daemon: label sync failed for {} task '{}', quarantining repo: {e}",
                    task.dispatch_mode.as_str(),
                    task.task_id
                );
                return Err(e);
            }
        }

        let idea = task
            .prompt
            .clone()
            .unwrap_or_else(|| format!("Automated task for issue {}", task.issue_ref));

        let req_svc = match self.build_requirements_service(effective_config) {
            Ok(svc) => svc,
            Err(e) => {
                let _ = DaemonTaskService::mark_failed(
                    self.store,
                    base_dir,
                    &task.task_id,
                    failure_class,
                    &format!("failed to build requirements service: {e}"),
                );
                let _ = self
                    .sync_label_after_mutation(github, base_dir, &task.task_id)
                    .await;
                return Err(e);
            }
        };
        let run_id = match task.dispatch_mode {
            DispatchMode::RequirementsDraft => {
                req_svc.draft(workspace_dir, &idea, Utc::now(), None).await
            }
            DispatchMode::RequirementsMilestone => {
                req_svc
                    .draft_milestone(workspace_dir, &idea, Utc::now(), None)
                    .await
            }
            _ => unreachable!("full-mode handler only supports draft/milestone dispatch"),
        };
        let run_id = match run_id {
            Ok(run_id) => run_id,
            Err(e) => {
                let _ = DaemonTaskService::mark_failed(
                    self.store,
                    base_dir,
                    &task.task_id,
                    failure_class,
                    &e.to_string(),
                );
                let _ = self
                    .sync_label_after_mutation(github, base_dir, &task.task_id)
                    .await;
                return Err(e);
            }
        };

        let run = req_service::read_requirements_run_status(req_store, workspace_dir, &run_id)?;

        if run.status == RequirementsStatus::Completed {
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
                        "dispatch_mode": task.dispatch_mode.as_str(),
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

            match run.output_kind {
                RequirementsOutputKind::ProjectSeed => {
                    let handoff = match req_service::extract_seed_handoff(
                        req_store,
                        workspace_dir,
                        &run_id,
                    ) {
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
                    let metadata_result: AppResult<()> = (|| {
                        let mut updated = self.store.read_task(base_dir, &task.task_id)?;
                        if handoff.flow != routed_flow {
                            let warning = format!(
                                "seed suggests flow '{}' but routed flow '{}' is authoritative",
                                handoff.flow.as_str(),
                                routed_flow.as_str()
                            );
                            updated.routing_warnings.push(warning.clone());
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

                    {
                        let mut t = self.store.read_task(base_dir, &task.task_id)?;
                        t.transition_to(TaskStatus::Pending, Utc::now())?;
                        self.store.write_task(base_dir, &t)?;
                    }

                    self.sync_label_after_mutation(github, base_dir, &task.task_id)
                        .await?;

                    println!(
                        "daemon: {} completed directly (empty questions) for task '{}', run_id='{}', requeued for workflow",
                        task.dispatch_mode.as_str(),
                        task.task_id,
                        run_id
                    );

                    Ok(())
                }
                RequirementsOutputKind::MilestoneBundle => {
                    let handoff = match req_service::extract_milestone_bundle_handoff(
                        req_store,
                        workspace_dir,
                        &run_id,
                    ) {
                        Ok(h) => h,
                        Err(e) => {
                            let _ = DaemonTaskService::mark_failed(
                                self.store,
                                base_dir,
                                &task.task_id,
                                "milestone_handoff_failed",
                                &e.to_string(),
                            );
                            let _ = self
                                .sync_label_after_mutation(github, base_dir, &task.task_id)
                                .await;
                            return Err(e);
                        }
                    };

                    if let Err(e) = milestone_service::materialize_bundle(
                        &FsMilestoneStore,
                        &FsMilestoneSnapshotStore,
                        &FsMilestoneJournalStore,
                        &FsMilestonePlanStore,
                        workspace_dir,
                        &handoff.bundle,
                        Utc::now(),
                    ) {
                        let _ = DaemonTaskService::mark_failed(
                            self.store,
                            base_dir,
                            &task.task_id,
                            "milestone_handoff_failed",
                            &e.to_string(),
                        );
                        let _ = self
                            .sync_label_after_mutation(github, base_dir, &task.task_id)
                            .await;
                        return Err(e);
                    }

                    if let Err(e) =
                        DaemonTaskService::mark_completed(self.store, base_dir, &task.task_id)
                    {
                        let _ = DaemonTaskService::mark_failed(
                            self.store,
                            base_dir,
                            &task.task_id,
                            "requirements_linking_failed",
                            &format!("milestone completion update failed: {e}"),
                        );
                        let _ = self
                            .sync_label_after_mutation(github, base_dir, &task.task_id)
                            .await;
                        return Err(e);
                    }

                    self.sync_label_after_mutation(github, base_dir, &task.task_id)
                        .await?;

                    println!(
                        "daemon: milestone requirements completed for task '{}', run_id='{}'",
                        task.task_id, run_id
                    );

                    Ok(())
                }
            }
        } else {
            // Non-empty questions: transition Active → WaitingForRequirements.
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
                            "dispatch_mode": task.dispatch_mode.as_str(),
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
                        "daemon: {} started for task '{}', waiting for answers (run_id='{}')",
                        task.dispatch_mode.as_str(),
                        task.task_id,
                        run_id
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

    #[allow(clippy::too_many_arguments)]
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

        let run_snapshot = self.load_dispatch_run_snapshot(workspace_dir, &project_id)?;
        let dispatch_future = self.dispatch_in_worktree(
            workspace_dir,
            &project_id,
            flow,
            run_snapshot.status,
            effective_config,
            &lease.worktree_path,
            Some(lease.lease_id.as_str()),
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
                        break self.finish_cancelled_dispatch(
                            base_dir,
                            workspace_dir,
                            task,
                            lease,
                            &project_id,
                            &mut heartbeat,
                            &mut dispatch_future,
                            DAEMON_TASK_CANCELLATION_STATUS_SUMMARY,
                            DAEMON_TASK_CANCELLATION_LOG_MESSAGE,
                        )
                        .await;
                    }
                }
                _ = shutdown.cancelled() => {
                    let _ = DaemonTaskService::mark_aborted(self.store, base_dir, &task.task_id);
                    task_cancel.cancel();
                    break self.finish_cancelled_dispatch(
                        base_dir,
                        workspace_dir,
                        task,
                        lease,
                        &project_id,
                        &mut heartbeat,
                        &mut dispatch_future,
                        DAEMON_SHUTDOWN_STATUS_SUMMARY,
                        DAEMON_SHUTDOWN_LOG_MESSAGE,
                    )
                    .await;
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
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

        let run_snapshot = self.load_dispatch_run_snapshot(workspace_dir, &project_id)?;
        let dispatch_future = self.dispatch_in_worktree(
            workspace_dir,
            &project_id,
            flow,
            run_snapshot.status,
            effective_config,
            &lease.worktree_path,
            Some(lease.lease_id.as_str()),
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
                        break self.finish_cancelled_dispatch(
                            base_dir,
                            workspace_dir,
                            task,
                            lease,
                            &project_id,
                            &mut heartbeat,
                            &mut dispatch_future,
                            DAEMON_TASK_CANCELLATION_STATUS_SUMMARY,
                            DAEMON_TASK_CANCELLATION_LOG_MESSAGE,
                        )
                        .await;
                    }
                }
                _ = shutdown.cancelled() => {
                    let _ = DaemonTaskService::mark_aborted(self.store, base_dir, &task.task_id);
                    task_cancel.cancel();
                    break self.finish_cancelled_dispatch(
                        base_dir,
                        workspace_dir,
                        task,
                        lease,
                        &project_id,
                        &mut heartbeat,
                        &mut dispatch_future,
                        DAEMON_SHUTDOWN_STATUS_SUMMARY,
                        DAEMON_SHUTDOWN_LOG_MESSAGE,
                    )
                    .await;
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn dispatch_in_worktree(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        flow: FlowPreset,
        run_status: RunStatus,
        effective_config: &EffectiveConfig,
        worktree_path: &Path,
        writer_owner: Option<&str>,
        cancellation_token: CancellationToken,
    ) -> AppResult<()> {
        if let Some(builder) = self.configured_agent_service_builder {
            let agent_service = builder(effective_config)?;
            return dispatch_in_worktree_with_service(
                &agent_service,
                self.run_snapshot_read,
                self.run_snapshot_write,
                self.journal_store,
                self.artifact_store,
                self.artifact_write,
                self.log_write,
                self.amendment_queue,
                base_dir,
                project_id,
                flow,
                run_status,
                effective_config,
                worktree_path,
                writer_owner,
                cancellation_token,
            )
            .await;
        }

        dispatch_in_worktree_with_service(
            self.agent_service,
            self.run_snapshot_read,
            self.run_snapshot_write,
            self.journal_store,
            self.artifact_store,
            self.artifact_write,
            self.log_write,
            self.amendment_queue,
            base_dir,
            project_id,
            flow,
            run_status,
            effective_config,
            worktree_path,
            writer_owner,
            cancellation_token,
        )
        .await
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
            task_source: None,
        };
        create_project(self.project_store, self.journal_store, base_dir, input)?;
        Ok(())
    }

    fn reconcile_cancelled_dispatch_run(
        &self,
        workspace_dir: &Path,
        project_id: &ProjectId,
        writer_owner: &str,
        summary: &'static str,
        log_message: &'static str,
    ) -> AppResult<bool> {
        engine::mark_current_process_running_run_interrupted(
            engine::InterruptedRunContext {
                run_snapshot_read: self.run_snapshot_read,
                run_snapshot_write: self.run_snapshot_write,
                journal_store: self.journal_store,
                log_write: self.log_write,
                base_dir: workspace_dir,
                project_id,
            },
            Some(writer_owner),
            engine::InterruptedRunUpdate {
                summary,
                log_message,
                failure_class: Some("cancellation"),
            },
        )
    }

    fn cancelled_dispatch_handoff_log_message(snapshot: &RunSnapshot) -> Option<&'static str> {
        match snapshot.status_summary.as_str() {
            DAEMON_TASK_CANCELLATION_STATUS_SUMMARY => Some(DAEMON_TASK_CANCELLATION_LOG_MESSAGE),
            DAEMON_SHUTDOWN_STATUS_SUMMARY => Some(DAEMON_SHUTDOWN_LOG_MESSAGE),
            _ => None,
        }
    }

    fn repair_missing_cancelled_dispatch_run_failed_event(
        &self,
        workspace_dir: &Path,
        project_id: &ProjectId,
        snapshot: &RunSnapshot,
    ) -> AppResult<bool> {
        if snapshot.status != RunStatus::Failed || snapshot.active_run.is_some() {
            return Ok(false);
        }

        let Some(log_message) = Self::cancelled_dispatch_handoff_log_message(snapshot) else {
            return Ok(false);
        };
        let Some(interrupted_run) = snapshot.interrupted_run.as_ref() else {
            return Ok(false);
        };

        // A later owner may already be finalizing this handoff. Only repair
        // when there is no durable evidence of a still-live orchestrator.
        if let Ok(Some(pid_record)) = FileSystem::read_pid_file(workspace_dir, project_id) {
            if FileSystem::is_pid_alive(&pid_record) {
                return Ok(false);
            }
        }

        self.finalize_cancelled_dispatch_handoff(
            workspace_dir,
            project_id,
            Some(&engine::RunningAttemptIdentity {
                run_id: interrupted_run.run_id.clone(),
                started_at: interrupted_run.started_at,
            }),
            log_message,
        )
    }

    fn repair_missing_cancelled_dispatch_run_failed_event_and_reload_snapshot(
        &self,
        workspace_dir: &Path,
        project_id: &ProjectId,
        snapshot: &mut RunSnapshot,
    ) -> AppResult<bool> {
        let repaired = self.repair_missing_cancelled_dispatch_run_failed_event(
            workspace_dir,
            project_id,
            snapshot,
        )?;
        if repaired {
            *snapshot = self
                .run_snapshot_read
                .read_run_snapshot(workspace_dir, project_id)?;
        }
        Ok(repaired)
    }

    fn load_dispatch_run_snapshot(
        &self,
        workspace_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<RunSnapshot> {
        let mut snapshot = self
            .run_snapshot_read
            .read_run_snapshot(workspace_dir, project_id)?;
        let _ = self.repair_missing_cancelled_dispatch_run_failed_event_and_reload_snapshot(
            workspace_dir,
            project_id,
            &mut snapshot,
        )?;
        Ok(snapshot)
    }

    fn prepare_cancelled_dispatch_handoff(
        &self,
        workspace_dir: &Path,
        project_id: &ProjectId,
        writer_owner: &str,
        summary: &'static str,
        log_message: &'static str,
    ) -> AppResult<PreparedCancelledDispatchHandoff> {
        let snapshot = self
            .run_snapshot_read
            .read_run_snapshot(workspace_dir, project_id)?;
        if snapshot.status != RunStatus::Running {
            return Ok(PreparedCancelledDispatchHandoff {
                expected_attempt: None,
                interrupted_marker_persisted: false,
            });
        }

        let Some(active_run) = snapshot.active_run.as_ref() else {
            return Ok(PreparedCancelledDispatchHandoff {
                expected_attempt: None,
                interrupted_marker_persisted: false,
            });
        };
        let expected_attempt = engine::RunningAttemptIdentity::from_active_run(active_run);
        let Some(pid_record) = FileSystem::read_pid_file(workspace_dir, project_id)? else {
            return Ok(PreparedCancelledDispatchHandoff {
                expected_attempt: Some(expected_attempt),
                interrupted_marker_persisted: false,
            });
        };
        if pid_record.pid != std::process::id()
            || pid_record.writer_owner.as_deref() != Some(writer_owner)
            || !FileSystem::pid_record_matches_attempt(
                &pid_record,
                &expected_attempt.run_id,
                expected_attempt.started_at,
            )
        {
            return Ok(PreparedCancelledDispatchHandoff {
                expected_attempt: Some(expected_attempt),
                interrupted_marker_persisted: false,
            });
        }

        let interrupted_marker_persisted = engine::mark_running_run_interrupted(
            engine::InterruptedRunContext {
                run_snapshot_read: self.run_snapshot_read,
                run_snapshot_write: self.run_snapshot_write,
                journal_store: self.journal_store,
                log_write: self.log_write,
                base_dir: workspace_dir,
                project_id,
            },
            &expected_attempt,
            engine::InterruptedRunUpdate {
                summary,
                log_message,
                failure_class: None,
            },
        )?;
        Ok(PreparedCancelledDispatchHandoff {
            expected_attempt: Some(expected_attempt),
            interrupted_marker_persisted,
        })
    }

    fn finalize_cancelled_dispatch_handoff(
        &self,
        workspace_dir: &Path,
        project_id: &ProjectId,
        expected_attempt: Option<&engine::RunningAttemptIdentity>,
        log_message: &'static str,
    ) -> AppResult<bool> {
        let Some(expected_attempt) = expected_attempt else {
            return Ok(false);
        };
        let snapshot = self
            .run_snapshot_read
            .read_run_snapshot(workspace_dir, project_id)?;
        if !(snapshot.status == RunStatus::Failed
            && snapshot.active_run.is_none()
            && snapshot
                .interrupted_run
                .as_ref()
                .is_some_and(|interrupted_run| {
                    interrupted_run.run_id == expected_attempt.run_id
                        && interrupted_run.started_at == expected_attempt.started_at
                }))
        {
            return Ok(false);
        }

        let events = self.journal_store.read_journal(workspace_dir, project_id)?;
        if queries::terminal_status_for_attempt(
            &expected_attempt.run_id,
            expected_attempt.started_at,
            &events,
        )
        .is_none()
        {
            let interrupted_run =
                snapshot
                    .interrupted_run
                    .as_ref()
                    .ok_or_else(|| AppError::CorruptRecord {
                        file: "run.json".to_owned(),
                        details: "expected interrupted_run while finalizing daemon cancellation"
                            .to_owned(),
                    })?;
            let run_id =
                RunId::new(&interrupted_run.run_id).map_err(|error| AppError::CorruptRecord {
                    file: "run.json".to_owned(),
                    details: format!("interrupted_run contains invalid run_id: {error}"),
                })?;
            let event = journal::run_failed_event(
                journal::last_sequence(&events) + 1,
                Utc::now(),
                &run_id,
                interrupted_run.stage_cursor.stage,
                "cancellation",
                log_message,
                snapshot.completion_rounds,
                snapshot.max_completion_rounds.unwrap_or(0),
                None,
            );
            let line = journal::serialize_event(&event)?;
            self.journal_store
                .append_event(workspace_dir, project_id, &line)?;
        }

        Ok(true)
    }

    fn cleanup_cancelled_dispatch_backend_processes(
        &self,
        workspace_dir: &Path,
        project_id: &ProjectId,
        expected_attempt: Option<&engine::RunningAttemptIdentity>,
    ) -> AppResult<()> {
        let Some(expected_attempt) = expected_attempt else {
            return Ok(());
        };
        cleanup_stale_backend_process_groups(workspace_dir, project_id, expected_attempt)
            .map(|_| ())
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_cancelled_dispatch<F>(
        &self,
        base_dir: &Path,
        workspace_dir: &Path,
        task: &DaemonTask,
        lease: &crate::contexts::automation_runtime::model::WorktreeLease,
        project_id: &ProjectId,
        heartbeat: &mut tokio::time::Interval,
        dispatch_future: &mut std::pin::Pin<&mut F>,
        summary: &'static str,
        log_message: &'static str,
    ) -> AppResult<()>
    where
        F: Future<Output = AppResult<()>>,
    {
        let interrupted_handoff = self.prepare_cancelled_dispatch_handoff(
            workspace_dir,
            project_id,
            &lease.lease_id,
            summary,
            log_message,
        )?;
        match tokio::time::timeout(DAEMON_DISPATCH_SHUTDOWN_GRACE_PERIOD, async {
            loop {
                tokio::select! {
                    result = dispatch_future.as_mut() => break result,
                    _ = heartbeat.tick() => {
                        let _ = LeaseService::heartbeat(self.store, base_dir, &lease.lease_id);
                    }
                }
            }
        })
        .await
        {
            Ok(result) => {
                if interrupted_handoff.interrupted_marker_persisted {
                    match self.finalize_cancelled_dispatch_handoff(
                        workspace_dir,
                        project_id,
                        interrupted_handoff.expected_attempt.as_ref(),
                        log_message,
                    ) {
                        Ok(true) => {
                            self.cleanup_cancelled_dispatch_backend_processes(
                                workspace_dir,
                                project_id,
                                interrupted_handoff.expected_attempt.as_ref(),
                            )?;
                            Err(AppError::InvocationCancelled {
                                backend: "daemon".to_owned(),
                                contract_id: task.task_id.clone(),
                            })
                        }
                        Ok(false) => result,
                        Err(error) => Err(error),
                    }
                } else {
                    match self.reconcile_cancelled_dispatch_run(
                        workspace_dir,
                        project_id,
                        &lease.lease_id,
                        summary,
                        log_message,
                    ) {
                        Ok(true) => {
                            self.cleanup_cancelled_dispatch_backend_processes(
                                workspace_dir,
                                project_id,
                                interrupted_handoff.expected_attempt.as_ref(),
                            )?;
                            Err(AppError::InvocationCancelled {
                                backend: "daemon".to_owned(),
                                contract_id: task.task_id.clone(),
                            })
                        }
                        Ok(false) => result,
                        Err(error) => Err(error),
                    }
                }
            }
            Err(_) => {
                let cleanup = if interrupted_handoff.interrupted_marker_persisted {
                    self.finalize_cancelled_dispatch_handoff(
                        workspace_dir,
                        project_id,
                        interrupted_handoff.expected_attempt.as_ref(),
                        log_message,
                    )
                } else {
                    self.reconcile_cancelled_dispatch_run(
                        workspace_dir,
                        project_id,
                        &lease.lease_id,
                        summary,
                        log_message,
                    )
                };
                let backend_cleanup = self.cleanup_cancelled_dispatch_backend_processes(
                    workspace_dir,
                    project_id,
                    interrupted_handoff.expected_attempt.as_ref(),
                );
                match cleanup {
                    Ok(true) => eprintln!(
                        "daemon: cancellation grace period expired for task '{}'; reconciled run state directly",
                        task.task_id
                    ),
                    Ok(false) => eprintln!(
                        "daemon: cancellation grace period expired for task '{}' but no matching running attempt was left to reconcile",
                        task.task_id
                    ),
                    Err(ref error) => eprintln!(
                        "daemon: cancellation grace period expired for task '{}' and interrupted-state cleanup failed: {}",
                        task.task_id, error
                    ),
                }
                match (cleanup, backend_cleanup) {
                    (Ok(_), Ok(())) => Err(AppError::InvocationCancelled {
                        backend: "daemon".to_owned(),
                        contract_id: task.task_id.clone(),
                    }),
                    (Err(error), Ok(())) => Err(error),
                    (Ok(_), Err(error)) => Err(error),
                    (Err(cleanup_error), Err(backend_error)) => {
                        Err(AppError::Io(std::io::Error::other(format!(
                            "daemon cancellation cleanup failed: interrupted-state cleanup error: {cleanup_error}; backend cleanup error: {backend_error}"
                        ))))
                    }
                }
            }
        }
    }

    fn cleanup_active_leases(&self, store_dir: &Path, repo_root: &Path) -> AppResult<()> {
        let leases = self.store.list_leases(store_dir)?;
        let mut failures = Vec::new();
        for lease in &leases {
            // Preserve checkpoint branch for tasks that were already failed
            // before shutdown (e.g. retained lease after label-sync failure).
            if let Ok(task) = self.store.read_task(store_dir, &lease.task_id) {
                if task.status == TaskStatus::Failed {
                    self.try_push_failed_task_branch(repo_root, lease);
                }
            }
            let _ = DaemonTaskService::mark_aborted(self.store, store_dir, &lease.task_id);
            if let Err(e) = self.release_task_lease(store_dir, repo_root, &lease.task_id, lease) {
                eprintln!(
                    "daemon: cleanup failed for lease '{}' (task '{}'): {}",
                    lease.lease_id, lease.task_id, e
                );
                failures.push((lease.lease_id.clone(), lease.task_id.clone(), e));
            }
        }
        match failures.len() {
            0 => Ok(()),
            1 => Err(failures
                .into_iter()
                .next()
                .expect("single cleanup failure")
                .2),
            _ => Err(AppError::Io(std::io::Error::other(format!(
                "daemon lease cleanup failed for multiple leases: {}",
                failures
                    .into_iter()
                    .map(|(lease_id, task_id, error)| {
                        format!("lease '{lease_id}' (task '{task_id}'): {error}")
                    })
                    .collect::<Vec<_>>()
                    .join("; ")
            )))),
        }
    }

    fn cleanup_registered_active_leases(
        &self,
        data_dir: &Path,
        registrations: &[RepoRegistration],
    ) -> AppResult<()> {
        let mut failures = Vec::new();
        for reg in registrations {
            if let Ok((owner, repo)) = parse_repo_slug(&reg.repo_slug) {
                let daemon_dir = DataDirLayout::daemon_dir(data_dir, owner, repo);
                if let Err(error) = self.cleanup_active_leases(&daemon_dir, &reg.repo_root) {
                    failures.push((reg.repo_slug.clone(), error));
                }
            }
        }

        match failures.len() {
            0 => Ok(()),
            1 => Err(failures
                .into_iter()
                .next()
                .expect("single repo cleanup failure")
                .1),
            _ => Err(AppError::Io(std::io::Error::other(format!(
                "daemon lease cleanup failed across multiple repos: {}",
                failures
                    .into_iter()
                    .map(|(repo_slug, error)| format!("{repo_slug}: {error}"))
                    .collect::<Vec<_>>()
                    .join("; ")
            )))),
        }
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
            self.journal_store,
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
                .map(|resolver| resolver.as_ref() as &dyn RebaseConflictResolver),
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

    #[allow(clippy::too_many_arguments)]
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
    ) -> AppResult<Box<dyn RebaseConflictResolver + 'a>> {
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
        let project_root = repo_root
            .join(WORKSPACE_DIR)
            .join("runtime")
            .join("rebase-agent")
            .join(&task.task_id);
        let timeout = Duration::from_secs(effective_config.rebase_policy().agent_timeout);

        if let Some(builder) = self.configured_agent_service_builder {
            return Ok(Box::new(ConfiguredDaemonRebaseConflictResolver {
                agent_service: builder(effective_config)?,
                project_root,
                working_dir: worktree_path.to_path_buf(),
                target,
                timeout,
                task_id: task.task_id.clone(),
            }));
        }

        Ok(Box::new(DaemonRebaseConflictResolver {
            agent_service: self.agent_service,
            project_root,
            working_dir: worktree_path.to_path_buf(),
            target,
            timeout,
            task_id: task.task_id.clone(),
        }))
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
        self.try_push_failed_task_branch(repo_root, lease);
        let cleanup_result = self.release_task_lease(base_dir, repo_root, task_id, lease);

        match (mark_result, cleanup_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Err(error), Err(_)) => Err(error),
        }
    }

    /// Best-effort push of the worktree branch to preserve checkpoint commits
    /// from a failed task run. Delegates to the centralized
    /// [`try_preserve_failed_branch`] helper.
    fn try_push_failed_task_branch(
        &self,
        repo_root: &Path,
        lease: &crate::contexts::automation_runtime::model::WorktreeLease,
    ) {
        crate::contexts::automation_runtime::lease_service::try_preserve_failed_branch(
            self.worktree,
            repo_root,
            lease,
        );
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
                let cleared =
                    DaemonTaskService::clear_lease_reference(self.store, base_dir, task_id)
                        .map(|_| ());
                if cleared.is_ok() {
                    if let Ok(project_id) = ProjectId::new(lease.project_id.clone()) {
                        // Only remove the pid file if it still belongs to
                        // this task's run.  A fresh run may have claimed the
                        // project and written a new pid file after the writer
                        // lock was released above.
                        if let Ok(Some(pid_record)) =
                            FileSystem::read_pid_file(base_dir, &project_id)
                        {
                            if pid_record.pid == std::process::id() {
                                let _ = FileSystem::remove_pid_file_if_matches(
                                    base_dir,
                                    &project_id,
                                    &pid_record,
                                );
                            }
                        }
                    }
                }
                cleared
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

#[allow(clippy::too_many_arguments)]
async fn dispatch_in_worktree_with_service<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    run_snapshot_read: &dyn RunSnapshotPort,
    run_snapshot_write: &dyn RunSnapshotWritePort,
    journal_store: &dyn JournalStorePort,
    artifact_store: &dyn ArtifactStorePort,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    amendment_queue: &dyn AmendmentQueuePort,
    base_dir: &Path,
    project_id: &ProjectId,
    flow: FlowPreset,
    run_status: RunStatus,
    effective_config: &EffectiveConfig,
    worktree_path: &Path,
    writer_owner: Option<&str>,
    cancellation_token: CancellationToken,
) -> AppResult<()>
where
    A: AgentExecutionPort + Sync,
    R: RawOutputPort + Sync,
    S: SessionStorePort + Sync,
{
    match run_status {
        RunStatus::NotStarted => {
            engine::execute_run_with_retry(
                agent_service,
                run_snapshot_read,
                run_snapshot_write,
                journal_store,
                artifact_write,
                log_write,
                amendment_queue,
                base_dir,
                Some(worktree_path),
                project_id,
                writer_owner,
                flow,
                effective_config,
                &RetryPolicy::default_policy().with_max_remediation_cycles(
                    effective_config.run_policy().max_review_iterations,
                ),
                cancellation_token,
            )
            .await
        }
        RunStatus::Failed | RunStatus::Paused => {
            engine::resume_run_with_retry(
                agent_service,
                run_snapshot_read,
                run_snapshot_write,
                journal_store,
                artifact_store,
                artifact_write,
                log_write,
                amendment_queue,
                base_dir,
                Some(worktree_path),
                project_id,
                writer_owner,
                flow,
                effective_config,
                &RetryPolicy::default_policy().with_max_remediation_cycles(
                    effective_config.run_policy().max_review_iterations,
                ),
                cancellation_token,
            )
            .await
        }
        RunStatus::Running => Err(AppError::TaskStateTransitionInvalid {
            task_id: project_id.to_string(),
            from: "run_running".to_owned(),
            to: "daemon_dispatch".to_owned(),
        }),
        RunStatus::Completed => {
            // The run already completed — nothing to dispatch. This happens
            // when a task is retried after a reconciliation-only failure
            // (e.g., reconciliation_br_failed, reconciliation_metadata_error,
            // reconciliation_milestone_update_failed). The run succeeded but
            // post-run bookkeeping (bead close, sync, milestone update) failed.
            // Returning Ok(()) lets the caller fall through to the success
            // path where try_reconcile_success runs again. All reconciliation
            // steps are idempotent: br close checks bead status, sync is
            // unconditional, and record_bead_completion handles replays.
            Ok(())
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

struct ConfiguredDaemonRebaseConflictResolver {
    agent_service: crate::composition::agent_execution_builder::ProductionAgentService,
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
        resolve_rebase_conflicts_with_service(
            self.agent_service,
            &self.project_root,
            &self.working_dir,
            &self.target,
            self.timeout,
            &self.task_id,
            request,
        )
    }
}

impl RebaseConflictResolver for ConfiguredDaemonRebaseConflictResolver {
    fn resolve_conflicts(
        &self,
        request: &RebaseConflictRequest,
    ) -> AppResult<RebaseConflictResolution> {
        resolve_rebase_conflicts_with_service(
            &self.agent_service,
            &self.project_root,
            &self.working_dir,
            &self.target,
            self.timeout,
            &self.task_id,
            request,
        )
    }
}

fn resolve_rebase_conflicts_with_service<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    project_root: &Path,
    working_dir: &Path,
    target: &ResolvedBackendTarget,
    timeout: Duration,
    task_id: &str,
    request: &RebaseConflictRequest,
) -> AppResult<RebaseConflictResolution>
where
    A: AgentExecutionPort + Sync,
    R: RawOutputPort + Sync,
    S: SessionStorePort + Sync,
{
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
        invocation_id: format!("rebase-{task_id}-{}", Utc::now().timestamp_millis()),
        project_root: project_root.to_path_buf(),
        working_dir: working_dir.to_path_buf(),
        contract: InvocationContract::Requirements {
            label: "daemon:rebase_resolution".to_owned(),
        },
        role: BackendRole::Implementer,
        resolved_target: target.clone(),
        payload: InvocationPayload { prompt, context },
        timeout,
        cancellation_token: CancellationToken::new(),
        session_policy: SessionPolicy::NewSession,
        prior_session: None,
        attempt_number: 1,
    };

    let envelope = thread::scope(|scope| {
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

    serde_json::from_value(envelope.parsed_payload).map_err(|error| AppError::InvocationFailed {
        backend: target.backend.family.to_string(),
        contract_id: "daemon:rebase_resolution".to_owned(),
        failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
        details: format!("invalid rebase agent response: {error}"),
    })
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

#[cfg(all(test, feature = "test-stub"))]
mod tests {
    use std::future::{poll_fn, Future};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::Poll;
    use std::time::Duration;

    use chrono::Utc;
    use tempfile::tempdir;

    use super::{
        DaemonLoop, DaemonLoopConfig, DAEMON_DISPATCH_SHUTDOWN_GRACE_PERIOD,
        DAEMON_SHUTDOWN_STATUS_SUMMARY,
    };
    use crate::adapters::fs::{
        FileSystem, FsAmendmentQueueStore, FsArtifactStore, FsDaemonStore, FsJournalStore,
        FsMilestoneJournalStore, FsMilestonePlanStore, FsMilestoneSnapshotStore, FsMilestoneStore,
        FsPayloadArtifactWriteStore, FsProjectStore, FsRawOutputStore, FsRequirementsStore,
        FsRunSnapshotStore, FsRunSnapshotWriteStore, FsRuntimeLogWriteStore, FsSessionStore,
        RunPidOwner,
    };
    use crate::adapters::stub_backend::StubBackendAdapter;
    use crate::adapters::worktree::WorktreeAdapter;
    use crate::contexts::agent_execution::model::CancellationToken;
    use crate::contexts::agent_execution::AgentExecutionService;
    use crate::contexts::automation_runtime::model::{
        DaemonJournalEvent, DaemonTask, DispatchMode, TaskStatus,
    };
    use crate::contexts::automation_runtime::{
        DaemonStorePort, LeaseRecord, ResourceCleanupOutcome, WorktreeCleanupOutcome,
        WorktreeLease, WorktreePort, WriterLockReleaseOutcome,
    };
    use crate::contexts::milestone_record::service::{
        create_milestone, update_status, CreateMilestoneInput, MilestoneSnapshotPort,
    };
    use crate::contexts::project_run_record::model::{ActiveRun, RunSnapshot, RunStatus};
    use crate::contexts::project_run_record::service::{
        create_project, CreateProjectInput, JournalStorePort, RunSnapshotPort, RunSnapshotWritePort,
    };
    use crate::contexts::requirements_drafting::service::{
        RequirementsService, RequirementsStorePort,
    };
    use crate::contexts::workspace_governance::initialize_workspace;
    use crate::shared::domain::FlowPreset;
    use crate::shared::domain::{ProjectId, StageCursor, StageId};
    use crate::shared::error::{AppError, AppResult};

    fn sample_waiting_task(task_id: &str, run_id: &str) -> DaemonTask {
        let now = Utc::now();
        DaemonTask {
            task_id: task_id.to_owned(),
            issue_ref: format!("acme/widgets#{}", task_id),
            project_id: "demo".to_owned(),
            project_name: Some("Demo".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: None,
            routing_warnings: vec![],
            status: TaskStatus::WaitingForRequirements,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: DispatchMode::RequirementsDraft,
            source_revision: None,
            requirements_run_id: Some(run_id.to_owned()),
            workflow_run_id: None,
            repo_slug: None,
            issue_number: None,
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        }
    }

    fn sample_pending_task(task_id: &str, project_id: &ProjectId) -> DaemonTask {
        let now = Utc::now();
        DaemonTask {
            task_id: task_id.to_owned(),
            issue_ref: format!("acme/widgets#{}", task_id),
            project_id: project_id.as_str().to_owned(),
            project_name: Some("Demo".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec![],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: None,
            routing_warnings: vec![],
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
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
        }
    }

    struct TestWorktreeAdapter {
        remove_outcome: WorktreeCleanupOutcome,
    }

    impl TestWorktreeAdapter {
        fn removing_as(remove_outcome: WorktreeCleanupOutcome) -> Self {
            Self { remove_outcome }
        }
    }

    impl WorktreePort for TestWorktreeAdapter {
        fn worktree_path(&self, base_dir: &std::path::Path, task_id: &str) -> std::path::PathBuf {
            base_dir.join(".test-worktrees").join(task_id)
        }

        fn branch_name(&self, task_id: &str) -> String {
            format!("rb/{task_id}")
        }

        fn create_worktree(
            &self,
            _repo_root: &std::path::Path,
            worktree_path: &std::path::Path,
            _branch_name: &str,
            _task_id: &str,
        ) -> AppResult<()> {
            std::fs::create_dir_all(worktree_path)?;
            Ok(())
        }

        fn remove_worktree(
            &self,
            _repo_root: &std::path::Path,
            worktree_path: &std::path::Path,
            _task_id: &str,
        ) -> AppResult<WorktreeCleanupOutcome> {
            match self.remove_outcome {
                WorktreeCleanupOutcome::Removed => {
                    if worktree_path.exists() {
                        std::fs::remove_dir_all(worktree_path)?;
                    }
                    Ok(WorktreeCleanupOutcome::Removed)
                }
                WorktreeCleanupOutcome::AlreadyAbsent => Ok(WorktreeCleanupOutcome::AlreadyAbsent),
            }
        }

        fn rebase_onto_default_branch(
            &self,
            _repo_root: &std::path::Path,
            _worktree_path: &std::path::Path,
            _branch_name: &str,
        ) -> AppResult<()> {
            Ok(())
        }
    }

    fn create_standard_project(base_dir: &std::path::Path, project_id: &str) -> ProjectId {
        let project_id = ProjectId::new(project_id).expect("project id");
        let prompt_contents = "# Test prompt";
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base_dir,
            CreateProjectInput {
                id: project_id.clone(),
                name: format!("Test {}", project_id.as_str()),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: prompt_contents.to_owned(),
                prompt_hash: FileSystem::prompt_hash(prompt_contents),
                created_at: Utc::now(),
                task_source: None,
            },
        )
        .expect("create project");
        project_id
    }

    struct FailOnceWaitingTaskWriteStore {
        fail_next_waiting_write: AtomicBool,
    }

    impl FailOnceWaitingTaskWriteStore {
        fn new() -> Self {
            Self {
                fail_next_waiting_write: AtomicBool::new(true),
            }
        }
    }

    impl DaemonStorePort for FailOnceWaitingTaskWriteStore {
        fn list_tasks(&self, base_dir: &std::path::Path) -> AppResult<Vec<DaemonTask>> {
            FsDaemonStore.list_tasks(base_dir)
        }

        fn read_task(&self, base_dir: &std::path::Path, task_id: &str) -> AppResult<DaemonTask> {
            FsDaemonStore.read_task(base_dir, task_id)
        }

        fn create_task(&self, base_dir: &std::path::Path, task: &DaemonTask) -> AppResult<()> {
            FsDaemonStore.create_task(base_dir, task)
        }

        fn write_task(&self, base_dir: &std::path::Path, task: &DaemonTask) -> AppResult<()> {
            if task.status == TaskStatus::WaitingForRequirements
                && self.fail_next_waiting_write.swap(false, Ordering::SeqCst)
            {
                return Err(AppError::Io(std::io::Error::other(
                    "simulated waiting-task metadata write failure",
                )));
            }

            FsDaemonStore.write_task(base_dir, task)
        }

        fn list_leases(&self, base_dir: &std::path::Path) -> AppResult<Vec<WorktreeLease>> {
            FsDaemonStore.list_leases(base_dir)
        }

        fn read_lease(
            &self,
            base_dir: &std::path::Path,
            lease_id: &str,
        ) -> AppResult<WorktreeLease> {
            FsDaemonStore.read_lease(base_dir, lease_id)
        }

        fn write_lease(&self, base_dir: &std::path::Path, lease: &WorktreeLease) -> AppResult<()> {
            FsDaemonStore.write_lease(base_dir, lease)
        }

        fn list_lease_records(&self, base_dir: &std::path::Path) -> AppResult<Vec<LeaseRecord>> {
            FsDaemonStore.list_lease_records(base_dir)
        }

        fn read_lease_record(
            &self,
            base_dir: &std::path::Path,
            lease_id: &str,
        ) -> AppResult<LeaseRecord> {
            FsDaemonStore.read_lease_record(base_dir, lease_id)
        }

        fn write_lease_record(
            &self,
            base_dir: &std::path::Path,
            lease: &LeaseRecord,
        ) -> AppResult<()> {
            FsDaemonStore.write_lease_record(base_dir, lease)
        }

        fn remove_lease(
            &self,
            base_dir: &std::path::Path,
            lease_id: &str,
        ) -> AppResult<ResourceCleanupOutcome> {
            FsDaemonStore.remove_lease(base_dir, lease_id)
        }

        fn read_daemon_journal(
            &self,
            base_dir: &std::path::Path,
        ) -> AppResult<Vec<DaemonJournalEvent>> {
            FsDaemonStore.read_daemon_journal(base_dir)
        }

        fn append_daemon_journal_event(
            &self,
            base_dir: &std::path::Path,
            event: &DaemonJournalEvent,
        ) -> AppResult<()> {
            FsDaemonStore.append_daemon_journal_event(base_dir, event)
        }

        fn acquire_writer_lock(
            &self,
            base_dir: &std::path::Path,
            project_id: &ProjectId,
            lease_id: &str,
        ) -> AppResult<()> {
            FsDaemonStore.acquire_writer_lock(base_dir, project_id, lease_id)
        }

        fn release_writer_lock(
            &self,
            base_dir: &std::path::Path,
            project_id: &ProjectId,
            expected_owner: &str,
        ) -> AppResult<WriterLockReleaseOutcome> {
            FsDaemonStore.release_writer_lock(base_dir, project_id, expected_owner)
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn check_waiting_tasks_reports_seed_handoff_failures_as_changed() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();

        let requirements_service = RequirementsService::new(
            AgentExecutionService::new(
                StubBackendAdapter::default(),
                FsRawOutputStore,
                FsSessionStore,
            ),
            FsRequirementsStore,
        );
        let run_id = requirements_service
            .draft(base, "seed validation failure", Utc::now(), None)
            .await
            .expect("draft seed");

        let req_store = FsRequirementsStore;
        let mut run = req_store.read_run(base, &run_id).expect("read run");
        run.latest_seed_id = None;
        req_store.write_run(base, &run_id, &run).expect("write run");

        let daemon_store = FsDaemonStore;
        daemon_store
            .create_task(base, &sample_waiting_task("waiting-seed-invalid", &run_id))
            .expect("create task");

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &daemon_store,
            &WorktreeAdapter,
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            &agent_service,
        )
        .with_requirements_store(&req_store);

        let changed = daemon
            .check_waiting_tasks(base, base)
            .expect("check waiting tasks");
        assert_eq!(changed, vec!["waiting-seed-invalid".to_owned()]);

        let failed = daemon_store
            .read_task(base, "waiting-seed-invalid")
            .expect("read failed task");
        assert_eq!(failed.status, TaskStatus::Failed);
        assert_eq!(failed.failure_class.as_deref(), Some("seed_handoff_failed"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn check_waiting_tasks_reports_post_seed_metadata_failures_as_changed() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();

        let requirements_service = RequirementsService::new(
            AgentExecutionService::new(
                StubBackendAdapter::default(),
                FsRawOutputStore,
                FsSessionStore,
            ),
            FsRequirementsStore,
        );
        let run_id = requirements_service
            .draft(base, "seed metadata failure", Utc::now(), None)
            .await
            .expect("draft seed");
        let req_store = FsRequirementsStore;

        let daemon_store = FailOnceWaitingTaskWriteStore::new();
        daemon_store
            .create_task(
                base,
                &sample_waiting_task("waiting-seed-metadata-fail", &run_id),
            )
            .expect("create task");

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &daemon_store,
            &WorktreeAdapter,
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            &agent_service,
        )
        .with_requirements_store(&req_store);

        let changed = daemon
            .check_waiting_tasks(base, base)
            .expect("check waiting tasks");
        assert_eq!(changed, vec!["waiting-seed-metadata-fail".to_owned()]);

        let failed = daemon_store
            .read_task(base, "waiting-seed-metadata-fail")
            .expect("read failed task");
        assert_eq!(failed.status, TaskStatus::Failed);
        assert_eq!(
            failed.failure_class.as_deref(),
            Some("requirements_linking_failed")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn check_waiting_tasks_reports_milestone_handoff_validation_failures_as_changed() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();

        let requirements_service = RequirementsService::new(
            AgentExecutionService::new(
                StubBackendAdapter::default(),
                FsRawOutputStore,
                FsSessionStore,
            ),
            FsRequirementsStore,
        );
        let run_id = requirements_service
            .draft_milestone(base, "validation failure", Utc::now(), None)
            .await
            .expect("draft milestone");

        let req_store = FsRequirementsStore;
        let mut run = req_store.read_run(base, &run_id).expect("read run");
        let payload_id = run
            .latest_milestone_bundle_id
            .clone()
            .expect("payload id should exist");
        let mut bundle = run.milestone_bundle.clone().expect("bundle");
        bundle.schema_version += 1;
        run.milestone_bundle = Some(bundle.clone());
        req_store.write_run(base, &run_id, &run).expect("write run");
        req_store
            .write_payload(
                base,
                &run_id,
                &payload_id,
                &serde_json::to_value(&bundle).expect("serialize invalid bundle"),
            )
            .expect("write invalid payload");

        let daemon_store = FsDaemonStore;
        daemon_store
            .create_task(
                base,
                &sample_waiting_task("waiting-invalid-bundle", &run_id),
            )
            .expect("create task");

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &daemon_store,
            &WorktreeAdapter,
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            &agent_service,
        )
        .with_requirements_store(&req_store);

        let changed = daemon
            .check_waiting_tasks(base, base)
            .expect("check waiting tasks");
        assert_eq!(changed, vec!["waiting-invalid-bundle".to_owned()]);

        let failed = daemon_store
            .read_task(base, "waiting-invalid-bundle")
            .expect("read failed task");
        assert_eq!(failed.status, TaskStatus::Failed);
        assert_eq!(
            failed.failure_class.as_deref(),
            Some("milestone_handoff_failed")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn check_waiting_tasks_completes_existing_terminal_milestone_handoffs() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();

        let now = Utc::now();
        let requirements_service = RequirementsService::new(
            AgentExecutionService::new(
                StubBackendAdapter::default(),
                FsRawOutputStore,
                FsSessionStore,
            ),
            FsRequirementsStore,
        );
        let run_id = requirements_service
            .draft_milestone(base, "materialize failure", now, None)
            .await
            .expect("draft milestone");
        let req_store = FsRequirementsStore;

        let milestone_store = FsMilestoneStore;
        let plan_store = FsMilestonePlanStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let handoff =
            crate::contexts::requirements_drafting::service::extract_milestone_bundle_handoff(
                &req_store, base, &run_id,
            )
            .expect("extract milestone handoff");
        let existing = create_milestone(
            &milestone_store,
            base,
            CreateMilestoneInput {
                id: "ms-stub".to_owned(),
                name: "Stub Milestone".to_owned(),
                description: "existing completed milestone".to_owned(),
            },
            now,
        )
        .expect("create existing milestone");
        crate::contexts::milestone_record::service::materialize_bundle(
            &milestone_store,
            &snapshot_store,
            &journal_store,
            &plan_store,
            base,
            &handoff.bundle,
            now,
        )
        .expect("materialize existing milestone");
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &existing.id,
            crate::contexts::milestone_record::model::MilestoneStatus::Running,
            now + chrono::Duration::seconds(1),
        )
        .expect("start milestone");
        let mut existing_snapshot = snapshot_store
            .read_snapshot(base, &existing.id)
            .expect("read existing snapshot");
        existing_snapshot.progress.completed_beads = existing_snapshot.progress.total_beads;
        snapshot_store
            .write_snapshot(base, &existing.id, &existing_snapshot)
            .expect("persist completed progress");
        update_status(
            &snapshot_store,
            &journal_store,
            base,
            &existing.id,
            crate::contexts::milestone_record::model::MilestoneStatus::Completed,
            now + chrono::Duration::seconds(2),
        )
        .expect("complete milestone");

        let daemon_store = FsDaemonStore;
        daemon_store
            .create_task(
                base,
                &sample_waiting_task("waiting-materialize-fail", &run_id),
            )
            .expect("create task");

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &daemon_store,
            &WorktreeAdapter,
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            &agent_service,
        )
        .with_requirements_store(&req_store);

        let changed = daemon
            .check_waiting_tasks(base, base)
            .expect("check waiting tasks");
        assert_eq!(changed, vec!["waiting-materialize-fail".to_owned()]);

        let failed = daemon_store
            .read_task(base, "waiting-materialize-fail")
            .expect("read completed task");
        assert_eq!(failed.status, TaskStatus::Completed);
        assert_eq!(failed.failure_class, None);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn finish_cancelled_dispatch_reconciles_running_snapshot_after_grace_timeout() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");
        let project_id = create_standard_project(base, "daemon-cancel-timeout");
        let started_at = Utc::now();
        let run_id = "run-daemon-cancel-timeout".to_owned();

        let snapshot = RunSnapshot {
            active_run: Some(ActiveRun {
                run_id: run_id.clone(),
                stage_cursor: StageCursor::initial(StageId::Planning),
                started_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                stage_resolution_snapshot: None,
            }),
            interrupted_run: None,
            status: RunStatus::Running,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "running: planning".to_owned(),
            last_stage_resolution_snapshot: None,
        };
        FsRunSnapshotWriteStore
            .write_run_snapshot(base, &project_id, &snapshot)
            .expect("write running snapshot");

        FileSystem::write_pid_file(
            base,
            &project_id,
            RunPidOwner::Daemon,
            Some("lease-daemon-cancel-timeout"),
            Some(run_id.as_str()),
            Some(started_at),
        )
        .expect("write pid file");

        let mut task = sample_waiting_task("daemon-cancel-timeout-task", "req-run");
        task.project_id = project_id.as_str().to_owned();
        task.status = TaskStatus::Active;
        task.dispatch_mode = DispatchMode::Workflow;
        task.requirements_run_id = None;

        let lease = WorktreeLease {
            lease_id: "lease-daemon-cancel-timeout".to_owned(),
            task_id: task.task_id.clone(),
            project_id: project_id.as_str().to_owned(),
            worktree_path: base.to_path_buf(),
            branch_name: "rb/test".to_owned(),
            acquired_at: Utc::now(),
            ttl_seconds: 300,
            last_heartbeat: Utc::now(),
        };

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &FsDaemonStore,
            &WorktreeAdapter,
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            &agent_service,
        );

        let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
        let dispatch_future = std::future::pending::<AppResult<()>>();
        tokio::pin!(dispatch_future);
        let mut cleanup_future = std::pin::pin!(daemon.finish_cancelled_dispatch(
            base,
            base,
            &task,
            &lease,
            &project_id,
            &mut heartbeat,
            &mut dispatch_future,
            "failed (interrupted by daemon shutdown)",
            "daemon shutdown interrupted the orchestrator before graceful shutdown completed",
        ));

        poll_fn(|cx| {
            assert!(
                cleanup_future.as_mut().poll(cx).is_pending(),
                "cleanup future should remain pending until the grace period expires"
            );
            Poll::Ready(())
        })
        .await;

        let interrupted = FsRunSnapshotStore
            .read_run_snapshot(base, &project_id)
            .expect("read interrupted snapshot");
        assert_eq!(interrupted.status, RunStatus::Failed);
        assert!(interrupted.active_run.is_none());
        assert!(
            interrupted.interrupted_run.is_some(),
            "interrupted run metadata should be preserved before the grace period expires"
        );
        assert!(
            FileSystem::read_pid_file(base, &project_id)
                .expect("read pid file")
                .is_none(),
            "daemon cancellation handoff should remove run.pid before the grace period expires"
        );

        tokio::time::advance(DAEMON_DISPATCH_SHUTDOWN_GRACE_PERIOD + Duration::from_millis(1))
            .await;

        let error = cleanup_future
            .await
            .expect_err("timeout cleanup should surface a cancellation error");
        assert!(
            matches!(error, AppError::InvocationCancelled { .. }),
            "unexpected error after timeout cleanup: {error:?}"
        );

        let recovered = FsRunSnapshotStore
            .read_run_snapshot(base, &project_id)
            .expect("read recovered snapshot");
        assert_eq!(recovered.status, RunStatus::Failed);
        assert!(recovered.active_run.is_none());
        assert!(
            recovered.interrupted_run.is_some(),
            "interrupted run metadata should be preserved for resume"
        );
        assert!(
            FileSystem::read_pid_file(base, &project_id)
                .expect("read pid file")
                .is_none(),
            "timeout cleanup should remove run.pid before returning"
        );
    }

    #[test]
    fn load_dispatch_run_snapshot_repairs_missing_run_failed_event_from_daemon_handoff() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");
        let project_id = create_standard_project(base, "daemon-handoff-repair");
        let started_at = Utc::now();

        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-daemon-handoff-repair".to_owned(),
                stage_cursor: StageCursor::initial(StageId::Implementation),
                started_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: DAEMON_SHUTDOWN_STATUS_SUMMARY.to_owned(),
            last_stage_resolution_snapshot: None,
        };
        FsRunSnapshotWriteStore
            .write_run_snapshot(base, &project_id, &snapshot)
            .expect("write interrupted snapshot");

        std::fs::write(
            base.join(format!(
                ".ralph-burning/projects/{}/journal.ndjson",
                project_id.as_str()
            )),
            format!(
                "{{\"sequence\":1,\"timestamp\":\"{}\",\"event_type\":\"project_created\",\"details\":{{\"project_id\":\"{}\",\"flow\":\"standard\"}}}}\n{{\"sequence\":2,\"timestamp\":\"{}\",\"event_type\":\"run_started\",\"details\":{{\"run_id\":\"run-daemon-handoff-repair\",\"first_stage\":\"implementation\",\"max_completion_rounds\":20}}}}",
                (started_at - chrono::Duration::seconds(1)).to_rfc3339(),
                project_id.as_str(),
                started_at.to_rfc3339(),
            ),
        )
        .expect("write journal without run_failed");

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &FsDaemonStore,
            &WorktreeAdapter,
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            &agent_service,
        );

        let repaired = daemon
            .load_dispatch_run_snapshot(base, &project_id)
            .expect("load repaired snapshot");
        assert_eq!(repaired.status, RunStatus::Failed);
        assert!(repaired.interrupted_run.is_some());

        let journal = FsJournalStore
            .read_journal(base, &project_id)
            .expect("read repaired journal");
        assert_eq!(
            journal
                .iter()
                .filter(|event| {
                    event.event_type
                        == crate::contexts::project_run_record::model::JournalEventType::RunFailed
                })
                .count(),
            1,
            "dispatch snapshot load should append the missing daemon run_failed event"
        );
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn finish_cancelled_dispatch_cleans_tracked_backend_processes_before_returning() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");
        let project_id = create_standard_project(base, "daemon-cancel-backend-cleanup");
        let started_at = Utc::now();
        let run_id = "run-daemon-cancel-backend-cleanup".to_owned();

        let snapshot = RunSnapshot {
            active_run: Some(ActiveRun {
                run_id: run_id.clone(),
                stage_cursor: StageCursor::initial(StageId::Implementation),
                started_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                stage_resolution_snapshot: None,
            }),
            interrupted_run: None,
            status: RunStatus::Running,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "running: implementation".to_owned(),
            last_stage_resolution_snapshot: None,
        };
        FsRunSnapshotWriteStore
            .write_run_snapshot(base, &project_id, &snapshot)
            .expect("write running snapshot");

        FileSystem::write_pid_file(
            base,
            &project_id,
            RunPidOwner::Daemon,
            Some("lease-daemon-backend-cleanup"),
            Some(run_id.as_str()),
            Some(started_at),
        )
        .expect("write pid file");

        let backend_pid_path = base.join("tracked-backend.pid");
        let script = format!(
            "setsid sh -c 'echo $$ > \"{}\"; exec sleep 60' & while [ ! -s \"{}\" ]; do sleep 0.05; done",
            backend_pid_path.display(),
            backend_pid_path.display(),
        );
        let mut parent = std::process::Command::new("bash")
            .args(["-lc", &script])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn backend helper");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while (!backend_pid_path.exists() || std::fs::read_to_string(&backend_pid_path).is_err())
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        let backend_pid = std::fs::read_to_string(&backend_pid_path)
            .expect("read backend pid")
            .trim()
            .parse::<u32>()
            .expect("parse backend pid");
        assert!(
            FileSystem::is_pid_running_unchecked(backend_pid),
            "tracked backend should be alive before cleanup"
        );
        let status = parent.wait().expect("wait for backend helper");
        assert!(
            status.success(),
            "backend helper should exit cleanly: {status:?}"
        );

        let project_root = FileSystem::live_project_root(base, &project_id);
        FileSystem::register_backend_process(&project_root, backend_pid)
            .expect("register tracked backend process");
        assert_eq!(
            FileSystem::read_backend_processes(base, &project_id)
                .expect("read tracked backend processes")
                .len(),
            1,
            "tracked backend cleanup precondition should be durable"
        );

        let mut task = sample_waiting_task("daemon-cancel-backend-cleanup-task", "req-run");
        task.project_id = project_id.as_str().to_owned();
        task.status = TaskStatus::Active;
        task.dispatch_mode = DispatchMode::Workflow;
        task.requirements_run_id = None;

        let lease = WorktreeLease {
            lease_id: "lease-daemon-backend-cleanup".to_owned(),
            task_id: task.task_id.clone(),
            project_id: project_id.as_str().to_owned(),
            worktree_path: base.to_path_buf(),
            branch_name: "rb/test".to_owned(),
            acquired_at: Utc::now(),
            ttl_seconds: 300,
            last_heartbeat: Utc::now(),
        };

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &FsDaemonStore,
            &WorktreeAdapter,
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            &agent_service,
        );

        let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
        let dispatch_future = std::future::pending::<AppResult<()>>();
        tokio::pin!(dispatch_future);
        let mut cleanup_future = std::pin::pin!(daemon.finish_cancelled_dispatch(
            base,
            base,
            &task,
            &lease,
            &project_id,
            &mut heartbeat,
            &mut dispatch_future,
            "failed (interrupted by daemon shutdown)",
            "daemon shutdown interrupted the orchestrator before graceful shutdown completed",
        ));

        poll_fn(|cx| {
            assert!(
                cleanup_future.as_mut().poll(cx).is_pending(),
                "cleanup future should remain pending until the grace period expires"
            );
            Poll::Ready(())
        })
        .await;

        tokio::time::advance(DAEMON_DISPATCH_SHUTDOWN_GRACE_PERIOD + Duration::from_millis(1))
            .await;
        let error = cleanup_future
            .await
            .expect_err("timeout cleanup should surface a cancellation error");
        assert!(
            matches!(error, AppError::InvocationCancelled { .. }),
            "unexpected error after daemon cancellation cleanup: {error:?}"
        );
        assert!(
            FileSystem::read_backend_processes(base, &project_id)
                .expect("read tracked backend processes after cleanup")
                .is_empty(),
            "daemon cancellation cleanup should prune tracked backend processes"
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while FileSystem::is_pid_running_unchecked(backend_pid)
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        assert!(
            !FileSystem::is_pid_running_unchecked(backend_pid),
            "daemon cancellation cleanup should SIGKILL the tracked backend process group"
        );
    }

    #[test]
    fn cleanup_active_leases_surfaces_partial_release_failures() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");
        let project_id = create_standard_project(base, "daemon-cleanup-partial");
        let now = Utc::now();

        let lease = WorktreeLease {
            lease_id: "lease-daemon-cleanup-partial".to_owned(),
            task_id: "task-daemon-cleanup-partial".to_owned(),
            project_id: project_id.as_str().to_owned(),
            worktree_path: base.join("missing-worktree"),
            branch_name: "rb/test".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
        };
        FsDaemonStore
            .write_lease(base, &lease)
            .expect("write worktree lease");
        FsDaemonStore
            .acquire_writer_lock(base, &project_id, &lease.lease_id)
            .expect("acquire writer lock");
        FsDaemonStore
            .write_task(
                base,
                &DaemonTask {
                    task_id: lease.task_id.clone(),
                    issue_ref: "acme/widgets#partial".to_owned(),
                    project_id: project_id.as_str().to_owned(),
                    project_name: Some("Partial cleanup".to_owned()),
                    prompt: Some("Prompt".to_owned()),
                    routing_command: None,
                    routing_labels: vec![],
                    resolved_flow: Some(FlowPreset::Standard),
                    routing_source: None,
                    routing_warnings: vec![],
                    status: TaskStatus::Active,
                    created_at: now,
                    updated_at: now,
                    attempt_count: 0,
                    lease_id: Some(lease.lease_id.clone()),
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

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &FsDaemonStore,
            &WorktreeAdapter,
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            &agent_service,
        );

        let error = daemon
            .cleanup_active_leases(base, base)
            .expect_err("missing worktree should surface partial cleanup failure");
        assert!(
            matches!(error, AppError::LeaseCleanupPartialFailure { .. }),
            "unexpected cleanup error: {error:?}"
        );
        assert!(
            FsDaemonStore.read_lease(base, &lease.lease_id).is_ok(),
            "partial cleanup must preserve the durable lease record"
        );
        let task = FsDaemonStore
            .read_task(base, &lease.task_id)
            .expect("read daemon task after cleanup failure");
        assert_eq!(
            task.lease_id.as_deref(),
            Some(lease.lease_id.as_str()),
            "partial cleanup must preserve the task lease reference"
        );
        assert!(matches!(
            FsDaemonStore
                .release_writer_lock(base, &project_id, &lease.lease_id)
                .expect("writer lock state after partial cleanup"),
            WriterLockReleaseOutcome::AlreadyAbsent
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_task_surfaces_partial_cleanup_failure_after_completion() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");
        let project_id = ProjectId::new("daemon-complete-partial").expect("project id");
        let task = sample_pending_task("task-daemon-complete-partial", &project_id);
        FsDaemonStore.create_task(base, &task).expect("create task");

        let worktree = TestWorktreeAdapter::removing_as(WorktreeCleanupOutcome::AlreadyAbsent);
        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &FsDaemonStore,
            &worktree,
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            &agent_service,
        );

        let error = daemon
            .process_task(
                base,
                &task,
                &DaemonLoopConfig::default(),
                CancellationToken::new(),
            )
            .await
            .expect_err("completed-task cleanup should surface partial lease cleanup");
        assert!(
            matches!(error, AppError::LeaseCleanupPartialFailure { .. }),
            "unexpected completion cleanup error: {error:?}"
        );

        let lease_id = format!("lease-{}", task.task_id);
        let completed = FsDaemonStore
            .read_task(base, &task.task_id)
            .expect("read completed task");
        assert_eq!(completed.status, TaskStatus::Completed);
        assert_eq!(completed.lease_id.as_deref(), Some(lease_id.as_str()));
        assert!(FsDaemonStore.read_lease(base, &lease_id).is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_task_surfaces_partial_cleanup_failure_after_dispatch_failure() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");
        let project_id = create_standard_project(base, "daemon-fail-partial");
        let started_at = Utc::now();
        FsRunSnapshotWriteStore
            .write_run_snapshot(
                base,
                &project_id,
                &RunSnapshot {
                    active_run: Some(ActiveRun {
                        run_id: "run-daemon-fail-partial".to_owned(),
                        stage_cursor: StageCursor::initial(StageId::Planning),
                        started_at,
                        prompt_hash_at_cycle_start: "hash".to_owned(),
                        prompt_hash_at_stage_start: "hash".to_owned(),
                        qa_iterations_current_cycle: 0,
                        review_iterations_current_cycle: 0,
                        final_review_restart_count: 0,
                        stage_resolution_snapshot: None,
                    }),
                    interrupted_run: None,
                    status: RunStatus::Running,
                    cycle_history: Vec::new(),
                    completion_rounds: 1,
                    max_completion_rounds: Some(20),
                    rollback_point_meta: Default::default(),
                    amendment_queue: Default::default(),
                    status_summary: "running: planning".to_owned(),
                    last_stage_resolution_snapshot: None,
                },
            )
            .expect("write running snapshot");
        let task = sample_pending_task("task-daemon-fail-partial", &project_id);
        FsDaemonStore.create_task(base, &task).expect("create task");

        let worktree = TestWorktreeAdapter::removing_as(WorktreeCleanupOutcome::AlreadyAbsent);
        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &FsDaemonStore,
            &worktree,
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            &agent_service,
        );

        let error = daemon
            .process_task(
                base,
                &task,
                &DaemonLoopConfig::default(),
                CancellationToken::new(),
            )
            .await
            .expect_err("failed-task cleanup should surface partial lease cleanup");
        assert!(
            matches!(error, AppError::LeaseCleanupPartialFailure { .. }),
            "unexpected failed-task cleanup error: {error:?}"
        );

        let lease_id = format!("lease-{}", task.task_id);
        let failed = FsDaemonStore
            .read_task(base, &task.task_id)
            .expect("read failed task");
        assert_eq!(failed.status, TaskStatus::Failed);
        assert_eq!(failed.lease_id.as_deref(), Some(lease_id.as_str()));
        assert!(FsDaemonStore.read_lease(base, &lease_id).is_ok());
    }

    #[test]
    fn aborted_dispatch_fast_path_allows_successful_outcomes() {
        assert!(super::allow_aborted_dispatch_fast_path(Ok(())).is_ok());
        assert!(
            super::allow_aborted_dispatch_fast_path(Err(AppError::InvocationCancelled {
                backend: "daemon".to_owned(),
                contract_id: "task-aborted".to_owned(),
            }))
            .is_ok()
        );
    }

    #[test]
    fn aborted_dispatch_fast_path_propagates_non_cancellation_errors() {
        let error = super::allow_aborted_dispatch_fast_path(Err(AppError::Io(
            std::io::Error::other("backend cleanup failed"),
        )))
        .expect_err("non-cancellation outcome should not be swallowed");
        assert!(
            matches!(error, AppError::Io(_)),
            "unexpected propagated error: {error:?}"
        );
    }

    /// Regression test: dispatch_in_worktree_with_service must return Ok(())
    /// for RunStatus::Completed so that reconciliation-only retries fall
    /// through to try_reconcile_success. Before round 13, this returned
    /// TaskStateTransitionInvalid, creating a permanent fail/retry/fail loop.
    #[tokio::test(flavor = "multi_thread")]
    async fn dispatch_completed_run_returns_ok_for_reconciliation_retry() {
        use crate::contexts::agent_execution::model::CancellationToken;
        use crate::contexts::project_run_record::model::RunStatus;
        use crate::contexts::workspace_governance::initialize_workspace;

        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let effective_config =
            crate::contexts::workspace_governance::config::EffectiveConfig::load(base)
                .expect("load config");
        let project_id = ProjectId::new("test-project".to_owned()).expect("project id");

        let result = super::dispatch_in_worktree_with_service(
            &agent_service,
            &FsRunSnapshotStore,
            &FsRunSnapshotWriteStore,
            &FsJournalStore,
            &FsArtifactStore,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsAmendmentQueueStore,
            base,
            &project_id,
            FlowPreset::Standard,
            RunStatus::Completed,
            &effective_config,
            base,
            None,
            CancellationToken::new(),
        )
        .await;

        assert!(
            result.is_ok(),
            "RunStatus::Completed must return Ok(()) for reconciliation retries, got: {result:?}"
        );
    }
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
