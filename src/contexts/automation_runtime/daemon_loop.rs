use std::future::Future;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::adapters::fs::{
    FileSystem, FsMilestoneControllerStore, FsMilestoneJournalStore, FsMilestonePlanStore,
    FsMilestoneSnapshotStore, FsMilestoneStore, FsTaskRunLineageStore,
};
use crate::adapters::github::GithubPort;
use crate::cli::run::{
    cleanup_stale_backend_process_groups, interrupted_handoff_cleanup_candidate_with_dirs,
    repair_missing_interrupted_handoff_run_failed_event_and_reload_snapshot_with_dirs,
    InterruptedHandoffCleanupCandidate,
};
use crate::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use crate::contexts::agent_execution::policy::BackendPolicyService;
use crate::contexts::agent_execution::service::{AgentExecutionPort, RawOutputPort};
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::AgentExecutionService;
use crate::contexts::automation_runtime::cli_writer_lease::{
    cleanup_detached_project_writer_owner, read_project_writer_lock_owner,
    remove_owned_run_pid_file,
};
use crate::contexts::automation_runtime::lease_service::LeaseService;
use crate::contexts::automation_runtime::model::{
    CliWriterCleanupHandoff, DaemonTask, DispatchMode, LeaseRecord, RebaseFailureClassification,
    RebaseOutcome, TaskStatus,
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
use crate::contexts::milestone_record::controller as milestone_controller;
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

fn reconcile_success_adapter_id(project_id: &str, bead_id: &str, task_id: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"reconcile-success:");
    hasher.update(project_id.as_bytes());
    hasher.update(b":");
    hasher.update(bead_id.as_bytes());
    hasher.update(b":");
    hasher.update(task_id.as_bytes());
    format!("reconcile-success-{:x}", hasher.finalize())
}

fn aborted_dispatch_cleanup_error<T>(outcome: AppResult<T>) -> Option<AppError> {
    match outcome {
        Ok(_) | Err(AppError::InvocationCancelled { .. }) => None,
        Err(error) => Some(error),
    }
}

fn finish_aborted_dispatch_task_cleanup<T>(
    task_id: &str,
    outcome: AppResult<T>,
    lease_release_result: AppResult<()>,
) -> AppResult<()> {
    match (
        lease_release_result,
        aborted_dispatch_cleanup_error(outcome),
    ) {
        (Ok(()), None) => Ok(()),
        (Ok(()), Some(error)) => Err(error),
        (Err(error), None) => Err(error),
        (Err(release_error), Some(dispatch_error)) => {
            eprintln!(
                "daemon: aborted task '{}' hit dispatch cleanup error before lease cleanup settled: {}",
                task_id, dispatch_error
            );
            Err(release_error)
        }
    }
}

fn durable_project_attempt_identity(
    events: &[crate::contexts::project_run_record::model::JournalEvent],
) -> Option<engine::RunningAttemptIdentity> {
    use crate::contexts::project_run_record::model::JournalEventType;

    events.iter().rev().find_map(|event| {
        matches!(
            event.event_type,
            JournalEventType::RunStarted | JournalEventType::RunResumed
        )
        .then(|| {
            event
                .details
                .get("run_id")
                .and_then(serde_json::Value::as_str)
                .map(|run_id| engine::RunningAttemptIdentity {
                    run_id: run_id.to_owned(),
                    started_at: event.timestamp,
                })
        })
        .flatten()
    })
}

fn durable_lineage_attempt_identity(
    project_dir: &Path,
    milestone_id: &crate::contexts::milestone_record::model::MilestoneId,
    bead_id: &str,
    project_id: &ProjectId,
) -> AppResult<Option<engine::RunningAttemptIdentity>> {
    Ok(milestone_service::find_runs_for_bead(
        &FsTaskRunLineageStore,
        project_dir,
        milestone_id,
        bead_id,
    )?
    .into_iter()
    .filter(|entry| entry.project_id == project_id.as_str())
    .filter_map(|entry| {
        entry.run_id.map(|run_id| engine::RunningAttemptIdentity {
            run_id,
            started_at: entry.started_at,
        })
    })
    .max_by(|left, right| left.started_at.cmp(&right.started_at)))
}

fn lineage_completion_attempt_identity(
    project_dir: &Path,
    milestone_id: &crate::contexts::milestone_record::model::MilestoneId,
    bead_id: &str,
    project_id: &ProjectId,
    attempt: &engine::RunningAttemptIdentity,
) -> AppResult<Option<engine::RunningAttemptIdentity>> {
    let runs = milestone_service::find_runs_for_bead(
        &FsTaskRunLineageStore,
        project_dir,
        milestone_id,
        bead_id,
    )?;
    let matching_runs: Vec<_> = runs
        .into_iter()
        .filter(|entry| entry.project_id == project_id.as_str())
        .collect();
    let same_run_id = |entry: &crate::contexts::milestone_record::model::TaskRunEntry| {
        entry.run_id.as_deref() == Some(attempt.run_id.as_str())
    };

    let selected_entry = matching_runs
        .iter()
        .filter(|entry| same_run_id(entry) && !entry.outcome.is_terminal())
        .max_by(|left, right| left.started_at.cmp(&right.started_at))
        .or_else(|| {
            matching_runs
                .iter()
                .filter(|entry| same_run_id(entry) && entry.started_at == attempt.started_at)
                .max_by(|left, right| left.started_at.cmp(&right.started_at))
        });

    Ok(selected_entry.and_then(|entry| {
        entry
            .run_id
            .clone()
            .map(|run_id| engine::RunningAttemptIdentity {
                run_id,
                started_at: entry.started_at,
            })
    }))
}

enum MissingFailureLineageRepairGuard {
    Allow,
    BlockedByActiveAttempt,
    AmbiguousActiveAttempts,
}

fn missing_failure_lineage_repair_guard(
    matching_runs: &[crate::contexts::milestone_record::model::TaskRunEntry],
    project_id: &ProjectId,
    run_id: &str,
    started_at: chrono::DateTime<chrono::Utc>,
) -> MissingFailureLineageRepairGuard {
    let mut active_attempts = matching_runs.iter().filter(|entry| {
        !entry.outcome.is_terminal()
            && (entry.project_id != project_id.as_str() || entry.run_id.as_deref() != Some(run_id))
    });
    let Some(first_active_attempt) = active_attempts.next() else {
        return MissingFailureLineageRepairGuard::Allow;
    };
    if active_attempts.next().is_some() {
        return MissingFailureLineageRepairGuard::AmbiguousActiveAttempts;
    }

    if first_active_attempt.started_at > started_at
        || (first_active_attempt.started_at == started_at
            && (first_active_attempt.project_id != project_id.as_str()
                || first_active_attempt.run_id.as_deref() != Some(run_id)))
    {
        MissingFailureLineageRepairGuard::BlockedByActiveAttempt
    } else {
        MissingFailureLineageRepairGuard::Allow
    }
}

fn worktree_lease_record_is_missing(error: &AppError) -> bool {
    matches!(error, AppError::Io(io_error) if io_error.kind() == std::io::ErrorKind::NotFound)
        || matches!(error, AppError::CorruptRecord { details, .. } if details == "canonical file is missing")
}

enum PersistedCancelledHandoffPhase0State {
    NotApplicable,
    WaitingForLiveOwner,
    ReadyForLeaseCleanup,
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
            let tasks = match DaemonTaskService::list_tasks(self.store, &daemon_dir) {
                Ok(tasks) => tasks,
                Err(error) => {
                    eprintln!("daemon: list_tasks failed for {}: {error}", reg.repo_slug);
                    continue;
                }
            };

            let mut phase0_quarantined = false;
            for terminal_task in tasks
                .iter()
                .filter(|task| !task.label_dirty && task.lease_id.is_some() && task.is_terminal())
            {
                let handoff_state = match self.phase0_finalize_persisted_cancelled_handoff(
                    &daemon_dir,
                    checkout,
                    terminal_task,
                ) {
                    Ok(state) => state,
                    Err(error) => {
                        eprintln!(
                            "daemon: phase-0 handoff finalization failed for recovered terminal task '{}' in {}: {error} — quarantining repo for this cycle",
                            terminal_task.task_id, reg.repo_slug
                        );
                        phase0_quarantined = true;
                        break;
                    }
                };
                match handoff_state {
                    PersistedCancelledHandoffPhase0State::NotApplicable
                    | PersistedCancelledHandoffPhase0State::WaitingForLiveOwner => {}
                    PersistedCancelledHandoffPhase0State::ReadyForLeaseCleanup => {
                        if let Err(error) =
                            github_intake::sync_label_for_task(github, terminal_task).await
                        {
                            let _ = DaemonTaskService::mark_label_dirty(
                                self.store,
                                &daemon_dir,
                                &terminal_task.task_id,
                            );
                            eprintln!(
                                "daemon: phase-0 label sync failed for recovered terminal task '{}' in {}: {error}",
                                terminal_task.task_id, reg.repo_slug
                            );
                            phase0_quarantined = true;
                            break;
                        }
                        if let Err(error) = self.phase0_release_terminal_task_lease(
                            &daemon_dir,
                            checkout,
                            terminal_task,
                        ) {
                            let _ = DaemonTaskService::mark_label_dirty(
                                self.store,
                                &daemon_dir,
                                &terminal_task.task_id,
                            );
                            eprintln!(
                                "daemon: phase-0 cleanup failed for recovered terminal task '{}' in {}: {error}",
                                terminal_task.task_id, reg.repo_slug
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

            // Phase 0: Attempt to repair label_dirty tasks from prior cycles.
            // A GitHub label failure during repair quarantines this repo for the
            // rest of the cycle, consistent with multi-repo failure isolation.
            // Partial lease cleanup failures also quarantine the repo until a
            // later Phase 0 pass can repair the contradictory durable state.
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
            for dirty_task in tasks.iter().filter(|t| t.label_dirty) {
                match github_intake::sync_label_for_task(github, dirty_task).await {
                    Ok(()) => {
                        if dirty_task.is_terminal() {
                            // Terminal tasks: release deferred lease, then clear dirty.
                            match self.phase0_release_terminal_task_lease(
                                &daemon_dir,
                                checkout,
                                dirty_task,
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
                                    // retries cleanup on the next cycle and block
                                    // the repo until it succeeds.
                                    eprintln!(
                                        "daemon: deferred lease release failed for terminal task '{}' in {}: {e}",
                                        dirty_task.task_id, reg.repo_slug
                                    );
                                    phase0_quarantined = true;
                                    break;
                                }
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
                                    // Revert failed (typically lease cleanup
                                    // partial): keep label_dirty for the next
                                    // Phase 0 cycle and block this repo until
                                    // recovery succeeds.
                                    eprintln!(
                                        "daemon: revert failed for task '{}' in {}: {e}",
                                        dirty_task.task_id, reg.repo_slug
                                    );
                                    phase0_quarantined = true;
                                    break;
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
                    if let AppError::LeaseCleanupPartialFailure { task_id } = &error {
                        let _ =
                            DaemonTaskService::mark_label_dirty(self.store, &daemon_dir, task_id);
                        eprintln!(
                            "daemon: task {} hit partial lease cleanup failure in {} and remains quarantined until Phase 0 cleanup succeeds",
                            task_id, reg.repo_slug
                        );
                    }
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
            return finish_aborted_dispatch_task_cleanup(
                &active_task.task_id,
                outcome,
                self.release_task_lease(store_dir, repo_root, &active_task.task_id, &lease),
            );
        }

        match outcome {
            Ok(run_id) => {
                let reconciliation_task = self.task_for_success_reconciliation(
                    store_dir,
                    &active_task,
                    run_id.as_deref(),
                    is_post_completion_retry,
                )?;

                // Reconciliation-only retries (reconciliation_*) skip the PR
                // handler: the PR was already created/merged on the original
                // dispatch, and the retry only needs local bead close/sync/
                // milestone bookkeeping. Re-running the PR handler on a
                // reconciliation retry is unnecessary and can mutate or fail
                // PR state (e.g. close-or-skip when the worktree is not
                // ahead because no branch was resumed).
                //
                // `task` here is the pre-claim snapshot; post-claim
                // `active_task` has `failure_class` cleared, so this
                // shadowed binding must drive the retry gate.
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
                if let Err((failure_class, failure_message)) = self
                    .try_reconcile_success(repo_root, &reconciliation_task)
                    .await
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
                let failure_message = error.to_string();
                let (task_failure_class, task_failure_message) = match self
                    .try_reconcile_failure(repo_root, &active_task, &failure_message)
                    .await
                {
                    Ok(_) => (failure_class, failure_message),
                    Err((reconciliation_failure_class, reconciliation_failure_message)) => (
                        reconciliation_failure_class,
                        format!(
                            "{failure_message}; failure reconciliation also failed: {reconciliation_failure_message}"
                        ),
                    ),
                };
                let failed_task = DaemonTaskService::mark_failed(
                    self.store,
                    store_dir,
                    &active_task.task_id,
                    &task_failure_class,
                    &task_failure_message,
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
        self.phase0_finalize_persisted_cancelled_handoffs(base_dir, base_dir)?;

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
            match self
                .process_task(base_dir, task, config, shutdown.clone())
                .await
            {
                Ok(()) => {}
                Err(error @ AppError::LeaseCleanupPartialFailure { .. }) => {
                    println!("daemon: task {} failed: {}", task.task_id, error);
                    return Err(error);
                }
                Err(error) => {
                    println!("daemon: task {} failed: {}", task.task_id, error);
                    // Continue scanning remaining pending tasks
                }
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
            return finish_aborted_dispatch_task_cleanup(
                &active_task.task_id,
                outcome,
                self.release_task_lease(base_dir, repo_root, &active_task.task_id, &lease),
            );
        }

        match outcome {
            Ok(run_id) => {
                let reconciliation_task = self.task_for_success_reconciliation(
                    base_dir,
                    &active_task,
                    run_id.as_deref(),
                    is_post_completion_retry,
                )?;

                // Skip PR handler for reconciliation-only retries — see
                // multi-repo path comment for rationale.
                //
                // `task` here is the pre-claim snapshot; post-claim
                // `active_task` has `failure_class` cleared, so this
                // shadowed binding must drive the retry gate.
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
                if let Err((failure_class, failure_message)) = self
                    .try_reconcile_success(base_dir, &reconciliation_task)
                    .await
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
                let failure_message = error.to_string();
                let (task_failure_class, task_failure_message) = match self
                    .try_reconcile_failure(base_dir, &active_task, &failure_message)
                    .await
                {
                    Ok(_) => (failure_class, failure_message),
                    Err((reconciliation_failure_class, reconciliation_failure_message)) => (
                        reconciliation_failure_class,
                        format!(
                            "{failure_message}; failure reconciliation also failed: {reconciliation_failure_message}"
                        ),
                    ),
                };
                DaemonTaskService::mark_failed(
                    self.store,
                    base_dir,
                    &active_task.task_id,
                    &task_failure_class,
                    &task_failure_message,
                )?;
                self.try_push_failed_task_branch(repo_root, &lease);
                self.release_task_lease(base_dir, repo_root, &active_task.task_id, &lease)?;
                println!("failed task {}: {}", active_task.task_id, error);
            }
        }

        Ok(())
    }

    /// Best-effort: persist the authoritative workflow run_id captured from
    /// the dispatch path onto the task so reconciliation retries bind to the
    /// exact attempt the daemon just executed.
    fn persist_workflow_run_id(&self, store_dir: &Path, task: &DaemonTask, run_id: Option<&str>) {
        // Skip if already set (reconciliation retry of a task that
        // previously had its run_id persisted).
        if task.workflow_run_id.is_some() {
            return;
        }
        let Some(run_id) = run_id else {
            tracing::warn!(
                task_id = %task.task_id,
                "persist_workflow_run_id: no authoritative run_id available; leaving task unbound",
            );
            return;
        };
        if let Err(e) =
            DaemonTaskService::set_workflow_run_id(self.store, store_dir, &task.task_id, run_id)
        {
            eprintln!(
                "daemon: failed to persist workflow_run_id on task '{}' (non-blocking): {e}",
                task.task_id
            );
        }
    }

    fn task_for_success_reconciliation(
        &self,
        store_dir: &Path,
        task: &DaemonTask,
        run_id: Option<&str>,
        is_post_completion_retry: bool,
    ) -> AppResult<DaemonTask> {
        // Only trust a workflow binding once it has been durably written to
        // the task record. A transient run_id captured from dispatch/resume is
        // not enough: if the metadata write failed, success reconciliation
        // must fail closed instead of partially reconciling the wrong attempt.
        if !is_post_completion_retry {
            self.persist_workflow_run_id(store_dir, task, run_id);
        }
        self.store.read_task(store_dir, &task.task_id)
    }

    async fn try_reconcile_failure(
        &self,
        project_dir: &Path,
        task: &DaemonTask,
        fallback_error_summary: &str,
    ) -> Result<
        Option<crate::contexts::automation_runtime::FailureReconciliationOutcome>,
        (String, String),
    > {
        use crate::contexts::automation_runtime::failure_reconciliation::{
            reconcile_failure, FailureReconciliationError,
        };
        use crate::contexts::project_run_record::model::JournalEventType;

        let project_id = match ProjectId::new(&task.project_id) {
            Ok(id) => id,
            Err(_) => {
                return Err((
                    "reconciliation_metadata_error".to_owned(),
                    "invalid project_id during failure reconciliation".to_owned(),
                ));
            }
        };

        let project_record = match self
            .project_store
            .read_project_record(project_dir, &project_id)
        {
            Ok(record) => record,
            Err(error) => {
                return Err((
                    "reconciliation_metadata_error".to_owned(),
                    format!("could not read project record during failure reconciliation: {error}"),
                ));
            }
        };

        let Some(task_source) = project_record.task_source.as_ref() else {
            return Ok(None);
        };

        let milestone_id = match crate::contexts::milestone_record::model::MilestoneId::new(
            &task_source.milestone_id,
        ) {
            Ok(id) => id,
            Err(error) => {
                return Err(self.failure_reconciliation_metadata_failure(
                    project_dir,
                    task,
                    task_source,
                    "reconciliation_metadata_error".to_owned(),
                    format!(
                        "invalid milestone_id '{}' during failure reconciliation: {error}",
                        task_source.milestone_id
                    ),
                ));
            }
        };

        let journal_events = match self.journal_store.read_journal(project_dir, &project_id) {
            Ok(events) => events,
            Err(error) => {
                return Err(self.failure_reconciliation_metadata_failure(
                    project_dir,
                    task,
                    task_source,
                    "reconciliation_metadata_error".to_owned(),
                    format!("could not read journal during failure reconciliation: {error}"),
                ));
            }
        };

        let run_snapshot = self
            .run_snapshot_read
            .read_run_snapshot(project_dir, &project_id)
            .ok();
        let snapshot_attempt = run_snapshot.as_ref().and_then(|snapshot| {
            snapshot
                .interrupted_run
                .as_ref()
                .or(snapshot.active_run.as_ref())
        });
        let snapshot_attempt_identity =
            snapshot_attempt.map(engine::RunningAttemptIdentity::from_active_run);

        let lineage_started_at = |run_id: &str| {
            milestone_service::find_runs_for_bead(
                &FsTaskRunLineageStore,
                project_dir,
                &milestone_id,
                &task_source.bead_id,
            )
            .ok()
            .and_then(|entries| {
                entries
                    .iter()
                    .rev()
                    .find(|entry| {
                        entry.project_id == project_id.as_str()
                            && entry.run_id.as_deref() == Some(run_id)
                    })
                    .map(|entry| entry.started_at)
            })
        };
        let lineage_attempt_for_run = |run_id: &str| {
            milestone_service::find_runs_for_bead(
                &FsTaskRunLineageStore,
                project_dir,
                &milestone_id,
                &task_source.bead_id,
            )
            .ok()
            .and_then(|entries| {
                entries
                    .into_iter()
                    .filter(|entry| entry.project_id == project_id.as_str())
                    .filter_map(|entry| {
                        (entry.run_id.as_deref() == Some(run_id)).then_some(
                            engine::RunningAttemptIdentity {
                                run_id: run_id.to_owned(),
                                started_at: entry.started_at,
                            },
                        )
                    })
                    .max_by(|left, right| left.started_at.cmp(&right.started_at))
            })
        };
        let journal_attempt_for_run = |run_id: &str| {
            journal_events.iter().rev().find_map(|event| {
                (matches!(
                    event.event_type,
                    JournalEventType::RunStarted | JournalEventType::RunResumed
                ) && event
                    .details
                    .get("run_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(run_id))
                .then_some(engine::RunningAttemptIdentity {
                    run_id: run_id.to_owned(),
                    started_at: event.timestamp,
                })
            })
        };
        let completion_lineage_attempt = |attempt: &engine::RunningAttemptIdentity| -> Result<
            engine::RunningAttemptIdentity,
            (String, String),
        > {
            let attempt_identity = lineage_completion_attempt_identity(
                project_dir,
                &milestone_id,
                &task_source.bead_id,
                &project_id,
                attempt,
            )
            .map_err(|error| {
                self.failure_reconciliation_metadata_failure(
                    project_dir,
                    task,
                    task_source,
                    "reconciliation_metadata_error".to_owned(),
                    format!(
                        "could not read milestone task-run lineage during failure reconciliation: {error}"
                    ),
                )
            })?;
            if let Some(attempt_identity) = attempt_identity {
                return Ok(attempt_identity);
            }

            let matching_runs = milestone_service::find_runs_for_bead(
                &FsTaskRunLineageStore,
                project_dir,
                &milestone_id,
                &task_source.bead_id,
            )
            .map_err(|error| {
                self.failure_reconciliation_metadata_failure(
                    project_dir,
                    task,
                    task_source,
                    "reconciliation_metadata_error".to_owned(),
                    format!(
                        "could not read milestone task-run lineage during failure reconciliation: {error}"
                    ),
                )
            })?;
            if matching_runs.iter().any(|entry| {
                entry.project_id == project_id.as_str()
                    && entry.run_id.as_deref() == Some(attempt.run_id.as_str())
                    && entry.started_at == attempt.started_at
            }) {
                return Ok(attempt.clone());
            }

            match missing_failure_lineage_repair_guard(
                &matching_runs,
                &project_id,
                attempt.run_id.as_str(),
                attempt.started_at,
            ) {
                MissingFailureLineageRepairGuard::Allow => {}
                MissingFailureLineageRepairGuard::BlockedByActiveAttempt => {
                    return Err(self.failure_reconciliation_metadata_failure(
                        project_dir,
                        task,
                        task_source,
                        "reconciliation_metadata_error".to_owned(),
                        format!(
                            "could not repair missing milestone lineage for bead={} project={} run_id={} started_at={} because a newer active lineage attempt already exists",
                            task_source.bead_id,
                            project_id,
                            attempt.run_id,
                            attempt.started_at.to_rfc3339(),
                        ),
                    ));
                }
                MissingFailureLineageRepairGuard::AmbiguousActiveAttempts => {
                    return Err(self.failure_reconciliation_metadata_failure(
                        project_dir,
                        task,
                        task_source,
                        "reconciliation_metadata_error".to_owned(),
                        format!(
                            "could not repair missing milestone lineage for bead={} project={} run_id={} started_at={}: multiple active lineage rows exist; manual cleanup required",
                            task_source.bead_id,
                            project_id,
                            attempt.run_id,
                            attempt.started_at.to_rfc3339(),
                        ),
                    ));
                }
            }

            let plan_hash = engine::milestone_lineage_plan_hash(
                &project_record,
                project_dir,
                &project_id,
                &milestone_id,
                &task_source.bead_id,
                attempt.run_id.as_str(),
            )
            .map_err(|error| {
                self.failure_reconciliation_metadata_failure(
                    project_dir,
                    task,
                    task_source,
                    "reconciliation_metadata_error".to_owned(),
                    format!(
                        "could not derive milestone plan hash while repairing missing lineage during failure reconciliation: {error}"
                    ),
                )
            })?;
            milestone_service::record_bead_start(
                &FsMilestoneSnapshotStore,
                &FsMilestoneJournalStore,
                &FsTaskRunLineageStore,
                project_dir,
                &milestone_id,
                &task_source.bead_id,
                project_id.as_str(),
                attempt.run_id.as_str(),
                &plan_hash,
                attempt.started_at,
            )
            .map_err(|error| {
                self.failure_reconciliation_metadata_failure(
                    project_dir,
                    task,
                    task_source,
                    "reconciliation_metadata_error".to_owned(),
                    format!(
                        "could not repair missing milestone lineage during failure reconciliation: {error}"
                    ),
                )
            })?;

            Ok(attempt.clone())
        };
        let validate_failure_attempt =
            |attempt: &engine::RunningAttemptIdentity| -> Result<(), (String, String)> {
                if let Some(durable_project_attempt) =
                    durable_project_attempt_identity(&journal_events)
                {
                    if durable_project_attempt != *attempt {
                        return Err(self.failure_reconciliation_metadata_failure(
                            project_dir,
                            task,
                            task_source,
                            "reconciliation_metadata_error".to_owned(),
                            format!(
                                "durable workflow_run_id={} resolved attempt run_id={} started_at={} but the newest durable project journal attempt is run_id={} started_at={}; the daemon cannot safely determine which workflow attempt failed",
                                task.workflow_run_id.as_deref().unwrap_or_default(),
                                attempt.run_id,
                                attempt.started_at.to_rfc3339(),
                                durable_project_attempt.run_id,
                                durable_project_attempt.started_at.to_rfc3339(),
                            ),
                        ));
                    }
                }
                let expected_lineage_attempt = completion_lineage_attempt(attempt)?;
                if let Some(durable_lineage_attempt) = durable_lineage_attempt_identity(
                    project_dir,
                    &milestone_id,
                    &task_source.bead_id,
                    &project_id,
                )
                .map_err(|error| {
                    self.failure_reconciliation_metadata_failure(
                        project_dir,
                        task,
                        task_source,
                        "reconciliation_metadata_error".to_owned(),
                        format!(
                            "could not read milestone task-run lineage during failure reconciliation: {error}"
                        ),
                    )
                })? {
                    if durable_lineage_attempt != expected_lineage_attempt {
                        return Err(self.failure_reconciliation_metadata_failure(
                            project_dir,
                            task,
                            task_source,
                            "reconciliation_metadata_error".to_owned(),
                            format!(
                                "durable workflow_run_id={} resolved attempt run_id={} started_at={} maps to milestone lineage run_id={} started_at={} but the newest durable milestone lineage attempt is run_id={} started_at={}; the daemon cannot safely determine which workflow attempt failed",
                                task.workflow_run_id.as_deref().unwrap_or_default(),
                                attempt.run_id,
                                attempt.started_at.to_rfc3339(),
                                expected_lineage_attempt.run_id,
                                expected_lineage_attempt.started_at.to_rfc3339(),
                                durable_lineage_attempt.run_id,
                                durable_lineage_attempt.started_at.to_rfc3339(),
                            ),
                        ));
                    }
                }
                if let Some(snapshot_attempt) = snapshot_attempt_identity.as_ref() {
                    if snapshot_attempt != attempt {
                        return Err(self.failure_reconciliation_metadata_failure(
                            project_dir,
                            task,
                            task_source,
                            "reconciliation_metadata_error".to_owned(),
                            format!(
                                "durable workflow_run_id={} resolved attempt run_id={} started_at={} but the run snapshot shows run_id={} started_at={}; the daemon cannot safely determine which workflow attempt failed",
                                task.workflow_run_id.as_deref().unwrap_or_default(),
                                attempt.run_id,
                                attempt.started_at.to_rfc3339(),
                                snapshot_attempt.run_id,
                                snapshot_attempt.started_at.to_rfc3339(),
                            ),
                        ));
                    }
                }
                Ok(())
            };

        let (run_id, started_at) = match task.workflow_run_id.as_deref() {
            Some(bound_run_id) => {
                let attempt = snapshot_attempt_identity
                    .as_ref()
                    .filter(|attempt| attempt.run_id == bound_run_id)
                    .cloned()
                    .into_iter()
                    .chain(journal_attempt_for_run(bound_run_id))
                    .chain(lineage_attempt_for_run(bound_run_id))
                    .max_by(|left, right| left.started_at.cmp(&right.started_at))
                    .ok_or_else(|| {
                        self.failure_reconciliation_metadata_failure(
                            project_dir,
                            task,
                            task_source,
                            "reconciliation_metadata_error".to_owned(),
                            format!(
                                "could not determine started_at for durable workflow_run_id={bound_run_id} during milestone reconciliation"
                            ),
                        )
                    })?;
                validate_failure_attempt(&attempt)?;
                (attempt.run_id, attempt.started_at)
            }
            None => {
                let durable_project_attempt = durable_project_attempt_identity(&journal_events);
                let durable_lineage_attempt = durable_lineage_attempt_identity(
                    project_dir,
                    &milestone_id,
                    &task_source.bead_id,
                    &project_id,
                )
                .map_err(|error| {
                    self.failure_reconciliation_metadata_failure(
                        project_dir,
                        task,
                        task_source,
                        "reconciliation_metadata_error".to_owned(),
                        format!(
                            "could not read milestone task-run lineage during failure reconciliation: {error}"
                        ),
                    )
                })?;
                let attempt = if let Some(snapshot_attempt) = snapshot_attempt {
                    let snapshot_attempt =
                        engine::RunningAttemptIdentity::from_active_run(snapshot_attempt);
                    if let Some(durable_project_attempt) = durable_project_attempt.as_ref() {
                        if durable_project_attempt != &snapshot_attempt {
                            return Err(self.failure_reconciliation_metadata_failure(
                                project_dir,
                                task,
                                task_source,
                                "reconciliation_no_run_binding".to_owned(),
                                format!(
                                    "workflow_run_id is missing and snapshot attempt run_id={} started_at={} does not match the newest durable project journal attempt run_id={} started_at={}; the daemon cannot safely determine which workflow attempt failed",
                                    snapshot_attempt.run_id,
                                    snapshot_attempt.started_at.to_rfc3339(),
                                    durable_project_attempt.run_id,
                                    durable_project_attempt.started_at.to_rfc3339(),
                                ),
                            ));
                        }
                    }
                    if let Some(durable_lineage_attempt) = durable_lineage_attempt.as_ref() {
                        let expected_lineage_attempt =
                            completion_lineage_attempt(&snapshot_attempt)?;
                        if durable_lineage_attempt != &expected_lineage_attempt {
                            return Err(self.failure_reconciliation_metadata_failure(
                                project_dir,
                                task,
                                task_source,
                                "reconciliation_no_run_binding".to_owned(),
                                format!(
                                    "workflow_run_id is missing and snapshot attempt run_id={} started_at={} maps to milestone lineage run_id={} started_at={} but the newest durable milestone lineage attempt is run_id={} started_at={}; the daemon cannot safely determine which workflow attempt failed",
                                    snapshot_attempt.run_id,
                                    snapshot_attempt.started_at.to_rfc3339(),
                                    expected_lineage_attempt.run_id,
                                    expected_lineage_attempt.started_at.to_rfc3339(),
                                    durable_lineage_attempt.run_id,
                                    durable_lineage_attempt.started_at.to_rfc3339(),
                                ),
                            ));
                        }
                    }
                    tracing::warn!(
                        project_id = project_id.as_str(),
                        bead_id = task_source.bead_id.as_str(),
                        task_id = task.task_id.as_str(),
                        run_id = snapshot_attempt.run_id.as_str(),
                        "failure reconciliation is using snapshot attempt metadata because workflow_run_id is missing"
                    );
                    snapshot_attempt
                } else {
                    match (
                        durable_project_attempt.as_ref(),
                        durable_lineage_attempt.as_ref(),
                    ) {
                        (Some(project_attempt), Some(lineage_attempt)) => {
                            let expected_lineage_attempt =
                                completion_lineage_attempt(project_attempt)?;
                            if &expected_lineage_attempt != lineage_attempt {
                                return Err(self.failure_reconciliation_metadata_failure(
                                    project_dir,
                                    task,
                                    task_source,
                                    "reconciliation_no_run_binding".to_owned(),
                                    format!(
                                        "workflow_run_id is missing, run snapshot has no exact active/interrupted attempt, and the durable project journal attempt run_id={} started_at={} maps to milestone lineage run_id={} started_at={} but the durable milestone lineage attempt is run_id={} started_at={}; the daemon cannot safely determine which workflow attempt failed",
                                        project_attempt.run_id,
                                        project_attempt.started_at.to_rfc3339(),
                                        expected_lineage_attempt.run_id,
                                        expected_lineage_attempt.started_at.to_rfc3339(),
                                        lineage_attempt.run_id,
                                        lineage_attempt.started_at.to_rfc3339(),
                                    ),
                                ));
                            }
                            tracing::warn!(
                                project_id = project_id.as_str(),
                                bead_id = task_source.bead_id.as_str(),
                                task_id = task.task_id.as_str(),
                                run_id = project_attempt.run_id.as_str(),
                                "failure reconciliation is using durable project journal and milestone lineage attempt metadata because workflow_run_id and snapshot attempt metadata are missing"
                            );
                            project_attempt.clone()
                        }
                        _ => {
                            return Err(self.failure_reconciliation_metadata_failure(
                                project_dir,
                                task,
                                task_source,
                                "reconciliation_no_run_binding".to_owned(),
                                "workflow_run_id is missing and run snapshot has no exact active/interrupted attempt; the daemon cannot safely determine which workflow attempt failed".to_owned(),
                            ));
                        }
                    }
                };
                (attempt.run_id, attempt.started_at)
            }
        };
        let completion_started_at = completion_lineage_attempt(&engine::RunningAttemptIdentity {
            run_id: run_id.clone(),
            started_at,
        })?
        .started_at;

        let run_failed = queries::terminal_event_for_attempt(
            run_id.as_str(),
            started_at,
            RunStatus::Failed,
            &journal_events,
        )
        .ok_or_else(|| {
            self.failure_reconciliation_metadata_failure(
                project_dir,
                task,
                task_source,
                "reconciliation_metadata_error".to_owned(),
                format!(
                    "could not find a durable run_failed event for the current workflow attempt run_id={} started_at={} during failure reconciliation",
                    run_id,
                    started_at.to_rfc3339(),
                ),
            )
        })?;
        let run_started = journal_events.iter().rev().find(|event| {
            event.event_type == JournalEventType::RunStarted
                && event
                    .details
                    .get("run_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(run_id.as_str())
        });
        let failed_at = run_failed.timestamp;
        let error_summary = run_failed
            .details
            .get("message")
            .and_then(serde_json::Value::as_str)
            .or(task.failure_message.as_deref())
            .unwrap_or(fallback_error_summary);
        if run_started.is_none()
            && lineage_started_at(run_id.as_str()).is_none()
            && snapshot_attempt
                .filter(|attempt| attempt.run_id == run_id)
                .is_none()
        {
            tracing::warn!(
                project_id = project_id.as_str(),
                bead_id = task_source.bead_id.as_str(),
                task_id = task.task_id.as_str(),
                run_id = run_id.as_str(),
                "failure reconciliation derived started_at without a durable run_started event"
            );
        }

        reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            project_dir,
            &task_source.bead_id,
            &task.task_id,
            project_id.as_str(),
            &task_source.milestone_id,
            &run_id,
            task_source.plan_hash.as_deref(),
            completion_started_at,
            failed_at,
            error_summary,
        )
        .await
        .map(Some)
        .map_err(|error| match error {
            FailureReconciliationError::MilestoneUpdateFailed { .. } => (
                "reconciliation_milestone_update_failed".to_owned(),
                error.to_string(),
            ),
        })
    }

    fn failure_reconciliation_metadata_failure(
        &self,
        project_dir: &Path,
        task: &DaemonTask,
        task_source: &crate::contexts::project_run_record::model::TaskSource,
        failure_class: String,
        details: String,
    ) -> (String, String) {
        let reason = format!(
            "task '{}' cannot reconcile failure because {details}. Operator intervention required before retrying reconciliation",
            task.task_id
        );

        let reason = match crate::contexts::milestone_record::model::MilestoneId::new(
            &task_source.milestone_id,
        ) {
            Ok(milestone_id) => {
                let transition_at = milestone_controller::load_controller(
                    &FsMilestoneControllerStore,
                    project_dir,
                    &milestone_id,
                )
                .ok()
                .flatten()
                .map(|controller| {
                    std::cmp::max(
                        controller.updated_at,
                        controller.last_transition_at + chrono::Duration::microseconds(1),
                    )
                })
                .unwrap_or_else(Utc::now);
                let request = milestone_controller::ControllerTransitionRequest::new(
                    milestone_controller::MilestoneControllerState::NeedsOperator,
                    reason.clone(),
                )
                .with_bead(&task_source.bead_id)
                .with_task(&task.project_id);
                match milestone_controller::sync_controller_state(
                    &FsMilestoneControllerStore,
                    project_dir,
                    &milestone_id,
                    request,
                    transition_at,
                ) {
                    Ok(_) => reason,
                    Err(error) => format!(
                        "{reason}; additionally failed to persist needs_operator controller state: {error}"
                    ),
                }
            }
            Err(error) => format!(
                "{reason}; additionally could not parse milestone_id '{}': {error}",
                task_source.milestone_id
            ),
        };

        (failure_class, reason)
    }

    fn missing_workflow_run_id_failure(
        &self,
        project_dir: &Path,
        task: &DaemonTask,
        task_source: &crate::contexts::project_run_record::model::TaskSource,
    ) -> (String, String) {
        let reason = format!(
            "task '{}' cannot reconcile success because workflow_run_id is missing; \
             persist_workflow_run_id did not record a durable task-to-run binding, so \
             the daemon cannot safely determine which workflow attempt to reconcile. \
             Operator intervention required before retrying reconciliation",
            task.task_id
        );

        let reason = match crate::contexts::milestone_record::model::MilestoneId::new(
            &task_source.milestone_id,
        ) {
            Ok(milestone_id) => {
                let request = milestone_controller::ControllerTransitionRequest::new(
                    milestone_controller::MilestoneControllerState::NeedsOperator,
                    reason.clone(),
                )
                .with_bead(&task_source.bead_id)
                .with_task(&task.task_id);
                match milestone_controller::sync_controller_state(
                    &FsMilestoneControllerStore,
                    project_dir,
                    &milestone_id,
                    request,
                    Utc::now(),
                ) {
                    Ok(_) => reason,
                    Err(error) => format!(
                        "{reason}; additionally failed to persist needs_operator controller state: {error}"
                    ),
                }
            }
            Err(error) => format!(
                "{reason}; additionally could not parse milestone_id '{}': {error}",
                task_source.milestone_id
            ),
        };

        ("reconciliation_no_run_binding".to_owned(), reason)
    }

    /// Attempt success reconciliation for a completed milestone task.
    ///
    /// Closes the bead in `br`, syncs, updates milestone state, and captures
    /// next-step hints. If the daemon cannot prove which workflow run belongs
    /// to this task, it fails closed, moves the milestone controller to
    /// needs-operator, and returns an error so the task is marked Failed.
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

        // NOTE: We intentionally do NOT transition the milestone snapshot to
        // Failed on reconciliation errors. MilestoneStatus::Failed is terminal
        // — once set, record_bead_start rejects all future bead starts,
        // permanently wedging the milestone with no automated recovery path.
        // Instead, we leave the milestone snapshot Running and return Err so
        // the *task* is marked Failed. The operator can then retry_task, which
        // re-dispatches reconciliation against the still-Running milestone.
        // For safety-critical metadata failures such as a missing durable
        // workflow_run_id binding, we additionally move the milestone
        // controller to needs-operator so the ambiguity is visible.

        // Extract run_id and started_at. Reconciliation must only use the
        // durable workflow_run_id persisted on the task record. Guessing from
        // the latest RunStarted journal event remains unsafe because a newer
        // manual re-run may have appended its own run identity, and a
        // transient in-memory run_id is not durable enough to prove the
        // binding survived persistence.
        let run_id_from_task = match task.workflow_run_id.as_deref() {
            Some(run_id) => run_id,
            None => {
                return Err(self.missing_workflow_run_id_failure(project_dir, task, task_source))
            }
        };

        let journal_events = match self.journal_store.read_journal(project_dir, &project_id) {
            Ok(events) => events,
            Err(e) => {
                return Err((
                    "reconciliation_metadata_error".to_owned(),
                    format!("could not read journal: {e}"),
                ));
            }
        };

        let run_started = journal_events.iter().rev().find(|event| {
            if event.event_type != JournalEventType::RunStarted {
                return false;
            }
            event
                .details
                .get("run_id")
                .and_then(serde_json::Value::as_str)
                == Some(run_id_from_task)
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
        let br_mutation = BrMutationAdapter::with_adapter_id(
            BrAdapter::<OsProcessRunner>::new().with_working_dir(project_dir.to_path_buf()),
            reconcile_success_adapter_id(project_id.as_str(), &task_source.bead_id, &task.task_id),
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
        let enable_review = self.requirements_quick_enable_review(task).map_err(|e| {
            let _ = DaemonTaskService::mark_failed(
                self.store,
                base_dir,
                &task.task_id,
                "requirements_quick_failed",
                &format!("failed to resolve requirements_quick review mode: {e}"),
            );
            e
        })?;

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
            .quick(workspace_dir, &idea, Utc::now(), None, enable_review)
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

    fn requirements_quick_enable_review(&self, task: &DaemonTask) -> AppResult<bool> {
        if let Some(command) = task.routing_command.as_deref() {
            if let Some(parsed) = watcher::parse_requirements_command_details(command)? {
                return Ok(parsed.enable_review);
            }
        }

        if let Some(prompt) = task.prompt.as_deref() {
            if let Some(parsed) = watcher::parse_requirements_command_details(prompt)? {
                return Ok(parsed.enable_review);
            }
        }

        Ok(false)
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
    ) -> AppResult<Option<String>> {
        let project_id = ProjectId::new(task.project_id.clone())?;
        let flow = task
            .resolved_flow
            .ok_or_else(|| AppError::RoutingResolutionFailed {
                input: task.task_id.clone(),
                details: "task has no resolved flow".to_owned(),
            })?;

        let run_snapshot = self.load_dispatch_run_snapshot(base_dir, workspace_dir, &project_id)?;
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
    ) -> AppResult<Option<String>> {
        let project_id = ProjectId::new(task.project_id.clone())?;
        let flow = task
            .resolved_flow
            .ok_or_else(|| AppError::RoutingResolutionFailed {
                input: task.task_id.clone(),
                details: "task has no resolved flow".to_owned(),
            })?;

        let run_snapshot = self.load_dispatch_run_snapshot(base_dir, workspace_dir, &project_id)?;
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
    ) -> AppResult<Option<String>> {
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
        expected_attempt: Option<&engine::RunningAttemptIdentity>,
        summary: &'static str,
        log_message: &'static str,
    ) -> AppResult<bool> {
        let interrupted_context = engine::InterruptedRunContext {
            run_snapshot_read: self.run_snapshot_read,
            run_snapshot_write: self.run_snapshot_write,
            journal_store: self.journal_store,
            log_write: self.log_write,
            base_dir: workspace_dir,
            project_id,
        };

        match expected_attempt {
            Some(expected_attempt) => engine::mark_running_run_interrupted(
                interrupted_context,
                expected_attempt,
                engine::InterruptedRunUpdate {
                    summary,
                    log_message,
                    failure_class: Some("cancellation"),
                },
            ),
            None => engine::mark_current_process_running_run_interrupted(
                interrupted_context,
                Some(writer_owner),
                engine::InterruptedRunUpdate {
                    summary,
                    log_message,
                    failure_class: Some("cancellation"),
                },
            ),
        }
    }

    fn load_dispatch_run_snapshot(
        &self,
        daemon_store_dir: &Path,
        workspace_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<RunSnapshot> {
        let mut snapshot = self
            .run_snapshot_read
            .read_run_snapshot(workspace_dir, project_id)?;
        let _ = repair_missing_interrupted_handoff_run_failed_event_and_reload_snapshot_with_dirs(
            daemon_store_dir,
            workspace_dir,
            project_id,
            &mut snapshot,
        )?;
        Ok(snapshot)
    }

    fn prepare_cancelled_dispatch_handoff(
        &self,
        daemon_store_dir: &Path,
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
        self.persist_daemon_cleanup_handoff(
            daemon_store_dir,
            project_id,
            writer_owner,
            &expected_attempt,
        )?;

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

    fn persist_daemon_cleanup_handoff(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        writer_owner: &str,
        expected_attempt: &engine::RunningAttemptIdentity,
    ) -> AppResult<()> {
        let record = self.store.read_lease_record(base_dir, writer_owner)?;
        let LeaseRecord::Worktree(mut lease) = record else {
            return Err(AppError::CorruptRecord {
                file: "lease".to_owned(),
                details: format!(
                    "expected worktree lease '{writer_owner}' while persisting daemon cleanup handoff"
                ),
            });
        };
        if lease.project_id != project_id.as_str() {
            return Err(AppError::CorruptRecord {
                file: "lease".to_owned(),
                details: format!(
                    "worktree lease '{writer_owner}' belongs to '{}' instead of '{}'",
                    lease.project_id,
                    project_id.as_str()
                ),
            });
        }

        let pid = std::process::id();
        lease.cleanup_handoff = Some(CliWriterCleanupHandoff {
            pid,
            recorded_at: Some(Utc::now()),
            run_id: Some(expected_attempt.run_id.clone()),
            run_started_at: Some(expected_attempt.started_at),
            proc_start_ticks: FileSystem::proc_start_ticks_for_pid(pid),
            proc_start_marker: FileSystem::proc_start_marker_for_pid(pid),
        });
        self.store
            .write_lease_record(base_dir, &LeaseRecord::Worktree(lease))
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
    ) -> AppResult<Option<String>>
    where
        F: Future<Output = AppResult<Option<String>>>,
    {
        let interrupted_handoff = self.prepare_cancelled_dispatch_handoff(
            base_dir,
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
                            // Preserve the real dispatch error if one arrived
                            // during the grace period; only synthesize
                            // InvocationCancelled when the dispatch itself
                            // succeeded or was already a cancellation.
                            match result {
                                Err(ref e)
                                    if !matches!(e, AppError::InvocationCancelled { .. }) =>
                                {
                                    result
                                }
                                _ => Err(AppError::InvocationCancelled {
                                    backend: "daemon".to_owned(),
                                    contract_id: task.task_id.clone(),
                                }),
                            }
                        }
                        Ok(false) => result,
                        Err(error) => Err(error),
                    }
                } else {
                    match self.reconcile_cancelled_dispatch_run(
                        workspace_dir,
                        project_id,
                        &lease.lease_id,
                        interrupted_handoff.expected_attempt.as_ref(),
                        summary,
                        log_message,
                    ) {
                        Ok(true) => {
                            self.cleanup_cancelled_dispatch_backend_processes(
                                workspace_dir,
                                project_id,
                                interrupted_handoff.expected_attempt.as_ref(),
                            )?;
                            match result {
                                Err(ref e)
                                    if !matches!(e, AppError::InvocationCancelled { .. }) =>
                                {
                                    result
                                }
                                _ => Err(AppError::InvocationCancelled {
                                    backend: "daemon".to_owned(),
                                    contract_id: task.task_id.clone(),
                                }),
                            }
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
                        interrupted_handoff.expected_attempt.as_ref(),
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
                if matches!(e, AppError::LeaseCleanupPartialFailure { .. }) {
                    let _ =
                        DaemonTaskService::mark_label_dirty(self.store, store_dir, &lease.task_id);
                }
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

    fn phase0_release_terminal_task_lease(
        &self,
        base_dir: &Path,
        repo_root: &Path,
        task: &DaemonTask,
    ) -> AppResult<()> {
        if let Some(ref lease_id) = task.lease_id {
            let project_id = ProjectId::new(task.project_id.clone())?;
            if read_project_writer_lock_owner(base_dir, &project_id)?.as_deref()
                != Some(lease_id.as_str())
            {
                remove_owned_run_pid_file(
                    base_dir,
                    repo_root,
                    &task.project_id,
                    Some(lease_id.as_str()),
                    &task.task_id,
                )?;
                if cleanup_detached_project_writer_owner(
                    self.store,
                    self.worktree,
                    base_dir,
                    repo_root,
                    &project_id,
                    lease_id,
                )? {
                    return Ok(());
                }
            }
            match self.store.read_lease(base_dir, lease_id) {
                Ok(lease) => {
                    if task.status == TaskStatus::Failed {
                        self.try_push_failed_task_branch(repo_root, &lease);
                    }
                    self.release_task_lease(base_dir, repo_root, &task.task_id, &lease)
                }
                Err(error) if worktree_lease_record_is_missing(&error) => {
                    self.clear_orphaned_task_lease_reference(base_dir, repo_root, task)
                }
                Err(error) => Err(error),
            }
        } else {
            Ok(())
        }
    }

    fn phase0_finalize_persisted_cancelled_handoff(
        &self,
        daemon_store_dir: &Path,
        workspace_dir: &Path,
        task: &DaemonTask,
    ) -> AppResult<PersistedCancelledHandoffPhase0State> {
        if !matches!(task.status, TaskStatus::Aborted | TaskStatus::Failed) {
            return Ok(PersistedCancelledHandoffPhase0State::NotApplicable);
        }
        let Some(lease_id) = task.lease_id.as_deref() else {
            return Ok(PersistedCancelledHandoffPhase0State::NotApplicable);
        };
        let lease_record = match self.store.read_lease_record(daemon_store_dir, lease_id) {
            Ok(record) => record,
            Err(error) if worktree_lease_record_is_missing(&error) => {
                return Ok(PersistedCancelledHandoffPhase0State::NotApplicable);
            }
            Err(error) => return Err(error),
        };
        let LeaseRecord::Worktree(lease) = lease_record else {
            return Err(AppError::CorruptRecord {
                file: "lease".to_owned(),
                details: format!(
                    "expected worktree lease '{lease_id}' while finalizing daemon cleanup handoff"
                ),
            });
        };
        if lease.project_id != task.project_id || lease.cleanup_handoff.is_none() {
            return Ok(PersistedCancelledHandoffPhase0State::NotApplicable);
        }

        let project_id = ProjectId::new(task.project_id.clone())?;
        let mut snapshot = self
            .run_snapshot_read
            .read_run_snapshot(workspace_dir, &project_id)?;
        match interrupted_handoff_cleanup_candidate_with_dirs(
            daemon_store_dir,
            workspace_dir,
            &project_id,
            &snapshot,
        )? {
            InterruptedHandoffCleanupCandidate::None => {
                Ok(PersistedCancelledHandoffPhase0State::NotApplicable)
            }
            InterruptedHandoffCleanupCandidate::WaitingForLiveOwner => {
                Ok(PersistedCancelledHandoffPhase0State::WaitingForLiveOwner)
            }
            InterruptedHandoffCleanupCandidate::Ready { .. } => {
                let _ =
                    repair_missing_interrupted_handoff_run_failed_event_and_reload_snapshot_with_dirs(
                        daemon_store_dir,
                        workspace_dir,
                        &project_id,
                        &mut snapshot,
                    )?;
                Ok(PersistedCancelledHandoffPhase0State::ReadyForLeaseCleanup)
            }
        }
    }

    fn phase0_finalize_persisted_cancelled_handoffs(
        &self,
        daemon_store_dir: &Path,
        workspace_dir: &Path,
    ) -> AppResult<()> {
        for task in DaemonTaskService::list_tasks(self.store, daemon_store_dir)?
            .into_iter()
            .filter(|task| !task.label_dirty && task.lease_id.is_some() && task.is_terminal())
        {
            if matches!(
                self.phase0_finalize_persisted_cancelled_handoff(
                    daemon_store_dir,
                    workspace_dir,
                    &task,
                )?,
                PersistedCancelledHandoffPhase0State::ReadyForLeaseCleanup
            ) {
                self.phase0_release_terminal_task_lease(daemon_store_dir, workspace_dir, &task)?;
            }
        }
        Ok(())
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
                remove_owned_run_pid_file(
                    base_dir,
                    repo_root,
                    &lease.project_id,
                    Some(lease.lease_id.as_str()),
                    task_id,
                )?;
                // All sub-steps succeeded — safe to clear durable lease reference.
                DaemonTaskService::clear_lease_reference(self.store, base_dir, task_id).map_err(
                    |_| AppError::LeaseCleanupPartialFailure {
                        task_id: task_id.to_owned(),
                    },
                )?;
                Ok(())
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

    fn clear_orphaned_task_lease_reference(
        &self,
        base_dir: &Path,
        repo_root: &Path,
        task: &DaemonTask,
    ) -> AppResult<()> {
        remove_owned_run_pid_file(
            base_dir,
            repo_root,
            &task.project_id,
            task.lease_id.as_deref(),
            &task.task_id,
        )?;
        DaemonTaskService::clear_lease_reference(self.store, base_dir, &task.task_id).map_err(
            |_| AppError::LeaseCleanupPartialFailure {
                task_id: task.task_id.clone(),
            },
        )?;
        Ok(())
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
) -> AppResult<Option<String>>
where
    A: AgentExecutionPort + Sync,
    R: RawOutputPort + Sync,
    S: SessionStorePort + Sync,
{
    match run_status {
        RunStatus::NotStarted => engine::execute_run_with_retry_and_capture_run_id(
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
            &RetryPolicy::default_policy()
                .with_max_remediation_cycles(effective_config.run_policy().max_review_iterations),
            cancellation_token,
        )
        .await
        .map(Some),
        RunStatus::Failed | RunStatus::Paused => engine::resume_run_with_retry_and_capture_run_id(
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
            &RetryPolicy::default_policy()
                .with_max_remediation_cycles(effective_config.run_policy().max_review_iterations),
            cancellation_token,
        )
        .await
        .map(Some),
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
            // Returning Ok(None) lets the caller fall through to the success
            // path where try_reconcile_success runs again. All reconciliation
            // steps are idempotent: br close checks bead status, sync is
            // unconditional, and record_bead_completion handles replays.
            Ok(None)
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
    let agent_service = AgentExecutionService::new(adapter, raw_output_store, session_store)
        .with_effective_config(effective_config.clone());
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
        DAEMON_SHUTDOWN_LOG_MESSAGE, DAEMON_SHUTDOWN_STATUS_SUMMARY,
    };
    use crate::adapters::fs::{
        FileSystem, FsAmendmentQueueStore, FsArtifactStore, FsDaemonStore, FsJournalStore,
        FsMilestoneControllerStore, FsMilestoneJournalStore, FsMilestonePlanStore,
        FsMilestoneSnapshotStore, FsMilestoneStore, FsPayloadArtifactWriteStore, FsProjectStore,
        FsRawOutputStore, FsRequirementsStore, FsRunSnapshotStore, FsRunSnapshotWriteStore,
        FsRuntimeLogWriteStore, FsSessionStore, FsTaskRunLineageStore, RunBackendProcessRecord,
        RunPidOwner, RunPidRecord,
    };
    use crate::adapters::github::InMemoryGithubClient;
    use crate::adapters::stub_backend::StubBackendAdapter;
    use crate::adapters::worktree::WorktreeAdapter;
    use crate::contexts::agent_execution::model::CancellationToken;
    use crate::contexts::agent_execution::AgentExecutionService;
    use crate::contexts::automation_runtime::model::{
        CliWriterCleanupHandoff, CliWriterLease, DaemonJournalEvent, DaemonTask, DispatchMode,
        TaskStatus,
    };
    use crate::contexts::automation_runtime::repo_registry::{DataDirLayout, RepoRegistration};
    use crate::contexts::automation_runtime::{
        DaemonStorePort, LeaseRecord, ResourceCleanupOutcome, WorktreeCleanupOutcome,
        WorktreeLease, WorktreePort, WriterLockReleaseOutcome,
    };
    use crate::contexts::milestone_record::bundle::{
        AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
    };
    use crate::contexts::milestone_record::controller::{self as milestone_controller};
    use crate::contexts::milestone_record::model::{MilestoneEventType, TaskRunOutcome};
    use crate::contexts::milestone_record::service::{
        self as milestone_service, create_milestone, persist_plan, read_journal, record_bead_start,
        update_status, CreateMilestoneInput, MilestoneSnapshotPort,
    };
    use crate::contexts::project_run_record::journal;
    use crate::contexts::project_run_record::model::{
        ActiveRun, RunSnapshot, RunStatus, TaskOrigin, TaskSource,
    };
    use crate::contexts::project_run_record::service::{
        create_project, CreateProjectInput, JournalStorePort, RunSnapshotPort, RunSnapshotWritePort,
    };
    use crate::contexts::requirements_drafting::model::{
        RequirementsJournalEventType, RequirementsStatus,
    };
    use crate::contexts::requirements_drafting::service::{
        RequirementsService, RequirementsStorePort,
    };
    use crate::contexts::workspace_governance::initialize_workspace;
    use crate::shared::domain::FlowPreset;
    use crate::shared::domain::{ProjectId, RunId, StageCursor, StageId};
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
            routing_labels: vec!["fixture".to_owned()],
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
            routing_labels: vec!["fixture".to_owned()],
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

    fn create_failure_reconciliation_milestone(
        base_dir: &std::path::Path,
        now: chrono::DateTime<Utc>,
    ) -> crate::contexts::milestone_record::model::MilestoneId {
        let record = create_milestone(
            &FsMilestoneStore,
            base_dir,
            CreateMilestoneInput {
                id: "ms-daemon-failure-reconcile".to_owned(),
                name: "Daemon failure reconcile".to_owned(),
                description: "Runtime failure reconciliation coverage".to_owned(),
            },
            now,
        )
        .expect("create milestone");
        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-daemon-failure-reconcile".to_owned(),
                name: "Daemon failure reconcile".to_owned(),
            },
            executive_summary: "One bead for daemon-side failure reconciliation".to_owned(),
            goals: vec!["Record failed milestone attempts".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "The bead is present in the plan".to_owned(),
                covered_by: vec!["ms-daemon-failure-reconcile.bead-1".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: Some("Fixture description.".to_owned()),
                beads: vec![BeadProposal {
                    bead_id: Some("ms-daemon-failure-reconcile.bead-1".to_owned()),
                    explicit_id: Some(true),
                    title: "Daemon failure bead".to_owned(),
                    description: Some("Fixture description.".to_owned()),
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec!["fixture".to_owned()],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::Minimal,
            agents_guidance: None,
        };
        persist_plan(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base_dir,
            &record.id,
            &bundle,
            now + chrono::Duration::milliseconds(1),
        )
        .expect("persist milestone plan");
        record.id
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

    struct FailOnceWorkflowRunIdWriteStore {
        fail_next_workflow_run_id_write: AtomicBool,
    }

    impl FailOnceWorkflowRunIdWriteStore {
        fn new() -> Self {
            Self {
                fail_next_workflow_run_id_write: AtomicBool::new(true),
            }
        }
    }

    struct FailOnceLeaseReferenceClearStore {
        task_id: String,
        fail_next_clear_lease_write: AtomicBool,
    }

    impl FailOnceLeaseReferenceClearStore {
        fn new(task_id: &str) -> Self {
            Self {
                task_id: task_id.to_owned(),
                fail_next_clear_lease_write: AtomicBool::new(true),
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

    impl DaemonStorePort for FailOnceWorkflowRunIdWriteStore {
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
            if task.workflow_run_id.is_some()
                && self
                    .fail_next_workflow_run_id_write
                    .swap(false, Ordering::SeqCst)
            {
                return Err(AppError::Io(std::io::Error::other(
                    "simulated workflow_run_id metadata write failure",
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

    struct RewritePidOnLeaseRemoveStore {
        trigger_lease_id: String,
        project_id: ProjectId,
        repo_root: std::path::PathBuf,
        successor_writer_owner: String,
        rewrote_pid: AtomicBool,
    }

    impl RewritePidOnLeaseRemoveStore {
        fn new(
            trigger_lease_id: &str,
            project_id: &ProjectId,
            repo_root: &std::path::Path,
            successor_writer_owner: &str,
        ) -> Self {
            Self {
                trigger_lease_id: trigger_lease_id.to_owned(),
                project_id: project_id.clone(),
                repo_root: repo_root.to_path_buf(),
                successor_writer_owner: successor_writer_owner.to_owned(),
                rewrote_pid: AtomicBool::new(false),
            }
        }
    }

    impl DaemonStorePort for FailOnceLeaseReferenceClearStore {
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
            if task.task_id == self.task_id
                && task.lease_id.is_none()
                && self
                    .fail_next_clear_lease_write
                    .swap(false, Ordering::SeqCst)
            {
                return Err(AppError::Io(std::io::Error::other(
                    "simulated clear_lease_reference write failure",
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

    impl DaemonStorePort for RewritePidOnLeaseRemoveStore {
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
            let outcome = FsDaemonStore.remove_lease(base_dir, lease_id)?;
            if lease_id == self.trigger_lease_id
                && matches!(outcome, ResourceCleanupOutcome::Removed)
                && !self.rewrote_pid.swap(true, Ordering::SeqCst)
            {
                FsDaemonStore.acquire_writer_lock(
                    base_dir,
                    &self.project_id,
                    &self.successor_writer_owner,
                )?;
                FsDaemonStore.write_lease_record(
                    base_dir,
                    &LeaseRecord::CliWriter(CliWriterLease {
                        lease_id: self.successor_writer_owner.clone(),
                        project_id: self.project_id.to_string(),
                        owner: "cli".to_owned(),
                        acquired_at: Utc::now(),
                        ttl_seconds: 300,
                        last_heartbeat: Utc::now(),
                        cleanup_handoff: None,
                    }),
                )?;
                FileSystem::write_pid_file(
                    &self.repo_root,
                    &self.project_id,
                    RunPidOwner::Cli,
                    Some(&self.successor_writer_owner),
                    Some("run-successor"),
                    Some(Utc::now()),
                )?;
            }
            Ok(outcome)
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
                iterative_implementer_state: None,
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
            cleanup_handoff: None,
        };
        FsDaemonStore
            .write_lease(base, &lease)
            .expect("write worktree lease");
        FsDaemonStore
            .acquire_writer_lock(base, &project_id, &lease.lease_id)
            .expect("acquire writer lock");

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
        let dispatch_future = std::future::pending::<AppResult<Option<String>>>();
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
        let persisted_handoff = FsDaemonStore
            .read_lease(base, &lease.lease_id)
            .expect("read lease after daemon handoff")
            .cleanup_handoff;
        assert!(
            persisted_handoff.is_some(),
            "daemon handoff should persist process liveness proof while the grace period is active"
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
    fn prepare_cancelled_dispatch_handoff_persists_cleanup_proof_in_multi_repo_daemon_store() {
        let temp = tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let repo_root = temp.path().join("repo");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        initialize_workspace(&repo_root, Utc::now()).expect("init repo workspace");

        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");
        let project_id = create_standard_project(&repo_root, "daemon-handoff-persist");
        let started_at = Utc::now();
        let run_id = "run-daemon-handoff-persist";
        let lease_id = "lease-daemon-handoff-persist";

        let snapshot = RunSnapshot {
            active_run: Some(ActiveRun {
                run_id: run_id.to_owned(),
                stage_cursor: StageCursor::initial(StageId::Implementation),
                started_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
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
            .write_run_snapshot(&repo_root, &project_id, &snapshot)
            .expect("write running snapshot");
        FileSystem::write_pid_file(
            &repo_root,
            &project_id,
            RunPidOwner::Daemon,
            Some(lease_id),
            Some(run_id),
            Some(started_at),
        )
        .expect("write repo pid file");

        let lease = crate::contexts::automation_runtime::model::WorktreeLease {
            lease_id: lease_id.to_owned(),
            task_id: "task-daemon-handoff-persist".to_owned(),
            project_id: project_id.as_str().to_owned(),
            worktree_path: repo_root.join("worktrees/daemon-handoff-persist"),
            branch_name: "rb/daemon-handoff-persist".to_owned(),
            acquired_at: started_at,
            ttl_seconds: 300,
            last_heartbeat: started_at,
            cleanup_handoff: None,
        };
        FsDaemonStore
            .write_lease_record(&daemon_dir, &LeaseRecord::Worktree(lease.clone()))
            .expect("write daemon-store lease");
        FsDaemonStore
            .acquire_writer_lock(&daemon_dir, &project_id, &lease.lease_id)
            .expect("acquire daemon-store writer lock");

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

        let prepared = daemon
            .prepare_cancelled_dispatch_handoff(
                &daemon_dir,
                &repo_root,
                &project_id,
                &lease.lease_id,
                DAEMON_SHUTDOWN_STATUS_SUMMARY,
                DAEMON_SHUTDOWN_LOG_MESSAGE,
            )
            .expect("prepare cancelled dispatch handoff");
        assert!(
            prepared.interrupted_marker_persisted,
            "multi-repo daemon cancellation should still mark the run interrupted"
        );

        let persisted = FsDaemonStore
            .read_lease(&daemon_dir, &lease.lease_id)
            .expect("read daemon-store lease after handoff");
        assert!(
            persisted.cleanup_handoff.is_some(),
            "cleanup handoff must be persisted to the daemon store root, not the checkout root"
        );
    }

    #[test]
    fn load_dispatch_run_snapshot_repairs_missing_run_failed_event_from_daemon_handoff() {
        let temp = tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let repo_root = temp.path().join("repo");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        initialize_workspace(&repo_root, Utc::now()).expect("init repo workspace");

        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");
        let project_id = create_standard_project(&repo_root, "daemon-handoff-repair");
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
                iterative_implementer_state: None,
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
            .write_run_snapshot(&repo_root, &project_id, &snapshot)
            .expect("write interrupted snapshot");
        let dead_handoff = CliWriterCleanupHandoff {
            pid: std::process::id().saturating_add(100_000),
            recorded_at: Some(Utc::now()),
            run_id: Some("run-daemon-handoff-repair".to_owned()),
            run_started_at: Some(started_at),
            proc_start_ticks: FileSystem::proc_start_ticks_for_pid(std::process::id()),
            proc_start_marker: FileSystem::proc_start_marker_for_pid(std::process::id()),
        };
        let lease = crate::contexts::automation_runtime::model::WorktreeLease {
            lease_id: "lease-daemon-handoff-repair".to_owned(),
            task_id: "task-daemon-handoff-repair".to_owned(),
            project_id: project_id.as_str().to_owned(),
            worktree_path: repo_root.join("worktrees/daemon-handoff-repair"),
            branch_name: "rb/daemon-handoff-repair".to_owned(),
            acquired_at: started_at,
            ttl_seconds: 300,
            last_heartbeat: started_at,
            cleanup_handoff: Some(dead_handoff),
        };
        FsDaemonStore
            .write_lease_record(&daemon_dir, &LeaseRecord::Worktree(lease.clone()))
            .expect("write daemon handoff lease");
        FsDaemonStore
            .acquire_writer_lock(&daemon_dir, &project_id, &lease.lease_id)
            .expect("acquire daemon handoff writer lock");
        FileSystem::write_backend_processes(
            &repo_root,
            &project_id,
            &[RunBackendProcessRecord {
                pid: std::process::id().saturating_add(100_000),
                recorded_at: started_at,
                run_id: Some("run-daemon-handoff-repair".to_owned()),
                run_started_at: Some(started_at),
                proc_start_ticks: Some(u64::MAX),
                proc_start_marker: None,
            }],
        )
        .expect("write stale daemon backend process record");

        std::fs::write(
            repo_root.join(format!(
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
            .load_dispatch_run_snapshot(&daemon_dir, &repo_root, &project_id)
            .expect("load repaired snapshot");
        assert_eq!(repaired.status, RunStatus::Failed);
        assert!(repaired.interrupted_run.is_some());

        let journal = FsJournalStore
            .read_journal(&repo_root, &project_id)
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
        assert!(
            FileSystem::read_backend_processes(&repo_root, &project_id)
                .expect("read backend processes after daemon repair")
                .is_empty(),
            "dispatch snapshot repair should also prune stale daemon-owned backend records"
        );
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[ignore = "flaky in CI: backend pid file race; tracked as ftx"]
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
                iterative_implementer_state: None,
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
            cleanup_handoff: None,
        };
        FsDaemonStore
            .write_lease(base, &lease)
            .expect("write worktree lease");
        FsDaemonStore
            .acquire_writer_lock(base, &project_id, &lease.lease_id)
            .expect("acquire writer lock");

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
        let dispatch_future = std::future::pending::<AppResult<Option<String>>>();
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

    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[ignore = "flaky in CI: backend pid file race; tracked as ftx"]
    async fn finish_cancelled_dispatch_recovers_by_attempt_without_run_pid_and_cleans_backends() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");
        let project_id = create_standard_project(base, "daemon-cancel-missing-pid");
        let started_at = Utc::now();
        let run_id = "run-daemon-cancel-missing-pid".to_owned();

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
                iterative_implementer_state: None,
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
            Some("lease-daemon-cancel-missing-pid"),
            Some(run_id.as_str()),
            Some(started_at),
        )
        .expect("write pid file");

        let backend_pid_path = base.join("tracked-backend-missing-pid.pid");
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
        FileSystem::remove_pid_file(base, &project_id).expect("remove pid file before cleanup");
        assert_eq!(
            FileSystem::read_backend_processes(base, &project_id)
                .expect("read tracked backend processes")
                .len(),
            1,
            "tracked backend cleanup precondition should be durable"
        );

        let mut task = sample_waiting_task("daemon-cancel-missing-pid-task", "req-run");
        task.project_id = project_id.as_str().to_owned();
        task.status = TaskStatus::Active;
        task.dispatch_mode = DispatchMode::Workflow;
        task.requirements_run_id = None;

        let lease = WorktreeLease {
            lease_id: "lease-daemon-cancel-missing-pid".to_owned(),
            task_id: task.task_id.clone(),
            project_id: project_id.as_str().to_owned(),
            worktree_path: base.to_path_buf(),
            branch_name: "rb/test".to_owned(),
            acquired_at: Utc::now(),
            ttl_seconds: 300,
            last_heartbeat: Utc::now(),
            cleanup_handoff: None,
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
        let dispatch_future = std::future::ready(Ok(Some("run-cleanup-missing-pid".to_owned())));
        tokio::pin!(dispatch_future);
        let error = daemon
            .finish_cancelled_dispatch(
                base,
                base,
                &task,
                &lease,
                &project_id,
                &mut heartbeat,
                &mut dispatch_future,
                "failed (interrupted by daemon shutdown)",
                "daemon shutdown interrupted the orchestrator before graceful shutdown completed",
            )
            .await
            .expect_err("missing pid cleanup should still surface cancellation");
        assert!(
            matches!(error, AppError::InvocationCancelled { .. }),
            "unexpected error after daemon cancellation cleanup: {error:?}"
        );

        let recovered = FsRunSnapshotStore
            .read_run_snapshot(base, &project_id)
            .expect("read recovered snapshot");
        assert_eq!(recovered.status, RunStatus::Failed);
        assert!(recovered.active_run.is_none());
        assert!(
            recovered.interrupted_run.is_some(),
            "expected-attempt fallback should preserve interrupted run metadata"
        );
        assert!(
            FileSystem::read_pid_file(base, &project_id)
                .expect("read pid file after cleanup")
                .is_none(),
            "missing run.pid fallback must not leave a pid file behind"
        );
        assert!(
            FileSystem::read_backend_processes(base, &project_id)
                .expect("read tracked backend processes after cleanup")
                .is_empty(),
            "missing run.pid fallback should still prune tracked backend processes"
        );

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while FileSystem::is_pid_running_unchecked(backend_pid)
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(std::time::Duration::from_millis(25));
        }
        assert!(
            !FileSystem::is_pid_running_unchecked(backend_pid),
            "missing run.pid fallback should still SIGKILL the tracked backend process group"
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
            cleanup_handoff: None,
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
                    routing_labels: vec!["fixture".to_owned()],
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

    #[test]
    fn cleanup_active_leases_marks_shutdown_metadata_partial_failures_dirty() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");
        let project_id = create_standard_project(base, "daemon-cleanup-metadata-partial");
        let now = Utc::now();

        let lease = WorktreeLease {
            lease_id: "lease-daemon-cleanup-metadata-partial".to_owned(),
            task_id: "task-daemon-cleanup-metadata-partial".to_owned(),
            project_id: project_id.as_str().to_owned(),
            worktree_path: base.join("worktrees/daemon-cleanup-metadata-partial"),
            branch_name: "rb/test".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
            cleanup_handoff: None,
        };
        std::fs::create_dir_all(&lease.worktree_path).expect("create worktree path");
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
                    issue_ref: "acme/widgets#metadata-partial".to_owned(),
                    project_id: project_id.as_str().to_owned(),
                    project_name: Some("Metadata partial cleanup".to_owned()),
                    prompt: Some("Prompt".to_owned()),
                    routing_command: None,
                    routing_labels: vec!["fixture".to_owned()],
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

        let store = FailOnceLeaseReferenceClearStore::new(&lease.task_id);
        let worktree = TestWorktreeAdapter::removing_as(WorktreeCleanupOutcome::Removed);
        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &store,
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
            .cleanup_active_leases(base, base)
            .expect_err("metadata cleanup failure should surface partial cleanup");
        assert!(
            matches!(error, AppError::LeaseCleanupPartialFailure { .. }),
            "unexpected cleanup error: {error:?}"
        );

        let task = FsDaemonStore
            .read_task(base, &lease.task_id)
            .expect("read daemon task after metadata cleanup failure");
        assert_eq!(task.status, TaskStatus::Aborted);
        assert!(task.label_dirty);
        assert_eq!(
            task.lease_id.as_deref(),
            Some(lease.lease_id.as_str()),
            "shutdown cleanup must preserve the orphaned task lease reference"
        );
        assert!(
            FsDaemonStore.read_lease(base, &lease.lease_id).is_err(),
            "physical lease cleanup should already have completed before the metadata write failed"
        );
    }

    #[test]
    fn release_task_lease_allows_successor_run_pid_after_resources_are_released() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");
        let project_id = create_standard_project(base, "daemon-successor-run-pid");
        let now = Utc::now();
        let lease = WorktreeLease {
            lease_id: "lease-daemon-successor-run-pid".to_owned(),
            task_id: "task-daemon-successor-run-pid".to_owned(),
            project_id: project_id.as_str().to_owned(),
            worktree_path: base.join(".test-worktrees/task-daemon-successor-run-pid"),
            branch_name: "rb/task-daemon-successor-run-pid".to_owned(),
            acquired_at: now,
            ttl_seconds: 300,
            last_heartbeat: now,
            cleanup_handoff: None,
        };
        std::fs::create_dir_all(&lease.worktree_path).expect("create worktree path");
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
                    issue_ref: "acme/widgets#successor-run-pid".to_owned(),
                    project_id: project_id.as_str().to_owned(),
                    project_name: Some("Successor pid race".to_owned()),
                    prompt: Some("Prompt".to_owned()),
                    routing_command: None,
                    routing_labels: vec!["fixture".to_owned()],
                    resolved_flow: Some(FlowPreset::Standard),
                    routing_source: None,
                    routing_warnings: vec![],
                    status: TaskStatus::Completed,
                    created_at: now,
                    updated_at: now,
                    attempt_count: 1,
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
            .expect("write completed task");
        FileSystem::write_pid_file(
            base,
            &project_id,
            RunPidOwner::Daemon,
            Some(&lease.lease_id),
            Some("run-old"),
            Some(now),
        )
        .expect("write original daemon pid");

        let store = RewritePidOnLeaseRemoveStore::new(
            &lease.lease_id,
            &project_id,
            base,
            "cli-successor-run-pid",
        );
        let worktree = TestWorktreeAdapter::removing_as(WorktreeCleanupOutcome::Removed);
        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &store,
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

        daemon
            .release_task_lease(base, base, &lease.task_id, &lease)
            .expect("successor pid should not be treated as old-task partial cleanup");

        let task_after = FsDaemonStore
            .read_task(base, &lease.task_id)
            .expect("read task after releasing lease");
        assert!(
            task_after.lease_id.is_none(),
            "old task metadata should clear once its lease resources are gone"
        );
        assert!(
            FsDaemonStore.read_lease(base, &lease.lease_id).is_err(),
            "old worktree lease record should stay removed"
        );
        let successor_pid = FileSystem::read_pid_file(base, &project_id)
            .expect("read successor pid")
            .expect("successor pid should remain");
        assert_eq!(successor_pid.owner, RunPidOwner::Cli);
        assert_eq!(
            successor_pid.writer_owner.as_deref(),
            Some("cli-successor-run-pid")
        );
        let successor_lock_owner =
            crate::contexts::automation_runtime::cli_writer_lease::read_project_writer_lock_owner(
                base,
                &project_id,
            )
            .expect("read successor writer lock owner");
        assert_eq!(
            successor_lock_owner.as_deref(),
            Some("cli-successor-run-pid")
        );
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
                        iterative_implementer_state: None,
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

    #[tokio::test(flavor = "multi_thread")]
    async fn process_cycle_propagates_partial_cleanup_failure() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");
        let project_id = ProjectId::new("daemon-cycle-partial").expect("project id");
        let task = sample_pending_task("task-daemon-cycle-partial", &project_id);
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
            .process_cycle(base, &DaemonLoopConfig::default(), CancellationToken::new())
            .await
            .expect_err("cycle should stop on partial cleanup failure");
        assert!(
            matches!(error, AppError::LeaseCleanupPartialFailure { .. }),
            "unexpected cycle error: {error:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_cycle_phase0_repairs_persisted_cancelled_handoff_for_aborted_task() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");
        let project_id = create_standard_project(base, "daemon-phase0-handoff");
        let started_at = Utc::now();

        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-phase0-handoff".to_owned(),
                stage_cursor: StageCursor::initial(StageId::Implementation),
                started_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
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
                "{{\"sequence\":1,\"timestamp\":\"{}\",\"event_type\":\"project_created\",\"details\":{{\"project_id\":\"{}\",\"flow\":\"standard\"}}}}\n{{\"sequence\":2,\"timestamp\":\"{}\",\"event_type\":\"run_started\",\"details\":{{\"run_id\":\"run-phase0-handoff\",\"first_stage\":\"implementation\",\"max_completion_rounds\":20}}}}",
                (started_at - chrono::Duration::seconds(1)).to_rfc3339(),
                project_id.as_str(),
                started_at.to_rfc3339(),
            ),
        )
        .expect("write journal without run_failed");
        FileSystem::write_backend_processes(
            base,
            &project_id,
            &[crate::adapters::fs::RunBackendProcessRecord {
                pid: std::process::id().saturating_add(100_000),
                recorded_at: started_at,
                run_id: Some("run-phase0-handoff".to_owned()),
                run_started_at: Some(started_at),
                proc_start_ticks: Some(u64::MAX),
                proc_start_marker: None,
            }],
        )
        .expect("write stale backend process record");

        let lease = crate::contexts::automation_runtime::model::WorktreeLease {
            lease_id: "lease-phase0-handoff".to_owned(),
            task_id: "task-phase0-handoff".to_owned(),
            project_id: project_id.as_str().to_owned(),
            worktree_path: base.join("worktrees/phase0-handoff"),
            branch_name: "rb/phase0-handoff".to_owned(),
            acquired_at: started_at,
            ttl_seconds: 300,
            last_heartbeat: started_at,
            cleanup_handoff: Some(CliWriterCleanupHandoff {
                pid: std::process::id().saturating_add(100_000),
                recorded_at: Some(Utc::now()),
                run_id: Some("run-phase0-handoff".to_owned()),
                run_started_at: Some(started_at),
                proc_start_ticks: FileSystem::proc_start_ticks_for_pid(std::process::id()),
                proc_start_marker: FileSystem::proc_start_marker_for_pid(std::process::id()),
            }),
        };
        FsDaemonStore
            .write_lease(base, &lease)
            .expect("write phase0 handoff lease");
        FsDaemonStore
            .write_task(
                base,
                &DaemonTask {
                    task_id: lease.task_id.clone(),
                    issue_ref: "acme/widgets#phase0-handoff".to_owned(),
                    project_id: project_id.as_str().to_owned(),
                    project_name: Some("phase0 handoff".to_owned()),
                    prompt: Some("Prompt".to_owned()),
                    routing_command: None,
                    routing_labels: vec!["fixture".to_owned()],
                    resolved_flow: Some(FlowPreset::Standard),
                    routing_source: None,
                    routing_warnings: vec![],
                    status: TaskStatus::Aborted,
                    created_at: started_at,
                    updated_at: started_at,
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
            .expect("write aborted task");

        let worktree = TestWorktreeAdapter::removing_as(WorktreeCleanupOutcome::Removed);
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

        let did_work = daemon
            .process_cycle(base, &DaemonLoopConfig::default(), CancellationToken::new())
            .await
            .expect("phase0 repair cycle should succeed");
        assert!(
            !did_work,
            "phase0 handoff recovery should settle the aborted task without dispatching new work"
        );

        let repaired_journal = FsJournalStore
            .read_journal(base, &project_id)
            .expect("read repaired journal");
        assert_eq!(
            repaired_journal
                .iter()
                .filter(|event| {
                    event.event_type
                        == crate::contexts::project_run_record::model::JournalEventType::RunFailed
                })
                .count(),
            1,
            "phase0 recovery should append the missing daemon run_failed event"
        );
        assert!(
            FileSystem::read_backend_processes(base, &project_id)
                .expect("read backend processes after phase0 repair")
                .is_empty(),
            "phase0 recovery should prune stale daemon backend process records"
        );
        let repaired_task = FsDaemonStore
            .read_task(base, &lease.task_id)
            .expect("read repaired task");
        assert!(
            repaired_task.lease_id.is_none(),
            "phase0 recovery should release the aborted task lease after repairing the handoff"
        );
        assert!(
            FsDaemonStore.read_lease(base, &lease.lease_id).is_err(),
            "phase0 recovery should remove the persisted worktree lease"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_cycle_multi_repo_quarantines_recovered_handoff_finalization_errors_per_repo() {
        let temp = tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let bad_repo_root = temp.path().join("repo-bad");
        let good_repo_root = temp.path().join("repo-good");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        initialize_workspace(&bad_repo_root, Utc::now()).expect("init bad workspace");
        initialize_workspace(&good_repo_root, Utc::now()).expect("init good workspace");

        let bad_registration = RepoRegistration {
            repo_slug: "acme/bad".to_owned(),
            repo_root: bad_repo_root.clone(),
            workspace_root: bad_repo_root.join(".ralph-burning"),
        };
        let good_registration = RepoRegistration {
            repo_slug: "acme/good".to_owned(),
            repo_root: good_repo_root.clone(),
            workspace_root: good_repo_root.join(".ralph-burning"),
        };

        let bad_daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "bad");
        let bad_project_id = create_standard_project(&bad_repo_root, "bad-phase0-handoff");
        let bad_task = DaemonTask {
            status: TaskStatus::Aborted,
            lease_id: Some("lease-bad-phase0-handoff".to_owned()),
            ..sample_pending_task("task-bad-phase0-handoff", &bad_project_id)
        };
        FsDaemonStore
            .create_task(&bad_daemon_dir, &bad_task)
            .expect("create bad recovered terminal task");
        FsDaemonStore
            .write_lease_record(
                &bad_daemon_dir,
                &LeaseRecord::CliWriter(CliWriterLease {
                    lease_id: "lease-bad-phase0-handoff".to_owned(),
                    project_id: bad_project_id.as_str().to_owned(),
                    owner: "cli".to_owned(),
                    acquired_at: Utc::now(),
                    ttl_seconds: 300,
                    last_heartbeat: Utc::now(),
                    cleanup_handoff: None,
                }),
            )
            .expect("write wrong-type recovered handoff lease");

        let good_daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "good");
        let good_project_id = create_standard_project(&good_repo_root, "good-phase0-progress");
        let good_task = sample_pending_task("task-good-phase0-progress", &good_project_id);
        FsDaemonStore
            .create_task(&good_daemon_dir, &good_task)
            .expect("create good pending task");

        let worktree = TestWorktreeAdapter::removing_as(WorktreeCleanupOutcome::Removed);
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
        let github = InMemoryGithubClient::new();

        daemon
            .process_cycle_multi_repo(
                &data_dir,
                &DaemonLoopConfig::default(),
                &github,
                CancellationToken::new(),
                &[bad_registration, good_registration],
            )
            .await
            .expect("bad recovered handoff should quarantine only its repo");

        let bad_after = FsDaemonStore
            .read_task(&bad_daemon_dir, &bad_task.task_id)
            .expect("read bad task after cycle");
        assert_eq!(
            bad_after.status,
            TaskStatus::Aborted,
            "the failing repo should be left untouched for later operator recovery"
        );
        assert_eq!(
            bad_after.lease_id.as_deref(),
            Some("lease-bad-phase0-handoff"),
            "the recovered handoff lease should remain visible after repo quarantine"
        );

        let good_after = FsDaemonStore
            .read_task(&good_daemon_dir, &good_task.task_id)
            .expect("read good task after cycle");
        assert_eq!(
            good_after.status,
            TaskStatus::Completed,
            "a corrupt recovered handoff in one repo must not block later repos in the same cycle"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_cycle_stops_on_post_release_metadata_cleanup_failure() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");
        let project_id = ProjectId::new("daemon-cycle-metadata-partial").expect("project id");
        let first_task = sample_pending_task("task-daemon-cycle-metadata-partial-a", &project_id);
        let second_task = sample_pending_task("task-daemon-cycle-metadata-partial-b", &project_id);
        FsDaemonStore
            .create_task(base, &first_task)
            .expect("create first task");
        FsDaemonStore
            .create_task(base, &second_task)
            .expect("create second task");

        let store = FailOnceLeaseReferenceClearStore::new(&first_task.task_id);
        let worktree = TestWorktreeAdapter::removing_as(WorktreeCleanupOutcome::Removed);
        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &store,
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
            .process_cycle(base, &DaemonLoopConfig::default(), CancellationToken::new())
            .await
            .expect_err("metadata cleanup failure should stop the single-repo cycle");
        assert!(
            matches!(error, AppError::LeaseCleanupPartialFailure { .. }),
            "unexpected cycle error: {error:?}"
        );

        let first_lease_id = format!("lease-{}", first_task.task_id);
        let completed_first = FsDaemonStore
            .read_task(base, &first_task.task_id)
            .expect("read first task after metadata cleanup failure");
        assert_eq!(completed_first.status, TaskStatus::Completed);
        assert_eq!(
            completed_first.lease_id.as_deref(),
            Some(first_lease_id.as_str()),
            "the stale task lease reference should remain visible for operator recovery"
        );
        assert!(
            FsDaemonStore.read_lease(base, &first_lease_id).is_err(),
            "physical lease resources should already be gone when only the metadata write failed"
        );

        let untouched_second = FsDaemonStore
            .read_task(base, &second_task.task_id)
            .expect("read second task after interrupted cycle");
        assert_eq!(
            untouched_second.status,
            TaskStatus::Pending,
            "the daemon must stop scanning once a post-release metadata write fails"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires real worktree for lease cleanup; partial cleanup path needs test fixture update"]
    async fn process_cycle_multi_repo_repairs_orphaned_lease_reference_after_metadata_failure() {
        let temp = tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let repo_root = temp.path().join("repo");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        initialize_workspace(&repo_root, Utc::now()).expect("init repo workspace");

        let repo_slug = "acme/widgets";
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");
        let registration = RepoRegistration {
            repo_slug: repo_slug.to_owned(),
            repo_root: repo_root.clone(),
            workspace_root: repo_root.join(".ralph-burning"),
        };

        let first_project_id = create_standard_project(&repo_root, "multi-repo-metadata-a");
        let second_project_id = create_standard_project(&repo_root, "multi-repo-metadata-b");
        FileSystem::write_pid_file(
            &repo_root,
            &first_project_id,
            RunPidOwner::Daemon,
            Some("daemon-owner"),
            Some("run-multi-repo-metadata-a"),
            Some(Utc::now()),
        )
        .expect("write multi-repo pid file");

        let mut first_task = sample_pending_task("task-multi-repo-metadata-a", &first_project_id);
        first_task.issue_ref = format!("{repo_slug}#11");
        first_task.repo_slug = Some(repo_slug.to_owned());
        first_task.issue_number = Some(11);
        FsDaemonStore
            .create_task(&daemon_dir, &first_task)
            .expect("create first multi-repo task");

        let mut second_task = sample_pending_task("task-multi-repo-metadata-b", &second_project_id);
        second_task.issue_ref = format!("{repo_slug}#12");
        second_task.repo_slug = Some(repo_slug.to_owned());
        second_task.issue_number = Some(12);
        FsDaemonStore
            .create_task(&daemon_dir, &second_task)
            .expect("create second multi-repo task");

        let store = FailOnceLeaseReferenceClearStore::new(&first_task.task_id);
        let worktree = TestWorktreeAdapter::removing_as(WorktreeCleanupOutcome::Removed);
        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &store,
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
        let github = InMemoryGithubClient::new();

        daemon
            .process_cycle_multi_repo(
                &data_dir,
                &DaemonLoopConfig::default(),
                &github,
                CancellationToken::new(),
                &[registration.clone()],
            )
            .await
            .expect("first multi-repo cycle");

        let first_after_first = FsDaemonStore
            .read_task(&daemon_dir, &first_task.task_id)
            .expect("read first task after metadata cleanup failure");
        let first_lease_id = format!("lease-{}", first_task.task_id);
        assert_eq!(first_after_first.status, TaskStatus::Completed);
        assert!(first_after_first.label_dirty);
        assert_eq!(
            first_after_first.lease_id.as_deref(),
            Some(first_lease_id.as_str())
        );
        assert!(
            FsDaemonStore
                .read_lease(&daemon_dir, &first_lease_id)
                .is_err(),
            "the repo should be quarantined even after the physical lease is already gone"
        );

        let second_after_first = FsDaemonStore
            .read_task(&daemon_dir, &second_task.task_id)
            .expect("read second task after first cycle");
        assert_eq!(
            second_after_first.status,
            TaskStatus::Pending,
            "follow-on tasks must stay blocked until the orphaned lease reference is repaired"
        );

        daemon
            .process_cycle_multi_repo(
                &data_dir,
                &DaemonLoopConfig::default(),
                &github,
                CancellationToken::new(),
                &[registration],
            )
            .await
            .expect("second multi-repo cycle");

        let repaired_first = FsDaemonStore
            .read_task(&daemon_dir, &first_task.task_id)
            .expect("read repaired first task");
        assert!(!repaired_first.label_dirty);
        assert!(
            repaired_first.lease_id.is_none(),
            "Phase 0 should repair orphaned task lease references even without a lease file"
        );
        assert!(
            FileSystem::read_pid_file(&repo_root, &first_project_id)
                .expect("read pid file after orphaned repair")
                .is_none(),
            "Phase 0 orphaned cleanup must remove the checkout run.pid, not look under daemon state"
        );

        let second_after_second = FsDaemonStore
            .read_task(&daemon_dir, &second_task.task_id)
            .expect("read second task after recovery cycle");
        assert_eq!(
            second_after_second.status,
            TaskStatus::Completed,
            "the repo should resume processing once the orphaned lease reference is repaired"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires real worktree for lease cleanup; partial cleanup path needs test fixture update"]
    async fn process_cycle_multi_repo_reclaims_dead_daemon_pid_record_during_orphaned_repair() {
        let temp = tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let repo_root = temp.path().join("repo");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        initialize_workspace(&repo_root, Utc::now()).expect("init repo workspace");

        let repo_slug = "acme/widgets";
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");
        let registration = RepoRegistration {
            repo_slug: repo_slug.to_owned(),
            repo_root: repo_root.clone(),
            workspace_root: repo_root.join(".ralph-burning"),
        };

        let first_project_id = create_standard_project(&repo_root, "multi-repo-stale-daemon-pid");
        let mut repaired_task =
            sample_pending_task("task-multi-repo-stale-daemon-pid", &first_project_id);
        repaired_task.issue_ref = format!("{repo_slug}#41");
        repaired_task.repo_slug = Some(repo_slug.to_owned());
        repaired_task.issue_number = Some(41);
        repaired_task.status = TaskStatus::Completed;
        repaired_task.lease_id = Some("lease-stale-daemon-pid".to_owned());
        repaired_task.label_dirty = true;
        FsDaemonStore
            .create_task(&daemon_dir, &repaired_task)
            .expect("create dirty task");

        let stale_pid_record = RunPidRecord {
            pid: 999_999,
            started_at: Utc::now(),
            owner: RunPidOwner::Daemon,
            writer_owner: repaired_task.lease_id.clone(),
            run_id: Some("run-stale-daemon-pid".to_owned()),
            run_started_at: Some(Utc::now()),
            proc_start_ticks: Some(1),
            proc_start_marker: None,
        };
        let stale_pid_path =
            FileSystem::live_project_root(&repo_root, &first_project_id).join("run.pid");
        std::fs::write(
            &stale_pid_path,
            serde_json::to_vec(&stale_pid_record).expect("serialize stale pid record"),
        )
        .expect("write stale pid record");

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
        let github = InMemoryGithubClient::new();

        daemon
            .process_cycle_multi_repo(
                &data_dir,
                &DaemonLoopConfig::default(),
                &github,
                CancellationToken::new(),
                &[registration],
            )
            .await
            .expect("cycle should repair stale daemon pid");

        let repaired_after = FsDaemonStore
            .read_task(&daemon_dir, &repaired_task.task_id)
            .expect("read repaired task");
        assert!(!repaired_after.label_dirty);
        assert!(
            repaired_after.lease_id.is_none(),
            "Phase 0 should clear the orphaned lease metadata after stale pid cleanup"
        );
        assert!(
            FileSystem::read_pid_file(&repo_root, &first_project_id)
                .expect("read stale pid after repair")
                .is_none(),
            "Phase 0 should reclaim dead daemon-owned pid files from prior daemon processes"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_cycle_multi_repo_quarantines_unreclaimable_orphaned_pid_records() {
        let temp = tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let repo_root = temp.path().join("repo");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        initialize_workspace(&repo_root, Utc::now()).expect("init repo workspace");

        let repo_slug = "acme/widgets";
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");
        let registration = RepoRegistration {
            repo_slug: repo_slug.to_owned(),
            repo_root: repo_root.clone(),
            workspace_root: repo_root.join(".ralph-burning"),
        };

        let first_project_id =
            create_standard_project(&repo_root, "multi-repo-live-daemon-pid-blocked");
        let second_project_id =
            create_standard_project(&repo_root, "multi-repo-live-daemon-pid-next");

        let mut dirty_task =
            sample_pending_task("task-multi-repo-live-daemon-pid-blocked", &first_project_id);
        dirty_task.issue_ref = format!("{repo_slug}#51");
        dirty_task.repo_slug = Some(repo_slug.to_owned());
        dirty_task.issue_number = Some(51);
        dirty_task.status = TaskStatus::Completed;
        dirty_task.lease_id = Some("lease-live-daemon-pid".to_owned());
        dirty_task.label_dirty = true;
        FsDaemonStore
            .create_task(&daemon_dir, &dirty_task)
            .expect("create dirty task");

        let mut blocked_task =
            sample_pending_task("task-multi-repo-live-daemon-pid-next", &second_project_id);
        blocked_task.issue_ref = format!("{repo_slug}#52");
        blocked_task.repo_slug = Some(repo_slug.to_owned());
        blocked_task.issue_number = Some(52);
        FsDaemonStore
            .create_task(&daemon_dir, &blocked_task)
            .expect("create blocked task");

        FileSystem::write_pid_file(
            &repo_root,
            &first_project_id,
            RunPidOwner::Daemon,
            Some("lease-fresh-daemon-pid"),
            Some("run-live-daemon-pid"),
            Some(Utc::now()),
        )
        .expect("write live daemon pid file");

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
        let github = InMemoryGithubClient::new();

        daemon
            .process_cycle_multi_repo(
                &data_dir,
                &DaemonLoopConfig::default(),
                &github,
                CancellationToken::new(),
                std::slice::from_ref(&registration),
            )
            .await
            .expect("unreclaimable pid should quarantine repo without crashing");

        let dirty_after = FsDaemonStore
            .read_task(&daemon_dir, &dirty_task.task_id)
            .expect("read dirty task after failed orphaned cleanup");
        assert!(dirty_after.label_dirty);
        assert_eq!(
            dirty_after.lease_id.as_deref(),
            Some("lease-live-daemon-pid"),
            "the orphaned lease reference must remain visible when pid cleanup is unresolved"
        );
        assert!(
            FileSystem::read_pid_file(&repo_root, &first_project_id)
                .expect("read live pid after failed cleanup")
                .is_some(),
            "the daemon must not remove a live pid file owned by a different run"
        );

        let blocked_after = FsDaemonStore
            .read_task(&daemon_dir, &blocked_task.task_id)
            .expect("read blocked task after failed cleanup");
        assert_eq!(
            blocked_after.status,
            TaskStatus::Pending,
            "Phase 0 should keep the repo quarantined until the orphaned pid record can be reconciled"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_cycle_multi_repo_removes_run_pid_from_checkout_root_on_completion() {
        let temp = tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let repo_root = temp.path().join("repo");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        initialize_workspace(&repo_root, Utc::now()).expect("init repo workspace");

        let repo_slug = "acme/widgets";
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");
        let registration = RepoRegistration {
            repo_slug: repo_slug.to_owned(),
            repo_root: repo_root.clone(),
            workspace_root: repo_root.join(".ralph-burning"),
        };

        let project_id = create_standard_project(&repo_root, "multi-repo-pid-cleanup");
        FileSystem::write_pid_file(
            &repo_root,
            &project_id,
            RunPidOwner::Daemon,
            Some("daemon-owner"),
            Some("run-multi-repo-pid-cleanup"),
            Some(Utc::now()),
        )
        .expect("write multi-repo pid file");

        let mut task = sample_pending_task("task-multi-repo-pid-cleanup", &project_id);
        task.issue_ref = format!("{repo_slug}#21");
        task.repo_slug = Some(repo_slug.to_owned());
        task.issue_number = Some(21);
        FsDaemonStore
            .create_task(&daemon_dir, &task)
            .expect("create multi-repo task");

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let worktree = TestWorktreeAdapter::removing_as(WorktreeCleanupOutcome::Removed);
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
        let github = InMemoryGithubClient::new();

        daemon
            .process_cycle_multi_repo(
                &data_dir,
                &DaemonLoopConfig::default(),
                &github,
                CancellationToken::new(),
                &[registration],
            )
            .await
            .expect("multi-repo cycle should complete");

        let completed_task = FsDaemonStore
            .read_task(&daemon_dir, &task.task_id)
            .expect("read completed multi-repo task");
        assert_eq!(completed_task.status, TaskStatus::Completed);
        assert!(
            FileSystem::read_pid_file(&repo_root, &project_id)
                .expect("read pid file after completion")
                .is_none(),
            "multi-repo completion cleanup must remove run.pid from the checkout root"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires real worktree for lease cleanup; partial cleanup path needs test fixture update"]
    async fn process_cycle_multi_repo_quarantines_corrupt_terminal_lease_records() {
        let temp = tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let repo_root = temp.path().join("repo");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        initialize_workspace(&repo_root, Utc::now()).expect("init repo workspace");

        let repo_slug = "acme/widgets";
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");
        let registration = RepoRegistration {
            repo_slug: repo_slug.to_owned(),
            repo_root: repo_root.clone(),
            workspace_root: repo_root.join(".ralph-burning"),
        };

        let project_id = create_standard_project(&repo_root, "multi-repo-corrupt-lease");
        let mut dirty_task = sample_pending_task("task-multi-repo-corrupt-lease", &project_id);
        dirty_task.issue_ref = format!("{repo_slug}#31");
        dirty_task.repo_slug = Some(repo_slug.to_owned());
        dirty_task.issue_number = Some(31);
        dirty_task.status = TaskStatus::Completed;
        dirty_task.lease_id = Some("lease-corrupt-terminal".to_owned());
        dirty_task.label_dirty = true;
        FsDaemonStore
            .create_task(&daemon_dir, &dirty_task)
            .expect("create dirty terminal task");

        let mut blocked_task =
            sample_pending_task("task-multi-repo-corrupt-lease-blocked", &project_id);
        blocked_task.issue_ref = format!("{repo_slug}#32");
        blocked_task.repo_slug = Some(repo_slug.to_owned());
        blocked_task.issue_number = Some(32);
        FsDaemonStore
            .create_task(&daemon_dir, &blocked_task)
            .expect("create blocked task");

        FsDaemonStore
            .write_lease_record(
                &daemon_dir,
                &LeaseRecord::CliWriter(CliWriterLease {
                    lease_id: "lease-corrupt-terminal".to_owned(),
                    project_id: project_id.as_str().to_owned(),
                    owner: "cli".to_owned(),
                    acquired_at: Utc::now(),
                    ttl_seconds: 300,
                    last_heartbeat: Utc::now(),
                    cleanup_handoff: None,
                }),
            )
            .expect("write wrong-type lease record");

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
        let github = InMemoryGithubClient::new();

        daemon
            .process_cycle_multi_repo(
                &data_dir,
                &DaemonLoopConfig::default(),
                &github,
                CancellationToken::new(),
                std::slice::from_ref(&registration),
            )
            .await
            .expect("corrupt lease should quarantine repo without crashing the daemon");

        let dirty_after_first = FsDaemonStore
            .read_task(&daemon_dir, &dirty_task.task_id)
            .expect("read dirty task after first cycle");
        assert!(dirty_after_first.label_dirty);
        assert_eq!(
            dirty_after_first.lease_id.as_deref(),
            Some("lease-corrupt-terminal"),
            "corrupt lease records must remain visible instead of being cleared as orphaned"
        );
        let blocked_after_first = FsDaemonStore
            .read_task(&daemon_dir, &blocked_task.task_id)
            .expect("read blocked task after first cycle");
        assert_eq!(
            blocked_after_first.status,
            TaskStatus::Pending,
            "Phase 0 must keep the repo quarantined while the terminal lease record is corrupt"
        );

        daemon
            .process_cycle_multi_repo(
                &data_dir,
                &DaemonLoopConfig::default(),
                &github,
                CancellationToken::new(),
                &[registration],
            )
            .await
            .expect("repeated corrupt lease reads should continue quarantining the repo");

        let blocked_after_second = FsDaemonStore
            .read_task(&daemon_dir, &blocked_task.task_id)
            .expect("read blocked task after second cycle");
        assert_eq!(
            blocked_after_second.status,
            TaskStatus::Pending,
            "the repo must stay blocked across cycles until the corrupt lease record is repaired"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_cycle_multi_repo_blocks_repo_after_recovered_terminal_cleanup_failure() {
        let temp = tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let repo_root = temp.path().join("repo");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        initialize_workspace(&repo_root, Utc::now()).expect("init repo workspace");

        let repo_slug = "acme/widgets";
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");
        let registration = RepoRegistration {
            repo_slug: repo_slug.to_owned(),
            repo_root: repo_root.clone(),
            workspace_root: repo_root.join(".ralph-burning"),
        };

        let project_id = create_standard_project(&repo_root, "multi-repo-recovered-terminal");
        let started_at = Utc::now();
        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-multi-repo-recovered-terminal".to_owned(),
                stage_cursor: StageCursor::initial(StageId::Implementation),
                started_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
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
            .write_run_snapshot(&repo_root, &project_id, &snapshot)
            .expect("write interrupted snapshot");
        std::fs::write(
            repo_root.join(format!(
                ".ralph-burning/projects/{}/journal.ndjson",
                project_id.as_str()
            )),
            format!(
                "{{\"sequence\":1,\"timestamp\":\"{}\",\"event_type\":\"project_created\",\"details\":{{\"project_id\":\"{}\",\"flow\":\"standard\"}}}}\n{{\"sequence\":2,\"timestamp\":\"{}\",\"event_type\":\"run_started\",\"details\":{{\"run_id\":\"run-multi-repo-recovered-terminal\",\"first_stage\":\"implementation\",\"max_completion_rounds\":20}}}}",
                (started_at - chrono::Duration::seconds(1)).to_rfc3339(),
                project_id.as_str(),
                started_at.to_rfc3339(),
            ),
        )
        .expect("write journal without run_failed");
        FileSystem::write_backend_processes(
            &repo_root,
            &project_id,
            &[RunBackendProcessRecord {
                pid: std::process::id().saturating_add(100_000),
                recorded_at: started_at,
                run_id: Some("run-multi-repo-recovered-terminal".to_owned()),
                run_started_at: Some(started_at),
                proc_start_ticks: Some(u64::MAX),
                proc_start_marker: None,
            }],
        )
        .expect("write stale backend process record");

        let recovered_lease = WorktreeLease {
            lease_id: "lease-multi-repo-recovered-terminal".to_owned(),
            task_id: "task-multi-repo-recovered-terminal".to_owned(),
            project_id: project_id.as_str().to_owned(),
            worktree_path: repo_root.join("worktrees/multi-repo-recovered-terminal"),
            branch_name: "rb/multi-repo-recovered-terminal".to_owned(),
            acquired_at: started_at,
            ttl_seconds: 300,
            last_heartbeat: started_at,
            cleanup_handoff: Some(CliWriterCleanupHandoff {
                pid: std::process::id().saturating_add(100_000),
                recorded_at: Some(Utc::now()),
                run_id: Some("run-multi-repo-recovered-terminal".to_owned()),
                run_started_at: Some(started_at),
                proc_start_ticks: FileSystem::proc_start_ticks_for_pid(std::process::id()),
                proc_start_marker: FileSystem::proc_start_marker_for_pid(std::process::id()),
            }),
        };
        FsDaemonStore
            .write_lease(&daemon_dir, &recovered_lease)
            .expect("write recovered terminal lease");
        let recovered_task = DaemonTask {
            task_id: recovered_lease.task_id.clone(),
            issue_ref: format!("{repo_slug}#41"),
            project_id: project_id.as_str().to_owned(),
            project_name: Some("Recovered terminal".to_owned()),
            prompt: Some("Prompt".to_owned()),
            routing_command: None,
            routing_labels: vec!["fixture".to_owned()],
            resolved_flow: Some(FlowPreset::Standard),
            routing_source: None,
            routing_warnings: vec![],
            status: TaskStatus::Aborted,
            created_at: started_at,
            updated_at: started_at,
            attempt_count: 0,
            lease_id: Some(recovered_lease.lease_id.clone()),
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
        };
        FsDaemonStore
            .write_task(&daemon_dir, &recovered_task)
            .expect("write recovered terminal task");

        let blocked_task =
            sample_pending_task("task-multi-repo-blocked-after-recovery", &project_id);
        FsDaemonStore
            .create_task(&daemon_dir, &blocked_task)
            .expect("create blocked task");

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
        let github = InMemoryGithubClient::new();

        daemon
            .process_cycle_multi_repo(
                &data_dir,
                &DaemonLoopConfig::default(),
                &github,
                CancellationToken::new(),
                &[registration],
            )
            .await
            .expect("recovered terminal cleanup failure should quarantine only this repo");

        let recovered_after = FsDaemonStore
            .read_task(&daemon_dir, &recovered_task.task_id)
            .expect("read recovered task after cycle");
        assert!(
            recovered_after.label_dirty,
            "phase-0 cleanup failures for recovered terminal tasks must stay dirty for retry"
        );
        assert_eq!(
            recovered_after.lease_id.as_deref(),
            Some("lease-multi-repo-recovered-terminal"),
            "the recovered task lease must remain visible after partial cleanup"
        );

        let blocked_after = FsDaemonStore
            .read_task(&daemon_dir, &blocked_task.task_id)
            .expect("read blocked task after cycle");
        assert_eq!(
            blocked_after.status,
            TaskStatus::Pending,
            "the repo must stop before processing later pending tasks in the same cycle"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn process_cycle_multi_repo_quarantines_repo_after_partial_cleanup_failure() {
        let temp = tempdir().expect("tempdir");
        let data_dir = temp.path().join("data");
        let repo_root = temp.path().join("repo");
        std::fs::create_dir_all(&data_dir).expect("create data dir");
        initialize_workspace(&repo_root, Utc::now()).expect("init repo workspace");

        let repo_slug = "acme/widgets";
        let daemon_dir = DataDirLayout::daemon_dir(&data_dir, "acme", "widgets");
        let registration = RepoRegistration {
            repo_slug: repo_slug.to_owned(),
            repo_root: repo_root.clone(),
            workspace_root: repo_root.join(".ralph-burning"),
        };

        let first_project_id = create_standard_project(&repo_root, "multi-repo-partial-a");
        let second_project_id = create_standard_project(&repo_root, "multi-repo-partial-b");

        let mut first_task = sample_pending_task("task-multi-repo-partial-a", &first_project_id);
        first_task.issue_ref = format!("{repo_slug}#1");
        first_task.repo_slug = Some(repo_slug.to_owned());
        first_task.issue_number = Some(1);
        FsDaemonStore
            .create_task(&daemon_dir, &first_task)
            .expect("create first multi-repo task");

        let mut second_task = sample_pending_task("task-multi-repo-partial-b", &second_project_id);
        second_task.issue_ref = format!("{repo_slug}#2");
        second_task.repo_slug = Some(repo_slug.to_owned());
        second_task.issue_number = Some(2);
        FsDaemonStore
            .create_task(&daemon_dir, &second_task)
            .expect("create second multi-repo task");

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
        let github = InMemoryGithubClient::new();

        daemon
            .process_cycle_multi_repo(
                &data_dir,
                &DaemonLoopConfig::default(),
                &github,
                CancellationToken::new(),
                &[registration.clone()],
            )
            .await
            .expect("first multi-repo cycle");

        let after_first = FsDaemonStore
            .read_task(&daemon_dir, &first_task.task_id)
            .expect("read first task after partial cleanup");
        assert_eq!(after_first.status, TaskStatus::Completed);
        assert!(after_first.label_dirty);
        assert!(after_first.lease_id.is_some());

        let second_after_first = FsDaemonStore
            .read_task(&daemon_dir, &second_task.task_id)
            .expect("read second task after first cycle");
        assert_eq!(second_after_first.status, TaskStatus::Pending);
        assert!(!second_after_first.label_dirty);

        daemon
            .process_cycle_multi_repo(
                &data_dir,
                &DaemonLoopConfig::default(),
                &github,
                CancellationToken::new(),
                &[registration],
            )
            .await
            .expect("second multi-repo cycle");

        let second_after_second = FsDaemonStore
            .read_task(&daemon_dir, &second_task.task_id)
            .expect("read second task after quarantine cycle");
        assert_eq!(
            second_after_second.status,
            TaskStatus::Pending,
            "repo should remain quarantined until the dirty task cleanup succeeds"
        );
    }

    #[test]
    fn aborted_dispatch_cleanup_allows_successful_outcomes() {
        assert!(
            super::finish_aborted_dispatch_task_cleanup::<()>("task-aborted", Ok(()), Ok(()))
                .is_ok()
        );
        assert!(super::finish_aborted_dispatch_task_cleanup::<()>(
            "task-aborted",
            Err(AppError::InvocationCancelled {
                backend: "daemon".to_owned(),
                contract_id: "task-aborted".to_owned(),
            }),
            Ok(())
        )
        .is_ok());
    }

    #[test]
    fn aborted_dispatch_cleanup_propagates_dispatch_errors_after_lease_release() {
        let error = super::finish_aborted_dispatch_task_cleanup::<()>(
            "task-aborted",
            Err(AppError::Io(std::io::Error::other(
                "backend cleanup failed",
            ))),
            Ok(()),
        )
        .expect_err("non-cancellation outcome should surface after lease release");
        assert!(
            matches!(error, AppError::Io(_)),
            "unexpected propagated error: {error:?}"
        );
    }

    #[test]
    fn aborted_dispatch_cleanup_prioritizes_lease_cleanup_failures() {
        let error = super::finish_aborted_dispatch_task_cleanup::<()>(
            "task-aborted",
            Err(AppError::Io(std::io::Error::other(
                "backend cleanup failed",
            ))),
            Err(AppError::LeaseCleanupPartialFailure {
                task_id: "task-aborted".to_owned(),
            }),
        )
        .expect_err("lease cleanup failures must stay visible for Phase 0 recovery");
        assert!(
            matches!(error, AppError::LeaseCleanupPartialFailure { .. }),
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

        assert_eq!(
            result.expect("dispatch completed status should not error"),
            None,
            "RunStatus::Completed must return Ok(None) for reconciliation retries"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_reconcile_success_missing_workflow_run_id_fails_closed_and_signals_operator() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        let now = Utc::now();
        initialize_workspace(base, now).expect("init workspace");

        let milestone = create_milestone(
            &FsMilestoneStore,
            base,
            CreateMilestoneInput {
                id: "ms-missing-run-binding".to_owned(),
                name: "Missing run binding".to_owned(),
                description: "Reconciliation should fail closed".to_owned(),
            },
            now,
        )
        .expect("create milestone");
        let task_source = TaskSource {
            milestone_id: milestone.id.to_string(),
            bead_id: "ms-missing-run-binding.bead-1".to_owned(),
            parent_epic_id: None,
            origin: TaskOrigin::Milestone,
            plan_hash: Some("plan-v1".to_owned()),
            plan_version: Some(1),
            plan_workstream_index: None,
            plan_bead_index: None,
        };
        let project_id = ProjectId::new("missing-run-binding-project").expect("project id");
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Missing run binding project".to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt"),
                created_at: now,
                task_source: Some(task_source.clone()),
            },
        )
        .expect("create project");

        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base,
            &milestone.id,
            milestone_controller::ControllerTransitionRequest::new(
                milestone_controller::MilestoneControllerState::Running,
                "bead execution started",
            )
            .with_bead(&task_source.bead_id)
            .with_task("task-missing-run-binding"),
            now - chrono::Duration::seconds(1),
        )
        .expect("mark controller running");

        let journal_events = FsJournalStore
            .read_journal(base, &project_id)
            .expect("read initial journal");
        let manual_rerun_id = RunId::new("run-newer-manual-rerun").expect("run id");
        let run_started = journal::run_started_event(
            journal::last_sequence(&journal_events) + 1,
            now + chrono::Duration::seconds(2),
            &manual_rerun_id,
            StageId::Implementation,
            1,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_started).expect("serialize run_started"),
            )
            .expect("append newer run_started");
        let run_completed = journal::run_completed_event(
            run_started.sequence + 1,
            now + chrono::Duration::seconds(3),
            &manual_rerun_id,
            1,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_completed).expect("serialize run_completed"),
            )
            .expect("append newer run_completed");

        let mut task = sample_pending_task("task-missing-run-binding", &project_id);
        task.status = TaskStatus::Active;
        task.attempt_count = 1;

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
            .try_reconcile_success(base, &task)
            .await
            .expect_err("missing workflow_run_id must fail closed");
        assert_eq!(error.0, "reconciliation_no_run_binding");
        assert!(
            error.1.contains("workflow_run_id is missing"),
            "error should explain the missing run binding: {}",
            error.1
        );
        assert!(
            error.1.contains("Operator intervention required"),
            "error should require operator action: {}",
            error.1
        );

        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &milestone.id)
                .expect("load controller")
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::NeedsOperator
        );
        assert_eq!(
            controller.active_bead_id.as_deref(),
            Some(task_source.bead_id.as_str())
        );
        assert_eq!(
            controller.active_task_id.as_deref(),
            Some(task.task_id.as_str())
        );
        assert!(
            controller
                .last_transition_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("workflow_run_id is missing")),
            "controller should carry the operator-facing missing-binding reason: {controller:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persist_workflow_run_id_requires_authoritative_binding() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        let now = Utc::now();
        initialize_workspace(base, now).expect("init workspace");

        let project_id = ProjectId::new("persist-run-binding-project").expect("project id");
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Persist run binding project".to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt"),
                created_at: now,
                task_source: None,
            },
        )
        .expect("create project");

        let original_run_id = RunId::new("run-original-dispatch").expect("original run id");
        let original_started = journal::run_started_event(
            2,
            now + chrono::Duration::seconds(1),
            &original_run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&original_started).expect("serialize original run"),
            )
            .expect("append original run");

        let manual_rerun_id = RunId::new("run-manual-rerun").expect("manual rerun id");
        let manual_started = journal::run_started_event(
            3,
            now + chrono::Duration::seconds(2),
            &manual_rerun_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&manual_started).expect("serialize manual rerun"),
            )
            .expect("append manual rerun");

        let store = FsDaemonStore;
        let task_without_binding =
            sample_pending_task("task-no-authoritative-binding", &project_id);
        store
            .create_task(base, &task_without_binding)
            .expect("create task without binding");

        let task_with_binding = sample_pending_task("task-authoritative-binding", &project_id);
        store
            .create_task(base, &task_with_binding)
            .expect("create task with binding");

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &store,
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

        daemon.persist_workflow_run_id(base, &task_without_binding, None);
        let unchanged = store
            .read_task(base, &task_without_binding.task_id)
            .expect("read unchanged task");
        assert!(
            unchanged.workflow_run_id.is_none(),
            "missing authoritative binding must leave workflow_run_id unset"
        );

        daemon.persist_workflow_run_id(base, &task_with_binding, Some(original_run_id.as_str()));
        let persisted = store
            .read_task(base, &task_with_binding.task_id)
            .expect("read persisted task");
        assert_eq!(
            persisted.workflow_run_id.as_deref(),
            Some(original_run_id.as_str()),
            "persisted binding must use the authoritative dispatch run_id, not the latest journal event"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_reconcile_success_fails_closed_when_workflow_run_id_persist_fails() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        let now = Utc::now();
        initialize_workspace(base, now).expect("init workspace");

        let milestone = create_milestone(
            &FsMilestoneStore,
            base,
            CreateMilestoneInput {
                id: "ms-authoritative-run-binding".to_owned(),
                name: "Authoritative run binding".to_owned(),
                description: "Fresh dispatch should use captured run_id".to_owned(),
            },
            now,
        )
        .expect("create milestone");
        let task_source = TaskSource {
            milestone_id: milestone.id.to_string(),
            bead_id: "ms-authoritative-run-binding.bead-1".to_owned(),
            parent_epic_id: None,
            origin: TaskOrigin::Milestone,
            plan_hash: Some("plan-v1".to_owned()),
            plan_version: Some(1),
            plan_workstream_index: None,
            plan_bead_index: None,
        };
        let project_id = ProjectId::new("authoritative-run-binding-project").expect("project id");
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Authoritative run binding project".to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt"),
                created_at: now,
                task_source: Some(task_source.clone()),
            },
        )
        .expect("create project");

        milestone_controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base,
            &milestone.id,
            milestone_controller::ControllerTransitionRequest::new(
                milestone_controller::MilestoneControllerState::Running,
                "bead execution started",
            )
            .with_bead(&task_source.bead_id)
            .with_task("task-authoritative-run-binding"),
            now - chrono::Duration::seconds(1),
        )
        .expect("mark controller running");

        let authoritative_run_id =
            RunId::new("run-authoritative-first-dispatch").expect("authoritative run id");
        let journal_events = FsJournalStore
            .read_journal(base, &project_id)
            .expect("read initial journal");
        let run_started = journal::run_started_event(
            journal::last_sequence(&journal_events) + 1,
            now + chrono::Duration::seconds(2),
            &authoritative_run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_started).expect("serialize run_started"),
            )
            .expect("append run_started");
        let run_completed = journal::run_completed_event(
            run_started.sequence + 1,
            now + chrono::Duration::seconds(3),
            &authoritative_run_id,
            1,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_completed).expect("serialize run_completed"),
            )
            .expect("append run_completed");

        let mut task = sample_pending_task("task-authoritative-run-binding", &project_id);
        task.status = TaskStatus::Active;
        task.attempt_count = 1;
        let store = FailOnceWorkflowRunIdWriteStore::new();
        store.create_task(base, &task).expect("create task");

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let daemon = DaemonLoop::new(
            &store,
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

        daemon.persist_workflow_run_id(base, &task, Some(authoritative_run_id.as_str()));
        let reloaded_task = store
            .read_task(base, &task.task_id)
            .expect("read reloaded task");
        assert!(
            reloaded_task.workflow_run_id.is_none(),
            "failed persistence must leave the durable workflow_run_id unset"
        );

        let error = daemon
            .try_reconcile_success(base, &reloaded_task)
            .await
            .expect_err("missing durable workflow_run_id must fail closed");
        assert_eq!(error.0, "reconciliation_no_run_binding");
        assert!(
            error.1.contains("workflow_run_id is missing"),
            "reconciliation should explain the missing durable binding: {}",
            error.1
        );

        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &milestone.id)
                .expect("load controller")
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::NeedsOperator,
            "missing durable workflow_run_id should escalate to needs_operator"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_reconcile_failure_records_failed_milestone_attempt() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        let now = Utc::now();
        initialize_workspace(base, now).expect("init workspace");

        let milestone_id = create_failure_reconciliation_milestone(base, now);
        let task_source = TaskSource {
            milestone_id: milestone_id.to_string(),
            bead_id: "ms-daemon-failure-reconcile.bead-1".to_owned(),
            parent_epic_id: None,
            origin: TaskOrigin::Milestone,
            plan_hash: Some("plan-v1".to_owned()),
            plan_version: Some(1),
            plan_workstream_index: None,
            plan_bead_index: None,
        };
        let project_id = ProjectId::new("daemon-failure-project").expect("project id");
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Daemon failure project".to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt"),
                created_at: now,
                task_source: Some(task_source.clone()),
            },
        )
        .expect("create project");

        let started_at = now + chrono::Duration::seconds(1);
        let failed_at = now + chrono::Duration::seconds(5);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "run-1",
            "plan-v1",
            started_at,
        )
        .expect("record bead start");
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "workflow execution started",
            started_at,
        )
        .expect("mark controller running");

        let initial_events = FsJournalStore
            .read_journal(base, &project_id)
            .expect("read initial journal");
        let run_id = RunId::new("run-1").expect("run id");
        let run_started = journal::run_started_event(
            journal::last_sequence(&initial_events) + 1,
            started_at,
            &run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_started).expect("serialize run_started"),
            )
            .expect("append run_started");
        let run_failed = journal::run_failed_event(
            run_started.sequence + 1,
            failed_at,
            &run_id,
            StageId::Implementation,
            "stage_failure",
            "daemon execution failed",
            0,
            20,
            None,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_failed).expect("serialize run_failed"),
            )
            .expect("append run_failed");

        let mut task = sample_pending_task("task-daemon-failure", &project_id);
        task.status = TaskStatus::Active;
        task.workflow_run_id = Some("run-1".to_owned());

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

        let outcome = daemon
            .try_reconcile_failure(base, &task, "fallback failure message")
            .await
            .expect("failure reconciliation should succeed")
            .expect("milestone task should reconcile");
        assert_eq!(
            outcome,
            crate::contexts::automation_runtime::FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: crate::contexts::automation_runtime::MAX_FAILURE_RETRIES,
            }
        );

        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &milestone_id)
                .expect("load controller")
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Blocked
        );
        assert_eq!(
            controller.active_bead_id.as_deref(),
            Some(task_source.bead_id.as_str())
        );
        assert_eq!(
            controller.active_task_id.as_deref(),
            Some(project_id.as_str())
        );

        let task_runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
        )
        .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(task_runs[0].finished_at, Some(failed_at));

        let milestone_journal = read_journal(&FsMilestoneJournalStore, base, &milestone_id)
            .expect("read milestone journal");
        let failed_event = milestone_journal
            .iter()
            .find(|event| event.event_type == MilestoneEventType::BeadFailed)
            .expect("bead_failed should be recorded");
        let details = failed_event
            .details
            .as_deref()
            .expect("bead_failed should carry details");
        assert!(details.contains("\"task_id\":\"task-daemon-failure\""));
        assert!(details.contains("daemon execution failed"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_reconcile_failure_uses_matching_durable_attempt_metadata_when_snapshot_attempt_is_missing(
    ) {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        let now = Utc::now();
        initialize_workspace(base, now).expect("init workspace");

        let milestone_id = create_failure_reconciliation_milestone(base, now);
        let task_source = TaskSource {
            milestone_id: milestone_id.to_string(),
            bead_id: "ms-daemon-failure-reconcile.bead-1".to_owned(),
            parent_epic_id: None,
            origin: TaskOrigin::Milestone,
            plan_hash: Some("plan-v1".to_owned()),
            plan_version: Some(1),
            plan_workstream_index: None,
            plan_bead_index: None,
        };
        let project_id = ProjectId::new("daemon-failure-durable-fallback").expect("project id");
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Daemon failure durable fallback".to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt"),
                created_at: now,
                task_source: Some(task_source.clone()),
            },
        )
        .expect("create project");

        let started_at = now + chrono::Duration::seconds(1);
        let failed_at = started_at + chrono::Duration::seconds(4);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "run-durable-fallback-1",
            "plan-v1",
            started_at,
        )
        .expect("record bead start");
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "workflow execution started",
            started_at,
        )
        .expect("mark controller running");

        let initial_events = FsJournalStore
            .read_journal(base, &project_id)
            .expect("read initial journal");
        let run_id = RunId::new("run-durable-fallback-1").expect("run id");
        let run_started = journal::run_started_event(
            journal::last_sequence(&initial_events) + 1,
            started_at,
            &run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_started).expect("serialize run_started"),
            )
            .expect("append run_started");
        let run_failed = journal::run_failed_event(
            run_started.sequence + 1,
            failed_at,
            &run_id,
            StageId::Implementation,
            "stage_failure",
            "durable fallback failure",
            0,
            20,
            None,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_failed).expect("serialize run_failed"),
            )
            .expect("append run_failed");

        let mut task = sample_pending_task("task-daemon-durable-fallback", &project_id);
        task.status = TaskStatus::Active;

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

        let outcome = daemon
            .try_reconcile_failure(base, &task, "durable fallback failure")
            .await
            .expect("failure reconciliation should succeed")
            .expect("milestone task should reconcile");
        assert_eq!(
            outcome,
            crate::contexts::automation_runtime::FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: crate::contexts::automation_runtime::MAX_FAILURE_RETRIES,
            }
        );

        let task_runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
        )
        .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(
            task_runs[0].run_id.as_deref(),
            Some("run-durable-fallback-1")
        );
        assert_eq!(task_runs[0].started_at, started_at);
        assert_eq!(task_runs[0].finished_at, Some(failed_at));
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Failed);
        assert!(task_runs[0]
            .outcome_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("durable fallback failure")));

        let milestone_journal = read_journal(&FsMilestoneJournalStore, base, &milestone_id)
            .expect("read milestone journal");
        assert!(milestone_journal.iter().any(|event| {
            event.event_type == MilestoneEventType::BeadFailed
                && event
                    .details
                    .as_deref()
                    .is_some_and(|details| details.contains("durable fallback failure"))
        }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_reconcile_failure_uses_snapshot_attempt_metadata_only_with_matching_run_failed_event(
    ) {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        let now = Utc::now();
        initialize_workspace(base, now).expect("init workspace");

        let milestone_id = create_failure_reconciliation_milestone(base, now);
        let task_source = TaskSource {
            milestone_id: milestone_id.to_string(),
            bead_id: "ms-daemon-failure-reconcile.bead-1".to_owned(),
            parent_epic_id: None,
            origin: TaskOrigin::Milestone,
            plan_hash: Some("plan-v1".to_owned()),
            plan_version: Some(1),
            plan_workstream_index: None,
            plan_bead_index: None,
        };
        let project_id = ProjectId::new("daemon-failure-snapshot-fallback").expect("project id");
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Daemon failure snapshot fallback".to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt"),
                created_at: now,
                task_source: Some(task_source.clone()),
            },
        )
        .expect("create project");

        let started_at = now + chrono::Duration::seconds(1);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "run-fallback-1",
            "plan-v1",
            started_at,
        )
        .expect("record bead start");
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "workflow execution started",
            started_at,
        )
        .expect("mark controller running");

        let initial_events = FsJournalStore
            .read_journal(base, &project_id)
            .expect("read initial journal");
        let run_id = RunId::new("run-fallback-1").expect("run id");
        let run_started = journal::run_started_event(
            journal::last_sequence(&initial_events) + 1,
            started_at,
            &run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_started).expect("serialize run_started"),
            )
            .expect("append run_started");
        let failed_at = started_at + chrono::Duration::seconds(4);
        let run_failed = journal::run_failed_event(
            run_started.sequence + 1,
            failed_at,
            &run_id,
            StageId::Implementation,
            "stage_failure",
            "snapshot fallback failure",
            0,
            20,
            None,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_failed).expect("serialize run_failed"),
            )
            .expect("append run_failed");

        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-fallback-1".to_owned(),
                stage_cursor: StageCursor::initial(StageId::Implementation),
                started_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed: snapshot fallback".to_owned(),
            last_stage_resolution_snapshot: None,
        };
        FsRunSnapshotWriteStore
            .write_run_snapshot(base, &project_id, &snapshot)
            .expect("write failed snapshot");

        let mut task = sample_pending_task("task-daemon-failure-fallback", &project_id);
        task.status = TaskStatus::Active;

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

        let outcome = daemon
            .try_reconcile_failure(base, &task, "snapshot fallback failure")
            .await
            .expect("failure reconciliation should succeed")
            .expect("milestone task should reconcile");
        assert_eq!(
            outcome,
            crate::contexts::automation_runtime::FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: crate::contexts::automation_runtime::MAX_FAILURE_RETRIES,
            }
        );

        let task_runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
        )
        .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(task_runs[0].finished_at, Some(failed_at));
        assert!(task_runs[0]
            .outcome_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("snapshot fallback failure")));

        let milestone_journal = read_journal(&FsMilestoneJournalStore, base, &milestone_id)
            .expect("read milestone journal");
        let failed_event = milestone_journal
            .iter()
            .find(|event| event.event_type == MilestoneEventType::BeadFailed)
            .expect("bead_failed should be recorded");
        assert!(failed_event
            .details
            .as_deref()
            .is_some_and(|details| details.contains("snapshot fallback failure")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_reconcile_failure_accepts_resumed_same_run_with_reused_running_lineage() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        let now = Utc::now();
        initialize_workspace(base, now).expect("init workspace");

        let milestone_id = create_failure_reconciliation_milestone(base, now);
        let task_source = TaskSource {
            milestone_id: milestone_id.to_string(),
            bead_id: "ms-daemon-failure-reconcile.bead-1".to_owned(),
            parent_epic_id: None,
            origin: TaskOrigin::Milestone,
            plan_hash: Some("plan-v1".to_owned()),
            plan_version: Some(1),
            plan_workstream_index: None,
            plan_bead_index: None,
        };
        let project_id = ProjectId::new("daemon-failure-resumed-same-run").expect("project id");
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Daemon resumed same-run failure".to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt"),
                created_at: now,
                task_source: Some(task_source.clone()),
            },
        )
        .expect("create project");

        let original_started_at = now + chrono::Duration::seconds(1);
        let resumed_at = now + chrono::Duration::seconds(20);
        let failed_at = resumed_at + chrono::Duration::seconds(10);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "run-resumed-1",
            "plan-v1",
            original_started_at,
        )
        .expect("record initial bead start");
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "workflow execution resumed",
            resumed_at,
        )
        .expect("mark controller running");

        let initial_events = FsJournalStore
            .read_journal(base, &project_id)
            .expect("read initial journal");
        let run_id = RunId::new("run-resumed-1").expect("run id");
        let run_started = journal::run_started_event(
            journal::last_sequence(&initial_events) + 1,
            original_started_at,
            &run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_started).expect("serialize run_started"),
            )
            .expect("append run_started");
        let run_resumed = journal::run_resumed_event(
            run_started.sequence + 1,
            resumed_at,
            &run_id,
            StageId::Implementation,
            2,
            1,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_resumed).expect("serialize run_resumed"),
            )
            .expect("append run_resumed");
        let run_failed = journal::run_failed_event(
            run_resumed.sequence + 1,
            failed_at,
            &run_id,
            StageId::Implementation,
            "stage_failure",
            "resumed attempt failed after pause",
            1,
            20,
            None,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_failed).expect("serialize run_failed"),
            )
            .expect("append run_failed");

        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-resumed-1".to_owned(),
                stage_cursor: StageCursor::initial(StageId::Implementation),
                started_at: resumed_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "resumed attempt failed after pause".to_owned(),
            last_stage_resolution_snapshot: None,
        };
        FsRunSnapshotWriteStore
            .write_run_snapshot(base, &project_id, &snapshot)
            .expect("write failed snapshot");

        let mut task = sample_pending_task("task-daemon-failure-resumed", &project_id);
        task.status = TaskStatus::Active;
        task.workflow_run_id = Some("run-resumed-1".to_owned());

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

        let outcome = daemon
            .try_reconcile_failure(base, &task, "fallback failure message")
            .await
            .expect("resumed same-run failure should reconcile")
            .expect("milestone task should reconcile");
        assert_eq!(
            outcome,
            crate::contexts::automation_runtime::FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: crate::contexts::automation_runtime::MAX_FAILURE_RETRIES,
            }
        );

        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &milestone_id)
                .expect("load controller")
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Blocked
        );

        let task_runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
        )
        .expect("read task-runs");
        assert_eq!(task_runs.len(), 1);
        assert_eq!(task_runs[0].run_id.as_deref(), Some("run-resumed-1"));
        assert_eq!(task_runs[0].started_at, original_started_at);
        assert_eq!(task_runs[0].finished_at, Some(failed_at));
        assert_eq!(task_runs[0].outcome, TaskRunOutcome::Failed);
        assert!(task_runs[0]
            .outcome_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("resumed attempt failed after pause")));

        let milestone_journal = read_journal(&FsMilestoneJournalStore, base, &milestone_id)
            .expect("read milestone journal");
        assert!(milestone_journal.iter().any(|event| {
            event.event_type == MilestoneEventType::BeadFailed
                && event
                    .details
                    .as_deref()
                    .is_some_and(|details| details.contains("resumed attempt failed after pause"))
        }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_reconcile_failure_repairs_missing_same_run_retry_lineage_before_recording_failure()
    {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        let now = Utc::now();
        initialize_workspace(base, now).expect("init workspace");

        let milestone_id = create_failure_reconciliation_milestone(base, now);
        let task_source = TaskSource {
            milestone_id: milestone_id.to_string(),
            bead_id: "ms-daemon-failure-reconcile.bead-1".to_owned(),
            parent_epic_id: None,
            origin: TaskOrigin::Milestone,
            plan_hash: Some("plan-v1".to_owned()),
            plan_version: Some(1),
            plan_workstream_index: None,
            plan_bead_index: None,
        };
        let project_id =
            ProjectId::new("daemon-failure-missing-same-run-lineage").expect("project id");
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Daemon missing same-run retry lineage".to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt"),
                created_at: now,
                task_source: Some(task_source.clone()),
            },
        )
        .expect("create project");

        let original_started_at = now + chrono::Duration::seconds(1);
        let original_failed_at = original_started_at + chrono::Duration::seconds(4);
        let resumed_at = now + chrono::Duration::seconds(20);
        let resumed_failed_at = resumed_at + chrono::Duration::seconds(5);

        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "run-1",
            "plan-v1",
            original_started_at,
        )
        .expect("record original bead start");
        milestone_service::record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "run-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("original attempt failed"),
            original_started_at,
            original_failed_at,
        )
        .expect("record original failure");
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "workflow execution resumed",
            resumed_at,
        )
        .expect("mark controller running");

        let initial_events = FsJournalStore
            .read_journal(base, &project_id)
            .expect("read initial journal");
        let run_id = RunId::new("run-1").expect("run id");
        let original_started = journal::run_started_event(
            journal::last_sequence(&initial_events) + 1,
            original_started_at,
            &run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&original_started).expect("serialize original start"),
            )
            .expect("append original run_started");
        let original_failed = journal::run_failed_event(
            original_started.sequence + 1,
            original_failed_at,
            &run_id,
            StageId::Implementation,
            "stage_failure",
            "original attempt failed",
            0,
            20,
            None,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&original_failed).expect("serialize original failed"),
            )
            .expect("append original run_failed");
        let resumed_event = journal::run_resumed_event(
            original_failed.sequence + 1,
            resumed_at,
            &run_id,
            StageId::Implementation,
            2,
            1,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&resumed_event).expect("serialize run_resumed"),
            )
            .expect("append run_resumed");
        let resumed_failed = journal::run_failed_event(
            resumed_event.sequence + 1,
            resumed_failed_at,
            &run_id,
            StageId::Implementation,
            "stage_failure",
            "resumed attempt failed after missing lineage",
            1,
            20,
            None,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&resumed_failed).expect("serialize resumed failed"),
            )
            .expect("append resumed run_failed");

        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-1".to_owned(),
                stage_cursor: StageCursor::new(StageId::Implementation, 2, 1, 1)
                    .expect("stage cursor"),
                started_at: resumed_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "resumed attempt failed after missing lineage".to_owned(),
            last_stage_resolution_snapshot: None,
        };
        FsRunSnapshotWriteStore
            .write_run_snapshot(base, &project_id, &snapshot)
            .expect("write failed snapshot");

        let mut task =
            sample_pending_task("task-daemon-failure-missing-same-run-lineage", &project_id);
        task.status = TaskStatus::Active;
        task.workflow_run_id = Some("run-1".to_owned());

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

        let outcome = daemon
            .try_reconcile_failure(base, &task, "fallback failure message")
            .await
            .expect("failure reconciliation should succeed")
            .expect("milestone task should reconcile");
        assert_eq!(
            outcome,
            crate::contexts::automation_runtime::FailureReconciliationOutcome::Retryable {
                attempt_number: 2,
                max_retries: crate::contexts::automation_runtime::MAX_FAILURE_RETRIES,
            }
        );

        let task_runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
        )
        .expect("read task-runs");
        assert_eq!(task_runs.len(), 2);
        let resumed_attempt = task_runs
            .iter()
            .find(|entry| {
                entry.run_id.as_deref() == Some("run-1") && entry.started_at == resumed_at
            })
            .expect("resumed failed attempt should remain present");
        assert_eq!(resumed_attempt.finished_at, Some(resumed_failed_at));
        assert_eq!(resumed_attempt.outcome, TaskRunOutcome::Failed);

        let failed_events = read_journal(&FsMilestoneJournalStore, base, &milestone_id)
            .expect("read milestone journal")
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .collect::<Vec<_>>();
        assert_eq!(failed_events.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_reconcile_failure_rejects_stale_run_failed_from_prior_resumed_attempt() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        let now = Utc::now();
        initialize_workspace(base, now).expect("init workspace");

        let milestone_id = create_failure_reconciliation_milestone(base, now);
        let task_source = TaskSource {
            milestone_id: milestone_id.to_string(),
            bead_id: "ms-daemon-failure-reconcile.bead-1".to_owned(),
            parent_epic_id: None,
            origin: TaskOrigin::Milestone,
            plan_hash: Some("plan-v1".to_owned()),
            plan_version: Some(1),
            plan_workstream_index: None,
            plan_bead_index: None,
        };
        let project_id = ProjectId::new("daemon-failure-stale-run-failed").expect("project id");
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Daemon stale run_failed rejection".to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt"),
                created_at: now,
                task_source: Some(task_source.clone()),
            },
        )
        .expect("create project");

        let first_started_at = now + chrono::Duration::seconds(1);
        let first_failed_at = first_started_at + chrono::Duration::seconds(4);
        let resumed_at = now + chrono::Duration::seconds(20);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "run-resumed-1",
            "plan-v1",
            first_started_at,
        )
        .expect("record initial bead start");
        milestone_service::record_bead_completion(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "run-resumed-1",
            Some("plan-v1"),
            TaskRunOutcome::Failed,
            Some("first attempt failed"),
            first_started_at,
            first_failed_at,
        )
        .expect("record first failed completion");
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "run-resumed-1",
            "plan-v1",
            resumed_at,
        )
        .expect("record resumed bead start");
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "workflow execution resumed",
            resumed_at,
        )
        .expect("mark controller running");

        let initial_events = FsJournalStore
            .read_journal(base, &project_id)
            .expect("read initial journal");
        let run_id = RunId::new("run-resumed-1").expect("run id");
        let run_started = journal::run_started_event(
            journal::last_sequence(&initial_events) + 1,
            first_started_at,
            &run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_started).expect("serialize run_started"),
            )
            .expect("append run_started");
        let run_failed = journal::run_failed_event(
            run_started.sequence + 1,
            first_failed_at,
            &run_id,
            StageId::Implementation,
            "stage_failure",
            "first attempt failed",
            0,
            20,
            None,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_failed).expect("serialize run_failed"),
            )
            .expect("append first run_failed");
        let run_resumed = journal::run_resumed_event(
            run_failed.sequence + 1,
            resumed_at,
            &run_id,
            StageId::Implementation,
            2,
            1,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&run_resumed).expect("serialize run_resumed"),
            )
            .expect("append run_resumed");

        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-resumed-1".to_owned(),
                stage_cursor: StageCursor::initial(StageId::Implementation),
                started_at: resumed_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "resumed attempt failed without a new durable run_failed event"
                .to_owned(),
            last_stage_resolution_snapshot: None,
        };
        FsRunSnapshotWriteStore
            .write_run_snapshot(base, &project_id, &snapshot)
            .expect("write failed snapshot");

        let mut task = sample_pending_task("task-daemon-failure-stale-run-failed", &project_id);
        task.status = TaskStatus::Active;
        task.workflow_run_id = Some("run-resumed-1".to_owned());

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
            .try_reconcile_failure(base, &task, "fallback failure message")
            .await
            .expect_err("stale prior run_failed must not satisfy resumed attempt reconciliation");
        assert_eq!(error.0, "reconciliation_metadata_error");
        assert!(error.1.contains("durable run_failed event"));
        assert!(error.1.contains(&resumed_at.to_rfc3339()));

        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &milestone_id)
                .expect("load controller")
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::NeedsOperator
        );

        let task_runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
        )
        .expect("read task-runs");
        assert_eq!(task_runs.len(), 2);
        let resumed_attempt = task_runs
            .iter()
            .find(|entry| entry.started_at == resumed_at)
            .expect("resumed running attempt should remain present");
        assert_eq!(resumed_attempt.outcome, TaskRunOutcome::Running);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_reconcile_failure_rejects_stale_bound_attempt_after_newer_durable_attempt_exists()
    {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        let now = Utc::now();
        initialize_workspace(base, now).expect("init workspace");

        let milestone_id = create_failure_reconciliation_milestone(base, now);
        let task_source = TaskSource {
            milestone_id: milestone_id.to_string(),
            bead_id: "ms-daemon-failure-reconcile.bead-1".to_owned(),
            parent_epic_id: None,
            origin: TaskOrigin::Milestone,
            plan_hash: Some("plan-v1".to_owned()),
            plan_version: Some(1),
            plan_workstream_index: None,
            plan_bead_index: None,
        };
        let project_id = ProjectId::new("daemon-failure-stale-bound-attempt").expect("project id");
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Daemon failure stale bound attempt".to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt"),
                created_at: now,
                task_source: Some(task_source.clone()),
            },
        )
        .expect("create project");

        let original_started_at = now + chrono::Duration::seconds(1);
        let original_failed_at = original_started_at + chrono::Duration::seconds(4);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "run-original-attempt",
            "plan-v1",
            original_started_at,
        )
        .expect("record original bead start");
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "workflow execution started",
            original_started_at,
        )
        .expect("mark controller running");

        let initial_events = FsJournalStore
            .read_journal(base, &project_id)
            .expect("read initial journal");
        let original_run_id = RunId::new("run-original-attempt").expect("run id");
        let original_started = journal::run_started_event(
            journal::last_sequence(&initial_events) + 1,
            original_started_at,
            &original_run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&original_started).expect("serialize original start"),
            )
            .expect("append original run_started");
        let original_failed = journal::run_failed_event(
            original_started.sequence + 1,
            original_failed_at,
            &original_run_id,
            StageId::Implementation,
            "stage_failure",
            "original attempt failed",
            0,
            20,
            None,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&original_failed).expect("serialize original failed"),
            )
            .expect("append original run_failed");

        let newer_started_at = now + chrono::Duration::seconds(20);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "run-newer-attempt",
            "plan-v1",
            newer_started_at,
        )
        .expect("record newer bead start");
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "workflow execution restarted",
            newer_started_at,
        )
        .expect("mark controller running for newer attempt");
        let newer_run_id = RunId::new("run-newer-attempt").expect("newer run id");
        let newer_started = journal::run_started_event(
            original_failed.sequence + 1,
            newer_started_at,
            &newer_run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&newer_started).expect("serialize newer start"),
            )
            .expect("append newer run_started");

        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-newer-attempt".to_owned(),
                stage_cursor: StageCursor::initial(StageId::Implementation),
                started_at: newer_started_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "newer attempt failed while the task still points at the old run"
                .to_owned(),
            last_stage_resolution_snapshot: None,
        };
        FsRunSnapshotWriteStore
            .write_run_snapshot(base, &project_id, &snapshot)
            .expect("write failed snapshot");

        let mut task = sample_pending_task("task-daemon-failure-stale-bound", &project_id);
        task.status = TaskStatus::Active;
        task.workflow_run_id = Some("run-original-attempt".to_owned());

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
            .try_reconcile_failure(base, &task, "fallback failure message")
            .await
            .expect_err("stale bound workflow_run_id must fail closed");
        assert_eq!(error.0, "reconciliation_metadata_error");
        assert!(
            error
                .1
                .contains("newest durable project journal attempt is run_id=run-newer-attempt"),
            "error should explain the newer durable attempt mismatch: {}",
            error.1
        );

        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &milestone_id)
                .expect("load controller")
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::NeedsOperator
        );

        let milestone_journal = read_journal(&FsMilestoneJournalStore, base, &milestone_id)
            .expect("read milestone journal");
        assert!(
            !milestone_journal
                .iter()
                .any(|event| event.event_type == MilestoneEventType::BeadFailed),
            "stale bound attempt must not reconcile the older durable run_failed event"
        );

        let task_runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
        )
        .expect("read task-runs");
        assert_eq!(task_runs.len(), 2);
        assert!(task_runs.iter().any(|entry| {
            entry.run_id.as_deref() == Some("run-newer-attempt")
                && entry.started_at == newer_started_at
                && entry.outcome == TaskRunOutcome::Running
        }));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_reconcile_failure_requires_exact_attempt_binding_before_reconciling() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        let now = Utc::now();
        initialize_workspace(base, now).expect("init workspace");

        let milestone_id = create_failure_reconciliation_milestone(base, now);
        let task_source = TaskSource {
            milestone_id: milestone_id.to_string(),
            bead_id: "ms-daemon-failure-reconcile.bead-1".to_owned(),
            parent_epic_id: None,
            origin: TaskOrigin::Milestone,
            plan_hash: Some("plan-v1".to_owned()),
            plan_version: Some(1),
            plan_workstream_index: None,
            plan_bead_index: None,
        };
        let project_id = ProjectId::new("daemon-failure-missing-binding").expect("project id");
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Daemon failure missing binding".to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt"),
                created_at: now,
                task_source: Some(task_source.clone()),
            },
        )
        .expect("create project");

        let started_at = now + chrono::Duration::seconds(1);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "run-original-attempt",
            "plan-v1",
            started_at,
        )
        .expect("record bead start");
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "workflow execution started",
            started_at,
        )
        .expect("mark controller running");

        let initial_events = FsJournalStore
            .read_journal(base, &project_id)
            .expect("read initial journal");
        let original_run_id = RunId::new("run-original-attempt").expect("run id");
        let original_started = journal::run_started_event(
            journal::last_sequence(&initial_events) + 1,
            started_at,
            &original_run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&original_started).expect("serialize run_started"),
            )
            .expect("append original run_started");
        let newer_run_id = RunId::new("run-newer-manual-rerun").expect("newer run id");
        let newer_started = journal::run_started_event(
            original_started.sequence + 1,
            now + chrono::Duration::seconds(20),
            &newer_run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&newer_started).expect("serialize newer run_started"),
            )
            .expect("append newer run_started");

        let mut task = sample_pending_task("task-daemon-failure-missing-binding", &project_id);
        task.status = TaskStatus::Active;

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
            .try_reconcile_failure(base, &task, "fallback failure message")
            .await
            .expect_err("missing exact run binding must fail closed");
        assert_eq!(error.0, "reconciliation_no_run_binding");
        assert!(
            error
                .1
                .contains("run snapshot has no exact active/interrupted attempt"),
            "error should explain the missing attempt binding: {}",
            error.1
        );
        assert!(
            error.1.contains("Operator intervention required"),
            "error should require operator action: {}",
            error.1
        );

        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &milestone_id)
                .expect("load controller")
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::NeedsOperator
        );
        assert_eq!(
            controller.active_bead_id.as_deref(),
            Some(task_source.bead_id.as_str())
        );
        assert_eq!(
            controller.active_task_id.as_deref(),
            Some(project_id.as_str())
        );

        let milestone_journal = read_journal(&FsMilestoneJournalStore, base, &milestone_id)
            .expect("read milestone journal");
        assert!(
            !milestone_journal
                .iter()
                .any(|event| event.event_type == MilestoneEventType::BeadFailed),
            "failure reconciliation must not invent a failed attempt from newer journal metadata"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn try_reconcile_failure_snapshot_fallback_rejects_stale_attempt_when_newer_journal_attempt_exists(
    ) {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        let now = Utc::now();
        initialize_workspace(base, now).expect("init workspace");

        let milestone_id = create_failure_reconciliation_milestone(base, now);
        let task_source = TaskSource {
            milestone_id: milestone_id.to_string(),
            bead_id: "ms-daemon-failure-reconcile.bead-1".to_owned(),
            parent_epic_id: None,
            origin: TaskOrigin::Milestone,
            plan_hash: Some("plan-v1".to_owned()),
            plan_version: Some(1),
            plan_workstream_index: None,
            plan_bead_index: None,
        };
        let project_id =
            ProjectId::new("daemon-failure-stale-snapshot-fallback").expect("project id");
        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: project_id.clone(),
                name: "Daemon failure stale snapshot fallback".to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt"),
                created_at: now,
                task_source: Some(task_source.clone()),
            },
        )
        .expect("create project");

        let original_started_at = now + chrono::Duration::seconds(1);
        record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "run-original-attempt",
            "plan-v1",
            original_started_at,
        )
        .expect("record bead start");
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
            &task_source.bead_id,
            project_id.as_str(),
            "workflow execution started",
            original_started_at,
        )
        .expect("mark controller running");

        let initial_events = FsJournalStore
            .read_journal(base, &project_id)
            .expect("read initial journal");
        let original_run_id = RunId::new("run-original-attempt").expect("run id");
        let original_started = journal::run_started_event(
            journal::last_sequence(&initial_events) + 1,
            original_started_at,
            &original_run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&original_started).expect("serialize original start"),
            )
            .expect("append original run_started");
        let newer_run_id = RunId::new("run-newer-attempt").expect("newer run id");
        let newer_started_at = now + chrono::Duration::seconds(20);
        let newer_started = journal::run_started_event(
            original_started.sequence + 1,
            newer_started_at,
            &newer_run_id,
            StageId::Implementation,
            20,
        );
        FsJournalStore
            .append_event(
                base,
                &project_id,
                &journal::serialize_event(&newer_started).expect("serialize newer start"),
            )
            .expect("append newer run_started");

        let snapshot = RunSnapshot {
            active_run: None,
            interrupted_run: Some(ActiveRun {
                run_id: "run-original-attempt".to_owned(),
                stage_cursor: StageCursor::initial(StageId::Implementation),
                started_at: original_started_at,
                prompt_hash_at_cycle_start: "hash".to_owned(),
                prompt_hash_at_stage_start: "hash".to_owned(),
                qa_iterations_current_cycle: 0,
                review_iterations_current_cycle: 0,
                final_review_restart_count: 0,
                iterative_implementer_state: None,
                stage_resolution_snapshot: None,
            }),
            status: RunStatus::Failed,
            cycle_history: Vec::new(),
            completion_rounds: 1,
            max_completion_rounds: Some(20),
            rollback_point_meta: Default::default(),
            amendment_queue: Default::default(),
            status_summary: "failed: stale snapshot fallback".to_owned(),
            last_stage_resolution_snapshot: None,
        };
        FsRunSnapshotWriteStore
            .write_run_snapshot(base, &project_id, &snapshot)
            .expect("write failed snapshot");

        let mut task = sample_pending_task("task-daemon-failure-stale-snapshot", &project_id);
        task.status = TaskStatus::Active;

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
            .try_reconcile_failure(base, &task, "snapshot fallback failure")
            .await
            .expect_err("stale snapshot fallback must fail closed");
        assert_eq!(error.0, "reconciliation_no_run_binding");
        assert!(
            error
                .1
                .contains("does not match the newest durable project journal attempt"),
            "error should explain the stale snapshot mismatch: {}",
            error.1
        );

        let controller =
            milestone_controller::load_controller(&FsMilestoneControllerStore, base, &milestone_id)
                .expect("load controller")
                .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::NeedsOperator
        );

        let milestone_journal = read_journal(&FsMilestoneJournalStore, base, &milestone_id)
            .expect("read milestone journal");
        assert!(
            !milestone_journal
                .iter()
                .any(|event| event.event_type == MilestoneEventType::BeadFailed),
            "stale snapshot fallback must not reconcile the older attempt"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handle_requirements_quick_skips_review_when_not_requested() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");

        let project_id = ProjectId::new("quick-no-review".to_owned()).expect("project id");
        let mut task = sample_pending_task("task-quick-no-review", &project_id);
        task.dispatch_mode = DispatchMode::RequirementsQuick;
        task.prompt = Some("/rb requirements quick\n\nShip the bootstrap flow.".to_owned());

        let store = FsDaemonStore;
        store.create_task(base, &task).expect("create task");

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let effective_config =
            crate::contexts::workspace_governance::config::EffectiveConfig::load(base)
                .expect("load config");
        let daemon = DaemonLoop::new(
            &store,
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
        .with_requirements_store(&FsRequirementsStore)
        .with_configured_requirements_service_builder(Box::new(|effective_config| {
            crate::composition::agent_execution_builder::build_requirements_service_for_selector(
                "stub",
                effective_config,
            )
        }));

        daemon
            .handle_requirements_quick(base, base, &task, &effective_config)
            .await
            .expect("handle requirements quick");

        let updated = store
            .read_task(base, &task.task_id)
            .expect("read updated task");
        let run_id = updated
            .requirements_run_id
            .as_deref()
            .expect("requirements run id");
        let run = FsRequirementsStore
            .read_run(base, run_id)
            .expect("read requirements run");
        let journal = FsRequirementsStore
            .read_journal(base, run_id)
            .expect("read requirements journal");

        assert_eq!(run.status, RequirementsStatus::Completed);
        assert!(
            run.latest_review_id.is_none(),
            "default daemon requirements_quick should skip review"
        );
        assert!(
            !journal
                .iter()
                .any(|entry| entry.event_type == RequirementsJournalEventType::ReviewCompleted),
            "default daemon requirements_quick should not journal a review"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn handle_requirements_quick_honors_enable_review_command() {
        let temp = tempdir().expect("tempdir");
        let base = temp.path();
        initialize_workspace(base, Utc::now()).expect("init workspace");

        let project_id = ProjectId::new("quick-review".to_owned()).expect("project id");
        let mut task = sample_pending_task("task-quick-review", &project_id);
        task.dispatch_mode = DispatchMode::RequirementsQuick;
        task.prompt = Some("/rb requirements quick\n\nShip the bootstrap flow.".to_owned());
        task.routing_command = Some("/rb requirements quick --enable-review".to_owned());

        let store = FsDaemonStore;
        store.create_task(base, &task).expect("create task");

        let agent_service = AgentExecutionService::new(
            StubBackendAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let effective_config =
            crate::contexts::workspace_governance::config::EffectiveConfig::load(base)
                .expect("load config");
        let daemon = DaemonLoop::new(
            &store,
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
        .with_requirements_store(&FsRequirementsStore)
        .with_configured_requirements_service_builder(Box::new(|effective_config| {
            crate::composition::agent_execution_builder::build_requirements_service_for_selector(
                "stub",
                effective_config,
            )
        }));

        daemon
            .handle_requirements_quick(base, base, &task, &effective_config)
            .await
            .expect("handle requirements quick");

        let updated = store
            .read_task(base, &task.task_id)
            .expect("read updated task");
        let run_id = updated
            .requirements_run_id
            .as_deref()
            .expect("requirements run id");
        let run = FsRequirementsStore
            .read_run(base, run_id)
            .expect("read requirements run");
        let journal = FsRequirementsStore
            .read_journal(base, run_id)
            .expect("read requirements journal");

        assert_eq!(run.status, RequirementsStatus::Completed);
        assert!(
            run.latest_review_id.is_some(),
            "enable-review should preserve the requirements review pass"
        );
        assert!(
            journal
                .iter()
                .any(|entry| entry.event_type == RequirementsJournalEventType::ReviewCompleted),
            "enable-review should journal the review completion event"
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
