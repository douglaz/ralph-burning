use std::path::Path;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use crate::adapters::fs::FileSystem;
use crate::contexts::agent_execution::model::CancellationToken;
use crate::contexts::agent_execution::service::{AgentExecutionPort, RawOutputPort};
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::AgentExecutionService;
use crate::contexts::automation_runtime::lease_service::LeaseService;
use crate::contexts::automation_runtime::model::{DaemonTask, DispatchMode, TaskStatus};
use crate::contexts::automation_runtime::routing::RoutingEngine;
use crate::contexts::automation_runtime::task_service::DaemonTaskService;
use crate::contexts::automation_runtime::watcher::{self, IssueWatcherPort};
use crate::contexts::automation_runtime::{DaemonStorePort, WorktreePort};
use crate::contexts::project_run_record::model::RunStatus;
use crate::contexts::project_run_record::service::{
    create_project, AmendmentQueuePort, ArtifactStorePort, JournalStorePort,
    PayloadArtifactWritePort, ProjectStorePort, RunSnapshotPort, RunSnapshotWritePort,
    RuntimeLogWritePort,
};
use crate::contexts::project_run_record::CreateProjectInput;
use crate::contexts::requirements_drafting::service::{
    self as req_service, RequirementsStorePort,
};
use crate::contexts::workflow_composition::engine;
use crate::contexts::workflow_composition::retry_policy::RetryPolicy;
use crate::contexts::workspace_governance::config::EffectiveConfig;
use crate::shared::domain::{FlowPreset, ProjectId};
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
}

impl<A, R, S> DaemonLoop<'_, A, R, S>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
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
                self.cleanup_active_leases(base_dir)?;
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
                    self.cleanup_active_leases(base_dir)?;
                    break;
                }
                _ = tokio::time::sleep(config.poll_interval) => {}
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
        self.check_waiting_tasks(base_dir)?;

        // Phase 3: Process next pending task
        let pending_task = DaemonTaskService::list_tasks(self.store, base_dir)?
            .into_iter()
            .find(|task| task.status == TaskStatus::Pending);
        let Some(task) = pending_task else {
            return Ok(false);
        };

        self.process_task(base_dir, &task, config, shutdown).await?;
        Ok(true)
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
                    println!(
                        "watcher: skipping issue '{}': {}",
                        issue.issue_ref, e
                    );
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
    /// linked requirements run has completed.
    fn check_waiting_tasks(&self, base_dir: &Path) -> AppResult<()> {
        let Some(req_store) = self.requirements_store else {
            return Ok(());
        };

        let tasks = DaemonTaskService::list_tasks(self.store, base_dir)?;
        for task in tasks {
            if task.status != TaskStatus::WaitingForRequirements {
                continue;
            }
            let Some(ref run_id) = task.requirements_run_id else {
                continue;
            };

            match req_service::is_requirements_run_complete(req_store, base_dir, run_id) {
                Ok(true) => {
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
                        }
                        Err(e) => {
                            println!(
                                "daemon: failed to resume task '{}': {}",
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

        Ok(())
    }

    async fn process_task(
        &self,
        base_dir: &Path,
        task: &DaemonTask,
        config: &DaemonLoopConfig,
        shutdown: CancellationToken,
    ) -> AppResult<()> {
        let effective_config = EffectiveConfig::load(base_dir)?;
        let default_flow = effective_config.default_flow();
        let repo_root = base_dir;

        // For requirements dispatch modes, handle before claiming lease/worktree
        match task.dispatch_mode {
            DispatchMode::RequirementsQuick => {
                return self
                    .handle_requirements_quick(base_dir, task)
                    .await;
            }
            DispatchMode::RequirementsDraft => {
                return self
                    .handle_requirements_draft(base_dir, task)
                    .await;
            }
            DispatchMode::Workflow => {
                // Fall through to standard workflow dispatch
            }
        }

        let (claimed_task, lease) = match DaemonTaskService::claim_task(
            self.store,
            self.worktree,
            &self.routing_engine,
            base_dir,
            repo_root,
            &task.task_id,
            default_flow,
            config.lease_ttl.as_secs(),
        ) {
            Ok(value) => value,
            Err(AppError::ProjectWriterLockHeld { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };

        println!("claimed task {}", claimed_task.task_id);

        if let Err(error) = self.worktree.rebase_onto_default_branch(
            repo_root,
            &lease.worktree_path,
            &lease.branch_name,
        ) {
            let _ = self.fail_claimed_task(
                base_dir,
                repo_root,
                &claimed_task.task_id,
                &lease,
                "rebase_conflict",
                &error.to_string(),
            );
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
                &active_task,
                &lease,
                &effective_config,
                config,
                shutdown,
                task_cancel,
            )
            .await;

        let latest_task = self.store.read_task(base_dir, &active_task.task_id)?;
        if latest_task.status == TaskStatus::Aborted {
            let _ = self.release_task_lease(base_dir, repo_root, &active_task.task_id, &lease);
            return Ok(());
        }

        match outcome {
            Ok(()) => {
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
    /// run ID to the task, derive seed, create project, then continue through
    /// workflow execution in the same cycle.
    async fn handle_requirements_quick(
        &self,
        base_dir: &Path,
        task: &DaemonTask,
    ) -> AppResult<()> {
        let req_store = self.requirements_store.ok_or_else(|| {
            AppError::RequirementsHandoffFailed {
                task_id: task.task_id.clone(),
                details: "no requirements store configured for daemon".to_owned(),
            }
        })?;

        let idea = task.prompt.clone().unwrap_or_else(|| {
            format!("Automated task for issue {}", task.issue_ref)
        });

        // Build a fresh requirements service (same pattern as CLI)
        let req_svc = build_requirements_service();
        let run_id = match req_svc.quick(base_dir, &idea, Utc::now()).await {
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

        // Link requirements run to task
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

        // Derive seed and create project from completed requirements run
        let handoff = match req_service::extract_seed_handoff(req_store, base_dir, &run_id) {
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
        if handoff.flow != routed_flow {
            println!(
                "daemon: seed suggests flow '{}' but routed flow '{}' is authoritative",
                handoff.flow.as_str(),
                routed_flow.as_str()
            );
        }

        // Update task to workflow mode for continuation
        let mut updated = self.store.read_task(base_dir, &task.task_id)?;
        updated.dispatch_mode = DispatchMode::Workflow;
        updated.project_id = handoff.project_id.clone();
        updated.project_name = Some(handoff.project_name.clone());
        updated.prompt = Some(handoff.prompt_body.clone());
        self.store.write_task(base_dir, &updated)?;

        // Mark task completed — the seed and requirements are done.
        // The project can be started separately via the normal workflow path.
        let _ = DaemonTaskService::mark_completed(self.store, base_dir, &task.task_id);
        println!(
            "daemon: requirements_quick completed for task '{}', run_id='{}'",
            task.task_id, run_id
        );

        Ok(())
    }

    /// Handle requirements_draft dispatch: invoke requirements draft to generate
    /// questions, then transition to WaitingForRequirements state.
    async fn handle_requirements_draft(
        &self,
        base_dir: &Path,
        task: &DaemonTask,
    ) -> AppResult<()> {
        let _req_store = self.requirements_store.ok_or_else(|| {
            AppError::RequirementsHandoffFailed {
                task_id: task.task_id.clone(),
                details: "no requirements store configured for daemon".to_owned(),
            }
        })?;

        let idea = task.prompt.clone().unwrap_or_else(|| {
            format!("Automated task for issue {}", task.issue_ref)
        });

        let req_svc = build_requirements_service();
        let run_id = match req_svc.draft(base_dir, &idea, Utc::now()).await {
            Ok(run_id) => run_id,
            Err(e) => {
                let _ = DaemonTaskService::mark_failed(
                    self.store,
                    base_dir,
                    &task.task_id,
                    "requirements_draft_failed",
                    &e.to_string(),
                );
                return Err(e);
            }
        };

        // Transition to waiting state — release all resources
        match DaemonTaskService::mark_waiting_for_requirements(
            self.store,
            base_dir,
            &task.task_id,
            &run_id,
        ) {
            Ok(_) => {
                DaemonTaskService::append_journal_event(
                    self.store,
                    base_dir,
                    super::model::DaemonJournalEventType::RequirementsHandoff,
                    json!({
                        "task_id": task.task_id,
                        "requirements_run_id": run_id,
                        "dispatch_mode": "requirements_draft",
                    }),
                )?;
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
                return Err(e);
            }
        }

        Ok(())
    }

    async fn drive_dispatch(
        &self,
        base_dir: &Path,
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
            .read_run_snapshot(base_dir, &project_id)?;
        let dispatch_future = self.dispatch_in_worktree(
            base_dir,
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
        let original_dir = std::env::current_dir()?;
        std::env::set_current_dir(worktree_path)?;

        let result = match run_status {
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
        };

        let reset_result = std::env::set_current_dir(original_dir);
        match (result, reset_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(error)) => Err(error.into()),
            (Err(error), Err(_)) => Err(error),
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

    fn cleanup_active_leases(&self, base_dir: &Path) -> AppResult<()> {
        let leases = self.store.list_leases(base_dir)?;
        for lease in leases {
            let _ = DaemonTaskService::mark_aborted(self.store, base_dir, &lease.task_id);
            let _ = self.release_task_lease(base_dir, base_dir, &lease.task_id, &lease);
        }
        Ok(())
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
        let release_result =
            LeaseService::release(self.store, self.worktree, base_dir, repo_root, lease);
        let clear_result =
            DaemonTaskService::clear_lease_reference(self.store, base_dir, task_id).map(|_| ());

        match (release_result, clear_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) => Err(error),
            (Ok(()), Err(error)) => Err(error),
            (Err(error), Err(_)) => Err(error),
        }
    }
}

/// Build a requirements service for daemon-initiated requirements runs.
/// Uses the same stub backend and filesystem stores as the CLI.
fn build_requirements_service(
) -> crate::contexts::requirements_drafting::service::RequirementsService<
    crate::adapters::stub_backend::StubBackendAdapter,
    crate::adapters::fs::FsRawOutputStore,
    crate::adapters::fs::FsSessionStore,
    crate::adapters::fs::FsRequirementsStore,
> {
    use crate::adapters::fs::{FsRawOutputStore, FsRequirementsStore, FsSessionStore};
    use crate::adapters::stub_backend::StubBackendAdapter;
    use crate::contexts::agent_execution::AgentExecutionService;
    use crate::contexts::requirements_drafting::service::RequirementsService;

    let adapter = StubBackendAdapter::default();
    let raw_output_store = FsRawOutputStore;
    let session_store = FsSessionStore;
    let agent_service = AgentExecutionService::new(adapter, raw_output_store, session_store);
    let requirements_store = FsRequirementsStore;
    RequirementsService::new(agent_service, requirements_store)
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
