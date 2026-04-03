use std::path::Path;

use chrono::Utc;
use serde_json::json;

use crate::contexts::automation_runtime::lease_service::{LeaseService, ReleaseMode};
use crate::contexts::automation_runtime::model::{
    DaemonJournalEvent, DaemonJournalEventType, DaemonTask, DispatchMode, TaskStatus,
    WatchedIssueMeta, WorktreeLease,
};
use crate::contexts::automation_runtime::routing::RoutingEngine;
use crate::contexts::automation_runtime::{DaemonStorePort, WorktreePort};
use crate::shared::domain::{FlowPreset, ProjectId};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTaskInput {
    pub task_id: String,
    pub issue_ref: String,
    pub project_id: String,
    pub project_name: Option<String>,
    pub prompt: Option<String>,
    pub routing_command: Option<String>,
    pub routing_labels: Vec<String>,
    pub dispatch_mode: DispatchMode,
    pub source_revision: Option<String>,
}

pub struct DaemonTaskService;

impl DaemonTaskService {
    pub fn list_tasks(store: &dyn DaemonStorePort, base_dir: &Path) -> AppResult<Vec<DaemonTask>> {
        store.list_tasks(base_dir)
    }

    pub fn create_task(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        routing_engine: &RoutingEngine,
        default_flow: FlowPreset,
        input: CreateTaskInput,
    ) -> AppResult<DaemonTask> {
        validate_identifier(&input.task_id)?;
        let project_id = ProjectId::new(input.project_id.clone())?;
        let issue_ref = normalize_required("issue_ref", &input.issue_ref)?;

        let existing_tasks = store.list_tasks(base_dir)?;
        if existing_tasks
            .iter()
            .any(|task| task.issue_ref == issue_ref && !task.is_terminal())
        {
            return Err(AppError::DuplicateTaskForIssue { issue_ref });
        }

        let resolution = routing_engine.resolve_flow(
            input.routing_command.as_deref(),
            &input.routing_labels,
            default_flow,
        )?;

        let now = Utc::now();
        let task = DaemonTask {
            task_id: input.task_id,
            issue_ref: normalize_required("issue_ref", &input.issue_ref)?,
            project_id: project_id.to_string(),
            project_name: input.project_name.filter(|value| !value.trim().is_empty()),
            prompt: input.prompt.filter(|value| !value.trim().is_empty()),
            routing_command: input
                .routing_command
                .filter(|value| !value.trim().is_empty()),
            routing_labels: input.routing_labels,
            resolved_flow: Some(resolution.flow),
            routing_source: Some(resolution.source),
            routing_warnings: resolution.warnings.clone(),
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode: input.dispatch_mode,
            source_revision: input.source_revision,
            requirements_run_id: None,
            repo_slug: None,
            issue_number: None,
            pr_url: None,
            last_seen_comment_id: None,
            last_seen_review_id: None,
            label_dirty: false,
        };

        store.create_task(base_dir, &task)?;
        Self::append_journal_event(
            store,
            base_dir,
            DaemonJournalEventType::TaskCreated,
            json!({
                "task_id": task.task_id,
                "issue_ref": task.issue_ref,
                "project_id": task.project_id,
                "flow": resolution.flow,
                "routing_source": resolution.source,
                "warnings": resolution.warnings,
            }),
        )?;

        Ok(task)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn claim_task(
        store: &dyn DaemonStorePort,
        worktree: &dyn WorktreePort,
        routing_engine: &RoutingEngine,
        base_dir: &Path,
        repo_root: &Path,
        task_id: &str,
        default_flow: FlowPreset,
        lease_ttl_seconds: u64,
        worktree_path_override: Option<std::path::PathBuf>,
        branch_name_override: Option<String>,
    ) -> AppResult<(DaemonTask, WorktreeLease)> {
        let mut task = store.read_task(base_dir, task_id)?;
        if task.status != TaskStatus::Pending {
            return Err(AppError::TaskStateTransitionInvalid {
                task_id: task.task_id.clone(),
                from: task.status.as_str().to_owned(),
                to: TaskStatus::Claimed.as_str().to_owned(),
            });
        }

        if task.lease_id.is_some()
            || store
                .list_leases(base_dir)?
                .iter()
                .any(|lease| lease.task_id == task.task_id)
        {
            return Err(AppError::TaskStateTransitionInvalid {
                task_id: task.task_id.clone(),
                from: task.status.as_str().to_owned(),
                to: "claimed_with_duplicate_lease".to_owned(),
            });
        }

        hydrate_routing(&mut task, routing_engine, default_flow)?;
        // Resume from a preserved branch only for failed-task retries.
        // Aborted retries (no failure_class) start fresh to avoid
        // resurrecting stale work from a user-cancelled run.
        let is_failed_retry = task.attempt_count > 0 && task.failure_class.is_some();
        // Save failure provenance before clearing so we can restore it if
        // the claim rolls back to Pending (e.g. LeaseAcquired journal failure).
        let saved_failure_class = task.failure_class.clone();
        let saved_failure_message = task.failure_message.clone();
        task.clear_failure();
        let project_id = ProjectId::new(task.project_id.clone())?;
        let lease = match LeaseService::acquire(
            store,
            worktree,
            base_dir,
            repo_root,
            &task.task_id,
            &project_id,
            lease_ttl_seconds,
            worktree_path_override,
            branch_name_override,
            is_failed_retry,
        ) {
            Ok(lease) => lease,
            Err(AppError::ProjectWriterLockHeld { .. }) => {
                return Err(AppError::ProjectWriterLockHeld {
                    project_id: project_id.to_string(),
                })
            }
            Err(error) => {
                let _ = Self::mark_failed(
                    store,
                    base_dir,
                    &task.task_id,
                    "worktree_creation_failed",
                    &error.to_string(),
                );
                return Err(error);
            }
        };

        let now = Utc::now();
        task.transition_to(TaskStatus::Claimed, now)?;
        task.attach_lease(lease.lease_id.clone());
        store.write_task(base_dir, &task).inspect_err(|_error| {
            let _ = LeaseService::release(
                store,
                worktree,
                base_dir,
                repo_root,
                &lease,
                ReleaseMode::Idempotent,
            );
        })?;

        if let Err(journal_err) = Self::append_journal_event(
            store,
            base_dir,
            DaemonJournalEventType::LeaseAcquired,
            json!({
                "task_id": task.task_id,
                "lease_id": lease.lease_id,
                "project_id": project_id,
                "worktree_path": lease.worktree_path,
                "branch_name": lease.branch_name,
                "ttl_seconds": lease.ttl_seconds,
            }),
        ) {
            // LeaseAcquired journal failed before TaskClaimed: only restore to
            // Pending if physical lease/worktree/writer-lock cleanup fully succeeds.
            // If physical cleanup fails, persist a terminal Failed state so the
            // durable model never hides retained claim resources.
            let release_result = LeaseService::release(
                store,
                worktree,
                base_dir,
                repo_root,
                &lease,
                ReleaseMode::Idempotent,
            );
            let resources_released = release_result.as_ref().is_ok_and(|r| r.resources_released);

            if resources_released {
                // Physical resources released — safe to restore Pending.
                // Restore failure provenance so the next claim attempt still
                // detects a failed-task retry and resumes from the preserved branch.
                let release_journal_err = release_result.ok().and_then(|r| r.journal_error);
                let rollback_result = (|| -> AppResult<()> {
                    task.status = TaskStatus::Pending;
                    task.clear_lease();
                    task.failure_class = saved_failure_class.clone();
                    task.failure_message = saved_failure_message.clone();
                    task.updated_at = Utc::now();
                    store.write_task(base_dir, &task)?;
                    let _ = Self::append_journal_event(
                        store,
                        base_dir,
                        DaemonJournalEventType::ClaimRollback,
                        json!({
                            "task_id": task.task_id,
                            "reason": format!("LeaseAcquired journal failed: {journal_err}"),
                            "rollback_target": "pending",
                            "lease_released": true,
                            "release_journal_error": release_journal_err,
                        }),
                    );
                    Ok(())
                })();
                if rollback_result.is_err() {
                    // Task-write failed after successful release: mark Failed.
                    let _ = (|| -> AppResult<()> {
                        let mut t = store.read_task(base_dir, &task.task_id)?;
                        t.transition_to(TaskStatus::Failed, Utc::now())?;
                        t.set_failure(
                            "claim_journal_failed",
                            format!(
                                "LeaseAcquired journal failed and rollback write failed: {journal_err}"
                            ),
                        );
                        t.clear_lease();
                        store.write_task(base_dir, &t)?;
                        Ok(())
                    })();
                }
            } else {
                // Physical release failed or partial — claim resources
                // (lease/worktree/lock) may remain on disk. Mark terminal so
                // durable state is truthful.
                // release_result may be Ok(partial) or Err(e): handle both.
                let release_detail = match &release_result {
                    Ok(r) => format!(
                        "partial cleanup (resources_released=false, worktree_absent={}, lease_file_absent={}, writer_lock_absent={}, lease_file_error={:?}, writer_lock_error={:?})",
                        r.worktree_already_absent, r.lease_file_already_absent,
                        r.writer_lock_already_absent, r.lease_file_error, r.writer_lock_error
                    ),
                    Err(e) => e.to_string(),
                };
                let _ = (|| -> AppResult<()> {
                    let mut t = store.read_task(base_dir, &task.task_id)?;
                    t.transition_to(TaskStatus::Failed, Utc::now())?;
                    t.set_failure(
                        "claim_journal_failed",
                        format!(
                            "LeaseAcquired journal failed and lease release failed: {journal_err}; release: {release_detail}"
                        ),
                    );
                    // Do NOT clear lease_id — the lease is still on disk.
                    store.write_task(base_dir, &t)?;
                    let _ = Self::append_journal_event(
                        store,
                        base_dir,
                        DaemonJournalEventType::ClaimRollback,
                        json!({
                            "task_id": task.task_id,
                            "reason": format!("LeaseAcquired journal failed: {journal_err}"),
                            "rollback_target": "failed",
                            "lease_released": false,
                            "release_error": release_detail,
                        }),
                    );
                    Ok(())
                })();
            }
            return Err(journal_err);
        }
        if let Err(journal_err) = Self::append_journal_event(
            store,
            base_dir,
            DaemonJournalEventType::TaskClaimed,
            json!({
                "task_id": task.task_id,
                "lease_id": lease.lease_id,
                "project_id": project_id,
            }),
        ) {
            // TaskClaimed journal failed: attempt lease release and mark failed.
            // Only clear lease_id if physical resources were actually released.
            let release_result = LeaseService::release(
                store,
                worktree,
                base_dir,
                repo_root,
                &lease,
                ReleaseMode::Idempotent,
            );
            let resources_released = release_result.as_ref().is_ok_and(|r| r.resources_released);
            let _ = (|| -> AppResult<()> {
                let mut t = store.read_task(base_dir, &task.task_id)?;
                t.transition_to(TaskStatus::Failed, Utc::now())?;
                t.set_failure(
                    "claim_journal_failed",
                    format!("TaskClaimed journal append failed: {journal_err}"),
                );
                if resources_released {
                    t.clear_lease();
                }
                store.write_task(base_dir, &t)?;
                let _ = Self::append_journal_event(
                    store,
                    base_dir,
                    DaemonJournalEventType::ClaimRollback,
                    json!({
                        "task_id": task.task_id,
                        "reason": format!("TaskClaimed journal failed: {journal_err}"),
                        "rollback_target": "failed",
                        "lease_released": resources_released,
                    }),
                );
                Ok(())
            })();
            return Err(journal_err);
        }

        Ok((task, lease))
    }

    pub fn mark_active(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        task_id: &str,
    ) -> AppResult<DaemonTask> {
        let mut task = store.read_task(base_dir, task_id)?;
        task.transition_to(TaskStatus::Active, Utc::now())?;
        store.write_task(base_dir, &task)?;
        Ok(task)
    }

    pub fn mark_completed(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        task_id: &str,
    ) -> AppResult<DaemonTask> {
        let original_task = store.read_task(base_dir, task_id)?;
        let mut task = original_task.clone();
        task.transition_to(TaskStatus::Completed, Utc::now())?;
        store.write_task(base_dir, &task)?;
        if let Err(journal_err) = Self::append_journal_event(
            store,
            base_dir,
            DaemonJournalEventType::TaskCompleted,
            json!({ "task_id": task.task_id }),
        ) {
            store
                .write_task(base_dir, &original_task)
                .map_err(|restore_err| AppError::CorruptRecord {
                    file: format!("daemon/tasks/{}.json", task.task_id),
                    details: format!(
                        "task completion journal append failed: {journal_err}; \
                         failed to restore prior task state: {restore_err}"
                    ),
                })?;
            return Err(journal_err);
        }
        Ok(task)
    }

    pub fn mark_failed(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        task_id: &str,
        failure_class: &str,
        failure_message: &str,
    ) -> AppResult<DaemonTask> {
        let mut task = store.read_task(base_dir, task_id)?;
        task.transition_to(TaskStatus::Failed, Utc::now())?;
        task.set_failure(failure_class, failure_message);
        store.write_task(base_dir, &task)?;
        Self::append_journal_event(
            store,
            base_dir,
            DaemonJournalEventType::TaskFailed,
            json!({
                "task_id": task.task_id,
                "failure_class": failure_class,
                "failure_message": failure_message,
            }),
        )?;
        Ok(task)
    }

    pub fn mark_aborted(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        task_id: &str,
    ) -> AppResult<DaemonTask> {
        let mut task = store.read_task(base_dir, task_id)?;
        if matches!(
            task.status,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Aborted
        ) {
            return Err(AppError::TaskStateTransitionInvalid {
                task_id: task.task_id.clone(),
                from: task.status.as_str().to_owned(),
                to: TaskStatus::Aborted.as_str().to_owned(),
            });
        }
        task.transition_to(TaskStatus::Aborted, Utc::now())?;
        store.write_task(base_dir, &task)?;
        Self::append_journal_event(
            store,
            base_dir,
            DaemonJournalEventType::TaskAborted,
            json!({ "task_id": task.task_id }),
        )?;
        Ok(task)
    }

    pub fn retry_task(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        task_id: &str,
    ) -> AppResult<DaemonTask> {
        let mut task = store.read_task(base_dir, task_id)?;
        if task.status != TaskStatus::Failed && task.status != TaskStatus::Aborted {
            return Err(AppError::TaskStateTransitionInvalid {
                task_id: task.task_id.clone(),
                from: task.status.as_str().to_owned(),
                to: TaskStatus::Pending.as_str().to_owned(),
            });
        }

        // Reject retry if the task still holds a lease reference — the
        // caller must clean up the lease/worktree first (via explicit
        // release or reconcile) to prevent stranding live resources.
        if task.lease_id.is_some() {
            return Err(AppError::LeaseCleanupPartialFailure {
                task_id: task.task_id.clone(),
            });
        }

        task.transition_to(TaskStatus::Pending, Utc::now())?;
        task.attempt_count += 1;
        // Do NOT clear failure info here — it serves as provenance so
        // claim_task can distinguish a failed-task retry (which should
        // resume from a preserved branch) from an aborted-task retry
        // (which should start fresh). claim_task clears it after reading.
        store.write_task(base_dir, &task)?;
        Ok(task)
    }

    /// Recovery-only: revert a non-terminal task (Claimed/Active) back to
    /// Pending so it can be re-processed by the daemon in the next cycle.
    /// This is used by Phase 0 label repair to recover tasks that were
    /// quarantined mid-processing due to a GitHub label-sync failure.
    /// The associated lease is released and the task's lease reference cleared.
    pub fn revert_to_pending_for_recovery(
        store: &dyn DaemonStorePort,
        worktree: &dyn WorktreePort,
        base_dir: &Path,
        repo_root: &Path,
        task_id: &str,
    ) -> AppResult<DaemonTask> {
        let mut task = store.read_task(base_dir, task_id)?;
        if task.is_terminal() {
            return Err(AppError::TaskStateTransitionInvalid {
                task_id: task.task_id.clone(),
                from: task.status.as_str().to_owned(),
                to: "revert_to_pending".to_owned(),
            });
        }

        // Release the lease if present. Only revert to Pending and clear
        // the lease reference after cleanup positively succeeds; otherwise
        // keep the task in its current state with lease preserved so the
        // resources remain visible for later repair (reconcile or Phase 0).
        if let Some(ref lid) = task.lease_id {
            if let Ok(lease) = store.read_lease(base_dir, lid) {
                let result = LeaseService::release(
                    store,
                    worktree,
                    base_dir,
                    repo_root,
                    &lease,
                    ReleaseMode::Idempotent,
                );
                match result {
                    Ok(ref r) if r.resources_released => {
                        // All sub-steps succeeded — safe to proceed with revert.
                    }
                    Ok(_) => {
                        // Partial cleanup: some resources remain. Preserve task
                        // state and lease ownership for later repair.
                        return Err(AppError::LeaseCleanupPartialFailure {
                            task_id: task_id.to_owned(),
                        });
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        }

        // Directly set status to Pending (bypasses transition_to validation
        // since Claimed → Pending is a recovery-only transition).
        task.status = TaskStatus::Pending;
        task.updated_at = Utc::now();
        task.clear_lease();
        store.write_task(base_dir, &task)?;
        Ok(task)
    }

    /// Find a task by repo_slug + issue_number. Returns the first non-terminal
    /// match, or the most recent terminal match if no non-terminal exists.
    pub fn find_task_by_issue(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        repo_slug: &str,
        issue_number: u64,
    ) -> AppResult<Option<DaemonTask>> {
        let tasks = store.list_tasks(base_dir)?;
        // Prefer non-terminal task for this issue
        let non_terminal = tasks.iter().find(|t| {
            t.repo_slug.as_deref() == Some(repo_slug)
                && t.issue_number == Some(issue_number)
                && !t.is_terminal()
        });
        if let Some(task) = non_terminal {
            return Ok(Some(task.clone()));
        }
        // Fall back to most recent terminal task
        let terminal = tasks.iter().rev().find(|t| {
            t.repo_slug.as_deref() == Some(repo_slug) && t.issue_number == Some(issue_number)
        });
        Ok(terminal.cloned())
    }

    pub fn clear_lease_reference(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        task_id: &str,
    ) -> AppResult<DaemonTask> {
        let mut task = store.read_task(base_dir, task_id)?;
        task.clear_lease();
        task.updated_at = Utc::now();
        store.write_task(base_dir, &task)?;
        Ok(task)
    }

    /// Mark a task's GitHub status label as out-of-sync with durable state.
    /// Called when `sync_label_for_task` fails so the mismatch is durable and
    /// `daemon reconcile` can repair it later.
    pub fn mark_label_dirty(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        task_id: &str,
    ) -> AppResult<()> {
        let mut task = store.read_task(base_dir, task_id)?;
        task.label_dirty = true;
        task.updated_at = Utc::now();
        store.write_task(base_dir, &task)?;
        Ok(())
    }

    /// Clear the label_dirty flag after a successful label re-sync.
    pub fn clear_label_dirty(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        task_id: &str,
    ) -> AppResult<()> {
        let mut task = store.read_task(base_dir, task_id)?;
        if task.label_dirty {
            task.label_dirty = false;
            task.updated_at = Utc::now();
            store.write_task(base_dir, &task)?;
        }
        Ok(())
    }

    /// Create a task from a watched issue, enforcing idempotency by
    /// `(issue_ref, source_revision)`. If a non-terminal task already exists
    /// for the same issue_ref and source_revision, the call is a no-op.
    /// If a prior task for the same issue_ref is terminal and a newer
    /// source_revision appears, a fresh task may be created.
    ///
    /// If `github_meta` is provided, the GitHub-specific fields (`repo_slug`,
    /// `issue_number`, `pr_url`, dedup cursors) are populated atomically on the
    /// initial task record. This prevents a window where a persisted task lacks
    /// GitHub metadata if a subsequent write fails.
    pub fn create_task_from_watched_issue(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        routing_engine: &RoutingEngine,
        default_flow: FlowPreset,
        issue: &WatchedIssueMeta,
        dispatch_mode: DispatchMode,
        github_meta: Option<&super::model::GithubTaskMeta>,
    ) -> AppResult<Option<DaemonTask>> {
        let issue_ref = normalize_required("issue_ref", &issue.issue_ref)?;
        let source_revision = normalize_required("source_revision", &issue.source_revision)?;

        let existing_tasks = store.list_tasks(base_dir)?;

        // Check for exact (issue_ref, source_revision) match on non-terminal task
        for task in &existing_tasks {
            if task.issue_ref == issue_ref && !task.is_terminal() {
                if task.source_revision.as_deref() == Some(source_revision.as_str()) {
                    // Idempotent: same issue + same revision, already tracked
                    return Ok(None);
                }
                // Different source_revision but same issue_ref with non-terminal task
                return Err(AppError::DuplicateWatchedIssue {
                    issue_ref,
                    source_revision,
                });
            }
        }

        // If the routing_command is a requirements command, don't pass it to flow
        // resolution — requirements commands are orthogonal to flow routing.
        // Flow precedence still applies via labels and repo default.
        let flow_routing_cmd = issue.routing_command.as_deref().filter(|cmd| {
            !super::watcher::is_requirements_command(cmd)
                && !super::routing::RoutingEngine::is_daemon_command(cmd)
        });
        let resolution =
            routing_engine.resolve_flow(flow_routing_cmd, &issue.labels, default_flow)?;

        let task_id = format!(
            "watch-{}-{}",
            issue_ref.replace('/', "-").replace('#', ""),
            &source_revision[..source_revision.len().min(8)]
        );
        validate_identifier(&task_id)?;

        let now = Utc::now();
        let task = DaemonTask {
            task_id,
            issue_ref: issue_ref.clone(),
            project_id: format!("watched-{}", issue_ref.replace('/', "-").replace('#', "")),
            project_name: Some(issue.title.clone()),
            prompt: Some(issue.body.clone()),
            routing_command: issue.routing_command.clone(),
            routing_labels: issue.labels.clone(),
            resolved_flow: Some(resolution.flow),
            routing_source: Some(resolution.source),
            routing_warnings: resolution.warnings.clone(),
            status: TaskStatus::Pending,
            created_at: now,
            updated_at: now,
            attempt_count: 0,
            lease_id: None,
            failure_class: None,
            failure_message: None,
            dispatch_mode,
            source_revision: Some(source_revision),
            requirements_run_id: None,
            repo_slug: github_meta.map(|m| m.repo_slug.clone()),
            issue_number: github_meta.map(|m| m.issue_number),
            pr_url: github_meta.and_then(|m| m.pr_url.clone()),
            last_seen_comment_id: github_meta.and_then(|m| m.last_seen_comment_id),
            last_seen_review_id: github_meta.and_then(|m| m.last_seen_review_id),
            label_dirty: false,
        };

        store.create_task(base_dir, &task)?;
        Self::append_journal_event(
            store,
            base_dir,
            DaemonJournalEventType::WatcherIngestion,
            json!({
                "task_id": task.task_id,
                "issue_ref": task.issue_ref,
                "source_revision": task.source_revision,
                "dispatch_mode": task.dispatch_mode,
                "flow": resolution.flow,
            }),
        )?;

        Ok(Some(task))
    }

    /// Transition an active task to waiting_for_requirements.
    /// Releases the lease and writer lock, leaving no external resources held.
    pub fn mark_waiting_for_requirements(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        task_id: &str,
        requirements_run_id: &str,
    ) -> AppResult<DaemonTask> {
        let mut task = store.read_task(base_dir, task_id)?;
        task.transition_to(TaskStatus::WaitingForRequirements, Utc::now())?;
        task.requirements_run_id = Some(requirements_run_id.to_owned());
        task.clear_lease();
        store.write_task(base_dir, &task)?;
        Self::append_journal_event(
            store,
            base_dir,
            DaemonJournalEventType::RequirementsWaiting,
            json!({
                "task_id": task.task_id,
                "requirements_run_id": requirements_run_id,
            }),
        )?;
        Ok(task)
    }

    /// Resume a waiting task back to pending for re-processing by the daemon.
    pub fn resume_from_waiting(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        task_id: &str,
    ) -> AppResult<DaemonTask> {
        let mut task = store.read_task(base_dir, task_id)?;
        if task.status != TaskStatus::WaitingForRequirements {
            return Err(AppError::TaskStateTransitionInvalid {
                task_id: task.task_id.clone(),
                from: task.status.as_str().to_owned(),
                to: TaskStatus::Pending.as_str().to_owned(),
            });
        }
        task.transition_to(TaskStatus::Pending, Utc::now())?;
        // Switch to workflow dispatch now that requirements are complete
        task.dispatch_mode = DispatchMode::Workflow;
        store.write_task(base_dir, &task)?;
        Self::append_journal_event(
            store,
            base_dir,
            DaemonJournalEventType::RequirementsResumed,
            json!({
                "task_id": task.task_id,
                "requirements_run_id": task.requirements_run_id,
            }),
        )?;
        Ok(task)
    }

    pub(crate) fn append_journal_event(
        store: &dyn DaemonStorePort,
        base_dir: &Path,
        event_type: DaemonJournalEventType,
        details: serde_json::Value,
    ) -> AppResult<()> {
        let events = store.read_daemon_journal(base_dir)?;
        let sequence = events.last().map_or(1, |event| event.sequence + 1);
        let event = DaemonJournalEvent {
            sequence,
            timestamp: Utc::now(),
            event_type,
            details,
        };
        store.append_daemon_journal_event(base_dir, &event)
    }
}

fn hydrate_routing(
    task: &mut DaemonTask,
    routing_engine: &RoutingEngine,
    default_flow: FlowPreset,
) -> AppResult<()> {
    if task.resolved_flow.is_some() {
        return Ok(());
    }

    let resolution = routing_engine.resolve_flow(
        task.routing_command.as_deref(),
        &task.routing_labels,
        default_flow,
    )?;
    task.resolved_flow = Some(resolution.flow);
    task.routing_source = Some(resolution.source);
    task.routing_warnings = resolution.warnings;
    Ok(())
}

fn validate_identifier(value: &str) -> AppResult<()> {
    let _ = ProjectId::new(value.to_owned())?;
    Ok(())
}

fn normalize_required(field: &'static str, value: &str) -> AppResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::InvalidConfigValue {
            key: field.to_owned(),
            value: value.to_owned(),
            reason: "value cannot be empty".to_owned(),
        });
    }
    Ok(trimmed.to_owned())
}
