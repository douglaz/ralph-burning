use std::path::Path;

use chrono::Utc;
use serde_json::json;

use crate::contexts::automation_runtime::lease_service::LeaseService;
use crate::contexts::automation_runtime::model::{
    DaemonJournalEvent, DaemonJournalEventType, DaemonTask, TaskStatus, WorktreeLease,
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

    pub fn claim_task(
        store: &dyn DaemonStorePort,
        worktree: &dyn WorktreePort,
        routing_engine: &RoutingEngine,
        base_dir: &Path,
        repo_root: &Path,
        task_id: &str,
        default_flow: FlowPreset,
        lease_ttl_seconds: u64,
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
        let project_id = ProjectId::new(task.project_id.clone())?;
        let lease = match LeaseService::acquire(
            store,
            worktree,
            base_dir,
            repo_root,
            &task.task_id,
            &project_id,
            lease_ttl_seconds,
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
        store.write_task(base_dir, &task).map_err(|error| {
            let _ = LeaseService::release(store, worktree, base_dir, repo_root, &lease);
            error
        })?;

        Self::append_journal_event(
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
        )?;
        Self::append_journal_event(
            store,
            base_dir,
            DaemonJournalEventType::TaskClaimed,
            json!({
                "task_id": task.task_id,
                "lease_id": lease.lease_id,
                "project_id": project_id,
            }),
        )?;

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
        let mut task = store.read_task(base_dir, task_id)?;
        task.transition_to(TaskStatus::Completed, Utc::now())?;
        store.write_task(base_dir, &task)?;
        Self::append_journal_event(
            store,
            base_dir,
            DaemonJournalEventType::TaskCompleted,
            json!({ "task_id": task.task_id }),
        )?;
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
        if task.status != TaskStatus::Failed {
            return Err(AppError::TaskStateTransitionInvalid {
                task_id: task.task_id.clone(),
                from: task.status.as_str().to_owned(),
                to: TaskStatus::Pending.as_str().to_owned(),
            });
        }

        task.transition_to(TaskStatus::Pending, Utc::now())?;
        task.attempt_count += 1;
        task.clear_failure();
        task.clear_lease();
        store.write_task(base_dir, &task)?;
        Ok(task)
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
