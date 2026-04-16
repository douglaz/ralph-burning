#![forbid(unsafe_code)]

//! Failure reconciliation handler for failed bead tasks.
//!
//! Unlike success reconciliation, this path never mutates bead state in `br`.
//! It records the failed attempt in milestone lineage/journal state, counts the
//! bead's failed attempts, and either keeps the controller retryable or
//! escalates to operator intervention after too many failures.

use std::path::Path;

use chrono::{DateTime, Utc};
use tracing::Instrument;

use crate::contexts::milestone_record::controller::{
    self as milestone_controller, MilestoneControllerPort, MilestoneControllerState,
};
use crate::contexts::milestone_record::model::{MilestoneId, TaskRunEntry, TaskRunOutcome};
use crate::contexts::milestone_record::service::{
    self as milestone_service, CompletionMilestoneDisposition, MilestoneJournalPort,
    MilestoneSnapshotPort, TaskRunLineagePort,
};

pub const MAX_FAILURE_RETRIES: u32 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureReconciliationOutcome {
    Retryable {
        attempt_number: u32,
        max_retries: u32,
    },
    EscalatedToOperator {
        attempt_number: u32,
        reason: String,
    },
}

#[derive(Debug)]
pub enum FailureReconciliationError {
    MilestoneUpdateFailed {
        bead_id: String,
        task_id: String,
        details: String,
    },
}

impl std::fmt::Display for FailureReconciliationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MilestoneUpdateFailed {
                bead_id,
                task_id,
                details,
            } => write!(
                f,
                "milestone update failed for bead={bead_id} task={task_id}: {details}"
            ),
        }
    }
}

impl std::error::Error for FailureReconciliationError {}

#[allow(clippy::too_many_arguments)]
pub async fn reconcile_failure<S, J, L, C>(
    snapshot_store: &S,
    journal_store: &J,
    lineage_store: &L,
    controller_store: &C,
    base_dir: &Path,
    bead_id: &str,
    task_id: &str,
    project_id: &str,
    milestone_id_str: &str,
    run_id: &str,
    plan_hash: Option<&str>,
    started_at: DateTime<Utc>,
    failed_at: DateTime<Utc>,
    error_summary: &str,
) -> Result<FailureReconciliationOutcome, FailureReconciliationError>
where
    S: MilestoneSnapshotPort,
    J: MilestoneJournalPort,
    L: TaskRunLineagePort,
    C: MilestoneControllerPort,
{
    async move {
        let milestone_id = MilestoneId::new(milestone_id_str).map_err(|error| {
            FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!("invalid milestone id: {error}"),
            }
        })?;
        let error_summary = normalize_error_summary(error_summary);

        let existing_runs =
            milestone_service::find_runs_for_bead(lineage_store, base_dir, &milestone_id, bead_id)
                .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
                    bead_id: bead_id.to_owned(),
                    task_id: task_id.to_owned(),
                    details: format!(
                        "failed to load task runs for failure reconciliation: {error}"
                    ),
                })?;

        let (already_recorded, predicted_attempt_number) =
            failure_attempt_number(&existing_runs, bead_id, task_id, project_id, run_id)?;
        let outcome_detail =
            format_failure_outcome_detail(task_id, predicted_attempt_number, &error_summary);

        if already_recorded {
            milestone_service::repair_task_run_with_disposition(
                snapshot_store,
                journal_store,
                lineage_store,
                base_dir,
                &milestone_id,
                bead_id,
                project_id,
                run_id,
                plan_hash,
                started_at,
                TaskRunOutcome::Failed,
                Some(outcome_detail.clone()),
                failed_at,
                CompletionMilestoneDisposition::ReconcileFromLineage,
            )
            .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: error.to_string(),
            })?;
        } else {
            milestone_service::record_bead_completion_with_disposition(
                snapshot_store,
                journal_store,
                lineage_store,
                base_dir,
                &milestone_id,
                bead_id,
                project_id,
                run_id,
                plan_hash,
                TaskRunOutcome::Failed,
                Some(&outcome_detail),
                started_at,
                failed_at,
                CompletionMilestoneDisposition::ReconcileFromLineage,
            )
            .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: error.to_string(),
            })?;
        }

        let recorded_runs =
            milestone_service::find_runs_for_bead(lineage_store, base_dir, &milestone_id, bead_id)
                .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
                    bead_id: bead_id.to_owned(),
                    task_id: task_id.to_owned(),
                    details: format!(
                        "failed to reload task runs after failure reconciliation: {error}"
                    ),
                })?;
        let (_, attempt_number) =
            failure_attempt_number(&recorded_runs, bead_id, task_id, project_id, run_id)?;

        if !recorded_runs.iter().any(|entry| {
            entry.project_id == project_id
                && entry.run_id.as_deref() == Some(run_id)
                && entry.outcome == TaskRunOutcome::Failed
        }) {
            return Err(FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!(
                    "failed run was not present after reconciliation for run_id={run_id}"
                ),
            });
        }

        tracing::warn!(
            bead_id = bead_id,
            task_id = task_id,
            attempt_number = attempt_number,
            max_retries = MAX_FAILURE_RETRIES,
            error_summary = error_summary.as_str(),
            already_recorded,
            "reconciled failed bead attempt"
        );

        let outcome = failure_outcome(bead_id, attempt_number, &error_summary);
        transition_controller_after_failure(
            controller_store,
            base_dir,
            &milestone_id,
            bead_id,
            task_id,
            project_id,
            run_id,
            started_at,
            failed_at,
            already_recorded,
            &recorded_runs,
            &outcome,
        )?;

        if let FailureReconciliationOutcome::EscalatedToOperator {
            attempt_number,
            reason,
        } = &outcome
        {
            tracing::error!(
                bead_id = bead_id,
                task_id = task_id,
                attempt_number = *attempt_number,
                max_retries = MAX_FAILURE_RETRIES,
                reason = reason.as_str(),
                "failed bead escalated to operator"
            );
        }

        Ok(outcome)
    }
    .instrument(tracing::warn_span!(
        "reconcile_failure",
        milestone_id = milestone_id_str,
        bead_id = bead_id,
        task_id = task_id,
        run_id = run_id
    ))
    .await
}

fn normalize_error_summary(error_summary: &str) -> String {
    let normalized = error_summary
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" | ");
    if normalized.is_empty() {
        "task failed without an error summary".to_owned()
    } else {
        normalized
    }
}

fn format_failure_outcome_detail(
    task_id: &str,
    attempt_number: u32,
    error_summary: &str,
) -> String {
    format!("task_id={task_id}\nattempt={attempt_number}\nerror={error_summary}")
}

fn attempt_identity_matches(
    entry: &TaskRunEntry,
    project_id: &str,
    run_id: &str,
    started_at: DateTime<Utc>,
) -> bool {
    entry.project_id == project_id
        && entry.run_id.as_deref() == Some(run_id)
        && entry.started_at == started_at
}

fn newer_attempt_exists(
    runs: &[TaskRunEntry],
    project_id: &str,
    run_id: &str,
    started_at: DateTime<Utc>,
) -> bool {
    runs.iter().any(|entry| {
        !attempt_identity_matches(entry, project_id, run_id, started_at)
            && entry.started_at > started_at
    })
}

fn controller_requires_failure_sync(
    controller: &milestone_controller::MilestoneControllerRecord,
    bead_id: &str,
    task_id: &str,
) -> bool {
    match controller.state {
        MilestoneControllerState::Idle => true,
        MilestoneControllerState::Selecting | MilestoneControllerState::Completed => false,
        MilestoneControllerState::Claimed
        | MilestoneControllerState::Running
        | MilestoneControllerState::Reconciling
        | MilestoneControllerState::Blocked
        | MilestoneControllerState::NeedsOperator => {
            controller
                .active_bead_id
                .as_deref()
                .is_none_or(|active_bead_id| active_bead_id == bead_id)
                && controller
                    .active_task_id
                    .as_deref()
                    .is_none_or(|active_task_id| active_task_id == task_id)
        }
    }
}

fn failure_attempt_number(
    runs: &[TaskRunEntry],
    bead_id: &str,
    task_id: &str,
    project_id: &str,
    run_id: &str,
) -> Result<(bool, u32), FailureReconciliationError> {
    let failed_runs: Vec<_> = runs
        .iter()
        .filter(|entry| entry.outcome == TaskRunOutcome::Failed)
        .cloned()
        .collect();

    let exact_run = runs
        .iter()
        .find(|entry| entry.project_id == project_id && entry.run_id.as_deref() == Some(run_id));

    if let Some(entry) = exact_run {
        if entry.outcome == TaskRunOutcome::Failed {
            let attempt_number = failed_runs
                .iter()
                .position(|candidate| TaskRunEntry::same_attempt(candidate, entry))
                .map(|index| index as u32 + 1)
                .unwrap_or(failed_runs.len() as u32);
            return Ok((true, attempt_number));
        }

        if entry.outcome.is_terminal() {
            return Err(FailureReconciliationError::MilestoneUpdateFailed {
                bead_id: bead_id.to_owned(),
                task_id: task_id.to_owned(),
                details: format!(
                    "run_id={run_id} is already finalized with non-failed outcome '{}'",
                    entry.outcome
                ),
            });
        }
    }

    Ok((false, failed_runs.len() as u32 + 1))
}

fn failure_outcome(
    bead_id: &str,
    attempt_number: u32,
    error_summary: &str,
) -> FailureReconciliationOutcome {
    if attempt_number < MAX_FAILURE_RETRIES {
        FailureReconciliationOutcome::Retryable {
            attempt_number,
            max_retries: MAX_FAILURE_RETRIES,
        }
    } else {
        FailureReconciliationOutcome::EscalatedToOperator {
            attempt_number,
            reason: format!("bead {bead_id} failed {attempt_number} times: {error_summary}"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn transition_controller_after_failure<C: MilestoneControllerPort>(
    controller_store: &C,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    task_id: &str,
    project_id: &str,
    run_id: &str,
    started_at: DateTime<Utc>,
    now: DateTime<Utc>,
    replay_already_settled: bool,
    recorded_runs: &[TaskRunEntry],
    outcome: &FailureReconciliationOutcome,
) -> Result<(), FailureReconciliationError> {
    let (desired_state, desired_reason, request) = match outcome {
        FailureReconciliationOutcome::Retryable {
            attempt_number,
            max_retries,
        } => {
            let reason = format!(
                "bead {bead_id} failed attempt {attempt_number}/{max_retries}; retry remains available"
            );
            let request = milestone_controller::ControllerTransitionRequest::new(
                MilestoneControllerState::Blocked,
                reason.clone(),
            );
            (MilestoneControllerState::Blocked, reason, request)
        }
        FailureReconciliationOutcome::EscalatedToOperator {
            attempt_number: _,
            reason,
        } => {
            let request = milestone_controller::ControllerTransitionRequest::new(
                MilestoneControllerState::NeedsOperator,
                reason.clone(),
            )
            .with_bead(bead_id)
            .with_task(task_id);
            (
                MilestoneControllerState::NeedsOperator,
                reason.clone(),
                request,
            )
        }
    };

    let current = milestone_controller::load_controller(controller_store, base_dir, milestone_id)
        .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
        bead_id: bead_id.to_owned(),
        task_id: task_id.to_owned(),
        details: format!("failed to load controller during failure reconciliation: {error}"),
    })?;

    if newer_attempt_exists(recorded_runs, project_id, run_id, started_at) {
        tracing::info!(
            bead_id = bead_id,
            task_id = task_id,
            run_id = run_id,
            "skipping failure controller transition because a newer bead attempt is already recorded"
        );
        return Ok(());
    }

    if replay_already_settled
        && current.as_ref().is_some_and(|controller| {
            !controller_requires_failure_sync(controller, bead_id, task_id)
        })
    {
        return Ok(());
    }

    if replay_already_settled
        && current.as_ref().is_some_and(|controller| {
            controller.state == desired_state
                && controller.last_transition_reason.as_deref() == Some(desired_reason.as_str())
        })
    {
        return Ok(());
    }

    if !current
        .as_ref()
        .is_some_and(|controller| controller.state == MilestoneControllerState::Reconciling)
    {
        milestone_controller::sync_controller_task_reconciling(
            controller_store,
            base_dir,
            milestone_id,
            bead_id,
            task_id,
            "workflow execution failed; reconciling milestone state",
            now,
        )
        .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: error.to_string(),
        })?;
    }

    let refreshed = milestone_controller::load_controller(controller_store, base_dir, milestone_id)
        .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
            bead_id: bead_id.to_owned(),
            task_id: task_id.to_owned(),
            details: format!("failed to reload controller during failure reconciliation: {error}"),
        })?;

    if replay_already_settled
        && refreshed.as_ref().is_some_and(|controller| {
            controller.state == desired_state
                && controller.last_transition_reason.as_deref() == Some(desired_reason.as_str())
        })
    {
        return Ok(());
    }

    milestone_controller::sync_controller_state(
        controller_store,
        base_dir,
        milestone_id,
        request,
        now,
    )
    .map_err(|error| FailureReconciliationError::MilestoneUpdateFailed {
        bead_id: bead_id.to_owned(),
        task_id: task_id.to_owned(),
        details: error.to_string(),
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{reconcile_failure, FailureReconciliationOutcome, MAX_FAILURE_RETRIES};
    use crate::adapters::fs::{
        FsMilestoneControllerStore, FsMilestoneJournalStore, FsMilestonePlanStore,
        FsMilestoneSnapshotStore, FsMilestoneStore, FsTaskRunLineageStore,
    };
    use crate::contexts::milestone_record::bundle::{
        AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
    };
    use crate::contexts::milestone_record::controller as milestone_controller;
    use crate::contexts::milestone_record::model::{MilestoneEventType, TaskRunOutcome};
    use crate::contexts::milestone_record::service::{
        self as milestone_service, create_milestone, persist_plan, read_journal,
        CreateMilestoneInput, TaskRunLineagePort,
    };
    use chrono::{Duration, Utc};
    use std::path::Path;

    fn write_beads_export(base_dir: &Path) -> Result<String, Box<dyn std::error::Error>> {
        let contents = "{\"id\":\"bead-failure\",\"status\":\"open\"}\n".to_owned();
        std::fs::create_dir_all(base_dir.join(".beads"))?;
        std::fs::write(base_dir.join(".beads/issues.jsonl"), &contents)?;
        Ok(contents)
    }

    fn create_test_milestone(
        base_dir: &Path,
    ) -> Result<crate::contexts::milestone_record::model::MilestoneId, Box<dyn std::error::Error>>
    {
        let now = Utc::now();
        let record = create_milestone(
            &FsMilestoneStore,
            base_dir,
            CreateMilestoneInput {
                id: "ms-failure-reconcile".to_owned(),
                name: "Failure reconcile milestone".to_owned(),
                description: "Exercises failure reconciliation".to_owned(),
            },
            now,
        )?;
        let bundle = MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: "ms-failure-reconcile".to_owned(),
                name: "Failure reconcile milestone".to_owned(),
            },
            executive_summary: "Single-bead failure reconciliation test plan".to_owned(),
            goals: vec!["Track failed attempts".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "The bead is represented in the plan".to_owned(),
                covered_by: vec!["bead-failure".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: None,
                beads: vec![BeadProposal {
                    bead_id: Some("bead-failure".to_owned()),
                    explicit_id: Some(true),
                    title: "Failure bead".to_owned(),
                    description: None,
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec![],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: crate::shared::domain::FlowPreset::QuickDev,
            agents_guidance: None,
        };
        persist_plan(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base_dir,
            &record.id,
            &bundle,
            now + Duration::milliseconds(1),
        )?;
        Ok(record.id)
    }

    fn start_attempt(
        base_dir: &Path,
        milestone_id: &crate::contexts::milestone_record::model::MilestoneId,
        task_id: &str,
        run_id: &str,
        started_at: chrono::DateTime<Utc>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        milestone_service::record_bead_start(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            base_dir,
            milestone_id,
            "bead-failure",
            "proj-failure",
            run_id,
            "plan-v1",
            started_at,
        )?;
        milestone_controller::sync_controller_task_running(
            &FsMilestoneControllerStore,
            base_dir,
            milestone_id,
            "bead-failure",
            task_id,
            "workflow execution started",
            started_at,
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_first_failure_records_retryable_state(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let started_at = Utc::now() + Duration::seconds(1);

        start_attempt(base, &milestone_id, "task-1", "run-1", started_at)?;

        let outcome = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            started_at + Duration::seconds(10),
            "agent crashed while applying patch",
        )
        .await?;

        assert_eq!(
            outcome,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
        )?
        .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Blocked
        );
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("failed attempt 1/3")));

        let snapshot =
            milestone_service::load_snapshot(&FsMilestoneSnapshotStore, base, &milestone_id)?;
        assert_eq!(snapshot.progress.failed_beads, 1);

        let runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].outcome, TaskRunOutcome::Failed);
        assert_eq!(
            runs[0].task_id.as_deref(),
            Some("task-1"),
            "task_id should be backfilled from failure outcome detail"
        );

        let journal = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?;
        let failed_event = journal
            .iter()
            .find(|event| {
                event.event_type == MilestoneEventType::BeadFailed
                    && event.bead_id.as_deref() == Some("bead-failure")
            })
            .expect("failed event should be present");
        let details = failed_event
            .details
            .as_deref()
            .expect("failed event should carry details");
        assert!(details.contains("\"task_id\":\"task-1\""));
        assert!(details.contains("attempt=1"));
        assert!(details.contains("agent crashed while applying patch"));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_counts_second_failure_and_escalates_on_third(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let base_time = Utc::now();

        start_attempt(
            base,
            &milestone_id,
            "task-1",
            "run-1",
            base_time + Duration::seconds(1),
        )?;
        let first = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            base_time + Duration::seconds(1),
            base_time + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(
            first,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        start_attempt(
            base,
            &milestone_id,
            "task-2",
            "run-2",
            base_time + Duration::seconds(11),
        )?;
        let second = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-2",
            "proj-failure",
            milestone_id.as_str(),
            "run-2",
            Some("plan-v1"),
            base_time + Duration::seconds(11),
            base_time + Duration::seconds(15),
            "second failure",
        )
        .await?;
        assert_eq!(
            second,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 2,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let runs_after_second = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        assert_eq!(
            runs_after_second
                .iter()
                .filter(|entry| entry.outcome == TaskRunOutcome::Failed)
                .count(),
            2
        );

        start_attempt(
            base,
            &milestone_id,
            "task-3",
            "run-3",
            base_time + Duration::seconds(21),
        )?;
        let third = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-3",
            "proj-failure",
            milestone_id.as_str(),
            "run-3",
            Some("plan-v1"),
            base_time + Duration::seconds(21),
            base_time + Duration::seconds(25),
            "third failure",
        )
        .await?;
        match third {
            FailureReconciliationOutcome::EscalatedToOperator {
                attempt_number,
                reason,
            } => {
                assert_eq!(attempt_number, 3);
                assert!(reason.contains("failed 3 times"));
                assert!(reason.contains("third failure"));
            }
            other => panic!("expected escalation on third failure, got {other:?}"),
        }

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
        )?
        .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::NeedsOperator
        );
        assert!(controller
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("failed 3 times")));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_is_idempotent_for_replayed_run(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let started_at = Utc::now() + Duration::seconds(1);

        start_attempt(base, &milestone_id, "task-1", "run-1", started_at)?;
        let first = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            started_at + Duration::seconds(10),
            "replayed failure",
        )
        .await?;
        assert_eq!(
            first,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let controller_journal_len =
            milestone_controller::MilestoneControllerPort::read_transition_journal(
                &FsMilestoneControllerStore,
                base,
                &milestone_id,
            )?
            .len();
        let failed_events_before = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .count();

        let replay = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            started_at + Duration::seconds(10),
            "replayed failure",
        )
        .await?;
        assert_eq!(replay, first);

        let failed_events_after = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .count();
        assert_eq!(failed_events_before, failed_events_after);

        let runs = milestone_service::find_runs_for_bead(
            &FsTaskRunLineageStore,
            base,
            &milestone_id,
            "bead-failure",
        )?;
        assert_eq!(
            runs.iter()
                .filter(|entry| entry.outcome == TaskRunOutcome::Failed)
                .count(),
            1
        );

        let controller_journal_after =
            milestone_controller::MilestoneControllerPort::read_transition_journal(
                &FsMilestoneControllerStore,
                base,
                &milestone_id,
            )?
            .len();
        assert_eq!(controller_journal_len, controller_journal_after);

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_replay_repairs_missing_snapshot_and_journal_state(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let started_at = Utc::now() + Duration::seconds(1);
        let failed_at = started_at + Duration::seconds(10);

        start_attempt(base, &milestone_id, "task-1", "run-1", started_at)?;
        FsTaskRunLineageStore.update_task_run(
            base,
            &milestone_id,
            "bead-failure",
            "proj-failure",
            "run-1",
            Some("plan-v1"),
            started_at,
            TaskRunOutcome::Failed,
            Some("task_id=task-1\nattempt=1\nerror=partial write".to_owned()),
            failed_at,
        )?;

        let snapshot_before =
            milestone_service::load_snapshot(&FsMilestoneSnapshotStore, base, &milestone_id)?;
        assert_eq!(snapshot_before.progress.failed_beads, 0);
        let failed_events_before = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .filter(|event| event.event_type == MilestoneEventType::BeadFailed)
            .count();
        assert_eq!(failed_events_before, 0);

        let outcome = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            failed_at,
            "partial write",
        )
        .await?;

        assert_eq!(
            outcome,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        let snapshot_after =
            milestone_service::load_snapshot(&FsMilestoneSnapshotStore, base, &milestone_id)?;
        assert_eq!(snapshot_after.progress.failed_beads, 1);
        let failed_event = read_journal(&FsMilestoneJournalStore, base, &milestone_id)?
            .into_iter()
            .find(|event| event.event_type == MilestoneEventType::BeadFailed)
            .expect("failure replay should repair bead_failed journal state");
        assert_eq!(failed_event.timestamp, failed_at);

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_replay_does_not_clobber_newer_running_retry(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let base_time = Utc::now();

        start_attempt(
            base,
            &milestone_id,
            "task-1",
            "run-1",
            base_time + Duration::seconds(1),
        )?;
        let first = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            base_time + Duration::seconds(1),
            base_time + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(
            first,
            FailureReconciliationOutcome::Retryable {
                attempt_number: 1,
                max_retries: MAX_FAILURE_RETRIES,
            }
        );

        start_attempt(
            base,
            &milestone_id,
            "task-1",
            "run-2",
            base_time + Duration::seconds(11),
        )?;
        let replay = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            base_time + Duration::seconds(1),
            base_time + Duration::seconds(5),
            "first failure",
        )
        .await?;
        assert_eq!(replay, first);

        let controller = milestone_controller::load_controller(
            &FsMilestoneControllerStore,
            base,
            &milestone_id,
        )?
        .expect("controller should exist");
        assert_eq!(
            controller.state,
            milestone_controller::MilestoneControllerState::Running
        );
        assert_eq!(controller.active_task_id.as_deref(), Some("task-1"));

        Ok(())
    }

    #[tokio::test]
    async fn reconcile_failure_does_not_mutate_beads_state(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        let export_before = write_beads_export(base)?;
        let milestone_id = create_test_milestone(base)?;
        let started_at = Utc::now() + Duration::seconds(1);

        start_attempt(base, &milestone_id, "task-1", "run-1", started_at)?;
        let _ = reconcile_failure(
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsTaskRunLineageStore,
            &FsMilestoneControllerStore,
            base,
            "bead-failure",
            "task-1",
            "proj-failure",
            milestone_id.as_str(),
            "run-1",
            Some("plan-v1"),
            started_at,
            started_at + Duration::seconds(10),
            "no bead mutation expected",
        )
        .await?;

        let export_after = std::fs::read_to_string(base.join(".beads/issues.jsonl"))?;
        assert_eq!(export_before, export_after);
        assert!(
            !base.join(".beads/.br-unsynced-mutations.d").exists(),
            "failure reconciliation should not prepare any br mutation records"
        );

        Ok(())
    }
}
