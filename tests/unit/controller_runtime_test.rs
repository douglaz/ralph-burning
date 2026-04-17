use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::Deserialize;

use ralph_burning::adapters::br_models::ReadyBead;
use ralph_burning::adapters::br_process::{BrAdapter, BrCommand, ProcessRunner};
use ralph_burning::adapters::bv_process::{
    BvAdapter, BvCommand, BvProcessRunner, NextBeadResponse,
};
use ralph_burning::adapters::fs::{
    FsMilestoneControllerStore, FsMilestoneJournalStore, FsMilestoneSnapshotStore,
    FsTaskRunLineageStore,
};
use ralph_burning::contexts::milestone_record::controller::{
    self, checkpoint_controller_stop, load_controller, resume_controller,
    sync_controller_task_claimed, sync_controller_task_reconciling, sync_controller_task_running,
    transition_controller, ControllerBeadStatus, ControllerTaskStatus, ControllerTransitionRequest,
    MilestoneControllerPort, MilestoneControllerResumePort, MilestoneControllerState,
    MilestoneControllerTransitionEvent,
};
use ralph_burning::contexts::milestone_record::model::{
    MilestoneEventType, MilestoneId, TaskRunOutcome,
};
use ralph_burning::contexts::milestone_record::service::{
    load_snapshot, read_journal, record_bead_completion, record_bead_start,
};
use ralph_burning::shared::error::{AppError, AppResult};
use ralph_burning::test_support::br::{MockBrAdapter, MockBrResponse};
use ralph_burning::test_support::bv::{MockBvAdapter, MockBvResponse};
use ralph_burning::test_support::fixtures::{MilestoneFixtureBuilder, TempWorkspaceBuilder};
use ralph_burning::test_support::logging::log_capture;

fn ts(minute_offset: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 4, 17, 12, 0, 0)
        .single()
        .expect("valid timestamp")
        + Duration::minutes(minute_offset)
}

fn build_workspace(
    milestone_slug: &str,
    bead_count: usize,
) -> AppResult<ralph_burning::test_support::fixtures::TempWorkspace> {
    let mut milestone =
        MilestoneFixtureBuilder::new(milestone_slug).with_name("Controller runtime");
    for index in 1..bead_count {
        milestone = milestone.add_bead(format!("Fixture bead {}", index + 1));
    }

    TempWorkspaceBuilder::new()
        .with_milestone(milestone)
        .build()
}

fn ready_beads_json(bead_ids: &[&str]) -> String {
    serde_json::to_string(
        &bead_ids
            .iter()
            .map(|bead_id| {
                serde_json::json!({
                    "id": bead_id,
                    "title": format!("Title for {bead_id}"),
                    "priority": 2,
                    "issue_type": "task",
                    "labels": [],
                })
            })
            .collect::<Vec<_>>(),
    )
    .expect("serialize ready beads")
}

fn next_bead_json(bead_id: &str, title: &str) -> String {
    serde_json::json!({
        "id": bead_id,
        "title": title,
        "score": 9.8,
        "reasons": ["ready"],
        "action": "implement",
    })
    .to_string()
}

fn no_work_json() -> String {
    serde_json::json!({
        "message": "No actionable beads are currently ready",
    })
    .to_string()
}

fn controller_journal(
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<Vec<MilestoneControllerTransitionEvent>> {
    MilestoneControllerPort::read_transition_journal(
        &FsMilestoneControllerStore,
        base_dir,
        milestone_id,
    )
}

fn latest_controller_state(
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<controller::MilestoneControllerRecord> {
    load_controller(&FsMilestoneControllerStore, base_dir, milestone_id)?.ok_or_else(|| {
        AppError::CorruptRecord {
            file: format!("milestones/{milestone_id}/controller.json"),
            details: "controller was not persisted".to_owned(),
        }
    })
}

#[derive(Debug, Deserialize)]
struct MessageOnlyResponse {
    #[allow(dead_code)]
    message: String,
}

#[derive(Default)]
struct FakeResumeRuntime {
    bead_statuses: HashMap<String, ControllerBeadStatus>,
    task_statuses: HashMap<String, ControllerTaskStatus>,
    ready_beads: bool,
    all_closed: bool,
}

impl FakeResumeRuntime {
    fn with_bead_status(mut self, bead_id: &str, status: ControllerBeadStatus) -> Self {
        self.bead_statuses.insert(bead_id.to_owned(), status);
        self
    }

    fn with_task_status(mut self, task_id: &str, status: ControllerTaskStatus) -> Self {
        self.task_statuses.insert(task_id.to_owned(), status);
        self
    }

    fn with_ready_beads(mut self, ready_beads: bool) -> Self {
        self.ready_beads = ready_beads;
        self
    }

    fn with_all_closed(mut self, all_closed: bool) -> Self {
        self.all_closed = all_closed;
        self
    }
}

impl MilestoneControllerResumePort for FakeResumeRuntime {
    fn bead_status(&self, bead_id: &str) -> AppResult<ControllerBeadStatus> {
        Ok(self
            .bead_statuses
            .get(bead_id)
            .copied()
            .unwrap_or(ControllerBeadStatus::Open))
    }

    fn task_status(&self, task_id: &str) -> AppResult<ControllerTaskStatus> {
        Ok(self
            .task_statuses
            .get(task_id)
            .copied()
            .unwrap_or(ControllerTaskStatus::Missing))
    }

    fn has_ready_beads(&self) -> AppResult<bool> {
        Ok(self.ready_beads)
    }

    fn all_beads_closed(&self) -> AppResult<bool> {
        Ok(self.all_closed)
    }
}

async fn select_next_bead_with_mocks<R: ProcessRunner, V: BvProcessRunner>(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    br: &BrAdapter<R>,
    bv: &BvAdapter<V>,
    now: DateTime<Utc>,
) -> AppResult<controller::MilestoneControllerRecord> {
    controller::sync_controller_state(
        &FsMilestoneControllerStore,
        base_dir,
        milestone_id,
        ControllerTransitionRequest::new(
            MilestoneControllerState::Selecting,
            "requesting the next bead recommendation from bv before any claim",
        ),
        now,
    )?;

    let bv_output = bv
        .exec_read(&BvCommand::robot_next())
        .await
        .map_err(|error| AppError::ResumeFailed {
            reason: format!(
                "milestone controller selection could not query bv --robot-next: {error}"
            ),
        })?;
    let recommendation = serde_json::from_str::<NextBeadResponse>(&bv_output.stdout)
        .map(Some)
        .or_else(|_| serde_json::from_str::<MessageOnlyResponse>(&bv_output.stdout).map(|_| None))
        .map_err(|error| AppError::ResumeFailed {
            reason: format!(
                "milestone controller selection could not parse bv --robot-next output: {error}"
            ),
        })?;

    let ready_beads = br
        .exec_json::<Vec<ReadyBead>>(&BrCommand::ready())
        .await
        .map_err(|error| AppError::ResumeFailed {
            reason: format!("milestone controller selection could not query br ready: {error}"),
        })?;

    match recommendation {
        Some(recommendation) => {
            let ready_match = ready_beads
                .iter()
                .find(|candidate| candidate.id == recommendation.id)
                .map(|candidate| candidate.id.as_str());
            if let Some(ready_bead_id) = ready_match {
                controller::sync_controller_state(
                    &FsMilestoneControllerStore,
                    base_dir,
                    milestone_id,
                    ControllerTransitionRequest::new(
                        MilestoneControllerState::Claimed,
                        format!(
                            "bv recommended bead '{}' and br ready confirmed it is actionable",
                            recommendation.id
                        ),
                    )
                    .with_bead(ready_bead_id),
                    now + Duration::seconds(1),
                )
            } else {
                controller::sync_controller_state(
                    &FsMilestoneControllerStore,
                    base_dir,
                    milestone_id,
                    ControllerTransitionRequest::new(
                        MilestoneControllerState::Blocked,
                        format!(
                            "bv recommended bead '{}', but br ready did not confirm it for milestone '{}'",
                            recommendation.id, milestone_id
                        ),
                    ),
                    now + Duration::seconds(1),
                )
            }
        }
        None => controller::sync_controller_state(
            &FsMilestoneControllerStore,
            base_dir,
            milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Blocked,
                if ready_beads.is_empty() {
                    "bv reported no actionable bead and br ready returned no ready beads".to_owned()
                } else {
                    format!(
                        "bv reported no actionable bead while br ready listed {} candidate(s)",
                        ready_beads.len()
                    )
                },
            ),
            now + Duration::seconds(1),
        ),
    }
}

async fn escalate_br_failure_to_needs_operator<R: ProcessRunner>(
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    task_id: &str,
    br: &BrAdapter<R>,
    now: DateTime<Utc>,
) -> AppResult<controller::MilestoneControllerRecord> {
    let error = br
        .exec_read(&BrCommand::close(bead_id, "Completed"))
        .await
        .expect_err("mock br failure should be exercised");

    tracing::error!(
        operation = "controller_runtime_br_failure",
        bead_id = bead_id,
        task_id = task_id,
        error = %error,
        "controller runtime escalated br failure to operator"
    );

    transition_controller(
        &FsMilestoneControllerStore,
        base_dir,
        milestone_id,
        ControllerTransitionRequest::new(
            MilestoneControllerState::NeedsOperator,
            format!("br close failed for active task '{task_id}': {error}"),
        )
        .with_bead(bead_id)
        .with_task(task_id),
        now,
    )
}

#[tokio::test]
async fn test_happy_path_idle_through_reconcile_to_completed() -> AppResult<()> {
    let capture = log_capture();
    let workspace = build_workspace("ms-runtime-happy", 1)?;
    let base_dir = workspace.path();
    let milestone_id = workspace.milestones[0].milestone_id.clone();
    let bead_id = format!("{}.bead-1", milestone_id.as_str());
    let plan_hash = workspace.milestones[0]
        .snapshot
        .plan_hash
        .clone()
        .expect("fixture milestone has plan hash");

    let br = MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&[
        bead_id.as_str()
    ]))]);
    let bv = MockBvAdapter::from_responses([MockBvResponse::success(next_bead_json(
        &bead_id,
        "Bootstrap fixture bead",
    ))]);

    let (started_snapshot, completed_snapshot) = capture
        .in_scope_async(async {
            controller::initialize_controller(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone_id,
                ts(0),
            )?;
            select_next_bead_with_mocks(
                base_dir,
                &milestone_id,
                &br.as_br_adapter(),
                &bv.as_bv_adapter(),
                ts(1),
            )
            .await?;
            sync_controller_task_running(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone_id,
                &bead_id,
                "task-happy-1",
                "task execution started",
                ts(2),
            )?;
            record_bead_start(
                &FsMilestoneSnapshotStore,
                &FsMilestoneJournalStore,
                &FsTaskRunLineageStore,
                base_dir,
                &milestone_id,
                &bead_id,
                "project-happy-1",
                "run-happy-1",
                &plan_hash,
                ts(2),
            )?;
            let started_snapshot =
                load_snapshot(&FsMilestoneSnapshotStore, base_dir, &milestone_id)?;

            sync_controller_task_reconciling(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone_id,
                &bead_id,
                "task-happy-1",
                "task completed; reconciling",
                ts(3),
            )?;
            record_bead_completion(
                &FsMilestoneSnapshotStore,
                &FsMilestoneJournalStore,
                &FsTaskRunLineageStore,
                base_dir,
                &milestone_id,
                &bead_id,
                "project-happy-1",
                "run-happy-1",
                Some(&plan_hash),
                TaskRunOutcome::Succeeded,
                Some("completed by runtime test"),
                ts(2),
                ts(3),
            )?;
            transition_controller(
                &FsMilestoneControllerStore,
                base_dir,
                &milestone_id,
                ControllerTransitionRequest::new(
                    MilestoneControllerState::Completed,
                    "reconciliation recorded the successful bead completion",
                ),
                ts(4),
            )?;
            let completed_snapshot =
                load_snapshot(&FsMilestoneSnapshotStore, base_dir, &milestone_id)?;

            Ok::<_, AppError>((started_snapshot, completed_snapshot))
        })
        .await?;

    let controller = latest_controller_state(base_dir, &milestone_id)?;
    assert_eq!(controller.state, MilestoneControllerState::Completed);
    assert_eq!(controller.active_bead_id, None);
    assert_eq!(controller.active_task_id, None);

    let transitions = controller_journal(base_dir, &milestone_id)?;
    let states = transitions
        .iter()
        .map(|event| event.to_state)
        .collect::<Vec<_>>();
    assert_eq!(
        states,
        vec![
            MilestoneControllerState::Idle,
            MilestoneControllerState::Selecting,
            MilestoneControllerState::Claimed,
            MilestoneControllerState::Running,
            MilestoneControllerState::Reconciling,
            MilestoneControllerState::Completed,
        ]
    );

    let milestone_journal = read_journal(&FsMilestoneJournalStore, base_dir, &milestone_id)?;
    assert_eq!(
        milestone_journal
            .iter()
            .filter(|event| {
                event.event_type == MilestoneEventType::BeadStarted
                    && event.bead_id.as_deref() == Some(bead_id.as_str())
            })
            .count(),
        1
    );
    assert_eq!(
        milestone_journal
            .iter()
            .filter(|event| {
                event.event_type == MilestoneEventType::BeadCompleted
                    && event.bead_id.as_deref() == Some(bead_id.as_str())
            })
            .count(),
        1
    );
    assert_eq!(started_snapshot.progress.total_beads, 1);
    assert_eq!(started_snapshot.progress.in_progress_beads, 1);
    assert_eq!(started_snapshot.progress.completed_beads, 0);
    assert_eq!(completed_snapshot.progress.completed_beads, 1);
    assert_eq!(completed_snapshot.progress.in_progress_beads, 0);

    capture.assert_event_has_fields(&[
        ("operation", "record_bead_start"),
        ("outcome", "success"),
        ("run_id", "run-happy-1"),
    ]);
    capture.assert_event_has_fields(&[
        ("operation", "record_bead_completion"),
        ("outcome", "success"),
        ("run_id", "run-happy-1"),
    ]);

    Ok(())
}

#[tokio::test]
async fn test_blocked_when_no_ready_beads() -> AppResult<()> {
    let capture = log_capture();
    let workspace = build_workspace("ms-runtime-blocked", 1)?;
    let base_dir = workspace.path();
    let milestone_id = workspace.milestones[0].milestone_id.clone();
    let bead_id = format!("{}.bead-1", milestone_id.as_str());

    controller::initialize_controller(&FsMilestoneControllerStore, base_dir, &milestone_id, ts(0))?;

    let blocked = capture
        .in_scope_async(async {
            select_next_bead_with_mocks(
                base_dir,
                &milestone_id,
                &MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&[]))])
                    .as_br_adapter(),
                &MockBvAdapter::from_responses([MockBvResponse::success(no_work_json())])
                    .as_bv_adapter(),
                ts(1),
            )
            .await
        })
        .await?;

    assert_eq!(blocked.state, MilestoneControllerState::Blocked);
    assert_eq!(blocked.active_bead_id, None);
    assert!(blocked
        .last_transition_reason
        .as_deref()
        .is_some_and(|reason| reason.contains("no ready beads")));

    let resumed = resume_controller(
        &FsMilestoneControllerStore,
        &FakeResumeRuntime::default()
            .with_ready_beads(true)
            .with_all_closed(false),
        base_dir,
        &milestone_id,
        ts(2),
    )?;
    assert_eq!(resumed.state, MilestoneControllerState::Selecting);
    assert!(resumed
        .last_transition_reason
        .as_deref()
        .is_some_and(|reason| reason.contains("ready beads")));

    let claimed = select_next_bead_with_mocks(
        base_dir,
        &milestone_id,
        &MockBrAdapter::from_responses([MockBrResponse::success(ready_beads_json(&[
            bead_id.as_str()
        ]))])
        .as_br_adapter(),
        &MockBvAdapter::from_responses([MockBvResponse::success(next_bead_json(
            &bead_id,
            "Bootstrap fixture bead",
        ))])
        .as_bv_adapter(),
        ts(3),
    )
    .await?;
    assert_eq!(claimed.state, MilestoneControllerState::Claimed);
    assert_eq!(claimed.active_bead_id.as_deref(), Some(bead_id.as_str()));

    let transitions = controller_journal(base_dir, &milestone_id)?;
    assert!(
        transitions
            .iter()
            .any(|event| event.to_state == MilestoneControllerState::Blocked),
        "blocked transition should be persisted: {transitions:?}"
    );
    assert!(
        transitions
            .iter()
            .any(|event| event.reason.contains("resume found ready beads")),
        "resume transition should explain why selection resumed: {transitions:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_restart_resumes_from_persisted_state() -> AppResult<()> {
    let capture = log_capture();
    let workspace = build_workspace("ms-runtime-restart", 1)?;
    let base_dir = workspace.path();
    let milestone_id = workspace.milestones[0].milestone_id.clone();
    let bead_id = format!("{}.bead-1", milestone_id.as_str());
    let task_id = "task-restart-1";

    controller::initialize_controller(&FsMilestoneControllerStore, base_dir, &milestone_id, ts(0))?;
    sync_controller_task_claimed(
        &FsMilestoneControllerStore,
        base_dir,
        &milestone_id,
        &bead_id,
        task_id,
        "task claim persisted before restart",
        ts(1),
    )?;
    sync_controller_task_running(
        &FsMilestoneControllerStore,
        base_dir,
        &milestone_id,
        &bead_id,
        task_id,
        "task execution started before restart",
        ts(2),
    )?;

    let events_before_checkpoint = controller_journal(base_dir, &milestone_id)?.len();
    capture.in_scope(|| {
        checkpoint_controller_stop(&FsMilestoneControllerStore, base_dir, &milestone_id, ts(3))
    })?;

    let persisted = latest_controller_state(base_dir, &milestone_id)?;
    assert_eq!(persisted.state, MilestoneControllerState::Running);
    assert_eq!(persisted.active_bead_id.as_deref(), Some(bead_id.as_str()));
    assert_eq!(persisted.active_task_id.as_deref(), Some(task_id));

    let resumed = resume_controller(
        &FsMilestoneControllerStore,
        &FakeResumeRuntime::default()
            .with_bead_status(&bead_id, ControllerBeadStatus::Open)
            .with_task_status(task_id, ControllerTaskStatus::Running)
            .with_ready_beads(false)
            .with_all_closed(false),
        base_dir,
        &milestone_id,
        ts(4),
    )?;

    assert_eq!(resumed.state, MilestoneControllerState::Running);
    assert_eq!(resumed.active_bead_id.as_deref(), Some(bead_id.as_str()));
    assert_eq!(resumed.active_task_id.as_deref(), Some(task_id));
    assert_eq!(
        controller_journal(base_dir, &milestone_id)?.len(),
        events_before_checkpoint,
        "resume should not append a duplicate running transition"
    );
    capture.assert_event_has_fields(&[
        ("operation", "checkpoint_controller_stop"),
        ("outcome", "success"),
    ]);

    Ok(())
}

#[tokio::test]
async fn test_tool_failure_transitions_to_needs_operator() -> AppResult<()> {
    let capture = log_capture();
    let workspace = build_workspace("ms-runtime-tool-failure", 1)?;
    let base_dir = workspace.path();
    let milestone_id = workspace.milestones[0].milestone_id.clone();
    let bead_id = format!("{}.bead-1", milestone_id.as_str());
    let task_id = "task-failure-1";

    controller::initialize_controller(&FsMilestoneControllerStore, base_dir, &milestone_id, ts(0))?;
    sync_controller_task_claimed(
        &FsMilestoneControllerStore,
        base_dir,
        &milestone_id,
        &bead_id,
        task_id,
        "task claim recorded for failure path",
        ts(1),
    )?;
    sync_controller_task_running(
        &FsMilestoneControllerStore,
        base_dir,
        &milestone_id,
        &bead_id,
        task_id,
        "task execution started for failure path",
        ts(2),
    )?;

    let failed = capture
        .in_scope_async(async {
            escalate_br_failure_to_needs_operator(
                base_dir,
                &milestone_id,
                &bead_id,
                task_id,
                &MockBrAdapter::from_responses([MockBrResponse::exit_failure(
                    17,
                    "simulated br close failure",
                )])
                .as_br_adapter(),
                ts(3),
            )
            .await
        })
        .await?;

    assert_eq!(failed.state, MilestoneControllerState::NeedsOperator);
    assert_eq!(failed.active_bead_id.as_deref(), Some(bead_id.as_str()));
    assert_eq!(failed.active_task_id.as_deref(), Some(task_id));
    assert!(failed
        .last_transition_reason
        .as_deref()
        .is_some_and(|reason| {
            reason.contains("simulated br close failure") && reason.contains(task_id)
        }));

    let transitions = controller_journal(base_dir, &milestone_id)?;
    let failure_event = transitions
        .last()
        .expect("needs-operator transition should be recorded");
    assert_eq!(
        failure_event.to_state,
        MilestoneControllerState::NeedsOperator
    );
    assert!(failure_event.reason.contains("simulated br close failure"));

    capture.assert_event_has_fields(&[
        ("operation", "controller_runtime_br_failure"),
        ("bead_id", bead_id.as_str()),
        ("task_id", task_id),
        (
            "message",
            "controller runtime escalated br failure to operator",
        ),
    ]);

    Ok(())
}

#[tokio::test]
async fn test_sequential_execution_enforced() -> AppResult<()> {
    let workspace = build_workspace("ms-runtime-invariant", 2)?;
    let base_dir = workspace.path();
    let milestone_id = workspace.milestones[0].milestone_id.clone();
    let first_bead_id = format!("{}.bead-1", milestone_id.as_str());
    let second_bead_id = format!("{}.bead-2", milestone_id.as_str());

    controller::initialize_controller(&FsMilestoneControllerStore, base_dir, &milestone_id, ts(0))?;
    sync_controller_task_claimed(
        &FsMilestoneControllerStore,
        base_dir,
        &milestone_id,
        &first_bead_id,
        "task-primary",
        "claimed first bead",
        ts(1),
    )?;
    sync_controller_task_running(
        &FsMilestoneControllerStore,
        base_dir,
        &milestone_id,
        &first_bead_id,
        "task-primary",
        "running first bead",
        ts(2),
    )?;

    let journal_len_before = controller_journal(base_dir, &milestone_id)?.len();
    let error = sync_controller_task_claimed(
        &FsMilestoneControllerStore,
        base_dir,
        &milestone_id,
        &second_bead_id,
        "task-secondary",
        "attempted second claim while first task was still running",
        ts(3),
    )
    .expect_err("controller should reject a second active bead");
    assert!(error.to_string().contains(&format!(
        "controller is already tracking active bead '{first_bead_id}'"
    )));

    let controller = latest_controller_state(base_dir, &milestone_id)?;
    assert_eq!(controller.state, MilestoneControllerState::Running);
    assert_eq!(
        controller.active_bead_id.as_deref(),
        Some(first_bead_id.as_str())
    );
    assert_eq!(controller.active_task_id.as_deref(), Some("task-primary"));
    assert_eq!(
        controller_journal(base_dir, &milestone_id)?.len(),
        journal_len_before,
        "rejected claim must not create a second active transition"
    );

    Ok(())
}
