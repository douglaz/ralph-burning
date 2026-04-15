use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::shared::error::{AppError, AppResult};

use super::bead_refs::milestone_bead_refs_match;
use super::model::MilestoneId;

pub const MILESTONE_CONTROLLER_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MilestoneControllerState {
    Idle,
    Selecting,
    Claimed,
    Running,
    Reconciling,
    Blocked,
    NeedsOperator,
    Completed,
}

impl MilestoneControllerState {
    pub fn allows_transition_to(self, next: Self) -> bool {
        use MilestoneControllerState as State;

        matches!(
            (self, next),
            (State::Idle, State::Selecting)
                | (State::Idle, State::Claimed)
                | (State::Idle, State::Blocked)
                | (State::Idle, State::Completed)
                | (State::Idle, State::NeedsOperator)
                | (State::Selecting, State::Claimed)
                | (State::Selecting, State::Blocked)
                | (State::Selecting, State::Completed)
                | (State::Selecting, State::NeedsOperator)
                | (State::Claimed, State::Running)
                | (State::Claimed, State::Reconciling)
                | (State::Claimed, State::NeedsOperator)
                | (State::Running, State::Claimed)
                | (State::Running, State::Reconciling)
                | (State::Running, State::NeedsOperator)
                | (State::Reconciling, State::Idle)
                | (State::Reconciling, State::Selecting)
                | (State::Reconciling, State::Blocked)
                | (State::Reconciling, State::Completed)
                | (State::Reconciling, State::NeedsOperator)
                | (State::Blocked, State::Selecting)
                | (State::Blocked, State::Claimed)
                | (State::Blocked, State::Completed)
                | (State::Blocked, State::NeedsOperator)
                | (State::NeedsOperator, State::Idle)
                | (State::NeedsOperator, State::Selecting)
                | (State::NeedsOperator, State::Claimed)
                | (State::NeedsOperator, State::Running)
                | (State::NeedsOperator, State::Reconciling)
                | (State::NeedsOperator, State::Blocked)
                | (State::NeedsOperator, State::Completed)
                | (State::Completed, State::NeedsOperator)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MilestoneControllerRecord {
    pub schema_version: u32,
    pub milestone_id: MilestoneId,
    pub state: MilestoneControllerState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_bead_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_task_id: Option<String>,
    pub last_transition_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_transition_reason: Option<String>,
}

impl MilestoneControllerRecord {
    pub fn idle(milestone_id: MilestoneId, now: DateTime<Utc>) -> Self {
        Self {
            schema_version: MILESTONE_CONTROLLER_SCHEMA_VERSION,
            milestone_id,
            state: MilestoneControllerState::Idle,
            active_bead_id: None,
            active_task_id: None,
            last_transition_at: now,
            updated_at: now,
            last_transition_reason: None,
        }
    }

    fn transitioned(
        milestone_id: MilestoneId,
        state: MilestoneControllerState,
        active_bead_id: Option<String>,
        active_task_id: Option<String>,
        reason: Option<String>,
        last_transition_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> AppResult<Self> {
        let record = Self {
            schema_version: MILESTONE_CONTROLLER_SCHEMA_VERSION,
            milestone_id,
            state,
            active_bead_id,
            active_task_id,
            last_transition_at,
            updated_at,
            last_transition_reason: reason,
        };
        record.validate_semantics()?;
        Ok(record)
    }

    pub fn validate_semantics(&self) -> AppResult<()> {
        validate_state_context(
            &self.milestone_id,
            self.state,
            self.active_bead_id.as_deref(),
            self.active_task_id.as_deref(),
        )?;

        if self.updated_at < self.last_transition_at {
            return Err(controller_corrupt_record(
                &self.milestone_id,
                "controller.json",
                "updated_at is earlier than last_transition_at",
            ));
        }

        Ok(())
    }

    fn matches_transition_event(&self, event: &MilestoneControllerTransitionEvent) -> bool {
        self.state == event.to_state
            && self.active_bead_id == event.bead_id
            && self.active_task_id == event.task_id
            && self.last_transition_at == event.timestamp
            && self.last_transition_reason.as_deref() == Some(event.reason.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MilestoneControllerTransitionEvent {
    pub timestamp: DateTime<Utc>,
    pub from_state: MilestoneControllerState,
    pub to_state: MilestoneControllerState,
    pub bead_id: Option<String>,
    pub task_id: Option<String>,
    pub reason: String,
}

impl MilestoneControllerTransitionEvent {
    fn new(
        timestamp: DateTime<Utc>,
        from_state: MilestoneControllerState,
        request: &ControllerTransitionRequest,
    ) -> Self {
        Self {
            timestamp,
            from_state,
            to_state: request.to_state,
            bead_id: request.bead_id.clone(),
            task_id: request.task_id.clone(),
            reason: request.reason.clone(),
        }
    }

    pub fn to_ndjson_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControllerTransitionRequest {
    pub to_state: MilestoneControllerState,
    pub bead_id: Option<String>,
    pub task_id: Option<String>,
    pub reason: String,
}

impl ControllerTransitionRequest {
    pub fn new(to_state: MilestoneControllerState, reason: impl Into<String>) -> Self {
        Self {
            to_state,
            bead_id: None,
            task_id: None,
            reason: reason.into(),
        }
    }

    pub fn with_bead(mut self, bead_id: impl Into<String>) -> Self {
        self.bead_id = Some(bead_id.into());
        self
    }

    pub fn with_task(mut self, task_id: impl Into<String>) -> Self {
        self.task_id = Some(task_id.into());
        self
    }
}

pub trait MilestoneControllerPort {
    fn read_controller(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Option<MilestoneControllerRecord>>;
    fn write_controller(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        controller: &MilestoneControllerRecord,
    ) -> AppResult<()>;
    fn read_transition_journal(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
    ) -> AppResult<Vec<MilestoneControllerTransitionEvent>>;
    fn append_transition_event(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        event: &MilestoneControllerTransitionEvent,
    ) -> AppResult<()>;
    fn with_controller_lock<T, F>(
        &self,
        base_dir: &Path,
        milestone_id: &MilestoneId,
        operation: F,
    ) -> AppResult<T>
    where
        F: FnOnce() -> AppResult<T>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerBeadStatus {
    Open,
    Closed,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerTaskStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Missing,
}

pub trait MilestoneControllerResumePort {
    fn bead_status(&self, bead_id: &str) -> AppResult<ControllerBeadStatus>;
    fn task_status(&self, task_id: &str) -> AppResult<ControllerTaskStatus>;
    fn has_ready_beads(&self) -> AppResult<bool>;
    fn all_beads_closed(&self) -> AppResult<bool>;
}

pub fn initialize_controller(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    initialize_controller_with_request(
        store,
        base_dir,
        milestone_id,
        ControllerTransitionRequest::new(
            MilestoneControllerState::Idle,
            controller_initialization_reason(MilestoneControllerState::Idle),
        ),
        now,
    )
}

pub fn initialize_controller_with_state(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    initial_state: MilestoneControllerState,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    initialize_controller_with_request(
        store,
        base_dir,
        milestone_id,
        ControllerTransitionRequest::new(
            initial_state,
            controller_initialization_reason(initial_state),
        ),
        now,
    )
}

pub fn initialize_controller_with_request(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    request: ControllerTransitionRequest,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    store.with_controller_lock(base_dir, milestone_id, || {
        if let Some(existing) = hydrate_controller_locked(store, base_dir, milestone_id)? {
            existing.validate_semantics()?;
            return Ok(existing);
        }

        initialize_controller_from_request_locked(store, base_dir, milestone_id, request, now)
    })
}

pub fn sync_controller_task_claimed(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    task_id: &str,
    reason: impl Into<String>,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    store.with_controller_lock(base_dir, milestone_id, || {
        let reason = reason.into();
        let claim_request = ControllerTransitionRequest::new(
            MilestoneControllerState::Claimed,
            reason.clone(),
        )
        .with_bead(bead_id)
        .with_task(task_id);
        let Some(current) = hydrate_controller_locked(store, base_dir, milestone_id)? else {
            return initialize_controller_from_request_locked(
                store,
                base_dir,
                milestone_id,
                claim_request,
                now,
            );
        };
        current.validate_semantics()?;

        match current.state {
            MilestoneControllerState::Claimed => {
                validate_active_context_alignment(&current, milestone_id, bead_id, task_id)?;
                // Use sync_existing_state_locked so that adopting a task_id
                // (when the controller was claimed without one) appends a
                // durable journal event. Pure same-bead/same-task checkpoints
                // also pass through safely (sync detects no-change and
                // checkpoints without a new event).
                sync_existing_state_locked(
                    store,
                    base_dir,
                    milestone_id,
                    current,
                    claim_request,
                    now,
                )
            }
            MilestoneControllerState::Idle
            | MilestoneControllerState::Selecting
            | MilestoneControllerState::Running
            | MilestoneControllerState::Blocked
            | MilestoneControllerState::NeedsOperator => {
                validate_active_context_alignment(&current, milestone_id, bead_id, task_id)?;
                transition_from_current_locked(
                    store,
                    base_dir,
                    milestone_id,
                    current,
                    claim_request,
                    now,
                )
            }
            MilestoneControllerState::Reconciling => Err(controller_corrupt_record(
                milestone_id,
                "controller.json",
                format!(
                    "cannot move controller from '{}' back to 'claimed' while reconciliation is active",
                    state_name(current.state)
                ),
            )),
            MilestoneControllerState::Completed => Err(controller_corrupt_record(
                milestone_id,
                "controller.json",
                "cannot re-claim a task after the controller marked the milestone completed",
            )),
        }
    })
}

pub fn sync_controller_task_running(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    task_id: &str,
    reason: impl Into<String>,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    store.with_controller_lock(base_dir, milestone_id, || {
        let reason = reason.into();
        let running_request = ControllerTransitionRequest::new(
            MilestoneControllerState::Running,
            reason.clone(),
        )
        .with_bead(bead_id)
        .with_task(task_id);
        let Some(current) = hydrate_controller_locked(store, base_dir, milestone_id)? else {
            return initialize_controller_from_request_locked(
                store,
                base_dir,
                milestone_id,
                running_request,
                now,
            );
        };
        current.validate_semantics()?;
        validate_active_context_alignment(&current, milestone_id, bead_id, task_id)?;

        match current.state {
            MilestoneControllerState::Running => {
                checkpoint_existing_controller_locked(store, base_dir, milestone_id, current, now)
            }
            MilestoneControllerState::Claimed => transition_from_current_locked(
                store,
                base_dir,
                milestone_id,
                current,
                running_request,
                now,
            ),
            MilestoneControllerState::Idle
            | MilestoneControllerState::Selecting
            | MilestoneControllerState::Blocked
            | MilestoneControllerState::NeedsOperator => {
                let claimed = transition_from_current_locked(
                    store,
                    base_dir,
                    milestone_id,
                    current,
                    ControllerTransitionRequest::new(
                        MilestoneControllerState::Claimed,
                        "controller adopted the bead-linked task before execution",
                    )
                    .with_bead(bead_id)
                    .with_task(task_id),
                    now,
                )?;
                transition_from_current_locked(
                    store,
                    base_dir,
                    milestone_id,
                    claimed,
                    running_request,
                    now,
                )
            }
            MilestoneControllerState::Reconciling => Err(controller_corrupt_record(
                milestone_id,
                "controller.json",
                "cannot restart a running task while the controller is still reconciling the prior outcome",
            )),
            MilestoneControllerState::Completed => Err(controller_corrupt_record(
                milestone_id,
                "controller.json",
                "cannot restart a task after the controller marked the milestone completed",
            )),
        }
    })
}

pub fn sync_controller_task_reconciling(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    bead_id: &str,
    task_id: &str,
    reason: impl Into<String>,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    store.with_controller_lock(base_dir, milestone_id, || {
        let reason = reason.into();
        let reconciling_request =
            ControllerTransitionRequest::new(MilestoneControllerState::Reconciling, reason.clone())
                .with_bead(bead_id)
                .with_task(task_id);
        let Some(current) = hydrate_controller_locked(store, base_dir, milestone_id)? else {
            return initialize_controller_from_request_locked(
                store,
                base_dir,
                milestone_id,
                reconciling_request,
                now,
            );
        };
        current.validate_semantics()?;
        validate_active_context_alignment(&current, milestone_id, bead_id, task_id)?;

        match current.state {
            MilestoneControllerState::Reconciling => {
                checkpoint_existing_controller_locked(store, base_dir, milestone_id, current, now)
            }
            MilestoneControllerState::Claimed | MilestoneControllerState::Running => {
                transition_from_current_locked(
                    store,
                    base_dir,
                    milestone_id,
                    current,
                    reconciling_request,
                    now,
                )
            }
            MilestoneControllerState::Idle
            | MilestoneControllerState::Selecting
            | MilestoneControllerState::Blocked
            | MilestoneControllerState::NeedsOperator => {
                let claimed = transition_from_current_locked(
                    store,
                    base_dir,
                    milestone_id,
                    current,
                    ControllerTransitionRequest::new(
                        MilestoneControllerState::Claimed,
                        "controller adopted the bead-linked task before reconciliation",
                    )
                    .with_bead(bead_id)
                    .with_task(task_id),
                    now,
                )?;
                transition_from_current_locked(
                    store,
                    base_dir,
                    milestone_id,
                    claimed,
                    reconciling_request,
                    now,
                )
            }
            MilestoneControllerState::Completed => {
                checkpoint_existing_controller_locked(store, base_dir, milestone_id, current, now)
            }
        }
    })
}

fn initialize_controller_from_request_locked(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    request: ControllerTransitionRequest,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    let controller = MilestoneControllerRecord::transitioned(
        milestone_id.clone(),
        request.to_state,
        request.bead_id.clone(),
        request.task_id.clone(),
        Some(request.reason.clone()),
        now,
        now,
    )?;
    let event = MilestoneControllerTransitionEvent::new(now, request.to_state, &request);
    store.append_transition_event(base_dir, milestone_id, &event)?;
    store.write_controller(base_dir, milestone_id, &controller)?;
    Ok(controller)
}

fn checkpoint_existing_controller_locked(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    mut controller: MilestoneControllerRecord,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    controller.updated_at = controller
        .updated_at
        .max(now)
        .max(controller.last_transition_at);
    store.write_controller(base_dir, milestone_id, &controller)?;
    Ok(controller)
}

fn same_state_context_error_details(
    state: MilestoneControllerState,
    milestone_id: &MilestoneId,
    current_bead_id: Option<&str>,
    current_task_id: Option<&str>,
    next_bead_id: Option<&str>,
    next_task_id: Option<&str>,
) -> Option<String> {
    match (current_bead_id, next_bead_id) {
        (Some(current), Some(next))
            if !milestone_bead_ids_equivalent(milestone_id, current, next) =>
        {
            return Some(format!(
                "same-state sync for '{}' must preserve active bead identifier '{}'",
                state_name(state),
                current
            ));
        }
        (Some(current), None) => {
            return Some(format!(
                "same-state sync for '{}' must preserve active bead identifier '{}'",
                state_name(state),
                current
            ));
        }
        (None, Some(next)) => {
            return Some(format!(
                "same-state sync for '{}' must not introduce active bead identifier '{}'",
                state_name(state),
                next
            ));
        }
        _ => {}
    }

    match (current_task_id, next_task_id) {
        (Some(current), Some(next)) if current != next => Some(format!(
            "same-state sync for '{}' must preserve active task identifier '{}'",
            state_name(state),
            current
        )),
        (Some(current), None) => Some(format!(
            "same-state sync for '{}' must preserve active task identifier '{}'",
            state_name(state),
            current
        )),
        (None, Some(next)) => {
            // Claimed state allows task_id to be None (selection sets bead_id
            // before a project exists). Adopting the task_id during a same-
            // state sync is the expected way to link the project after creation.
            if state == MilestoneControllerState::Claimed {
                None
            } else {
                Some(format!(
                    "same-state sync for '{}' must not introduce active task identifier '{}'",
                    state_name(state),
                    next
                ))
            }
        }
        _ => None,
    }
}

fn sync_existing_state_locked(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    current: MilestoneControllerRecord,
    request: ControllerTransitionRequest,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    let bead_id = request
        .bead_id
        .clone()
        .or_else(|| current.active_bead_id.clone());
    let task_id = request
        .task_id
        .clone()
        .or_else(|| current.active_task_id.clone());

    if let Some(details) = same_state_context_error_details(
        current.state,
        milestone_id,
        current.active_bead_id.as_deref(),
        current.active_task_id.as_deref(),
        bead_id.as_deref(),
        task_id.as_deref(),
    ) {
        return Err(controller_corrupt_record(
            milestone_id,
            "controller.json",
            details,
        ));
    }

    if current.last_transition_reason.as_deref() == Some(request.reason.as_str())
        && current.active_bead_id == bead_id
        && current.active_task_id == task_id
    {
        return checkpoint_existing_controller_locked(store, base_dir, milestone_id, current, now);
    }

    let event = MilestoneControllerTransitionEvent::new(
        now,
        current.state,
        &ControllerTransitionRequest {
            to_state: current.state,
            bead_id: bead_id.clone(),
            task_id: task_id.clone(),
            reason: request.reason.clone(),
        },
    );
    store.append_transition_event(base_dir, milestone_id, &event)?;

    let synced = MilestoneControllerRecord::transitioned(
        milestone_id.clone(),
        current.state,
        bead_id,
        task_id,
        Some(request.reason),
        now,
        now,
    )?;
    store.write_controller(base_dir, milestone_id, &synced)?;
    Ok(synced)
}

pub fn load_controller(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<Option<MilestoneControllerRecord>> {
    store.with_controller_lock(base_dir, milestone_id, || {
        let controller = hydrate_controller_locked(store, base_dir, milestone_id)?;
        if let Some(ref record) = controller {
            record.validate_semantics()?;
        }
        Ok(controller)
    })
}

pub fn transition_controller(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    request: ControllerTransitionRequest,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    store.with_controller_lock(base_dir, milestone_id, || {
        let current = hydrate_controller_locked(store, base_dir, milestone_id)?
            .unwrap_or_else(|| MilestoneControllerRecord::idle(milestone_id.clone(), now));
        transition_from_current_locked(store, base_dir, milestone_id, current, request, now)
    })
}

pub fn sync_controller_state(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    request: ControllerTransitionRequest,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    store.with_controller_lock(base_dir, milestone_id, || {
        let Some(current) = hydrate_controller_locked(store, base_dir, milestone_id)? else {
            return initialize_controller_from_request_locked(
                store,
                base_dir,
                milestone_id,
                request,
                now,
            );
        };
        current.validate_semantics()?;
        if current.state == request.to_state {
            sync_existing_state_locked(store, base_dir, milestone_id, current, request, now)
        } else {
            transition_from_current_locked(store, base_dir, milestone_id, current, request, now)
        }
    })
}

pub fn checkpoint_controller_stop(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    store.with_controller_lock(base_dir, milestone_id, || {
        let controller = hydrate_controller_locked(store, base_dir, milestone_id)?
            .unwrap_or_else(|| MilestoneControllerRecord::idle(milestone_id.clone(), now));
        controller.validate_semantics()?;
        checkpoint_existing_controller_locked(store, base_dir, milestone_id, controller, now)
    })
}

pub fn resume_controller(
    store: &impl MilestoneControllerPort,
    runtime: &impl MilestoneControllerResumePort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    store.with_controller_lock(base_dir, milestone_id, || {
        let current = hydrate_controller_locked(store, base_dir, milestone_id)?
            .unwrap_or_else(|| MilestoneControllerRecord::idle(milestone_id.clone(), now));

        if let Some(request) = resume_transition_request(&current, runtime, milestone_id)? {
            transition_from_current_locked(store, base_dir, milestone_id, current, request, now)
        } else {
            Ok(current)
        }
    })
}

fn hydrate_controller_locked(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
) -> AppResult<Option<MilestoneControllerRecord>> {
    let controller = store.read_controller(base_dir, milestone_id)?;
    let journal = store.read_transition_journal(base_dir, milestone_id)?;
    validate_transition_journal(milestone_id, &journal)?;

    let hydrated = if let Some(last_event) = journal.last() {
        if controller
            .as_ref()
            .is_some_and(|existing| existing.matches_transition_event(last_event))
        {
            controller.clone()
        } else {
            Some(MilestoneControllerRecord::transitioned(
                milestone_id.clone(),
                last_event.to_state,
                last_event.bead_id.clone(),
                last_event.task_id.clone(),
                Some(last_event.reason.clone()),
                last_event.timestamp,
                last_event.timestamp,
            )?)
        }
    } else {
        controller.clone()
    };

    if let Some(ref record) = hydrated {
        if hydrated.as_ref() != controller.as_ref() {
            store.write_controller(base_dir, milestone_id, record)?;
        }
    }

    Ok(hydrated)
}

fn transition_from_current_locked(
    store: &impl MilestoneControllerPort,
    base_dir: &Path,
    milestone_id: &MilestoneId,
    current: MilestoneControllerRecord,
    request: ControllerTransitionRequest,
    now: DateTime<Utc>,
) -> AppResult<MilestoneControllerRecord> {
    let current_semantics_error = current.validate_semantics().err();

    if current.state == request.to_state {
        return Err(controller_corrupt_record(
            milestone_id,
            "controller.json",
            format!(
                "controller transition must change state; '{}' -> '{}'",
                state_name(current.state),
                state_name(request.to_state)
            ),
        ));
    }

    if !current.state.allows_transition_to(request.to_state) {
        return Err(controller_corrupt_record(
            milestone_id,
            "controller.json",
            format!(
                "illegal controller transition '{}' -> '{}'",
                state_name(current.state),
                state_name(request.to_state)
            ),
        ));
    }

    if let Some(error) = current_semantics_error {
        if request.to_state != MilestoneControllerState::NeedsOperator {
            return Err(error);
        }
    }

    if let Some(details) = transition_context_error_details(
        current.state,
        milestone_id,
        current.active_bead_id.as_deref(),
        current.active_task_id.as_deref(),
        request.to_state,
        request.bead_id.as_deref(),
        request.task_id.as_deref(),
    ) {
        return Err(controller_corrupt_record(
            milestone_id,
            "controller.json",
            details,
        ));
    }

    let event = MilestoneControllerTransitionEvent::new(now, current.state, &request);
    store.append_transition_event(base_dir, milestone_id, &event)?;

    let next = MilestoneControllerRecord::transitioned(
        milestone_id.clone(),
        request.to_state,
        request.bead_id,
        request.task_id,
        Some(request.reason),
        now,
        now,
    )?;
    store.write_controller(base_dir, milestone_id, &next)?;
    Ok(next)
}

fn resume_transition_request(
    current: &MilestoneControllerRecord,
    runtime: &impl MilestoneControllerResumePort,
    milestone_id: &MilestoneId,
) -> AppResult<Option<ControllerTransitionRequest>> {
    if let Err(error) = current.validate_semantics() {
        if current.state == MilestoneControllerState::NeedsOperator {
            return Ok(None);
        }
        return Ok(Some(ControllerTransitionRequest {
            to_state: MilestoneControllerState::NeedsOperator,
            bead_id: current.active_bead_id.clone(),
            task_id: current.active_task_id.clone(),
            reason: format!("resume divergence: {error}"),
        }));
    }

    let no_open_beads = runtime.all_beads_closed()?;

    let needs_operator_for_bead = |reason: String| ControllerTransitionRequest {
        to_state: MilestoneControllerState::NeedsOperator,
        bead_id: current.active_bead_id.clone(),
        task_id: current.active_task_id.clone(),
        reason,
    };

    let bead_closed_externally = |bead_id: &str| -> AppResult<bool> {
        Ok(matches!(
            runtime.bead_status(bead_id)?,
            ControllerBeadStatus::Closed | ControllerBeadStatus::Missing
        ))
    };

    match current.state {
        MilestoneControllerState::Idle => {
            if no_open_beads {
                Ok(Some(ControllerTransitionRequest::new(
                    MilestoneControllerState::Completed,
                    "resume detected that all milestone beads are already closed",
                )))
            } else {
                Ok(None)
            }
        }
        MilestoneControllerState::Selecting => {
            if no_open_beads {
                Ok(Some(ControllerTransitionRequest::new(
                    MilestoneControllerState::Completed,
                    "resume detected that all milestone beads are already closed",
                )))
            } else if runtime.has_ready_beads()? {
                Ok(None)
            } else {
                Ok(Some(ControllerTransitionRequest::new(
                    MilestoneControllerState::Blocked,
                    "resume found no ready beads to select",
                )))
            }
        }
        MilestoneControllerState::Claimed => {
            let bead_id = required_controller_bead_id(current, milestone_id)?;
            let bead_closed = bead_closed_externally(bead_id)?;

            let Some(task_id) = current.active_task_id.as_deref() else {
                return if bead_closed {
                    Ok(Some(needs_operator_for_bead(
                        "resume divergence: active bead was already closed externally".to_owned(),
                    )))
                } else {
                    Ok(None)
                };
            };

            match runtime.task_status(task_id)? {
                ControllerTaskStatus::Pending => {
                    if bead_closed {
                        Ok(Some(needs_operator_for_bead(
                            "resume divergence: active bead was already closed externally"
                                .to_owned(),
                        )))
                    } else {
                        Ok(None)
                    }
                }
                ControllerTaskStatus::Running => {
                    if bead_closed {
                        Ok(Some(needs_operator_for_bead(
                            "resume divergence: active bead was already closed externally"
                                .to_owned(),
                        )))
                    } else {
                        Ok(Some(
                            ControllerTransitionRequest::new(
                                MilestoneControllerState::Running,
                                "resume detected that the claimed task is already running",
                            )
                            .with_bead(bead_id)
                            .with_task(task_id),
                        ))
                    }
                }
                ControllerTaskStatus::Succeeded | ControllerTaskStatus::Failed => {
                    let reason = if bead_closed {
                        "resume detected that the claimed task already reached a terminal state after bead closure; continuing reconciliation"
                    } else {
                        "resume detected that the claimed task already reached a terminal state"
                    };
                    Ok(Some(
                        ControllerTransitionRequest::new(
                            MilestoneControllerState::Reconciling,
                            reason,
                        )
                        .with_bead(bead_id)
                        .with_task(task_id),
                    ))
                }
                ControllerTaskStatus::Missing => Ok(Some(needs_operator_for_bead(
                    "resume divergence: claimed task could not be found".to_owned(),
                ))),
            }
        }
        MilestoneControllerState::Running => {
            let bead_id = required_controller_bead_id(current, milestone_id)?;
            let task_id = required_controller_task_id(current, milestone_id)?;
            let bead_closed = bead_closed_externally(bead_id)?;

            match runtime.task_status(task_id)? {
                ControllerTaskStatus::Pending => {
                    if bead_closed {
                        Ok(Some(needs_operator_for_bead(
                            "resume divergence: running bead was already closed externally"
                                .to_owned(),
                        )))
                    } else {
                        Ok(Some(
                            ControllerTransitionRequest::new(
                                MilestoneControllerState::Claimed,
                                "resume detected that the task is not running yet; preserving the claimed task context",
                            )
                            .with_bead(bead_id)
                            .with_task(task_id),
                        ))
                    }
                }
                ControllerTaskStatus::Running => {
                    if bead_closed {
                        Ok(Some(needs_operator_for_bead(
                            "resume divergence: running bead was already closed externally"
                                .to_owned(),
                        )))
                    } else {
                        Ok(None)
                    }
                }
                ControllerTaskStatus::Succeeded | ControllerTaskStatus::Failed => {
                    let reason = if bead_closed {
                        "resume detected that the running task already reached a terminal state after bead closure; continuing reconciliation"
                    } else {
                        "resume detected that the running task already reached a terminal state"
                    };
                    Ok(Some(
                        ControllerTransitionRequest::new(
                            MilestoneControllerState::Reconciling,
                            reason,
                        )
                        .with_bead(bead_id)
                        .with_task(task_id),
                    ))
                }
                ControllerTaskStatus::Missing => Ok(Some(needs_operator_for_bead(
                    "resume divergence: running task could not be found".to_owned(),
                ))),
            }
        }
        MilestoneControllerState::Reconciling => {
            let bead_id = required_controller_bead_id(current, milestone_id)?;
            let task_id = required_controller_task_id(current, milestone_id)?;
            let bead_closed = bead_closed_externally(bead_id)?;

            match runtime.task_status(task_id)? {
                ControllerTaskStatus::Succeeded | ControllerTaskStatus::Failed => Ok(None),
                ControllerTaskStatus::Pending | ControllerTaskStatus::Running => {
                    Ok(Some(needs_operator_for_bead(if bead_closed {
                        "resume divergence: reconciling task is no longer terminal after bead closure"
                            .to_owned()
                    } else {
                        "resume divergence: reconciling task is no longer terminal".to_owned()
                    })))
                }
                ControllerTaskStatus::Missing => Ok(Some(needs_operator_for_bead(
                    "resume divergence: reconciling task could not be found".to_owned(),
                ))),
            }
        }
        MilestoneControllerState::Blocked => {
            if no_open_beads {
                Ok(Some(ControllerTransitionRequest::new(
                    MilestoneControllerState::Completed,
                    "resume detected that all milestone beads are already closed",
                )))
            } else if runtime.has_ready_beads()? {
                Ok(Some(ControllerTransitionRequest::new(
                    MilestoneControllerState::Selecting,
                    "resume found ready beads after a blocked controller state",
                )))
            } else {
                Ok(None)
            }
        }
        MilestoneControllerState::NeedsOperator => Ok(None),
        MilestoneControllerState::Completed => {
            if no_open_beads {
                Ok(None)
            } else {
                Ok(Some(ControllerTransitionRequest::new(
                    MilestoneControllerState::NeedsOperator,
                    "resume divergence: completed controller still has open beads",
                )))
            }
        }
    }
}

fn validate_transition_journal(
    milestone_id: &MilestoneId,
    journal: &[MilestoneControllerTransitionEvent],
) -> AppResult<()> {
    for (index, event) in journal.iter().enumerate() {
        let same_state_sync = event.from_state == event.to_state;
        if !same_state_sync && !event.from_state.allows_transition_to(event.to_state) {
            return Err(controller_corrupt_record(
                milestone_id,
                "controller-journal.ndjson",
                format!(
                    "line {}: illegal controller transition '{}' -> '{}'",
                    index + 1,
                    state_name(event.from_state),
                    state_name(event.to_state)
                ),
            ));
        }

        if let Some(details) = state_context_error_details(
            event.to_state,
            event.bead_id.as_deref(),
            event.task_id.as_deref(),
        ) {
            return Err(controller_corrupt_record(
                milestone_id,
                "controller-journal.ndjson",
                format!("line {}: {}", index + 1, details),
            ));
        }

        if let Some(previous) = index.checked_sub(1).and_then(|i| journal.get(i)) {
            if previous.to_state != event.from_state {
                return Err(controller_corrupt_record(
                    milestone_id,
                    "controller-journal.ndjson",
                    format!(
                        "line {}: expected from_state '{}' to match prior to_state '{}'",
                        index + 1,
                        state_name(event.from_state),
                        state_name(previous.to_state)
                    ),
                ));
            }
            if event.timestamp < previous.timestamp {
                return Err(controller_corrupt_record(
                    milestone_id,
                    "controller-journal.ndjson",
                    format!(
                        "line {}: timestamp regressed from '{}' to '{}'",
                        index + 1,
                        previous.timestamp.to_rfc3339(),
                        event.timestamp.to_rfc3339()
                    ),
                ));
            }
            let transition_error = if same_state_sync {
                same_state_context_error_details(
                    event.to_state,
                    milestone_id,
                    previous.bead_id.as_deref(),
                    previous.task_id.as_deref(),
                    event.bead_id.as_deref(),
                    event.task_id.as_deref(),
                )
            } else {
                transition_context_error_details(
                    previous.to_state,
                    milestone_id,
                    previous.bead_id.as_deref(),
                    previous.task_id.as_deref(),
                    event.to_state,
                    event.bead_id.as_deref(),
                    event.task_id.as_deref(),
                )
            };
            if let Some(details) = transition_error {
                return Err(controller_corrupt_record(
                    milestone_id,
                    "controller-journal.ndjson",
                    format!("line {}: {}", index + 1, details),
                ));
            }
        }
    }

    Ok(())
}

fn validate_state_context(
    milestone_id: &MilestoneId,
    state: MilestoneControllerState,
    bead_id: Option<&str>,
    task_id: Option<&str>,
) -> AppResult<()> {
    if let Some(details) = state_context_error_details(state, bead_id, task_id) {
        return Err(controller_corrupt_record(
            milestone_id,
            "controller.json",
            details,
        ));
    }

    Ok(())
}

fn controller_initialization_reason(state: MilestoneControllerState) -> String {
    format!("controller initialized in '{}' state", state_name(state))
}

fn state_context_error_details(
    state: MilestoneControllerState,
    bead_id: Option<&str>,
    task_id: Option<&str>,
) -> Option<String> {
    match state {
        MilestoneControllerState::Idle
        | MilestoneControllerState::Selecting
        | MilestoneControllerState::Blocked
        | MilestoneControllerState::Completed => {
            if bead_id.is_some() || task_id.is_some() {
                Some(format!(
                    "state '{}' must not carry active bead/task identifiers",
                    state_name(state)
                ))
            } else {
                None
            }
        }
        MilestoneControllerState::Claimed => {
            if bead_id.is_none() {
                Some("state 'claimed' requires an active bead identifier".to_owned())
            } else {
                None
            }
        }
        MilestoneControllerState::Running | MilestoneControllerState::Reconciling => {
            if bead_id.is_none() || task_id.is_none() {
                Some(format!(
                    "state '{}' requires both active bead and active task identifiers",
                    state_name(state)
                ))
            } else {
                None
            }
        }
        MilestoneControllerState::NeedsOperator => None,
    }
}

fn transition_context_error_details(
    from_state: MilestoneControllerState,
    milestone_id: &MilestoneId,
    from_bead_id: Option<&str>,
    from_task_id: Option<&str>,
    to_state: MilestoneControllerState,
    to_bead_id: Option<&str>,
    to_task_id: Option<&str>,
) -> Option<String> {
    if let Some(details) = state_context_error_details(to_state, to_bead_id, to_task_id) {
        return Some(details);
    }

    if !state_preserves_active_context(from_state) || state_clears_active_context(to_state) {
        return None;
    }

    if let Some(bead_id) = from_bead_id {
        if to_bead_id
            .map(|next| !milestone_bead_ids_equivalent(milestone_id, bead_id, next))
            .unwrap_or(true)
        {
            return Some(format!(
                "transition from '{}' must preserve active bead identifier '{}' until the controller clears it",
                state_name(from_state),
                bead_id
            ));
        }
    }

    if let Some(task_id) = from_task_id {
        if to_task_id != Some(task_id) {
            return Some(format!(
                "transition from '{}' must preserve active task identifier '{}' until the controller clears it",
                state_name(from_state),
                task_id
            ));
        }
    }

    None
}

fn state_preserves_active_context(state: MilestoneControllerState) -> bool {
    matches!(
        state,
        MilestoneControllerState::Claimed
            | MilestoneControllerState::Running
            | MilestoneControllerState::Reconciling
    )
}

fn state_clears_active_context(state: MilestoneControllerState) -> bool {
    matches!(
        state,
        MilestoneControllerState::Idle
            | MilestoneControllerState::Selecting
            | MilestoneControllerState::Blocked
            | MilestoneControllerState::Completed
    )
}

fn required_controller_bead_id<'a>(
    controller: &'a MilestoneControllerRecord,
    milestone_id: &MilestoneId,
) -> AppResult<&'a str> {
    controller.active_bead_id.as_deref().ok_or_else(|| {
        controller_corrupt_record(milestone_id, "controller.json", "active_bead_id is missing")
    })
}

fn required_controller_task_id<'a>(
    controller: &'a MilestoneControllerRecord,
    milestone_id: &MilestoneId,
) -> AppResult<&'a str> {
    controller.active_task_id.as_deref().ok_or_else(|| {
        controller_corrupt_record(milestone_id, "controller.json", "active_task_id is missing")
    })
}

fn validate_active_context_alignment(
    current: &MilestoneControllerRecord,
    milestone_id: &MilestoneId,
    bead_id: &str,
    task_id: &str,
) -> AppResult<()> {
    if let Some(current_bead_id) = current.active_bead_id.as_deref() {
        if !milestone_bead_ids_equivalent(milestone_id, current_bead_id, bead_id) {
            return Err(controller_corrupt_record(
                milestone_id,
                "controller.json",
                format!(
                    "controller is already tracking active bead '{}' and cannot adopt '{}'",
                    current_bead_id, bead_id
                ),
            ));
        }
    }
    if let Some(current_task_id) = current.active_task_id.as_deref() {
        if current_task_id != task_id {
            return Err(controller_corrupt_record(
                milestone_id,
                "controller.json",
                format!(
                    "controller is already tracking active task '{}' and cannot adopt '{}'",
                    current_task_id, task_id
                ),
            ));
        }
    }
    Ok(())
}

fn milestone_bead_ids_equivalent(milestone_id: &MilestoneId, left: &str, right: &str) -> bool {
    milestone_bead_refs_match(milestone_id, left, right)
}

fn state_name(state: MilestoneControllerState) -> &'static str {
    match state {
        MilestoneControllerState::Idle => "idle",
        MilestoneControllerState::Selecting => "selecting",
        MilestoneControllerState::Claimed => "claimed",
        MilestoneControllerState::Running => "running",
        MilestoneControllerState::Reconciling => "reconciling",
        MilestoneControllerState::Blocked => "blocked",
        MilestoneControllerState::NeedsOperator => "needs_operator",
        MilestoneControllerState::Completed => "completed",
    }
}

fn controller_corrupt_record(
    milestone_id: &MilestoneId,
    file: &str,
    details: impl Into<String>,
) -> AppError {
    AppError::CorruptRecord {
        file: format!("milestones/{}/{}", milestone_id, file),
        details: details.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::fs;
    use std::path::Path;

    use chrono::TimeZone;

    use super::*;
    use crate::adapters::fs::FsMilestoneControllerStore;
    use crate::test_support::logging::log_capture;

    fn milestone_id() -> MilestoneId {
        MilestoneId::new("ms-alpha").expect("milestone id")
    }

    fn ts(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 4, 8, hour, 0, 0)
            .single()
            .expect("timestamp")
    }

    #[derive(Default)]
    struct FakeControllerStore {
        controller: RefCell<Option<MilestoneControllerRecord>>,
        journal: RefCell<Vec<MilestoneControllerTransitionEvent>>,
    }

    impl MilestoneControllerPort for FakeControllerStore {
        fn read_controller(
            &self,
            _base_dir: &Path,
            _milestone_id: &MilestoneId,
        ) -> AppResult<Option<MilestoneControllerRecord>> {
            Ok(self.controller.borrow().clone())
        }

        fn write_controller(
            &self,
            _base_dir: &Path,
            _milestone_id: &MilestoneId,
            controller: &MilestoneControllerRecord,
        ) -> AppResult<()> {
            *self.controller.borrow_mut() = Some(controller.clone());
            Ok(())
        }

        fn read_transition_journal(
            &self,
            _base_dir: &Path,
            _milestone_id: &MilestoneId,
        ) -> AppResult<Vec<MilestoneControllerTransitionEvent>> {
            Ok(self.journal.borrow().clone())
        }

        fn append_transition_event(
            &self,
            _base_dir: &Path,
            _milestone_id: &MilestoneId,
            event: &MilestoneControllerTransitionEvent,
        ) -> AppResult<()> {
            self.journal.borrow_mut().push(event.clone());
            Ok(())
        }

        fn with_controller_lock<T, F>(
            &self,
            _base_dir: &Path,
            _milestone_id: &MilestoneId,
            operation: F,
        ) -> AppResult<T>
        where
            F: FnOnce() -> AppResult<T>,
        {
            operation()
        }
    }

    struct FakeResumeRuntime {
        bead_status: RefCell<ControllerBeadStatus>,
        task_status: RefCell<ControllerTaskStatus>,
        ready_beads: RefCell<bool>,
        all_closed: RefCell<bool>,
    }

    impl MilestoneControllerResumePort for FakeResumeRuntime {
        fn bead_status(&self, _bead_id: &str) -> AppResult<ControllerBeadStatus> {
            Ok(*self.bead_status.borrow())
        }

        fn task_status(&self, _task_id: &str) -> AppResult<ControllerTaskStatus> {
            Ok(*self.task_status.borrow())
        }

        fn has_ready_beads(&self) -> AppResult<bool> {
            Ok(*self.ready_beads.borrow())
        }

        fn all_beads_closed(&self) -> AppResult<bool> {
            Ok(*self.all_closed.borrow())
        }
    }

    #[test]
    fn transition_persists_controller_and_journal_event() -> Result<(), Box<dyn std::error::Error>>
    {
        let store = FakeControllerStore::default();
        let base = Path::new(".");
        let milestone_id = milestone_id();

        let selecting = transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Selecting,
                "begin selecting the next bead",
            ),
            ts(10),
        )?;

        assert_eq!(selecting.state, MilestoneControllerState::Selecting);
        assert_eq!(store.journal.borrow().len(), 1);
        assert_eq!(
            store.journal.borrow()[0].from_state,
            MilestoneControllerState::Idle
        );
        assert_eq!(
            store.journal.borrow()[0].to_state,
            MilestoneControllerState::Selecting
        );

        let claimed = transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "claimed the selected bead and started task creation",
            )
            .with_bead("ms-alpha.bead-2"),
            ts(11),
        )?;

        assert_eq!(claimed.state, MilestoneControllerState::Claimed);
        assert_eq!(claimed.active_bead_id.as_deref(), Some("ms-alpha.bead-2"));
        assert_eq!(store.journal.borrow().len(), 2);
        assert_eq!(
            store
                .controller
                .borrow()
                .as_ref()
                .map(|record| record.state),
            Some(MilestoneControllerState::Claimed)
        );
        Ok(())
    }

    #[test]
    fn initialize_controller_appends_an_initialization_journal_event(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = FakeControllerStore::default();
        let base = Path::new(".");
        let milestone_id = milestone_id();

        let controller = initialize_controller_with_state(
            &store,
            base,
            &milestone_id,
            MilestoneControllerState::Blocked,
            ts(10),
        )?;

        assert_eq!(controller.state, MilestoneControllerState::Blocked);
        assert_eq!(store.journal.borrow().len(), 1);
        assert_eq!(
            store.journal.borrow()[0].from_state,
            MilestoneControllerState::Blocked
        );
        assert_eq!(
            store.journal.borrow()[0].to_state,
            MilestoneControllerState::Blocked
        );
        assert!(store.journal.borrow()[0]
            .reason
            .contains("controller initialized"));
        Ok(())
    }

    #[test]
    fn running_transition_requires_task_id() {
        let store = FakeControllerStore::default();
        let base = Path::new(".");
        let milestone_id = milestone_id();

        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Selecting,
                "begin selecting the next bead",
            ),
            ts(9),
        )
        .expect("selecting transition");
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "claimed the selected bead and started task creation",
            )
            .with_bead("ms-alpha.bead-2"),
            ts(10),
        )
        .expect("claimed transition");

        let error = transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Running,
                "task execution started",
            )
            .with_bead("ms-alpha.bead-2"),
            ts(11),
        )
        .expect_err("running should require a task id");

        assert!(error
            .to_string()
            .contains("requires both active bead and active task"));
    }

    #[test]
    fn checkpoint_stop_preserves_active_context_without_new_journal_event(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = FakeControllerStore::default();
        let base = Path::new(".");
        let milestone_id = milestone_id();

        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Selecting,
                "begin selecting the next bead",
            ),
            ts(10),
        )?;
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "claimed the selected bead and started task creation",
            )
            .with_bead("ms-alpha.bead-2"),
            ts(11),
        )?;
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Running,
                "task execution started",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task("task-42"),
            ts(12),
        )?;

        let before_events = store.journal.borrow().len();
        let checkpointed = checkpoint_controller_stop(&store, base, &milestone_id, ts(13))?;

        assert_eq!(checkpointed.state, MilestoneControllerState::Running);
        assert_eq!(checkpointed.active_task_id.as_deref(), Some("task-42"));
        assert_eq!(checkpointed.updated_at, ts(13));
        assert_eq!(checkpointed.last_transition_at, ts(12));
        assert_eq!(store.journal.borrow().len(), before_events);
        Ok(())
    }

    #[test]
    fn sync_controller_state_records_same_state_reason_updates(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = FakeControllerStore::default();
        let base = Path::new(".");
        let milestone_id = milestone_id();

        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Selecting,
                "begin selecting the next bead",
            ),
            ts(10),
        )?;

        let synced = sync_controller_state(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Selecting,
                "requesting the next bead recommendation from bv before any claim",
            ),
            ts(11),
        )?;

        assert_eq!(synced.state, MilestoneControllerState::Selecting);
        assert_eq!(synced.last_transition_at, ts(11));
        assert_eq!(
            synced.last_transition_reason.as_deref(),
            Some("requesting the next bead recommendation from bv before any claim")
        );
        assert_eq!(store.journal.borrow().len(), 2);
        assert_eq!(
            store.journal.borrow()[1].from_state,
            MilestoneControllerState::Selecting
        );
        assert_eq!(
            store.journal.borrow()[1].to_state,
            MilestoneControllerState::Selecting
        );

        let hydrated =
            load_controller(&store, Path::new("."), &milestone_id)?.expect("controller exists");
        assert_eq!(hydrated.last_transition_at, ts(11));
        assert_eq!(
            hydrated.last_transition_reason.as_deref(),
            Some("requesting the next bead recommendation from bv before any claim")
        );
        Ok(())
    }

    #[test]
    fn load_controller_recovers_from_journal_when_state_file_is_stale(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = FakeControllerStore::default();
        let milestone_id = milestone_id();

        store.write_controller(
            Path::new("."),
            &milestone_id,
            &MilestoneControllerRecord::transitioned(
                milestone_id.clone(),
                MilestoneControllerState::Selecting,
                None,
                None,
                Some("begin selecting".to_owned()),
                ts(10),
                ts(10),
            )?,
        )?;
        store.append_transition_event(
            Path::new("."),
            &milestone_id,
            &MilestoneControllerTransitionEvent {
                timestamp: ts(10),
                from_state: MilestoneControllerState::Idle,
                to_state: MilestoneControllerState::Selecting,
                bead_id: None,
                task_id: None,
                reason: "begin selecting".to_owned(),
            },
        )?;
        store.append_transition_event(
            Path::new("."),
            &milestone_id,
            &MilestoneControllerTransitionEvent {
                timestamp: ts(11),
                from_state: MilestoneControllerState::Selecting,
                to_state: MilestoneControllerState::Claimed,
                bead_id: Some("ms-alpha.bead-2".to_owned()),
                task_id: None,
                reason: "claimed bead".to_owned(),
            },
        )?;

        let hydrated =
            load_controller(&store, Path::new("."), &milestone_id)?.expect("controller exists");
        assert_eq!(hydrated.state, MilestoneControllerState::Claimed);
        assert_eq!(hydrated.active_bead_id.as_deref(), Some("ms-alpha.bead-2"));
        assert_eq!(hydrated.last_transition_at, ts(11));
        Ok(())
    }

    #[test]
    fn resume_running_closed_bead_moves_to_needs_operator() -> Result<(), Box<dyn std::error::Error>>
    {
        let store = FakeControllerStore::default();
        let runtime = FakeResumeRuntime {
            bead_status: RefCell::new(ControllerBeadStatus::Closed),
            task_status: RefCell::new(ControllerTaskStatus::Running),
            ready_beads: RefCell::new(false),
            all_closed: RefCell::new(false),
        };
        let base = Path::new(".");
        let milestone_id = milestone_id();

        initialize_controller(&store, base, &milestone_id, ts(9))?;
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Selecting,
                "begin selecting the next bead",
            ),
            ts(10),
        )?;
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "claimed the selected bead and started task creation",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task("task-42"),
            ts(11),
        )?;
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Running,
                "task execution started",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task("task-42"),
            ts(12),
        )?;

        let resumed = resume_controller(&store, &runtime, base, &milestone_id, ts(13))?;

        assert_eq!(resumed.state, MilestoneControllerState::NeedsOperator);
        assert_eq!(resumed.active_bead_id.as_deref(), Some("ms-alpha.bead-2"));
        assert!(resumed
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("closed externally")));
        Ok(())
    }

    #[test]
    fn resume_running_closed_bead_with_terminal_task_moves_to_reconciling(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = FakeControllerStore::default();
        let runtime = FakeResumeRuntime {
            bead_status: RefCell::new(ControllerBeadStatus::Closed),
            task_status: RefCell::new(ControllerTaskStatus::Succeeded),
            ready_beads: RefCell::new(false),
            all_closed: RefCell::new(false),
        };
        let base = Path::new(".");
        let milestone_id = milestone_id();

        initialize_controller(&store, base, &milestone_id, ts(9))?;
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Selecting,
                "begin selecting the next bead",
            ),
            ts(10),
        )?;
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "claimed the selected bead and started task creation",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task("task-42"),
            ts(11),
        )?;
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Running,
                "task execution started",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task("task-42"),
            ts(12),
        )?;

        let resumed = resume_controller(&store, &runtime, base, &milestone_id, ts(13))?;

        assert_eq!(resumed.state, MilestoneControllerState::Reconciling);
        assert_eq!(resumed.active_bead_id.as_deref(), Some("ms-alpha.bead-2"));
        assert_eq!(resumed.active_task_id.as_deref(), Some("task-42"));
        assert!(resumed
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("after bead closure")));
        Ok(())
    }

    #[test]
    fn resume_invalid_running_state_moves_to_needs_operator(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = FakeControllerStore::default();
        let runtime = FakeResumeRuntime {
            bead_status: RefCell::new(ControllerBeadStatus::Open),
            task_status: RefCell::new(ControllerTaskStatus::Running),
            ready_beads: RefCell::new(false),
            all_closed: RefCell::new(false),
        };
        let base = Path::new(".");
        let milestone_id = milestone_id();

        store.write_controller(
            base,
            &milestone_id,
            &MilestoneControllerRecord {
                schema_version: MILESTONE_CONTROLLER_SCHEMA_VERSION,
                milestone_id: milestone_id.clone(),
                state: MilestoneControllerState::Running,
                active_bead_id: Some("ms-alpha.bead-2".to_owned()),
                active_task_id: None,
                last_transition_at: ts(12),
                updated_at: ts(12),
                last_transition_reason: Some("task execution started".to_owned()),
            },
        )?;

        let resumed = resume_controller(&store, &runtime, base, &milestone_id, ts(13))?;

        assert_eq!(resumed.state, MilestoneControllerState::NeedsOperator);
        assert_eq!(resumed.active_bead_id.as_deref(), Some("ms-alpha.bead-2"));
        assert_eq!(resumed.active_task_id, None);
        assert!(resumed
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("requires both active bead and active task")));
        assert_eq!(store.journal.borrow().len(), 1);
        assert_eq!(
            store.journal.borrow()[0].from_state,
            MilestoneControllerState::Running
        );
        assert_eq!(
            store.journal.borrow()[0].to_state,
            MilestoneControllerState::NeedsOperator
        );
        Ok(())
    }

    #[test]
    fn load_controller_rejects_invalid_snapshot_without_journal_recovery() {
        let store = FakeControllerStore::default();
        let milestone_id = milestone_id();

        store
            .write_controller(
                Path::new("."),
                &milestone_id,
                &MilestoneControllerRecord {
                    schema_version: MILESTONE_CONTROLLER_SCHEMA_VERSION,
                    milestone_id: milestone_id.clone(),
                    state: MilestoneControllerState::Running,
                    active_bead_id: Some("ms-alpha.bead-2".to_owned()),
                    active_task_id: None,
                    last_transition_at: ts(12),
                    updated_at: ts(12),
                    last_transition_reason: Some("task execution started".to_owned()),
                },
            )
            .expect("controller write");

        let error =
            load_controller(&store, Path::new("."), &milestone_id).expect_err("invalid snapshot");

        assert!(matches!(error, AppError::CorruptRecord { .. }));
        assert!(error
            .to_string()
            .contains("requires both active bead and active task"));
    }

    #[test]
    fn resume_running_pending_task_moves_back_to_claimed() -> Result<(), Box<dyn std::error::Error>>
    {
        let store = FakeControllerStore::default();
        let runtime = FakeResumeRuntime {
            bead_status: RefCell::new(ControllerBeadStatus::Open),
            task_status: RefCell::new(ControllerTaskStatus::Pending),
            ready_beads: RefCell::new(false),
            all_closed: RefCell::new(false),
        };
        let base = Path::new(".");
        let milestone_id = milestone_id();

        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Selecting,
                "begin selecting the next bead",
            ),
            ts(10),
        )?;
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "claimed the selected bead and started task creation",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task("task-42"),
            ts(11),
        )?;
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Running,
                "task execution started",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task("task-42"),
            ts(12),
        )?;

        let resumed = resume_controller(&store, &runtime, base, &milestone_id, ts(13))?;

        assert_eq!(resumed.state, MilestoneControllerState::Claimed);
        assert_eq!(resumed.active_bead_id.as_deref(), Some("ms-alpha.bead-2"));
        assert_eq!(resumed.active_task_id.as_deref(), Some("task-42"));
        assert!(resumed
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("not running yet")));
        Ok(())
    }

    #[test]
    fn resume_reconciling_closed_bead_with_terminal_task_remains_reconciling(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = FakeControllerStore::default();
        let runtime = FakeResumeRuntime {
            bead_status: RefCell::new(ControllerBeadStatus::Closed),
            task_status: RefCell::new(ControllerTaskStatus::Succeeded),
            ready_beads: RefCell::new(false),
            all_closed: RefCell::new(false),
        };
        let base = Path::new(".");
        let milestone_id = milestone_id();

        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Selecting,
                "begin selecting the next bead",
            ),
            ts(10),
        )?;
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "claimed the selected bead and started task creation",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task("task-42"),
            ts(11),
        )?;
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Running,
                "task execution started",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task("task-42"),
            ts(12),
        )?;
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Reconciling,
                "task completed; reconciling",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task("task-42"),
            ts(13),
        )?;

        let events_before_resume = store.journal.borrow().len();
        let resumed = resume_controller(&store, &runtime, base, &milestone_id, ts(14))?;

        assert_eq!(resumed.state, MilestoneControllerState::Reconciling);
        assert_eq!(resumed.active_bead_id.as_deref(), Some("ms-alpha.bead-2"));
        assert_eq!(resumed.active_task_id.as_deref(), Some("task-42"));
        assert_eq!(store.journal.borrow().len(), events_before_resume);
        Ok(())
    }

    #[test]
    fn resume_blocked_reenters_selecting_when_ready_work_appears(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let store = FakeControllerStore::default();
        let runtime = FakeResumeRuntime {
            bead_status: RefCell::new(ControllerBeadStatus::Open),
            task_status: RefCell::new(ControllerTaskStatus::Pending),
            ready_beads: RefCell::new(true),
            all_closed: RefCell::new(false),
        };
        let base = Path::new(".");
        let milestone_id = milestone_id();

        initialize_controller_with_state(
            &store,
            base,
            &milestone_id,
            MilestoneControllerState::Blocked,
            ts(10),
        )?;

        let resumed = resume_controller(&store, &runtime, base, &milestone_id, ts(11))?;

        assert_eq!(resumed.state, MilestoneControllerState::Selecting);
        assert!(resumed
            .last_transition_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("ready beads")));
        Ok(())
    }

    #[test]
    fn transition_rejects_swapping_active_task_identity() {
        let store = FakeControllerStore::default();
        let base = Path::new(".");
        let milestone_id = milestone_id();

        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Selecting,
                "begin selecting the next bead",
            ),
            ts(10),
        )
        .expect("selecting transition");
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "claimed the selected bead and started task creation",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task("task-42"),
            ts(11),
        )
        .expect("claimed transition");
        transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Running,
                "task execution started",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task("task-42"),
            ts(12),
        )
        .expect("running transition");

        let error = transition_controller(
            &store,
            base,
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Reconciling,
                "task completed",
            )
            .with_bead("ms-alpha.bead-2")
            .with_task("task-99"),
            ts(13),
        )
        .expect_err("reconciling should preserve the active task identity");

        assert!(error
            .to_string()
            .contains("must preserve active task identifier 'task-42'"));
    }

    #[test]
    fn load_controller_rejects_journal_that_swaps_inflight_bead_identity() {
        let store = FakeControllerStore::default();
        let milestone_id = milestone_id();

        store
            .append_transition_event(
                Path::new("."),
                &milestone_id,
                &MilestoneControllerTransitionEvent {
                    timestamp: ts(10),
                    from_state: MilestoneControllerState::Idle,
                    to_state: MilestoneControllerState::Selecting,
                    bead_id: None,
                    task_id: None,
                    reason: "begin selecting".to_owned(),
                },
            )
            .expect("selecting journal event");
        store
            .append_transition_event(
                Path::new("."),
                &milestone_id,
                &MilestoneControllerTransitionEvent {
                    timestamp: ts(11),
                    from_state: MilestoneControllerState::Selecting,
                    to_state: MilestoneControllerState::Claimed,
                    bead_id: Some("ms-alpha.bead-2".to_owned()),
                    task_id: Some("task-42".to_owned()),
                    reason: "claimed bead".to_owned(),
                },
            )
            .expect("claimed journal event");
        store
            .append_transition_event(
                Path::new("."),
                &milestone_id,
                &MilestoneControllerTransitionEvent {
                    timestamp: ts(12),
                    from_state: MilestoneControllerState::Claimed,
                    to_state: MilestoneControllerState::Running,
                    bead_id: Some("ms-alpha.bead-2".to_owned()),
                    task_id: Some("task-42".to_owned()),
                    reason: "task running".to_owned(),
                },
            )
            .expect("running journal event");
        store
            .append_transition_event(
                Path::new("."),
                &milestone_id,
                &MilestoneControllerTransitionEvent {
                    timestamp: ts(13),
                    from_state: MilestoneControllerState::Running,
                    to_state: MilestoneControllerState::Reconciling,
                    bead_id: Some("ms-alpha.bead-9".to_owned()),
                    task_id: Some("task-42".to_owned()),
                    reason: "task completed".to_owned(),
                },
            )
            .expect("reconciling journal event");

        let error = load_controller(&store, Path::new("."), &milestone_id)
            .expect_err("journal identity swap should be rejected");

        assert!(matches!(error, AppError::CorruptRecord { .. }));
        assert!(error
            .to_string()
            .contains("line 4: transition from 'running' must preserve active bead identifier 'ms-alpha.bead-2'"));
    }

    #[test]
    fn fs_store_persists_controller_json_and_transition_journal(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let milestone_id = milestone_id();
        let milestone_root = temp_dir
            .path()
            .join(".ralph-burning/milestones")
            .join(milestone_id.as_str());
        fs::create_dir_all(&milestone_root)?;
        let store = FsMilestoneControllerStore;

        initialize_controller(&store, temp_dir.path(), &milestone_id, ts(10))?;
        transition_controller(
            &store,
            temp_dir.path(),
            &milestone_id,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Selecting,
                "begin selecting the next bead",
            ),
            ts(11),
        )?;

        let controller_path = milestone_root.join("controller.json");
        let journal_path = milestone_root.join("controller-journal.ndjson");
        assert!(controller_path.is_file());
        assert!(journal_path.is_file());

        let controller = fs::read_to_string(&controller_path)?;
        assert!(controller.contains("\"state\": \"selecting\""));

        let journal = fs::read_to_string(&journal_path)?;
        assert!(journal.contains("\"from_state\":\"idle\""));
        assert!(journal.contains("\"to_state\":\"selecting\""));
        Ok(())
    }

    #[test]
    fn fs_store_rejects_unsupported_controller_schema_version() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let milestone_id = milestone_id();
        let milestone_root = temp_dir
            .path()
            .join(".ralph-burning/milestones")
            .join(milestone_id.as_str());
        fs::create_dir_all(&milestone_root).expect("milestone root");
        let store = FsMilestoneControllerStore;

        fs::write(
            milestone_root.join("controller.json"),
            serde_json::json!({
                "schema_version": MILESTONE_CONTROLLER_SCHEMA_VERSION + 1,
                "milestone_id": milestone_id.as_str(),
                "state": "idle",
                "last_transition_at": ts(10),
                "updated_at": ts(10),
            })
            .to_string(),
        )
        .expect("write controller");

        let error = load_controller(&store, temp_dir.path(), &milestone_id)
            .expect_err("unsupported schema version should be rejected");

        assert!(matches!(error, AppError::CorruptRecord { .. }));
        assert!(error
            .to_string()
            .contains("unsupported controller schema_version"));
    }

    #[test]
    fn fs_store_ignores_malformed_trailing_controller_journal_line(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let capture = log_capture();
        let temp_dir = tempfile::tempdir()?;
        let milestone_id = milestone_id();
        let milestone_root = temp_dir
            .path()
            .join(".ralph-burning/milestones")
            .join(milestone_id.as_str());
        fs::create_dir_all(&milestone_root)?;
        let journal_path = milestone_root.join("controller-journal.ndjson");
        let store = FsMilestoneControllerStore;

        let valid_event = MilestoneControllerTransitionEvent {
            timestamp: ts(10),
            from_state: MilestoneControllerState::Idle,
            to_state: MilestoneControllerState::Selecting,
            bead_id: None,
            task_id: None,
            reason: "begin selecting".to_owned(),
        }
        .to_ndjson_line()?;
        fs::write(
            &journal_path,
            format!(
                "{valid_event}\n{{\"timestamp\":\"{}\",\"from_state\":\"selecting\"",
                ts(11).to_rfc3339()
            ),
        )?;

        let loaded = capture.in_scope(|| {
            load_controller(&store, temp_dir.path(), &milestone_id)
                .expect("controller load should succeed")
                .expect("controller should be recovered from the valid journal prefix")
        });

        assert_eq!(loaded.state, MilestoneControllerState::Selecting);
        assert_eq!(loaded.last_transition_at, ts(10));

        let warning = capture.assert_event_has_fields(&[
            ("file", "controller-journal.ndjson"),
            ("line_number", "2"),
            (
                "message",
                "discarding malformed trailing controller journal line",
            ),
        ]);
        assert_eq!(warning.level, "WARN");
        Ok(())
    }

    #[test]
    fn fs_store_rejects_nonfinal_malformed_controller_journal_line() {
        let temp_dir = tempfile::tempdir().expect("tempdir");
        let milestone_id = milestone_id();
        let milestone_root = temp_dir
            .path()
            .join(".ralph-burning/milestones")
            .join(milestone_id.as_str());
        fs::create_dir_all(&milestone_root).expect("milestone root");
        let journal_path = milestone_root.join("controller-journal.ndjson");
        let store = FsMilestoneControllerStore;

        fs::write(
            &journal_path,
            format!(
                "{}\n{{\"timestamp\":\"{}\",\"from_state\":\"selecting\"\n{}",
                MilestoneControllerTransitionEvent {
                    timestamp: ts(10),
                    from_state: MilestoneControllerState::Idle,
                    to_state: MilestoneControllerState::Selecting,
                    bead_id: None,
                    task_id: None,
                    reason: "begin selecting".to_owned(),
                }
                .to_ndjson_line()
                .expect("valid selecting event"),
                ts(11).to_rfc3339(),
                MilestoneControllerTransitionEvent {
                    timestamp: ts(12),
                    from_state: MilestoneControllerState::Selecting,
                    to_state: MilestoneControllerState::Blocked,
                    bead_id: None,
                    task_id: None,
                    reason: "no ready work".to_owned(),
                }
                .to_ndjson_line()
                .expect("valid blocked event")
            ),
        )
        .expect("journal contents");

        let error = load_controller(&store, temp_dir.path(), &milestone_id)
            .expect_err("nonfinal malformed journal line should be rejected");

        assert!(matches!(error, AppError::CorruptRecord { .. }));
        assert!(error.to_string().contains("line 2"));
    }

    #[test]
    fn sync_task_claimed_adopts_task_id_when_claimed_without_one() -> AppResult<()> {
        let store = FakeControllerStore::default();
        let mid = milestone_id();
        let base_dir = Path::new("/tmp/fake");

        // Initialize controller in Claimed state with bead_id but no task_id
        initialize_controller_with_request(
            &store,
            base_dir,
            &mid,
            ControllerTransitionRequest::new(
                MilestoneControllerState::Claimed,
                "selection picked bead-x",
            )
            .with_bead("bead-x"),
            ts(1),
        )?;

        // Verify: task_id is None after initialization
        let before = store
            .controller
            .borrow()
            .clone()
            .expect("controller exists");
        assert_eq!(before.active_task_id, None);

        // Call sync_controller_task_claimed with a task_id
        let result = sync_controller_task_claimed(
            &store,
            base_dir,
            &mid,
            "bead-x",
            "project-99",
            "adopting project as task owner",
            ts(2),
        )?;

        assert_eq!(result.state, MilestoneControllerState::Claimed);
        assert_eq!(result.active_bead_id.as_deref(), Some("bead-x"));
        assert_eq!(result.active_task_id.as_deref(), Some("project-99"));

        // Also verify persisted state
        let persisted = store
            .controller
            .borrow()
            .clone()
            .expect("controller exists");
        assert_eq!(persisted.active_task_id.as_deref(), Some("project-99"));
        Ok(())
    }
}
