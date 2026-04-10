use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::contexts::workflow_composition::panel_contracts::RecordKind;
use crate::shared::domain::StageId;
use crate::shared::error::AppResult;

use super::model::{
    ArtifactRecord, JournalEvent, JournalEventType, PayloadRecord, RunSnapshot, RunStatus,
    RuntimeLogEntry,
};

/// Reconcile a snapshot's status against the journal's last terminal event.
///
/// When a `fail_run()` snapshot write fails, `run.json` may still say `Running`
/// while the journal has a `run_failed` event. This function detects the
/// mismatch and patches the in-memory snapshot so downstream consumers
/// (status display, resume) see the authoritative journal state.
///
/// Returns `true` if the snapshot was patched.
pub fn reconcile_snapshot_status(snapshot: &mut RunSnapshot, events: &[JournalEvent]) -> bool {
    if snapshot.status != RunStatus::Running {
        return false;
    }
    let Some(run_id) = snapshot
        .active_run
        .as_ref()
        .map(|active_run| active_run.run_id.as_str())
    else {
        return false;
    };

    let last_terminal_for_run = events.iter().rev().find_map(|event| {
        (event.details.get("run_id").and_then(|value| value.as_str()) == Some(run_id)
            && matches!(
                event.event_type,
                JournalEventType::RunFailed | JournalEventType::RunCompleted
            ))
        .then_some(event.event_type.clone())
    });

    match last_terminal_for_run {
        Some(JournalEventType::RunFailed) => {
            eprintln!(
                "status: snapshot shows Running but journal has run_failed — \
                 reporting as Failed (stale snapshot from failed write)"
            );
            snapshot.status = RunStatus::Failed;
            snapshot.active_run = None;
            snapshot.status_summary = "failed (reconciled from journal)".to_owned();
            true
        }
        Some(JournalEventType::RunCompleted) => {
            eprintln!(
                "status: snapshot shows Running but journal has run_completed — \
                 reporting as Completed (stale snapshot from failed write)"
            );
            snapshot.status = RunStatus::Completed;
            snapshot.active_run = None;
            snapshot.status_summary = "completed (reconciled from journal)".to_owned();
            true
        }
        _ => false,
    }
}

/// Read model for `run status`: canonical state only, no inference from artifacts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunStatusView {
    pub project_id: String,
    pub status: String,
    pub stage: Option<String>,
    pub cycle: Option<u32>,
    pub completion_round: Option<u32>,
    pub summary: String,
}

impl RunStatusView {
    pub fn from_snapshot(project_id: &str, snapshot: &RunSnapshot) -> Self {
        match &snapshot.active_run {
            Some(active) => Self {
                project_id: project_id.to_owned(),
                status: snapshot.status.display_str().to_owned(),
                stage: Some(active.stage_cursor.stage.as_str().to_owned()),
                cycle: Some(active.stage_cursor.cycle),
                completion_round: Some(active.stage_cursor.completion_round),
                summary: snapshot.status_summary.clone(),
            },
            None => Self {
                project_id: project_id.to_owned(),
                status: snapshot.status.display_str().to_owned(),
                stage: None,
                cycle: None,
                completion_round: None,
                summary: snapshot.status_summary.clone(),
            },
        }
    }
}

/// Stable JSON read model for `run status --json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunStatusJsonView {
    pub project_id: String,
    pub status: String,
    pub stage: Option<String>,
    pub cycle: Option<u32>,
    pub completion_round: Option<u32>,
    pub summary: String,
    pub amendment_queue_depth: usize,
}

impl RunStatusJsonView {
    pub fn from_snapshot(project_id: &str, snapshot: &RunSnapshot) -> Self {
        let (stage, cycle, completion_round) = match &snapshot.active_run {
            Some(active) => (
                Some(active.stage_cursor.stage.as_str().to_owned()),
                Some(active.stage_cursor.cycle),
                Some(active.stage_cursor.completion_round),
            ),
            None => (None, None, None),
        };

        Self {
            project_id: project_id.to_owned(),
            status: match snapshot.status {
                super::model::RunStatus::NotStarted => "not_started",
                super::model::RunStatus::Running => "running",
                super::model::RunStatus::Paused => "paused",
                super::model::RunStatus::Completed => "completed",
                super::model::RunStatus::Failed => "failed",
            }
            .to_owned(),
            stage,
            cycle,
            completion_round,
            summary: snapshot.status_summary.clone(),
            amendment_queue_depth: snapshot.amendment_queue.pending.len(),
        }
    }
}

/// Read model for `run history`: durable history only (journal + payloads + artifacts).
/// Runtime logs never appear here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunHistoryView {
    pub project_id: String,
    pub events: Vec<JournalEvent>,
    pub payloads: Vec<PayloadRecord>,
    pub artifacts: Vec<ArtifactRecord>,
}

/// Read model for `run tail`: durable history by default, with optional runtime logs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunTailView {
    pub project_id: String,
    pub events: Vec<JournalEvent>,
    pub payloads: Vec<PayloadRecord>,
    pub artifacts: Vec<ArtifactRecord>,
    pub runtime_logs: Option<Vec<RuntimeLogEntry>>,
}

/// Read model for rollback target listings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRollbackTargetView {
    pub rollback_id: String,
    pub stage_id: String,
    pub cycle: u32,
    pub created_at: DateTime<Utc>,
    pub git_sha: Option<String>,
}

/// Build a status view from canonical run state.
pub fn build_status_view(project_id: &str, snapshot: &RunSnapshot) -> RunStatusView {
    RunStatusView::from_snapshot(project_id, snapshot)
}

/// Build a history view from durable records only. No runtime logs.
pub fn build_history_view(
    project_id: &str,
    events: Vec<JournalEvent>,
    payloads: Vec<PayloadRecord>,
    artifacts: Vec<ArtifactRecord>,
) -> RunHistoryView {
    RunHistoryView {
        project_id: project_id.to_owned(),
        events,
        payloads,
        artifacts,
    }
}

/// Build a tail view: durable history always, runtime logs only when requested.
pub fn build_tail_view(
    project_id: &str,
    events: Vec<JournalEvent>,
    payloads: Vec<PayloadRecord>,
    artifacts: Vec<ArtifactRecord>,
    include_logs: bool,
    runtime_logs: Vec<RuntimeLogEntry>,
) -> RunTailView {
    RunTailView {
        project_id: project_id.to_owned(),
        events,
        payloads,
        artifacts,
        runtime_logs: if include_logs {
            Some(runtime_logs)
        } else {
            None
        },
    }
}

/// Filter durable history records to a single stage.
pub fn filter_by_stage(
    events: &[JournalEvent],
    payloads: &[PayloadRecord],
    artifacts: &[ArtifactRecord],
    stage_id: StageId,
) -> (Vec<JournalEvent>, Vec<PayloadRecord>, Vec<ArtifactRecord>) {
    let stage_name = stage_id.as_str();

    let filtered_events = events
        .iter()
        .filter(|event| {
            event
                .details
                .get("stage_id")
                .and_then(|value| value.as_str())
                .is_some_and(|value| value == stage_name)
                || event
                    .details
                    .get("source_stage")
                    .and_then(|value| value.as_str())
                    .is_some_and(|value| value == stage_name)
        })
        .cloned()
        .collect();
    let filtered_payloads = payloads
        .iter()
        .filter(|payload| payload.stage_id == stage_id)
        .cloned()
        .collect();
    let filtered_artifacts = artifacts
        .iter()
        .filter(|artifact| artifact.stage_id == stage_id)
        .cloned()
        .collect();

    (filtered_events, filtered_payloads, filtered_artifacts)
}

/// Return the most recent `n` visible journal events and their associated
/// payload/artifact records.
pub fn tail_last_n(
    events: &[JournalEvent],
    payloads: &[PayloadRecord],
    artifacts: &[ArtifactRecord],
    n: usize,
) -> (Vec<JournalEvent>, Vec<PayloadRecord>, Vec<ArtifactRecord>) {
    if n == 0 {
        return (Vec::new(), Vec::new(), Vec::new());
    }

    let event_start = events.len().saturating_sub(n);
    let selected_events = events[event_start..].to_vec();
    let mut payload_ids = HashSet::new();
    let mut artifact_ids = HashSet::new();

    for event in &selected_events {
        if let Some(payload_id) = event
            .details
            .get("payload_id")
            .and_then(|value| value.as_str())
        {
            payload_ids.insert(payload_id.to_owned());
        }
        if let Some(artifact_id) = event
            .details
            .get("artifact_id")
            .and_then(|value| value.as_str())
        {
            artifact_ids.insert(artifact_id.to_owned());
        }
    }

    let selected_payloads = payloads
        .iter()
        .filter(|payload| payload_ids.contains(payload.payload_id.as_str()))
        .cloned()
        .collect();
    let selected_artifacts = artifacts
        .iter()
        .filter(|artifact| {
            artifact_ids.contains(artifact.artifact_id.as_str())
                || payload_ids.contains(artifact.payload_id.as_str())
        })
        .cloned()
        .collect();

    (selected_events, selected_payloads, selected_artifacts)
}

/// Validate that durable history records are consistent:
/// every artifact has a matching payload and every payload has a matching artifact.
/// Payload + artifact are treated as a paired durable-history unit.
pub fn validate_history_consistency(
    payloads: &[PayloadRecord],
    artifacts: &[ArtifactRecord],
) -> AppResult<()> {
    // Check orphaned artifacts (artifact without matching payload)
    for artifact in artifacts {
        if !payloads.iter().any(|p| p.payload_id == artifact.payload_id) {
            return Err(crate::shared::error::AppError::CorruptRecord {
                file: format!("history/artifacts/{}", artifact.artifact_id),
                details: format!(
                    "artifact references payload '{}' which does not exist",
                    artifact.payload_id
                ),
            });
        }
    }
    // Check orphaned payloads (payload without matching artifact)
    for payload in payloads {
        if !artifacts.iter().any(|a| a.payload_id == payload.payload_id) {
            return Err(crate::shared::error::AppError::CorruptRecord {
                file: format!("history/payloads/{}", payload.payload_id),
                details: "payload has no matching artifact".to_owned(),
            });
        }
    }
    Ok(())
}

/// Apply logical rollback boundaries to an append-only journal.
///
/// Each `rollback_performed` event rewinds the visible event stream back to the
/// referenced durable boundary, then becomes the new branch point for
/// subsequent events.
pub fn visible_journal_events(events: &[JournalEvent]) -> AppResult<Vec<JournalEvent>> {
    let mut visible: Vec<JournalEvent> = Vec::with_capacity(events.len());

    for event in events {
        if event.event_type == super::model::JournalEventType::RollbackPerformed {
            let visible_through_sequence = rollback_boundary_sequence(event)?;
            visible.retain(|prior| prior.sequence <= visible_through_sequence);
        }
        visible.push(event.clone());
    }

    Ok(visible)
}

/// Filter payload/artifact history to the records reachable from the visible
/// journal branch after applying rollback boundaries.
///
/// Primary records are matched via `stage_completed` journal events.
/// Supporting and aggregate records are always included (they are persisted
/// outside the journal event flow and remain durable evidence even on failure).
pub fn filter_history_records(
    events: &[JournalEvent],
    payloads: Vec<PayloadRecord>,
    artifacts: Vec<ArtifactRecord>,
) -> AppResult<(Vec<PayloadRecord>, Vec<ArtifactRecord>)> {
    let mut visible_payload_ids = HashSet::new();
    let mut visible_artifact_ids = HashSet::new();

    for event in events {
        if event.event_type != super::model::JournalEventType::StageCompleted {
            continue;
        }

        visible_payload_ids.insert(detail_string(event, "payload_id")?.to_owned());
        visible_artifact_ids.insert(detail_string(event, "artifact_id")?.to_owned());
    }

    let mut visible_payloads: Vec<_> = payloads
        .into_iter()
        .filter(|payload| {
            // Supporting and aggregate records are always visible (durable evidence).
            matches!(
                payload.record_kind,
                RecordKind::StageSupporting | RecordKind::StageAggregate
            ) || visible_payload_ids.contains(payload.payload_id.as_str())
        })
        .collect();
    let mut visible_artifacts: Vec<_> = artifacts
        .into_iter()
        .filter(|artifact| {
            matches!(
                artifact.record_kind,
                RecordKind::StageSupporting | RecordKind::StageAggregate
            ) || (visible_artifact_ids.contains(artifact.artifact_id.as_str())
                && visible_payload_ids.contains(artifact.payload_id.as_str()))
        })
        .collect();

    visible_payloads.sort_by_key(|record| record.created_at);
    visible_artifacts.sort_by_key(|record| record.created_at);

    Ok((visible_payloads, visible_artifacts))
}

fn rollback_boundary_sequence(event: &JournalEvent) -> AppResult<u64> {
    event
        .details
        .get("visible_through_sequence")
        .and_then(|value| value.as_u64())
        .ok_or_else(|| crate::shared::error::AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: format!(
                "rollback_performed event sequence {} is missing 'visible_through_sequence'",
                event.sequence
            ),
        })
}

fn detail_string<'a>(event: &'a JournalEvent, key: &str) -> AppResult<&'a str> {
    event
        .details
        .get(key)
        .and_then(|value| value.as_str())
        .ok_or_else(|| crate::shared::error::AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: format!(
                "event sequence {} is missing string field '{}'",
                event.sequence, key
            ),
        })
}
