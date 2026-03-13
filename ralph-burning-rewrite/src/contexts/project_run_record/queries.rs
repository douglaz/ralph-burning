use std::collections::HashSet;

use crate::shared::error::AppResult;

use super::model::{ArtifactRecord, JournalEvent, PayloadRecord, RunSnapshot, RuntimeLogEntry};

/// Read model for `run status`: canonical state only, no inference from artifacts.
#[derive(Debug, Clone)]
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

/// Read model for `run history`: durable history only (journal + payloads + artifacts).
/// Runtime logs never appear here.
#[derive(Debug, Clone)]
pub struct RunHistoryView {
    pub project_id: String,
    pub events: Vec<JournalEvent>,
    pub payloads: Vec<PayloadRecord>,
    pub artifacts: Vec<ArtifactRecord>,
}

/// Read model for `run tail`: durable history by default, with optional runtime logs.
#[derive(Debug, Clone)]
pub struct RunTailView {
    pub project_id: String,
    pub events: Vec<JournalEvent>,
    pub payloads: Vec<PayloadRecord>,
    pub artifacts: Vec<ArtifactRecord>,
    pub runtime_logs: Option<Vec<RuntimeLogEntry>>,
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
        .filter(|payload| visible_payload_ids.contains(payload.payload_id.as_str()))
        .collect();
    let mut visible_artifacts: Vec<_> = artifacts
        .into_iter()
        .filter(|artifact| {
            visible_artifact_ids.contains(artifact.artifact_id.as_str())
                && visible_payload_ids.contains(artifact.payload_id.as_str())
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
