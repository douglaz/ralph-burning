use crate::shared::error::{AppError, AppResult};

use super::model::{JournalEvent, JournalEventType};

/// Validates that a new event's sequence follows the last known sequence.
/// The first event must have sequence 1.
pub fn validate_sequence(last_sequence: u64, event: &JournalEvent) -> AppResult<()> {
    let expected = last_sequence + 1;
    if event.sequence != expected {
        return Err(AppError::JournalSequence {
            details: format!("expected sequence {expected}, got {}", event.sequence),
        });
    }
    Ok(())
}

/// Serializes a journal event to a single NDJSON line (no trailing newline).
pub fn serialize_event(event: &JournalEvent) -> AppResult<String> {
    Ok(serde_json::to_string(event)?)
}

/// Deserializes a single NDJSON line into a journal event.
pub fn deserialize_event(line: &str) -> AppResult<JournalEvent> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Err(AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: "empty line in journal".to_owned(),
        });
    }
    serde_json::from_str(trimmed).map_err(|e| AppError::CorruptRecord {
        file: "journal.ndjson".to_owned(),
        details: format!("invalid journal event: {e}"),
    })
}

/// Parses the full contents of a journal.ndjson file into ordered events.
/// Validates monotonic sequence ordering and journal integrity (first event
/// must be `project_created` with sequence 1).
pub fn parse_journal(contents: &str) -> AppResult<Vec<JournalEvent>> {
    let mut events = Vec::new();
    let mut last_sequence = 0u64;

    for line in contents.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let event = deserialize_event(line)?;
        validate_sequence(last_sequence, &event)?;
        last_sequence = event.sequence;
        events.push(event);
    }

    validate_journal_integrity(&events)?;

    Ok(events)
}

/// Validates that a journal has the required structure:
/// - Must not be empty (must contain at least the initial `project_created` event)
/// - First event must be `project_created` with sequence 1
pub fn validate_journal_integrity(events: &[JournalEvent]) -> AppResult<()> {
    if events.is_empty() {
        return Err(AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: "journal is empty — must contain at least the initial project_created event"
                .to_owned(),
        });
    }

    let first = &events[0];
    if first.event_type != JournalEventType::ProjectCreated {
        return Err(AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: format!(
                "first journal event type is '{:?}', expected 'project_created'",
                first.event_type
            ),
        });
    }

    Ok(())
}

/// Returns the last sequence number from a set of events, or 0 if empty.
pub fn last_sequence(events: &[JournalEvent]) -> u64 {
    events.last().map_or(0, |e| e.sequence)
}

// ── Lifecycle event builders ────────────────────────────────────────────────

use crate::shared::domain::{FailureClass, RunId, StageId};
use chrono::{DateTime, Utc};

/// Build a `run_started` journal event.
pub fn run_started_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    run_id: &RunId,
    first_stage: StageId,
) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::RunStarted,
        details: serde_json::json!({
            "run_id": run_id.as_str(),
            "first_stage": first_stage.as_str(),
        }),
    }
}

/// Build a `run_resumed` journal event.
pub fn run_resumed_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    run_id: &RunId,
    resume_stage: StageId,
    cycle: u32,
    completion_round: u32,
) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::RunResumed,
        details: serde_json::json!({
            "run_id": run_id.as_str(),
            "resume_stage": resume_stage.as_str(),
            "cycle": cycle,
            "completion_round": completion_round,
        }),
    }
}

/// Build a `stage_entered` journal event.
pub fn stage_entered_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    run_id: &RunId,
    stage_id: StageId,
    cycle: u32,
    attempt: u32,
) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::StageEntered,
        details: serde_json::json!({
            "run_id": run_id.as_str(),
            "stage_id": stage_id.as_str(),
            "cycle": cycle,
            "attempt": attempt,
        }),
    }
}

/// Build a `stage_completed` journal event.
#[allow(clippy::too_many_arguments)]
pub fn stage_completed_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    run_id: &RunId,
    stage_id: StageId,
    cycle: u32,
    attempt: u32,
    payload_id: &str,
    artifact_id: &str,
) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::StageCompleted,
        details: serde_json::json!({
            "run_id": run_id.as_str(),
            "stage_id": stage_id.as_str(),
            "cycle": cycle,
            "attempt": attempt,
            "payload_id": payload_id,
            "artifact_id": artifact_id,
        }),
    }
}

/// Build a `stage_failed` journal event.
#[allow(clippy::too_many_arguments)]
pub fn stage_failed_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    run_id: &RunId,
    stage_id: StageId,
    cycle: u32,
    attempt: u32,
    failure_class: FailureClass,
    message: &str,
    will_retry: bool,
) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::StageFailed,
        details: serde_json::json!({
            "run_id": run_id.as_str(),
            "stage_id": stage_id.as_str(),
            "cycle": cycle,
            "attempt": attempt,
            "failure_class": failure_class,
            "message": message,
            "will_retry": will_retry,
        }),
    }
}

/// Build a `cycle_advanced` journal event.
pub fn cycle_advanced_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    run_id: &RunId,
    from_stage: StageId,
    from_cycle: u32,
    to_cycle: u32,
    resume_stage: StageId,
) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::CycleAdvanced,
        details: serde_json::json!({
            "run_id": run_id.as_str(),
            "from_stage": from_stage.as_str(),
            "from_cycle": from_cycle,
            "to_cycle": to_cycle,
            "resume_stage": resume_stage.as_str(),
        }),
    }
}

/// Build a `run_completed` journal event.
pub fn run_completed_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    run_id: &RunId,
    completion_rounds: u32,
) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::RunCompleted,
        details: serde_json::json!({
            "run_id": run_id.as_str(),
            "completion_rounds": completion_rounds,
        }),
    }
}

/// Build a `completion_round_advanced` journal event.
#[allow(clippy::too_many_arguments)]
pub fn completion_round_advanced_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    run_id: &RunId,
    source_stage: StageId,
    from_round: u32,
    to_round: u32,
    amendment_count: u32,
) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::CompletionRoundAdvanced,
        details: serde_json::json!({
            "run_id": run_id.as_str(),
            "source_stage": source_stage.as_str(),
            "from_round": from_round,
            "to_round": to_round,
            "amendment_count": amendment_count,
        }),
    }
}

/// Build an `amendment_queued` journal event with source metadata.
#[allow(clippy::too_many_arguments)]
pub fn amendment_queued_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    run_id: &RunId,
    amendment_id: &str,
    source_stage: StageId,
    body: &str,
    source: &str,
    dedup_key: &str,
) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::AmendmentQueued,
        details: serde_json::json!({
            "run_id": run_id.as_str(),
            "amendment_id": amendment_id,
            "source_stage": source_stage.as_str(),
            "body": body,
            "source": source,
            "dedup_key": dedup_key,
        }),
    }
}

/// Build an `amendment_queued` journal event for manual/batch amendments (no run_id).
pub fn amendment_queued_manual_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    amendment_id: &str,
    body: &str,
    source: &str,
    source_stage: &str,
    dedup_key: &str,
) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::AmendmentQueued,
        details: serde_json::json!({
            "amendment_id": amendment_id,
            "source": source,
            "source_stage": source_stage,
            "dedup_key": dedup_key,
            "body": body,
        }),
    }
}

/// Build a `rollback_created` journal event.
pub fn rollback_created_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    run_id: &RunId,
    rollback_id: &str,
    stage_id: StageId,
    cycle: u32,
    git_sha: Option<&str>,
) -> JournalEvent {
    let mut details = serde_json::json!({
        "run_id": run_id.as_str(),
        "rollback_id": rollback_id,
        "stage_id": stage_id.as_str(),
        "cycle": cycle,
    });

    if let Some(git_sha) = git_sha {
        details["git_sha"] = serde_json::json!(git_sha);
    }

    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::RollbackCreated,
        details,
    }
}

/// Build a `rollback_performed` journal event.
#[allow(clippy::too_many_arguments)]
pub fn rollback_performed_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    rollback_id: &str,
    stage_id: StageId,
    cycle: u32,
    visible_through_sequence: u64,
    hard: bool,
    git_sha: Option<&str>,
    rollback_count: u32,
) -> JournalEvent {
    let mut details = serde_json::json!({
        "rollback_id": rollback_id,
        "stage_id": stage_id.as_str(),
        "cycle": cycle,
        "visible_through_sequence": visible_through_sequence,
        "hard": hard,
        "rollback_count": rollback_count,
    });

    if let Some(git_sha) = git_sha {
        details["git_sha"] = serde_json::json!(git_sha);
    }

    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::RollbackPerformed,
        details,
    }
}

/// Build a `durable_warning` journal event for resume drift or other persistent warnings.
pub fn durable_warning_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    run_id: &RunId,
    stage_id: StageId,
    warning_kind: &str,
    message: &str,
    details: serde_json::Value,
) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::DurableWarning,
        details: serde_json::json!({
            "run_id": run_id.as_str(),
            "stage_id": stage_id.as_str(),
            "warning_kind": warning_kind,
            "message": message,
            "details": details,
        }),
    }
}

/// Build a `run_failed` journal event.
pub fn run_failed_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    run_id: &RunId,
    stage_id: StageId,
    failure_class: &str,
    message: &str,
) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp,
        event_type: JournalEventType::RunFailed,
        details: serde_json::json!({
            "run_id": run_id.as_str(),
            "stage_id": stage_id.as_str(),
            "failure_class": failure_class,
            "message": message,
        }),
    }
}
