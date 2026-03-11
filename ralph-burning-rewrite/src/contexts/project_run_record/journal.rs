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
