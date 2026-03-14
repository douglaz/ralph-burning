use chrono::{TimeZone, Utc};

use ralph_burning::contexts::project_run_record::journal;
use ralph_burning::contexts::project_run_record::model::{JournalEvent, JournalEventType};
use ralph_burning::shared::domain::{FailureClass, RunId, StageId};
use ralph_burning::shared::error::AppError;

fn test_timestamp() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 3, 11, 19, 0, 0)
        .single()
        .expect("valid timestamp")
}

fn make_event(sequence: u64, event_type: JournalEventType) -> JournalEvent {
    JournalEvent {
        sequence,
        timestamp: test_timestamp(),
        event_type,
        details: serde_json::json!({}),
    }
}

// ── Sequence Validation ──

#[test]
fn validate_sequence_accepts_first_event_at_1() {
    let event = make_event(1, JournalEventType::ProjectCreated);
    assert!(journal::validate_sequence(0, &event).is_ok());
}

#[test]
fn validate_sequence_accepts_monotonic_increment() {
    let event = make_event(5, JournalEventType::StageEntered);
    assert!(journal::validate_sequence(4, &event).is_ok());
}

#[test]
fn validate_sequence_rejects_gap() {
    let event = make_event(3, JournalEventType::StageCompleted);
    let result = journal::validate_sequence(1, &event);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        AppError::JournalSequence { .. }
    ));
}

#[test]
fn validate_sequence_rejects_duplicate() {
    let event = make_event(2, JournalEventType::CycleAdvanced);
    let result = journal::validate_sequence(2, &event);
    assert!(result.is_err());
}

#[test]
fn validate_sequence_rejects_backwards() {
    let event = make_event(1, JournalEventType::RunStarted);
    let result = journal::validate_sequence(3, &event);
    assert!(result.is_err());
}

// ── Serialization ──

#[test]
fn serialize_event_produces_single_line_json() {
    let event = make_event(1, JournalEventType::ProjectCreated);
    let line = journal::serialize_event(&event).expect("serialize");
    assert!(!line.contains('\n'));
    assert!(line.contains("\"project_created\""));
    assert!(line.contains("\"sequence\":1"));
}

#[test]
fn deserialize_event_round_trips() {
    let event = make_event(1, JournalEventType::ProjectCreated);
    let line = journal::serialize_event(&event).expect("serialize");
    let parsed = journal::deserialize_event(&line).expect("deserialize");
    assert_eq!(event, parsed);
}

#[test]
fn deserialize_event_rejects_empty_line() {
    let result = journal::deserialize_event("");
    assert!(matches!(
        result.unwrap_err(),
        AppError::CorruptRecord { .. }
    ));
}

#[test]
fn deserialize_event_rejects_malformed_json() {
    let result = journal::deserialize_event("{not valid json}");
    assert!(matches!(
        result.unwrap_err(),
        AppError::CorruptRecord { .. }
    ));
}

// ── Full Journal Parsing ──

#[test]
fn parse_journal_rejects_empty_content() {
    let result = journal::parse_journal("");
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
    match err {
        AppError::CorruptRecord { details, .. } => {
            assert!(details.contains("empty"));
        }
        _ => panic!("expected CorruptRecord"),
    }
}

#[test]
fn parse_journal_rejects_non_project_created_first_event() {
    let event = make_event(1, JournalEventType::RunStarted);
    let content = format!("{}\n", journal::serialize_event(&event).unwrap());

    let result = journal::parse_journal(&content);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
    match err {
        AppError::CorruptRecord { details, .. } => {
            assert!(details.contains("project_created"));
        }
        _ => panic!("expected CorruptRecord"),
    }
}

#[test]
fn parse_journal_parses_ordered_events() {
    let e1 = make_event(1, JournalEventType::ProjectCreated);
    let e2 = make_event(2, JournalEventType::RunStarted);
    let content = format!(
        "{}\n{}\n",
        journal::serialize_event(&e1).unwrap(),
        journal::serialize_event(&e2).unwrap()
    );

    let events = journal::parse_journal(&content).expect("parse");
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].sequence, 1);
    assert_eq!(events[1].sequence, 2);
}

#[test]
fn parse_journal_rejects_out_of_order_sequence() {
    let e1 = make_event(1, JournalEventType::ProjectCreated);
    let e2 = make_event(3, JournalEventType::RunStarted); // gap
    let content = format!(
        "{}\n{}\n",
        journal::serialize_event(&e1).unwrap(),
        journal::serialize_event(&e2).unwrap()
    );

    let result = journal::parse_journal(&content);
    assert!(matches!(
        result.unwrap_err(),
        AppError::JournalSequence { .. }
    ));
}

#[test]
fn parse_journal_skips_blank_lines() {
    let e1 = make_event(1, JournalEventType::ProjectCreated);
    let content = format!("\n{}\n\n", journal::serialize_event(&e1).unwrap());

    let events = journal::parse_journal(&content).expect("parse");
    assert_eq!(events.len(), 1);
}

// ── last_sequence ──

#[test]
fn last_sequence_returns_0_for_empty() {
    assert_eq!(journal::last_sequence(&[]), 0);
}

#[test]
fn last_sequence_returns_last_event_sequence() {
    let events = vec![
        make_event(1, JournalEventType::ProjectCreated),
        make_event(2, JournalEventType::RunStarted),
    ];
    assert_eq!(journal::last_sequence(&events), 2);
}

#[test]
fn stage_failed_event_builder_serializes_failure_metadata() {
    let run_id = RunId::new("run-1").expect("run id");
    let event = journal::stage_failed_event(
        2,
        test_timestamp(),
        &run_id,
        StageId::Implementation,
        1,
        2,
        FailureClass::TransportFailure,
        "temporary network issue",
        true,
    );

    assert_eq!(event.event_type, JournalEventType::StageFailed);
    assert_eq!(event.details["stage_id"], "implementation");
    assert_eq!(event.details["attempt"], 2);
    assert_eq!(event.details["failure_class"], "transport_failure");
    assert_eq!(event.details["will_retry"], true);
}

#[test]
fn run_resumed_event_builder_serializes_resume_cursor() {
    let run_id = RunId::new("run-1").expect("run id");
    let event = journal::run_resumed_event(3, test_timestamp(), &run_id, StageId::Planning, 2, 4);

    assert_eq!(event.event_type, JournalEventType::RunResumed);
    assert_eq!(event.details["run_id"], "run-1");
    assert_eq!(event.details["resume_stage"], "planning");
    assert_eq!(event.details["cycle"], 2);
    assert_eq!(event.details["completion_round"], 4);
}

#[test]
fn completion_round_advanced_event_builder_serializes_round_metadata() {
    let run_id = RunId::new("run-1").expect("run id");
    let event = journal::completion_round_advanced_event(
        4,
        test_timestamp(),
        &run_id,
        StageId::CompletionPanel,
        1,
        2,
        3,
    );

    assert_eq!(event.event_type, JournalEventType::CompletionRoundAdvanced);
    assert_eq!(event.sequence, 4);
    assert_eq!(event.details["run_id"], "run-1");
    assert_eq!(event.details["source_stage"], "completion_panel");
    assert_eq!(event.details["from_round"], 1);
    assert_eq!(event.details["to_round"], 2);
    assert_eq!(event.details["amendment_count"], 3);
}

#[test]
fn amendment_queued_event_builder_serializes_amendment_metadata() {
    let run_id = RunId::new("run-1").expect("run id");
    let event = journal::amendment_queued_event(
        5,
        test_timestamp(),
        &run_id,
        "amend-001",
        StageId::AcceptanceQa,
        "fix the widget alignment",
    );

    assert_eq!(event.event_type, JournalEventType::AmendmentQueued);
    assert_eq!(event.sequence, 5);
    assert_eq!(event.details["run_id"], "run-1");
    assert_eq!(event.details["amendment_id"], "amend-001");
    assert_eq!(event.details["source_stage"], "acceptance_qa");
    assert_eq!(event.details["body"], "fix the widget alignment");
}
