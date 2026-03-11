use chrono::{TimeZone, Utc};

use ralph_burning::contexts::project_run_record::model::*;
use ralph_burning::contexts::project_run_record::queries;
use ralph_burning::shared::domain::{StageCursor, StageId};
use ralph_burning::shared::error::AppError;

fn test_timestamp() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 3, 11, 19, 0, 0)
        .single()
        .expect("valid timestamp")
}

// ── RunStatusView ──

#[test]
fn status_view_not_started_when_no_active_run() {
    let snapshot = RunSnapshot::initial();
    let view = queries::build_status_view("alpha", &snapshot);

    assert_eq!(view.project_id, "alpha");
    assert_eq!(view.status, "not started");
    assert!(view.stage.is_none());
    assert!(view.cycle.is_none());
    assert!(view.completion_round.is_none());
}

#[test]
fn status_view_reports_running_with_cursor() {
    let snapshot = RunSnapshot {
        active_run: Some(ActiveRun {
            run_id: "run-1".to_owned(),
            stage_cursor: StageCursor::initial(StageId::Planning),
            started_at: test_timestamp(),
        }),
        status: RunStatus::Running,
        status_summary: "running at planning".to_owned(),
    };
    let view = queries::build_status_view("alpha", &snapshot);

    assert_eq!(view.status, "running");
    assert_eq!(view.stage, Some("planning".to_owned()));
    assert_eq!(view.cycle, Some(1));
    assert_eq!(view.completion_round, Some(1));
}

// ── RunHistoryView ──

#[test]
fn history_view_contains_only_durable_records() {
    let events = vec![JournalEvent {
        sequence: 1,
        timestamp: test_timestamp(),
        event_type: JournalEventType::ProjectCreated,
        details: serde_json::json!({}),
    }];

    let view = queries::build_history_view("alpha", events.clone(), vec![], vec![]);

    assert_eq!(view.project_id, "alpha");
    assert_eq!(view.events.len(), 1);
    assert!(view.payloads.is_empty());
    assert!(view.artifacts.is_empty());
}

// ── RunTailView ──

#[test]
fn tail_view_omits_logs_when_not_requested() {
    let view = queries::build_tail_view("alpha", vec![], vec![], vec![], false, vec![]);
    assert!(view.runtime_logs.is_none());
}

#[test]
fn tail_view_includes_logs_when_requested() {
    let logs = vec![RuntimeLogEntry {
        timestamp: test_timestamp(),
        level: LogLevel::Info,
        source: "agent".to_owned(),
        message: "started".to_owned(),
    }];

    let view = queries::build_tail_view("alpha", vec![], vec![], vec![], true, logs);
    assert!(view.runtime_logs.is_some());
    assert_eq!(view.runtime_logs.as_ref().unwrap().len(), 1);
}

#[test]
fn tail_view_includes_empty_logs_when_requested_but_none_exist() {
    let view = queries::build_tail_view("alpha", vec![], vec![], vec![], true, vec![]);
    assert!(view.runtime_logs.is_some());
    assert!(view.runtime_logs.as_ref().unwrap().is_empty());
}

// ── History Consistency Validation ──

#[test]
fn validate_history_consistency_passes_with_matching_records() {
    let payloads = vec![PayloadRecord {
        payload_id: "p1".to_owned(),
        stage_id: StageId::Planning,
        cycle: 1,
        attempt: 1,
        created_at: test_timestamp(),
        payload: serde_json::json!({}),
    }];
    let artifacts = vec![ArtifactRecord {
        artifact_id: "a1".to_owned(),
        payload_id: "p1".to_owned(),
        stage_id: StageId::Planning,
        created_at: test_timestamp(),
        content: "# Planning".to_owned(),
    }];

    assert!(queries::validate_history_consistency(&payloads, &artifacts).is_ok());
}

#[test]
fn validate_history_consistency_fails_with_orphaned_artifact() {
    let payloads = vec![];
    let artifacts = vec![ArtifactRecord {
        artifact_id: "a1".to_owned(),
        payload_id: "p-missing".to_owned(),
        stage_id: StageId::Planning,
        created_at: test_timestamp(),
        content: "# Planning".to_owned(),
    }];

    let result = queries::validate_history_consistency(&payloads, &artifacts);
    assert!(matches!(
        result.unwrap_err(),
        AppError::CorruptRecord { .. }
    ));
}

#[test]
fn validate_history_consistency_passes_with_no_records() {
    assert!(queries::validate_history_consistency(&[], &[]).is_ok());
}

// ── Runtime Log Separation ──

#[test]
fn runtime_logs_never_appear_in_history_view() {
    // History view has no field for runtime logs - enforced by type system.
    let view = queries::build_history_view("alpha", vec![], vec![], vec![]);
    // RunHistoryView has no runtime_logs field, so logs can never leak.
    assert_eq!(view.project_id, "alpha");
}
