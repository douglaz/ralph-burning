use chrono::{TimeZone, Utc};

use ralph_burning::contexts::project_run_record::model::*;
use ralph_burning::contexts::project_run_record::queries;
use ralph_burning::contexts::workflow_composition::panel_contracts::RecordKind;
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
    let snapshot = RunSnapshot::initial(20);
    let view = queries::build_status_view("alpha", &snapshot);

    assert_eq!(view.project_id, "alpha");
    assert_eq!(view.status, "not started");
    assert!(view.stage.is_none());
    assert!(view.cycle.is_none());
    assert!(view.completion_round.is_none());
}

#[test]
fn status_view_reports_completed_terminal_state_without_active_run() {
    let snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Completed,
        cycle_history: Vec::new(),
        completion_rounds: 3,
        max_completion_rounds: Some(0),
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "completed after 3 rounds".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    let view = queries::build_status_view("alpha", &snapshot);

    assert_eq!(view.status, "completed");
    assert!(view.stage.is_none());
    assert_eq!(view.summary, "completed after 3 rounds");
}

#[test]
fn status_view_reports_failed_terminal_state_without_active_run() {
    let snapshot = RunSnapshot {
        active_run: None,
        interrupted_run: None,
        status: RunStatus::Failed,
        cycle_history: Vec::new(),
        completion_rounds: 0,
        max_completion_rounds: Some(0),
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "failed at implementation".to_owned(),
        last_stage_resolution_snapshot: None,
    };
    let view = queries::build_status_view("alpha", &snapshot);

    assert_eq!(view.status, "failed");
    assert_eq!(view.summary, "failed at implementation");
}

#[test]
fn status_view_reports_running_with_cursor() {
    let snapshot = RunSnapshot {
        active_run: Some(ActiveRun {
            run_id: "run-1".to_owned(),
            stage_cursor: StageCursor::initial(StageId::Planning),
            started_at: test_timestamp(),
            prompt_hash_at_cycle_start: "prompt-hash".to_owned(),
            prompt_hash_at_stage_start: "prompt-hash".to_owned(),
            qa_iterations_current_cycle: 0,
            review_iterations_current_cycle: 0,
            final_review_restart_count: 0,
            iterative_implementer_state: None,
            stage_resolution_snapshot: None,
        }),
        interrupted_run: None,
        status: RunStatus::Running,
        cycle_history: Vec::new(),
        completion_rounds: 0,
        max_completion_rounds: Some(0),
        rollback_point_meta: RollbackPointMeta::default(),
        amendment_queue: AmendmentQueueState::default(),
        status_summary: "running at planning".to_owned(),
        last_stage_resolution_snapshot: None,
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

    let view = queries::build_history_view("alpha", None, None, events.clone(), vec![], vec![]);

    assert_eq!(view.project_id, "alpha");
    assert_eq!(view.events.len(), 1);
    assert!(view.payloads.is_empty());
    assert!(view.artifacts.is_empty());
}

#[test]
fn history_view_preserves_optional_lineage_fields() {
    let events = vec![JournalEvent {
        sequence: 1,
        timestamp: test_timestamp(),
        event_type: JournalEventType::ProjectCreated,
        details: serde_json::json!({
            "project_id": "alpha",
            "flow": "standard",
            "milestone_id": "ms-alpha",
            "bead_id": "ms-alpha.bead-1"
        }),
    }];

    let (milestone_id, bead_id) = queries::history_lineage(&events);
    let view = queries::build_history_view("alpha", milestone_id, bead_id, events, vec![], vec![]);

    assert_eq!(view.milestone_id.as_deref(), Some("ms-alpha"));
    assert_eq!(view.bead_id.as_deref(), Some("ms-alpha.bead-1"));
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

// ── Rollback Boundary Filtering ──

#[test]
fn visible_journal_events_prune_rolled_back_branch_but_keep_new_branch() {
    let events = vec![
        JournalEvent {
            sequence: 1,
            timestamp: test_timestamp(),
            event_type: JournalEventType::ProjectCreated,
            details: serde_json::json!({}),
        },
        JournalEvent {
            sequence: 2,
            timestamp: test_timestamp(),
            event_type: JournalEventType::StageCompleted,
            details: serde_json::json!({
                "stage_id": "planning",
                "cycle": 1,
                "payload_id": "p1",
                "artifact_id": "a1"
            }),
        },
        JournalEvent {
            sequence: 3,
            timestamp: test_timestamp(),
            event_type: JournalEventType::RollbackCreated,
            details: serde_json::json!({
                "rollback_id": "rb-planning",
                "stage_id": "planning",
                "cycle": 1
            }),
        },
        JournalEvent {
            sequence: 4,
            timestamp: test_timestamp(),
            event_type: JournalEventType::StageCompleted,
            details: serde_json::json!({
                "stage_id": "implementation",
                "cycle": 1,
                "payload_id": "p2",
                "artifact_id": "a2"
            }),
        },
        JournalEvent {
            sequence: 5,
            timestamp: test_timestamp(),
            event_type: JournalEventType::RollbackPerformed,
            details: serde_json::json!({
                "rollback_id": "rb-planning",
                "stage_id": "planning",
                "cycle": 1,
                "visible_through_sequence": 3,
                "hard": false,
                "rollback_count": 1
            }),
        },
        JournalEvent {
            sequence: 6,
            timestamp: test_timestamp(),
            event_type: JournalEventType::RunResumed,
            details: serde_json::json!({}),
        },
        JournalEvent {
            sequence: 7,
            timestamp: test_timestamp(),
            event_type: JournalEventType::StageCompleted,
            details: serde_json::json!({
                "stage_id": "implementation",
                "cycle": 1,
                "payload_id": "p3",
                "artifact_id": "a3"
            }),
        },
    ];

    let visible = queries::visible_journal_events(&events).expect("visible journal");
    let sequences: Vec<_> = visible.iter().map(|event| event.sequence).collect();
    assert_eq!(sequences, vec![1, 2, 3, 5, 6, 7]);
}

#[test]
fn filter_history_records_hides_payloads_from_rolled_back_branch() {
    let events = queries::visible_journal_events(&[
        JournalEvent {
            sequence: 1,
            timestamp: test_timestamp(),
            event_type: JournalEventType::ProjectCreated,
            details: serde_json::json!({}),
        },
        JournalEvent {
            sequence: 2,
            timestamp: test_timestamp(),
            event_type: JournalEventType::StageCompleted,
            details: serde_json::json!({
                "stage_id": "planning",
                "cycle": 1,
                "payload_id": "p1",
                "artifact_id": "a1"
            }),
        },
        JournalEvent {
            sequence: 3,
            timestamp: test_timestamp(),
            event_type: JournalEventType::RollbackCreated,
            details: serde_json::json!({
                "rollback_id": "rb-planning",
                "stage_id": "planning",
                "cycle": 1
            }),
        },
        JournalEvent {
            sequence: 4,
            timestamp: test_timestamp(),
            event_type: JournalEventType::StageCompleted,
            details: serde_json::json!({
                "stage_id": "implementation",
                "cycle": 1,
                "payload_id": "p2",
                "artifact_id": "a2"
            }),
        },
        JournalEvent {
            sequence: 5,
            timestamp: test_timestamp(),
            event_type: JournalEventType::RollbackPerformed,
            details: serde_json::json!({
                "rollback_id": "rb-planning",
                "stage_id": "planning",
                "cycle": 1,
                "visible_through_sequence": 3,
                "hard": false,
                "rollback_count": 1
            }),
        },
    ])
    .expect("visible events");

    let payloads = vec![
        PayloadRecord {
            payload_id: "p1".to_owned(),
            stage_id: StageId::Planning,
            cycle: 1,
            attempt: 1,
            created_at: test_timestamp(),
            payload: serde_json::json!({}),
            record_kind: RecordKind::StagePrimary,
            producer: None,
            completion_round: 0,
        },
        PayloadRecord {
            payload_id: "p2".to_owned(),
            stage_id: StageId::Implementation,
            cycle: 1,
            attempt: 1,
            created_at: test_timestamp(),
            payload: serde_json::json!({}),
            record_kind: RecordKind::StagePrimary,
            producer: None,
            completion_round: 0,
        },
    ];
    let artifacts = vec![
        ArtifactRecord {
            artifact_id: "a1".to_owned(),
            payload_id: "p1".to_owned(),
            stage_id: StageId::Planning,
            created_at: test_timestamp(),
            content: "planning".to_owned(),
            record_kind: RecordKind::StagePrimary,
            producer: None,
            completion_round: 0,
        },
        ArtifactRecord {
            artifact_id: "a2".to_owned(),
            payload_id: "p2".to_owned(),
            stage_id: StageId::Implementation,
            created_at: test_timestamp(),
            content: "implementation".to_owned(),
            record_kind: RecordKind::StagePrimary,
            producer: None,
            completion_round: 0,
        },
    ];

    let (visible_payloads, visible_artifacts) =
        queries::filter_history_records(&events, payloads, artifacts).expect("filtered history");
    assert_eq!(visible_payloads.len(), 1);
    assert_eq!(visible_payloads[0].payload_id, "p1");
    assert_eq!(visible_artifacts.len(), 1);
    assert_eq!(visible_artifacts[0].artifact_id, "a1");
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
        record_kind: RecordKind::StagePrimary,
        producer: None,
        completion_round: 0,
    }];
    let artifacts = vec![ArtifactRecord {
        artifact_id: "a1".to_owned(),
        payload_id: "p1".to_owned(),
        stage_id: StageId::Planning,
        created_at: test_timestamp(),
        content: "# Planning".to_owned(),
        record_kind: RecordKind::StagePrimary,
        producer: None,
        completion_round: 0,
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
        record_kind: RecordKind::StagePrimary,
        producer: None,
        completion_round: 0,
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

// ── Orphaned Payload Detection ──

#[test]
fn validate_history_consistency_fails_with_orphaned_payload() {
    let payloads = vec![PayloadRecord {
        payload_id: "p-orphan".to_owned(),
        stage_id: StageId::Planning,
        cycle: 1,
        attempt: 1,
        created_at: test_timestamp(),
        payload: serde_json::json!({}),
        record_kind: RecordKind::StagePrimary,
        producer: None,
        completion_round: 0,
    }];
    let artifacts = vec![];

    let result = queries::validate_history_consistency(&payloads, &artifacts);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
    match err {
        AppError::CorruptRecord { details, .. } => {
            assert!(details.contains("no matching artifact"));
        }
        _ => panic!("expected CorruptRecord"),
    }
}

// ── RunSnapshot Schema Completeness ──

#[test]
fn run_snapshot_initial_includes_all_canonical_fields() {
    let snapshot = RunSnapshot::initial(20);
    let json = serde_json::to_string_pretty(&snapshot).expect("serialize");

    // Verify all canonical fields are present in the serialized form
    assert!(json.contains("\"active_run\""));
    assert!(json.contains("\"status\""));
    assert!(json.contains("\"cycle_history\""));
    assert!(json.contains("\"completion_rounds\""));
    assert!(json.contains("\"max_completion_rounds\""));
    assert!(json.contains("\"rollback_point_meta\""));
    assert!(json.contains("\"amendment_queue\""));
    assert!(json.contains("\"status_summary\""));
}

#[test]
fn run_snapshot_round_trips_through_json() {
    let snapshot = RunSnapshot::initial(20);
    let json = serde_json::to_string(&snapshot).expect("serialize");
    let parsed: RunSnapshot = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(snapshot, parsed);
}

// ── Runtime Log Separation ──

#[test]
fn runtime_logs_never_appear_in_history_view() {
    // History view has no field for runtime logs - enforced by type system.
    let view = queries::build_history_view("alpha", None, None, vec![], vec![], vec![]);
    // RunHistoryView has no runtime_logs field, so logs can never leak.
    assert_eq!(view.project_id, "alpha");
}
