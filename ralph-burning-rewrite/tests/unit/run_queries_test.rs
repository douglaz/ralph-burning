use std::path::Path;

use chrono::{TimeZone, Utc};

use ralph_burning::contexts::project_run_record::model::{
    AmendmentQueueState, ArtifactRecord, JournalEvent, JournalEventType, PayloadRecord,
    RollbackPointMeta, RunSnapshot, RunStatus, RuntimeLogEntry,
};
use ralph_burning::contexts::project_run_record::queries::{
    self, RunHistoryView, RunRollbackTargetView, RunStatusJsonView, RunStatusView, RunTailView,
};
use ralph_burning::contexts::project_run_record::service::{
    self, ArtifactStorePort, JournalStorePort,
};
use ralph_burning::contexts::workflow_composition::panel_contracts::RecordKind;
use ralph_burning::shared::domain::{ProjectId, StageId};
use ralph_burning::shared::error::AppError;

fn test_timestamp() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 3, 19, 3, 0, 0)
        .single()
        .expect("valid timestamp")
}

fn project_id() -> ProjectId {
    ProjectId::new("alpha").expect("valid project id")
}

fn planning_payload() -> PayloadRecord {
    PayloadRecord {
        payload_id: "p1".to_owned(),
        stage_id: StageId::Planning,
        cycle: 1,
        attempt: 1,
        created_at: test_timestamp(),
        payload: serde_json::json!({
            "summary": "planning payload"
        }),
        record_kind: RecordKind::StagePrimary,
        producer: None,
        completion_round: 1,
    }
}

fn implementation_payload() -> PayloadRecord {
    PayloadRecord {
        payload_id: "p2".to_owned(),
        stage_id: StageId::Implementation,
        cycle: 1,
        attempt: 1,
        created_at: test_timestamp(),
        payload: serde_json::json!({
            "summary": "implementation payload"
        }),
        record_kind: RecordKind::StagePrimary,
        producer: None,
        completion_round: 1,
    }
}

fn planning_artifact() -> ArtifactRecord {
    ArtifactRecord {
        artifact_id: "a1".to_owned(),
        payload_id: "p1".to_owned(),
        stage_id: StageId::Planning,
        created_at: test_timestamp(),
        content: "# Planning\nvisible".to_owned(),
        record_kind: RecordKind::StagePrimary,
        producer: None,
        completion_round: 1,
    }
}

fn implementation_artifact() -> ArtifactRecord {
    ArtifactRecord {
        artifact_id: "a2".to_owned(),
        payload_id: "p2".to_owned(),
        stage_id: StageId::Implementation,
        created_at: test_timestamp(),
        content: "# Implementation\nrolled back".to_owned(),
        record_kind: RecordKind::StagePrimary,
        producer: None,
        completion_round: 1,
    }
}

fn stage_history_events() -> Vec<JournalEvent> {
    vec![
        JournalEvent {
            sequence: 1,
            timestamp: test_timestamp(),
            event_type: JournalEventType::ProjectCreated,
            details: serde_json::json!({
                "project_id": "alpha",
                "flow": "standard"
            }),
        },
        JournalEvent {
            sequence: 2,
            timestamp: test_timestamp(),
            event_type: JournalEventType::StageEntered,
            details: serde_json::json!({
                "stage_id": "planning",
                "run_id": "run-1"
            }),
        },
        JournalEvent {
            sequence: 3,
            timestamp: test_timestamp(),
            event_type: JournalEventType::StageCompleted,
            details: serde_json::json!({
                "stage_id": "planning",
                "cycle": 1,
                "attempt": 1,
                "payload_id": "p1",
                "artifact_id": "a1"
            }),
        },
        JournalEvent {
            sequence: 4,
            timestamp: test_timestamp(),
            event_type: JournalEventType::StageEntered,
            details: serde_json::json!({
                "stage_id": "implementation",
                "run_id": "run-1"
            }),
        },
        JournalEvent {
            sequence: 5,
            timestamp: test_timestamp(),
            event_type: JournalEventType::StageCompleted,
            details: serde_json::json!({
                "stage_id": "implementation",
                "cycle": 1,
                "attempt": 1,
                "payload_id": "p2",
                "artifact_id": "a2"
            }),
        },
    ]
}

fn rollback_history_events() -> Vec<JournalEvent> {
    vec![
        JournalEvent {
            sequence: 1,
            timestamp: test_timestamp(),
            event_type: JournalEventType::ProjectCreated,
            details: serde_json::json!({
                "project_id": "alpha",
                "flow": "standard"
            }),
        },
        JournalEvent {
            sequence: 2,
            timestamp: test_timestamp(),
            event_type: JournalEventType::StageCompleted,
            details: serde_json::json!({
                "stage_id": "planning",
                "cycle": 1,
                "attempt": 1,
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
                "attempt": 1,
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
    ]
}

#[derive(Clone)]
struct StaticJournalStore {
    events: Vec<JournalEvent>,
}

impl JournalStorePort for StaticJournalStore {
    fn read_journal(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> ralph_burning::shared::error::AppResult<Vec<JournalEvent>> {
        Ok(self.events.clone())
    }

    fn append_event(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        _line: &str,
    ) -> ralph_burning::shared::error::AppResult<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct StaticArtifactStore {
    payloads: Vec<PayloadRecord>,
    artifacts: Vec<ArtifactRecord>,
}

impl ArtifactStorePort for StaticArtifactStore {
    fn list_payloads(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> ralph_burning::shared::error::AppResult<Vec<PayloadRecord>> {
        Ok(self.payloads.clone())
    }

    fn list_artifacts(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> ralph_burning::shared::error::AppResult<Vec<ArtifactRecord>> {
        Ok(self.artifacts.clone())
    }
}

#[test]
fn filter_by_stage_keeps_only_matching_stage_records() {
    let (events, payloads, artifacts) = queries::filter_by_stage(
        &stage_history_events(),
        &[planning_payload(), implementation_payload()],
        &[planning_artifact(), implementation_artifact()],
        StageId::Planning,
    );

    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| {
        event
            .details
            .get("stage_id")
            .and_then(|value| value.as_str())
            == Some("planning")
    }));
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0].payload_id, "p1");
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].artifact_id, "a1");
}

#[test]
fn tail_last_n_keeps_last_events_and_associated_records() {
    let (events, payloads, artifacts) = queries::tail_last_n(
        &stage_history_events(),
        &[planning_payload(), implementation_payload()],
        &[planning_artifact(), implementation_artifact()],
        2,
    );

    let sequences: Vec<_> = events.iter().map(|event| event.sequence).collect();
    assert_eq!(sequences, vec![4, 5]);
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0].payload_id, "p2");
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].artifact_id, "a2");
}

#[test]
fn get_payload_by_id_returns_visible_payload() {
    let journal = StaticJournalStore {
        events: rollback_history_events(),
    };
    let artifacts = StaticArtifactStore {
        payloads: vec![planning_payload(), implementation_payload()],
        artifacts: vec![planning_artifact(), implementation_artifact()],
    };

    let payload =
        service::get_payload_by_id(&journal, &artifacts, Path::new("."), &project_id(), "p1")
            .expect("visible payload");

    assert_eq!(payload.payload_id, "p1");
    assert_eq!(payload.payload["summary"], "planning payload");
}

#[test]
fn get_payload_by_id_hides_rolled_back_payloads() {
    let journal = StaticJournalStore {
        events: rollback_history_events(),
    };
    let artifacts = StaticArtifactStore {
        payloads: vec![planning_payload(), implementation_payload()],
        artifacts: vec![planning_artifact(), implementation_artifact()],
    };

    let error =
        service::get_payload_by_id(&journal, &artifacts, Path::new("."), &project_id(), "p2")
            .expect_err("rolled-back payload must be hidden");

    assert!(matches!(error, AppError::PayloadNotFound { .. }));
}

#[test]
fn get_artifact_by_id_returns_visible_artifact() {
    let journal = StaticJournalStore {
        events: rollback_history_events(),
    };
    let artifacts = StaticArtifactStore {
        payloads: vec![planning_payload(), implementation_payload()],
        artifacts: vec![planning_artifact(), implementation_artifact()],
    };

    let artifact =
        service::get_artifact_by_id(&journal, &artifacts, Path::new("."), &project_id(), "a1")
            .expect("visible artifact");

    assert_eq!(artifact.artifact_id, "a1");
    assert!(artifact.content.contains("Planning"));
}

#[test]
fn get_artifact_by_id_hides_rolled_back_artifacts() {
    let journal = StaticJournalStore {
        events: rollback_history_events(),
    };
    let artifacts = StaticArtifactStore {
        payloads: vec![planning_payload(), implementation_payload()],
        artifacts: vec![planning_artifact(), implementation_artifact()],
    };

    let error =
        service::get_artifact_by_id(&journal, &artifacts, Path::new("."), &project_id(), "a2")
            .expect_err("rolled-back artifact must be hidden");

    assert!(matches!(error, AppError::ArtifactNotFound { .. }));
}

#[test]
fn run_query_views_round_trip_through_json() {
    let status_view = RunStatusView {
        project_id: "alpha".to_owned(),
        status: "running".to_owned(),
        stage: Some("planning".to_owned()),
        cycle: Some(1),
        completion_round: Some(1),
        summary: "running at planning".to_owned(),
    };
    let status_json_view = RunStatusJsonView::from_snapshot(
        "alpha",
        &RunSnapshot {
            active_run: None,
            interrupted_run: None,
            status: RunStatus::Paused,
            cycle_history: Vec::new(),
            completion_rounds: 0,
            rollback_point_meta: RollbackPointMeta::default(),
            amendment_queue: AmendmentQueueState {
                pending: vec![],
                processed_count: 2,
                recorded_follow_ups: Vec::new(),
            },
            status_summary: "paused".to_owned(),
            last_stage_resolution_snapshot: None,
        },
    );
    let history_view = RunHistoryView {
        project_id: "alpha".to_owned(),
        events: stage_history_events(),
        payloads: vec![planning_payload()],
        artifacts: vec![planning_artifact()],
    };
    let tail_view = RunTailView {
        project_id: "alpha".to_owned(),
        events: stage_history_events(),
        payloads: vec![planning_payload()],
        artifacts: vec![planning_artifact()],
        runtime_logs: Some(vec![RuntimeLogEntry {
            timestamp: test_timestamp(),
            level: ralph_burning::contexts::project_run_record::model::LogLevel::Info,
            source: "agent".to_owned(),
            message: "hello".to_owned(),
        }]),
    };
    let rollback_view = RunRollbackTargetView {
        rollback_id: "rb-planning".to_owned(),
        stage_id: "planning".to_owned(),
        cycle: 1,
        created_at: test_timestamp(),
        git_sha: Some("deadbeef".to_owned()),
    };

    let status_view_json = serde_json::to_string(&status_view).expect("serialize status");
    let status_json_view_json =
        serde_json::to_string(&status_json_view).expect("serialize status json");
    let history_json = serde_json::to_string(&history_view).expect("serialize history");
    let tail_json = serde_json::to_string(&tail_view).expect("serialize tail");
    let rollback_json = serde_json::to_string(&rollback_view).expect("serialize rollback");

    let decoded_status: RunStatusView =
        serde_json::from_str(&status_view_json).expect("deserialize status");
    let decoded_status_json: RunStatusJsonView =
        serde_json::from_str(&status_json_view_json).expect("deserialize status json");
    let decoded_history: RunHistoryView =
        serde_json::from_str(&history_json).expect("deserialize history");
    let decoded_tail: RunTailView = serde_json::from_str(&tail_json).expect("deserialize tail");
    let decoded_rollback: RunRollbackTargetView =
        serde_json::from_str(&rollback_json).expect("deserialize rollback");

    assert_eq!(decoded_status, status_view);
    assert_eq!(decoded_status_json, status_json_view);
    assert_eq!(decoded_history, history_view);
    assert_eq!(decoded_tail, tail_view);
    assert_eq!(decoded_rollback, rollback_view);
}
