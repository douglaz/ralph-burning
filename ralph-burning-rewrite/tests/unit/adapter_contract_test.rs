/// Adapter contract tests for filesystem-backed stores.
/// Verifies round-trip persistence, corruption visibility, and edge cases
/// for FsProjectStore, FsJournalStore, FsArtifactStore, FsRuntimeLogStore,
/// FsRunSnapshotStore, and FsActiveProjectStore.
use std::fs;

use chrono::{TimeZone, Utc};
use tempfile::tempdir;

use ralph_burning::adapters::fs::{
    FileSystem, FsActiveProjectStore, FsArtifactStore, FsJournalStore, FsProjectStore,
    FsRunSnapshotStore, FsRuntimeLogStore,
};
use ralph_burning::contexts::project_run_record::journal;
use ralph_burning::contexts::project_run_record::model::*;
use ralph_burning::contexts::project_run_record::service::{
    ActiveProjectPort, ArtifactStorePort, JournalStorePort, ProjectStorePort, RunSnapshotPort,
    RuntimeLogStorePort,
};
use ralph_burning::shared::domain::{FlowPreset, ProjectId};
use ralph_burning::shared::error::AppError;

fn test_timestamp() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 3, 11, 19, 0, 0)
        .single()
        .expect("valid timestamp")
}

fn make_project_record(id: &str) -> ProjectRecord {
    ProjectRecord {
        id: ProjectId::new(id).unwrap(),
        name: format!("Project {id}"),
        flow: FlowPreset::Standard,
        prompt_reference: "prompt.md".to_owned(),
        prompt_hash: "abc123".to_owned(),
        created_at: test_timestamp(),
        status_summary: ProjectStatusSummary::Created,
    }
}

fn setup_workspace(base_dir: &std::path::Path) {
    let ws = base_dir.join(".ralph-burning");
    fs::create_dir_all(ws.join("projects")).expect("create projects dir");
    let config = ralph_burning::shared::domain::WorkspaceConfig::new(test_timestamp());
    let rendered = FileSystem::render_workspace_config(&config).expect("render config");
    FileSystem::write_atomic(&ws.join("workspace.toml"), &rendered).expect("write config");
}

fn create_project_on_disk(base_dir: &std::path::Path, id: &str) {
    let store = FsProjectStore;
    let record = make_project_record(id);
    let snapshot = RunSnapshot::initial();
    let sessions = SessionStore::empty();
    let event = JournalEvent {
        sequence: 1,
        timestamp: test_timestamp(),
        event_type: JournalEventType::ProjectCreated,
        details: serde_json::json!({"project_id": id, "flow": "standard"}),
    };
    let journal_line = journal::serialize_event(&event).expect("serialize event");
    store
        .create_project_atomic(
            base_dir,
            &record,
            "# Prompt\n",
            &snapshot,
            &journal_line,
            &sessions,
        )
        .expect("create project");
}

// ── FsProjectStore ──

#[test]
fn project_store_round_trip_create_and_read() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let store = FsProjectStore;
    let pid = ProjectId::new("alpha").unwrap();
    let record = store.read_project_record(tmp.path(), &pid).unwrap();

    assert_eq!(record.id.as_str(), "alpha");
    assert_eq!(record.flow, FlowPreset::Standard);
    assert_eq!(record.prompt_reference, "prompt.md");
    assert_eq!(record.prompt_hash, "abc123");
    assert_eq!(record.status_summary, ProjectStatusSummary::Created);
}

#[test]
fn project_store_exists_returns_true_for_created_project() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let store = FsProjectStore;
    let pid = ProjectId::new("alpha").unwrap();
    assert!(store.project_exists(tmp.path(), &pid).unwrap());
}

#[test]
fn project_store_exists_returns_false_for_missing_project() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());

    let store = FsProjectStore;
    let pid = ProjectId::new("nonexistent").unwrap();
    assert!(!store.project_exists(tmp.path(), &pid).unwrap());
}

#[test]
fn project_store_list_returns_sorted_ids() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "beta");
    create_project_on_disk(tmp.path(), "alpha");

    let store = FsProjectStore;
    let ids = store.list_project_ids(tmp.path()).unwrap();
    let id_strs: Vec<&str> = ids.iter().map(|p| p.as_str()).collect();
    assert_eq!(id_strs, vec!["alpha", "beta"]);
}

#[test]
fn project_store_stage_and_commit_delete_removes_project() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let store = FsProjectStore;
    let pid = ProjectId::new("alpha").unwrap();

    // Stage: project becomes invisible
    store.stage_delete(tmp.path(), &pid).unwrap();
    assert!(!store.project_exists(tmp.path(), &pid).unwrap());

    // Commit: permanently remove
    store.commit_delete(tmp.path(), &pid).unwrap();

    // Verify pending-delete dir is also gone
    let pending = tmp
        .path()
        .join(".ralph-burning/projects/.alpha.pending-delete");
    assert!(!pending.exists());
}

#[test]
fn project_store_stage_and_rollback_restores_project() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let store = FsProjectStore;
    let pid = ProjectId::new("alpha").unwrap();

    // Stage: project becomes invisible
    store.stage_delete(tmp.path(), &pid).unwrap();
    assert!(!store.project_exists(tmp.path(), &pid).unwrap());

    // Rollback: project is restored
    store.rollback_delete(tmp.path(), &pid).unwrap();
    assert!(store.project_exists(tmp.path(), &pid).unwrap());

    // Verify the record is intact
    let record = store.read_project_record(tmp.path(), &pid).unwrap();
    assert_eq!(record.id.as_str(), "alpha");
}

#[test]
fn project_store_rollback_noop_when_no_pending_delete() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());

    let store = FsProjectStore;
    let pid = ProjectId::new("alpha").unwrap();

    // Rollback with nothing pending should be a no-op
    store.rollback_delete(tmp.path(), &pid).unwrap();
}

#[test]
fn project_store_commit_noop_when_no_pending_delete() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());

    let store = FsProjectStore;
    let pid = ProjectId::new("alpha").unwrap();

    // Commit with nothing pending should be a no-op
    store.commit_delete(tmp.path(), &pid).unwrap();
}

#[test]
fn project_store_read_missing_project_toml_returns_corrupt() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    // Remove project.toml but keep directory
    let project_root = tmp.path().join(".ralph-burning/projects/alpha");
    fs::remove_file(project_root.join("project.toml")).unwrap();

    let store = FsProjectStore;
    let pid = ProjectId::new("alpha").unwrap();
    let err = store.read_project_record(tmp.path(), &pid).unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
}

#[test]
fn project_store_exists_with_missing_project_toml_returns_corrupt() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    fs::remove_file(
        tmp.path()
            .join(".ralph-burning/projects/alpha/project.toml"),
    )
    .unwrap();

    let store = FsProjectStore;
    let pid = ProjectId::new("alpha").unwrap();
    let err = store.project_exists(tmp.path(), &pid).unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
}

#[test]
fn project_store_list_with_corrupt_project_toml_returns_error() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    // Remove project.toml to simulate corruption
    fs::remove_file(
        tmp.path()
            .join(".ralph-burning/projects/alpha/project.toml"),
    )
    .unwrap();

    let store = FsProjectStore;
    let err = store.list_project_ids(tmp.path()).unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
}

#[test]
fn project_store_read_malformed_toml_returns_corrupt() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    // Write malformed TOML
    fs::write(
        tmp.path()
            .join(".ralph-burning/projects/alpha/project.toml"),
        "this is not valid toml ][}{",
    )
    .unwrap();

    let store = FsProjectStore;
    let pid = ProjectId::new("alpha").unwrap();
    let err = store.read_project_record(tmp.path(), &pid).unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
}

#[test]
fn project_store_atomic_create_does_not_leave_staging_dirs() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let projects_dir = tmp.path().join(".ralph-burning/projects");
    for entry in fs::read_dir(&projects_dir).unwrap() {
        let name = entry.unwrap().file_name();
        let name_str = name.to_string_lossy();
        assert!(
            !name_str.starts_with('.'),
            "staging directory should not remain: {name_str}"
        );
    }
}

// ── FsJournalStore ──

#[test]
fn journal_store_round_trip_read() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let store = FsJournalStore;
    let pid = ProjectId::new("alpha").unwrap();
    let events = store.read_journal(tmp.path(), &pid).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence, 1);
    assert_eq!(events[0].event_type, JournalEventType::ProjectCreated);
}

#[test]
fn journal_store_append_and_read() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let store = FsJournalStore;
    let pid = ProjectId::new("alpha").unwrap();

    let event = JournalEvent {
        sequence: 2,
        timestamp: test_timestamp(),
        event_type: JournalEventType::RunStarted,
        details: serde_json::json!({"run_id": "run-1"}),
    };
    let line = journal::serialize_event(&event).unwrap();
    store.append_event(tmp.path(), &pid, &line).unwrap();

    let events = store.read_journal(tmp.path(), &pid).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[1].sequence, 2);
    assert_eq!(events[1].event_type, JournalEventType::RunStarted);
}

#[test]
fn journal_store_missing_file_returns_corrupt() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    fs::remove_file(
        tmp.path()
            .join(".ralph-burning/projects/alpha/journal.ndjson"),
    )
    .unwrap();

    let store = FsJournalStore;
    let pid = ProjectId::new("alpha").unwrap();
    let err = store.read_journal(tmp.path(), &pid).unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
}

#[test]
fn journal_store_empty_file_returns_corrupt_error() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    // Truncate journal to empty
    fs::write(
        tmp.path()
            .join(".ralph-burning/projects/alpha/journal.ndjson"),
        "",
    )
    .unwrap();

    let store = FsJournalStore;
    let pid = ProjectId::new("alpha").unwrap();
    let err = store.read_journal(tmp.path(), &pid).unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
    match err {
        AppError::CorruptRecord { details, .. } => {
            assert!(details.contains("empty"));
        }
        _ => panic!("expected CorruptRecord"),
    }
}

#[test]
fn journal_store_corrupt_json_returns_error() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    fs::write(
        tmp.path()
            .join(".ralph-burning/projects/alpha/journal.ndjson"),
        "{not valid json}\n",
    )
    .unwrap();

    let store = FsJournalStore;
    let pid = ProjectId::new("alpha").unwrap();
    let err = store.read_journal(tmp.path(), &pid).unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
}

// ── FsRunSnapshotStore ──

#[test]
fn run_snapshot_store_round_trip() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let store = FsRunSnapshotStore;
    let pid = ProjectId::new("alpha").unwrap();
    let snapshot = store.read_run_snapshot(tmp.path(), &pid).unwrap();

    assert_eq!(snapshot.status, RunStatus::NotStarted);
    assert!(snapshot.active_run.is_none());
    assert!(snapshot.cycle_history.is_empty());
    assert_eq!(snapshot.completion_rounds, 0);
}

#[test]
fn run_snapshot_store_missing_file_returns_corrupt() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    fs::remove_file(tmp.path().join(".ralph-burning/projects/alpha/run.json")).unwrap();

    let store = FsRunSnapshotStore;
    let pid = ProjectId::new("alpha").unwrap();
    let err = store.read_run_snapshot(tmp.path(), &pid).unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
}

#[test]
fn run_snapshot_store_corrupt_json_returns_error() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    fs::write(
        tmp.path().join(".ralph-burning/projects/alpha/run.json"),
        "not json at all",
    )
    .unwrap();

    let store = FsRunSnapshotStore;
    let pid = ProjectId::new("alpha").unwrap();
    let err = store.read_run_snapshot(tmp.path(), &pid).unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
}

#[test]
fn run_snapshot_store_semantically_inconsistent_returns_corrupt() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    // Write a syntactically valid but semantically inconsistent run.json
    let bad_snapshot = serde_json::json!({
        "active_run": null,
        "status": "running",
        "cycle_history": [],
        "completion_rounds": 0,
        "rollback_point_meta": {"last_rollback_id": null, "rollback_count": 0},
        "amendment_queue": {"pending": [], "processed_count": 0},
        "status_summary": "running"
    });
    fs::write(
        tmp.path().join(".ralph-burning/projects/alpha/run.json"),
        serde_json::to_string_pretty(&bad_snapshot).unwrap(),
    )
    .unwrap();

    let store = FsRunSnapshotStore;
    let pid = ProjectId::new("alpha").unwrap();
    let err = store.read_run_snapshot(tmp.path(), &pid).unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
}

// ── FsArtifactStore ──

#[test]
fn artifact_store_empty_dirs_return_empty_vecs() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let store = FsArtifactStore;
    let pid = ProjectId::new("alpha").unwrap();
    assert!(store.list_payloads(tmp.path(), &pid).unwrap().is_empty());
    assert!(store.list_artifacts(tmp.path(), &pid).unwrap().is_empty());
}

#[test]
fn artifact_store_round_trip_payload() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let payload = PayloadRecord {
        payload_id: "p1".to_owned(),
        stage_id: ralph_burning::shared::domain::StageId::Planning,
        cycle: 1,
        attempt: 1,
        created_at: test_timestamp(),
        payload: serde_json::json!({"plan": "build it"}),
    };
    let payload_json = serde_json::to_string_pretty(&payload).unwrap();
    fs::write(
        tmp.path()
            .join(".ralph-burning/projects/alpha/history/payloads/p1.json"),
        payload_json,
    )
    .unwrap();

    let store = FsArtifactStore;
    let pid = ProjectId::new("alpha").unwrap();
    let payloads = store.list_payloads(tmp.path(), &pid).unwrap();
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0].payload_id, "p1");
}

#[test]
fn artifact_store_round_trip_artifact() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let artifact = ArtifactRecord {
        artifact_id: "a1".to_owned(),
        payload_id: "p1".to_owned(),
        stage_id: ralph_burning::shared::domain::StageId::Planning,
        created_at: test_timestamp(),
        content: "# Planning\nBuild it.".to_owned(),
    };
    let artifact_json = serde_json::to_string_pretty(&artifact).unwrap();
    fs::write(
        tmp.path()
            .join(".ralph-burning/projects/alpha/history/artifacts/a1.json"),
        artifact_json,
    )
    .unwrap();

    let store = FsArtifactStore;
    let pid = ProjectId::new("alpha").unwrap();
    let artifacts = store.list_artifacts(tmp.path(), &pid).unwrap();
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0].artifact_id, "a1");
    assert_eq!(artifacts[0].payload_id, "p1");
}

#[test]
fn artifact_store_corrupt_payload_returns_error() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    fs::write(
        tmp.path()
            .join(".ralph-burning/projects/alpha/history/payloads/bad.json"),
        "not valid json",
    )
    .unwrap();

    let store = FsArtifactStore;
    let pid = ProjectId::new("alpha").unwrap();
    let err = store.list_payloads(tmp.path(), &pid).unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
}

#[test]
fn artifact_store_corrupt_artifact_returns_error() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    fs::write(
        tmp.path()
            .join(".ralph-burning/projects/alpha/history/artifacts/bad.json"),
        "not valid json",
    )
    .unwrap();

    let store = FsArtifactStore;
    let pid = ProjectId::new("alpha").unwrap();
    let err = store.list_artifacts(tmp.path(), &pid).unwrap_err();
    assert!(matches!(err, AppError::CorruptRecord { .. }));
}

// ── FsRuntimeLogStore ──

#[test]
fn runtime_log_store_empty_returns_empty() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let store = FsRuntimeLogStore;
    let pid = ProjectId::new("alpha").unwrap();
    assert!(store
        .read_runtime_logs(tmp.path(), &pid)
        .unwrap()
        .is_empty());
}

#[test]
fn runtime_log_store_reads_ndjson_entries() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let entry = RuntimeLogEntry {
        timestamp: test_timestamp(),
        level: LogLevel::Info,
        source: "agent".to_owned(),
        message: "started execution".to_owned(),
    };
    let line = serde_json::to_string(&entry).unwrap();
    fs::write(
        tmp.path()
            .join(".ralph-burning/projects/alpha/runtime/logs/session-1.ndjson"),
        format!("{line}\n"),
    )
    .unwrap();

    let store = FsRuntimeLogStore;
    let pid = ProjectId::new("alpha").unwrap();
    let logs = store.read_runtime_logs(tmp.path(), &pid).unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].message, "started execution");
}

#[test]
fn runtime_log_store_skips_malformed_lines() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let entry = RuntimeLogEntry {
        timestamp: test_timestamp(),
        level: LogLevel::Info,
        source: "agent".to_owned(),
        message: "good line".to_owned(),
    };
    let good_line = serde_json::to_string(&entry).unwrap();
    let content = format!("not json\n{good_line}\nalso bad\n");
    fs::write(
        tmp.path()
            .join(".ralph-burning/projects/alpha/runtime/logs/session-1.ndjson"),
        content,
    )
    .unwrap();

    let store = FsRuntimeLogStore;
    let pid = ProjectId::new("alpha").unwrap();
    let logs = store.read_runtime_logs(tmp.path(), &pid).unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].message, "good line");
}

#[test]
fn runtime_log_store_reads_only_newest_file() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let old_entry = RuntimeLogEntry {
        timestamp: test_timestamp(),
        level: LogLevel::Info,
        source: "agent".to_owned(),
        message: "old log".to_owned(),
    };
    let new_entry = RuntimeLogEntry {
        timestamp: test_timestamp(),
        level: LogLevel::Info,
        source: "agent".to_owned(),
        message: "new log".to_owned(),
    };
    let old_line = serde_json::to_string(&old_entry).unwrap();
    let new_line = serde_json::to_string(&new_entry).unwrap();

    let logs_dir = tmp
        .path()
        .join(".ralph-burning/projects/alpha/runtime/logs");
    fs::write(logs_dir.join("001.ndjson"), format!("{old_line}\n")).unwrap();
    fs::write(logs_dir.join("002.ndjson"), format!("{new_line}\n")).unwrap();

    let store = FsRuntimeLogStore;
    let pid = ProjectId::new("alpha").unwrap();
    let logs = store.read_runtime_logs(tmp.path(), &pid).unwrap();

    // Only the newest file (002.ndjson) should be read
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].message, "new log");
}

// ── FsActiveProjectStore ──

#[test]
fn active_project_store_read_none_when_no_file() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());

    let store = FsActiveProjectStore;
    assert!(store.read_active_project_id(tmp.path()).unwrap().is_none());
}

#[test]
fn active_project_store_round_trip() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());

    FileSystem::write_active_project(&tmp.path().join(".ralph-burning"), "alpha").unwrap();

    let store = FsActiveProjectStore;
    let id = store.read_active_project_id(tmp.path()).unwrap();
    assert_eq!(id.as_deref(), Some("alpha"));
}

#[test]
fn active_project_store_clear() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());

    FileSystem::write_active_project(&tmp.path().join(".ralph-burning"), "alpha").unwrap();

    let store = FsActiveProjectStore;
    store.clear_active_project(tmp.path()).unwrap();
    assert!(store.read_active_project_id(tmp.path()).unwrap().is_none());
}

#[test]
fn active_project_store_clear_when_already_absent() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());

    let store = FsActiveProjectStore;
    // Should not error when file doesn't exist
    store.clear_active_project(tmp.path()).unwrap();
}

// ── FsPayloadArtifactWriteStore ──

use ralph_burning::adapters::fs::FsPayloadArtifactWriteStore;
use ralph_burning::contexts::project_run_record::service::PayloadArtifactWritePort;

#[test]
fn payload_artifact_write_pair_round_trip() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let pid = ProjectId::new("alpha").unwrap();
    let store = FsPayloadArtifactWriteStore;
    let payload = PayloadRecord {
        payload_id: "p1".to_owned(),
        stage_id: ralph_burning::shared::domain::StageId::Planning,
        cycle: 1,
        attempt: 1,
        created_at: test_timestamp(),
        payload: serde_json::json!({"plan": "test"}),
    };
    let artifact = ArtifactRecord {
        artifact_id: "a1".to_owned(),
        payload_id: "p1".to_owned(),
        stage_id: ralph_burning::shared::domain::StageId::Planning,
        created_at: test_timestamp(),
        content: "# Planning\nTest.".to_owned(),
    };

    store
        .write_payload_artifact_pair(tmp.path(), &pid, &payload, &artifact)
        .unwrap();

    // Verify both files exist
    let payload_path = tmp
        .path()
        .join(".ralph-burning/projects/alpha/history/payloads/p1.json");
    let artifact_path = tmp
        .path()
        .join(".ralph-burning/projects/alpha/history/artifacts/a1.json");
    assert!(payload_path.is_file());
    assert!(artifact_path.is_file());
}

#[test]
fn payload_artifact_remove_pair_removes_both_files() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let pid = ProjectId::new("alpha").unwrap();
    let store = FsPayloadArtifactWriteStore;
    let payload = PayloadRecord {
        payload_id: "p1".to_owned(),
        stage_id: ralph_burning::shared::domain::StageId::Planning,
        cycle: 1,
        attempt: 1,
        created_at: test_timestamp(),
        payload: serde_json::json!({"plan": "test"}),
    };
    let artifact = ArtifactRecord {
        artifact_id: "a1".to_owned(),
        payload_id: "p1".to_owned(),
        stage_id: ralph_burning::shared::domain::StageId::Planning,
        created_at: test_timestamp(),
        content: "# Planning\nTest.".to_owned(),
    };

    store
        .write_payload_artifact_pair(tmp.path(), &pid, &payload, &artifact)
        .unwrap();
    store
        .remove_payload_artifact_pair(tmp.path(), &pid, "p1", "a1")
        .unwrap();

    let payload_path = tmp
        .path()
        .join(".ralph-burning/projects/alpha/history/payloads/p1.json");
    let artifact_path = tmp
        .path()
        .join(".ralph-burning/projects/alpha/history/artifacts/a1.json");
    assert!(!payload_path.exists());
    assert!(!artifact_path.exists());
}

#[test]
fn payload_artifact_remove_pair_not_found_is_ok() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let pid = ProjectId::new("alpha").unwrap();
    let store = FsPayloadArtifactWriteStore;

    // Removing non-existent files should succeed (NotFound is not an error)
    let result =
        store.remove_payload_artifact_pair(tmp.path(), &pid, "nonexistent", "nonexistent-artifact");
    assert!(result.is_ok(), "removing non-existent pair should succeed");
}

#[test]
fn payload_artifact_remove_pair_propagates_removal_error() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let pid = ProjectId::new("alpha").unwrap();
    let store = FsPayloadArtifactWriteStore;
    let payload = PayloadRecord {
        payload_id: "p1".to_owned(),
        stage_id: ralph_burning::shared::domain::StageId::Planning,
        cycle: 1,
        attempt: 1,
        created_at: test_timestamp(),
        payload: serde_json::json!({"plan": "test"}),
    };
    let artifact = ArtifactRecord {
        artifact_id: "a1".to_owned(),
        payload_id: "p1".to_owned(),
        stage_id: ralph_burning::shared::domain::StageId::Planning,
        created_at: test_timestamp(),
        content: "# Planning\nTest.".to_owned(),
    };

    store
        .write_payload_artifact_pair(tmp.path(), &pid, &payload, &artifact)
        .unwrap();

    // Replace the payload file with a non-empty directory so fs::remove_file
    // fails with "is a directory" — works regardless of user (including root).
    let payload_path = tmp
        .path()
        .join(".ralph-burning/projects/alpha/history/payloads/p1.json");
    fs::remove_file(&payload_path).unwrap();
    fs::create_dir(&payload_path).unwrap();
    fs::write(payload_path.join("block"), "prevent removal").unwrap();

    let result = store.remove_payload_artifact_pair(tmp.path(), &pid, "p1", "a1");

    assert!(
        result.is_err(),
        "removing pair should propagate error when payload is a directory"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("payload"),
        "error should reference the failed payload removal: {err_msg}"
    );
}

#[test]
fn payload_artifact_write_pair_cleans_up_on_artifact_failure() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());
    create_project_on_disk(tmp.path(), "alpha");

    let pid = ProjectId::new("alpha").unwrap();
    let store = FsPayloadArtifactWriteStore;
    let payload = PayloadRecord {
        payload_id: "p1".to_owned(),
        stage_id: ralph_burning::shared::domain::StageId::Planning,
        cycle: 1,
        attempt: 1,
        created_at: test_timestamp(),
        payload: serde_json::json!({"plan": "test"}),
    };
    let artifact = ArtifactRecord {
        artifact_id: "a1".to_owned(),
        payload_id: "p1".to_owned(),
        stage_id: ralph_burning::shared::domain::StageId::Planning,
        created_at: test_timestamp(),
        content: "# Planning\nTest.".to_owned(),
    };

    // Make the artifacts directory a file so artifact write fails
    let artifacts_dir = tmp
        .path()
        .join(".ralph-burning/projects/alpha/history/artifacts");
    fs::remove_dir(&artifacts_dir).unwrap();
    fs::write(&artifacts_dir, "not a directory").unwrap();

    let result = store.write_payload_artifact_pair(tmp.path(), &pid, &payload, &artifact);
    assert!(
        result.is_err(),
        "write should fail when artifact dir is a file"
    );

    // Payload should have been cleaned up — no leaked file
    let payload_path = tmp
        .path()
        .join(".ralph-burning/projects/alpha/history/payloads/p1.json");
    assert!(
        !payload_path.exists(),
        "payload should be cleaned up when artifact write fails"
    );
}

// ── Project prompt.md round trip ──

#[test]
fn project_create_copies_prompt_and_records_canonical_reference() {
    let tmp = tempdir().unwrap();
    setup_workspace(tmp.path());

    let store = FsProjectStore;
    let record = make_project_record("alpha");
    let snapshot = RunSnapshot::initial();
    let sessions = SessionStore::empty();
    let event = JournalEvent {
        sequence: 1,
        timestamp: test_timestamp(),
        event_type: JournalEventType::ProjectCreated,
        details: serde_json::json!({"project_id": "alpha"}),
    };
    let journal_line = journal::serialize_event(&event).unwrap();
    store
        .create_project_atomic(
            tmp.path(),
            &record,
            "# External Prompt\nContent here.",
            &snapshot,
            &journal_line,
            &sessions,
        )
        .unwrap();

    // Verify the copied prompt.md contains the original content
    let copied =
        fs::read_to_string(tmp.path().join(".ralph-burning/projects/alpha/prompt.md")).unwrap();
    assert_eq!(copied, "# External Prompt\nContent here.");

    // Verify project.toml records the canonical reference, not a source path
    let pid = ProjectId::new("alpha").unwrap();
    let loaded = store.read_project_record(tmp.path(), &pid).unwrap();
    assert_eq!(loaded.prompt_reference, "prompt.md");
}
