use crate::shared::error::AppResult;

use super::model::{
    JournalEvent, PayloadRecord, ArtifactRecord, RuntimeLogEntry, RunSnapshot,
};

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
                status: format!("{:?}", snapshot.status).to_lowercase(),
                stage: Some(active.stage_cursor.stage.as_str().to_owned()),
                cycle: Some(active.stage_cursor.cycle),
                completion_round: Some(active.stage_cursor.completion_round),
                summary: snapshot.status_summary.clone(),
            },
            None => Self {
                project_id: project_id.to_owned(),
                status: "not started".to_owned(),
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

/// Validate that durable history records are consistent: every artifact has a matching payload.
pub fn validate_history_consistency(
    payloads: &[PayloadRecord],
    artifacts: &[ArtifactRecord],
) -> AppResult<()> {
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
    Ok(())
}
