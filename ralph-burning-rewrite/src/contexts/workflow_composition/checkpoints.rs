use std::path::Path;

use crate::shared::domain::{ProjectId, RunId, StageId};
use crate::shared::error::AppResult;

pub trait VcsCheckpointPort {
    fn create_checkpoint(
        &self,
        repo_root: &Path,
        project_id: &ProjectId,
        run_id: &RunId,
        stage_id: StageId,
        cycle: u32,
        completion_round: u32,
    ) -> AppResult<String>;

    fn find_checkpoint(
        &self,
        repo_root: &Path,
        project_id: &ProjectId,
        stage_id: StageId,
        cycle: u32,
        completion_round: u32,
    ) -> AppResult<Option<String>>;

    fn reset_to_checkpoint(&self, repo_root: &Path, sha: &str) -> AppResult<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointMessageMetadata {
    pub project_id: String,
    pub run_id: String,
    pub stage_id: String,
    pub cycle: u32,
    pub completion_round: u32,
}

pub fn checkpoint_subject(
    project_id: &ProjectId,
    stage_id: StageId,
    cycle: u32,
    completion_round: u32,
) -> String {
    checkpoint_subject_parts(
        project_id.as_str(),
        stage_id.as_str(),
        cycle,
        completion_round,
    )
}

pub fn checkpoint_body(
    project_id: &ProjectId,
    run_id: &RunId,
    stage_id: StageId,
    cycle: u32,
    completion_round: u32,
) -> String {
    checkpoint_body_parts(
        project_id.as_str(),
        run_id.as_str(),
        stage_id.as_str(),
        cycle,
        completion_round,
    )
}

pub fn checkpoint_commit_message(
    project_id: &ProjectId,
    run_id: &RunId,
    stage_id: StageId,
    cycle: u32,
    completion_round: u32,
) -> String {
    format!(
        "{}\n\n{}",
        checkpoint_subject(project_id, stage_id, cycle, completion_round),
        checkpoint_body(project_id, run_id, stage_id, cycle, completion_round)
    )
}

pub fn parse_checkpoint_commit_message(message: &str) -> Option<CheckpointMessageMetadata> {
    let normalized = message.replace("\r\n", "\n");
    let trimmed = normalized.trim_end_matches('\n');
    let lines: Vec<&str> = trimmed.split('\n').collect();
    if lines.len() != 7 || !lines[1].is_empty() {
        return None;
    }

    let project_id = lines[2].strip_prefix("RB-Project: ")?.to_owned();
    let run_id = lines[3].strip_prefix("RB-Run: ")?.to_owned();
    let stage_id = lines[4].strip_prefix("RB-Stage: ")?.to_owned();
    let cycle = lines[5].strip_prefix("RB-Cycle: ")?.parse::<u32>().ok()?;
    let completion_round = lines[6]
        .strip_prefix("RB-Completion-Round: ")?
        .parse::<u32>()
        .ok()?;

    let expected_subject = checkpoint_subject_parts(
        project_id.as_str(),
        stage_id.as_str(),
        cycle,
        completion_round,
    );
    if lines[0] != expected_subject {
        return None;
    }

    Some(CheckpointMessageMetadata {
        project_id,
        run_id,
        stage_id,
        cycle,
        completion_round,
    })
}

pub(crate) fn checkpoint_subject_parts(
    project_id: &str,
    stage_id: &str,
    cycle: u32,
    completion_round: u32,
) -> String {
    format!(
        "rb: checkpoint project={project_id} stage={stage_id} cycle={cycle} round={completion_round}"
    )
}

pub(crate) fn checkpoint_body_parts(
    project_id: &str,
    run_id: &str,
    stage_id: &str,
    cycle: u32,
    completion_round: u32,
) -> String {
    format!(
        "RB-Project: {project_id}\nRB-Run: {run_id}\nRB-Stage: {stage_id}\nRB-Cycle: {cycle}\nRB-Completion-Round: {completion_round}"
    )
}
