use std::path::Path;

use chrono::{DateTime, Utc};

use crate::adapters::fs::FileSystem;
use crate::contexts::milestone_record::model::{MilestoneProgress, MilestoneStatus};
use crate::contexts::milestone_record::queries::BeadLineageView;
use crate::contexts::milestone_record::service::{MilestonePlanPort, MilestoneStorePort};
use crate::contexts::requirements_drafting::service::SeedHandoff;
use crate::contexts::workflow_composition;
use crate::contexts::workspace_governance::config::{
    EffectiveConfig, DEFAULT_MAX_COMPLETION_ROUNDS,
};
use crate::shared::domain::{FlowPreset, ProjectId, StageCursor, StageId};
use crate::shared::error::{AppError, AppResult};
use crate::shared::text::truncate_with_ascii_ellipsis;

use super::fence_util::{closes_fence, opening_fence_delimiter};
use super::journal;
use super::model::{
    ActiveRun, AmendmentSource, ArtifactRecord, JournalEvent, JournalEventType, PayloadRecord,
    ProjectDetail, ProjectListEntry, ProjectRecord, ProjectStatusSummary, QueuedAmendment,
    RollbackPoint, RunSnapshot, RunStatus, RuntimeLogEntry, SessionStore, TaskSource,
};
use super::queries::{
    self, RunHistoryView, RunRollbackTargetView, RunStatusJsonView, RunStatusView, RunTailView,
};
use super::task_prompt_contract;

/// Port for reading and writing project records.
pub trait ProjectStorePort {
    fn project_exists(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<bool>;
    fn read_project_record(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<ProjectRecord>;
    fn list_project_ids(&self, base_dir: &Path) -> AppResult<Vec<ProjectId>>;

    /// Stage a project for deletion: makes it invisible to list/show but
    /// keeps data on disk for potential rollback.
    fn stage_delete(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()>;

    /// Finalize a staged delete: permanently removes the project data.
    fn commit_delete(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()>;

    /// Roll back a staged delete: restores the project to its canonical path.
    fn rollback_delete(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()>;

    /// Atomically stage and commit a new project. If any step fails,
    /// the project must not be visible to list/show.
    fn create_project_atomic(
        &self,
        base_dir: &Path,
        record: &ProjectRecord,
        prompt_contents: &str,
        run_snapshot: &RunSnapshot,
        initial_journal_line: &str,
        sessions: &SessionStore,
    ) -> AppResult<()>;
}

/// Port for reading and appending journal events.
pub trait JournalStorePort {
    fn read_journal(&self, base_dir: &Path, project_id: &ProjectId)
        -> AppResult<Vec<JournalEvent>>;
    fn append_event(&self, base_dir: &Path, project_id: &ProjectId, line: &str) -> AppResult<()>;
}

/// Port for reading payload and artifact durable history.
pub trait ArtifactStorePort {
    fn list_payloads(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<PayloadRecord>>;
    fn list_artifacts(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<ArtifactRecord>>;

    fn read_payload_by_id(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        payload_id: &str,
    ) -> AppResult<PayloadRecord> {
        self.list_payloads(base_dir, project_id)?
            .into_iter()
            .find(|payload| payload.payload_id == payload_id)
            .ok_or_else(|| AppError::PayloadNotFound {
                payload_id: payload_id.to_owned(),
            })
    }

    fn read_artifact_by_id(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        artifact_id: &str,
    ) -> AppResult<ArtifactRecord> {
        self.list_artifacts(base_dir, project_id)?
            .into_iter()
            .find(|artifact| artifact.artifact_id == artifact_id)
            .ok_or_else(|| AppError::ArtifactNotFound {
                artifact_id: artifact_id.to_owned(),
            })
    }
}

/// Port for reading runtime logs (separate from durable history).
pub trait RuntimeLogStorePort {
    fn read_runtime_logs(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<RuntimeLogEntry>>;
}

/// Port for reading/writing the run snapshot.
pub trait RunSnapshotPort {
    fn read_run_snapshot(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<RunSnapshot>;
}

/// Port for writing the run snapshot atomically.
pub trait RunSnapshotWritePort {
    fn write_run_snapshot(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        snapshot: &RunSnapshot,
    ) -> AppResult<()>;
}

/// Port for durable rollback point persistence.
pub trait RollbackPointStorePort {
    fn write_rollback_point(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        rollback_point: &RollbackPoint,
    ) -> AppResult<()>;

    fn list_rollback_points(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<RollbackPoint>>;

    fn read_rollback_point_by_stage(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        stage_id: StageId,
    ) -> AppResult<Option<RollbackPoint>>;
}

/// Port for repository reset operations during hard rollback.
pub trait RepositoryResetPort {
    fn reset_to_sha(&self, repo_root: &Path, sha: &str) -> AppResult<()>;
}

/// Port for writing payload and artifact records atomically as a pair.
pub trait PayloadArtifactWritePort {
    fn write_payload_artifact_pair(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        payload: &PayloadRecord,
        artifact: &ArtifactRecord,
    ) -> AppResult<()>;

    /// Remove a previously written payload/artifact pair.
    /// Used to roll back a stage commit when journal or snapshot persistence
    /// fails after the pair was already written.
    fn remove_payload_artifact_pair(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        payload_id: &str,
        artifact_id: &str,
    ) -> AppResult<()>;
}

/// Port for appending runtime log entries (best-effort, not durable history).
pub trait RuntimeLogWritePort {
    fn append_runtime_log(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        entry: &RuntimeLogEntry,
    ) -> AppResult<()>;
}

/// Port for durable amendment queue persistence under `projects/<id>/amendments/`.
pub trait AmendmentQueuePort {
    /// Write a single amendment file atomically.
    fn write_amendment(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        amendment: &super::model::QueuedAmendment,
    ) -> AppResult<()>;

    /// List all pending amendment files from disk.
    fn list_pending_amendments(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<super::model::QueuedAmendment>>;

    /// Remove a single amendment file by ID.
    fn remove_amendment(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        amendment_id: &str,
    ) -> AppResult<()>;

    /// Remove all pending amendment files. Returns count removed.
    fn drain_amendments(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<u32>;

    /// Check if any amendment files exist on disk.
    fn has_pending_amendments(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<bool>;
}

/// Port for reading/writing/clearing the active project pointer.
pub trait ActiveProjectPort {
    fn read_active_project_id(&self, base_dir: &Path) -> AppResult<Option<String>>;
    fn clear_active_project(&self, base_dir: &Path) -> AppResult<()>;
    fn write_active_project(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()>;
}

/// Input for the `project create` use case.
pub struct CreateProjectInput {
    pub id: ProjectId,
    pub name: String,
    pub flow: FlowPreset,
    pub prompt_path: String,
    pub prompt_contents: String,
    pub prompt_hash: String,
    pub created_at: DateTime<Utc>,
    /// Milestone/bead linkage metadata. None for standalone projects.
    pub task_source: Option<TaskSource>,
}

/// Bead-backed execution context used to bootstrap a project from milestone state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeadDependencyPromptContext {
    pub id: String,
    pub title: Option<String>,
    pub relationship: String,
    pub status: Option<String>,
    pub outcome: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedElsewherePromptContext {
    pub id: String,
    pub title: String,
    pub relationship: String,
    pub status: Option<String>,
    pub summary: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeadProjectContext {
    pub milestone_id: String,
    pub milestone_name: String,
    pub milestone_description: String,
    pub milestone_summary: Option<String>,
    pub milestone_status: MilestoneStatus,
    pub milestone_progress: MilestoneProgress,
    pub milestone_goals: Vec<String>,
    pub milestone_non_goals: Vec<String>,
    pub milestone_constraints: Vec<String>,
    pub agents_guidance: Option<String>,
    pub bead_id: String,
    pub bead_title: String,
    pub bead_description: Option<String>,
    pub bead_acceptance_criteria: Vec<String>,
    pub upstream_dependencies: Vec<BeadDependencyPromptContext>,
    pub downstream_dependents: Vec<BeadDependencyPromptContext>,
    pub planned_elsewhere: Vec<PlannedElsewherePromptContext>,
    pub review_policy: Vec<String>,
    pub parent_epic_id: Option<String>,
    pub flow: FlowPreset,
    pub plan_hash: Option<String>,
    pub plan_version: Option<u32>,
}

/// Input for creating a project directly from milestone + bead context.
pub struct CreateProjectFromBeadContextInput {
    pub project_id: Option<ProjectId>,
    pub prompt_override: Option<String>,
    pub created_at: DateTime<Utc>,
    pub context: BeadProjectContext,
}

/// Create a new project with all canonical files.
pub fn create_project(
    store: &dyn ProjectStorePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    input: CreateProjectInput,
) -> AppResult<ProjectRecord> {
    let mut initial_details = serde_json::Map::from_iter([
        (
            "project_id".to_owned(),
            serde_json::Value::String(input.id.to_string()),
        ),
        (
            "flow".to_owned(),
            serde_json::Value::String(input.flow.as_str().to_owned()),
        ),
    ]);
    inject_task_source_initial_details(&mut initial_details, input.task_source.as_ref());
    create_project_with_initial_details(
        store,
        journal_store,
        base_dir,
        input,
        serde_json::Value::Object(initial_details),
    )
}

pub fn create_project_from_seed(
    store: &dyn ProjectStorePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    handoff: SeedHandoff,
    flow_override: Option<FlowPreset>,
    created_at: DateTime<Utc>,
) -> AppResult<ProjectRecord> {
    let SeedHandoff {
        requirements_run_id,
        project_id,
        project_name,
        flow: seed_flow,
        prompt_body,
        prompt_path: _,
        recommended_flow,
    } = handoff;

    let selected_flow = flow_override.unwrap_or(seed_flow);
    let prompt_hash = FileSystem::prompt_hash(&prompt_body);
    let project_id = ProjectId::new(project_id)?;

    let mut initial_details = serde_json::Map::from_iter([
        (
            "project_id".to_owned(),
            serde_json::Value::String(project_id.to_string()),
        ),
        (
            "flow".to_owned(),
            serde_json::Value::String(selected_flow.as_str().to_owned()),
        ),
        (
            "source".to_owned(),
            serde_json::Value::String("requirements".to_owned()),
        ),
        (
            "requirements_run_id".to_owned(),
            serde_json::Value::String(requirements_run_id),
        ),
        (
            "seed_flow".to_owned(),
            serde_json::Value::String(seed_flow.as_str().to_owned()),
        ),
    ]);
    if let Some(flow) = recommended_flow {
        initial_details.insert(
            "recommended_flow".to_owned(),
            serde_json::Value::String(flow.as_str().to_owned()),
        );
    }

    let input = CreateProjectInput {
        id: project_id,
        name: project_name,
        flow: selected_flow,
        prompt_path: "prompt.md".to_owned(),
        prompt_contents: prompt_body,
        prompt_hash,
        created_at,
        task_source: None,
    };

    create_project_with_initial_details(
        store,
        journal_store,
        base_dir,
        input,
        serde_json::Value::Object(initial_details),
    )
}

pub fn create_project_from_bead_context(
    store: &dyn ProjectStorePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    input: CreateProjectFromBeadContextInput,
) -> AppResult<ProjectRecord> {
    let CreateProjectFromBeadContextInput {
        project_id,
        prompt_override,
        created_at,
        context,
    } = input;

    let project_id = project_id.unwrap_or(default_project_id_for_bead(
        &context.milestone_id,
        &context.bead_id,
    )?);
    let (prompt_contents, prompt_path) = match prompt_override {
        Some(prompt) => (prompt, "<prompt override>".to_owned()),
        None => (
            render_bead_task_prompt(&context),
            "generated bead task prompt".to_owned(),
        ),
    };
    if task_prompt_contract::prompt_declares_contract(&prompt_contents) {
        task_prompt_contract::validate_canonical_prompt_shape(&prompt_contents).map_err(
            |errors| AppError::InvalidPrompt {
                path: prompt_path.clone(),
                reason: format!(
                    "canonical bead task contract violated: {}",
                    errors.join("; ")
                ),
            },
        )?;
    }
    let prompt_hash = FileSystem::prompt_hash(&prompt_contents);

    let mut initial_details = serde_json::Map::from_iter([
        (
            "project_id".to_owned(),
            serde_json::Value::String(project_id.to_string()),
        ),
        (
            "flow".to_owned(),
            serde_json::Value::String(context.flow.as_str().to_owned()),
        ),
        (
            "source".to_owned(),
            serde_json::Value::String("milestone_bead".to_owned()),
        ),
        (
            "bead_title".to_owned(),
            serde_json::Value::String(context.bead_title.clone()),
        ),
    ]);
    inject_task_source_initial_details(
        &mut initial_details,
        Some(&TaskSource {
            milestone_id: context.milestone_id.clone(),
            bead_id: context.bead_id.clone(),
            parent_epic_id: context.parent_epic_id.clone(),
            origin: super::model::TaskOrigin::Milestone,
            plan_hash: context.plan_hash.clone(),
            plan_version: context.plan_version,
        }),
    );

    let project_name = format!("{}: {}", context.milestone_name, context.bead_title);
    let task_source = Some(TaskSource {
        milestone_id: context.milestone_id,
        bead_id: context.bead_id,
        parent_epic_id: context.parent_epic_id,
        origin: super::model::TaskOrigin::Milestone,
        plan_hash: context.plan_hash,
        plan_version: context.plan_version,
    });

    let input = CreateProjectInput {
        id: project_id,
        name: project_name,
        flow: context.flow,
        prompt_path: "prompt.md".to_owned(),
        prompt_contents,
        prompt_hash,
        created_at,
        task_source,
    };

    create_project_with_initial_details(
        store,
        journal_store,
        base_dir,
        input,
        serde_json::Value::Object(initial_details),
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BeadDescriptionSectionKind {
    AcceptanceCriteria,
    NonGoals,
}

fn markdown_heading_title(line: &str) -> Option<&str> {
    let trimmed = line.trim();
    let hash_count = trimmed.chars().take_while(|ch| *ch == '#').count();
    if !(1..=6).contains(&hash_count) {
        return None;
    }
    let rest = &trimmed[hash_count..];
    if !rest.is_empty()
        && !rest
            .chars()
            .next()
            .map(char::is_whitespace)
            .unwrap_or(false)
    {
        return None;
    }
    Some(rest.trim().trim_end_matches('#').trim())
}

fn normalized_section_label(label: &str) -> String {
    label
        .trim()
        .trim_end_matches(':')
        .chars()
        .filter(|ch| !matches!(ch, '-' | '_' | ' '))
        .flat_map(char::to_lowercase)
        .collect()
}

fn bead_description_section_kind_from_label(label: &str) -> Option<BeadDescriptionSectionKind> {
    match normalized_section_label(label).as_str() {
        "acceptancecriteria" => Some(BeadDescriptionSectionKind::AcceptanceCriteria),
        "nongoals" => Some(BeadDescriptionSectionKind::NonGoals),
        _ => None,
    }
}

fn section_kind_for_bead_description_line(line: &str) -> Option<BeadDescriptionSectionKind> {
    let trimmed = line.trim();
    if let Some(title) = markdown_heading_title(trimmed) {
        return bead_description_section_kind_from_label(title);
    }

    trimmed
        .strip_suffix(':')
        .and_then(bead_description_section_kind_from_label)
}

fn strip_markdown_list_marker(line: &str) -> Option<(usize, &str)> {
    let leading_indent = line.len() - line.trim_start().len();
    let trimmed = &line[leading_indent..];
    if let Some(item) = trimmed
        .strip_prefix("- ")
        .or_else(|| trimmed.strip_prefix("* "))
    {
        return Some((leading_indent, item));
    }

    let bytes = trimmed.as_bytes();
    let mut marker_len = 0usize;
    while marker_len < bytes.len() && bytes[marker_len].is_ascii_digit() {
        marker_len += 1;
    }

    if marker_len == 0 || marker_len + 1 >= bytes.len() {
        return None;
    }

    if matches!(bytes[marker_len], b'.' | b')') && bytes[marker_len + 1] == b' ' {
        Some((leading_indent, &trimmed[marker_len + 2..]))
    } else {
        None
    }
}

fn append_section_item_line(current: &mut String, line: &str) {
    let trimmed_end = line.trim_end();
    if current.is_empty() {
        current.push_str(trimmed_end.trim_start());
        return;
    }

    if current.contains('\n') || trimmed_end != trimmed_end.trim_start() {
        current.push('\n');
        current.push_str(trimmed_end);
    } else {
        current.push(' ');
        current.push_str(trimmed_end.trim_start());
    }
}

fn is_indented_continuation_line(line: &str) -> bool {
    !line.trim().is_empty() && line.trim_start() != line
}

fn collect_section_items(lines: &[String]) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut active_fence = None;
    let mut index = 0usize;

    while index < lines.len() {
        let line = &lines[index];
        if let Some(opening) = active_fence {
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(line);
            if closes_fence(line, opening) {
                active_fence = None;
            }
            index += 1;
            continue;
        }

        if let Some(opening) = opening_fence_delimiter(line) {
            if !current.is_empty() {
                current.push('\n');
            }
            current.push_str(line);
            active_fence = Some(opening);
            index += 1;
            continue;
        }

        let trimmed_end = line.trim_end();
        if trimmed_end.trim().is_empty() {
            let preserves_current_item = !current.is_empty()
                && lines
                    .get(index + 1)
                    .map(|next| is_indented_continuation_line(next))
                    .unwrap_or(false);
            if preserves_current_item {
                current.push('\n');
                index += 1;
                continue;
            }
            if !current.is_empty() {
                items.push(current.trim().to_owned());
                current.clear();
            }
            index += 1;
            continue;
        }

        if let Some((leading_indent, item)) = strip_markdown_list_marker(trimmed_end) {
            if leading_indent > 0 {
                append_section_item_line(&mut current, trimmed_end);
                index += 1;
                continue;
            }
            if !current.is_empty() {
                items.push(current.trim().to_owned());
                current.clear();
            }
            current.push_str(item.trim_end());
            index += 1;
            continue;
        }

        append_section_item_line(&mut current, trimmed_end);
        index += 1;
    }

    if !current.is_empty() {
        items.push(current.trim().to_owned());
    }

    items
}

fn merge_unique_lines(primary: &[String], secondary: &[String]) -> Vec<String> {
    let mut merged = Vec::new();
    for item in primary.iter().chain(secondary.iter()) {
        if !merged.iter().any(|existing: &String| existing == item) {
            merged.push(item.clone());
        }
    }
    merged
}

fn is_standalone_plain_subsection_label(line: &str) -> bool {
    let trimmed_end = line.trim_end();
    if trimmed_end.is_empty() || trimmed_end != line.trim_start() || !trimmed_end.ends_with(':') {
        return false;
    }

    if strip_markdown_list_marker(trimmed_end).is_some() {
        return false;
    }

    let label = trimmed_end[..trimmed_end.len() - 1].trim_end();
    if label.is_empty() {
        return false;
    }

    label.split_whitespace().count() >= 2
}

fn is_bead_description_subsection_boundary(line: &str) -> bool {
    markdown_heading_title(line).is_some() || is_standalone_plain_subsection_label(line)
}

fn split_bead_description_scope(description: &str) -> (String, Vec<String>, Vec<String>) {
    let mut must_do_lines = Vec::new();
    let mut non_goal_lines = Vec::new();
    let mut acceptance_criteria_lines = Vec::new();
    let mut active_section = None;
    let mut active_fence = None;
    let mut previous_line_blank = true;

    let mut push_line = |section: Option<BeadDescriptionSectionKind>, line: &str| match section {
        Some(BeadDescriptionSectionKind::AcceptanceCriteria) => {
            acceptance_criteria_lines.push(line.to_owned())
        }
        Some(BeadDescriptionSectionKind::NonGoals) => non_goal_lines.push(line.to_owned()),
        None => must_do_lines.push(line.to_owned()),
    };

    for line in description.lines() {
        if let Some(opening) = active_fence {
            push_line(active_section, line);
            if closes_fence(line, opening) {
                active_fence = None;
            }
            previous_line_blank = line.trim().is_empty();
            continue;
        }

        if let Some(opening) = opening_fence_delimiter(line) {
            push_line(active_section, line);
            active_fence = Some(opening);
            previous_line_blank = line.trim().is_empty();
            continue;
        }

        if let Some(section_kind) = section_kind_for_bead_description_line(line) {
            active_section = Some(section_kind);
            previous_line_blank = false;
            continue;
        }

        // Only a subsection label after a blank line ends an active embedded
        // Non-goals / Acceptance Criteria block. Adjacent labels stay inside
        // the section so examples like `Details:` or `### Notes` can be kept as
        // body content instead of snapping back into must-do scope mid-list.
        if active_section.is_some()
            && previous_line_blank
            && is_bead_description_subsection_boundary(line)
        {
            active_section = None;
        }

        push_line(active_section, line);
        previous_line_blank = line.trim().is_empty();
    }

    let must_do_scope = must_do_lines.join("\n").trim().to_owned();
    let non_goals = collect_section_items(&non_goal_lines);
    let acceptance_criteria = collect_section_items(&acceptance_criteria_lines);
    (must_do_scope, non_goals, acceptance_criteria)
}

pub fn render_bead_task_prompt(context: &BeadProjectContext) -> String {
    const DEPENDENCY_CONTEXT_MAX_ITEMS: usize = 8;
    const DEPENDENCY_CONTEXT_MAX_BYTES: usize = 1536;

    fn escape_canonical_heading_lines(value: &str) -> String {
        let mut active_fence = None;
        value
            .lines()
            .map(|line| {
                if let Some(opening) = active_fence {
                    if closes_fence(line, opening) {
                        active_fence = None;
                    }
                    return line.to_owned();
                }

                if let Some(opening) = opening_fence_delimiter(line) {
                    active_fence = Some(opening);
                    return line.to_owned();
                }

                let trimmed_end = line.trim_end();
                let leading_spaces = trimmed_end.chars().take_while(|ch| *ch == ' ').count();
                let is_canonical_heading = leading_spaces <= 3
                    && trimmed_end[leading_spaces..]
                        .strip_prefix("## ")
                        .map(|title| {
                            task_prompt_contract::BEAD_TASK_PROMPT_SECTION_TITLES.contains(&title)
                        })
                        .unwrap_or(false);
                if is_canonical_heading {
                    format!("    {line}")
                } else {
                    line.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn render_bullet_item(prefix: &str, value: &str) -> String {
        let mut lines = value.lines();
        let first_line = lines.next().unwrap_or_default();
        let continuation_indent = " ".repeat(prefix.len().max(4));
        let mut rendered =
            if !first_line.is_empty() && opening_fence_delimiter(first_line).is_some() {
                format!(
                    "{}\n{}{}",
                    prefix.trim_end(),
                    continuation_indent,
                    first_line
                )
            } else {
                format!("{prefix}{first_line}")
            };
        for line in lines {
            rendered.push('\n');
            rendered.push_str(&continuation_indent);
            rendered.push_str(line);
        }
        rendered
    }

    fn bullet_lines(items: &[String]) -> String {
        items
            .iter()
            .map(|item| render_bullet_item("- ", item))
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn bullet_lines_or_default(items: &[String], default: &str) -> String {
        if items.is_empty() {
            default.to_owned()
        } else {
            bullet_lines(items)
        }
    }

    fn bullet_item_bytes(prefix: &str, value: &str) -> usize {
        render_bullet_item(prefix, value).len()
    }

    fn bullet_block_bytes(items: &[String]) -> usize {
        if items.is_empty() {
            0
        } else {
            items
                .iter()
                .map(|item| bullet_item_bytes("- ", item))
                .sum::<usize>()
                + (items.len() - 1)
        }
    }

    fn indent_section_body(value: &str) -> String {
        value
            .lines()
            .map(|line| {
                if line.is_empty() {
                    String::new()
                } else {
                    format!("    {line}")
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn format_milestone_progress(progress: &MilestoneProgress) -> String {
        format!(
            "{}/{} completed; {} in progress; {} failed; {} blocked; {} skipped; {} remaining",
            progress.completed_beads,
            progress.total_beads,
            progress.in_progress_beads,
            progress.failed_beads,
            progress.blocked_beads,
            progress.skipped_beads,
            progress.remaining()
        )
    }

    fn dependency_context_item(item: &BeadDependencyPromptContext) -> String {
        let mut line = match item.title.as_deref() {
            Some(title) if !title.trim().is_empty() => {
                format!("{} ({title})", item.id)
            }
            _ => item.id.clone(),
        };
        line.push_str(&format!(" - {}", item.relationship));
        if let Some(status) = item.status.as_deref() {
            line.push_str(&format!("; status: {status}"));
        }
        if let Some(outcome) = item.outcome.as_deref() {
            line.push_str(&format!("; outcome: {outcome}"));
        }
        line
    }

    fn dependency_context_suffix(item: &BeadDependencyPromptContext) -> String {
        let mut suffix = String::new();
        if let Some(status) = item.status.as_deref() {
            suffix.push_str(&format!("; status: {status}"));
        }
        if let Some(outcome) = item.outcome.as_deref() {
            suffix.push_str(&format!("; outcome: {outcome}"));
        }
        suffix
    }

    fn dependency_context_item_without_title(
        item: &BeadDependencyPromptContext,
        relationship: &str,
    ) -> String {
        let mut line = item.id.clone();
        if !relationship.is_empty() {
            line.push_str(&format!(" - {relationship}"));
        }
        line.push_str(&dependency_context_suffix(item));
        line
    }

    fn fit_dependency_context_item_to_budget(
        item: &BeadDependencyPromptContext,
        max_bytes: usize,
    ) -> Option<String> {
        let full = dependency_context_item(item);
        if bullet_item_bytes("- ", &full) <= max_bytes {
            return Some(full);
        }

        let without_title = dependency_context_item_without_title(item, &item.relationship);
        if bullet_item_bytes("- ", &without_title) <= max_bytes {
            return Some(without_title);
        }

        let minimal = dependency_context_item_without_title(item, "");
        if bullet_item_bytes("- ", &minimal) > max_bytes {
            return None;
        }

        let mut best_fit = minimal;
        let mut low = 1usize;
        let mut high = item.relationship.len();
        while low <= high {
            let mid = low + (high - low) / 2;
            let Some(truncated_relationship) =
                truncate_with_ascii_ellipsis(&item.relationship, mid)
            else {
                break;
            };
            let candidate = dependency_context_item_without_title(item, &truncated_relationship);
            if bullet_item_bytes("- ", &candidate) <= max_bytes {
                best_fit = candidate;
                low = mid.saturating_add(1);
            } else if mid == 0 {
                break;
            } else {
                high = mid - 1;
            }
        }

        Some(best_fit)
    }

    fn omitted_dependency_context_line(count: usize, label: &str) -> String {
        format!("{count} additional {label} omitted for prompt budget.")
    }

    fn format_dependency_context(items: &[BeadDependencyPromptContext], label: &str) -> String {
        if items.is_empty() {
            return "None.".to_owned();
        }

        let rendered_items = items
            .iter()
            .map(dependency_context_item)
            .collect::<Vec<_>>();
        if rendered_items.len() <= DEPENDENCY_CONTEXT_MAX_ITEMS
            && bullet_block_bytes(&rendered_items) <= DEPENDENCY_CONTEXT_MAX_BYTES
        {
            return bullet_lines(&rendered_items);
        }

        let mut selected = Vec::new();
        for item in items {
            if selected.len() >= DEPENDENCY_CONTEXT_MAX_ITEMS {
                break;
            }

            let omitted_count = items.len().saturating_sub(selected.len() + 1);
            let selected_bytes = bullet_block_bytes(&selected);
            let separator_before_item = usize::from(!selected.is_empty());
            let omission_line =
                (omitted_count > 0).then(|| omitted_dependency_context_line(omitted_count, label));
            let separator_before_omission = usize::from(omission_line.is_some());
            let omission_bytes = omission_line
                .as_ref()
                .map(|line| bullet_item_bytes("- ", line))
                .unwrap_or(0);
            let remaining_bytes = DEPENDENCY_CONTEXT_MAX_BYTES
                .saturating_sub(selected_bytes)
                .saturating_sub(separator_before_item)
                .saturating_sub(separator_before_omission)
                .saturating_sub(omission_bytes);
            let Some(fitted_item) = fit_dependency_context_item_to_budget(item, remaining_bytes)
            else {
                break;
            };

            let mut candidate = selected.clone();
            candidate.push(fitted_item.clone());
            if let Some(omission_line) = omission_line {
                candidate.push(omission_line);
            }
            if bullet_block_bytes(&candidate) > DEPENDENCY_CONTEXT_MAX_BYTES {
                break;
            }

            selected.push(fitted_item);
        }

        let omitted_count = rendered_items.len().saturating_sub(selected.len());
        if omitted_count == 0 {
            return bullet_lines(&selected);
        }

        while !selected.is_empty() {
            let omission_line =
                omitted_dependency_context_line(rendered_items.len() - selected.len(), label);
            let mut candidate = selected.clone();
            candidate.push(omission_line.clone());
            if bullet_block_bytes(&candidate) <= DEPENDENCY_CONTEXT_MAX_BYTES {
                selected = candidate;
                return bullet_lines(&selected);
            }
            selected.pop();
        }

        bullet_lines(&[omitted_dependency_context_line(rendered_items.len(), label)])
    }

    fn format_planned_elsewhere(items: &[PlannedElsewherePromptContext]) -> String {
        if items.is_empty() {
            return "No explicit planned-elsewhere items were supplied. Do not absorb adjacent bead work unless it is required to satisfy this bead's acceptance criteria.".to_owned();
        }

        bullet_lines(
            &items
                .iter()
                .map(|item| {
                    let mut line = format!("{} ({}) - {}", item.id, item.title, item.relationship);
                    if let Some(status) = item.status.as_deref() {
                        line.push_str(&format!("; status: {status}"));
                    }
                    if let Some(summary) = item.summary.as_deref() {
                        line.push_str("\nSummary:");
                        line.push('\n');
                        line.push_str(summary);
                    }
                    line
                })
                .collect::<Vec<_>>(),
        )
    }

    let (bead_must_do_scope, bead_local_non_goals, bead_local_acceptance_criteria) = context
        .bead_description
        .as_deref()
        .map(split_bead_description_scope)
        .unwrap_or_else(|| (String::new(), Vec::new(), Vec::new()));

    let milestone_summary = {
        let mut lines = vec![
            render_bullet_item("- Milestone ID: ", &format!("`{}`", context.milestone_id)),
            render_bullet_item("- Milestone Name: ", &context.milestone_name),
            render_bullet_item(
                "- Status: ",
                &format!("`{}`", context.milestone_status.as_str()),
            ),
            render_bullet_item(
                "- Total beads: ",
                &context.milestone_progress.total_beads.to_string(),
            ),
            render_bullet_item(
                "- Progress: ",
                &format_milestone_progress(&context.milestone_progress),
            ),
        ];
        if let Some(summary) = &context.milestone_summary {
            lines.push(render_bullet_item("- Goal: ", summary));
        }
        lines.push(render_bullet_item(
            "- Description: ",
            &context.milestone_description,
        ));
        if !context.milestone_goals.is_empty() {
            lines.push("- Goals:".to_owned());
            lines.extend(
                context
                    .milestone_goals
                    .iter()
                    .map(|goal| render_bullet_item("  - ", goal)),
            );
        }
        lines.join("\n")
    };

    let current_bead_details = {
        let mut lines = vec![
            render_bullet_item("- Bead ID: ", &format!("`{}`", context.bead_id)),
            render_bullet_item("- Title: ", &context.bead_title),
            render_bullet_item("- Flow: ", &format!("`{}`", context.flow.as_str())),
        ];
        if let Some(parent_epic_id) = &context.parent_epic_id {
            lines.push(render_bullet_item(
                "- Parent epic: ",
                &format!("`{parent_epic_id}`"),
            ));
        } else {
            lines.push("- Parent epic: None.".to_owned());
        }
        if let Some(plan_hash) = &context.plan_hash {
            lines.push(render_bullet_item(
                "- Plan hash: ",
                &format!("`{plan_hash}`"),
            ));
        } else {
            lines.push("- Plan hash: None.".to_owned());
        }
        if let Some(plan_version) = context.plan_version {
            lines.push(render_bullet_item(
                "- Plan version: ",
                &plan_version.to_string(),
            ));
        } else {
            lines.push("- Plan version: None.".to_owned());
        }
        lines.push(String::new());
        lines.push("### Dependency Context".to_owned());
        lines.push(String::new());
        lines.push("- Upstream dependencies:".to_owned());
        lines.push(indent_section_body(&format_dependency_context(
            &context.upstream_dependencies,
            "upstream dependencies",
        )));
        lines.push("- Downstream dependents:".to_owned());
        lines.push(indent_section_body(&format_dependency_context(
            &context.downstream_dependents,
            "downstream dependents",
        )));
        lines.join("\n")
    };

    let must_do_scope = if bead_must_do_scope.is_empty() {
        "No explicit scope description was supplied. Use the bead title and acceptance criteria as the required scope boundary.".to_owned()
    } else {
        bead_must_do_scope
    };
    let merged_non_goals = merge_unique_lines(&bead_local_non_goals, &context.milestone_non_goals);
    let non_goals =
        bullet_lines_or_default(&merged_non_goals, "None captured in the milestone plan.");
    let merged_acceptance_criteria = merge_unique_lines(
        &context.bead_acceptance_criteria,
        &bead_local_acceptance_criteria,
    );
    let acceptance_criteria = bullet_lines_or_default(
        &merged_acceptance_criteria,
        "No explicit acceptance criteria were supplied.",
    );
    let planned_elsewhere = format_planned_elsewhere(&context.planned_elsewhere);
    let review_policy = {
        let review_policy = bullet_lines_or_default(
            &context.review_policy,
            "Use the active bead scope as the review boundary.",
        );
        if context.milestone_constraints.is_empty() {
            review_policy
        } else {
            format!(
                "{review_policy}\n\n### Milestone Plan Constraints\n\n{}",
                bullet_lines(&context.milestone_constraints)
            )
        }
    };
    let repo_guidance = match &context.agents_guidance {
        Some(guidance) => format!(
            "Follow the repository AGENTS.md instructions for this repo.\n\n{guidance}"
        ),
        None => "Follow the repository AGENTS.md instructions for this repo.\n\nNo milestone-specific AGENTS guidance was supplied.".to_owned(),
    };
    let repo_guidance = escape_canonical_heading_lines(&repo_guidance);

    [format!(
            "# Ralph Task Prompt\n\n{}\n\n- Contract: `{}`\n- Version: `{}`\n- Milestone: `{}`\n- Bead: `{}`\n\nThis project executes bead `{}` for milestone `{}`.",
            task_prompt_contract::contract_marker(),
            task_prompt_contract::BEAD_TASK_PROMPT_CONTRACT_NAME,
            task_prompt_contract::BEAD_TASK_PROMPT_CONTRACT_VERSION,
            context.milestone_id,
            context.bead_id,
            context.bead_id,
            context.milestone_id
        ),
        format!(
            "## {}\n\n{}",
            task_prompt_contract::SECTION_MILESTONE_SUMMARY,
            milestone_summary
        ),
        format!(
            "## {}\n\n{}",
            task_prompt_contract::SECTION_CURRENT_BEAD_DETAILS,
            current_bead_details
        ),
        format!(
            "## {}\n\n{}",
            task_prompt_contract::SECTION_MUST_DO_SCOPE,
            indent_section_body(&must_do_scope)
        ),
        format!(
            "## {}\n\n{}",
            task_prompt_contract::SECTION_EXPLICIT_NON_GOALS,
            non_goals
        ),
        format!(
            "## {}\n\n{}",
            task_prompt_contract::SECTION_ACCEPTANCE_CRITERIA,
            acceptance_criteria
        ),
        format!(
            "## {}\n\n{}",
            task_prompt_contract::SECTION_ALREADY_PLANNED_ELSEWHERE,
            planned_elsewhere
        ),
        format!(
            "## {}\n\n{}",
            task_prompt_contract::SECTION_REVIEW_POLICY,
            review_policy
        ),
        format!(
            "## {}\n\n{}",
            task_prompt_contract::SECTION_AGENTS_REPO_GUIDANCE,
            repo_guidance
        )]
    .join("\n\n")
}

pub(crate) fn default_project_id_for_bead(
    milestone_id: &str,
    bead_id: &str,
) -> AppResult<ProjectId> {
    let bead_segment = bead_id
        .strip_prefix(milestone_id)
        .and_then(|suffix| suffix.strip_prefix('.'))
        .unwrap_or(bead_id);
    ProjectId::new(format!(
        "task-{}-{}",
        sanitize_project_id_component(milestone_id),
        sanitize_project_id_component(bead_segment)
    ))
}

fn sanitize_project_id_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '-',
        })
        .collect()
}

fn create_project_with_initial_details(
    store: &dyn ProjectStorePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    input: CreateProjectInput,
    initial_details: serde_json::Value,
) -> AppResult<ProjectRecord> {
    // Check for duplicate project ID
    if store.project_exists(base_dir, &input.id)? {
        return Err(AppError::DuplicateProject {
            project_id: input.id.to_string(),
        });
    }

    let record = ProjectRecord {
        id: input.id.clone(),
        name: input.name,
        flow: input.flow,
        prompt_reference: "prompt.md".to_owned(),
        prompt_hash: input.prompt_hash,
        created_at: input.created_at,
        status_summary: ProjectStatusSummary::Created,
        task_source: input.task_source,
    };

    let max_completion_rounds = EffectiveConfig::load(base_dir)
        .map(|cfg| cfg.run_policy().max_completion_rounds)
        .unwrap_or(DEFAULT_MAX_COMPLETION_ROUNDS);
    let run_snapshot = RunSnapshot::initial(max_completion_rounds);
    let sessions = SessionStore::empty();

    // Create the initial journal event
    let initial_event = JournalEvent {
        sequence: 1,
        timestamp: input.created_at,
        event_type: JournalEventType::ProjectCreated,
        details: initial_details,
    };
    let journal_line = journal::serialize_event(&initial_event)?;

    // Atomic creation: all files or nothing
    store.create_project_atomic(
        base_dir,
        &record,
        &input.prompt_contents,
        &run_snapshot,
        &journal_line,
        &sessions,
    )?;

    // create_project_atomic writes the journal as part of the atomic init,
    // but we don't call journal_store.append_event here because the atomic
    // creation already wrote the initial journal line. The journal_store
    // parameter is kept for interface consistency and future use.
    let _ = journal_store;

    Ok(record)
}

fn inject_task_source_initial_details(
    details: &mut serde_json::Map<String, serde_json::Value>,
    task_source: Option<&TaskSource>,
) {
    let Some(task_source) = task_source else {
        return;
    };

    details.insert(
        "milestone_id".to_owned(),
        serde_json::Value::String(task_source.milestone_id.clone()),
    );
    details.insert(
        "bead_id".to_owned(),
        serde_json::Value::String(task_source.bead_id.clone()),
    );
    details.insert(
        "origin".to_owned(),
        serde_json::Value::String(
            serde_json::to_string(&task_source.origin)
                .expect("task origin should serialize")
                .trim_matches('"')
                .to_owned(),
        ),
    );
    if let Some(parent_epic_id) = &task_source.parent_epic_id {
        details.insert(
            "parent_epic_id".to_owned(),
            serde_json::Value::String(parent_epic_id.clone()),
        );
    }
    if let Some(plan_hash) = &task_source.plan_hash {
        details.insert(
            "plan_hash".to_owned(),
            serde_json::Value::String(plan_hash.clone()),
        );
    }
    if let Some(plan_version) = task_source.plan_version {
        details.insert(
            "plan_version".to_owned(),
            serde_json::Value::Number(plan_version.into()),
        );
    }
}

fn build_missing_task_lineage_detail(task_source: &TaskSource) -> BeadLineageView {
    BeadLineageView {
        milestone_id: task_source.milestone_id.clone(),
        milestone_name: "<milestone metadata unavailable>".to_owned(),
        bead_id: task_source.bead_id.clone(),
        bead_title: None,
        acceptance_criteria: Vec::new(),
    }
}

/// List all projects with summary data.
pub fn list_projects(
    store: &dyn ProjectStorePort,
    active_port: &dyn ActiveProjectPort,
    base_dir: &Path,
) -> AppResult<Vec<ProjectListEntry>> {
    list_project_entries(store, active_port, base_dir, false)
}

/// List only bead-backed task projects with summary data.
pub fn list_task_projects(
    store: &dyn ProjectStorePort,
    active_port: &dyn ActiveProjectPort,
    base_dir: &Path,
) -> AppResult<Vec<ProjectListEntry>> {
    list_project_entries(store, active_port, base_dir, true)
}

fn list_project_entries(
    store: &dyn ProjectStorePort,
    active_port: &dyn ActiveProjectPort,
    base_dir: &Path,
    tasks_only: bool,
) -> AppResult<Vec<ProjectListEntry>> {
    let active_id = active_port.read_active_project_id(base_dir)?;
    let project_ids = store.list_project_ids(base_dir)?;

    let mut entries = Vec::new();
    for pid in project_ids {
        let record = store.read_project_record(base_dir, &pid)?;
        if tasks_only && !record.is_milestone_task() {
            continue;
        }
        let is_active = active_id.as_deref() == Some(pid.as_str());
        entries.push(ProjectListEntry {
            id: record.id,
            name: record.name,
            flow: record.flow,
            status_summary: record.status_summary,
            created_at: record.created_at,
            is_active,
        });
    }

    Ok(entries)
}

fn load_task_lineage_detail(
    milestone_store: &dyn MilestoneStorePort,
    plan_store: &dyn MilestonePlanPort,
    base_dir: &Path,
    task_source: Option<&TaskSource>,
) -> AppResult<Option<BeadLineageView>> {
    let Some(task_source) = task_source else {
        return Ok(None);
    };

    let milestone_id = crate::contexts::milestone_record::model::MilestoneId::new(
        task_source.milestone_id.clone(),
    )?;

    match crate::contexts::milestone_record::service::read_bead_lineage(
        milestone_store,
        plan_store,
        base_dir,
        &milestone_id,
        &task_source.bead_id,
        task_source.plan_hash.as_deref(),
    ) {
        Ok(lineage) => Ok(Some(lineage)),
        Err(AppError::MilestoneNotFound { .. }) => {
            Ok(Some(build_missing_task_lineage_detail(task_source)))
        }
        Err(error) => Err(error),
    }
}

/// Show detailed project information.
#[allow(clippy::too_many_arguments)]
pub fn show_project(
    store: &dyn ProjectStorePort,
    run_port: &dyn RunSnapshotPort,
    journal_port: &dyn JournalStorePort,
    active_port: &dyn ActiveProjectPort,
    milestone_store: &dyn MilestoneStorePort,
    plan_store: &dyn MilestonePlanPort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<ProjectDetail> {
    if !store.project_exists(base_dir, project_id)? {
        return Err(AppError::ProjectNotFound {
            project_id: project_id.to_string(),
        });
    }

    let record = store.read_project_record(base_dir, project_id)?;
    let run_snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    let events = journal_port.read_journal(base_dir, project_id)?;
    let journal_event_count = events.len() as u64;
    let active_id = active_port.read_active_project_id(base_dir)?;
    let is_active = active_id.as_deref() == Some(project_id.as_str());
    let task_lineage = load_task_lineage_detail(
        milestone_store,
        plan_store,
        base_dir,
        record.task_source.as_ref(),
    )?;

    // Count rollback points from journal events
    let rollback_count = events
        .iter()
        .filter(|e| e.event_type == JournalEventType::RollbackCreated)
        .count();

    Ok(ProjectDetail {
        record,
        run_snapshot,
        journal_event_count,
        rollback_count,
        is_active,
        task_lineage,
    })
}

/// Delete a project. Fails if project has an active run.
/// Clears active-project pointer if it pointed to the deleted project.
///
/// Transactional: if any post-validation step fails, the project remains
/// addressable at its canonical path and the active-project pointer is
/// unchanged.
pub fn delete_project(
    store: &dyn ProjectStorePort,
    run_port: &dyn RunSnapshotPort,
    active_port: &dyn ActiveProjectPort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<()> {
    if !store.project_exists(base_dir, project_id)? {
        return Err(AppError::ProjectNotFound {
            project_id: project_id.to_string(),
        });
    }

    // Check for active run
    let run_snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    if run_snapshot.has_active_run() {
        return Err(AppError::ActiveRunDelete {
            project_id: project_id.to_string(),
        });
    }

    // Check if this is the active project (before delete)
    let active_id = active_port.read_active_project_id(base_dir)?;
    let was_active = active_id.as_deref() == Some(project_id.as_str());

    // Phase 1: Stage the delete — project becomes invisible to list/show
    // but data remains on disk for rollback.
    store.stage_delete(base_dir, project_id)?;

    // Phase 2: Clear the active-project pointer if needed.
    // If this fails, roll back the staged delete so the project remains
    // addressable and the pointer is unchanged.
    if was_active {
        if let Err(clear_err) = active_port.clear_active_project(base_dir) {
            store
                .rollback_delete(base_dir, project_id)
                .map_err(|restore_err| AppError::CorruptRecord {
                    file: format!("projects/{}", project_id),
                    details: format!(
                        "delete partially failed: pointer clear error: {}, restore error: {}",
                        clear_err, restore_err
                    ),
                })?;
            return Err(clear_err);
        }
    }

    // Phase 3: Finalize — permanently remove the project data.
    // At this point the logical delete has succeeded (project is invisible,
    // pointer is cleared). Best-effort cleanup of the staged data.
    let _ = store.commit_delete(base_dir, project_id);

    Ok(())
}

/// Get run status for the active project.
///
/// Cross-checks the snapshot against the journal to detect stale snapshot
/// state caused by `fail_run()` snapshot write failures.
pub fn run_status(
    run_port: &dyn RunSnapshotPort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<RunStatusView> {
    let snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    Ok(queries::build_status_view(project_id.as_str(), &snapshot))
}

/// Get run status with journal reconciliation for the active project.
///
/// When a journal store is available, cross-checks the snapshot against
/// the journal's terminal events and corrects stale snapshot state.
pub fn run_status_reconciled(
    run_port: &dyn RunSnapshotPort,
    journal_port: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<RunStatusView> {
    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    if let Ok(events) = journal_port.read_journal(base_dir, project_id) {
        queries::reconcile_snapshot_status(&mut snapshot, &events);
    }
    Ok(queries::build_status_view(project_id.as_str(), &snapshot))
}

/// Get stable JSON run status for the active project.
pub fn run_status_json(
    run_port: &dyn RunSnapshotPort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<RunStatusJsonView> {
    let snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    Ok(RunStatusJsonView::from_snapshot(
        project_id.as_str(),
        &snapshot,
    ))
}

/// Get stable JSON run status with journal reconciliation.
pub fn run_status_json_reconciled(
    run_port: &dyn RunSnapshotPort,
    journal_port: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<RunStatusJsonView> {
    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    if let Ok(events) = journal_port.read_journal(base_dir, project_id) {
        queries::reconcile_snapshot_status(&mut snapshot, &events);
    }
    Ok(RunStatusJsonView::from_snapshot(
        project_id.as_str(),
        &snapshot,
    ))
}

/// Get run history (durable only, no runtime logs).
pub fn run_history(
    store: &dyn ProjectStorePort,
    journal_port: &dyn JournalStorePort,
    artifact_port: &dyn ArtifactStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<RunHistoryView> {
    let record = store.read_project_record(base_dir, project_id)?;
    let events =
        queries::visible_journal_events(&journal_port.read_journal(base_dir, project_id)?)?;
    let (milestone_id, bead_id) = queries::history_lineage(&events);
    let milestone_id = milestone_id.or_else(|| {
        record
            .task_source
            .as_ref()
            .map(|task_source| task_source.milestone_id.clone())
    });
    let bead_id = bead_id.or_else(|| {
        record
            .task_source
            .as_ref()
            .map(|task_source| task_source.bead_id.clone())
    });
    let (payloads, artifacts) = queries::filter_history_records(
        &events,
        artifact_port.list_payloads(base_dir, project_id)?,
        artifact_port.list_artifacts(base_dir, project_id)?,
    )?;

    queries::validate_history_consistency(&payloads, &artifacts)?;

    Ok(queries::build_history_view(
        project_id.as_str(),
        milestone_id,
        bead_id,
        events,
        payloads,
        artifacts,
    ))
}

/// Get run tail: durable history by default, with optional runtime logs.
pub fn run_tail(
    journal_port: &dyn JournalStorePort,
    artifact_port: &dyn ArtifactStorePort,
    log_port: &dyn RuntimeLogStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    include_logs: bool,
) -> AppResult<RunTailView> {
    let events =
        queries::visible_journal_events(&journal_port.read_journal(base_dir, project_id)?)?;
    let (payloads, artifacts) = queries::filter_history_records(
        &events,
        artifact_port.list_payloads(base_dir, project_id)?,
        artifact_port.list_artifacts(base_dir, project_id)?,
    )?;

    queries::validate_history_consistency(&payloads, &artifacts)?;

    let runtime_logs = if include_logs {
        log_port.read_runtime_logs(base_dir, project_id)?
    } else {
        Vec::new()
    };

    Ok(queries::build_tail_view(
        project_id.as_str(),
        events,
        payloads,
        artifacts,
        include_logs,
        runtime_logs,
    ))
}

/// List visible rollback points for the current logical history branch.
pub fn list_rollback_points(
    rollback_store: &dyn RollbackPointStorePort,
    journal_port: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<Vec<RollbackPoint>> {
    let events =
        queries::visible_journal_events(&journal_port.read_journal(base_dir, project_id)?)?;
    let visible_ids = visible_rollback_ids(&events);
    let mut points = rollback_store
        .list_rollback_points(base_dir, project_id)?
        .into_iter()
        .filter(|point| visible_ids.contains(point.rollback_id.as_str()))
        .collect::<Vec<_>>();
    points.sort_by_key(|point| point.created_at);
    Ok(points)
}

/// List visible rollback targets in a CLI-ready view format.
pub fn list_rollback_targets(
    rollback_store: &dyn RollbackPointStorePort,
    journal_port: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<Vec<RunRollbackTargetView>> {
    Ok(
        list_rollback_points(rollback_store, journal_port, base_dir, project_id)?
            .into_iter()
            .map(|point| RunRollbackTargetView {
                rollback_id: point.rollback_id,
                stage_id: point.stage_id.as_str().to_owned(),
                cycle: point.cycle,
                created_at: point.created_at,
                git_sha: point.git_sha,
            })
            .collect(),
    )
}

/// Look up the latest visible rollback point for a stage.
pub fn get_rollback_point_for_stage(
    rollback_store: &dyn RollbackPointStorePort,
    journal_port: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    stage_id: StageId,
) -> AppResult<Option<RollbackPoint>> {
    let visible_events =
        queries::visible_journal_events(&journal_port.read_journal(base_dir, project_id)?)?;
    let visible_ids = visible_rollback_ids(&visible_events);

    Ok(rollback_store
        .list_rollback_points(base_dir, project_id)?
        .into_iter()
        .filter(|point| point.stage_id == stage_id)
        .filter(|point| visible_ids.contains(point.rollback_id.as_str()))
        .max_by_key(|point| point.created_at))
}

/// Resolve a visible payload record by ID.
pub fn get_payload_by_id(
    journal_port: &dyn JournalStorePort,
    artifact_port: &dyn ArtifactStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    payload_id: &str,
) -> AppResult<PayloadRecord> {
    let events =
        queries::visible_journal_events(&journal_port.read_journal(base_dir, project_id)?)?;
    let (payloads, artifacts) = queries::filter_history_records(
        &events,
        artifact_port.list_payloads(base_dir, project_id)?,
        artifact_port.list_artifacts(base_dir, project_id)?,
    )?;
    queries::validate_history_consistency(&payloads, &artifacts)?;

    if !payloads
        .iter()
        .any(|payload| payload.payload_id == payload_id)
    {
        return Err(AppError::PayloadNotFound {
            payload_id: payload_id.to_owned(),
        });
    }

    artifact_port.read_payload_by_id(base_dir, project_id, payload_id)
}

/// Resolve a visible artifact record by ID.
pub fn get_artifact_by_id(
    journal_port: &dyn JournalStorePort,
    artifact_port: &dyn ArtifactStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    artifact_id: &str,
) -> AppResult<ArtifactRecord> {
    let events =
        queries::visible_journal_events(&journal_port.read_journal(base_dir, project_id)?)?;
    let (payloads, artifacts) = queries::filter_history_records(
        &events,
        artifact_port.list_payloads(base_dir, project_id)?,
        artifact_port.list_artifacts(base_dir, project_id)?,
    )?;
    queries::validate_history_consistency(&payloads, &artifacts)?;

    if !artifacts
        .iter()
        .any(|artifact| artifact.artifact_id == artifact_id)
    {
        return Err(AppError::ArtifactNotFound {
            artifact_id: artifact_id.to_owned(),
        });
    }

    artifact_port.read_artifact_by_id(base_dir, project_id, artifact_id)
}

/// Perform a logical or hard rollback to a visible checkpoint.
#[allow(clippy::too_many_arguments)]
pub fn perform_rollback(
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    journal_port: &dyn JournalStorePort,
    rollback_store: &dyn RollbackPointStorePort,
    reset_port: Option<&dyn RepositoryResetPort>,
    base_dir: &Path,
    project_id: &ProjectId,
    flow: FlowPreset,
    target_stage: StageId,
    hard: bool,
) -> AppResult<RollbackPoint> {
    let current_snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    match current_snapshot.status {
        RunStatus::Failed | RunStatus::Paused => {}
        status => {
            return Err(AppError::RollbackInvalidStatus {
                project_id: project_id.to_string(),
                status: status.to_string(),
            });
        }
    }

    if !flow_stage_membership(flow, target_stage) {
        return Err(AppError::RollbackStageNotInFlow {
            project_id: project_id.to_string(),
            stage_id: target_stage.to_string(),
            flow: flow.to_string(),
        });
    }

    let rollback_point = get_rollback_point_for_stage(
        rollback_store,
        journal_port,
        base_dir,
        project_id,
        target_stage,
    )?
    .ok_or_else(|| AppError::RollbackPointNotFound {
        project_id: project_id.to_string(),
        stage_id: target_stage.to_string(),
    })?;

    let events = journal_port.read_journal(base_dir, project_id)?;
    let visible_through_sequence =
        rollback_created_sequence_for(&events, rollback_point.rollback_id.as_str())?;

    let mut restored_snapshot = rollback_point.run_snapshot.clone();
    restored_snapshot.status = RunStatus::Paused;
    restored_snapshot.interrupted_run = restored_snapshot
        .active_run
        .clone()
        .or_else(|| restored_snapshot.interrupted_run.clone());
    restored_snapshot.active_run = None;
    restored_snapshot.rollback_point_meta.rollback_count =
        current_snapshot.rollback_point_meta.rollback_count + 1;
    restored_snapshot.rollback_point_meta.last_rollback_id =
        Some(rollback_point.rollback_id.clone());
    restored_snapshot.status_summary = format!(
        "paused after rollback to {} (cycle {}); run `ralph-burning run resume` to continue",
        rollback_point.stage_id.display_name(),
        rollback_point.cycle
    );

    let sequence = journal::last_sequence(&events) + 1;
    let rollback_event = journal::rollback_performed_event(
        sequence,
        Utc::now(),
        rollback_point.rollback_id.as_str(),
        rollback_point.stage_id,
        rollback_point.cycle,
        visible_through_sequence,
        hard,
        rollback_point.git_sha.as_deref(),
        restored_snapshot.rollback_point_meta.rollback_count,
    );
    let rollback_line = journal::serialize_event(&rollback_event)?;
    run_write_port.write_run_snapshot(base_dir, project_id, &restored_snapshot)?;
    if let Err(append_error) = journal_port.append_event(base_dir, project_id, &rollback_line) {
        if let Err(restore_error) =
            run_write_port.write_run_snapshot(base_dir, project_id, &current_snapshot)
        {
            return Err(AppError::CorruptRecord {
                file: format!("projects/{}/run.json", project_id.as_str()),
                details: format!(
                    "rollback journal append failed after snapshot write: {append_error}; failed to restore the previous snapshot: {restore_error}"
                ),
            });
        }
        return Err(append_error);
    }

    if hard {
        let git_sha =
            rollback_point
                .git_sha
                .as_deref()
                .ok_or_else(|| AppError::RollbackGitResetFailed {
                    project_id: project_id.to_string(),
                    rollback_id: rollback_point.rollback_id.clone(),
                    details: "rollback point does not record a git commit SHA".to_owned(),
                })?;

        let reset_port = reset_port.ok_or_else(|| AppError::RollbackGitResetFailed {
            project_id: project_id.to_string(),
            rollback_id: rollback_point.rollback_id.clone(),
            details: "no repository reset adapter was provided".to_owned(),
        })?;

        if let Err(error) = reset_port.reset_to_sha(base_dir, git_sha) {
            return Err(AppError::RollbackGitResetFailed {
                project_id: project_id.to_string(),
                rollback_id: rollback_point.rollback_id.clone(),
                details: error.to_string(),
            });
        }
    }

    Ok(rollback_point)
}

fn flow_stage_membership(flow: FlowPreset, stage_id: StageId) -> bool {
    workflow_composition::flow_definition(flow)
        .stages
        .contains(&stage_id)
}

fn visible_rollback_ids(events: &[JournalEvent]) -> std::collections::HashSet<&str> {
    events
        .iter()
        .filter(|event| event.event_type == JournalEventType::RollbackCreated)
        .filter_map(|event| {
            event
                .details
                .get("rollback_id")
                .and_then(|value| value.as_str())
        })
        .collect()
}

fn rollback_created_sequence_for(events: &[JournalEvent], rollback_id: &str) -> AppResult<u64> {
    events
        .iter()
        .find(|event| {
            event.event_type == JournalEventType::RollbackCreated
                && event
                    .details
                    .get("rollback_id")
                    .and_then(|value| value.as_str())
                    == Some(rollback_id)
        })
        .map(|event| event.sequence)
        .ok_or_else(|| AppError::CorruptRecord {
            file: "journal.ndjson".to_owned(),
            details: format!(
                "missing rollback_created event for rollback point '{}'",
                rollback_id
            ),
        })
}

// ── Shared Amendment Service ──────────────────────────────────────────────

/// Result of staging a manual amendment.
#[derive(Debug)]
pub enum AmendmentAddResult {
    /// A new amendment was created.
    Created { amendment_id: String },
    /// An existing amendment with the same dedup key was found.
    Duplicate { amendment_id: String },
}

/// Add a manual amendment. Performs dedup check, writes durably, emits a journal
/// event, syncs the canonical snapshot, and reopens completed projects.
#[allow(clippy::too_many_arguments)]
pub fn add_manual_amendment(
    amendment_queue: &dyn AmendmentQueuePort,
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    journal_port: &dyn JournalStorePort,
    project_store: &dyn ProjectStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    body: &str,
) -> AppResult<AmendmentAddResult> {
    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;

    // Reject while a run is actively writing.
    if snapshot.status == RunStatus::Running {
        return Err(AppError::AmendmentLeaseConflict {
            project_id: project_id.to_string(),
        });
    }

    let source = AmendmentSource::Manual;
    let dedup_key = QueuedAmendment::compute_dedup_key(&source, body);

    // Dedup check against canonical snapshot pending queue.
    if let Some(existing) = snapshot
        .amendment_queue
        .pending
        .iter()
        .find(|a| a.dedup_key == dedup_key)
    {
        return Ok(AmendmentAddResult::Duplicate {
            amendment_id: existing.amendment_id.clone(),
        });
    }

    // Also check staged amendment files on disk. For non-completed projects
    // this is a true duplicate. For completed projects, a matching file means
    // a prior reopen attempt failed after preserving the amendment on disk, so
    // the retry must reuse that file's ID instead of creating a new orphan.
    let matching_on_disk = amendment_queue
        .list_pending_amendments(base_dir, project_id)?
        .into_iter()
        .filter(|a| a.dedup_key == dedup_key)
        .collect::<Vec<_>>();
    if snapshot.status != RunStatus::Completed {
        if let Some(existing) = matching_on_disk.first() {
            return Ok(AmendmentAddResult::Duplicate {
                amendment_id: existing.amendment_id.clone(),
            });
        }
    }
    let existing_on_disk = matching_on_disk.first().cloned();
    let duplicate_disk_ids = if snapshot.status == RunStatus::Completed {
        matching_on_disk
            .iter()
            .skip(1)
            .map(|amendment| amendment.amendment_id.clone())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let now = Utc::now();
    let amendment_id = existing_on_disk
        .as_ref()
        .map(|existing| existing.amendment_id.clone())
        .unwrap_or_else(|| format!("manual-{}", uuid::Uuid::new_v4()));

    let current_cycle = snapshot
        .cycle_history
        .last()
        .map(|entry| entry.cycle)
        .unwrap_or(1);
    let completion_round = snapshot.completion_rounds.max(1);

    let amendment = QueuedAmendment {
        amendment_id: amendment_id.clone(),
        source_stage: StageId::Planning,
        source_cycle: current_cycle,
        source_completion_round: completion_round,
        body: body.to_owned(),
        created_at: now,
        batch_sequence: 0,
        source,
        dedup_key: dedup_key.clone(),
    };

    // Prepare the journal line BEFORE any mutations so that fallible
    // read_journal / serialize_event calls cannot fail after canonical state
    // is already committed.
    let journal_line = {
        let events = journal_port.read_journal(base_dir, project_id)?;
        let seq = journal::last_sequence(&events) + 1;
        let journal_event = journal::amendment_queued_manual_event(
            seq,
            now,
            &amendment_id,
            body,
            "manual",
            "planning",
            &dedup_key,
        );
        journal::serialize_event(&journal_event)?
    };

    if !duplicate_disk_ids.is_empty() {
        let mut cleanup_failures: Vec<String> = Vec::new();
        for duplicate_id in &duplicate_disk_ids {
            if let Err(err) = amendment_queue.remove_amendment(base_dir, project_id, duplicate_id) {
                cleanup_failures.push(format!("{duplicate_id}: {err}"));
            }
        }
        if !cleanup_failures.is_empty() {
            let retained_id = existing_on_disk
                .as_ref()
                .map(|amendment| amendment.amendment_id.as_str())
                .unwrap_or("<new>");
            return Err(AppError::AmendmentQueueError {
                details: format!(
                    "failed to collapse duplicate on-disk amendments for dedup_key '{dedup_key}' \
                     while preserving '{retained_id}': {}",
                    cleanup_failures.join("; ")
                ),
            });
        }
    }

    // Write durable amendment file.
    amendment_queue.write_amendment(base_dir, project_id, &amendment)?;

    // Save pre-mutation snapshot so we can restore it if a later step fails.
    let old_snapshot = snapshot.clone();

    // Sync canonical snapshot: add the amendment to run.json pending queue.
    // The snapshot is committed BEFORE the journal event so that a snapshot
    // write failure leaves no orphaned journal entry.
    snapshot.amendment_queue.pending.push(amendment.clone());

    // If the project is completed, reopen it with the pending amendment
    // already reflected in the snapshot.
    let snap_result = if snapshot.status == RunStatus::Completed {
        reopen_completed_project_with_snapshot(
            run_write_port,
            project_store,
            base_dir,
            project_id,
            &mut snapshot,
        )
    } else {
        // Persist the updated snapshot with the new pending amendment.
        run_write_port.write_run_snapshot(base_dir, project_id, &snapshot)
    };

    if let Err(snap_err) = snap_result {
        if old_snapshot.status == RunStatus::Completed {
            // Preserve the amendment file for completed-project reopen failures
            // so the operator's input is not lost on retry.
            return Err(snap_err);
        }
        // For non-completed projects, roll back the amendment file so it
        // doesn't become an orphan that blocks completion_guard.
        if let Err(cleanup_err) =
            amendment_queue.remove_amendment(base_dir, project_id, &amendment_id)
        {
            return Err(AppError::CorruptRecord {
                file: format!("projects/{}/run.json", project_id.as_str()),
                details: format!(
                    "snapshot/reopen write failed: {snap_err}; \
                     amendment file cleanup also failed: {cleanup_err}"
                ),
            });
        }
        return Err(snap_err);
    }

    // Durably append the journal event. A successful add must record the
    // amendment_queued event. If the append fails, roll back the snapshot
    // and amendment file so no amendment is visible without its history.
    if let Err(journal_err) = journal_port.append_event(base_dir, project_id, &journal_line) {
        // Attempt to restore pre-mutation state.
        let snap_result = run_write_port.write_run_snapshot(base_dir, project_id, &old_snapshot);
        // If we reused a preserved amendment file from a prior failed reopen,
        // don't delete it during rollback — the operator's input must survive.
        let file_result = if existing_on_disk.is_some() {
            Ok(())
        } else {
            amendment_queue.remove_amendment(base_dir, project_id, &amendment_id)
        };

        // If rollback itself failed, return a composite error so the caller
        // knows canonical state may be inconsistent, matching the pattern
        // used in execute_rollback.
        if snap_result.is_err() || file_result.is_err() {
            let snap_detail = snap_result
                .err()
                .map_or_else(|| "ok".to_owned(), |e| e.to_string());
            let file_detail = file_result
                .err()
                .map_or_else(|| "ok".to_owned(), |e| e.to_string());
            return Err(AppError::CorruptRecord {
                file: format!("projects/{}/run.json", project_id.as_str()),
                details: format!(
                    "amendment journal append failed: {journal_err}; \
                     rollback also failed — snapshot restore: {snap_detail}, \
                     file cleanup: {file_detail}"
                ),
            });
        }

        return Err(journal_err);
    }

    Ok(AmendmentAddResult::Created { amendment_id })
}

/// List pending amendments for a project from the canonical run.json snapshot.
pub fn list_amendments(
    run_port: &dyn RunSnapshotPort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<Vec<QueuedAmendment>> {
    let snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    Ok(snapshot.amendment_queue.pending)
}

fn restore_completed_state_after_reopen_if_no_amendments(
    snapshot: &mut RunSnapshot,
    project_id: &ProjectId,
) -> bool {
    let pre_reopen_completion_round = if snapshot.status == RunStatus::Paused {
        snapshot.interrupted_run.as_ref().and_then(|run| {
            (run.run_id == reopen_run_id(project_id))
                .then_some(run.stage_cursor.completion_round.saturating_sub(1).max(1))
        })
    } else {
        None
    };

    if let Some(pre_reopen_completion_round) =
        pre_reopen_completion_round.filter(|_| snapshot.amendment_queue.pending.is_empty())
    {
        snapshot.active_run = None;
        snapshot.status = RunStatus::Completed;
        snapshot.interrupted_run = None;
        snapshot.completion_rounds = pre_reopen_completion_round;
        snapshot.status_summary = "completed".to_owned();
        return true;
    }

    false
}

fn reopen_run_id(project_id: &ProjectId) -> String {
    format!("reopen-{}", project_id.as_str())
}

/// Remove a single pending amendment by ID. Updates both run.json and disk.
///
/// Deletes the durable file first, then updates the canonical snapshot. If the
/// file deletion fails, the amendment remains pending everywhere and the command
/// fails cleanly. If the snapshot write fails after a successful file deletion,
/// the amendment file is restored and the command fails.
pub fn remove_amendment(
    amendment_queue: &dyn AmendmentQueuePort,
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    amendment_id: &str,
) -> AppResult<()> {
    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;

    // Reject while a run is actively writing.
    if snapshot.status == RunStatus::Running {
        return Err(AppError::AmendmentLeaseConflict {
            project_id: project_id.to_string(),
        });
    }

    // Verify the amendment exists in canonical snapshot state.
    let amendment = snapshot
        .amendment_queue
        .pending
        .iter()
        .find(|a| a.amendment_id == amendment_id)
        .cloned();
    let amendment = match amendment {
        Some(a) => a,
        None => {
            return Err(AppError::AmendmentNotFound {
                amendment_id: amendment_id.to_owned(),
            });
        }
    };

    // Delete the durable file first. If this fails, the amendment stays pending
    // everywhere and completion will correctly block on it.
    amendment_queue.remove_amendment(base_dir, project_id, amendment_id)?;

    // Update canonical snapshot to reflect the removal.
    snapshot
        .amendment_queue
        .pending
        .retain(|a| a.amendment_id != amendment_id);
    restore_completed_state_after_reopen_if_no_amendments(&mut snapshot, project_id);
    if let Err(snap_err) = run_write_port.write_run_snapshot(base_dir, project_id, &snapshot) {
        // Restore the amendment file so disk and snapshot stay consistent.
        // If restore also fails, return a composite error so the caller knows
        // the amendment file is missing while the snapshot still lists it.
        if let Err(restore_err) = amendment_queue.write_amendment(base_dir, project_id, &amendment)
        {
            return Err(AppError::CorruptRecord {
                file: format!("projects/{}/run.json", project_id.as_str()),
                details: format!(
                    "snapshot write failed after amendment file deletion: {snap_err}; \
                     amendment file restore also failed: {restore_err}"
                ),
            });
        }
        return Err(snap_err);
    }

    Ok(())
}

/// Clear all pending amendments. Returns removed and remaining IDs.
/// On partial failure, reports exactly which amendments were removed and which remain.
///
/// Drives the pending set from canonical snapshot state. Files are deleted
/// first, then the snapshot is updated to reflect only the remaining amendments.
/// If the snapshot write fails after partial deletion, `AmendmentClearPartial`
/// is still returned so the caller always gets the exact removed/remaining IDs.
pub fn clear_amendments(
    amendment_queue: &dyn AmendmentQueuePort,
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<Vec<String>> {
    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;

    // Reject while a run is actively writing.
    if snapshot.status == RunStatus::Running {
        return Err(AppError::AmendmentLeaseConflict {
            project_id: project_id.to_string(),
        });
    }

    let pending: Vec<QueuedAmendment> = std::mem::take(&mut snapshot.amendment_queue.pending);
    if pending.is_empty() {
        if restore_completed_state_after_reopen_if_no_amendments(&mut snapshot, project_id) {
            run_write_port.write_run_snapshot(base_dir, project_id, &snapshot)?;
        }
        return Ok(Vec::new());
    }

    let total = pending.len();

    // Delete files first, track removed/remaining.
    let mut removed = Vec::new();
    let mut remaining = Vec::new();

    for amendment in &pending {
        match amendment_queue.remove_amendment(base_dir, project_id, &amendment.amendment_id) {
            Ok(()) => removed.push(amendment.amendment_id.clone()),
            Err(_) => remaining.push(amendment.amendment_id.clone()),
        }
    }

    // Update canonical snapshot to reflect only the remaining amendments.
    if remaining.is_empty() {
        // All files deleted — clear the snapshot pending queue.
        // snapshot.amendment_queue.pending is already empty from std::mem::take.
        restore_completed_state_after_reopen_if_no_amendments(&mut snapshot, project_id);
        if let Err(snap_err) = run_write_port.write_run_snapshot(base_dir, project_id, &snapshot) {
            // Restore all amendment files so disk and snapshot stay consistent.
            // If any restore fails, return a composite error so the caller knows
            // which files could not be restored.
            let mut restore_failures: Vec<String> = Vec::new();
            for a in &pending {
                if let Err(e) = amendment_queue.write_amendment(base_dir, project_id, a) {
                    restore_failures.push(format!("{}: {e}", a.amendment_id));
                }
            }
            if !restore_failures.is_empty() {
                return Err(AppError::CorruptRecord {
                    file: format!("projects/{}/run.json", project_id.as_str()),
                    details: format!(
                        "snapshot write failed after clearing amendments: {snap_err}; \
                         amendment file restore also failed: {}",
                        restore_failures.join("; ")
                    ),
                });
            }
            return Err(snap_err);
        }
    } else {
        // Partial failure — put remaining amendments back into canonical state.
        let remaining_set: std::collections::HashSet<&str> =
            remaining.iter().map(|s| s.as_str()).collect();
        for a in &pending {
            if remaining_set.contains(a.amendment_id.as_str()) {
                snapshot.amendment_queue.pending.push(a.clone());
            }
        }
        // The snapshot MUST reflect only the remaining amendments before we
        // report partial success. If this repair write fails, restore the
        // deleted files so disk matches the unmodified snapshot and return the
        // underlying I/O error instead of AmendmentClearPartial.
        if let Err(repair_err) = run_write_port.write_run_snapshot(base_dir, project_id, &snapshot)
        {
            // Restore deleted amendment files so disk stays consistent with
            // the unmodified on-disk snapshot. If restore fails, return a
            // composite error with both the repair and restore failures.
            let mut restore_failures: Vec<String> = Vec::new();
            for a in &pending {
                if !remaining_set.contains(a.amendment_id.as_str()) {
                    if let Err(e) = amendment_queue.write_amendment(base_dir, project_id, a) {
                        restore_failures.push(format!("{}: {e}", a.amendment_id));
                    }
                }
            }
            if !restore_failures.is_empty() {
                return Err(AppError::CorruptRecord {
                    file: format!("projects/{}/run.json", project_id.as_str()),
                    details: format!(
                        "snapshot repair write failed after partial clear: {repair_err}; \
                         amendment file restore also failed: {}",
                        restore_failures.join("; ")
                    ),
                });
            }
            return Err(repair_err);
        }

        return Err(AppError::AmendmentClearPartial {
            removed_count: removed.len(),
            total,
            removed,
            remaining,
        });
    }

    Ok(removed)
}

/// Stage a batch of amendments from an automated source (e.g. PR-review).
/// Writes each amendment durably, emits journal events, syncs the canonical
/// snapshot, and reopens the project if it is completed. This is the shared
/// path that both manual and automated amendment intake converge on.
/// Returns the IDs of amendments that were actually staged (after dedup).
#[allow(clippy::too_many_arguments)]
pub fn stage_amendment_batch(
    amendment_queue: &dyn AmendmentQueuePort,
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    journal_port: &dyn JournalStorePort,
    project_store: &dyn ProjectStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    amendments: &[QueuedAmendment],
) -> AppResult<Vec<String>> {
    if amendments.is_empty() {
        return Ok(Vec::new());
    }

    // Prepare the journal sequence number BEFORE any mutations so that the
    // fallible read_journal call cannot fail after canonical state is committed.
    let base_journal_seq = {
        let events = journal_port.read_journal(base_dir, project_id)?;
        journal::last_sequence(&events)
    };

    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    // Save pre-mutation snapshot so we can restore it if journal append fails.
    let old_snapshot = snapshot.clone();
    let mut staged_ids: Vec<String> = Vec::new();
    let mut staged_amendments: Vec<&QueuedAmendment> = Vec::new();

    for amendment in amendments {
        // Dedup check against canonical snapshot pending queue.
        if snapshot
            .amendment_queue
            .pending
            .iter()
            .any(|a| a.dedup_key == amendment.dedup_key)
        {
            continue;
        }

        // Write durable amendment file. If this fails mid-batch, roll back
        // all earlier file writes so no pre-commit files leak.
        if let Err(write_err) = amendment_queue.write_amendment(base_dir, project_id, amendment) {
            let mut cleanup_failures: Vec<String> = Vec::new();
            for id in &staged_ids {
                if let Err(e) = amendment_queue.remove_amendment(base_dir, project_id, id) {
                    cleanup_failures.push(format!("{id}: {e}"));
                }
            }
            if !cleanup_failures.is_empty() {
                return Err(AppError::CorruptRecord {
                    file: format!("projects/{}/run.json", project_id.as_str()),
                    details: format!(
                        "batch file write failed: {write_err}; \
                         amendment file cleanup also failed: {}",
                        cleanup_failures.join("; ")
                    ),
                });
            }
            return Err(write_err);
        }

        // Sync canonical snapshot (in memory — committed below).
        snapshot.amendment_queue.pending.push(amendment.clone());
        staged_ids.push(amendment.amendment_id.clone());
        staged_amendments.push(amendment);
    }

    if staged_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Commit canonical snapshot BEFORE journal events so that a snapshot
    // write failure leaves no orphaned journal entries.
    let was_completed = snapshot.status == RunStatus::Completed;
    let snap_result = if was_completed {
        reopen_completed_project_with_snapshot(
            run_write_port,
            project_store,
            base_dir,
            project_id,
            &mut snapshot,
        )
    } else {
        run_write_port.write_run_snapshot(base_dir, project_id, &snapshot)
    };

    if let Err(snap_err) = snap_result {
        if was_completed {
            // When the project was completed and the reopen/snapshot write
            // fails, amendment files that have already been written to disk
            // must remain. The project snapshot stays at its last committed
            // completed state and the caller must not advance cursors or
            // journal events. On the next poll cycle the daemon will re-
            // fetch, re-deduplicate (files overwrite safely), and retry the
            // reopen.
            return Err(snap_err);
        }
        // For non-completed projects, roll back all amendment files written
        // in this batch so no pre-commit files leak.
        let mut cleanup_failures: Vec<String> = Vec::new();
        for id in &staged_ids {
            if let Err(e) = amendment_queue.remove_amendment(base_dir, project_id, id) {
                cleanup_failures.push(format!("{id}: {e}"));
            }
        }
        if !cleanup_failures.is_empty() {
            return Err(AppError::CorruptRecord {
                file: format!("projects/{}/run.json", project_id.as_str()),
                details: format!(
                    "snapshot/reopen write failed: {snap_err}; \
                     amendment file cleanup also failed: {}",
                    cleanup_failures.join("; ")
                ),
            });
        }
        return Err(snap_err);
    }

    // Pre-serialize all journal events so serialization failures are caught
    // before any appends. This prevents partial journal writes on serialize
    // errors.
    let mut journal_lines: Vec<String> = Vec::new();
    let mut seq = base_journal_seq;
    for amendment in &staged_amendments {
        seq += 1;
        let journal_event = journal::amendment_queued_manual_event(
            seq,
            amendment.created_at,
            &amendment.amendment_id,
            &amendment.body,
            amendment.source.as_str(),
            amendment.source_stage.as_str(),
            &amendment.dedup_key,
        );
        match journal::serialize_event(&journal_event) {
            Ok(line) => journal_lines.push(line),
            Err(ser_err) => {
                // Serialization failed — roll back all staged files and snapshot.
                let snap_result =
                    run_write_port.write_run_snapshot(base_dir, project_id, &old_snapshot);
                let mut file_failures: Vec<String> = Vec::new();
                for id in &staged_ids {
                    if let Err(e) = amendment_queue.remove_amendment(base_dir, project_id, id) {
                        file_failures.push(format!("{id}: {e}"));
                    }
                }
                if snap_result.is_err() || !file_failures.is_empty() {
                    let snap_detail = snap_result
                        .err()
                        .map_or_else(|| "ok".to_owned(), |e| e.to_string());
                    let file_detail = if file_failures.is_empty() {
                        "ok".to_owned()
                    } else {
                        file_failures.join("; ")
                    };
                    return Err(AppError::CorruptRecord {
                        file: format!("projects/{}/run.json", project_id.as_str()),
                        details: format!(
                            "journal event serialization failed: {ser_err}; \
                             rollback also failed — snapshot restore: {snap_detail}, \
                             file cleanup: {file_detail}"
                        ),
                    });
                }
                return Err(ser_err);
            }
        }
    }

    // Durably append all journal events. A successful staging must record all
    // amendment_queued events. If any append fails, roll back the snapshot
    // and amendment files so no amendments are visible without history.
    //
    // Track successful appends so we can detect partial-journal state: if
    // earlier lines are already on disk when a later append fails, the
    // journal has orphaned entries that cannot be un-appended.
    for (appended_count, line) in journal_lines.iter().enumerate() {
        if let Err(journal_err) = journal_port.append_event(base_dir, project_id, line) {
            // Attempt rollback: restore snapshot and remove amendment files.
            let snap_result =
                run_write_port.write_run_snapshot(base_dir, project_id, &old_snapshot);
            let mut file_failures: Vec<String> = Vec::new();
            for id in &staged_ids {
                if let Err(e) = amendment_queue.remove_amendment(base_dir, project_id, id) {
                    file_failures.push(format!("{id}: {e}"));
                }
            }

            let rollback_failed = snap_result.is_err() || !file_failures.is_empty();

            // If earlier journal lines were already appended, canonical state
            // (snapshot + files) was rolled back but the journal still contains
            // orphaned amendment_queued entries. Surface this as an unrecovered
            // consistency failure rather than implying a clean rollback.
            // Similarly, if rollback itself failed, the caller needs to know
            // that canonical state may be inconsistent.
            if appended_count > 0 || rollback_failed {
                let snap_detail = snap_result
                    .err()
                    .map_or_else(|| "ok".to_owned(), |e| e.to_string());
                let file_detail = if file_failures.is_empty() {
                    "ok".to_owned()
                } else {
                    file_failures.join("; ")
                };
                return Err(AppError::CorruptRecord {
                    file: format!("projects/{}/run.json", project_id.as_str()),
                    details: format!(
                        "batch journal append failed after {appended_count} of {} events: \
                         {journal_err}; snapshot restore: {snap_detail}, file cleanup: {file_detail}",
                        journal_lines.len()
                    ),
                });
            }

            return Err(journal_err);
        }
    }

    Ok(staged_ids)
}

/// Reopen a completed project to paused state with an interrupted run pointing
/// at the flow planning stage. Shared between manual and PR-review amendment paths.
///
/// Reads the current snapshot from disk. If the caller already holds a modified
/// snapshot (e.g. with a pending amendment already added), use
/// `reopen_completed_project_with_snapshot` instead.
pub fn reopen_completed_project(
    run_port: &dyn RunSnapshotPort,
    run_write_port: &dyn RunSnapshotWritePort,
    project_store: &dyn ProjectStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
) -> AppResult<()> {
    let mut snapshot = run_port.read_run_snapshot(base_dir, project_id)?;
    reopen_completed_project_with_snapshot(
        run_write_port,
        project_store,
        base_dir,
        project_id,
        &mut snapshot,
    )
}

/// Reopen a completed project using an already-loaded (possibly modified) snapshot.
/// The snapshot is mutated in place and persisted atomically.
pub fn reopen_completed_project_with_snapshot(
    run_write_port: &dyn RunSnapshotWritePort,
    project_store: &dyn ProjectStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    snapshot: &mut RunSnapshot,
) -> AppResult<()> {
    let record = project_store.read_project_record(base_dir, project_id)?;

    if snapshot.status != RunStatus::Completed {
        return Ok(());
    }

    let planning_stage = planning_stage_for_flow(record.flow);
    let current_cycle = snapshot
        .cycle_history
        .last()
        .map(|entry| entry.cycle)
        .unwrap_or(1);
    let completion_round = snapshot.completion_rounds.max(1);

    let project_root = FileSystem::project_root(base_dir, project_id);
    let prompt_path = project_root.join(&record.prompt_reference);
    let prompt_contents =
        std::fs::read_to_string(&prompt_path).map_err(|error| AppError::CorruptRecord {
            file: prompt_path.display().to_string(),
            details: format!("failed to read prompt for project reopen: {error}"),
        })?;
    let prompt_hash = FileSystem::prompt_hash(&prompt_contents);

    // Advance the completion round so the resumed run creates new
    // payload/artifact IDs instead of overwriting the original history.
    let next_completion_round = completion_round + 1;
    snapshot.completion_rounds = next_completion_round;

    snapshot.interrupted_run = Some(ActiveRun {
        run_id: reopen_run_id(project_id),
        stage_cursor: StageCursor::new(planning_stage, current_cycle, 1, next_completion_round)?,
        started_at: Utc::now(),
        prompt_hash_at_cycle_start: prompt_hash.clone(),
        prompt_hash_at_stage_start: prompt_hash,
        qa_iterations_current_cycle: 0,
        review_iterations_current_cycle: 0,
        final_review_restart_count: 0,
        iterative_implementer_state: None,
        stage_resolution_snapshot: snapshot.last_stage_resolution_snapshot.clone(),
    });
    snapshot.active_run = None;
    snapshot.status = RunStatus::Paused;
    snapshot.status_summary = "paused: amendments staged".to_owned();
    run_write_port.write_run_snapshot(base_dir, project_id, snapshot)?;
    Ok(())
}

fn planning_stage_for_flow(flow: FlowPreset) -> StageId {
    match flow {
        FlowPreset::Standard => StageId::Planning,
        FlowPreset::QuickDev => StageId::PlanAndImplement,
        FlowPreset::DocsChange => StageId::DocsPlan,
        FlowPreset::CiImprovement => StageId::CiPlan,
        FlowPreset::Minimal => StageId::PlanAndImplement,
        FlowPreset::IterativeMinimal => StageId::PlanAndImplement,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    use crate::adapters::fs::{
        FsActiveProjectStore, FsArtifactStore, FsJournalStore, FsMilestoneJournalStore,
        FsMilestonePlanStore, FsMilestoneSnapshotStore, FsMilestoneStore, FsProjectStore,
        FsRunSnapshotStore,
    };
    use crate::contexts::milestone_record::bundle::{
        AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
    };
    use crate::contexts::milestone_record::service::materialize_bundle;
    use crate::contexts::project_run_record::model::TaskOrigin;
    use crate::shared::domain::{FlowPreset, ProjectId};

    fn setup_workspace(base: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(base.join(".ralph-burning/milestones"))?;
        std::fs::create_dir_all(base.join(".ralph-burning/projects"))?;
        Ok(())
    }

    fn sample_bundle(id: &str, name: &str) -> MilestoneBundle {
        MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: id.to_owned(),
                name: name.to_owned(),
            },
            executive_summary: "Test plan.".to_owned(),
            goals: vec!["Goal 1".to_owned()],
            non_goals: vec![],
            constraints: vec![],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Tests pass".to_owned(),
                covered_by: vec!["bead-1".to_owned()],
            }],
            workstreams: vec![Workstream {
                name: "Core".to_owned(),
                description: Some("Fixture description.".to_owned()),
                beads: vec![BeadProposal {
                    bead_id: None,
                    explicit_id: None,
                    title: "Implement feature".to_owned(),
                    description: Some("Fixture description.".to_owned()),
                    bead_type: Some("task".to_owned()),
                    priority: Some(1),
                    labels: vec!["fixture".to_owned()],
                    depends_on: vec![],
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: FlowPreset::Minimal,
            agents_guidance: None,
        }
    }

    fn hash_text(content: &str) -> String {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(content.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    #[test]
    fn show_project_omits_stale_task_plan_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base)?;
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 14, 0, 0)
            .single()
            .unwrap();
        let milestone = materialize_bundle(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            &sample_bundle("ms-alpha", "Alpha"),
            now,
        )?;

        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: ProjectId::new("task-alpha")?,
                name: "Task Alpha".to_owned(),
                flow: FlowPreset::QuickDev,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: "hash".to_owned(),
                created_at: now,
                task_source: Some(TaskSource {
                    milestone_id: milestone.id.to_string(),
                    bead_id: "bead-1".to_owned(),
                    parent_epic_id: None,
                    origin: TaskOrigin::Milestone,
                    plan_hash: Some("stale-plan-hash".to_owned()),
                    plan_version: Some(1),
                }),
            },
        )?;

        let detail = show_project(
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsJournalStore,
            &FsActiveProjectStore,
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            base,
            &ProjectId::new("task-alpha")?,
        )?;

        let lineage = detail
            .task_lineage
            .expect("milestone lineage should still exist");
        assert_eq!(lineage.milestone_id, "ms-alpha");
        assert_eq!(lineage.milestone_name, "Alpha");
        assert_eq!(lineage.bead_id, "bead-1");
        assert_eq!(lineage.bead_title, None);
        assert!(lineage.acceptance_criteria.is_empty());
        Ok(())
    }

    #[test]
    fn show_project_omits_plan_metadata_when_task_source_has_no_plan_hash(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base)?;
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 14, 15, 0)
            .single()
            .unwrap();
        let milestone = materialize_bundle(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            &sample_bundle("ms-alpha", "Alpha"),
            now,
        )?;

        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: ProjectId::new("task-alpha")?,
                name: "Task Alpha".to_owned(),
                flow: FlowPreset::QuickDev,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: "hash".to_owned(),
                created_at: now,
                task_source: Some(TaskSource {
                    milestone_id: milestone.id.to_string(),
                    bead_id: "bead-1".to_owned(),
                    parent_epic_id: None,
                    origin: TaskOrigin::Milestone,
                    plan_hash: None,
                    plan_version: Some(1),
                }),
            },
        )?;

        let detail = show_project(
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsJournalStore,
            &FsActiveProjectStore,
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            base,
            &ProjectId::new("task-alpha")?,
        )?;

        let lineage = detail
            .task_lineage
            .expect("milestone lineage should still exist");
        assert_eq!(lineage.milestone_id, "ms-alpha");
        assert_eq!(lineage.milestone_name, "Alpha");
        assert_eq!(lineage.bead_id, "bead-1");
        assert_eq!(lineage.bead_title, None);
        assert!(lineage.acceptance_criteria.is_empty());
        Ok(())
    }

    #[test]
    fn show_project_preserves_fallback_lineage_when_plan_is_missing(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base)?;
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 14, 30, 0)
            .single()
            .unwrap();
        let milestone = materialize_bundle(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            &sample_bundle("ms-alpha", "Alpha"),
            now,
        )?;
        let current_plan_hash =
            hash_text(&FsMilestonePlanStore.read_plan_json(base, &milestone.id)?);

        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: ProjectId::new("task-alpha")?,
                name: "Task Alpha".to_owned(),
                flow: FlowPreset::QuickDev,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: "hash".to_owned(),
                created_at: now,
                task_source: Some(TaskSource {
                    milestone_id: milestone.id.to_string(),
                    bead_id: "bead-1".to_owned(),
                    parent_epic_id: None,
                    origin: TaskOrigin::Milestone,
                    plan_hash: Some(current_plan_hash),
                    plan_version: Some(1),
                }),
            },
        )?;

        std::fs::remove_file(FileSystem::milestone_root(base, &milestone.id).join("plan.json"))?;

        let detail = show_project(
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsJournalStore,
            &FsActiveProjectStore,
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            base,
            &ProjectId::new("task-alpha")?,
        )?;

        assert_eq!(detail.record.task_source.unwrap().milestone_id, "ms-alpha");
        let lineage = detail
            .task_lineage
            .expect("milestone lineage should fall back without plan metadata");
        assert_eq!(lineage.milestone_id, "ms-alpha");
        assert_eq!(lineage.milestone_name, "Alpha");
        assert_eq!(lineage.bead_id, "bead-1");
        assert_eq!(lineage.bead_title, None);
        assert!(lineage.acceptance_criteria.is_empty());
        Ok(())
    }

    #[test]
    fn show_project_preserves_fallback_lineage_when_plan_is_corrupt(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base)?;
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 15, 0, 0)
            .single()
            .unwrap();
        let milestone = materialize_bundle(
            &FsMilestoneStore,
            &FsMilestoneSnapshotStore,
            &FsMilestoneJournalStore,
            &FsMilestonePlanStore,
            base,
            &sample_bundle("ms-alpha", "Alpha"),
            now,
        )?;
        let current_plan_hash =
            hash_text(&FsMilestonePlanStore.read_plan_json(base, &milestone.id)?);

        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: ProjectId::new("task-alpha")?,
                name: "Task Alpha".to_owned(),
                flow: FlowPreset::QuickDev,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: "hash".to_owned(),
                created_at: now,
                task_source: Some(TaskSource {
                    milestone_id: milestone.id.to_string(),
                    bead_id: "bead-1".to_owned(),
                    parent_epic_id: None,
                    origin: TaskOrigin::Milestone,
                    plan_hash: Some(current_plan_hash),
                    plan_version: Some(1),
                }),
            },
        )?;

        std::fs::write(
            FileSystem::milestone_root(base, &milestone.id).join("plan.json"),
            "{not valid json",
        )?;

        let detail = show_project(
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsJournalStore,
            &FsActiveProjectStore,
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            base,
            &ProjectId::new("task-alpha")?,
        )?;

        assert_eq!(detail.record.task_source.unwrap().milestone_id, "ms-alpha");
        let lineage = detail
            .task_lineage
            .expect("milestone lineage should fall back when plan is corrupt");
        assert_eq!(lineage.milestone_id, "ms-alpha");
        assert_eq!(lineage.milestone_name, "Alpha");
        assert_eq!(lineage.bead_id, "bead-1");
        assert_eq!(lineage.bead_title, None);
        assert!(lineage.acceptance_criteria.is_empty());
        Ok(())
    }

    #[test]
    fn show_project_preserves_fallback_lineage_when_milestone_is_missing(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base)?;
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 15, 15, 0)
            .single()
            .unwrap();

        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: ProjectId::new("task-alpha")?,
                name: "Task Alpha".to_owned(),
                flow: FlowPreset::QuickDev,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: "hash".to_owned(),
                created_at: now,
                task_source: Some(TaskSource {
                    milestone_id: "ms-missing".to_owned(),
                    bead_id: "bead-1".to_owned(),
                    parent_epic_id: None,
                    origin: TaskOrigin::Milestone,
                    plan_hash: Some("plan-hash".to_owned()),
                    plan_version: Some(1),
                }),
            },
        )?;

        let detail = show_project(
            &FsProjectStore,
            &FsRunSnapshotStore,
            &FsJournalStore,
            &FsActiveProjectStore,
            &FsMilestoneStore,
            &FsMilestonePlanStore,
            base,
            &ProjectId::new("task-alpha")?,
        )?;

        assert_eq!(
            detail.record.task_source.unwrap().milestone_id,
            "ms-missing"
        );
        let lineage = detail
            .task_lineage
            .expect("task lineage should preserve durable milestone/bead ids");
        assert_eq!(lineage.milestone_id, "ms-missing");
        assert_eq!(lineage.milestone_name, "<milestone metadata unavailable>");
        assert_eq!(lineage.bead_id, "bead-1");
        assert_eq!(lineage.bead_title, None);
        assert!(lineage.acceptance_criteria.is_empty());
        Ok(())
    }

    #[test]
    fn run_history_falls_back_to_task_source_lineage_when_journal_lacks_it(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let base = tmp.path();
        setup_workspace(base)?;
        let now = Utc
            .with_ymd_and_hms(2026, 4, 15, 15, 30, 0)
            .single()
            .unwrap();

        create_project(
            &FsProjectStore,
            &FsJournalStore,
            base,
            CreateProjectInput {
                id: ProjectId::new("task-alpha")?,
                name: "Task Alpha".to_owned(),
                flow: FlowPreset::QuickDev,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt".to_owned(),
                prompt_hash: "hash".to_owned(),
                created_at: now,
                task_source: Some(TaskSource {
                    milestone_id: "ms-alpha".to_owned(),
                    bead_id: "bead-1".to_owned(),
                    parent_epic_id: None,
                    origin: TaskOrigin::Milestone,
                    plan_hash: Some("plan-hash".to_owned()),
                    plan_version: Some(1),
                }),
            },
        )?;

        let journal_path = base
            .join(".ralph-burning/projects/task-alpha")
            .join("journal.ndjson");
        let journal = std::fs::read_to_string(&journal_path)?;
        let journal = journal
            .replace(r#","milestone_id":"ms-alpha""#, "")
            .replace(r#","bead_id":"bead-1""#, "")
            .replace(r#","origin":"milestone""#, "")
            .replace(r#","plan_hash":"plan-hash""#, "")
            .replace(r#","plan_version":1"#, "");
        std::fs::write(&journal_path, journal)?;

        let history = run_history(
            &FsProjectStore,
            &FsJournalStore,
            &FsArtifactStore,
            base,
            &ProjectId::new("task-alpha")?,
        )?;

        assert_eq!(history.milestone_id.as_deref(), Some("ms-alpha"));
        assert_eq!(history.bead_id.as_deref(), Some("bead-1"));
        Ok(())
    }
}
