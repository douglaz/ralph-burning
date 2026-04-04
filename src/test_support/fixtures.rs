//! Builder-based test fixtures for workspaces, milestones, and bead graphs.
//!
//! # Examples
//!
//! ```ignore
//! use ralph_burning::shared::domain::FlowPreset;
//! use ralph_burning::test_support::fixtures::{
//!     MilestoneFixtureBuilder, TaskRunFixture, TempWorkspaceBuilder,
//! };
//!
//! let workspace = TempWorkspaceBuilder::new()
//!     .with_milestone(
//!         MilestoneFixtureBuilder::new("ms-alpha")
//!             .with_name("Alpha milestone")
//!             .add_bead("Implement shared support")
//!             .with_task_run(TaskRunFixture::succeeded(
//!                 "ms-alpha.bead-1",
//!                 "project-alpha",
//!                 "run-1",
//!             )),
//!     )
//!     .build()
//!     .expect("workspace fixture");
//!
//! assert!(workspace.audit_root().join("milestones/ms-alpha/plan.json").is_file());
//! assert!(workspace.beads_root().join("issues.jsonl").is_file());
//! ```

use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use tempfile::{tempdir, TempDir};

use crate::adapters::br_models::{BeadPriority, BeadStatus, BeadType, DependencyKind};
use crate::adapters::fs::{
    FileSystem, FsMilestoneJournalStore, FsMilestonePlanStore, FsMilestoneSnapshotStore,
    FsMilestoneStore, FsTaskRunLineageStore,
};
use crate::contexts::milestone_record::bundle::{
    AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
};
use crate::contexts::milestone_record::model::{
    MilestoneId, MilestoneJournalEvent, MilestoneRecord, MilestoneSnapshot, MilestoneStatus,
    TaskRunEntry, TaskRunOutcome,
};
use crate::contexts::milestone_record::service::{
    create_milestone, load_snapshot, materialize_bundle, persist_plan, read_journal,
    read_task_runs, record_bead_completion, record_bead_start, update_status, CreateMilestoneInput,
};
use crate::contexts::workspace_governance::initialize_workspace;
use crate::shared::domain::FlowPreset;
use crate::shared::error::AppResult;

fn fixture_timestamp() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 3, 19, 3, 28, 0)
        .single()
        .expect("valid fixture timestamp")
}

fn fixture_actor() -> String {
    "fixture".to_owned()
}

fn serialize_acceptance_criteria(items: &[String]) -> Option<String> {
    if items.is_empty() {
        None
    } else if items.len() == 1 {
        Some(items[0].clone())
    } else {
        Some(
            items
                .iter()
                .map(|item| format!("- {item}"))
                .collect::<Vec<_>>()
                .join("\n"),
        )
    }
}

fn canonical_bead_reference(milestone_id: &str, value: &str) -> String {
    if value.contains('.') {
        value.to_owned()
    } else {
        format!("{milestone_id}.{value}")
    }
}

fn bead_type_from_name(value: Option<&str>) -> BeadType {
    match value.unwrap_or("task") {
        "task" => BeadType::Task,
        "bug" => BeadType::Bug,
        "feature" => BeadType::Feature,
        "epic" => BeadType::Epic,
        "chore" => BeadType::Chore,
        "docs" => BeadType::Docs,
        "question" => BeadType::Question,
        "spike" => BeadType::Spike,
        "meta" => BeadType::Meta,
        other => BeadType::Other(other.to_owned()),
    }
}

/// Typed bead fixture metadata that writes the real `.beads/issues.jsonl` schema.
#[derive(Debug, Clone, PartialEq)]
pub struct BeadGraphIssue {
    pub id: String,
    pub title: String,
    pub status: BeadStatus,
    pub priority: BeadPriority,
    pub bead_type: BeadType,
    pub labels: Vec<String>,
    pub description: Option<String>,
    pub acceptance_criteria: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
    pub updated_at: DateTime<Utc>,
    pub source_repo: String,
    pub compaction_level: u32,
    pub original_size: u64,
    pub dependencies: Vec<BeadGraphDependency>,
    pub closed_at: Option<DateTime<Utc>>,
    pub close_reason: Option<String>,
}

/// Typed dependency row metadata for `.beads/issues.jsonl`.
#[derive(Debug, Clone, PartialEq)]
pub struct BeadGraphDependency {
    pub issue_id: String,
    pub depends_on_id: String,
    pub kind: DependencyKind,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
    pub metadata: String,
    pub thread_id: String,
}

impl BeadGraphDependency {
    /// Create a dependency row with the same defaults emitted by `br`.
    pub fn new(
        issue_id: impl Into<String>,
        depends_on_id: impl Into<String>,
        kind: DependencyKind,
        created_at: DateTime<Utc>,
        created_by: impl Into<String>,
    ) -> Self {
        Self {
            issue_id: issue_id.into(),
            depends_on_id: depends_on_id.into(),
            kind,
            created_at,
            created_by: created_by.into(),
            metadata: "{}".to_owned(),
            thread_id: String::new(),
        }
    }
}

impl BeadGraphIssue {
    /// Create a realistic open task bead fixture.
    pub fn open_task(id: impl Into<String>, title: impl Into<String>) -> Self {
        let created_at = fixture_timestamp();
        Self {
            id: id.into(),
            title: title.into(),
            status: BeadStatus::Open,
            priority: BeadPriority::new(2),
            bead_type: BeadType::Task,
            labels: Vec::new(),
            description: None,
            acceptance_criteria: Vec::new(),
            created_at,
            created_by: fixture_actor(),
            updated_at: created_at,
            source_repo: ".".to_owned(),
            compaction_level: 0,
            original_size: 0,
            dependencies: Vec::new(),
            closed_at: None,
            close_reason: None,
        }
    }

    /// Replace the acceptance criteria with structured lines.
    pub fn with_acceptance_criteria(
        mut self,
        criteria: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.acceptance_criteria = criteria.into_iter().map(Into::into).collect();
        self
    }

    /// Add a dependency row using the current issue metadata defaults.
    pub fn add_dependency(
        mut self,
        depends_on_id: impl Into<String>,
        kind: DependencyKind,
    ) -> Self {
        self.dependencies.push(BeadGraphDependency::new(
            self.id.clone(),
            depends_on_id,
            kind,
            self.updated_at,
            self.created_by.clone(),
        ));
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SerializedBeadGraphIssue {
    id: String,
    title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    acceptance_criteria: Option<String>,
    status: BeadStatus,
    priority: BeadPriority,
    #[serde(rename = "issue_type")]
    bead_type: BeadType,
    created_at: DateTime<Utc>,
    created_by: String,
    updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    closed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    close_reason: Option<String>,
    source_repo: String,
    compaction_level: u32,
    original_size: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    dependencies: Vec<SerializedBeadGraphDependency>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SerializedBeadGraphDependency {
    issue_id: String,
    depends_on_id: String,
    #[serde(rename = "type")]
    kind: DependencyKind,
    created_at: DateTime<Utc>,
    created_by: String,
    metadata: String,
    thread_id: String,
}

impl From<&BeadGraphIssue> for SerializedBeadGraphIssue {
    fn from(issue: &BeadGraphIssue) -> Self {
        Self {
            id: issue.id.clone(),
            title: issue.title.clone(),
            description: issue.description.clone(),
            acceptance_criteria: serialize_acceptance_criteria(&issue.acceptance_criteria),
            status: issue.status.clone(),
            priority: issue.priority.clone(),
            bead_type: issue.bead_type.clone(),
            created_at: issue.created_at,
            created_by: issue.created_by.clone(),
            updated_at: issue.updated_at,
            closed_at: issue.closed_at,
            close_reason: issue.close_reason.clone(),
            source_repo: issue.source_repo.clone(),
            compaction_level: issue.compaction_level,
            original_size: issue.original_size,
            labels: issue.labels.clone(),
            dependencies: issue
                .dependencies
                .iter()
                .map(SerializedBeadGraphDependency::from)
                .collect(),
        }
    }
}

impl From<&BeadGraphDependency> for SerializedBeadGraphDependency {
    fn from(dependency: &BeadGraphDependency) -> Self {
        Self {
            issue_id: dependency.issue_id.clone(),
            depends_on_id: dependency.depends_on_id.clone(),
            kind: dependency.kind.clone(),
            created_at: dependency.created_at,
            created_by: dependency.created_by.clone(),
            metadata: dependency.metadata.clone(),
            thread_id: dependency.thread_id.clone(),
        }
    }
}

/// Builder for `.beads/issues.jsonl` fixture state.
#[derive(Debug, Clone, Default)]
pub struct BeadGraphFixtureBuilder {
    issues: Vec<BeadGraphIssue>,
}

impl BeadGraphFixtureBuilder {
    /// Create an empty bead graph fixture builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a single issue record.
    pub fn with_issue(mut self, issue: BeadGraphIssue) -> Self {
        self.issues.push(issue);
        self
    }

    /// Build issue records from a milestone bundle using canonical bead ids.
    pub fn from_bundle(bundle: &MilestoneBundle) -> Self {
        let mut issues = Vec::new();
        let mut next_implicit_bead = 1usize;
        let acceptance_map = bundle
            .acceptance_map
            .iter()
            .map(|criterion| (criterion.id.as_str(), criterion.description.as_str()))
            .collect::<std::collections::HashMap<_, _>>();
        for workstream in &bundle.workstreams {
            for bead in &workstream.beads {
                let implicit_id = format!("{}.bead-{}", bundle.identity.id, next_implicit_bead);
                next_implicit_bead += 1;
                let bead_id = bead
                    .bead_id
                    .as_deref()
                    .map(|value| canonical_bead_reference(&bundle.identity.id, value))
                    .unwrap_or(implicit_id);
                let created_at = fixture_timestamp();
                let created_by = fixture_actor();
                let dependencies = bead
                    .depends_on
                    .iter()
                    .map(|dependency| {
                        BeadGraphDependency::new(
                            bead_id.clone(),
                            canonical_bead_reference(&bundle.identity.id, dependency),
                            DependencyKind::Blocks,
                            created_at,
                            created_by.clone(),
                        )
                    })
                    .collect::<Vec<_>>();
                issues.push(BeadGraphIssue {
                    id: bead_id,
                    title: bead.title.clone(),
                    status: BeadStatus::Open,
                    priority: BeadPriority::new(bead.priority.unwrap_or(2)),
                    bead_type: bead_type_from_name(bead.bead_type.as_deref()),
                    labels: bead.labels.clone(),
                    description: bead.description.clone(),
                    acceptance_criteria: bead
                        .acceptance_criteria
                        .iter()
                        .map(|criterion_id| {
                            acceptance_map
                                .get(criterion_id.as_str())
                                .copied()
                                .unwrap_or(criterion_id.as_str())
                                .to_owned()
                        })
                        .collect(),
                    created_at,
                    created_by,
                    updated_at: created_at,
                    source_repo: ".".to_owned(),
                    compaction_level: 0,
                    original_size: 0,
                    dependencies,
                    closed_at: None,
                    close_reason: None,
                });
            }
        }
        Self { issues }
    }

    fn write_into(&self, base_dir: &Path) -> AppResult<BeadGraphFixture> {
        let beads_root = base_dir.join(".beads");
        fs::create_dir_all(&beads_root)?;
        let issues_path = beads_root.join("issues.jsonl");
        let mut content = String::new();
        for issue in &self.issues {
            let record = SerializedBeadGraphIssue::from(issue);
            content.push_str(&serde_json::to_string(&record)?);
            content.push('\n');
        }
        FileSystem::write_atomic(&issues_path, &content)?;
        Ok(BeadGraphFixture {
            root: beads_root,
            issues_path,
            issues: self.issues.clone(),
        })
    }
}

/// Result of writing a `.beads` graph fixture.
#[derive(Debug, Clone)]
pub struct BeadGraphFixture {
    pub root: PathBuf,
    pub issues_path: PathBuf,
    pub issues: Vec<BeadGraphIssue>,
}

/// A task-run fixture spec that replays realistic milestone task lineage.
#[derive(Debug, Clone)]
pub struct TaskRunFixture {
    pub bead_id: String,
    pub project_id: String,
    pub run_id: String,
    pub outcome: TaskRunOutcome,
    pub outcome_detail: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl TaskRunFixture {
    /// Create a running attempt.
    pub fn running(
        bead_id: impl Into<String>,
        project_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Self {
        Self {
            bead_id: bead_id.into(),
            project_id: project_id.into(),
            run_id: run_id.into(),
            outcome: TaskRunOutcome::Running,
            outcome_detail: None,
            started_at: fixture_timestamp() + Duration::minutes(5),
            finished_at: None,
        }
    }

    /// Create a successful completed attempt.
    pub fn succeeded(
        bead_id: impl Into<String>,
        project_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Self {
        let started_at = fixture_timestamp() + Duration::minutes(5);
        Self {
            bead_id: bead_id.into(),
            project_id: project_id.into(),
            run_id: run_id.into(),
            outcome: TaskRunOutcome::Succeeded,
            outcome_detail: Some("completed by fixture".to_owned()),
            started_at,
            finished_at: Some(started_at + Duration::minutes(2)),
        }
    }
}

/// Builder for realistic milestone state persisted on disk.
#[derive(Debug, Clone)]
pub struct MilestoneFixtureBuilder {
    id: String,
    name: String,
    executive_summary: String,
    goals: Vec<String>,
    acceptance_map: Vec<AcceptanceCriterion>,
    workstreams: Vec<Workstream>,
    default_flow: FlowPreset,
    status: MilestoneStatus,
    created_at: DateTime<Utc>,
    task_runs: Vec<TaskRunFixture>,
    extra_journal_events: Vec<MilestoneJournalEvent>,
}

impl MilestoneFixtureBuilder {
    /// Create a minimally valid milestone fixture with one acceptance criterion
    /// and one bead.
    pub fn new(id: impl Into<String>) -> Self {
        let id = id.into();
        let bead_id = format!("{id}.bead-1");
        Self {
            id: id.clone(),
            name: format!("Fixture {id}"),
            executive_summary: format!("Deliver fixture milestone {id}."),
            goals: vec!["Keep milestone test fixtures realistic.".to_owned()],
            acceptance_map: vec![AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Fixture bead is materialized".to_owned(),
                covered_by: vec![bead_id.clone()],
            }],
            workstreams: vec![Workstream {
                name: "Fixture workstream".to_owned(),
                description: Some("Shared test fixture workstream".to_owned()),
                beads: vec![BeadProposal {
                    bead_id: Some(bead_id),
                    explicit_id: Some(true),
                    title: "Bootstrap fixture bead".to_owned(),
                    description: Some("Create the default milestone bead fixture.".to_owned()),
                    bead_type: Some("task".to_owned()),
                    priority: Some(2),
                    labels: vec!["tests".to_owned(), "fixtures".to_owned()],
                    depends_on: Vec::new(),
                    acceptance_criteria: vec!["AC-1".to_owned()],
                    flow_override: None,
                }],
            }],
            default_flow: FlowPreset::QuickDev,
            status: MilestoneStatus::Ready,
            created_at: fixture_timestamp(),
            task_runs: Vec::new(),
            extra_journal_events: Vec::new(),
        }
    }

    /// Override the human-readable milestone name.
    pub fn with_name(mut self, value: impl Into<String>) -> Self {
        self.name = value.into();
        self
    }

    /// Override the milestone summary.
    pub fn with_executive_summary(mut self, value: impl Into<String>) -> Self {
        self.executive_summary = value.into();
        self
    }

    /// Replace the default goals.
    pub fn with_goals(mut self, goals: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.goals = goals.into_iter().map(Into::into).collect();
        self
    }

    /// Add a bead to the first workstream and wire it to `AC-1`.
    pub fn add_bead(mut self, title: impl Into<String>) -> Self {
        let next_index = self.workstreams[0].beads.len() + 1;
        let bead_id = format!("{}.bead-{}", self.id, next_index);
        self.acceptance_map[0].covered_by.push(bead_id.clone());
        self.workstreams[0].beads.push(BeadProposal {
            bead_id: Some(bead_id),
            explicit_id: Some(true),
            title: title.into(),
            description: Some("Additional fixture bead".to_owned()),
            bead_type: Some("task".to_owned()),
            priority: Some(2),
            labels: vec!["tests".to_owned()],
            depends_on: Vec::new(),
            acceptance_criteria: vec!["AC-1".to_owned()],
            flow_override: None,
        });
        self
    }

    /// Override the resulting milestone status after fixture materialization.
    pub fn with_status(mut self, status: MilestoneStatus) -> Self {
        self.status = status;
        self
    }

    /// Add a realistic task-run fixture that updates lineage, journal, and snapshot state.
    pub fn with_task_run(mut self, task_run: TaskRunFixture) -> Self {
        self.task_runs.push(task_run);
        self
    }

    /// Append an extra journal event after the core milestone state has been built.
    pub fn with_journal_event(mut self, event: MilestoneJournalEvent) -> Self {
        self.extra_journal_events.push(event);
        self
    }

    /// Build the typed milestone bundle for reuse in other fixtures.
    pub fn bundle(&self) -> MilestoneBundle {
        MilestoneBundle {
            schema_version: 1,
            identity: MilestoneIdentity {
                id: self.id.clone(),
                name: self.name.clone(),
            },
            executive_summary: self.executive_summary.clone(),
            goals: self.goals.clone(),
            non_goals: Vec::new(),
            constraints: Vec::new(),
            acceptance_map: self.acceptance_map.clone(),
            workstreams: self.workstreams.clone(),
            default_flow: self.default_flow,
            agents_guidance: None,
        }
    }

    fn write_into(&self, base_dir: &Path) -> AppResult<MilestoneFixture> {
        let store = FsMilestoneStore;
        let snapshot_store = FsMilestoneSnapshotStore;
        let journal_store = FsMilestoneJournalStore;
        let plan_store = FsMilestonePlanStore;
        let lineage_store = FsTaskRunLineageStore;
        let bundle = self.bundle();

        let record = if self.status == MilestoneStatus::Planning {
            let record = create_milestone(
                &store,
                base_dir,
                CreateMilestoneInput {
                    id: self.id.clone(),
                    name: self.name.clone(),
                    description: self.executive_summary.clone(),
                },
                self.created_at,
            )?;
            persist_plan(
                &snapshot_store,
                &journal_store,
                &plan_store,
                base_dir,
                &record.id,
                &bundle,
                self.created_at,
            )?;
            record
        } else {
            materialize_bundle(
                &store,
                &snapshot_store,
                &journal_store,
                &plan_store,
                base_dir,
                &bundle,
                self.created_at,
            )?
        };

        let milestone_id = record.id.clone();
        let mut snapshot = load_snapshot(&snapshot_store, base_dir, &milestone_id)?;
        let plan_hash = snapshot
            .plan_hash
            .clone()
            .expect("fixture milestones always persist a plan hash");

        for task_run in &self.task_runs {
            record_bead_start(
                &snapshot_store,
                &journal_store,
                &lineage_store,
                base_dir,
                &milestone_id,
                &task_run.bead_id,
                &task_run.project_id,
                &task_run.run_id,
                &plan_hash,
                task_run.started_at,
            )?;

            if task_run.outcome.is_terminal() {
                record_bead_completion(
                    &snapshot_store,
                    &journal_store,
                    &lineage_store,
                    base_dir,
                    &milestone_id,
                    &task_run.bead_id,
                    &task_run.project_id,
                    &task_run.run_id,
                    Some(&plan_hash),
                    task_run.outcome,
                    task_run.outcome_detail.as_deref(),
                    task_run.started_at,
                    task_run
                        .finished_at
                        .unwrap_or(task_run.started_at + Duration::minutes(1)),
                )?;
            }
        }

        for event in &self.extra_journal_events {
            let line = event.to_ndjson_line()?;
            crate::contexts::milestone_record::MilestoneJournalPort::append_event(
                &journal_store,
                base_dir,
                &milestone_id,
                &line,
            )?;
        }

        snapshot = load_snapshot(&snapshot_store, base_dir, &milestone_id)?;
        if snapshot.status != self.status {
            snapshot = update_status(
                &snapshot_store,
                &journal_store,
                base_dir,
                &milestone_id,
                self.status,
                snapshot.updated_at + Duration::seconds(1),
            )?;
        }

        Ok(MilestoneFixture {
            milestone_id,
            bundle,
            record,
            snapshot,
            root: FileSystem::audit_workspace_root_path(base_dir)
                .join("milestones")
                .join(self.id.clone()),
            journal: read_journal(
                &journal_store,
                base_dir,
                &MilestoneId::new(self.id.clone())?,
            )?,
            task_runs: read_task_runs(
                &lineage_store,
                base_dir,
                &MilestoneId::new(self.id.clone())?,
            )?,
        })
    }
}

/// Result of writing a milestone fixture to disk.
#[derive(Debug, Clone)]
pub struct MilestoneFixture {
    pub milestone_id: MilestoneId,
    pub bundle: MilestoneBundle,
    pub record: MilestoneRecord,
    pub snapshot: MilestoneSnapshot,
    pub root: PathBuf,
    pub journal: Vec<MilestoneJournalEvent>,
    pub task_runs: Vec<TaskRunEntry>,
}

/// Built temp workspace plus any generated milestone and bead fixtures.
#[derive(Debug)]
pub struct TempWorkspace {
    temp_dir: TempDir,
    pub milestones: Vec<MilestoneFixture>,
    pub bead_graph: BeadGraphFixture,
}

impl TempWorkspace {
    /// Return the temporary workspace base path.
    pub fn path(&self) -> &Path {
        self.temp_dir.path()
    }

    /// Return the audit workspace root (`.ralph-burning`).
    pub fn audit_root(&self) -> PathBuf {
        FileSystem::audit_workspace_root_path(self.path())
    }

    /// Return the live workspace root (`ralph-burning-live`).
    pub fn live_root(&self) -> PathBuf {
        FileSystem::live_workspace_root_path(self.path())
    }

    /// Return the `.beads` root.
    pub fn beads_root(&self) -> PathBuf {
        self.path().join(".beads")
    }

    /// Consume the workspace wrapper and return the underlying `TempDir`.
    pub fn into_temp_dir(self) -> TempDir {
        self.temp_dir
    }
}

/// Builder for realistic temp workspaces with `.ralph-burning` and `.beads`.
#[derive(Debug, Clone)]
pub struct TempWorkspaceBuilder {
    created_at: DateTime<Utc>,
    milestones: Vec<MilestoneFixtureBuilder>,
    bead_graph: Option<BeadGraphFixtureBuilder>,
}

impl Default for TempWorkspaceBuilder {
    fn default() -> Self {
        Self {
            created_at: fixture_timestamp(),
            milestones: Vec::new(),
            bead_graph: None,
        }
    }
}

impl TempWorkspaceBuilder {
    /// Create a new empty workspace builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a milestone fixture to materialize into the workspace.
    pub fn with_milestone(mut self, milestone: MilestoneFixtureBuilder) -> Self {
        self.milestones.push(milestone);
        self
    }

    /// Override the `.beads` graph builder. When omitted, the graph is derived
    /// from any configured milestone bundles.
    pub fn with_bead_graph(mut self, bead_graph: BeadGraphFixtureBuilder) -> Self {
        self.bead_graph = Some(bead_graph);
        self
    }

    /// Build the complete temp workspace fixture.
    pub fn build(self) -> AppResult<TempWorkspace> {
        let temp_dir = tempdir()?;
        initialize_workspace(temp_dir.path(), self.created_at)?;

        let mut milestones = Vec::new();
        for milestone in &self.milestones {
            milestones.push(milestone.write_into(temp_dir.path())?);
        }

        let bead_graph = match self.bead_graph {
            Some(builder) => builder.write_into(temp_dir.path())?,
            None => {
                let mut builder = BeadGraphFixtureBuilder::new();
                for milestone in &self.milestones {
                    for issue in BeadGraphFixtureBuilder::from_bundle(&milestone.bundle()).issues {
                        builder = builder.with_issue(issue);
                    }
                }
                builder.write_into(temp_dir.path())?
            }
        };

        Ok(TempWorkspace {
            temp_dir,
            milestones,
            bead_graph,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn bead_graph_builder_writes_realistic_jsonl() {
        let workspace = TempWorkspaceBuilder::new()
            .with_bead_graph(
                BeadGraphFixtureBuilder::new().with_issue(
                    BeadGraphIssue::open_task("ms-alpha.bead-1", "Shared fixture")
                        .with_acceptance_criteria(["Ship the shared fixture"])
                        .add_dependency("ms-alpha.bead-0", DependencyKind::Blocks),
                ),
            )
            .build()
            .expect("workspace fixture");

        assert!(workspace.audit_root().join("workspace.toml").is_file());
        assert!(workspace.live_root().join("workspace.toml").is_file());
        assert!(workspace.beads_root().join("issues.jsonl").is_file());
        assert_eq!(workspace.bead_graph.issues.len(), 1);

        let raw = fs::read_to_string(workspace.beads_root().join("issues.jsonl"))
            .expect("read bead fixture");
        let issue: Value = serde_json::from_str(raw.lines().next().expect("fixture line"))
            .expect("parse issue json");
        assert_eq!(issue["created_by"], "fixture");
        assert_eq!(issue["source_repo"], ".");
        assert_eq!(issue["compaction_level"], 0);
        assert_eq!(issue["original_size"], 0);
        assert_eq!(issue["acceptance_criteria"], "Ship the shared fixture");
        assert_eq!(issue["dependencies"][0]["issue_id"], "ms-alpha.bead-1");
        assert_eq!(issue["dependencies"][0]["depends_on_id"], "ms-alpha.bead-0");
        assert_eq!(issue["dependencies"][0]["type"], "blocks");
        assert_eq!(issue["dependencies"][0]["metadata"], "{}");
    }

    #[test]
    fn milestone_fixture_builder_materializes_bundle_and_task_runs() {
        let workspace = TempWorkspaceBuilder::new()
            .with_milestone(
                MilestoneFixtureBuilder::new("ms-alpha")
                    .add_bead("Verify captured logs")
                    .with_task_run(TaskRunFixture::succeeded(
                        "ms-alpha.bead-1",
                        "alpha-project",
                        "run-1",
                    )),
            )
            .build()
            .expect("workspace fixture");

        let milestone = &workspace.milestones[0];
        assert!(milestone.root.join("plan.json").is_file());
        assert_eq!(milestone.snapshot.progress.total_beads, 2);
        assert_eq!(milestone.task_runs.len(), 1);
    }

    #[test]
    fn bead_graph_builder_from_bundle_uses_issue_wire_schema() {
        let workspace = TempWorkspaceBuilder::new()
            .with_milestone(MilestoneFixtureBuilder::new("ms-alpha").add_bead("Verify logs"))
            .build()
            .expect("workspace fixture");

        let raw = fs::read_to_string(workspace.beads_root().join("issues.jsonl"))
            .expect("read bead fixture");
        let issues = raw
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).expect("parse issue json"))
            .collect::<Vec<_>>();

        assert_eq!(issues.len(), 2);
        assert_eq!(
            issues[0]["acceptance_criteria"],
            "Fixture bead is materialized"
        );
        assert!(
            issues.iter().all(|issue| issue.get("created_at").is_some()),
            "expected created_at on every issue: {issues:?}"
        );
    }
}
