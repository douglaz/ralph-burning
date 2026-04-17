//! Scenario-specific workspace fixtures for integration-style tests.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::json;

use crate::adapters::br_models::{BeadPriority, BeadStatus, BeadType, DependencyKind};
use crate::adapters::fs::{
    FileSystem, FsMilestoneJournalStore, FsMilestonePlanStore, FsMilestoneSnapshotStore,
    FsMilestoneStore, FsTaskRunLineageStore,
};
use crate::contexts::milestone_record::bundle::{
    AcceptanceCriterion, BeadProposal, MilestoneBundle, MilestoneIdentity, Workstream,
};
use crate::contexts::milestone_record::model::MilestoneId;
use crate::contexts::milestone_record::service::{
    load_milestone, load_snapshot, materialize_bundle, read_journal, read_task_runs,
};
use crate::shared::domain::FlowPreset;
use crate::test_support::br::{MockBrAdapter, MockBrResponse};
use crate::test_support::bv::{MockBvAdapter, MockBvResponse};
use crate::test_support::fixtures::{
    write_bead_graph_issues, BeadGraphDependency, BeadGraphFixtureBuilder, BeadGraphIssue,
    MilestoneFixture, TempWorkspace, TempWorkspaceBuilder,
};

const MILESTONE_ID: &str = "ms-e2e-scenario";
const MILESTONE_NAME: &str = "E2E Milestone Scenario Fixture";
const ROOT_EPIC_ID: &str = "ms-e2e-scenario.milestone-root";
const WORKSPACE_EPIC_ID: &str = "ms-e2e-scenario.workstream-workspace-assembly";
const VALIDATION_EPIC_ID: &str = "ms-e2e-scenario.workstream-validation";
const ASSEMBLE_TASK_ID: &str = "ms-e2e-scenario.task-assemble-workspace";
const PREPARE_TASK_ID: &str = "ms-e2e-scenario.task-prepare-workspace";
const VALIDATE_TASK_ID: &str = "ms-e2e-scenario.task-validate-mocks";
const FOLLOW_UP_TASK_ID: &str = "ms-e2e-scenario.task-follow-up-validation";
const WORKSPACE_LABEL: &str = "workstream:workspace-assembly-1";
const VALIDATION_LABEL: &str = "workstream:validation-2";

fn scenario_timestamp() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 4, 17, 9, 0, 0)
        .single()
        .expect("valid e2e fixture timestamp")
}

/// Complete temp-workspace fixture for milestone controller and CLI tests.
#[derive(Debug)]
pub struct E2eScenarioFixture {
    pub workspace: TempWorkspace,
    pub milestone_id: MilestoneId,
    pub bundle: MilestoneBundle,
    pub bead_ids: Vec<String>,
    pub mock_br: MockBrAdapter,
    pub mock_bv: MockBvAdapter,
}

/// Build a workspace fixture with a planned milestone, `.beads` graph, and
/// shared mock adapter state for scenario-style tests.
pub fn build_e2e_milestone_scenario_fixture() -> E2eScenarioFixture {
    let bundle = scenario_bundle();
    let bead_graph_issues = scenario_bead_graph_issues(&bundle);
    let bead_graph = bead_graph_issues
        .iter()
        .cloned()
        .fold(BeadGraphFixtureBuilder::new(), |builder, issue| {
            builder.with_issue(issue)
        });

    let mut workspace = TempWorkspaceBuilder::new()
        .with_bead_graph(bead_graph)
        .build()
        .expect("build e2e scenario temp workspace");

    materialize_bundle(
        &FsMilestoneStore,
        &FsMilestoneSnapshotStore,
        &FsMilestoneJournalStore,
        &FsMilestonePlanStore,
        workspace.path(),
        &bundle,
        scenario_timestamp() + Duration::minutes(1),
    )
    .expect("materialize scenario milestone bundle");

    workspace.milestones = vec![load_materialized_fixture(workspace.path(), &bundle)];

    let scenario_state = Arc::new(Mutex::new(ScenarioState::new(
        workspace.path().to_path_buf(),
        bead_graph_issues.clone(),
    )));
    let bead_ids = bead_graph_issues
        .iter()
        .map(|issue| issue.id.clone())
        .collect();
    let mock_br = build_mock_br_adapter(Arc::clone(&scenario_state));
    let mock_bv = build_mock_bv_adapter(scenario_state);
    let milestone_id = workspace.milestones[0].milestone_id.clone();

    E2eScenarioFixture {
        workspace,
        milestone_id,
        bundle,
        bead_ids,
        mock_br,
        mock_bv,
    }
}

fn milestone_scope_label() -> String {
    format!("milestone:{MILESTONE_ID}")
}

fn proposal_label(bead_id: &str) -> String {
    format!("proposal:{bead_id}")
}

fn scenario_bundle() -> MilestoneBundle {
    MilestoneBundle {
        schema_version: 1,
        identity: MilestoneIdentity {
            id: MILESTONE_ID.to_owned(),
            name: MILESTONE_NAME.to_owned(),
        },
        executive_summary:
            "Assemble a deterministic temp workspace fixture with milestone artifacts and bead state."
                .to_owned(),
        goals: vec![
            "Materialize a Ready milestone with durable plan artifacts.".to_owned(),
            "Expose a production-shaped `.beads` graph for integration-style tests.".to_owned(),
            "Keep mocked br and bv responses aligned with filesystem mutations.".to_owned(),
        ],
        non_goals: vec!["Running real br or bv subprocesses.".to_owned()],
        constraints: vec![
            "Fixture creation must stay local-only and deterministic.".to_owned(),
            "Milestone and bead identifiers must remain stable for assertions.".to_owned(),
        ],
        acceptance_map: vec![
            AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Workspace roots and the initial bead graph are assembled.".to_owned(),
                covered_by: vec![ASSEMBLE_TASK_ID.to_owned()],
            },
            AcceptanceCriterion {
                id: "AC-2".to_owned(),
                description: "The milestone plan artifacts are persisted in Ready state.".to_owned(),
                covered_by: vec![PREPARE_TASK_ID.to_owned()],
            },
            AcceptanceCriterion {
                id: "AC-3".to_owned(),
                description: "Mocked br and bv responses stay consistent with graph mutations.".to_owned(),
                covered_by: vec![VALIDATE_TASK_ID.to_owned()],
            },
        ],
        workstreams: vec![
            Workstream {
                name: "Workspace Assembly".to_owned(),
                description: Some(
                    "Create the temp workspace scaffold and persist the milestone plan artifacts."
                        .to_owned(),
                ),
                beads: vec![
                    BeadProposal {
                        bead_id: Some(ASSEMBLE_TASK_ID.to_owned()),
                        explicit_id: Some(true),
                        title: "Assemble temp workspace scaffold".to_owned(),
                        description: Some(
                            "Create the temp workspace roots and seed the initial `.beads` graph."
                                .to_owned(),
                        ),
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec!["integration".to_owned(), "workspace".to_owned()],
                        depends_on: Vec::new(),
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                    BeadProposal {
                        bead_id: Some(PREPARE_TASK_ID.to_owned()),
                        explicit_id: Some(true),
                        title: "Prepare milestone planning artifacts".to_owned(),
                        description: Some(
                            "Persist the Ready milestone record with `plan.md` and `plan.json`."
                                .to_owned(),
                        ),
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec!["integration".to_owned(), "workspace".to_owned()],
                        depends_on: vec![ASSEMBLE_TASK_ID.to_owned()],
                        acceptance_criteria: vec!["AC-2".to_owned()],
                        flow_override: None,
                    },
                ],
            },
            Workstream {
                name: "Validation".to_owned(),
                description: Some(
                    "Exercise the mocked adapters against the staged workspace state.".to_owned(),
                ),
                beads: vec![BeadProposal {
                    bead_id: Some(VALIDATE_TASK_ID.to_owned()),
                    explicit_id: Some(true),
                    title: "Validate mocked adapter responses".to_owned(),
                    description: Some(
                        "Confirm `br` and `bv` stay in sync with the persisted scenario graph."
                            .to_owned(),
                    ),
                    bead_type: Some("task".to_owned()),
                    priority: Some(2),
                    labels: vec!["integration".to_owned(), "mocks".to_owned()],
                    depends_on: vec![PREPARE_TASK_ID.to_owned()],
                    acceptance_criteria: vec!["AC-3".to_owned()],
                    flow_override: None,
                }],
            },
        ],
        default_flow: FlowPreset::QuickDev,
        agents_guidance: Some(
            "Use the staged milestone artifacts and shared mock adapter state instead of real tooling."
                .to_owned(),
        ),
    }
}

fn scenario_bead_graph_issues(bundle: &MilestoneBundle) -> Vec<BeadGraphIssue> {
    let acceptance_lookup = bundle
        .acceptance_map
        .iter()
        .map(|criterion| (criterion.id.as_str(), criterion.description.as_str()))
        .collect::<HashMap<_, _>>();
    let milestone_label = milestone_scope_label();
    let created_at = scenario_timestamp();
    let actor = "fixture".to_owned();

    let mut issues = vec![
        BeadGraphIssue {
            id: ROOT_EPIC_ID.to_owned(),
            title: MILESTONE_NAME.to_owned(),
            status: BeadStatus::Open,
            priority: BeadPriority::new(1),
            bead_type: BeadType::Epic,
            labels: vec![milestone_label.clone(), "milestone-root".to_owned()],
            description: Some(bundle.executive_summary.clone()),
            acceptance_criteria: Vec::new(),
            created_at,
            created_by: actor.clone(),
            updated_at: created_at,
            source_repo: ".".to_owned(),
            compaction_level: 0,
            original_size: 0,
            dependencies: Vec::new(),
            closed_at: None,
            close_reason: None,
        },
        BeadGraphIssue {
            id: WORKSPACE_EPIC_ID.to_owned(),
            title: bundle.workstreams[0].name.clone(),
            status: BeadStatus::Open,
            priority: BeadPriority::new(1),
            bead_type: BeadType::Epic,
            labels: vec![milestone_label.clone(), WORKSPACE_LABEL.to_owned()],
            description: bundle.workstreams[0].description.clone(),
            acceptance_criteria: Vec::new(),
            created_at,
            created_by: actor.clone(),
            updated_at: created_at,
            source_repo: ".".to_owned(),
            compaction_level: 0,
            original_size: 0,
            dependencies: vec![BeadGraphDependency::new(
                WORKSPACE_EPIC_ID,
                ROOT_EPIC_ID,
                DependencyKind::ParentChild,
                created_at,
                actor.clone(),
            )],
            closed_at: None,
            close_reason: None,
        },
        BeadGraphIssue {
            id: VALIDATION_EPIC_ID.to_owned(),
            title: bundle.workstreams[1].name.clone(),
            status: BeadStatus::Open,
            priority: BeadPriority::new(1),
            bead_type: BeadType::Epic,
            labels: vec![milestone_label.clone(), VALIDATION_LABEL.to_owned()],
            description: bundle.workstreams[1].description.clone(),
            acceptance_criteria: Vec::new(),
            created_at,
            created_by: actor.clone(),
            updated_at: created_at,
            source_repo: ".".to_owned(),
            compaction_level: 0,
            original_size: 0,
            dependencies: vec![BeadGraphDependency::new(
                VALIDATION_EPIC_ID,
                ROOT_EPIC_ID,
                DependencyKind::ParentChild,
                created_at,
                actor.clone(),
            )],
            closed_at: None,
            close_reason: None,
        },
    ];

    for (workstream_index, workstream) in bundle.workstreams.iter().enumerate() {
        let workstream_epic_id = match workstream_index {
            0 => WORKSPACE_EPIC_ID,
            1 => VALIDATION_EPIC_ID,
            _ => unreachable!("scenario bundle has two workstreams"),
        };
        for proposal in &workstream.beads {
            let bead_id = proposal
                .bead_id
                .as_deref()
                .expect("scenario bead ids are explicit");
            let mut dependencies = vec![BeadGraphDependency::new(
                bead_id,
                workstream_epic_id,
                DependencyKind::ParentChild,
                created_at,
                actor.clone(),
            )];
            dependencies.extend(proposal.depends_on.iter().map(|depends_on_id| {
                BeadGraphDependency::new(
                    bead_id,
                    depends_on_id.as_str(),
                    DependencyKind::Blocks,
                    created_at,
                    actor.clone(),
                )
            }));

            issues.push(BeadGraphIssue {
                id: bead_id.to_owned(),
                title: proposal.title.clone(),
                status: BeadStatus::Open,
                priority: BeadPriority::new(proposal.priority.unwrap_or(2)),
                bead_type: bead_type_from_name(proposal.bead_type.as_deref()),
                labels: scenario_task_labels(&milestone_label, bead_id, &proposal.labels),
                description: proposal.description.clone(),
                acceptance_criteria: proposal
                    .acceptance_criteria
                    .iter()
                    .filter_map(|criterion_id| {
                        acceptance_lookup
                            .get(criterion_id.as_str())
                            .map(|description| (*description).to_owned())
                    })
                    .collect(),
                created_at,
                created_by: actor.clone(),
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

    issues
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

fn scenario_task_labels(milestone_label: &str, bead_id: &str, labels: &[String]) -> Vec<String> {
    let mut merged = vec![milestone_label.to_owned(), proposal_label(bead_id)];
    for label in labels {
        if !merged.iter().any(|existing| existing == label) {
            merged.push(label.clone());
        }
    }
    merged
}

fn load_materialized_fixture(base_dir: &Path, bundle: &MilestoneBundle) -> MilestoneFixture {
    let milestone_id = MilestoneId::new(bundle.identity.id.clone()).expect("scenario milestone id");
    let record = load_milestone(&FsMilestoneStore, base_dir, &milestone_id)
        .expect("load scenario milestone record");
    let snapshot = load_snapshot(&FsMilestoneSnapshotStore, base_dir, &milestone_id)
        .expect("load scenario milestone snapshot");
    let journal = read_journal(&FsMilestoneJournalStore, base_dir, &milestone_id)
        .expect("read scenario milestone journal");
    let task_runs = read_task_runs(&FsTaskRunLineageStore, base_dir, &milestone_id)
        .expect("read scenario task runs");

    MilestoneFixture {
        milestone_id: milestone_id.clone(),
        bundle: bundle.clone(),
        record,
        snapshot,
        root: FileSystem::audit_workspace_root_path(base_dir)
            .join("milestones")
            .join(milestone_id.as_str()),
        journal,
        task_runs,
    }
}

fn build_mock_br_adapter(state: Arc<Mutex<ScenarioState>>) -> MockBrAdapter {
    MockBrAdapter::from_dispatch(move |call| scenario_br_response(&state, &call.args))
}

fn build_mock_bv_adapter(state: Arc<Mutex<ScenarioState>>) -> MockBvAdapter {
    MockBvAdapter::from_dispatch(move |call| scenario_bv_response(&state, &call.args))
}

fn next_follow_up_task_id(create_index: usize) -> String {
    if create_index == 0 {
        FOLLOW_UP_TASK_ID.to_owned()
    } else {
        format!("{FOLLOW_UP_TASK_ID}-{create_index}")
    }
}

#[derive(Debug)]
struct ScenarioState {
    workspace_root: PathBuf,
    issues: Vec<BeadGraphIssue>,
    next_follow_up_index: usize,
}

impl ScenarioState {
    fn new(workspace_root: PathBuf, issues: Vec<BeadGraphIssue>) -> Self {
        Self {
            workspace_root,
            issues,
            next_follow_up_index: 0,
        }
    }

    fn current_issues(&self) -> Vec<BeadGraphIssue> {
        self.issues.clone()
    }

    fn create_issue(&mut self, spec: CreateIssueSpec) -> Result<String, String> {
        let bead_id = next_follow_up_task_id(self.next_follow_up_index);
        self.next_follow_up_index += 1;
        let created_at = scenario_timestamp()
            + Duration::minutes(10)
            + Duration::seconds(self.next_follow_up_index as i64);
        self.issues.push(BeadGraphIssue {
            id: bead_id.clone(),
            title: spec.title,
            status: BeadStatus::Open,
            priority: spec.priority,
            bead_type: spec.bead_type,
            labels: spec.labels,
            description: spec.description,
            acceptance_criteria: Vec::new(),
            created_at,
            created_by: "fixture".to_owned(),
            updated_at: created_at,
            source_repo: ".".to_owned(),
            compaction_level: 0,
            original_size: 0,
            dependencies: Vec::new(),
            closed_at: None,
            close_reason: None,
        });
        self.persist()?;
        Ok(bead_id)
    }

    fn add_dependency(
        &mut self,
        issue_id: &str,
        depends_on_id: &str,
        kind: DependencyKind,
    ) -> Result<(), String> {
        if !self.issues.iter().any(|issue| issue.id == depends_on_id) {
            return Err(format!("bead not found: {depends_on_id}"));
        }
        let index = self
            .issues
            .iter()
            .position(|issue| issue.id == issue_id)
            .ok_or_else(|| format!("bead not found: {issue_id}"))?;
        if self.issues[index]
            .dependencies
            .iter()
            .any(|dependency| dependency.depends_on_id == depends_on_id && dependency.kind == kind)
        {
            return Ok(());
        }

        let updated_at = self.issues[index].updated_at;
        let created_by = self.issues[index].created_by.clone();
        self.issues[index]
            .dependencies
            .push(BeadGraphDependency::new(
                issue_id,
                depends_on_id,
                kind,
                updated_at,
                created_by,
            ));
        self.persist()
    }

    fn close_issue(&mut self, issue_id: &str, reason: String) -> Result<(), String> {
        let index = self
            .issues
            .iter()
            .position(|issue| issue.id == issue_id)
            .ok_or_else(|| format!("bead not found: {issue_id}"))?;
        if self.issues[index].status == BeadStatus::Closed {
            return Ok(());
        }

        let closed_at = self.issues[index].updated_at + Duration::minutes(1);
        self.issues[index].status = BeadStatus::Closed;
        self.issues[index].updated_at = closed_at;
        self.issues[index].closed_at = Some(closed_at);
        self.issues[index].close_reason = Some(reason);
        self.persist()
    }

    fn next_ready_issue(&self) -> Option<BeadGraphIssue> {
        let issue_lookup = self
            .issues
            .iter()
            .map(|issue| (issue.id.as_str(), issue))
            .collect::<HashMap<_, _>>();

        self.issues
            .iter()
            .find(|issue| {
                issue.status == BeadStatus::Open
                    && !matches!(issue.bead_type, BeadType::Epic)
                    && issue.dependencies.iter().all(|dependency| {
                        dependency.kind != DependencyKind::Blocks
                            || issue_lookup
                                .get(dependency.depends_on_id.as_str())
                                .is_none_or(|depends_on| depends_on.status == BeadStatus::Closed)
                    })
            })
            .cloned()
    }

    fn persist(&self) -> Result<(), String> {
        let issues_path = self.workspace_root.join(".beads").join("issues.jsonl");
        write_bead_graph_issues(&issues_path, &self.issues).map_err(|error| error.to_string())
    }
}

#[derive(Debug)]
struct CreateIssueSpec {
    title: String,
    bead_type: BeadType,
    priority: BeadPriority,
    labels: Vec<String>,
    description: Option<String>,
}

fn scenario_br_response(
    state: &Arc<Mutex<ScenarioState>>,
    args: &[String],
) -> Option<MockBrResponse> {
    if matches!(args, [command, flag] if command == "list" && flag == "--json") {
        let issues = state
            .lock()
            .expect("scenario state lock poisoned")
            .current_issues();
        return Some(MockBrResponse::success(br_list_json(&issues)));
    }

    if let [command, bead_id, flag] = args {
        if command == "show" && flag == "--json" {
            let issues = state
                .lock()
                .expect("scenario state lock poisoned")
                .current_issues();
            let Some(issue) = issues.iter().find(|issue| issue.id == *bead_id).cloned() else {
                return Some(MockBrResponse::exit_failure(
                    1,
                    format!("bead not found: {bead_id}"),
                ));
            };
            return Some(MockBrResponse::success(br_show_json(&issue, &issues)));
        }
    }

    if let Some(spec) = parse_create_issue_spec(args) {
        let result = state
            .lock()
            .expect("scenario state lock poisoned")
            .create_issue(spec);
        return Some(match result {
            Ok(bead_id) => MockBrResponse::success(format!("Created bead {bead_id}")),
            Err(message) => MockBrResponse::exit_failure(1, message),
        });
    }

    if let Some((issue_id, depends_on_id, kind)) = parse_dep_add_args(args) {
        let result = state
            .lock()
            .expect("scenario state lock poisoned")
            .add_dependency(&issue_id, &depends_on_id, kind);
        return Some(match result {
            Ok(()) => MockBrResponse::success("dependency added"),
            Err(message) => MockBrResponse::exit_failure(1, message),
        });
    }

    if let Some((issue_id, reason)) = parse_close_args(args) {
        let result = state
            .lock()
            .expect("scenario state lock poisoned")
            .close_issue(&issue_id, reason);
        return Some(match result {
            Ok(()) => MockBrResponse::success(format!("Closed {issue_id}")),
            Err(message) => MockBrResponse::exit_failure(1, message),
        });
    }

    if matches!(args, [command, flag] if command == "sync" && flag == "--flush-only") {
        return Some(MockBrResponse::success("synced"));
    }

    None
}

fn scenario_bv_response(
    state: &Arc<Mutex<ScenarioState>>,
    args: &[String],
) -> Option<MockBvResponse> {
    match args {
        [command] if command == "--robot-next" => {
            let maybe_issue = state
                .lock()
                .expect("scenario state lock poisoned")
                .next_ready_issue();
            Some(match maybe_issue {
                Some(issue) => MockBvResponse::success(next_bead_json(&issue)),
                None => MockBvResponse::exit_failure(1, "no ready bead"),
            })
        }
        _ => None,
    }
}

fn parse_flag_value(arg: &str, name: &str) -> Option<String> {
    arg.strip_prefix(&format!("--{name}=")).map(str::to_owned)
}

fn parse_create_issue_spec(args: &[String]) -> Option<CreateIssueSpec> {
    if args.first().map(String::as_str) != Some("create") {
        return None;
    }

    let mut title = None;
    let mut bead_type = None;
    let mut priority = None;
    let mut labels = Vec::new();
    let mut description = None;
    for arg in &args[1..] {
        if title.is_none() {
            title = parse_flag_value(arg, "title");
        }
        if bead_type.is_none() {
            bead_type = parse_flag_value(arg, "type");
        }
        if priority.is_none() {
            priority = parse_flag_value(arg, "priority");
        }
        if labels.is_empty() {
            labels = parse_flag_value(arg, "labels")
                .map(|value| {
                    value
                        .split(',')
                        .filter(|label| !label.is_empty())
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
        }
        if description.is_none() {
            description = parse_flag_value(arg, "description");
        }
    }

    let title = title?;
    let bead_type = bead_type
        .as_deref()
        .map(|value| bead_type_from_name(Some(value)))
        .unwrap_or(BeadType::Task);
    let priority = priority
        .as_deref()
        .and_then(parse_priority)
        .unwrap_or_else(|| BeadPriority::new(2));

    Some(CreateIssueSpec {
        title,
        bead_type,
        priority,
        labels,
        description,
    })
}

fn parse_priority(value: &str) -> Option<BeadPriority> {
    BeadPriority::from_str(value).ok()
}

fn parse_dep_add_args(args: &[String]) -> Option<(String, String, DependencyKind)> {
    match args {
        [command, action, from_id, to_id] if command == "dep" && action == "add" => {
            Some((from_id.clone(), to_id.clone(), DependencyKind::Blocks))
        }
        [command, action, from_id, to_id, kind]
            if command == "dep" && action == "add" && kind == "--type=parent-child" =>
        {
            Some((from_id.clone(), to_id.clone(), DependencyKind::ParentChild))
        }
        [command, action, from_id, to_id, kind]
            if command == "dep" && action == "add" && kind == "--type=blocks" =>
        {
            Some((from_id.clone(), to_id.clone(), DependencyKind::Blocks))
        }
        _ => None,
    }
}

fn parse_close_args(args: &[String]) -> Option<(String, String)> {
    if args.first().map(String::as_str) != Some("close") {
        return None;
    }
    let issue_id = args.get(1)?.clone();
    let reason = args[2..]
        .iter()
        .find_map(|arg| parse_flag_value(arg, "reason"))?;
    Some((issue_id, reason))
}

fn br_list_json(issues: &[BeadGraphIssue]) -> String {
    serde_json::to_string(
        &issues
            .iter()
            .map(|issue| {
                json!({
                    "id": issue.id,
                    "title": issue.title,
                    "status": issue.status,
                    "priority": issue.priority,
                    "issue_type": issue.bead_type,
                    "labels": issue.labels,
                })
            })
            .collect::<Vec<_>>(),
    )
    .expect("serialize br list fixture")
}

fn br_show_json(issue: &BeadGraphIssue, issues: &[BeadGraphIssue]) -> String {
    let issue_lookup = issues
        .iter()
        .map(|candidate| (candidate.id.as_str(), candidate))
        .collect::<HashMap<_, _>>();
    json!({
        "id": issue.id,
        "title": issue.title,
        "status": issue.status,
        "priority": issue.priority,
        "issue_type": issue.bead_type,
        "labels": issue.labels,
        "description": issue.description,
        "acceptance_criteria": issue.acceptance_criteria,
        "dependencies": issue.dependencies.iter().map(|dependency| {
            let linked_issue = issue_lookup
                .get(dependency.depends_on_id.as_str())
                .expect("dependency issue must exist");
            json!({
                "id": dependency.depends_on_id,
                "dependency_type": dependency_kind_json(&dependency.kind),
                "title": linked_issue.title,
                "status": linked_issue.status,
            })
        }).collect::<Vec<_>>(),
        "dependents": issues
            .iter()
            .flat_map(|candidate| {
                candidate
                    .dependencies
                    .iter()
                    .filter(|dependency| dependency.depends_on_id == issue.id)
                    .map(|dependency| {
                        json!({
                            "id": candidate.id,
                            "dependency_type": dependency_kind_json(&dependency.kind),
                            "title": candidate.title,
                            "status": candidate.status,
                        })
                    })
            })
            .collect::<Vec<_>>(),
        "comments": Vec::<serde_json::Value>::new(),
        "owner": "fixture",
        "created_at": issue.created_at.to_rfc3339(),
        "updated_at": issue.updated_at.to_rfc3339(),
    })
    .to_string()
}

fn next_bead_json(issue: &BeadGraphIssue) -> String {
    json!({
        "id": issue.id,
        "title": issue.title,
        "score": 9.7,
        "reasons": ["ready", "unblocked"],
        "action": "implement",
    })
    .to_string()
}

fn dependency_kind_json(kind: &DependencyKind) -> &'static str {
    match kind {
        DependencyKind::Blocks => "blocks",
        DependencyKind::ParentChild => "parent_child",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::adapters::br_models::{BeadDetail, BeadSummary};
    use crate::adapters::br_process::{BrAdapter, BrMutationAdapter, BrOutput};
    use crate::adapters::bv_process::{BvCommand, NextBeadResponse};
    use crate::contexts::milestone_record::model::{MilestoneEventType, MilestoneStatus};

    fn created_bead_id(stdout: &str) -> String {
        stdout
            .strip_prefix("Created bead ")
            .expect("created bead output prefix")
            .to_owned()
    }

    fn bead_rows(path: &Path) -> Vec<serde_json::Value> {
        fs::read_to_string(path)
            .expect("read bead graph")
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("parse bead json"))
            .collect()
    }

    fn mutation_adapter(
        mock: &MockBrAdapter,
        working_dir: PathBuf,
    ) -> BrMutationAdapter<MockBrAdapter> {
        BrMutationAdapter::with_adapter(
            BrAdapter::with_runner(mock.clone()).with_working_dir(working_dir),
        )
    }

    #[tokio::test]
    async fn build_e2e_milestone_scenario_fixture_smoke_test() {
        let started = Instant::now();
        let fixture = build_e2e_milestone_scenario_fixture();
        let elapsed = started.elapsed();

        assert!(
            elapsed < Duration::from_secs(10),
            "fixture creation should avoid pathological slowdowns, took {elapsed:?}"
        );

        assert_eq!(fixture.milestone_id.as_str(), MILESTONE_ID);
        assert_eq!(fixture.workspace.milestones.len(), 1);
        assert!(fixture.workspace.audit_root().is_dir());
        assert!(fixture.workspace.live_root().is_dir());
        assert!(fixture
            .workspace
            .beads_root()
            .join("issues.jsonl")
            .is_file());

        let milestone = &fixture.workspace.milestones[0];
        assert_eq!(milestone.snapshot.status, MilestoneStatus::Ready);
        assert_eq!(milestone.record.id, fixture.milestone_id);
        assert_eq!(milestone.bundle, fixture.bundle);
        assert!(milestone.root.join("plan.md").is_file());
        assert!(milestone.root.join("plan.json").is_file());
        assert_eq!(milestone.snapshot.progress.total_beads, 3);
        assert!(milestone.task_runs.is_empty());
        assert_eq!(
            milestone
                .journal
                .iter()
                .filter(|event| event.event_type == MilestoneEventType::PlanDrafted)
                .count(),
            1
        );
        assert!(milestone
            .journal
            .iter()
            .all(|event| event.event_type != MilestoneEventType::PlanUpdated));

        let plan_json =
            fs::read_to_string(milestone.root.join("plan.json")).expect("read plan.json");
        let persisted_bundle: MilestoneBundle =
            serde_json::from_str(&plan_json).expect("parse plan.json bundle");
        assert_eq!(persisted_bundle.identity.id, MILESTONE_ID);
        assert_eq!(persisted_bundle.workstreams.len(), 2);
        assert_eq!(
            persisted_bundle
                .workstreams
                .iter()
                .map(|workstream| workstream.beads.len())
                .sum::<usize>(),
            3
        );
        assert_eq!(
            persisted_bundle
                .workstreams
                .iter()
                .flat_map(|workstream| workstream.beads.iter())
                .map(|bead| {
                    (
                        bead.bead_id.clone().expect("explicit scenario bead id"),
                        bead.title.clone(),
                    )
                })
                .collect::<BTreeSet<_>>(),
            [
                (
                    ASSEMBLE_TASK_ID.to_owned(),
                    "Assemble temp workspace scaffold".to_owned()
                ),
                (
                    PREPARE_TASK_ID.to_owned(),
                    "Prepare milestone planning artifacts".to_owned()
                ),
                (
                    VALIDATE_TASK_ID.to_owned(),
                    "Validate mocked adapter responses".to_owned(),
                ),
            ]
            .into_iter()
            .collect::<BTreeSet<_>>()
        );
        assert!(persisted_bundle
            .workstreams
            .iter()
            .flat_map(|workstream| workstream.beads.iter())
            .all(|bead| bead.bead_id.as_deref() != Some(ROOT_EPIC_ID)));

        let issues_path = fixture.workspace.beads_root().join("issues.jsonl");
        let initial_rows = bead_rows(&issues_path);
        assert_eq!(initial_rows.len(), 6);
        assert_eq!(
            initial_rows
                .iter()
                .map(|row| row["id"].as_str().expect("string bead id").to_owned())
                .collect::<BTreeSet<_>>(),
            fixture.bead_ids.iter().cloned().collect::<BTreeSet<_>>()
        );
        let prepare_row = initial_rows
            .iter()
            .find(|row| row["id"] == PREPARE_TASK_ID)
            .expect("prepare task row");
        assert_eq!(
            prepare_row["dependencies"]
                .as_array()
                .expect("prepare dependencies")
                .iter()
                .map(|dependency| dependency["type"].as_str().expect("dependency type"))
                .collect::<BTreeSet<_>>(),
            ["blocks", "parent_child"]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        let validate_row = initial_rows
            .iter()
            .find(|row| row["id"] == VALIDATE_TASK_ID)
            .expect("validate task row");
        assert_eq!(
            validate_row["dependencies"][0]["depends_on_id"].as_str(),
            Some(VALIDATION_EPIC_ID)
        );
        assert_eq!(
            validate_row["dependencies"][1]["depends_on_id"].as_str(),
            Some(PREPARE_TASK_ID)
        );

        let working_dir = fixture.workspace.path().to_path_buf();
        let br_read = fixture
            .mock_br
            .as_br_adapter()
            .with_working_dir(working_dir.clone());
        let br_mutation = mutation_adapter(&fixture.mock_br, working_dir.clone());
        let bv = fixture
            .mock_bv
            .as_bv_adapter()
            .with_working_dir(working_dir);

        let root_detail: BeadDetail = br_read
            .exec_json(&crate::adapters::br_process::BrCommand::show(ROOT_EPIC_ID))
            .await
            .expect("br show root");
        assert_eq!(root_detail.id, ROOT_EPIC_ID);
        assert_eq!(root_detail.dependencies.len(), 0);
        assert_eq!(
            root_detail
                .dependents
                .iter()
                .map(|dependency| (dependency.id.clone(), dependency.kind.to_string()))
                .collect::<BTreeSet<_>>(),
            [
                (VALIDATION_EPIC_ID.to_owned(), "parent_child".to_owned()),
                (WORKSPACE_EPIC_ID.to_owned(), "parent_child".to_owned()),
            ]
            .into_iter()
            .collect::<BTreeSet<_>>()
        );

        let prepare_detail: BeadDetail = br_read
            .exec_json(&crate::adapters::br_process::BrCommand::show(
                PREPARE_TASK_ID,
            ))
            .await
            .expect("br show prepare");
        assert_eq!(prepare_detail.id, PREPARE_TASK_ID);
        assert_eq!(prepare_detail.dependencies.len(), 2);
        assert_eq!(prepare_detail.dependencies[0].id, WORKSPACE_EPIC_ID);
        assert_eq!(
            prepare_detail.dependencies[0].kind,
            DependencyKind::ParentChild
        );
        assert_eq!(prepare_detail.dependencies[1].id, ASSEMBLE_TASK_ID);
        assert_eq!(prepare_detail.dependencies[1].kind, DependencyKind::Blocks);
        assert_eq!(prepare_detail.dependents.len(), 1);
        assert_eq!(prepare_detail.dependents[0].id, VALIDATE_TASK_ID);

        let listed: Vec<BeadSummary> = br_read
            .exec_json(&crate::adapters::br_process::BrCommand::list())
            .await
            .expect("br list");
        assert_eq!(listed.len(), 6);
        assert_eq!(listed[0].id, ROOT_EPIC_ID);
        assert_eq!(listed[3].id, ASSEMBLE_TASK_ID);
        assert_eq!(listed[5].id, VALIDATE_TASK_ID);

        let initial_next: NextBeadResponse = bv
            .exec_json(&BvCommand::robot_next())
            .await
            .expect("initial bv robot-next");
        assert_eq!(initial_next.id, ASSEMBLE_TASK_ID);

        let close_assemble: BrOutput = br_mutation
            .close_bead(
                ASSEMBLE_TASK_ID,
                &format!("task {ASSEMBLE_TASK_ID} completed successfully"),
            )
            .await
            .expect("close assemble task");
        assert!(close_assemble.stdout.contains(ASSEMBLE_TASK_ID));

        let rows_after_assemble_close = bead_rows(&issues_path);
        assert_eq!(
            rows_after_assemble_close
                .iter()
                .find(|row| row["id"] == ASSEMBLE_TASK_ID)
                .and_then(|row| row["status"].as_str()),
            Some("closed")
        );

        let next_after_assemble: NextBeadResponse = bv
            .exec_json(&BvCommand::robot_next())
            .await
            .expect("bv robot-next after assemble close");
        assert_eq!(next_after_assemble.id, PREPARE_TASK_ID);

        let follow_up_labels = vec!["integration".to_owned(), "follow-up".to_owned()];
        let create_output = br_mutation
            .create_bead(
                "Follow-up validation bead",
                "task",
                "2",
                &follow_up_labels,
                Some("Capture a synthetic follow-up task created during scenario execution."),
            )
            .await
            .expect("create follow-up bead");
        let created_id = created_bead_id(&create_output.stdout);
        assert_eq!(created_id, FOLLOW_UP_TASK_ID);

        br_mutation
            .add_dependency(&created_id, VALIDATE_TASK_ID)
            .await
            .expect("link follow-up dependency");

        let created_detail: BeadDetail = br_read
            .exec_json(&crate::adapters::br_process::BrCommand::show(&created_id))
            .await
            .expect("show created bead");
        assert_eq!(created_detail.id, created_id);
        assert_eq!(created_detail.title, "Follow-up validation bead");
        assert_eq!(created_detail.labels, follow_up_labels);
        assert_eq!(
            created_detail.description.as_deref(),
            Some("Capture a synthetic follow-up task created during scenario execution.")
        );
        assert_eq!(created_detail.dependencies.len(), 1);
        assert_eq!(created_detail.dependencies[0].id, VALIDATE_TASK_ID);
        assert_eq!(created_detail.dependencies[0].kind, DependencyKind::Blocks);

        let rows_after_create = bead_rows(&issues_path);
        assert_eq!(rows_after_create.len(), 7);
        let created_row = rows_after_create
            .iter()
            .find(|row| row["id"] == created_id)
            .expect("created row on disk");
        assert_eq!(
            created_row["dependencies"][0]["depends_on_id"].as_str(),
            Some(VALIDATE_TASK_ID)
        );

        br_mutation
            .close_bead(
                PREPARE_TASK_ID,
                &format!("task {PREPARE_TASK_ID} completed successfully"),
            )
            .await
            .expect("close prepare task");

        let next_after_prepare: NextBeadResponse = bv
            .exec_json(&BvCommand::robot_next())
            .await
            .expect("bv robot-next after prepare close");
        assert_eq!(next_after_prepare.id, VALIDATE_TASK_ID);

        br_mutation
            .close_bead(
                VALIDATE_TASK_ID,
                &format!("task {VALIDATE_TASK_ID} completed successfully"),
            )
            .await
            .expect("close validate task");

        let next_after_validate: NextBeadResponse = bv
            .exec_json(&BvCommand::robot_next())
            .await
            .expect("bv robot-next after validate close");
        assert_eq!(next_after_validate.id, created_id);

        br_mutation
            .close_bead(&created_id, "Fixture cleanup")
            .await
            .expect("close follow-up bead");
        let rows_after_follow_up_close = bead_rows(&issues_path);
        let closed_follow_up_row = rows_after_follow_up_close
            .iter()
            .find(|row| row["id"] == created_id)
            .expect("closed follow-up row");
        assert_eq!(closed_follow_up_row["status"].as_str(), Some("closed"));
        assert_eq!(
            closed_follow_up_row["close_reason"].as_str(),
            Some("Fixture cleanup")
        );

        let listed_after_mutations: Vec<BeadSummary> = br_read
            .exec_json(&crate::adapters::br_process::BrCommand::list())
            .await
            .expect("br list after mutations");
        assert_eq!(listed_after_mutations.len(), 7);
        assert_eq!(
            listed_after_mutations
                .iter()
                .find(|summary| summary.id == created_id)
                .map(|summary| summary.status.clone()),
            Some(BeadStatus::Closed)
        );

        let sync_output = br_mutation.sync_flush().await.expect("br sync");
        assert_eq!(sync_output.stdout, "synced");

        let br_calls = fixture
            .mock_br
            .calls()
            .iter()
            .map(|call| call.args.clone())
            .collect::<Vec<_>>();
        assert!(br_calls.iter().any(|call| {
            call == &vec![
                "create".to_owned(),
                "--title=Follow-up validation bead".to_owned(),
                "--type=task".to_owned(),
                "--priority=2".to_owned(),
                "--labels=integration,follow-up".to_owned(),
                "--description=Capture a synthetic follow-up task created during scenario execution."
                    .to_owned(),
            ]
        }));
        assert!(br_calls.iter().any(|call| {
            call == &vec![
                "dep".to_owned(),
                "add".to_owned(),
                created_id.clone(),
                VALIDATE_TASK_ID.to_owned(),
            ]
        }));
        assert!(br_calls.iter().any(|call| {
            call == &vec![
                "close".to_owned(),
                ASSEMBLE_TASK_ID.to_owned(),
                format!("--reason=task {ASSEMBLE_TASK_ID} completed successfully"),
            ]
        }));
        assert!(br_calls.iter().any(|call| {
            call == &vec![
                "close".to_owned(),
                created_id.clone(),
                "--reason=Fixture cleanup".to_owned(),
            ]
        }));
        assert!(br_calls
            .iter()
            .any(|call| { call == &vec!["sync".to_owned(), "--flush-only".to_owned()] }));

        assert_eq!(
            fixture
                .mock_bv
                .calls()
                .iter()
                .map(|call| call.args.clone())
                .collect::<Vec<_>>(),
            vec![
                vec!["--robot-next".to_owned()],
                vec!["--robot-next".to_owned()],
                vec!["--robot-next".to_owned()],
                vec!["--robot-next".to_owned()],
            ]
        );
    }
}
