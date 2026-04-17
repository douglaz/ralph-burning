//! Scenario-specific workspace fixtures for integration-style tests.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::path::Path;
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
    BeadGraphDependency, BeadGraphFixtureBuilder, BeadGraphIssue, MilestoneFixture, TempWorkspace,
    TempWorkspaceBuilder,
};

const MILESTONE_ID: &str = "ms-e2e-scenario";
const MILESTONE_NAME: &str = "E2E Milestone Scenario Fixture";
const ROOT_EPIC_ID: &str = "ms-e2e-scenario.root-epic";
const PREPARE_TASK_ID: &str = "ms-e2e-scenario.task-prepare-workspace";
const VALIDATE_TASK_ID: &str = "ms-e2e-scenario.task-validate-mocks";
const FOLLOW_UP_TASK_ID: &str = "ms-e2e-scenario.task-follow-up-validation";
const FOLLOW_UP_TASK_TITLE: &str = "Follow-up validation bead";

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
/// queued mock adapter responses for scenario-style tests.
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

    let milestone_id = workspace.milestones[0].milestone_id.clone();
    let bead_ids = bead_graph_issues
        .iter()
        .map(|issue| issue.id.clone())
        .collect();
    let mock_br = build_mock_br_adapter(&bead_graph_issues);
    let mock_bv = build_mock_bv_adapter(&bead_graph_issues);

    E2eScenarioFixture {
        workspace,
        milestone_id,
        bundle,
        bead_ids,
        mock_br,
        mock_bv,
    }
}

#[derive(Debug, Clone)]
struct CreatedFollowUpBead {
    issue: BeadGraphIssue,
}

#[derive(Debug, Default)]
struct ScenarioBrState {
    created_follow_ups: Vec<CreatedFollowUpBead>,
}

impl ScenarioBrState {
    fn create_follow_up(&mut self) -> String {
        let create_index = self.created_follow_ups.len();
        let bead_id = next_follow_up_task_id(create_index);
        self.created_follow_ups.push(CreatedFollowUpBead {
            issue: follow_up_validation_issue(&bead_id, BeadStatus::Open, create_index),
        });
        bead_id
    }

    fn close_follow_up(&mut self, bead_id: &str) -> Result<(), String> {
        let Some(create_index) = self
            .created_follow_ups
            .iter()
            .position(|created| created.issue.id == bead_id)
        else {
            return Err(format!("bead not found: {bead_id}"));
        };
        let created = &mut self.created_follow_ups[create_index];

        if created.issue.status != BeadStatus::Closed {
            let created_at = created.issue.created_at;
            created.issue = follow_up_validation_issue(bead_id, BeadStatus::Closed, create_index)
                .with_created_at(created_at);
        }

        Ok(())
    }

    fn created_issues(&self) -> Vec<BeadGraphIssue> {
        self.created_follow_ups
            .iter()
            .map(|created| created.issue.clone())
            .collect()
    }
}

trait ScenarioIssueExt {
    fn with_created_at(self, created_at: DateTime<Utc>) -> Self;
}

impl ScenarioIssueExt for BeadGraphIssue {
    fn with_created_at(mut self, created_at: DateTime<Utc>) -> Self {
        self.created_at = created_at;
        self.updated_at = if self.status == BeadStatus::Closed {
            created_at + Duration::minutes(1)
        } else {
            created_at
        };
        self.closed_at =
            (self.status == BeadStatus::Closed).then_some(created_at + Duration::minutes(1));
        self
    }
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
            "Expose a realistic `.beads` graph for integration-style tests.".to_owned(),
            "Preload mock br/bv responses that line up with the scenario graph.".to_owned(),
        ],
        non_goals: vec!["Running real br or bv subprocesses.".to_owned()],
        constraints: vec![
            "Fixture creation must stay local-only and deterministic.".to_owned(),
            "All milestone and bead identifiers must remain inspectable in test assertions."
                .to_owned(),
        ],
        acceptance_map: vec![
            AcceptanceCriterion {
                id: "AC-1".to_owned(),
                description: "Workspace scaffolding is prepared in the temp directory.".to_owned(),
                covered_by: vec![ROOT_EPIC_ID.to_owned()],
            },
            AcceptanceCriterion {
                id: "AC-2".to_owned(),
                description: "Milestone plan artifacts are written and readable.".to_owned(),
                covered_by: vec![PREPARE_TASK_ID.to_owned()],
            },
            AcceptanceCriterion {
                id: "AC-3".to_owned(),
                description: "Mock adapters mirror the staged bead graph.".to_owned(),
                covered_by: vec![VALIDATE_TASK_ID.to_owned()],
            },
        ],
        workstreams: vec![
            Workstream {
                name: "Workspace Assembly".to_owned(),
                description: Some(
                    "Create the temp workspace and persist the scenario milestone plan."
                        .to_owned(),
                ),
                beads: vec![
                    BeadProposal {
                        bead_id: Some(ROOT_EPIC_ID.to_owned()),
                        explicit_id: Some(true),
                        title: "Assemble temp workspace fixture".to_owned(),
                        description: Some(
                            "Parent epic for the scenario-specific temp workspace and milestone setup."
                                .to_owned(),
                        ),
                        bead_type: Some("epic".to_owned()),
                        priority: Some(1),
                        labels: vec!["integration".to_owned(), "scenario".to_owned()],
                        depends_on: Vec::new(),
                        acceptance_criteria: vec!["AC-1".to_owned()],
                        flow_override: None,
                    },
                    BeadProposal {
                        bead_id: Some(PREPARE_TASK_ID.to_owned()),
                        explicit_id: Some(true),
                        title: "Prepare scenario workspace".to_owned(),
                        description: Some(
                            "Seed the temp workspace with milestone artifacts and ready bead state."
                                .to_owned(),
                        ),
                        bead_type: Some("task".to_owned()),
                        priority: Some(1),
                        labels: vec!["integration".to_owned(), "workspace".to_owned()],
                        depends_on: vec![ROOT_EPIC_ID.to_owned()],
                        acceptance_criteria: vec!["AC-2".to_owned()],
                        flow_override: None,
                    },
                ],
            },
            Workstream {
                name: "Validation".to_owned(),
                description: Some(
                    "Exercise the mocked adapters against the prepared workspace.".to_owned(),
                ),
                beads: vec![BeadProposal {
                    bead_id: Some(VALIDATE_TASK_ID.to_owned()),
                    explicit_id: Some(true),
                    title: "Validate mocked adapter responses".to_owned(),
                    description: Some(
                        "Confirm the staged br/bv responses line up with the workspace bead graph."
                            .to_owned(),
                    ),
                    bead_type: Some("task".to_owned()),
                    priority: Some(2),
                    labels: vec!["integration".to_owned(), "mocks".to_owned()],
                    depends_on: vec![ROOT_EPIC_ID.to_owned(), PREPARE_TASK_ID.to_owned()],
                    acceptance_criteria: vec!["AC-3".to_owned()],
                    flow_override: None,
                }],
            },
        ],
        default_flow: FlowPreset::QuickDev,
        agents_guidance: Some(
            "Use the staged milestone artifacts and mock adapter outputs instead of calling real tooling."
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
    let proposals = bundle
        .workstreams
        .iter()
        .flat_map(|workstream| workstream.beads.iter())
        .collect::<Vec<_>>();
    let proposal_lookup = proposals
        .iter()
        .filter_map(|proposal| {
            proposal
                .bead_id
                .as_deref()
                .map(|bead_id| (bead_id, *proposal))
        })
        .collect::<BTreeMap<_, _>>();
    let created_at = scenario_timestamp();
    let actor = "fixture".to_owned();
    [ROOT_EPIC_ID, PREPARE_TASK_ID, VALIDATE_TASK_ID]
        .into_iter()
        .map(|bead_id| {
            let proposal = proposal_lookup
                .get(bead_id)
                .copied()
                .expect("scenario bead proposal must exist");
            let dependencies = proposal
                .depends_on
                .iter()
                .map(|depends_on_id| {
                    let kind = if depends_on_id == ROOT_EPIC_ID {
                        DependencyKind::ParentChild
                    } else {
                        DependencyKind::Blocks
                    };
                    BeadGraphDependency::new(
                        bead_id,
                        depends_on_id.as_str(),
                        kind,
                        created_at,
                        actor.clone(),
                    )
                })
                .collect::<Vec<_>>();

            BeadGraphIssue {
                id: bead_id.to_owned(),
                title: proposal.title.clone(),
                status: BeadStatus::Open,
                priority: BeadPriority::new(proposal.priority.unwrap_or(2)),
                bead_type: match proposal.bead_type.as_deref() {
                    Some("epic") => BeadType::Epic,
                    Some("task") | None => BeadType::Task,
                    Some(other) => BeadType::Other(other.to_owned()),
                },
                labels: proposal.labels.clone(),
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
            }
        })
        .collect()
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

fn build_mock_br_adapter(issues: &[BeadGraphIssue]) -> MockBrAdapter {
    let scenario = ScenarioBrMock::new(issues);
    MockBrAdapter::from_dispatch(move |call| scenario.response_for_call(&call.args))
}

fn build_mock_bv_adapter(issues: &[BeadGraphIssue]) -> MockBvAdapter {
    let prepare_issue = issues
        .iter()
        .find(|issue| issue.id == PREPARE_TASK_ID)
        .expect("prepare task issue");
    let response_json = next_bead_json(prepare_issue);
    MockBvAdapter::from_dispatch(move |call| match call.args.as_slice() {
        [command] if command == "--robot-next" => {
            Some(MockBvResponse::success(response_json.clone()))
        }
        _ => None,
    })
}

fn next_follow_up_task_id(create_index: usize) -> String {
    if create_index == 0 {
        FOLLOW_UP_TASK_ID.to_owned()
    } else {
        format!("{FOLLOW_UP_TASK_ID}-{create_index}")
    }
}

fn follow_up_validation_issue(
    bead_id: &str,
    status: BeadStatus,
    create_index: usize,
) -> BeadGraphIssue {
    let created_at =
        scenario_timestamp() + Duration::minutes(2) + Duration::seconds(create_index as i64);
    BeadGraphIssue {
        id: bead_id.to_owned(),
        title: FOLLOW_UP_TASK_TITLE.to_owned(),
        status: status.clone(),
        priority: BeadPriority::new(2),
        bead_type: BeadType::Task,
        labels: vec!["integration".to_owned(), "follow-up".to_owned()],
        description: Some(
            "Capture a synthetic follow-up task created during scenario execution.".to_owned(),
        ),
        acceptance_criteria: vec![
            "The mocked `br create` flow returns a unique bead id.".to_owned(),
            "A subsequent `br show` can inspect the created bead.".to_owned(),
        ],
        created_at,
        created_by: "fixture".to_owned(),
        updated_at: if status == BeadStatus::Closed {
            created_at + Duration::minutes(1)
        } else {
            created_at
        },
        source_repo: ".".to_owned(),
        compaction_level: 0,
        original_size: 0,
        dependencies: vec![BeadGraphDependency::new(
            bead_id,
            VALIDATE_TASK_ID,
            DependencyKind::Blocks,
            created_at,
            "fixture",
        )],
        closed_at: (status == BeadStatus::Closed).then_some(created_at + Duration::minutes(1)),
        close_reason: (status == BeadStatus::Closed).then_some("Fixture cleanup".to_owned()),
    }
}

fn br_list_json(issues: &[BeadGraphIssue]) -> String {
    serde_json::to_string(
        &issues
            .iter()
            .map(|issue| {
                json!({
                    "id": issue.id,
                    "title": issue.title,
                    "status": issue.status.to_string(),
                    "priority": issue.priority.value(),
                    "issue_type": issue.bead_type.to_string(),
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
        "status": issue.status.to_string(),
        "priority": issue.priority.value(),
        "issue_type": issue.bead_type.to_string(),
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
                "status": linked_issue.status.to_string(),
            })
        }).collect::<Vec<_>>(),
        "dependents": issues
            .iter()
            .filter_map(|candidate| {
                candidate
                    .dependencies
                    .iter()
                    .find(|dependency| dependency.depends_on_id == issue.id)
                    .map(|dependency| {
                        json!({
                            "id": candidate.id,
                            "dependency_type": dependency_kind_json(&dependency.kind),
                            "title": candidate.title,
                            "status": candidate.status.to_string(),
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

#[derive(Debug, Clone)]
struct ScenarioBrMock {
    issues: Vec<BeadGraphIssue>,
    state: Arc<Mutex<ScenarioBrState>>,
}

impl ScenarioBrMock {
    fn new(issues: &[BeadGraphIssue]) -> Self {
        Self {
            issues: issues.to_vec(),
            state: Arc::new(Mutex::new(ScenarioBrState::default())),
        }
    }

    fn response_for_call(&self, args: &[String]) -> Option<MockBrResponse> {
        match args {
            [command, flag] if command == "list" && flag == "--json" => Some(
                MockBrResponse::success(br_list_json(&self.current_issues())),
            ),
            [command, bead_id, flag] if command == "show" && flag == "--json" => {
                Some(self.show_response(bead_id))
            }
            [command, title, bead_type, priority]
                if command == "create"
                    && title == "--title=Follow-up validation bead"
                    && bead_type == "--type=task"
                    && priority == "--priority=2" =>
            {
                Some(self.create_response())
            }
            [command, bead_id, reason]
                if command == "close" && reason == "--reason=Fixture cleanup" =>
            {
                Some(self.close_response(bead_id))
            }
            [command, flag] if command == "sync" && flag == "--flush-only" => {
                Some(MockBrResponse::success("synced"))
            }
            _ => None,
        }
    }

    fn show_response(&self, bead_id: &str) -> MockBrResponse {
        let issues = self.current_issues();
        let Some(issue) = issues.iter().find(|issue| issue.id == bead_id).cloned() else {
            return MockBrResponse::exit_failure(1, format!("bead not found: {bead_id}"));
        };

        MockBrResponse::success(br_show_json(&issue, &issues))
    }

    fn create_response(&self) -> MockBrResponse {
        let bead_id = self
            .state
            .lock()
            .expect("scenario follow-up state lock poisoned")
            .create_follow_up();
        MockBrResponse::success(format!("Created bead {bead_id}"))
    }

    fn close_response(&self, bead_id: &str) -> MockBrResponse {
        match self
            .state
            .lock()
            .expect("scenario follow-up state lock poisoned")
            .close_follow_up(bead_id)
        {
            Ok(()) => MockBrResponse::success(format!("Closed {bead_id}")),
            Err(message) => MockBrResponse::exit_failure(1, message),
        }
    }

    fn current_issues(&self) -> Vec<BeadGraphIssue> {
        let mut issues = self.issues.clone();
        issues.extend(
            self.state
                .lock()
                .expect("scenario follow-up state lock poisoned")
                .created_issues(),
        );
        issues
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::adapters::br_models::{BeadDetail, BeadSummary};
    use crate::adapters::br_process::BrCommand;
    use crate::adapters::bv_process::{BvCommand, NextBeadResponse};
    use crate::contexts::milestone_record::model::{MilestoneEventType, MilestoneStatus};

    fn created_bead_id(stdout: &str) -> String {
        stdout
            .strip_prefix("Created bead ")
            .expect("created bead output prefix")
            .to_owned()
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
        assert!(fixture.workspace.audit_root().join("projects").is_dir());
        assert!(fixture.workspace.live_root().join("projects").is_dir());

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
                    ROOT_EPIC_ID.to_owned(),
                    "Assemble temp workspace fixture".to_owned()
                ),
                (
                    PREPARE_TASK_ID.to_owned(),
                    "Prepare scenario workspace".to_owned()
                ),
                (
                    VALIDATE_TASK_ID.to_owned(),
                    "Validate mocked adapter responses".to_owned(),
                ),
            ]
            .into_iter()
            .collect::<BTreeSet<_>>()
        );

        let raw_beads = fs::read_to_string(fixture.workspace.beads_root().join("issues.jsonl"))
            .expect("read bead graph");
        let bead_rows = raw_beads
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("parse bead json"))
            .collect::<Vec<_>>();
        assert_eq!(bead_rows.len(), 3);
        assert_eq!(
            bead_rows
                .iter()
                .map(|row| row["id"].as_str().expect("string bead id").to_owned())
                .collect::<BTreeSet<_>>(),
            fixture.bead_ids.iter().cloned().collect::<BTreeSet<_>>()
        );

        let validate_row = bead_rows
            .iter()
            .find(|row| row["id"] == VALIDATE_TASK_ID)
            .expect("validate task row");
        assert_eq!(validate_row["status"], "open");
        assert_eq!(
            validate_row["dependencies"].as_array().map(Vec::len),
            Some(2)
        );
        let prepare_row = bead_rows
            .iter()
            .find(|row| row["id"] == PREPARE_TASK_ID)
            .expect("prepare task row");
        assert_eq!(
            prepare_row["dependencies"][0]["type"].as_str(),
            Some("parent_child")
        );
        assert_eq!(
            validate_row["dependencies"]
                .as_array()
                .expect("validate dependencies")
                .iter()
                .map(|dependency| dependency["type"].as_str().expect("dependency type"))
                .collect::<BTreeSet<_>>(),
            ["blocks", "parent_child"]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        let working_dir = fixture.workspace.path().to_path_buf();
        let br = fixture
            .mock_br
            .as_br_adapter()
            .with_working_dir(working_dir.clone());
        let bv = fixture
            .mock_bv
            .as_bv_adapter()
            .with_working_dir(working_dir);

        let prepare_detail: BeadDetail = br
            .exec_json(&BrCommand::show(PREPARE_TASK_ID))
            .await
            .expect("br show");
        assert_eq!(prepare_detail.id, PREPARE_TASK_ID);
        assert_eq!(prepare_detail.dependencies.len(), 1);
        assert_eq!(prepare_detail.dependencies[0].id, ROOT_EPIC_ID);
        assert_eq!(prepare_detail.dependents.len(), 1);
        assert_eq!(prepare_detail.dependents[0].id, VALIDATE_TASK_ID);
        assert_eq!(prepare_detail.dependents[0].kind, DependencyKind::Blocks);

        let listed: Vec<BeadSummary> = br.exec_json(&BrCommand::list()).await.expect("br list");
        assert_eq!(listed.len(), 3);
        assert_eq!(listed[0].id, ROOT_EPIC_ID);
        assert_eq!(listed[1].id, PREPARE_TASK_ID);
        assert_eq!(listed[2].id, VALIDATE_TASK_ID);

        let root_detail: BeadDetail = br
            .exec_json(&BrCommand::show(ROOT_EPIC_ID))
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
                (PREPARE_TASK_ID.to_owned(), "parent_child".to_owned()),
                (VALIDATE_TASK_ID.to_owned(), "parent_child".to_owned()),
            ]
            .into_iter()
            .collect::<BTreeSet<_>>()
        );

        let validate_detail: BeadDetail = br
            .exec_json(&BrCommand::show(VALIDATE_TASK_ID))
            .await
            .expect("br show validate");
        assert_eq!(validate_detail.id, VALIDATE_TASK_ID);
        assert_eq!(validate_detail.dependencies.len(), 2);
        assert_eq!(validate_detail.dependencies[0].id, ROOT_EPIC_ID);
        assert_eq!(validate_detail.dependencies[1].id, PREPARE_TASK_ID);
        assert!(validate_detail.dependents.is_empty());

        let repeated_prepare_detail: BeadDetail = br
            .exec_json(&BrCommand::show(PREPARE_TASK_ID))
            .await
            .expect("repeated br show");
        assert_eq!(repeated_prepare_detail, prepare_detail);

        let create_output = br
            .exec_read(&BrCommand::create("Follow-up validation bead", "task", "2"))
            .await
            .expect("br create");
        let first_follow_up_id = created_bead_id(&create_output.stdout);
        assert_eq!(first_follow_up_id, FOLLOW_UP_TASK_ID);

        let listed_with_follow_up: Vec<BeadSummary> = br
            .exec_json(&BrCommand::list())
            .await
            .expect("br list after create");
        assert_eq!(listed_with_follow_up.len(), 4);
        assert_eq!(listed_with_follow_up[3].id, first_follow_up_id);
        assert_eq!(listed_with_follow_up[3].status, BeadStatus::Open);

        let created_detail: BeadDetail = br
            .exec_json(&BrCommand::show(&first_follow_up_id))
            .await
            .expect("br show created bead");
        assert_eq!(created_detail.id, first_follow_up_id);
        assert_eq!(created_detail.title, FOLLOW_UP_TASK_TITLE);
        assert_eq!(created_detail.dependencies.len(), 1);
        assert_eq!(created_detail.dependencies[0].id, VALIDATE_TASK_ID);
        assert_eq!(created_detail.status, BeadStatus::Open);

        let second_create_output = br
            .exec_read(&BrCommand::create("Follow-up validation bead", "task", "2"))
            .await
            .expect("second br create");
        let second_follow_up_id = created_bead_id(&second_create_output.stdout);
        assert_eq!(second_follow_up_id, next_follow_up_task_id(1));
        assert_ne!(second_follow_up_id, first_follow_up_id);

        let listed_with_two_follow_ups: Vec<BeadSummary> = br
            .exec_json(&BrCommand::list())
            .await
            .expect("br list after second create");
        assert_eq!(listed_with_two_follow_ups.len(), 5);
        assert_eq!(listed_with_two_follow_ups[3].id, first_follow_up_id);
        assert_eq!(listed_with_two_follow_ups[4].id, second_follow_up_id);
        assert_eq!(listed_with_two_follow_ups[4].status, BeadStatus::Open);

        let second_created_detail: BeadDetail = br
            .exec_json(&BrCommand::show(&second_follow_up_id))
            .await
            .expect("br show second created bead");
        assert_eq!(second_created_detail.id, second_follow_up_id);
        assert_eq!(second_created_detail.title, FOLLOW_UP_TASK_TITLE);
        assert_eq!(second_created_detail.dependencies.len(), 1);
        assert_eq!(second_created_detail.dependencies[0].id, VALIDATE_TASK_ID);
        assert_eq!(second_created_detail.status, BeadStatus::Open);

        let validate_after_create: BeadDetail = br
            .exec_json(&BrCommand::show(VALIDATE_TASK_ID))
            .await
            .expect("br show validate after create");
        assert_eq!(validate_after_create.dependents.len(), 2);
        assert_eq!(
            validate_after_create
                .dependents
                .iter()
                .map(|dependency| {
                    (
                        dependency.id.clone(),
                        dependency.kind.to_string(),
                        dependency.status.as_ref().map(ToString::to_string),
                    )
                })
                .collect::<BTreeSet<_>>(),
            [
                (
                    first_follow_up_id.clone(),
                    DependencyKind::Blocks.to_string(),
                    Some(BeadStatus::Open.to_string()),
                ),
                (
                    second_follow_up_id.clone(),
                    DependencyKind::Blocks.to_string(),
                    Some(BeadStatus::Open.to_string()),
                ),
            ]
            .into_iter()
            .collect::<BTreeSet<_>>()
        );

        let close_output = br
            .exec_read(&BrCommand::close(&first_follow_up_id, "Fixture cleanup"))
            .await
            .expect("br close");
        assert!(close_output.stdout.contains(&first_follow_up_id));

        let closed_detail: BeadDetail = br
            .exec_json(&BrCommand::show(&first_follow_up_id))
            .await
            .expect("br show closed bead");
        assert_eq!(closed_detail.id, first_follow_up_id);
        assert_eq!(closed_detail.status, BeadStatus::Closed);

        let validate_after_close: BeadDetail = br
            .exec_json(&BrCommand::show(VALIDATE_TASK_ID))
            .await
            .expect("br show validate after close");
        assert_eq!(validate_after_close.dependents.len(), 2);
        assert_eq!(
            validate_after_close
                .dependents
                .iter()
                .map(|dependency| {
                    (
                        dependency.id.clone(),
                        dependency.status.as_ref().map(ToString::to_string),
                    )
                })
                .collect::<BTreeSet<_>>(),
            [
                (
                    first_follow_up_id.clone(),
                    Some(BeadStatus::Closed.to_string()),
                ),
                (
                    second_follow_up_id.clone(),
                    Some(BeadStatus::Open.to_string()),
                ),
            ]
            .into_iter()
            .collect::<BTreeSet<_>>()
        );

        let listed_after_close: Vec<BeadSummary> = br
            .exec_json(&BrCommand::list())
            .await
            .expect("br list after close");
        assert_eq!(listed_after_close.len(), 5);
        assert_eq!(listed_after_close[3].id, first_follow_up_id);
        assert_eq!(listed_after_close[3].status, BeadStatus::Closed);
        assert_eq!(listed_after_close[4].id, second_follow_up_id);
        assert_eq!(listed_after_close[4].status, BeadStatus::Open);

        let sync_output = br
            .exec_read(&BrCommand::sync_flush())
            .await
            .expect("br sync");
        assert_eq!(sync_output.stdout, "synced");

        let next_bead: NextBeadResponse = bv
            .exec_json(&BvCommand::robot_next())
            .await
            .expect("bv robot-next");
        assert_eq!(next_bead.id, PREPARE_TASK_ID);
        assert_eq!(next_bead.title, "Prepare scenario workspace");
        let repeated_next_bead: NextBeadResponse = bv
            .exec_json(&BvCommand::robot_next())
            .await
            .expect("repeated bv robot-next");
        assert_eq!(repeated_next_bead.id, next_bead.id);
        assert_eq!(repeated_next_bead.title, next_bead.title);

        assert_eq!(
            fixture
                .mock_br
                .calls()
                .iter()
                .map(|call| call.args.clone())
                .collect::<Vec<_>>(),
            vec![
                vec![
                    "show".to_owned(),
                    PREPARE_TASK_ID.to_owned(),
                    "--json".to_owned()
                ],
                vec!["list".to_owned(), "--json".to_owned()],
                vec![
                    "show".to_owned(),
                    ROOT_EPIC_ID.to_owned(),
                    "--json".to_owned()
                ],
                vec![
                    "show".to_owned(),
                    VALIDATE_TASK_ID.to_owned(),
                    "--json".to_owned()
                ],
                vec![
                    "show".to_owned(),
                    PREPARE_TASK_ID.to_owned(),
                    "--json".to_owned()
                ],
                vec![
                    "create".to_owned(),
                    "--title=Follow-up validation bead".to_owned(),
                    "--type=task".to_owned(),
                    "--priority=2".to_owned(),
                ],
                vec!["list".to_owned(), "--json".to_owned()],
                vec![
                    "show".to_owned(),
                    first_follow_up_id.clone(),
                    "--json".to_owned()
                ],
                vec![
                    "create".to_owned(),
                    "--title=Follow-up validation bead".to_owned(),
                    "--type=task".to_owned(),
                    "--priority=2".to_owned(),
                ],
                vec!["list".to_owned(), "--json".to_owned()],
                vec![
                    "show".to_owned(),
                    second_follow_up_id.clone(),
                    "--json".to_owned()
                ],
                vec![
                    "show".to_owned(),
                    VALIDATE_TASK_ID.to_owned(),
                    "--json".to_owned()
                ],
                vec![
                    "close".to_owned(),
                    first_follow_up_id.clone(),
                    "--reason=Fixture cleanup".to_owned(),
                ],
                vec![
                    "show".to_owned(),
                    first_follow_up_id.clone(),
                    "--json".to_owned()
                ],
                vec![
                    "show".to_owned(),
                    VALIDATE_TASK_ID.to_owned(),
                    "--json".to_owned()
                ],
                vec!["list".to_owned(), "--json".to_owned()],
                vec!["sync".to_owned(), "--flush-only".to_owned()],
            ]
        );
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
            ]
        );
    }
}
