use std::cell::RefCell;
use std::collections::BTreeSet;
use std::path::Path;

use chrono::Utc;
use tempfile::tempdir;

use ralph_burning::adapters::br_models::{
    BeadDetail, BeadPriority, BeadStatus, BeadType, DepTreeNode, DependencyKind, DependencyRef,
};
use ralph_burning::contexts::bead_workflow::create_project::{
    create_project_from_bead, BeadProjectBrPort, BeadProjectCreationError,
    CreateProjectFromBeadInput, FeatureBranchPort,
};
use ralph_burning::contexts::bead_workflow::project_prompt::{
    render_project_prompt_from_bead, BeadPromptBrPort, BeadPromptReadError,
};
use ralph_burning::contexts::project_run_record::model::{
    JournalEvent, ProjectRecord, RunSnapshot, SessionStore,
};
use ralph_burning::contexts::project_run_record::service::{JournalStorePort, ProjectStorePort};
use ralph_burning::shared::domain::{FlowPreset, ProjectId};
use ralph_burning::shared::error::{AppError, AppResult};

struct MockBr {
    detail: Result<BeadDetail, MockReadError>,
    dep_tree: Result<Vec<DepTreeNode>, MockReadError>,
    update_result: RefCell<Result<(), String>>,
    updates: RefCell<Vec<String>>,
}

#[derive(Clone)]
enum MockReadError {
    NotFound,
}

impl BeadPromptBrPort for MockBr {
    async fn bead_show(&self, bead_id: &str) -> Result<BeadDetail, BeadPromptReadError> {
        self.detail.clone().map_err(|error| match error {
            MockReadError::NotFound => BeadPromptReadError::NotFound {
                bead_id: bead_id.to_owned(),
            },
        })
    }

    async fn bead_dep_tree(&self, bead_id: &str) -> Result<Vec<DepTreeNode>, BeadPromptReadError> {
        self.dep_tree.clone().map_err(|error| match error {
            MockReadError::NotFound => BeadPromptReadError::NotFound {
                bead_id: bead_id.to_owned(),
            },
        })
    }
}

impl BeadProjectBrPort for MockBr {
    async fn mark_bead_in_progress(&self, bead_id: &str) -> Result<(), String> {
        self.updates
            .borrow_mut()
            .push(format!("{bead_id}:in_progress"));
        self.update_result.borrow().clone()
    }
}

#[derive(Default)]
struct FakeProjectStore {
    existing_ids: BTreeSet<String>,
    captured: RefCell<Option<CapturedCreate>>,
}

#[derive(Clone)]
struct CapturedCreate {
    record: ProjectRecord,
    prompt_contents: String,
    run_snapshot: RunSnapshot,
    initial_journal_line: String,
}

impl ProjectStorePort for FakeProjectStore {
    fn project_exists(&self, _base_dir: &Path, project_id: &ProjectId) -> AppResult<bool> {
        Ok(self.existing_ids.contains(project_id.as_str()))
    }

    fn read_project_record(
        &self,
        _base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<ProjectRecord> {
        self.captured
            .borrow()
            .as_ref()
            .filter(|captured| captured.record.id == *project_id)
            .map(|captured| captured.record.clone())
            .ok_or_else(|| AppError::ProjectNotFound {
                project_id: project_id.to_string(),
            })
    }

    fn list_project_ids(&self, _base_dir: &Path) -> AppResult<Vec<ProjectId>> {
        self.existing_ids
            .iter()
            .map(|id| ProjectId::new(id.as_str()))
            .collect()
    }

    fn stage_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
        Ok(())
    }

    fn commit_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
        Ok(())
    }

    fn rollback_delete(&self, _base_dir: &Path, _project_id: &ProjectId) -> AppResult<()> {
        Ok(())
    }

    fn create_project_atomic(
        &self,
        _base_dir: &Path,
        record: &ProjectRecord,
        prompt_contents: &str,
        run_snapshot: &RunSnapshot,
        initial_journal_line: &str,
        _sessions: &SessionStore,
    ) -> AppResult<()> {
        self.captured.replace(Some(CapturedCreate {
            record: record.clone(),
            prompt_contents: prompt_contents.to_owned(),
            run_snapshot: run_snapshot.clone(),
            initial_journal_line: initial_journal_line.to_owned(),
        }));
        Ok(())
    }
}

struct FakeJournalStore;

impl JournalStorePort for FakeJournalStore {
    fn read_journal(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
    ) -> AppResult<Vec<JournalEvent>> {
        Ok(vec![])
    }

    fn append_event(
        &self,
        _base_dir: &Path,
        _project_id: &ProjectId,
        _line: &str,
    ) -> AppResult<()> {
        Ok(())
    }
}

struct FakeBranchPort;

impl FeatureBranchPort for FakeBranchPort {
    fn create_branch(&self, _base_dir: &Path, _branch_name: &str) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Default)]
struct RecordingBranchPort {
    branches: RefCell<Vec<String>>,
}

impl FeatureBranchPort for RecordingBranchPort {
    fn create_branch(&self, _base_dir: &Path, branch_name: &str) -> Result<(), String> {
        self.branches.borrow_mut().push(branch_name.to_owned());
        Ok(())
    }
}

#[tokio::test]
async fn create_project_from_bead_happy_path_uses_renderer_and_marks_in_progress(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempdir()?;
    let br = mock_br(bead_detail(
        "ralph-burning-d31l",
        "Create project from bead",
        BeadStatus::Open,
        Some("Wire the CLI to the renderer."),
        vec!["Project is created".to_owned()],
        vec![],
    ));
    let store = FakeProjectStore::default();

    let output = create_project_from_bead(
        &store,
        &FakeJournalStore,
        &br,
        &FakeBranchPort,
        temp.path(),
        input("ralph-burning-d31l"),
    )
    .await?;

    assert_eq!(output.project.id.as_str(), "d31l");
    assert_eq!(
        br.updates.borrow().as_slice(),
        ["ralph-burning-d31l:in_progress"]
    );

    let captured = store.captured.borrow().clone().expect("project captured");
    let expected_prompt =
        render_project_prompt_from_bead("ralph-burning-d31l", &br, temp.path().to_path_buf())
            .await?;
    assert_eq!(captured.prompt_contents, expected_prompt);
    assert!(captured
        .prompt_contents
        .contains("# Work Item: ralph-burning-d31l - Create project from bead"));
    assert_eq!(
        captured.record.name,
        "ralph-burning-d31l: Create project from bead"
    );
    assert_eq!(captured.run_snapshot.status_summary, "not started");
    assert!(captured
        .initial_journal_line
        .contains("\"project_id\":\"d31l\""));
    Ok(())
}

#[tokio::test]
async fn create_project_from_bead_does_not_create_branch_by_default() {
    let temp = tempdir().unwrap();
    let br = mock_br(bead_detail(
        "ralph-burning-d31l",
        "Create project from bead",
        BeadStatus::Open,
        Some("Work."),
        vec![],
        vec![],
    ));
    let store = FakeProjectStore::default();
    let branch_port = RecordingBranchPort::default();

    create_project_from_bead(
        &store,
        &FakeJournalStore,
        &br,
        &branch_port,
        temp.path(),
        input("ralph-burning-d31l"),
    )
    .await
    .expect("create project");

    assert!(branch_port.branches.borrow().is_empty());
}

#[tokio::test]
async fn create_project_from_bead_derives_branch_when_requested_without_name() {
    let temp = tempdir().unwrap();
    let br = mock_br(bead_detail(
        "ralph-burning-d31l",
        "Create Project From Bead!",
        BeadStatus::Open,
        Some("Work."),
        vec![],
        vec![],
    ));
    let store = FakeProjectStore::default();
    let branch_port = RecordingBranchPort::default();
    let mut create_input = input("ralph-burning-d31l");
    create_input.branch = Some(String::new());

    create_project_from_bead(
        &store,
        &FakeJournalStore,
        &br,
        &branch_port,
        temp.path(),
        create_input,
    )
    .await
    .expect("create project");

    assert_eq!(
        branch_port.branches.borrow().as_slice(),
        ["feat/ralph-burning-d31l-create-project-from-bead"]
    );
}

#[tokio::test]
async fn create_project_from_bead_closed_bead_fails_without_project() {
    let temp = tempdir().unwrap();
    let br = mock_br(bead_detail(
        "ralph-burning-d31l",
        "Closed",
        BeadStatus::Closed,
        Some("Done."),
        vec![],
        vec![],
    ));
    let store = FakeProjectStore::default();

    let error = create_project_from_bead(
        &store,
        &FakeJournalStore,
        &br,
        &FakeBranchPort,
        temp.path(),
        input("ralph-burning-d31l"),
    )
    .await
    .expect_err("closed bead should fail");

    assert!(error.to_string().contains("closed bead"));
    assert!(store.captured.borrow().is_none());
    assert!(br.updates.borrow().is_empty());
}

#[tokio::test]
async fn create_project_from_bead_open_blockers_fail_and_list_blockers() {
    let temp = tempdir().unwrap();
    let br = mock_br(bead_detail(
        "ralph-burning-d31l",
        "Blocked",
        BeadStatus::Open,
        Some("Blocked work."),
        vec![],
        vec![dependency(
            "ralph-burning-aaaa",
            "Blocking bead",
            BeadStatus::Open,
        )],
    ));
    let store = FakeProjectStore::default();

    let error = create_project_from_bead(
        &store,
        &FakeJournalStore,
        &br,
        &FakeBranchPort,
        temp.path(),
        input("ralph-burning-d31l"),
    )
    .await
    .expect_err("blocked bead should fail");

    let message = error.to_string();
    assert!(message.contains("open blockers"));
    assert!(message.contains("ralph-burning-aaaa"));
    assert!(message.contains("Blocking bead"));
    assert!(store.captured.borrow().is_none());
}

#[tokio::test]
async fn create_project_from_bead_missing_bead_fails_without_project() {
    let temp = tempdir().unwrap();
    let br = MockBr {
        detail: Err(MockReadError::NotFound),
        dep_tree: Ok(dep_tree("missing", "Missing", vec![])),
        update_result: RefCell::new(Ok(())),
        updates: RefCell::new(vec![]),
    };
    let store = FakeProjectStore::default();

    let error = create_project_from_bead(
        &store,
        &FakeJournalStore,
        &br,
        &FakeBranchPort,
        temp.path(),
        input("missing"),
    )
    .await
    .expect_err("missing bead should fail");

    assert!(error.to_string().contains("bead 'missing' was not found"));
    assert!(store.captured.borrow().is_none());
}

#[tokio::test]
async fn create_project_from_bead_project_id_collision_points_to_resume() {
    let temp = tempdir().unwrap();
    let br = mock_br(bead_detail(
        "ralph-burning-d31l",
        "Collision",
        BeadStatus::Open,
        Some("Work."),
        vec![],
        vec![],
    ));
    let store = FakeProjectStore {
        existing_ids: BTreeSet::from(["d31l".to_owned()]),
        captured: RefCell::new(None),
    };

    let error = create_project_from_bead(
        &store,
        &FakeJournalStore,
        &br,
        &FakeBranchPort,
        temp.path(),
        input("ralph-burning-d31l"),
    )
    .await
    .expect_err("project collision should fail");

    assert!(matches!(
        error,
        BeadProjectCreationError::ProjectExists { .. }
    ));
    assert!(error.to_string().contains("run resume"));
    assert!(store.captured.borrow().is_none());
    assert!(br.updates.borrow().is_empty());
}

#[tokio::test]
async fn create_project_from_bead_status_update_failure_reports_partial_success() {
    let temp = tempdir().unwrap();
    let br = MockBr {
        detail: Ok(bead_detail(
            "ralph-burning-d31l",
            "Partial",
            BeadStatus::Open,
            Some("Work."),
            vec![],
            vec![],
        )),
        dep_tree: Ok(dep_tree("ralph-burning-d31l", "Partial", vec![])),
        update_result: RefCell::new(Err("br update failed".to_owned())),
        updates: RefCell::new(vec![]),
    };
    let store = FakeProjectStore::default();

    let error = create_project_from_bead(
        &store,
        &FakeJournalStore,
        &br,
        &FakeBranchPort,
        temp.path(),
        input("ralph-burning-d31l"),
    )
    .await
    .expect_err("status update failure should report partial state");

    assert!(store.captured.borrow().is_some());
    let message = error.to_string();
    assert!(message.contains("project 'd31l' was created"));
    assert!(message.contains("br update ralph-burning-d31l --status=in_progress"));
    assert!(message.contains("br update failed"));
}

fn input(bead_id: &str) -> CreateProjectFromBeadInput {
    CreateProjectFromBeadInput {
        bead_id: bead_id.to_owned(),
        flow: FlowPreset::Minimal,
        branch: None,
        created_at: Utc::now(),
        prior_failure_context: None,
    }
}

fn mock_br(detail: BeadDetail) -> MockBr {
    let dep_tree = dep_tree(&detail.id, &detail.title, vec![]);
    MockBr {
        detail: Ok(detail),
        dep_tree: Ok(dep_tree),
        update_result: RefCell::new(Ok(())),
        updates: RefCell::new(vec![]),
    }
}

fn bead_detail(
    id: &str,
    title: &str,
    status: BeadStatus,
    description: Option<&str>,
    acceptance_criteria: Vec<String>,
    blockers: Vec<DependencyRef>,
) -> BeadDetail {
    BeadDetail {
        id: id.to_owned(),
        title: title.to_owned(),
        status,
        priority: BeadPriority::new(1),
        bead_type: BeadType::Task,
        labels: vec![],
        description: description.map(ToOwned::to_owned),
        acceptance_criteria,
        dependencies: blockers,
        dependents: vec![],
        comments: vec![],
        owner: None,
        created_at: None,
        updated_at: None,
    }
}

fn dependency(id: &str, title: &str, status: BeadStatus) -> DependencyRef {
    DependencyRef {
        id: id.to_owned(),
        kind: DependencyKind::Blocks,
        title: Some(title.to_owned()),
        status: Some(status),
    }
}

fn dep_tree(id: &str, title: &str, children: Vec<DepTreeNode>) -> Vec<DepTreeNode> {
    let mut nodes = vec![DepTreeNode {
        id: id.to_owned(),
        title: title.to_owned(),
        status: "open".to_owned(),
        depth: 0,
        parent_id: None,
        priority: Some(1),
        truncated: false,
        children: vec![],
    }];
    nodes.extend(children);
    nodes
}
