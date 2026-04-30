use std::path::PathBuf;

use tempfile::tempdir;

use ralph_burning::adapters::br_models::{
    BeadDetail, BeadPriority, BeadStatus, BeadType, DepTreeNode, DependencyKind, DependencyRef,
};
use ralph_burning::contexts::bead_workflow::project_prompt::{
    render_project_prompt_from_bead, BeadPromptBrPort, BeadPromptReadError,
    ORCHESTRATION_STATE_EXCLUSION_BOILERPLATE,
};

#[derive(Clone)]
struct MockBeadPromptBr {
    detail: Result<BeadDetail, MockReadError>,
    dep_tree: Result<Vec<DepTreeNode>, MockReadError>,
}

#[derive(Clone)]
enum MockReadError {
    NotFound,
}

impl BeadPromptBrPort for MockBeadPromptBr {
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

#[tokio::test]
async fn render_project_prompt_happy_path_in_expected_order(
) -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempdir()?;
    std::fs::write(temp.path().join("AGENTS.md"), "# norms")?;
    std::fs::write(temp.path().join("CLAUDE.md"), "# claude")?;
    let br = MockBeadPromptBr {
        detail: Ok(bead_detail(
            "ralph-burning-4wdb",
            "Render project prompt from bead",
            BeadStatus::Open,
            Some(
                "Add renderer support.\n\nWhere to look:\n- `src/adapters/br_process.rs`\n- `tests/unit/`",
            ),
            vec!["Renderer is pure".to_owned(), "Tests cover graph".to_owned()],
            vec![
                dependency(
                    "ralph-burning-parent",
                    "Drain loop foundation",
                    DependencyKind::ParentChild,
                    Some(BeadStatus::Open),
                ),
                dependency(
                    "ralph-burning-done",
                    "Milestone bundle path",
                    DependencyKind::Blocks,
                    None,
                ),
            ],
            vec![dependency(
                "ralph-burning-next",
                "Create project from bead",
                DependencyKind::Blocks,
                Some(BeadStatus::Open),
            )],
        )),
        dep_tree: Ok(dep_tree(
            "ralph-burning-4wdb",
            "Render project prompt from bead",
            vec![dep_tree_child(
                "ralph-burning-4wdb",
                "ralph-burning-done",
                "Milestone bundle path",
                "closed",
            )],
        )),
    };

    let prompt =
        render_project_prompt_from_bead("ralph-burning-4wdb", &br, temp.path().to_path_buf())
            .await?;

    assert_in_order(
        &prompt,
        &[
            "# Work Item: ralph-burning-4wdb - Render project prompt from bead",
            "## Goal",
            "Add renderer support.",
            "## Acceptance Criteria",
            "- Renderer is pure",
            "## Nearby Graph Context",
            "ralph-burning-parent - Drain loop foundation",
            "ralph-burning-done - Milestone bundle path",
            "ralph-burning-next - Create project from bead",
            "## Where to Look",
            "`src/adapters/br_process.rs`",
            "`tests/unit/`",
            "## IMPORTANT: Exclude orchestration state from review scope",
            ORCHESTRATION_STATE_EXCLUSION_BOILERPLATE,
            "## Repository Norms",
            "`AGENTS.md`",
            "`CLAUDE.md`",
        ],
    );
    Ok(())
}

#[tokio::test]
async fn render_project_prompt_omits_empty_acceptance_criteria(
) -> Result<(), Box<dyn std::error::Error>> {
    let br = MockBeadPromptBr {
        detail: Ok(bead_detail(
            "bead-no-ac",
            "No AC",
            BeadStatus::Open,
            Some("Implement the work."),
            vec![],
            vec![],
            vec![],
        )),
        dep_tree: Ok(dep_tree("bead-no-ac", "No AC", vec![])),
    };

    let prompt = render_project_prompt_from_bead("bead-no-ac", &br, PathBuf::from("/nope")).await?;

    assert!(
        !prompt.contains("## Acceptance Criteria"),
        "empty acceptance criteria should not render an empty section"
    );
    assert!(prompt.contains("## Nearby Graph Context"));
    Ok(())
}

#[tokio::test]
async fn render_project_prompt_with_no_nearby_graph_stays_well_formed(
) -> Result<(), Box<dyn std::error::Error>> {
    let br = MockBeadPromptBr {
        detail: Ok(bead_detail(
            "bead-leaf",
            "Leaf",
            BeadStatus::InProgress,
            Some("Implement local behavior."),
            vec![],
            vec![],
            vec![],
        )),
        dep_tree: Ok(dep_tree("bead-leaf", "Leaf", vec![])),
    };

    let prompt = render_project_prompt_from_bead("bead-leaf", &br, PathBuf::from("/nope")).await?;

    assert!(prompt.contains("- **Parent:**\n  - None."));
    assert!(prompt.contains("- **Closed blockers:**\n  - None."));
    assert!(prompt.contains("- **Direct dependents:**\n  - None."));
    assert!(prompt.contains(ORCHESTRATION_STATE_EXCLUSION_BOILERPLATE));
    Ok(())
}

#[tokio::test]
async fn render_project_prompt_does_not_treat_slash_prose_as_path_pointers(
) -> Result<(), Box<dyn std::error::Error>> {
    let br = MockBeadPromptBr {
        detail: Ok(bead_detail(
            "bead-paths",
            "Path extraction",
            BeadStatus::Open,
            Some(
                "Where to look:\n- src/contexts/bead_workflow/project_prompt.rs\n\nCoordinate backend / frontend review/fix notes.",
            ),
            vec![],
            vec![],
            vec![],
        )),
        dep_tree: Ok(dep_tree("bead-paths", "Path extraction", vec![])),
    };

    let prompt = render_project_prompt_from_bead("bead-paths", &br, PathBuf::from("/nope")).await?;

    assert!(prompt.contains("## Where to Look"));
    assert!(prompt.contains("`src/contexts/bead_workflow/project_prompt.rs`"));
    assert!(
        !prompt.contains("- `/`"),
        "slash separators should not be rendered as path pointers"
    );
    assert!(
        !prompt.contains("`review/fix`"),
        "slash-separated prose should not be rendered as a path pointer"
    );
    Ok(())
}

#[tokio::test]
async fn render_project_prompt_omits_where_to_look_for_norms_slash_prose(
) -> Result<(), Box<dyn std::error::Error>> {
    let br = MockBeadPromptBr {
        detail: Ok(bead_detail(
            "bead-prose",
            "Prose",
            BeadStatus::Open,
            Some("Use AGENTS.md / CLAUDE.md for norms and coordinate docs/review.md notes."),
            vec![],
            vec![],
            vec![],
        )),
        dep_tree: Ok(dep_tree("bead-prose", "Prose", vec![])),
    };

    let prompt = render_project_prompt_from_bead("bead-prose", &br, PathBuf::from("/nope")).await?;

    assert!(
        !prompt.contains("## Where to Look"),
        "bare filename/path-like prose outside an explicit path section should not render pointers"
    );
    Ok(())
}

#[tokio::test]
async fn render_project_prompt_returns_open_blocker_error() {
    let br = MockBeadPromptBr {
        detail: Ok(bead_detail(
            "bead-blocked",
            "Blocked",
            BeadStatus::Open,
            Some("Implement blocked work."),
            vec![],
            vec![dependency(
                "bead-open-blocker",
                "Still open",
                DependencyKind::Blocks,
                Some(BeadStatus::Open),
            )],
            vec![],
        )),
        dep_tree: Ok(dep_tree("bead-blocked", "Blocked", vec![])),
    };

    let error = render_project_prompt_from_bead("bead-blocked", &br, PathBuf::from("/nope"))
        .await
        .expect_err("open blockers should refuse rendering");

    let message = error.to_string();
    assert!(message.contains("open blockers"));
    assert!(message.contains("bead-open-blocker"));
    assert!(message.contains("Still open"));
}

#[tokio::test]
async fn render_project_prompt_returns_closed_bead_error() {
    let br = MockBeadPromptBr {
        detail: Ok(bead_detail(
            "bead-closed",
            "Closed",
            BeadStatus::Closed,
            Some("Already done."),
            vec![],
            vec![],
            vec![],
        )),
        dep_tree: Ok(dep_tree("bead-closed", "Closed", vec![])),
    };

    let error = render_project_prompt_from_bead("bead-closed", &br, PathBuf::from("/nope"))
        .await
        .expect_err("closed beads should refuse rendering");

    assert!(error.to_string().contains("closed bead 'bead-closed'"));
}

#[tokio::test]
async fn render_project_prompt_returns_bead_not_found_error() {
    let br = MockBeadPromptBr {
        detail: Err(MockReadError::NotFound),
        dep_tree: Ok(dep_tree("missing", "Missing", vec![])),
    };

    let error = render_project_prompt_from_bead("missing", &br, PathBuf::from("/nope"))
        .await
        .expect_err("missing beads should return a not-found error");

    assert!(error.to_string().contains("bead 'missing' was not found"));
}

fn bead_detail(
    id: &str,
    title: &str,
    status: BeadStatus,
    description: Option<&str>,
    acceptance_criteria: Vec<String>,
    dependencies: Vec<DependencyRef>,
    dependents: Vec<DependencyRef>,
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
        dependencies,
        dependents,
        comments: vec![],
        owner: None,
        created_at: None,
        updated_at: None,
    }
}

fn dependency(
    id: &str,
    title: &str,
    kind: DependencyKind,
    status: Option<BeadStatus>,
) -> DependencyRef {
    DependencyRef {
        id: id.to_owned(),
        kind,
        title: Some(title.to_owned()),
        status,
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

fn dep_tree_child(parent_id: &str, id: &str, title: &str, status: &str) -> DepTreeNode {
    DepTreeNode {
        id: id.to_owned(),
        title: title.to_owned(),
        status: status.to_owned(),
        depth: 1,
        parent_id: Some(parent_id.to_owned()),
        priority: Some(1),
        truncated: false,
        children: vec![],
    }
}

fn assert_in_order(haystack: &str, needles: &[&str]) {
    let mut cursor = 0;
    for needle in needles {
        let remaining = &haystack[cursor..];
        let offset = remaining
            .find(needle)
            .unwrap_or_else(|| panic!("expected prompt to contain {needle:?} after byte {cursor}"));
        cursor += offset + needle.len();
    }
}
