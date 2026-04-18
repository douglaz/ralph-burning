use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;
use tempfile::tempdir;

use ralph_burning::adapters::br_models::{
    BeadDetail, BeadStatus, BeadSummary, DepTreeNode, DependencyKind, ReadyBead,
};
use ralph_burning::adapters::br_process::{
    BrAdapter, BrCommand, BrError, BrMutationAdapter, BrOutput, ProcessRunner,
};

const LEGACY_PENDING_MUTATIONS_MARKER: &str = ".beads/.br-unsynced-mutations";
const PENDING_MUTATIONS_DIR: &str = ".beads/.br-unsynced-mutations.d";

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecordedBrCall {
    args: Vec<String>,
    working_dir: Option<PathBuf>,
}

#[derive(Debug)]
struct ScriptedBrCall {
    expected_args: Vec<String>,
    result: Result<BrOutput, BrError>,
}

#[derive(Debug)]
struct MockBrRunnerState {
    scripted_calls: Mutex<VecDeque<ScriptedBrCall>>,
    actual_calls: Mutex<Vec<RecordedBrCall>>,
}

#[derive(Clone, Debug)]
struct MockBrRunner {
    state: Arc<MockBrRunnerState>,
}

impl MockBrRunner {
    fn new(scripted_calls: Vec<ScriptedBrCall>) -> Self {
        Self {
            state: Arc::new(MockBrRunnerState {
                scripted_calls: Mutex::new(scripted_calls.into()),
                actual_calls: Mutex::new(Vec::new()),
            }),
        }
    }

    fn ok(stdout: impl Into<String>) -> Result<BrOutput, BrError> {
        Ok(BrOutput {
            stdout: stdout.into(),
            stderr: String::new(),
            exit_code: 0,
        })
    }

    fn exit(
        exit_code: i32,
        stdout: impl Into<String>,
        stderr: impl Into<String>,
    ) -> Result<BrOutput, BrError> {
        Ok(BrOutput {
            stdout: stdout.into(),
            stderr: stderr.into(),
            exit_code,
        })
    }

    fn actual_calls(&self) -> Vec<RecordedBrCall> {
        self.state
            .actual_calls
            .lock()
            .expect("mock br actual_calls mutex must not be poisoned")
            .clone()
    }
}

impl ProcessRunner for MockBrRunner {
    async fn run(
        &self,
        args: Vec<String>,
        _timeout: Duration,
        working_dir: Option<&Path>,
    ) -> Result<BrOutput, BrError> {
        self.state
            .actual_calls
            .lock()
            .expect("mock br actual_calls mutex must not be poisoned")
            .push(RecordedBrCall {
                args: args.clone(),
                working_dir: working_dir.map(Path::to_path_buf),
            });

        let scripted = self
            .state
            .scripted_calls
            .lock()
            .expect("mock br scripted_calls mutex must not be poisoned")
            .pop_front()
            .expect("mock br runner must have a scripted response for every call");

        assert_eq!(
            args, scripted.expected_args,
            "mock br runner received unexpected args"
        );

        scripted.result
    }
}

#[derive(Debug, Deserialize)]
struct WorkspaceGraph {
    nodes: Vec<WorkspaceGraphNode>,
    edges: Vec<WorkspaceGraphEdge>,
}

#[derive(Debug, Deserialize)]
struct WorkspaceGraphNode {
    id: String,
}

#[derive(Debug, Deserialize)]
struct WorkspaceGraphEdge {
    from: String,
    to: String,
}

fn pending_record_path(base_dir: &Path, adapter_id: &str) -> PathBuf {
    base_dir
        .join(PENDING_MUTATIONS_DIR)
        .join(format!("{adapter_id}.json"))
}

fn legacy_marker_path(base_dir: &Path) -> PathBuf {
    base_dir.join(LEGACY_PENDING_MUTATIONS_MARKER)
}

fn write_issues_file(base_dir: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let beads_dir = base_dir.join(".beads");
    std::fs::create_dir_all(&beads_dir)?;
    std::fs::write(beads_dir.join("issues.jsonl"), "{\"id\":\"bead-1\"}\n")?;
    Ok(())
}

fn read_adapter_with(
    scripted_calls: Vec<ScriptedBrCall>,
) -> (BrAdapter<MockBrRunner>, MockBrRunner) {
    let runner = MockBrRunner::new(scripted_calls);
    (BrAdapter::with_runner(runner.clone()), runner)
}

fn mutation_adapter_in(
    base_dir: &Path,
    adapter_id: &str,
    scripted_calls: Vec<ScriptedBrCall>,
) -> (BrMutationAdapter<MockBrRunner>, MockBrRunner) {
    let runner = MockBrRunner::new(scripted_calls);
    let adapter = BrAdapter::with_runner(runner.clone()).with_working_dir(base_dir.to_path_buf());
    (
        BrMutationAdapter::with_adapter_id(adapter, adapter_id.to_owned()),
        runner,
    )
}

#[tokio::test]
async fn br_show_parses_well_formed_json() -> Result<(), Box<dyn std::error::Error>> {
    let bead_json = r#"{
        "id":"bead-123",
        "title":"Tighten adapter tests",
        "status":"open",
        "priority":"P1",
        "issue_type":"task",
        "labels":["testing","adapters"],
        "description":"Add coverage for br adapters",
        "acceptance_criteria":"One\nTwo",
        "dependencies":[{"id":"bead-100","kind":"blocks","title":"Prep"}],
        "comments":[{"id":1,"issue_id":"bead-123","author":"alice","text":"ship it"}]
    }"#;
    let (adapter, runner) = read_adapter_with(vec![ScriptedBrCall {
        expected_args: vec![
            "show".to_owned(),
            "bead-123".to_owned(),
            "--json".to_owned(),
        ],
        result: MockBrRunner::ok(bead_json),
    }]);

    let detail: BeadDetail = adapter.exec_json(&BrCommand::show("bead-123")).await?;

    assert_eq!(detail.id, "bead-123", "show should parse the bead id");
    assert_eq!(
        detail.title, "Tighten adapter tests",
        "show should parse the bead title"
    );
    assert_eq!(
        detail.acceptance_criteria,
        vec!["One".to_owned(), "Two".to_owned()],
        "show should split acceptance criteria text into stable list entries"
    );
    assert_eq!(
        detail.dependencies.len(),
        1,
        "show should parse nested dependency references"
    );
    assert_eq!(
        runner.actual_calls(),
        vec![RecordedBrCall {
            args: vec![
                "show".to_owned(),
                "bead-123".to_owned(),
                "--json".to_owned()
            ],
            working_dir: None,
        }],
        "show should invoke the expected br command line"
    );
    Ok(())
}

#[tokio::test]
async fn br_ready_returns_empty_list_when_no_ready_beads() -> Result<(), Box<dyn std::error::Error>>
{
    let (adapter, runner) = read_adapter_with(vec![ScriptedBrCall {
        expected_args: vec!["ready".to_owned(), "--json".to_owned()],
        result: MockBrRunner::ok("[]"),
    }]);

    let ready: Vec<ReadyBead> = adapter.exec_json(&BrCommand::ready()).await?;

    assert!(
        ready.is_empty(),
        "ready should deserialize an empty array into an empty bead list"
    );
    assert_eq!(
        runner.actual_calls().len(),
        1,
        "ready should issue exactly one br command"
    );
    Ok(())
}

#[tokio::test]
async fn br_list_by_status_parses_array_output() -> Result<(), Box<dyn std::error::Error>> {
    let list_json = r#"[
        {"id":"bead-1","title":"Open bead","status":"open","priority":1,"issue_type":"task","labels":["backend"]},
        {"id":"bead-2","title":"Still open","status":"open","priority":"P2","issue_type":"bug","labels":[]}
    ]"#;
    let (adapter, _) = read_adapter_with(vec![ScriptedBrCall {
        expected_args: vec![
            "list".to_owned(),
            "--status=open".to_owned(),
            "--json".to_owned(),
        ],
        result: MockBrRunner::ok(list_json),
    }]);

    let beads: Vec<BeadSummary> = adapter
        .exec_json(&BrCommand::list_by_status("open"))
        .await?;

    assert_eq!(
        beads.len(),
        2,
        "list_by_status should parse every returned bead"
    );
    assert_eq!(
        beads[0].id, "bead-1",
        "list_by_status should preserve bead ordering"
    );
    assert_eq!(
        beads[1].priority.value(),
        2,
        "list_by_status should parse string priorities into numeric values"
    );
    Ok(())
}

#[tokio::test]
async fn br_dep_tree_parses_nested_dependency_output() -> Result<(), Box<dyn std::error::Error>> {
    let tree_json = r#"{
        "id":"bead-root",
        "title":"Root bead",
        "status":"open",
        "children":[
            {
                "id":"bead-child",
                "title":"Child bead",
                "status":"in_progress",
                "children":[
                    {
                        "id":"bead-leaf",
                        "title":"Leaf bead",
                        "status":"closed",
                        "children":[]
                    }
                ]
            }
        ]
    }"#;
    let (adapter, _) = read_adapter_with(vec![ScriptedBrCall {
        expected_args: vec![
            "dep".to_owned(),
            "tree".to_owned(),
            "bead-root".to_owned(),
            "--json".to_owned(),
        ],
        result: MockBrRunner::ok(tree_json),
    }]);

    let tree: DepTreeNode = adapter.exec_json(&BrCommand::dep_tree("bead-root")).await?;

    assert_eq!(
        tree.id, "bead-root",
        "dep tree should parse the requested root bead"
    );
    assert_eq!(
        tree.status,
        BeadStatus::Open,
        "dep tree should deserialize the public bead status contract"
    );
    assert_eq!(
        tree.children[0].children[0].id, "bead-leaf",
        "dep tree should preserve nested dependency structure"
    );
    Ok(())
}

#[tokio::test]
async fn br_graph_parses_workspace_graph_output() -> Result<(), Box<dyn std::error::Error>> {
    let graph_json = r#"{
        "nodes":[{"id":"bead-1"},{"id":"bead-2"}],
        "edges":[{"from":"bead-1","to":"bead-2"}]
    }"#;
    let (adapter, _) = read_adapter_with(vec![ScriptedBrCall {
        expected_args: vec!["graph".to_owned(), "--json".to_owned()],
        result: MockBrRunner::ok(graph_json),
    }]);

    let graph: WorkspaceGraph = adapter.exec_json(&BrCommand::graph()).await?;

    assert_eq!(graph.nodes.len(), 2, "graph should parse every node");
    assert_eq!(
        graph.nodes[0].id, "bead-1",
        "graph should preserve node ids"
    );
    assert_eq!(
        graph.edges[0].from, "bead-1",
        "graph should parse edge sources"
    );
    assert_eq!(
        graph.edges[0].to, "bead-2",
        "graph should parse edge targets"
    );
    Ok(())
}

#[tokio::test]
async fn br_create_mutation_issues_correct_command_line() -> Result<(), Box<dyn std::error::Error>>
{
    let tmp = tempdir()?;
    let adapter_id = "create-owner";
    let (adapter, runner) = mutation_adapter_in(
        tmp.path(),
        adapter_id,
        vec![ScriptedBrCall {
            expected_args: vec![
                "create".to_owned(),
                "--title=New adapter coverage".to_owned(),
                "--type=task".to_owned(),
                "--priority=1".to_owned(),
                "--labels=testing,adapters".to_owned(),
                "--description=Add happy and error-path coverage".to_owned(),
            ],
            result: MockBrRunner::ok("created"),
        }],
    );

    adapter
        .create_bead(
            "New adapter coverage",
            "task",
            "1",
            &["testing".to_owned(), "adapters".to_owned()],
            Some("Add happy and error-path coverage"),
        )
        .await?;

    assert!(
        pending_record_path(tmp.path(), adapter_id).is_file(),
        "successful create should persist a pending mutation journal record"
    );
    assert_eq!(
        runner.actual_calls()[0].working_dir,
        Some(tmp.path().to_path_buf()),
        "mutation commands should run inside the configured workspace"
    );
    Ok(())
}

#[tokio::test]
async fn br_update_status_mutation_issues_correct_command_line(
) -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let adapter_id = "update-owner";
    let (adapter, _) = mutation_adapter_in(
        tmp.path(),
        adapter_id,
        vec![ScriptedBrCall {
            expected_args: vec![
                "update".to_owned(),
                "bead-42".to_owned(),
                "--status=in_progress".to_owned(),
            ],
            result: MockBrRunner::ok("updated"),
        }],
    );

    adapter.update_bead_status("bead-42", "in_progress").await?;

    assert!(
        pending_record_path(tmp.path(), adapter_id).is_file(),
        "successful status updates should persist a pending mutation journal record"
    );
    Ok(())
}

#[tokio::test]
async fn br_close_mutation_issues_correct_command_line() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let adapter_id = "close-owner";
    let (adapter, _) = mutation_adapter_in(
        tmp.path(),
        adapter_id,
        vec![ScriptedBrCall {
            expected_args: vec![
                "close".to_owned(),
                "bead-77".to_owned(),
                "--reason=Completed".to_owned(),
            ],
            result: MockBrRunner::ok("closed"),
        }],
    );

    adapter.close_bead("bead-77", "Completed").await?;

    assert!(
        pending_record_path(tmp.path(), adapter_id).is_file(),
        "successful close operations should persist a pending mutation journal record"
    );
    Ok(())
}

#[tokio::test]
async fn br_add_dependency_mutation_issues_correct_command_line(
) -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let adapter_id = "dep-add-owner";
    let (adapter, runner) = mutation_adapter_in(
        tmp.path(),
        adapter_id,
        vec![ScriptedBrCall {
            expected_args: vec![
                "dep".to_owned(),
                "add".to_owned(),
                "bead-42".to_owned(),
                "bead-7".to_owned(),
            ],
            result: MockBrRunner::ok("dependency added"),
        }],
    );

    adapter.add_dependency("bead-42", "bead-7").await?;

    assert_eq!(
        runner.actual_calls()[0].args,
        vec![
            "dep".to_owned(),
            "add".to_owned(),
            "bead-42".to_owned(),
            "bead-7".to_owned(),
        ],
        "default dependency mutations should issue `br dep add <from> <to>` without an explicit kind flag"
    );
    assert!(
        pending_record_path(tmp.path(), adapter_id).is_file(),
        "successful dependency adds should persist a pending mutation journal record"
    );
    assert!(
        adapter.has_pending_mutations(),
        "successful dependency adds should leave the adapter dirty until sync_flush runs"
    );
    Ok(())
}

#[tokio::test]
async fn br_add_parent_child_dependency_mutation_issues_correct_command_line(
) -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let adapter_id = "dep-parent-owner";
    let (adapter, runner) = mutation_adapter_in(
        tmp.path(),
        adapter_id,
        vec![ScriptedBrCall {
            expected_args: vec![
                "dep".to_owned(),
                "add".to_owned(),
                "bead-parent".to_owned(),
                "bead-child".to_owned(),
                "--type=parent-child".to_owned(),
            ],
            result: MockBrRunner::ok("parent-child dependency added"),
        }],
    );

    adapter
        .add_dependency_with_kind("bead-parent", "bead-child", DependencyKind::ParentChild)
        .await?;

    assert_eq!(
        runner.actual_calls()[0].args,
        vec![
            "dep".to_owned(),
            "add".to_owned(),
            "bead-parent".to_owned(),
            "bead-child".to_owned(),
            "--type=parent-child".to_owned(),
        ],
        "parent-child dependency mutations should render the explicit dependency kind flag"
    );
    assert!(
        pending_record_path(tmp.path(), adapter_id).is_file(),
        "successful parent-child dependency adds should persist a pending mutation journal record"
    );
    assert!(
        adapter.has_pending_mutations(),
        "successful parent-child dependency adds should leave the adapter dirty until sync_flush runs"
    );
    Ok(())
}

#[tokio::test]
async fn br_remove_dependency_mutation_issues_correct_command_line(
) -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let adapter_id = "dep-remove-owner";
    let (adapter, runner) = mutation_adapter_in(
        tmp.path(),
        adapter_id,
        vec![ScriptedBrCall {
            expected_args: vec![
                "dep".to_owned(),
                "remove".to_owned(),
                "bead-42".to_owned(),
                "bead-7".to_owned(),
            ],
            result: MockBrRunner::ok("dependency removed"),
        }],
    );

    adapter.remove_dependency("bead-42", "bead-7").await?;

    assert_eq!(
        runner.actual_calls()[0].args,
        vec![
            "dep".to_owned(),
            "remove".to_owned(),
            "bead-42".to_owned(),
            "bead-7".to_owned(),
        ],
        "dependency removals should issue `br dep remove <from> <to>`"
    );
    assert!(
        pending_record_path(tmp.path(), adapter_id).is_file(),
        "successful dependency removals should persist a pending mutation journal record"
    );
    assert!(
        adapter.has_pending_mutations(),
        "successful dependency removals should leave the adapter dirty until sync_flush runs"
    );
    Ok(())
}

#[tokio::test]
async fn br_sync_flush_clears_dirty_marker_on_success() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let adapter_id = "flush-owner";
    let (adapter, runner) = mutation_adapter_in(
        tmp.path(),
        adapter_id,
        vec![
            ScriptedBrCall {
                expected_args: vec![
                    "update".to_owned(),
                    "bead-1".to_owned(),
                    "--status=in_progress".to_owned(),
                ],
                result: MockBrRunner::ok("updated"),
            },
            ScriptedBrCall {
                expected_args: vec!["sync".to_owned(), "--flush-only".to_owned()],
                result: MockBrRunner::ok("flushed"),
            },
        ],
    );

    adapter.update_bead_status("bead-1", "in_progress").await?;
    std::fs::create_dir_all(
        legacy_marker_path(tmp.path())
            .parent()
            .expect("legacy marker path must have a parent directory"),
    )?;
    std::fs::write(legacy_marker_path(tmp.path()), "pending\n")?;

    adapter.sync_flush().await?;

    assert!(
        !pending_record_path(tmp.path(), adapter_id).exists(),
        "successful sync_flush must remove the pending mutation journal record"
    );
    assert!(
        !legacy_marker_path(tmp.path()).exists(),
        "successful sync_flush must clear the legacy dirty marker when present"
    );
    assert!(
        !adapter.has_pending_mutations(),
        "successful sync_flush must leave the adapter clean"
    );
    assert_eq!(
        runner.actual_calls().len(),
        2,
        "sync_flush success path should execute the mutation and the flush only once each"
    );
    Ok(())
}

#[tokio::test]
async fn br_sync_import_runs_after_flush_when_pending_mutations_present(
) -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    write_issues_file(tmp.path())?;
    let adapter_id = "import-owner";
    let (adapter, runner) = mutation_adapter_in(
        tmp.path(),
        adapter_id,
        vec![
            ScriptedBrCall {
                expected_args: vec![
                    "update".to_owned(),
                    "bead-1".to_owned(),
                    "--status=in_progress".to_owned(),
                ],
                result: MockBrRunner::ok("updated"),
            },
            ScriptedBrCall {
                expected_args: vec!["sync".to_owned(), "--flush-only".to_owned()],
                result: MockBrRunner::ok("flushed"),
            },
            ScriptedBrCall {
                expected_args: vec!["sync".to_owned(), "--import-only".to_owned()],
                result: MockBrRunner::ok("imported"),
            },
        ],
    );

    adapter.update_bead_status("bead-1", "in_progress").await?;
    adapter.sync_flush().await?;

    assert!(
        !pending_record_path(tmp.path(), adapter_id).exists(),
        "sync_import precondition test requires sync_flush to clear pending mutation state first"
    );

    let import_output = adapter.sync_import().await?;

    assert_eq!(
        import_output.stdout, "imported",
        "sync_import should return the import command output after a successful flush"
    );
    assert_eq!(
        runner
            .actual_calls()
            .into_iter()
            .map(|call| call.args)
            .collect::<Vec<_>>(),
        vec![
            vec![
                "update".to_owned(),
                "bead-1".to_owned(),
                "--status=in_progress".to_owned(),
            ],
            vec!["sync".to_owned(), "--flush-only".to_owned()],
            vec!["sync".to_owned(), "--import-only".to_owned()],
        ],
        "sync_import should run only after the pending mutation has been flushed"
    );
    Ok(())
}

#[tokio::test]
async fn br_missing_binary_produces_br_not_found_error() -> Result<(), Box<dyn std::error::Error>> {
    let (adapter, _) = read_adapter_with(vec![ScriptedBrCall {
        expected_args: vec!["ready".to_owned(), "--json".to_owned()],
        result: Err(BrError::BrNotFound {
            details: "could not find 'br' in PATH".to_owned(),
        }),
    }]);

    let error = adapter
        .exec_read(&BrCommand::ready())
        .await
        .expect_err("missing br binary should surface as BrNotFound");

    match error {
        BrError::BrNotFound { details } => assert!(
            details.contains("could not find 'br'"),
            "BrNotFound should preserve the missing-binary details"
        ),
        other => panic!("expected BrNotFound, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn br_timeout_produces_br_timeout_error_with_command_context(
) -> Result<(), Box<dyn std::error::Error>> {
    let (adapter, _) = read_adapter_with(vec![ScriptedBrCall {
        expected_args: vec!["ready".to_owned(), "--json".to_owned()],
        result: Err(BrError::BrTimeout {
            command: "br ready --json".to_owned(),
            timeout_ms: 30_000,
        }),
    }]);

    let error = adapter
        .exec_read(&BrCommand::ready())
        .await
        .expect_err("timeouts should propagate the original BrTimeout");

    match error {
        BrError::BrTimeout {
            command,
            timeout_ms,
        } => {
            assert_eq!(
                command, "br ready --json",
                "timeout errors should retain command context"
            );
            assert_eq!(
                timeout_ms, 30_000,
                "timeout errors should retain the configured timeout"
            );
        }
        other => panic!("expected BrTimeout, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn br_non_zero_exit_produces_br_exit_error_preserving_stdout_stderr(
) -> Result<(), Box<dyn std::error::Error>> {
    let (adapter, _) = read_adapter_with(vec![ScriptedBrCall {
        expected_args: vec!["ready".to_owned(), "--json".to_owned()],
        result: MockBrRunner::exit(23, "partial output", "fatal problem"),
    }]);

    let error = adapter
        .exec_read(&BrCommand::ready())
        .await
        .expect_err("non-zero br exits should produce BrExitError");

    match error {
        BrError::BrExitError {
            exit_code,
            stdout,
            stderr,
            command,
        } => {
            assert_eq!(
                exit_code, 23,
                "BrExitError should preserve the subprocess exit code"
            );
            assert_eq!(
                stdout, "partial output",
                "BrExitError should preserve stdout for debugging"
            );
            assert_eq!(
                stderr, "fatal problem",
                "BrExitError should preserve stderr for debugging"
            );
            assert_eq!(
                command, "br ready --json",
                "BrExitError should preserve the rendered command"
            );
        }
        other => panic!("expected BrExitError, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn br_malformed_json_produces_br_parse_error_with_raw_output(
) -> Result<(), Box<dyn std::error::Error>> {
    let (adapter, _) = read_adapter_with(vec![ScriptedBrCall {
        expected_args: vec!["ready".to_owned(), "--json".to_owned()],
        result: MockBrRunner::ok("not valid json"),
    }]);

    let error = adapter
        .exec_json::<Vec<ReadyBead>>(&BrCommand::ready())
        .await
        .expect_err("malformed JSON should surface as BrParseError");

    match error {
        BrError::BrParseError {
            raw_output,
            command,
            ..
        } => {
            assert_eq!(
                raw_output, "not valid json",
                "BrParseError should retain the raw stdout"
            );
            assert_eq!(
                command, "br ready --json",
                "BrParseError should identify the failed command"
            );
        }
        other => panic!("expected BrParseError, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn br_partial_json_output_surfaces_parse_error_not_panic(
) -> Result<(), Box<dyn std::error::Error>> {
    let (adapter, _) = read_adapter_with(vec![ScriptedBrCall {
        expected_args: vec![
            "show".to_owned(),
            "bead-123".to_owned(),
            "--json".to_owned(),
        ],
        result: MockBrRunner::ok("{\"id\":\"bead-123\""),
    }]);

    let error = adapter
        .exec_json::<BeadDetail>(&BrCommand::show("bead-123"))
        .await
        .expect_err("truncated JSON should return BrParseError rather than panic");

    assert!(
        matches!(error, BrError::BrParseError { .. }),
        "partial JSON output should surface a parse error"
    );
    Ok(())
}

#[tokio::test]
async fn br_sync_flush_failure_keeps_dirty_marker_so_retry_is_safe(
) -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempdir()?;
    let adapter_id = "retry-owner";
    let (adapter, _) = mutation_adapter_in(
        tmp.path(),
        adapter_id,
        vec![
            ScriptedBrCall {
                expected_args: vec![
                    "update".to_owned(),
                    "bead-1".to_owned(),
                    "--status=in_progress".to_owned(),
                ],
                result: MockBrRunner::ok("updated"),
            },
            ScriptedBrCall {
                expected_args: vec!["sync".to_owned(), "--flush-only".to_owned()],
                result: MockBrRunner::exit(9, "partial flush", "sync failed"),
            },
        ],
    );

    adapter.update_bead_status("bead-1", "in_progress").await?;
    std::fs::create_dir_all(
        legacy_marker_path(tmp.path())
            .parent()
            .expect("legacy marker path must have a parent directory"),
    )?;
    std::fs::write(legacy_marker_path(tmp.path()), "pending\n")?;

    let error = adapter
        .sync_flush()
        .await
        .expect_err("failing sync_flush should return the underlying BrExitError");

    assert!(
        matches!(error, BrError::BrExitError { exit_code: 9, .. }),
        "sync_flush failure should preserve the subprocess failure details"
    );
    assert!(
        pending_record_path(tmp.path(), adapter_id).is_file(),
        "sync_flush failure must leave the journal record in place for safe retry"
    );
    assert!(
        legacy_marker_path(tmp.path()).is_file(),
        "sync_flush failure must leave the dirty marker in place for safe retry"
    );
    assert!(
        adapter.has_pending_mutations(),
        "sync_flush failure must leave the adapter marked dirty for safe retry"
    );
    Ok(())
}
