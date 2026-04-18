use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;

use ralph_burning::adapters::bv_process::{
    BvAdapter, BvCommand, BvError, BvOutput, BvProcessRunner, NextBeadResponse, TriageResponse,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecordedBvCall {
    args: Vec<String>,
    working_dir: Option<PathBuf>,
}

#[derive(Debug)]
struct ScriptedBvCall {
    expected_args: Vec<String>,
    result: Result<BvOutput, BvError>,
}

#[derive(Debug)]
struct MockBvRunnerState {
    scripted_calls: Mutex<VecDeque<ScriptedBvCall>>,
    actual_calls: Mutex<Vec<RecordedBvCall>>,
}

#[derive(Clone, Debug)]
struct MockBvRunner {
    state: Arc<MockBvRunnerState>,
}

impl MockBvRunner {
    fn new(scripted_calls: Vec<ScriptedBvCall>) -> Self {
        Self {
            state: Arc::new(MockBvRunnerState {
                scripted_calls: Mutex::new(scripted_calls.into()),
                actual_calls: Mutex::new(Vec::new()),
            }),
        }
    }

    fn ok(stdout: impl Into<String>) -> Result<BvOutput, BvError> {
        Ok(BvOutput {
            stdout: stdout.into(),
            stderr: String::new(),
            exit_code: 0,
        })
    }

    fn exit(
        exit_code: i32,
        stdout: impl Into<String>,
        stderr: impl Into<String>,
    ) -> Result<BvOutput, BvError> {
        Ok(BvOutput {
            stdout: stdout.into(),
            stderr: stderr.into(),
            exit_code,
        })
    }

    fn actual_calls(&self) -> Vec<RecordedBvCall> {
        self.state
            .actual_calls
            .lock()
            .expect("mock bv actual_calls mutex must not be poisoned")
            .clone()
    }
}

impl BvProcessRunner for MockBvRunner {
    async fn run(
        &self,
        args: Vec<String>,
        _timeout: Duration,
        working_dir: Option<&Path>,
    ) -> Result<BvOutput, BvError> {
        self.state
            .actual_calls
            .lock()
            .expect("mock bv actual_calls mutex must not be poisoned")
            .push(RecordedBvCall {
                args: args.clone(),
                working_dir: working_dir.map(Path::to_path_buf),
            });

        let scripted = self
            .state
            .scripted_calls
            .lock()
            .expect("mock bv scripted_calls mutex must not be poisoned")
            .pop_front()
            .expect("mock bv runner must have a scripted response for every call");

        assert_eq!(
            args, scripted.expected_args,
            "mock bv runner received unexpected args"
        );

        scripted.result
    }
}

#[derive(Debug, Deserialize)]
struct EmptyNextSignal {
    next: Option<NextBeadResponse>,
    reason: String,
}

#[derive(Debug, Deserialize)]
struct BlockedGraphSignal {
    ready: Vec<NextBeadResponse>,
    blocked: Vec<BlockedBead>,
}

#[derive(Debug, Deserialize)]
struct BlockedBead {
    id: String,
    blocked_by: Vec<String>,
}

fn adapter_with(scripted_calls: Vec<ScriptedBvCall>) -> (BvAdapter<MockBvRunner>, MockBvRunner) {
    let runner = MockBvRunner::new(scripted_calls);
    (BvAdapter::with_runner(runner.clone()), runner)
}

#[tokio::test]
async fn bv_robot_next_parses_recommended_bead() -> Result<(), Box<dyn std::error::Error>> {
    let next_json = r#"{
        "id":"bead-12",
        "title":"Ship adapter coverage",
        "score":9.7,
        "reasons":["highest impact","ready now"],
        "action":"claim"
    }"#;
    let (adapter, runner) = adapter_with(vec![ScriptedBvCall {
        expected_args: vec!["--robot-next".to_owned()],
        result: MockBvRunner::ok(next_json),
    }]);

    let next: NextBeadResponse = adapter.exec_json(&BvCommand::robot_next()).await?;

    assert_eq!(
        next.id, "bead-12",
        "robot-next should parse the recommended bead id"
    );
    assert_eq!(
        next.action, "claim",
        "robot-next should parse the recommended follow-up action"
    );
    assert_eq!(
        runner.actual_calls(),
        vec![RecordedBvCall {
            args: vec!["--robot-next".to_owned()],
            working_dir: None,
        }],
        "robot-next should issue the expected bv command line"
    );
    Ok(())
}

#[tokio::test]
async fn bv_robot_triage_parses_triage_output() -> Result<(), Box<dyn std::error::Error>> {
    let triage_json = r#"{
        "quick_ref":{
            "top_picks":[{"id":"bead-1","title":"Fix CI","score":8.5,"reasons":["blocked merge"],"unblocks":2}],
            "open_count":14,
            "actionable_count":3
        },
        "recommendations":[
            {"id":"bead-1","title":"Fix CI","score":8.5,"reasons":["blocked merge"],"action":"claim","unblocks_ids":["bead-2"],"blocked_by":[]}
        ],
        "quick_wins":["bead-7"],
        "blockers_to_clear":["bead-9"]
    }"#;
    let (adapter, _) = adapter_with(vec![ScriptedBvCall {
        expected_args: vec!["--robot-triage".to_owned()],
        result: MockBvRunner::ok(triage_json),
    }]);

    let triage: TriageResponse = adapter.exec_json(&BvCommand::robot_triage()).await?;

    assert_eq!(
        triage.quick_ref.open_count, 14,
        "robot-triage should parse aggregate open counts"
    );
    assert_eq!(
        triage.recommendations[0].id, "bead-1",
        "robot-triage should parse recommendation ids"
    );
    assert_eq!(
        triage.blockers_to_clear,
        vec!["bead-9".to_owned()],
        "robot-triage should parse blocker recommendations"
    );
    Ok(())
}

#[tokio::test]
async fn bv_missing_binary_produces_bv_not_found_error() -> Result<(), Box<dyn std::error::Error>> {
    let (adapter, _) = adapter_with(vec![ScriptedBvCall {
        expected_args: vec!["--robot-triage".to_owned()],
        result: Err(BvError::BvNotFound {
            details: "could not find 'bv' in PATH".to_owned(),
        }),
    }]);

    let error = adapter
        .exec_read(&BvCommand::robot_triage())
        .await
        .expect_err("missing bv binary should surface as BvNotFound");

    match error {
        BvError::BvNotFound { details } => assert!(
            details.contains("could not find 'bv'"),
            "BvNotFound should preserve the missing-binary details"
        ),
        other => panic!("expected BvNotFound, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn bv_empty_graph_reports_no_next_bead_gracefully() -> Result<(), Box<dyn std::error::Error>>
{
    let (adapter, _) = adapter_with(vec![ScriptedBvCall {
        expected_args: vec!["--robot-next".to_owned()],
        result: MockBvRunner::ok(r#"{"next":null,"reason":"nothing ready"}"#),
    }]);

    let response: EmptyNextSignal = adapter.exec_json(&BvCommand::robot_next()).await?;

    assert!(
        response.next.is_none(),
        "an empty graph should deserialize to an explicit no-next-bead signal"
    );
    assert_eq!(
        response.reason, "nothing ready",
        "an empty graph should explain why no bead was recommended"
    );
    Ok(())
}

#[tokio::test]
async fn bv_blocked_graph_reports_all_beads_blocked() -> Result<(), Box<dyn std::error::Error>> {
    let (adapter, _) = adapter_with(vec![ScriptedBvCall {
        expected_args: vec!["--robot-next".to_owned()],
        result: MockBvRunner::ok(
            r#"{"ready":[],"blocked":[{"id":"bead-3","blocked_by":["bead-1","bead-2"]}]}"#,
        ),
    }]);

    let response: BlockedGraphSignal = adapter.exec_json(&BvCommand::robot_next()).await?;

    assert!(
        response.ready.is_empty(),
        "a fully blocked graph should report no ready beads"
    );
    assert_eq!(
        response.blocked[0].id, "bead-3",
        "a blocked-graph response should identify the blocked bead"
    );
    assert_eq!(
        response.blocked[0].blocked_by,
        vec!["bead-1".to_owned(), "bead-2".to_owned()],
        "a blocked-graph response should list every blocking dependency"
    );
    Ok(())
}

#[tokio::test]
async fn bv_non_zero_exit_surfaces_stderr_in_error() -> Result<(), Box<dyn std::error::Error>> {
    let (adapter, _) = adapter_with(vec![ScriptedBvCall {
        expected_args: vec!["--robot-triage".to_owned()],
        result: MockBvRunner::exit(2, "partial triage", "graph load failed"),
    }]);

    let error = adapter
        .exec_read(&BvCommand::robot_triage())
        .await
        .expect_err("non-zero bv exits should surface as BvExitError");

    match error {
        BvError::BvExitError {
            exit_code,
            stdout,
            stderr,
            command,
        } => {
            assert_eq!(
                exit_code, 2,
                "BvExitError should preserve the subprocess exit code"
            );
            assert_eq!(
                stdout, "partial triage",
                "BvExitError should preserve stdout"
            );
            assert_eq!(
                stderr, "graph load failed",
                "BvExitError should preserve stderr"
            );
            assert_eq!(
                command, "bv --robot-triage",
                "BvExitError should preserve the rendered command"
            );
        }
        other => panic!("expected BvExitError, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn bv_malformed_output_produces_bv_parse_error() -> Result<(), Box<dyn std::error::Error>> {
    let (adapter, _) = adapter_with(vec![ScriptedBvCall {
        expected_args: vec!["--robot-triage".to_owned()],
        result: MockBvRunner::ok("not valid json"),
    }]);

    let error = adapter
        .exec_json::<TriageResponse>(&BvCommand::robot_triage())
        .await
        .expect_err("malformed bv output should surface as BvParseError");

    match error {
        BvError::BvParseError {
            raw_output,
            command,
            ..
        } => {
            assert_eq!(
                raw_output, "not valid json",
                "BvParseError should retain the raw stdout"
            );
            assert_eq!(
                command, "bv --robot-triage",
                "BvParseError should preserve the failed command"
            );
        }
        other => panic!("expected BvParseError, got {other:?}"),
    }
    Ok(())
}

#[tokio::test]
async fn bv_timeout_produces_bv_timeout_error() -> Result<(), Box<dyn std::error::Error>> {
    let (adapter, _) = adapter_with(vec![ScriptedBvCall {
        expected_args: vec!["--robot-triage".to_owned()],
        result: Err(BvError::BvTimeout {
            command: "bv --robot-triage".to_owned(),
            timeout_ms: 30_000,
        }),
    }]);

    let error = adapter
        .exec_read(&BvCommand::robot_triage())
        .await
        .expect_err("timeouts should propagate the original BvTimeout");

    match error {
        BvError::BvTimeout {
            command,
            timeout_ms,
        } => {
            assert_eq!(
                command, "bv --robot-triage",
                "BvTimeout should preserve command context"
            );
            assert_eq!(
                timeout_ms, 30_000,
                "BvTimeout should preserve timeout duration"
            );
        }
        other => panic!("expected BvTimeout, got {other:?}"),
    }
    Ok(())
}
