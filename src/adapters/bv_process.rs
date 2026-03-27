#![forbid(unsafe_code)]

//! Process execution wrapper for the `bv` (beads_viewer) CLI.
//!
//! Provides type-safe command construction, structured error handling,
//! and configurable timeouts. Uses direct process execution — never
//! shells out through sh/bash.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::process::Command;

// ── Defaults ────────────────────────────────────────────────────────────────

/// Default timeout for read-only bv commands (30 seconds).
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(30);

// ── Errors ──────────────────────────────────────────────────────────────────

/// Structured error for bv subprocess execution.
#[derive(Debug, Error)]
pub enum BvError {
    /// `bv` binary was not found in PATH.
    #[error("bv binary not found: {details}")]
    BvNotFound { details: String },

    /// Command exceeded the configured timeout.
    #[error("bv command timed out after {timeout_ms}ms: {command}")]
    BvTimeout { command: String, timeout_ms: u64 },

    /// Command exited with a non-zero status.
    #[error("bv command failed (exit {exit_code}): {stderr}")]
    BvExitError {
        exit_code: i32,
        stdout: String,
        stderr: String,
        command: String,
    },

    /// Command succeeded but output was not parseable JSON.
    #[error("bv output parse error: {details}")]
    BvParseError {
        details: String,
        raw_output: String,
        command: String,
    },

    /// Underlying I/O error during process spawn/wait.
    #[error("bv process I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl BvError {
    /// Returns the raw stdout from a failed command, if available.
    pub fn raw_output(&self) -> Option<&str> {
        match self {
            Self::BvExitError { stdout, .. } => Some(stdout),
            Self::BvParseError { raw_output, .. } => Some(raw_output),
            _ => None,
        }
    }
}

// ── Command output ──────────────────────────────────────────────────────────

/// Raw output from a bv subprocess.
#[derive(Debug, Clone)]
pub struct BvOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

// ── Command builder ─────────────────────────────────────────────────────────

/// Type-safe builder for `bv` command lines.
///
/// Unlike `BrCommand`, bv uses top-level flags (e.g. `--robot-triage`)
/// rather than subcommands. The builder accumulates flags and key-value
/// arguments for direct process execution.
#[derive(Debug, Clone)]
pub struct BvCommand {
    flags: Vec<String>,
    kv_flags: Vec<(String, String)>,
}

impl BvCommand {
    fn new() -> Self {
        Self {
            flags: Vec::new(),
            kv_flags: Vec::new(),
        }
    }

    /// Add a boolean flag (e.g., `--robot-triage`).
    pub fn flag(mut self, name: impl Into<String>) -> Self {
        self.flags.push(name.into());
        self
    }

    /// Add a key-value flag (e.g., `--label backend`).
    pub fn kv(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.kv_flags.push((key.into(), value.into()));
        self
    }

    /// Add a label filter (e.g., `--label backend`).
    pub fn with_label(self, label: impl Into<String>) -> Self {
        self.kv("label", label)
    }

    /// Add a recipe (e.g., `--recipe actionable`).
    pub fn with_recipe(self, recipe: impl Into<String>) -> Self {
        self.kv("recipe", recipe)
    }

    /// Build the argument list for direct process execution.
    fn build_args(&self) -> Vec<String> {
        let mut args = Vec::new();
        for flag in &self.flags {
            args.push(format!("--{flag}"));
        }
        for (key, value) in &self.kv_flags {
            args.push(format!("--{key}"));
            args.push(value.clone());
        }
        args
    }

    /// Format for display in error messages.
    fn display_string(&self) -> String {
        let args = self.build_args();
        format!("bv {}", args.join(" "))
    }
}

// ── Common command constructors ─────────────────────────────────────────────

impl BvCommand {
    /// `bv --robot-triage`
    pub fn robot_triage() -> Self {
        Self::new().flag("robot-triage")
    }

    /// `bv --robot-next`
    pub fn robot_next() -> Self {
        Self::new().flag("robot-next")
    }

    /// `bv --robot-plan`
    pub fn robot_plan() -> Self {
        Self::new().flag("robot-plan")
    }

    /// `bv --robot-priority`
    pub fn robot_priority() -> Self {
        Self::new().flag("robot-priority")
    }

    /// `bv --robot-insights`
    pub fn robot_insights() -> Self {
        Self::new().flag("robot-insights")
    }

    /// `bv --robot-alerts`
    pub fn robot_alerts() -> Self {
        Self::new().flag("robot-alerts")
    }

    /// `bv --robot-suggest`
    pub fn robot_suggest() -> Self {
        Self::new().flag("robot-suggest")
    }

    /// `bv --robot-graph --graph-format=<format>`
    pub fn robot_graph(format: impl Into<String>) -> Self {
        Self::new().flag("robot-graph").kv("graph-format", format)
    }
}

// ── Process runner ──────────────────────────────────────────────────────────

/// Configurable runner for bv subprocess execution.
///
/// Testable via the `BvProcessRunner` trait. Production code uses
/// `OsBvProcessRunner`; tests can substitute a mock.
pub trait BvProcessRunner: Send + Sync {
    /// Execute a bv command and return the raw output.
    fn run(
        &self,
        args: Vec<String>,
        timeout: Duration,
        working_dir: Option<&std::path::Path>,
    ) -> impl std::future::Future<Output = Result<BvOutput, BvError>> + Send;
}

/// Production process runner that spawns real subprocesses.
#[derive(Debug, Clone)]
pub struct OsBvProcessRunner {
    /// Path to the bv binary. If None, resolves from PATH.
    bv_binary: Option<PathBuf>,
}

impl OsBvProcessRunner {
    pub fn new() -> Self {
        Self { bv_binary: None }
    }

    pub fn with_binary(binary: PathBuf) -> Self {
        Self {
            bv_binary: Some(binary),
        }
    }

    fn bv_path(&self) -> &str {
        self.bv_binary
            .as_deref()
            .and_then(|p| p.to_str())
            .unwrap_or("bv")
    }
}

impl Default for OsBvProcessRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl BvProcessRunner for OsBvProcessRunner {
    async fn run(
        &self,
        args: Vec<String>,
        timeout: Duration,
        working_dir: Option<&std::path::Path>,
    ) -> Result<BvOutput, BvError> {
        let bv_path = self.bv_path();
        let mut cmd = Command::new(bv_path);
        cmd.args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if let Some(dir) = working_dir {
            cmd.current_dir(dir);
        }

        // kill_on_drop ensures the child is terminated if the future is
        // cancelled by tokio::time::timeout, preventing orphaned processes.
        cmd.kill_on_drop(true);

        let child = cmd.spawn().map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                BvError::BvNotFound {
                    details: format!("could not find '{bv_path}' in PATH"),
                }
            } else {
                BvError::Io(e)
            }
        })?;

        let command_display = format!("bv {}", args.join(" "));

        match tokio::time::timeout(timeout, bv_wait_for_output(child)).await {
            Ok(result) => result,
            Err(_) => Err(BvError::BvTimeout {
                command: command_display,
                timeout_ms: timeout.as_millis() as u64,
            }),
        }
    }
}

async fn bv_wait_for_output(child: tokio::process::Child) -> Result<BvOutput, BvError> {
    let output = child.wait_with_output().await?;
    Ok(BvOutput {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        exit_code: output.status.code().unwrap_or(-1),
    })
}

// ── Response types ──────────────────────────────────────────────────────────

/// Quick reference summary returned inside a triage response.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TriageQuickRef {
    #[serde(default)]
    pub top_picks: Vec<TriagePick>,
    #[serde(default)]
    pub open_count: u32,
    #[serde(default)]
    pub actionable_count: u32,
}

/// A single recommended bead from triage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriageRecommendation {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub score: f64,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub unblocks_ids: Vec<String>,
    #[serde(default)]
    pub blocked_by: Vec<String>,
}

/// A top pick in the quick-reference section.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriagePick {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub score: f64,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub unblocks: u32,
}

/// Full response from `bv --robot-triage`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriageResponse {
    #[serde(default)]
    pub quick_ref: TriageQuickRef,
    #[serde(default)]
    pub recommendations: Vec<TriageRecommendation>,
    #[serde(default)]
    pub quick_wins: Vec<String>,
    #[serde(default)]
    pub blockers_to_clear: Vec<String>,
}

/// Response from `bv --robot-next`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextBeadResponse {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub score: f64,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub action: String,
}

// ── BvAdapter ───────────────────────────────────────────────────────────────

/// High-level adapter for executing bv commands.
///
/// Wraps a `BvProcessRunner` with configurable timeouts and provides
/// convenience methods for common operations.
pub struct BvAdapter<R: BvProcessRunner = OsBvProcessRunner> {
    runner: R,
    read_timeout: Duration,
    working_dir: Option<PathBuf>,
}

impl BvAdapter<OsBvProcessRunner> {
    pub fn new() -> Self {
        Self {
            runner: OsBvProcessRunner::new(),
            read_timeout: DEFAULT_READ_TIMEOUT,
            working_dir: None,
        }
    }
}

impl Default for BvAdapter<OsBvProcessRunner> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: BvProcessRunner> BvAdapter<R> {
    pub fn with_runner(runner: R) -> Self {
        Self {
            runner,
            read_timeout: DEFAULT_READ_TIMEOUT,
            working_dir: None,
        }
    }

    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = timeout;
        self
    }

    pub fn with_working_dir(mut self, dir: PathBuf) -> Self {
        self.working_dir = Some(dir);
        self
    }

    /// Execute a read-only bv command and return raw output.
    pub async fn exec_read(&self, cmd: &BvCommand) -> Result<BvOutput, BvError> {
        let args = cmd.build_args();
        let output = self
            .runner
            .run(args, self.read_timeout, self.working_dir.as_deref())
            .await?;

        if output.exit_code != 0 {
            return Err(BvError::BvExitError {
                exit_code: output.exit_code,
                stdout: output.stdout,
                stderr: output.stderr,
                command: cmd.display_string(),
            });
        }

        Ok(output)
    }

    /// Execute a read-only command and parse the JSON output.
    pub async fn exec_json<T: DeserializeOwned>(&self, cmd: &BvCommand) -> Result<T, BvError> {
        let output = self.exec_read(cmd).await?;
        serde_json::from_str(&output.stdout).map_err(|e| BvError::BvParseError {
            details: e.to_string(),
            raw_output: output.stdout,
            command: cmd.display_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Command builder tests ─────────────────────────────────────────

    #[test]
    fn command_robot_triage() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BvCommand::robot_triage();
        let args = cmd.build_args();
        assert_eq!(args, vec!["--robot-triage"]);
        Ok(())
    }

    #[test]
    fn command_robot_next() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BvCommand::robot_next();
        let args = cmd.build_args();
        assert_eq!(args, vec!["--robot-next"]);
        Ok(())
    }

    #[test]
    fn command_robot_plan() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BvCommand::robot_plan();
        let args = cmd.build_args();
        assert_eq!(args, vec!["--robot-plan"]);
        Ok(())
    }

    #[test]
    fn command_robot_priority() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BvCommand::robot_priority();
        let args = cmd.build_args();
        assert_eq!(args, vec!["--robot-priority"]);
        Ok(())
    }

    #[test]
    fn command_robot_insights() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BvCommand::robot_insights();
        let args = cmd.build_args();
        assert_eq!(args, vec!["--robot-insights"]);
        Ok(())
    }

    #[test]
    fn command_robot_alerts() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BvCommand::robot_alerts();
        let args = cmd.build_args();
        assert_eq!(args, vec!["--robot-alerts"]);
        Ok(())
    }

    #[test]
    fn command_robot_suggest() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BvCommand::robot_suggest();
        let args = cmd.build_args();
        assert_eq!(args, vec!["--robot-suggest"]);
        Ok(())
    }

    #[test]
    fn command_robot_graph() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BvCommand::robot_graph("json");
        let args = cmd.build_args();
        assert_eq!(args, vec!["--robot-graph", "--graph-format", "json"]);
        Ok(())
    }

    #[test]
    fn command_with_label() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BvCommand::robot_triage().with_label("backend");
        let args = cmd.build_args();
        assert_eq!(args, vec!["--robot-triage", "--label", "backend"]);
        Ok(())
    }

    #[test]
    fn command_with_recipe() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BvCommand::robot_triage().with_recipe("actionable");
        let args = cmd.build_args();
        assert_eq!(args, vec!["--robot-triage", "--recipe", "actionable"]);
        Ok(())
    }

    #[test]
    fn command_with_label_and_recipe() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BvCommand::robot_next()
            .with_label("backend")
            .with_recipe("actionable");
        let args = cmd.build_args();
        assert_eq!(
            args,
            vec![
                "--robot-next",
                "--label",
                "backend",
                "--recipe",
                "actionable"
            ]
        );
        Ok(())
    }

    #[test]
    fn command_display_string() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BvCommand::robot_triage().with_label("backend");
        assert_eq!(cmd.display_string(), "bv --robot-triage --label backend");
        Ok(())
    }

    // ── Error tests ───────────────────────────────────────────────────

    #[test]
    fn bv_error_raw_output_exit_error() -> Result<(), Box<dyn std::error::Error>> {
        let err = BvError::BvExitError {
            exit_code: 1,
            stdout: "some output".to_owned(),
            stderr: "error message".to_owned(),
            command: "bv --robot-triage".to_owned(),
        };
        assert_eq!(err.raw_output(), Some("some output"));
        Ok(())
    }

    #[test]
    fn bv_error_raw_output_parse_error() -> Result<(), Box<dyn std::error::Error>> {
        let err = BvError::BvParseError {
            details: "invalid json".to_owned(),
            raw_output: "bad data".to_owned(),
            command: "bv --robot-triage".to_owned(),
        };
        assert_eq!(err.raw_output(), Some("bad data"));
        Ok(())
    }

    #[test]
    fn bv_error_raw_output_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let err = BvError::BvNotFound {
            details: "not found".to_owned(),
        };
        assert_eq!(err.raw_output(), None);
        Ok(())
    }

    // ── OS runner tests ───────────────────────────────────────────────

    #[test]
    fn os_bv_runner_default_path() -> Result<(), Box<dyn std::error::Error>> {
        let runner = OsBvProcessRunner::new();
        assert_eq!(runner.bv_path(), "bv");
        Ok(())
    }

    #[test]
    fn os_bv_runner_custom_path() -> Result<(), Box<dyn std::error::Error>> {
        let runner = OsBvProcessRunner::with_binary(PathBuf::from("/usr/local/bin/bv"));
        assert_eq!(runner.bv_path(), "/usr/local/bin/bv");
        Ok(())
    }

    // ── Adapter config tests ──────────────────────────────────────────

    #[test]
    fn adapter_default_timeout() -> Result<(), Box<dyn std::error::Error>> {
        let adapter = BvAdapter::new();
        assert_eq!(adapter.read_timeout, DEFAULT_READ_TIMEOUT);
        Ok(())
    }

    #[test]
    fn adapter_custom_timeout() -> Result<(), Box<dyn std::error::Error>> {
        let adapter = BvAdapter::new().with_read_timeout(Duration::from_secs(10));
        assert_eq!(adapter.read_timeout, Duration::from_secs(10));
        Ok(())
    }

    // ── Mock runner ───────────────────────────────────────────────────

    /// A mock process runner for unit testing without real bv binary.
    struct MockBvRunner {
        responses: std::sync::Mutex<Vec<Result<BvOutput, BvError>>>,
    }

    impl MockBvRunner {
        fn new(responses: Vec<Result<BvOutput, BvError>>) -> Self {
            Self {
                responses: std::sync::Mutex::new(responses),
            }
        }

        fn success(stdout: &str) -> Result<BvOutput, BvError> {
            Ok(BvOutput {
                stdout: stdout.to_owned(),
                stderr: String::new(),
                exit_code: 0,
            })
        }

        fn failure(exit_code: i32, stderr: &str) -> Result<BvOutput, BvError> {
            Ok(BvOutput {
                stdout: String::new(),
                stderr: stderr.to_owned(),
                exit_code,
            })
        }
    }

    impl BvProcessRunner for MockBvRunner {
        async fn run(
            &self,
            _args: Vec<String>,
            _timeout: Duration,
            _working_dir: Option<&std::path::Path>,
        ) -> Result<BvOutput, BvError> {
            let mut responses = self.responses.lock().expect("mock lock poisoned");
            if responses.is_empty() {
                panic!("MockBvRunner: no more responses configured");
            }
            responses.remove(0)
        }
    }

    // ── Adapter mock tests ────────────────────────────────────────────

    #[tokio::test]
    async fn adapter_exec_read_success() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockBvRunner::new(vec![MockBvRunner::success(r#"{"status":"ok"}"#)]);
        let adapter = BvAdapter::with_runner(runner);
        let output = adapter.exec_read(&BvCommand::robot_triage()).await?;
        assert_eq!(output.exit_code, 0);
        assert!(output.stdout.contains("ok"));
        Ok(())
    }

    #[tokio::test]
    async fn adapter_exec_read_failure() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockBvRunner::new(vec![MockBvRunner::failure(1, "command failed")]);
        let adapter = BvAdapter::with_runner(runner);
        let result = adapter.exec_read(&BvCommand::robot_next()).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, BvError::BvExitError { exit_code: 1, .. }));
        Ok(())
    }

    #[tokio::test]
    async fn adapter_bv_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockBvRunner::new(vec![Err(BvError::BvNotFound {
            details: "could not find 'bv' in PATH".to_owned(),
        })]);
        let adapter = BvAdapter::with_runner(runner);
        let result = adapter.exec_read(&BvCommand::robot_triage()).await;
        assert!(matches!(result, Err(BvError::BvNotFound { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn adapter_timeout_error() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockBvRunner::new(vec![Err(BvError::BvTimeout {
            command: "bv --robot-triage".to_owned(),
            timeout_ms: 30000,
        })]);
        let adapter = BvAdapter::with_runner(runner);
        let result = adapter.exec_read(&BvCommand::robot_triage()).await;
        assert!(matches!(result, Err(BvError::BvTimeout { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn adapter_exec_json_parse_error() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockBvRunner::new(vec![MockBvRunner::success("not valid json")]);
        let adapter = BvAdapter::with_runner(runner);
        let result: Result<serde_json::Value, _> =
            adapter.exec_json(&BvCommand::robot_triage()).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, BvError::BvParseError { .. }));
        Ok(())
    }

    // ── JSON response parsing tests ───────────────────────────────────

    #[tokio::test]
    async fn adapter_parse_triage_response() -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::json!({
            "quick_ref": {
                "top_picks": [
                    {
                        "id": "bead-1",
                        "title": "Fix login bug",
                        "score": 9.5,
                        "reasons": ["critical path", "easy fix"],
                        "unblocks": 3
                    }
                ],
                "open_count": 12,
                "actionable_count": 5
            },
            "recommendations": [
                {
                    "id": "bead-2",
                    "title": "Add caching",
                    "score": 7.0,
                    "reasons": ["performance"],
                    "action": "implement",
                    "unblocks_ids": ["bead-3"],
                    "blocked_by": []
                }
            ],
            "quick_wins": ["bead-4", "bead-5"],
            "blockers_to_clear": ["bead-6"]
        });

        let runner = MockBvRunner::new(vec![MockBvRunner::success(&json.to_string())]);
        let adapter = BvAdapter::with_runner(runner);
        let triage: TriageResponse = adapter.exec_json(&BvCommand::robot_triage()).await?;

        assert_eq!(triage.quick_ref.open_count, 12);
        assert_eq!(triage.quick_ref.actionable_count, 5);
        assert_eq!(triage.quick_ref.top_picks.len(), 1);
        assert_eq!(triage.quick_ref.top_picks[0].id, "bead-1");
        assert_eq!(triage.quick_ref.top_picks[0].title, "Fix login bug");
        assert!((triage.quick_ref.top_picks[0].score - 9.5).abs() < f64::EPSILON);
        assert_eq!(triage.quick_ref.top_picks[0].unblocks, 3);

        assert_eq!(triage.recommendations.len(), 1);
        assert_eq!(triage.recommendations[0].id, "bead-2");
        assert_eq!(triage.recommendations[0].action, "implement");
        assert_eq!(triage.recommendations[0].unblocks_ids, vec!["bead-3"]);

        assert_eq!(triage.quick_wins, vec!["bead-4", "bead-5"]);
        assert_eq!(triage.blockers_to_clear, vec!["bead-6"]);
        Ok(())
    }

    #[tokio::test]
    async fn adapter_parse_next_bead_response() -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::json!({
            "id": "bead-7",
            "title": "Implement auth flow",
            "score": 8.2,
            "reasons": ["unblocks 4 beads", "well-scoped"],
            "action": "start implementing"
        });

        let runner = MockBvRunner::new(vec![MockBvRunner::success(&json.to_string())]);
        let adapter = BvAdapter::with_runner(runner);
        let next: NextBeadResponse = adapter.exec_json(&BvCommand::robot_next()).await?;

        assert_eq!(next.id, "bead-7");
        assert_eq!(next.title, "Implement auth flow");
        assert!((next.score - 8.2).abs() < f64::EPSILON);
        assert_eq!(next.reasons.len(), 2);
        assert_eq!(next.action, "start implementing");
        Ok(())
    }

    #[test]
    fn triage_response_forward_compatible() -> Result<(), Box<dyn std::error::Error>> {
        // Minimal JSON with only required fields — all defaulted fields should work.
        let json = r#"{"quick_ref":{},"recommendations":[]}"#;
        let triage: TriageResponse = serde_json::from_str(json)?;
        assert!(triage.quick_ref.top_picks.is_empty());
        assert_eq!(triage.quick_ref.open_count, 0);
        assert_eq!(triage.quick_ref.actionable_count, 0);
        assert!(triage.recommendations.is_empty());
        assert!(triage.quick_wins.is_empty());
        assert!(triage.blockers_to_clear.is_empty());
        Ok(())
    }

    #[test]
    fn next_bead_response_forward_compatible() -> Result<(), Box<dyn std::error::Error>> {
        // Only required fields present.
        let json = r#"{"id":"b-1","title":"Test"}"#;
        let next: NextBeadResponse = serde_json::from_str(json)?;
        assert_eq!(next.id, "b-1");
        assert_eq!(next.title, "Test");
        assert!((next.score - 0.0).abs() < f64::EPSILON);
        assert!(next.reasons.is_empty());
        assert!(next.action.is_empty());
        Ok(())
    }
}
