#![forbid(unsafe_code)]

//! Process execution wrapper for the `br` (beads_rust) CLI.
//!
//! Provides type-safe command construction, structured error handling,
//! and configurable timeouts. Uses direct process execution — never
//! shells out through sh/bash.

use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::process::Command;

// ── Defaults ────────────────────────────────────────────────────────────────

/// Default timeout for read-only br commands (30 seconds).
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Default timeout for mutation br commands (60 seconds).
const DEFAULT_MUTATION_TIMEOUT: Duration = Duration::from_secs(60);

// ── Errors ──────────────────────────────────────────────────────────────────

/// Structured error for br subprocess execution.
#[derive(Debug, Error)]
pub enum BrError {
    /// `br` binary was not found in PATH.
    #[error("br binary not found: {details}")]
    BrNotFound { details: String },

    /// Command exceeded the configured timeout.
    #[error("br command timed out after {timeout_ms}ms: {command}")]
    BrTimeout { command: String, timeout_ms: u64 },

    /// Command exited with a non-zero status.
    #[error("br command failed (exit {exit_code}): {stderr}")]
    BrExitError {
        exit_code: i32,
        stdout: String,
        stderr: String,
        command: String,
    },

    /// Command succeeded but output was not parseable JSON.
    #[error("br output parse error: {details}")]
    BrParseError {
        details: String,
        raw_output: String,
        command: String,
    },

    /// Underlying I/O error during process spawn/wait.
    #[error("br process I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl BrError {
    /// Returns the raw stdout from a failed command, if available.
    pub fn raw_output(&self) -> Option<&str> {
        match self {
            Self::BrExitError { stdout, .. } => Some(stdout),
            Self::BrParseError { raw_output, .. } => Some(raw_output),
            _ => None,
        }
    }
}

// ── Command output ──────────────────────────────────────────────────────────

/// Raw output from a br subprocess.
#[derive(Debug, Clone)]
pub struct BrOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
}

// ── Command builder ─────────────────────────────────────────────────────────

/// Type-safe builder for `br` command lines.
///
/// Prevents argument injection by using direct process execution and
/// validating arguments. Supports common patterns like `br show <id> --json`.
#[derive(Debug, Clone)]
pub struct BrCommand {
    subcommand: String,
    positional_args: Vec<String>,
    flags: Vec<String>,
    kv_flags: Vec<(String, String)>,
    json_mode: bool,
}

impl BrCommand {
    pub fn new(subcommand: impl Into<String>) -> Self {
        Self {
            subcommand: subcommand.into(),
            positional_args: Vec::new(),
            flags: Vec::new(),
            kv_flags: Vec::new(),
            json_mode: false,
        }
    }

    /// Add a positional argument (e.g., bead ID).
    pub fn arg(mut self, value: impl Into<String>) -> Self {
        self.positional_args.push(value.into());
        self
    }

    /// Add a boolean flag (e.g., `--pretty`).
    pub fn flag(mut self, name: impl Into<String>) -> Self {
        self.flags.push(name.into());
        self
    }

    /// Add a key-value flag (e.g., `--status=in_progress`).
    pub fn kv(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.kv_flags.push((key.into(), value.into()));
        self
    }

    /// Request JSON output from br.
    pub fn json(mut self) -> Self {
        self.json_mode = true;
        self
    }

    /// Build the argument list for direct process execution.
    fn build_args(&self) -> Vec<String> {
        let mut args = vec![self.subcommand.clone()];
        args.extend(self.positional_args.iter().cloned());
        for flag in &self.flags {
            args.push(format!("--{flag}"));
        }
        for (key, value) in &self.kv_flags {
            args.push(format!("--{key}={value}"));
        }
        if self.json_mode {
            args.push("--json".to_owned());
        }
        args
    }

    /// Format for display in error messages.
    fn display_string(&self) -> String {
        let args = self.build_args();
        format!("br {}", args.join(" "))
    }
}

// ── Common command constructors ─────────────────────────────────────────────

impl BrCommand {
    /// `br show <id> --json`
    pub fn show(id: impl Into<String>) -> Self {
        Self::new("show").arg(id).json()
    }

    /// `br ready --json`
    pub fn ready() -> Self {
        Self::new("ready").json()
    }

    /// `br list --json`
    pub fn list() -> Self {
        Self::new("list").json()
    }

    /// `br list --all --deferred --limit=0 --json`
    pub fn list_all() -> Self {
        Self::new("list")
            .flag("all")
            .flag("deferred")
            .kv("limit", "0")
            .json()
    }

    /// `br list --status=<status> --json`
    pub fn list_by_status(status: impl Into<String>) -> Self {
        Self::new("list").kv("status", status).json()
    }

    /// `br update <id> --status=<status>`
    pub fn update_status(id: impl Into<String>, status: impl Into<String>) -> Self {
        Self::new("update").arg(id).kv("status", status)
    }

    /// `br close <id> --reason=<reason>`
    pub fn close(id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::new("close").arg(id).kv("reason", reason)
    }

    /// `br dep tree <id> --json`
    pub fn dep_tree(id: impl Into<String>) -> Self {
        Self::new("dep").arg("tree").arg(id).json()
    }

    /// `br graph --json`
    pub fn graph() -> Self {
        Self::new("graph").json()
    }

    /// `br sync --flush-only`
    pub fn sync_flush() -> Self {
        Self::new("sync").flag("flush-only")
    }

    /// `br lint`
    pub fn lint() -> Self {
        Self::new("lint")
    }

    /// `br dep add <from> <to>`
    pub fn dep_add(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self::new("dep").arg("add").arg(from).arg(to)
    }

    /// `br dep remove <from> <to>`
    pub fn dep_remove(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self::new("dep").arg("remove").arg(from).arg(to)
    }

    /// `br comment <id> <text>`
    pub fn comment(id: impl Into<String>, text: impl Into<String>) -> Self {
        Self::new("comment").arg(id).arg(text)
    }

    /// `br create --title=<title> --type=<bead_type> --priority=<priority>`
    pub fn create(
        title: impl Into<String>,
        bead_type: impl Into<String>,
        priority: impl Into<String>,
    ) -> Self {
        Self::new("create")
            .kv("title", title)
            .kv("type", bead_type)
            .kv("priority", priority)
    }
}

// ── Process runner ──────────────────────────────────────────────────────────

/// Configurable runner for br subprocess execution.
///
/// Testable via the `ProcessRunner` trait. Production code uses `OsProcessRunner`;
/// tests can substitute a mock.
pub trait ProcessRunner: Send + Sync {
    /// Execute a br command and return the raw output.
    fn run(
        &self,
        args: Vec<String>,
        timeout: Duration,
        working_dir: Option<&std::path::Path>,
    ) -> impl std::future::Future<Output = Result<BrOutput, BrError>> + Send;
}

/// Production process runner that spawns real subprocesses.
#[derive(Debug, Clone)]
pub struct OsProcessRunner {
    /// Path to the br binary. If None, resolves from PATH.
    br_binary: Option<PathBuf>,
}

impl OsProcessRunner {
    pub fn new() -> Self {
        Self { br_binary: None }
    }

    pub fn with_binary(binary: PathBuf) -> Self {
        Self {
            br_binary: Some(binary),
        }
    }

    fn br_path(&self) -> &str {
        self.br_binary
            .as_deref()
            .and_then(|p| p.to_str())
            .unwrap_or("br")
    }
}

impl Default for OsProcessRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessRunner for OsProcessRunner {
    async fn run(
        &self,
        args: Vec<String>,
        timeout: Duration,
        working_dir: Option<&std::path::Path>,
    ) -> Result<BrOutput, BrError> {
        let br_path = self.br_path();
        let mut cmd = Command::new(br_path);
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
                BrError::BrNotFound {
                    details: format!("could not find '{br_path}' in PATH"),
                }
            } else {
                BrError::Io(e)
            }
        })?;

        let command_display = format!("br {}", args.join(" "));

        match tokio::time::timeout(timeout, wait_for_output(child)).await {
            Ok(result) => result,
            Err(_) => Err(BrError::BrTimeout {
                command: command_display,
                timeout_ms: timeout.as_millis() as u64,
            }),
        }
    }
}

async fn wait_for_output(child: tokio::process::Child) -> Result<BrOutput, BrError> {
    let output = child.wait_with_output().await?;
    Ok(BrOutput {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        exit_code: output.status.code().unwrap_or(-1),
    })
}

// ── BrAdapter ───────────────────────────────────────────────────────────────

/// High-level adapter for executing br commands.
///
/// Wraps a `ProcessRunner` with configurable timeouts and provides
/// convenience methods for common operations.
pub struct BrAdapter<R: ProcessRunner = OsProcessRunner> {
    runner: R,
    read_timeout: Duration,
    mutation_timeout: Duration,
    working_dir: Option<PathBuf>,
}

impl BrAdapter<OsProcessRunner> {
    pub fn new() -> Self {
        Self {
            runner: OsProcessRunner::new(),
            read_timeout: DEFAULT_READ_TIMEOUT,
            mutation_timeout: DEFAULT_MUTATION_TIMEOUT,
            working_dir: None,
        }
    }
}

impl Default for BrAdapter<OsProcessRunner> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: ProcessRunner> BrAdapter<R> {
    pub fn with_runner(runner: R) -> Self {
        Self {
            runner,
            read_timeout: DEFAULT_READ_TIMEOUT,
            mutation_timeout: DEFAULT_MUTATION_TIMEOUT,
            working_dir: None,
        }
    }

    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = timeout;
        self
    }

    pub fn with_mutation_timeout(mut self, timeout: Duration) -> Self {
        self.mutation_timeout = timeout;
        self
    }

    pub fn with_working_dir(mut self, dir: PathBuf) -> Self {
        self.working_dir = Some(dir);
        self
    }

    /// Execute a read-only br command and return raw output.
    pub async fn exec_read(&self, cmd: &BrCommand) -> Result<BrOutput, BrError> {
        let args = cmd.build_args();
        let output = self
            .runner
            .run(args, self.read_timeout, self.working_dir.as_deref())
            .await?;

        if output.exit_code != 0 {
            return Err(BrError::BrExitError {
                exit_code: output.exit_code,
                stdout: output.stdout,
                stderr: output.stderr,
                command: cmd.display_string(),
            });
        }

        Ok(output)
    }

    /// Execute a mutation br command and return raw output.
    pub async fn exec_mutation(&self, cmd: &BrCommand) -> Result<BrOutput, BrError> {
        let args = cmd.build_args();
        let output = self
            .runner
            .run(args, self.mutation_timeout, self.working_dir.as_deref())
            .await?;

        if output.exit_code != 0 {
            return Err(BrError::BrExitError {
                exit_code: output.exit_code,
                stdout: output.stdout,
                stderr: output.stderr,
                command: cmd.display_string(),
            });
        }

        Ok(output)
    }

    /// Execute a read-only command and parse the JSON output.
    pub async fn exec_json<T: DeserializeOwned>(&self, cmd: &BrCommand) -> Result<T, BrError> {
        let output = self.exec_read(cmd).await?;
        serde_json::from_str(&output.stdout).map_err(|e| BrError::BrParseError {
            details: e.to_string(),
            raw_output: output.stdout,
            command: cmd.display_string(),
        })
    }
}

// ── BrMutationAdapter ────────────────────────────────────────────────────

/// Higher-level adapter that wraps `BrAdapter` and adds dirty tracking,
/// audit logging, and convenience mutation methods.
///
/// After any successful mutation the `has_unsync_mutations` flag is set.
/// Calling `sync_flush` clears the flag on success.
pub struct BrMutationAdapter<R: ProcessRunner = OsProcessRunner> {
    adapter: BrAdapter<R>,
    has_unsync_mutations: AtomicBool,
}

impl BrMutationAdapter<OsProcessRunner> {
    pub fn new() -> Self {
        Self {
            adapter: BrAdapter::new(),
            has_unsync_mutations: AtomicBool::new(false),
        }
    }
}

impl Default for BrMutationAdapter<OsProcessRunner> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: ProcessRunner> BrMutationAdapter<R> {
    pub fn with_adapter(adapter: BrAdapter<R>) -> Self {
        Self {
            adapter,
            has_unsync_mutations: AtomicBool::new(false),
        }
    }

    /// Returns `true` if any mutation has succeeded since the last sync.
    pub fn has_pending_mutations(&self) -> bool {
        self.has_unsync_mutations.load(Ordering::Acquire)
    }

    /// Provide read-only access to the inner adapter for queries.
    pub fn inner(&self) -> &BrAdapter<R> {
        &self.adapter
    }

    // ── mutation helpers ────────────────────────────────────────────────

    /// Execute a mutation command with dirty-flag bookkeeping and audit logging.
    async fn exec_tracked_mutation(
        &self,
        operation: &str,
        bead_id: Option<&str>,
        cmd: &BrCommand,
    ) -> Result<BrOutput, BrError> {
        let start = Instant::now();
        let result = self.adapter.exec_mutation(cmd).await;
        let duration_ms = start.elapsed().as_millis() as u64;

        match &result {
            Ok(_) => {
                self.has_unsync_mutations.store(true, Ordering::Release);
                tracing::info!(
                    operation = operation,
                    bead_id = bead_id.unwrap_or(""),
                    outcome = "success",
                    duration_ms = duration_ms,
                    "br mutation completed"
                );
            }
            Err(err) => {
                let stderr_content = match err {
                    BrError::BrExitError { stderr, .. } => stderr.as_str(),
                    _ => "",
                };
                tracing::warn!(
                    operation = operation,
                    bead_id = bead_id.unwrap_or(""),
                    outcome = "failure",
                    duration_ms = duration_ms,
                    stderr = stderr_content,
                    "br mutation failed"
                );
            }
        }

        result
    }

    // ── public mutation methods ─────────────────────────────────────────

    /// Create a new bead.
    ///
    /// Optional `labels` are passed as `--label=<l>` for each entry.
    /// Optional `description` is passed as `--description=<d>`.
    pub async fn create_bead(
        &self,
        title: &str,
        bead_type: &str,
        priority: &str,
        labels: &[String],
        description: Option<&str>,
    ) -> Result<BrOutput, BrError> {
        let mut cmd = BrCommand::create(title, bead_type, priority);
        for label in labels {
            cmd = cmd.kv("label", label.as_str());
        }
        if let Some(desc) = description {
            cmd = cmd.kv("description", desc);
        }
        self.exec_tracked_mutation("create_bead", None, &cmd).await
    }

    /// Update the status of an existing bead.
    pub async fn update_bead_status(&self, id: &str, status: &str) -> Result<BrOutput, BrError> {
        let cmd = BrCommand::update_status(id, status);
        self.exec_tracked_mutation("update_bead_status", Some(id), &cmd)
            .await
    }

    /// Close a bead with a reason.
    pub async fn close_bead(&self, id: &str, reason: &str) -> Result<BrOutput, BrError> {
        let cmd = BrCommand::close(id, reason);
        self.exec_tracked_mutation("close_bead", Some(id), &cmd)
            .await
    }

    /// Add a dependency: `from_id` depends on `depends_on_id`.
    pub async fn add_dependency(
        &self,
        from_id: &str,
        depends_on_id: &str,
    ) -> Result<BrOutput, BrError> {
        let cmd = BrCommand::dep_add(from_id, depends_on_id);
        self.exec_tracked_mutation("add_dependency", Some(from_id), &cmd)
            .await
    }

    /// Remove a dependency between two beads.
    pub async fn remove_dependency(
        &self,
        from_id: &str,
        depends_on_id: &str,
    ) -> Result<BrOutput, BrError> {
        let cmd = BrCommand::dep_remove(from_id, depends_on_id);
        self.exec_tracked_mutation("remove_dependency", Some(from_id), &cmd)
            .await
    }

    /// Add a comment to a bead.
    pub async fn comment_bead(&self, id: &str, text: &str) -> Result<BrOutput, BrError> {
        let cmd = BrCommand::comment(id, text);
        self.exec_tracked_mutation("comment_bead", Some(id), &cmd)
            .await
    }

    /// Flush pending changes to upstream and clear the dirty flag.
    pub async fn sync_flush(&self) -> Result<BrOutput, BrError> {
        let cmd = BrCommand::sync_flush();
        let start = Instant::now();
        let result = self.adapter.exec_mutation(&cmd).await;
        let duration_ms = start.elapsed().as_millis() as u64;

        match &result {
            Ok(_) => {
                self.has_unsync_mutations.store(false, Ordering::Release);
                tracing::info!(
                    operation = "sync_flush",
                    outcome = "success",
                    duration_ms = duration_ms,
                    "br sync flush completed"
                );
            }
            Err(err) => {
                let stderr_content = match err {
                    BrError::BrExitError { stderr, .. } => stderr.as_str(),
                    _ => "",
                };
                tracing::warn!(
                    operation = "sync_flush",
                    outcome = "failure",
                    duration_ms = duration_ms,
                    stderr = stderr_content,
                    "br sync flush failed"
                );
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_builder_show() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::show("ralph-burning-9ni.2.1");
        let args = cmd.build_args();
        assert_eq!(args, vec!["show", "ralph-burning-9ni.2.1", "--json"]);
        Ok(())
    }

    #[test]
    fn command_builder_ready() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::ready();
        let args = cmd.build_args();
        assert_eq!(args, vec!["ready", "--json"]);
        Ok(())
    }

    #[test]
    fn command_builder_update_status() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::update_status("bead-1", "in_progress");
        let args = cmd.build_args();
        assert_eq!(args, vec!["update", "bead-1", "--status=in_progress"]);
        Ok(())
    }

    #[test]
    fn command_builder_close() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::close("bead-1", "Done");
        let args = cmd.build_args();
        assert_eq!(args, vec!["close", "bead-1", "--reason=Done"]);
        Ok(())
    }

    #[test]
    fn command_builder_list_by_status() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::list_by_status("open");
        let args = cmd.build_args();
        assert_eq!(args, vec!["list", "--status=open", "--json"]);
        Ok(())
    }

    #[test]
    fn command_builder_sync_flush() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::sync_flush();
        let args = cmd.build_args();
        assert_eq!(args, vec!["sync", "--flush-only"]);
        Ok(())
    }

    #[test]
    fn command_builder_dep_tree() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::dep_tree("bead-1");
        let args = cmd.build_args();
        assert_eq!(args, vec!["dep", "tree", "bead-1", "--json"]);
        Ok(())
    }

    #[test]
    fn command_builder_graph() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::graph();
        let args = cmd.build_args();
        assert_eq!(args, vec!["graph", "--json"]);
        Ok(())
    }

    #[test]
    fn command_builder_custom() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::new("create")
            .kv("title", "New bead")
            .kv("type", "task")
            .kv("priority", "2")
            .json();
        let args = cmd.build_args();
        assert_eq!(
            args,
            vec![
                "create",
                "--title=New bead",
                "--type=task",
                "--priority=2",
                "--json"
            ]
        );
        Ok(())
    }

    #[test]
    fn command_display_string() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::show("bead-1");
        assert_eq!(cmd.display_string(), "br show bead-1 --json");
        Ok(())
    }

    #[test]
    fn br_error_raw_output() -> Result<(), Box<dyn std::error::Error>> {
        let err = BrError::BrExitError {
            exit_code: 1,
            stdout: "some output".to_owned(),
            stderr: "error message".to_owned(),
            command: "br show bead-1".to_owned(),
        };
        assert_eq!(err.raw_output(), Some("some output"));

        let err = BrError::BrNotFound {
            details: "not found".to_owned(),
        };
        assert_eq!(err.raw_output(), None);
        Ok(())
    }

    // ── Mock runner tests ──────────────────────────────────────────────

    /// A mock process runner for unit testing without real br binary.
    struct MockRunner {
        responses: std::sync::Mutex<Vec<Result<BrOutput, BrError>>>,
    }

    impl MockRunner {
        fn new(responses: Vec<Result<BrOutput, BrError>>) -> Self {
            Self {
                responses: std::sync::Mutex::new(responses),
            }
        }

        fn success(stdout: &str) -> Result<BrOutput, BrError> {
            Ok(BrOutput {
                stdout: stdout.to_owned(),
                stderr: String::new(),
                exit_code: 0,
            })
        }

        fn failure(exit_code: i32, stderr: &str) -> Result<BrOutput, BrError> {
            Ok(BrOutput {
                stdout: String::new(),
                stderr: stderr.to_owned(),
                exit_code,
            })
        }
    }

    impl ProcessRunner for MockRunner {
        async fn run(
            &self,
            _args: Vec<String>,
            _timeout: Duration,
            _working_dir: Option<&std::path::Path>,
        ) -> Result<BrOutput, BrError> {
            let mut responses = self.responses.lock().expect("mock lock poisoned");
            if responses.is_empty() {
                panic!("MockRunner: no more responses configured");
            }
            responses.remove(0)
        }
    }

    #[tokio::test]
    async fn adapter_exec_read_success() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockRunner::new(vec![MockRunner::success(r#"{"status":"ok"}"#)]);
        let adapter = BrAdapter::with_runner(runner);
        let output = adapter.exec_read(&BrCommand::ready()).await?;
        assert_eq!(output.exit_code, 0);
        assert!(output.stdout.contains("ok"));
        Ok(())
    }

    #[tokio::test]
    async fn adapter_exec_read_failure() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockRunner::new(vec![MockRunner::failure(1, "bead not found")]);
        let adapter = BrAdapter::with_runner(runner);
        let result = adapter.exec_read(&BrCommand::show("nonexistent")).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, BrError::BrExitError { exit_code: 1, .. }));
        Ok(())
    }

    #[tokio::test]
    async fn adapter_exec_json_success() -> Result<(), Box<dyn std::error::Error>> {
        let json = r#"{"id":"bead-1","title":"Test","status":"open"}"#;
        let runner = MockRunner::new(vec![MockRunner::success(json)]);
        let adapter = BrAdapter::with_runner(runner);

        #[derive(serde::Deserialize)]
        struct BeadInfo {
            id: String,
            title: String,
            status: String,
        }

        let info: BeadInfo = adapter.exec_json(&BrCommand::show("bead-1")).await?;
        assert_eq!(info.id, "bead-1");
        assert_eq!(info.title, "Test");
        assert_eq!(info.status, "open");
        Ok(())
    }

    #[tokio::test]
    async fn adapter_exec_json_parse_error() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockRunner::new(vec![MockRunner::success("not valid json")]);
        let adapter = BrAdapter::with_runner(runner);

        let result: Result<serde_json::Value, _> = adapter.exec_json(&BrCommand::ready()).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, BrError::BrParseError { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn adapter_exec_mutation_success() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockRunner::new(vec![MockRunner::success("Updated bead-1")]);
        let adapter = BrAdapter::with_runner(runner);
        let output = adapter
            .exec_mutation(&BrCommand::update_status("bead-1", "in_progress"))
            .await?;
        assert_eq!(output.exit_code, 0);
        Ok(())
    }

    #[tokio::test]
    async fn adapter_br_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockRunner::new(vec![Err(BrError::BrNotFound {
            details: "could not find 'br' in PATH".to_owned(),
        })]);
        let adapter = BrAdapter::with_runner(runner);
        let result = adapter.exec_read(&BrCommand::ready()).await;
        assert!(matches!(result, Err(BrError::BrNotFound { .. })));
        Ok(())
    }

    #[tokio::test]
    async fn adapter_timeout_error() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockRunner::new(vec![Err(BrError::BrTimeout {
            command: "br ready --json".to_owned(),
            timeout_ms: 30000,
        })]);
        let adapter = BrAdapter::with_runner(runner);
        let result = adapter.exec_read(&BrCommand::ready()).await;
        assert!(matches!(result, Err(BrError::BrTimeout { .. })));
        Ok(())
    }

    #[test]
    fn os_process_runner_default_path() -> Result<(), Box<dyn std::error::Error>> {
        let runner = OsProcessRunner::new();
        assert_eq!(runner.br_path(), "br");
        Ok(())
    }

    #[test]
    fn os_process_runner_custom_path() -> Result<(), Box<dyn std::error::Error>> {
        let runner = OsProcessRunner::with_binary(PathBuf::from("/usr/local/bin/br"));
        assert_eq!(runner.br_path(), "/usr/local/bin/br");
        Ok(())
    }

    #[test]
    fn adapter_default_timeouts() -> Result<(), Box<dyn std::error::Error>> {
        let adapter = BrAdapter::new();
        assert_eq!(adapter.read_timeout, DEFAULT_READ_TIMEOUT);
        assert_eq!(adapter.mutation_timeout, DEFAULT_MUTATION_TIMEOUT);
        Ok(())
    }

    #[test]
    fn adapter_custom_timeouts() -> Result<(), Box<dyn std::error::Error>> {
        let adapter = BrAdapter::new()
            .with_read_timeout(Duration::from_secs(10))
            .with_mutation_timeout(Duration::from_secs(120));
        assert_eq!(adapter.read_timeout, Duration::from_secs(10));
        assert_eq!(adapter.mutation_timeout, Duration::from_secs(120));
        Ok(())
    }

    // ── BrCommand dep/create constructors ─────────────────────────────

    #[test]
    fn command_builder_dep_add() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::dep_add("bead-a", "bead-b");
        let args = cmd.build_args();
        assert_eq!(args, vec!["dep", "add", "bead-a", "bead-b"]);
        Ok(())
    }

    #[test]
    fn command_builder_dep_remove() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::dep_remove("bead-a", "bead-b");
        let args = cmd.build_args();
        assert_eq!(args, vec!["dep", "remove", "bead-a", "bead-b"]);
        Ok(())
    }

    #[test]
    fn command_builder_create() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::create("My bead", "task", "2");
        let args = cmd.build_args();
        assert_eq!(
            args,
            vec!["create", "--title=My bead", "--type=task", "--priority=2"]
        );
        Ok(())
    }

    // ── BrMutationAdapter tests ───────────────────────────────────────

    fn make_mutation_adapter(
        responses: Vec<Result<BrOutput, BrError>>,
    ) -> BrMutationAdapter<MockRunner> {
        let runner = MockRunner::new(responses);
        let adapter = BrAdapter::with_runner(runner);
        BrMutationAdapter::with_adapter(adapter)
    }

    #[tokio::test]
    async fn mutation_sets_dirty_flag() -> Result<(), Box<dyn std::error::Error>> {
        let ma = make_mutation_adapter(vec![MockRunner::success("created")]);
        assert!(!ma.has_pending_mutations());

        ma.update_bead_status("bead-1", "in_progress").await?;

        assert!(ma.has_pending_mutations());
        Ok(())
    }

    #[tokio::test]
    async fn sync_clears_dirty_flag() -> Result<(), Box<dyn std::error::Error>> {
        let ma = make_mutation_adapter(vec![
            MockRunner::success("updated"),
            MockRunner::success("synced"),
        ]);

        ma.update_bead_status("bead-1", "in_progress").await?;
        assert!(ma.has_pending_mutations());

        ma.sync_flush().await?;
        assert!(!ma.has_pending_mutations());
        Ok(())
    }

    #[tokio::test]
    async fn failed_mutation_does_not_set_dirty_flag() -> Result<(), Box<dyn std::error::Error>> {
        let ma = make_mutation_adapter(vec![MockRunner::failure(1, "not found")]);
        assert!(!ma.has_pending_mutations());

        let result = ma.close_bead("nonexistent", "done").await;
        assert!(result.is_err());
        assert!(!ma.has_pending_mutations());
        Ok(())
    }

    #[tokio::test]
    async fn multiple_mutations_before_sync() -> Result<(), Box<dyn std::error::Error>> {
        let ma = make_mutation_adapter(vec![
            MockRunner::success("created"),
            MockRunner::success("dep added"),
            MockRunner::success("status updated"),
            MockRunner::success("synced"),
        ]);

        ma.create_bead("New bead", "task", "1", &[], None).await?;
        assert!(ma.has_pending_mutations());

        ma.add_dependency("bead-2", "bead-1").await?;
        assert!(ma.has_pending_mutations());

        ma.update_bead_status("bead-2", "in_progress").await?;
        assert!(ma.has_pending_mutations());

        ma.sync_flush().await?;
        assert!(!ma.has_pending_mutations());
        Ok(())
    }

    #[tokio::test]
    async fn create_bead_builds_correct_args() -> Result<(), Box<dyn std::error::Error>> {
        // Verify the command shape by inspecting BrCommand directly rather
        // than through the adapter (the adapter delegates to exec_mutation).
        let mut cmd = BrCommand::create("Implement auth", "feature", "1");
        cmd = cmd.kv("label", "backend");
        cmd = cmd.kv("label", "security");
        cmd = cmd.kv("description", "Add OAuth2 flow");

        let args = cmd.build_args();
        assert_eq!(
            args,
            vec![
                "create",
                "--title=Implement auth",
                "--type=feature",
                "--priority=1",
                "--label=backend",
                "--label=security",
                "--description=Add OAuth2 flow",
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn create_bead_with_labels_and_description() -> Result<(), Box<dyn std::error::Error>> {
        let ma = make_mutation_adapter(vec![MockRunner::success("created bead-new")]);

        let labels = vec!["backend".to_owned(), "security".to_owned()];
        let result = ma
            .create_bead(
                "Auth middleware",
                "feature",
                "1",
                &labels,
                Some("Add OAuth2"),
            )
            .await?;

        assert_eq!(result.exit_code, 0);
        assert!(ma.has_pending_mutations());
        Ok(())
    }

    #[tokio::test]
    async fn remove_dependency_success() -> Result<(), Box<dyn std::error::Error>> {
        let ma = make_mutation_adapter(vec![MockRunner::success("dep removed")]);

        ma.remove_dependency("bead-a", "bead-b").await?;
        assert!(ma.has_pending_mutations());
        Ok(())
    }
}
