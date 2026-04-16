#![forbid(unsafe_code)]

//! Process execution wrapper for the `br` (beads_rust) CLI.
//!
//! Provides type-safe command construction, structured error handling,
//! and configurable timeouts. Uses direct process execution — never
//! shells out through sh/bash.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use nix::fcntl::{Flock, FlockArg};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;
use tokio::process::Command;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::adapters::br_health::{
    beads_health_failure_details, check_beads_health_with_availability,
};

// ── Defaults ────────────────────────────────────────────────────────────────

/// Default timeout for read-only br commands (30 seconds).
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Default timeout for mutation br commands (60 seconds).
const DEFAULT_MUTATION_TIMEOUT: Duration = Duration::from_secs(60);

/// Poll interval for synchronous availability probes.
const AVAILABILITY_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Legacy single-file marker from the first crash-recovery implementation.
///
/// New code writes per-mutation journal records under `PENDING_MUTATIONS_DIR`,
/// but we still honor this file as a dirty signal so pre-existing workspaces
/// stay protected until their next successful flush clears it.
const LEGACY_PENDING_MUTATIONS_MARKER: &str = ".beads/.br-unsynced-mutations";

/// Per-mutation journal persisted inside `.beads/` whenever a local mutation
/// may still need `br sync --flush-only` before external imports are safe.
///
/// Each successful mutation keeps its own record so recovered adapters can
/// verify which operation(s) were pending instead of relying on a single
/// repo-global boolean marker.
const PENDING_MUTATIONS_DIR: &str = ".beads/.br-unsynced-mutations.d";

/// Repo-wide lock file used to serialize mutation/flush/import decisions across
/// adapters and processes that share a working tree.
const REPO_OPERATION_LOCK: &str = ".beads/.br-sync.lock";

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PendingMutationRecord {
    adapter_id: String,
    operation: String,
    bead_id: Option<String>,
    status: Option<String>,
}

impl PendingMutationRecord {
    fn new(
        adapter_id: String,
        operation: impl Into<String>,
        bead_id: Option<&str>,
        status: Option<&str>,
    ) -> Self {
        Self {
            adapter_id,
            operation: operation.into(),
            bead_id: bead_id.map(ToOwned::to_owned),
            status: status.map(ToOwned::to_owned),
        }
    }

    fn unknown(adapter_id: impl Into<String>, operation: impl Into<String>) -> Self {
        Self {
            adapter_id: adapter_id.into(),
            operation: operation.into(),
            bead_id: None,
            status: None,
        }
    }
}

#[derive(Debug, Clone)]
struct PendingMutationSnapshot {
    records: Vec<PendingMutationRecord>,
    legacy_marker_present: bool,
}

impl PendingMutationSnapshot {
    fn is_dirty(&self) -> bool {
        self.legacy_marker_present || !self.records.is_empty()
    }

    fn has_foreign_records_for(&self, adapter_id: &str) -> bool {
        self.records
            .iter()
            .any(|record| record.adapter_id != adapter_id)
    }

    fn first_foreign_record_for(&self, adapter_id: &str) -> Option<&PendingMutationRecord> {
        self.records
            .iter()
            .find(|record| record.adapter_id != adapter_id)
    }
}

#[derive(Debug)]
struct RepoOperationGuard {
    #[cfg(unix)]
    _file: Flock<std::fs::File>,
}

#[derive(Debug, Clone)]
pub struct RecoveredStatusUpdate {
    adapter_id: String,
    bead_id: String,
    status: String,
}

#[derive(Debug, Clone)]
pub enum SyncIfDirtyOutcome {
    Clean,
    Flushed {
        output: BrOutput,
        flushed_mutations: usize,
        recovered_status_updates: Vec<RecoveredStatusUpdate>,
    },
}

impl SyncIfDirtyOutcome {
    pub fn is_clean(&self) -> bool {
        matches!(self, Self::Clean)
    }

    pub fn output(&self) -> Option<&BrOutput> {
        match self {
            Self::Clean => None,
            Self::Flushed { output, .. } => Some(output),
        }
    }

    pub fn flushed_mutations(&self) -> usize {
        match self {
            Self::Clean => 0,
            Self::Flushed {
                flushed_mutations, ..
            } => *flushed_mutations,
        }
    }

    pub fn includes_update_status(&self, bead_id: &str, status: &str) -> bool {
        match self {
            Self::Clean => false,
            Self::Flushed {
                recovered_status_updates,
                ..
            } => recovered_status_updates
                .iter()
                .any(|candidate| candidate.bead_id == bead_id && candidate.status == status),
        }
    }

    pub fn includes_owned_update_status(
        &self,
        adapter_id: &str,
        bead_id: &str,
        status: &str,
    ) -> bool {
        match self {
            Self::Clean => false,
            Self::Flushed {
                recovered_status_updates,
                ..
            } => recovered_status_updates.iter().any(|candidate| {
                candidate.adapter_id == adapter_id
                    && candidate.bead_id == bead_id
                    && candidate.status == status
            }),
        }
    }
}

#[derive(Debug, Error)]
pub enum SyncIfDirtyHealthError {
    #[error("{details}")]
    UnsafeBeadsState { details: String },

    #[error(transparent)]
    Br(#[from] BrError),
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

    /// `br sync --import-only`
    pub fn sync_import() -> Self {
        Self::new("sync").flag("import-only")
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

    /// `br comments add <id> <text>`
    pub fn comment(id: impl Into<String>, text: impl Into<String>) -> Self {
        Self::new("comments").arg("add").arg(id).arg(text)
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

    /// Verify that the configured runner can launch `br` in this environment.
    ///
    /// Custom test runners can inherit the default success response so health
    /// gates do not incorrectly fail closed before the runner is even asked to
    /// execute the real command under test.
    fn check_available(&self, _timeout: Duration) -> Result<(), BrError> {
        Ok(())
    }
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

    fn check_available(&self, timeout: Duration) -> Result<(), BrError> {
        let mut child = std::process::Command::new(self.br_path())
            .arg("--version")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    BrError::BrNotFound {
                        details: format!("could not find '{}' in PATH", self.br_path()),
                    }
                } else {
                    BrError::Io(error)
                }
            })?;

        let start = Instant::now();
        loop {
            if child.try_wait()?.is_some() {
                break;
            }

            if start.elapsed() >= timeout {
                let _ = child.kill();
                let _ = child.wait();
                return Err(BrError::BrTimeout {
                    command: format!("{} --version", self.br_path()),
                    timeout_ms: timeout.as_millis() as u64,
                });
            }

            std::thread::sleep(AVAILABILITY_POLL_INTERVAL);
        }

        let output = child.wait_with_output().map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                BrError::BrNotFound {
                    details: format!("could not find '{}' in PATH", self.br_path()),
                }
            } else {
                BrError::Io(error)
            }
        })?;

        if output.status.success() {
            return Ok(());
        }

        Err(BrError::BrExitError {
            exit_code: output.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            command: format!("{} --version", self.br_path()),
        })
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

    /// Verify that the configured `br` runner can be launched from the current
    /// environment without touching repository state.
    pub fn check_available(&self) -> Result<(), BrError> {
        self.check_available_with_timeout(self.read_timeout)
    }

    fn check_available_with_timeout(&self, timeout: Duration) -> Result<(), BrError> {
        self.runner.check_available(timeout)
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
/// Sync lifecycle invariants:
/// - successful local mutations set `has_unsync_mutations`
/// - pending mutation state is persisted under `.beads/.br-unsynced-mutations.d/`
///   with one journal record per adapter instance, and all decisions that can
///   clear or rely on that state are serialized behind `.beads/.br-sync.lock`
/// - a fresh adapter re-reads the repo journal under that lock so crash
///   recovery and long-lived processes see the same pending-mutation view
/// - `sync_flush` is the only operation that clears the dirty flag
/// - `sync_import` never sets the dirty flag and refuses to run while local
///   mutations are still pending, preventing upstream imports from being mixed
///   with unflushed local state; callers must also provide a working directory
///   so the persisted journal can prove the workspace was recovered cleanly
/// - `sync_if_dirty` is the safe convenience path when callers want to flush
///   only if prior mutation steps actually dirtied the local workspace
pub struct BrMutationAdapter<R: ProcessRunner = OsProcessRunner> {
    adapter: BrAdapter<R>,
    adapter_id: String,
    has_unsync_mutations: AtomicBool,
    operation_lock: Mutex<()>,
}

impl BrMutationAdapter<OsProcessRunner> {
    pub fn new() -> Self {
        Self {
            adapter: BrAdapter::new(),
            adapter_id: Uuid::new_v4().to_string(),
            has_unsync_mutations: AtomicBool::new(false),
            operation_lock: Mutex::new(()),
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
        Self::with_adapter_id(adapter, Uuid::new_v4().to_string())
    }

    pub fn with_adapter_id(adapter: BrAdapter<R>, adapter_id: impl Into<String>) -> Self {
        let has_unsync_mutations = Self::recover_pending_mutation_state(&adapter);
        Self {
            adapter,
            adapter_id: adapter_id.into(),
            has_unsync_mutations: AtomicBool::new(has_unsync_mutations),
            operation_lock: Mutex::new(()),
        }
    }

    /// Returns `true` if any mutation has succeeded since the last sync.
    pub fn has_pending_mutations(&self) -> bool {
        self.has_unsync_mutations.load(Ordering::Acquire)
    }

    pub fn adapter_id(&self) -> &str {
        &self.adapter_id
    }

    /// Provide read-only access to the inner adapter for queries.
    pub fn inner(&self) -> &BrAdapter<R> {
        &self.adapter
    }

    fn legacy_pending_mutation_marker_path_for(adapter: &BrAdapter<R>) -> Option<PathBuf> {
        adapter
            .working_dir
            .as_ref()
            .map(|dir| dir.join(LEGACY_PENDING_MUTATIONS_MARKER))
    }

    fn legacy_pending_mutation_marker_path(&self) -> Option<PathBuf> {
        Self::legacy_pending_mutation_marker_path_for(&self.adapter)
    }

    fn pending_mutations_dir_for(adapter: &BrAdapter<R>) -> Option<PathBuf> {
        adapter
            .working_dir
            .as_ref()
            .map(|dir| dir.join(PENDING_MUTATIONS_DIR))
    }

    fn pending_mutations_dir(&self) -> Option<PathBuf> {
        Self::pending_mutations_dir_for(&self.adapter)
    }

    fn pending_mutation_record_path_for(
        adapter: &BrAdapter<R>,
        adapter_id: &str,
    ) -> Option<PathBuf> {
        Self::pending_mutations_dir_for(adapter).map(|dir| dir.join(format!("{adapter_id}.json")))
    }

    fn pending_mutation_record_path(&self) -> Option<PathBuf> {
        Self::pending_mutation_record_path_for(&self.adapter, &self.adapter_id)
    }

    fn repo_operation_lock_path_for(adapter: &BrAdapter<R>) -> Option<PathBuf> {
        adapter
            .working_dir
            .as_ref()
            .map(|dir| dir.join(REPO_OPERATION_LOCK))
    }

    fn acquire_repo_operation_lock(&self) -> Result<Option<RepoOperationGuard>, BrError> {
        let Some(lock_path) = Self::repo_operation_lock_path_for(&self.adapter) else {
            return Ok(None);
        };
        let Some(parent) = lock_path.parent() else {
            return Ok(None);
        };

        std::fs::create_dir_all(parent)?;

        #[cfg(unix)]
        {
            let file = std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(lock_path)?;
            let lock = Flock::lock(file, FlockArg::LockExclusive)
                .map_err(|(_, error)| BrError::Io(std::io::Error::from(error)))?;
            Ok(Some(RepoOperationGuard { _file: lock }))
        }

        #[cfg(not(unix))]
        {
            Ok(Some(RepoOperationGuard {}))
        }
    }

    fn recover_pending_mutation_state(adapter: &BrAdapter<R>) -> bool {
        let Some(working_dir) = adapter.working_dir.as_ref() else {
            return false;
        };

        let lock_path = working_dir.join(REPO_OPERATION_LOCK);
        let Some(parent) = lock_path.parent() else {
            return false;
        };
        if let Err(error) = std::fs::create_dir_all(parent) {
            tracing::warn!(
                lock_path = %lock_path.display(),
                %error,
                "failed to prepare br repo-operation lock; assuming workspace is dirty"
            );
            return true;
        }

        #[cfg(unix)]
        let file = match std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
        {
            Ok(file) => file,
            Err(error) => {
                tracing::warn!(
                    lock_path = %lock_path.display(),
                    %error,
                    "failed to open br repo-operation lock; assuming workspace is dirty"
                );
                return true;
            }
        };

        #[cfg(unix)]
        let _lock = match Flock::lock(file, FlockArg::LockExclusive) {
            Ok(lock) => lock,
            Err((_, error)) => {
                tracing::warn!(
                    lock_path = %lock_path.display(),
                    error = %std::io::Error::from(error),
                    "failed to acquire br repo-operation lock; assuming workspace is dirty"
                );
                return true;
            }
        };

        let snapshot = match Self::read_pending_mutation_snapshot(adapter) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(
                    lock_path = %lock_path.display(),
                    %error,
                    "failed to inspect br pending mutations; assuming workspace is dirty"
                );
                PendingMutationSnapshot {
                    records: Vec::new(),
                    legacy_marker_present: true,
                }
            }
        };
        snapshot.is_dirty()
    }

    fn read_pending_mutation_snapshot(
        adapter: &BrAdapter<R>,
    ) -> Result<PendingMutationSnapshot, BrError> {
        let legacy_marker_present = Self::legacy_pending_mutation_marker_path_for(adapter)
            .map(|path| path.exists())
            .unwrap_or(false);

        let Some(records_dir) = Self::pending_mutations_dir_for(adapter) else {
            return Ok(PendingMutationSnapshot {
                records: Vec::new(),
                legacy_marker_present,
            });
        };

        let mut records = Vec::new();
        match std::fs::read_dir(&records_dir) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry?;
                    let entry_type = entry.file_type()?;
                    if !entry_type.is_file() {
                        continue;
                    }

                    let path = entry.path();
                    let contents = std::fs::read_to_string(&path)?;
                    match serde_json::from_str::<PendingMutationRecord>(&contents) {
                        Ok(record) => records.push(record),
                        Err(error) => {
                            tracing::warn!(
                                record_path = %path.display(),
                                %error,
                                "failed to parse pending br mutation record; preserving it as unknown dirty state"
                            );
                            let adapter_id = path
                                .file_stem()
                                .and_then(|stem| stem.to_str())
                                .unwrap_or("unknown")
                                .to_owned();
                            records.push(PendingMutationRecord::unknown(
                                adapter_id,
                                "unknown_pending_mutation_record",
                            ));
                        }
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(BrError::Io(error)),
        }

        Ok(PendingMutationSnapshot {
            records,
            legacy_marker_present,
        })
    }

    fn refresh_pending_mutation_state_locked(&self) -> Result<PendingMutationSnapshot, BrError> {
        let snapshot = Self::read_pending_mutation_snapshot(&self.adapter)?;
        self.has_unsync_mutations
            .store(snapshot.is_dirty(), Ordering::Release);
        Ok(snapshot)
    }

    fn persist_pending_mutation_record(
        &self,
        record: &PendingMutationRecord,
    ) -> Result<(), BrError> {
        let Some(record_path) = self.pending_mutation_record_path() else {
            return Ok(());
        };
        let Some(parent) = record_path.parent() else {
            return Ok(());
        };

        std::fs::create_dir_all(parent)?;
        let payload = serde_json::to_vec(record)
            .map_err(|error| BrError::Io(std::io::Error::other(error.to_string())))?;
        std::fs::write(&record_path, payload)?;
        Ok(())
    }

    fn clear_own_pending_mutation_record(&self) -> Result<(), BrError> {
        let Some(record_path) = self.pending_mutation_record_path() else {
            return Ok(());
        };

        match std::fs::remove_file(&record_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(BrError::Io(error)),
        }
    }

    fn clear_all_pending_mutation_state(&self) -> Result<(), BrError> {
        if let Some(legacy_marker_path) = self.legacy_pending_mutation_marker_path() {
            match std::fs::remove_file(&legacy_marker_path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(BrError::Io(error)),
            }
        }

        if let Some(records_dir) = self.pending_mutations_dir() {
            match std::fs::remove_dir_all(&records_dir) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(BrError::Io(error)),
            }
        }

        Ok(())
    }

    /// Refuse operations that require a clean bead workspace while pending
    /// local mutations still need `br sync --flush-only`.
    pub async fn ensure_synced(&self) -> Result<(), BrError> {
        let _operation_guard = self.operation_lock.lock().await;
        let _repo_guard = self.acquire_repo_operation_lock()?;
        self.ensure_synced_locked()
    }

    fn ensure_synced_locked(&self) -> Result<(), BrError> {
        self.working_dir_for_sync_import()?;

        if !self.refresh_pending_mutation_state_locked()?.is_dirty() {
            return Ok(());
        }

        tracing::warn!(
            operation = "ensure_synced",
            "refusing br operation while unsynced local mutations are pending"
        );
        Err(BrError::Io(std::io::Error::other(
            "pending local br mutations detected; run `br sync --flush-only` before importing or reconciling external bead changes",
        )))
    }

    fn working_dir_for_sync_import(&self) -> Result<&Path, BrError> {
        self.adapter.working_dir.as_deref().ok_or_else(|| {
            BrError::Io(std::io::Error::other(
                "br sync --import-only requires a configured working directory so pending mutation state can be recovered across restarts",
            ))
        })
    }

    fn ensure_healthy_beads_for_import(&self) -> Result<(), BrError> {
        let base_dir = self.working_dir_for_sync_import()?;
        let status = check_beads_health_with_availability(base_dir, || {
            self.adapter.check_available().is_ok()
        });
        if let Some(details) = beads_health_failure_details(&status) {
            return Err(BrError::Io(std::io::Error::other(format!(
                "cannot run `br sync --import-only`: {details}"
            ))));
        }

        Ok(())
    }

    fn ensure_healthy_beads_for_pending_sync(
        &self,
        base_dir: &Path,
    ) -> Result<(), SyncIfDirtyHealthError> {
        let status = check_beads_health_with_availability(base_dir, || {
            self.adapter.check_available().is_ok()
        });
        if let Some(details) = beads_health_failure_details(&status) {
            return Err(SyncIfDirtyHealthError::UnsafeBeadsState { details });
        }

        Ok(())
    }

    fn ensure_current_adapter_owns_pending_snapshot(
        &self,
        snapshot: &PendingMutationSnapshot,
        operation: &str,
    ) -> Result<(), SyncIfDirtyHealthError> {
        if snapshot.legacy_marker_present {
            return Err(SyncIfDirtyHealthError::UnsafeBeadsState {
                details: format!(
                    "legacy pending br mutation marker `{LEGACY_PENDING_MUTATIONS_MARKER}` is present; refusing to run `{operation}` because ownership of the unsynced mutation cannot be proven. Run an explicit repo-wide `br sync --flush-only` after confirming bead state, or remove the legacy marker once the previously published mutations are verified"
                ),
            });
        }

        if !snapshot.has_foreign_records_for(&self.adapter_id) {
            return Ok(());
        }

        let details = match snapshot.first_foreign_record_for(&self.adapter_id) {
            Some(record) => {
                let bead_detail = record
                    .bead_id
                    .as_deref()
                    .map(|bead_id| format!(" on bead '{bead_id}'"))
                    .unwrap_or_default();
                format!(
                    "another local bead workflow still has pending `{}`{} owned by adapter `{}`; \
                     refusing to run `{operation}` because it could publish someone else's half-finished \
                     bead changes. Let that workflow finish its own `br sync --flush-only` first",
                    record.operation, bead_detail, record.adapter_id
                )
            }
            None => format!(
                "another local bead workflow still has pending mutations; refusing to run `{operation}` \
                 because it could publish someone else's half-finished bead changes. Let that workflow \
                 finish its own `br sync --flush-only` first"
            ),
        };

        Err(SyncIfDirtyHealthError::UnsafeBeadsState { details })
    }

    fn recovered_status_updates(snapshot: &PendingMutationSnapshot) -> Vec<RecoveredStatusUpdate> {
        snapshot
            .records
            .iter()
            .filter_map(|record| {
                record
                    .bead_id
                    .as_ref()
                    .zip(record.status.as_ref())
                    .filter(|_| record.operation == "update_bead_status")
                    .map(|(bead_id, status)| RecoveredStatusUpdate {
                        adapter_id: record.adapter_id.clone(),
                        bead_id: bead_id.clone(),
                        status: status.clone(),
                    })
            })
            .collect()
    }

    fn flushed_sync_if_dirty_outcome(
        snapshot: &PendingMutationSnapshot,
        output: BrOutput,
    ) -> SyncIfDirtyOutcome {
        SyncIfDirtyOutcome::Flushed {
            output,
            flushed_mutations: snapshot.records.len() + usize::from(snapshot.legacy_marker_present),
            recovered_status_updates: Self::recovered_status_updates(snapshot),
        }
    }

    // ── mutation helpers ────────────────────────────────────────────────

    /// Execute a mutation command with dirty-flag bookkeeping and audit logging.
    async fn exec_tracked_mutation(
        &self,
        operation: &str,
        bead_id: Option<&str>,
        status: Option<&str>,
        cmd: &BrCommand,
    ) -> Result<BrOutput, BrError> {
        let _operation_guard = self.operation_lock.lock().await;
        let _repo_guard = self.acquire_repo_operation_lock()?;
        self.exec_tracked_mutation_locked(operation, bead_id, status, cmd)
            .await
    }

    async fn exec_tracked_mutation_locked(
        &self,
        operation: &str,
        bead_id: Option<&str>,
        status: Option<&str>,
        cmd: &BrCommand,
    ) -> Result<BrOutput, BrError> {
        let own_record_existed = self
            .pending_mutation_record_path()
            .as_ref()
            .is_some_and(|path| path.exists());
        let pending_record =
            PendingMutationRecord::new(self.adapter_id.clone(), operation, bead_id, status);
        self.persist_pending_mutation_record(&pending_record)?;

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
                if !own_record_existed {
                    if let Err(error) = self.clear_own_pending_mutation_record() {
                        tracing::warn!(
                            operation = operation,
                            bead_id = bead_id.unwrap_or(""),
                            %error,
                            "failed to clear pending mutation record after failed br mutation"
                        );
                    }
                }
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
        self.exec_tracked_mutation("create_bead", None, None, &cmd)
            .await
    }

    /// Update the status of an existing bead.
    pub async fn update_bead_status(&self, id: &str, status: &str) -> Result<BrOutput, BrError> {
        let cmd = BrCommand::update_status(id, status);
        self.exec_tracked_mutation("update_bead_status", Some(id), Some(status), &cmd)
            .await
    }

    /// Close a bead with a reason.
    pub async fn close_bead(&self, id: &str, reason: &str) -> Result<BrOutput, BrError> {
        let cmd = BrCommand::close(id, reason);
        self.exec_tracked_mutation("close_bead", Some(id), None, &cmd)
            .await
    }

    /// Add a dependency: `from_id` depends on `depends_on_id`.
    pub async fn add_dependency(
        &self,
        from_id: &str,
        depends_on_id: &str,
    ) -> Result<BrOutput, BrError> {
        let cmd = BrCommand::dep_add(from_id, depends_on_id);
        self.exec_tracked_mutation("add_dependency", Some(from_id), None, &cmd)
            .await
    }

    /// Remove a dependency between two beads.
    pub async fn remove_dependency(
        &self,
        from_id: &str,
        depends_on_id: &str,
    ) -> Result<BrOutput, BrError> {
        let cmd = BrCommand::dep_remove(from_id, depends_on_id);
        self.exec_tracked_mutation("remove_dependency", Some(from_id), None, &cmd)
            .await
    }

    /// Add a comment to a bead.
    pub async fn comment_bead(&self, id: &str, text: &str) -> Result<BrOutput, BrError> {
        let cmd = BrCommand::comment(id, text);
        self.exec_tracked_mutation("comment_bead", Some(id), None, &cmd)
            .await
    }

    /// Flush pending local bead mutations to upstream storage.
    ///
    /// Call this after one or more successful mutation commands once the local
    /// mutation set is ready to be published. A successful flush clears the
    /// adapter's dirty flag and removes the persisted pending-mutation marker.
    /// On failure both remain in place so crash recovery or a later retry can
    /// safely re-run `br sync --flush-only` without losing the local mutation
    /// intent.
    pub async fn sync_flush(&self) -> Result<BrOutput, BrError> {
        let _operation_guard = self.operation_lock.lock().await;
        let _repo_guard = self.acquire_repo_operation_lock()?;
        self.sync_flush_locked().await
    }

    async fn sync_flush_locked(&self) -> Result<BrOutput, BrError> {
        let cmd = BrCommand::sync_flush();
        let start = Instant::now();
        let result = self.adapter.exec_mutation(&cmd).await;
        let duration_ms = start.elapsed().as_millis() as u64;

        match &result {
            Ok(_) => {
                self.clear_all_pending_mutation_state()?;
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

    /// Flush pending mutations only when the adapter is currently dirty.
    ///
    /// Returns `SyncIfDirtyOutcome::Clean` when no pending mutation journal is
    /// present, avoiding an unnecessary subprocess. When a flush does happen,
    /// the outcome also reports which persisted status updates were replayed,
    /// including the adapter ownership token for each record, so callers can
    /// distinguish their own recovered claim from unrelated work.
    pub async fn sync_if_dirty(&self) -> Result<SyncIfDirtyOutcome, BrError> {
        let _operation_guard = self.operation_lock.lock().await;
        let _repo_guard = self.acquire_repo_operation_lock()?;
        if self.adapter.working_dir.is_none() {
            if !self.has_pending_mutations() {
                tracing::debug!(
                    operation = "sync_if_dirty",
                    outcome = "skipped",
                    "skipping br sync because no local mutations are pending"
                );
                return Ok(SyncIfDirtyOutcome::Clean);
            }

            return self
                .sync_flush_locked()
                .await
                .map(|output| SyncIfDirtyOutcome::Flushed {
                    output,
                    flushed_mutations: 1,
                    recovered_status_updates: Vec::new(),
                });
        }

        let snapshot = self.refresh_pending_mutation_state_locked()?;
        if !snapshot.is_dirty() {
            tracing::debug!(
                operation = "sync_if_dirty",
                outcome = "skipped",
                "skipping br sync because no local mutations are pending"
            );
            return Ok(SyncIfDirtyOutcome::Clean);
        }

        self.sync_flush_locked()
            .await
            .map(|output| Self::flushed_sync_if_dirty_outcome(&snapshot, output))
    }

    /// Flush pending mutations only when the repo is dirty and `.beads/` is safe.
    ///
    /// Unlike `sync_if_dirty()`, this helper re-checks the shared pending-mutation
    /// journal under the repo lock and blocks the flush when `.beads/issues.jsonl`
    /// is missing, conflicted, malformed, unreadable, or the `br` binary is
    /// unavailable. Callers that need replay-safe idempotency can use the
    /// returned `SyncIfDirtyOutcome` to distinguish a clean no-op from a
    /// recovered flush, without relying on the adapter's cached dirty flag.
    pub async fn sync_if_dirty_when_beads_healthy(
        &self,
        base_dir: &Path,
    ) -> Result<SyncIfDirtyOutcome, SyncIfDirtyHealthError> {
        let _operation_guard = self.operation_lock.lock().await;
        let _repo_guard = self.acquire_repo_operation_lock()?;
        if self.adapter.working_dir.is_none() {
            if !self.has_pending_mutations() {
                tracing::debug!(
                    operation = "sync_if_dirty_when_beads_healthy",
                    outcome = "skipped",
                    "skipping guarded br sync because no local mutations are pending"
                );
                return Ok(SyncIfDirtyOutcome::Clean);
            }

            self.ensure_healthy_beads_for_pending_sync(base_dir)?;
            return self
                .sync_flush_locked()
                .await
                .map(|output| SyncIfDirtyOutcome::Flushed {
                    output,
                    flushed_mutations: 1,
                    recovered_status_updates: Vec::new(),
                })
                .map_err(Into::into);
        }

        let snapshot = self.refresh_pending_mutation_state_locked()?;
        if !snapshot.is_dirty() {
            tracing::debug!(
                operation = "sync_if_dirty_when_beads_healthy",
                outcome = "skipped",
                "skipping guarded br sync because no local mutations are pending"
            );
            return Ok(SyncIfDirtyOutcome::Clean);
        }

        self.ensure_healthy_beads_for_pending_sync(base_dir)?;
        self.sync_flush_locked()
            .await
            .map(|output| Self::flushed_sync_if_dirty_outcome(&snapshot, output))
            .map_err(Into::into)
    }

    /// Flush pending mutations only when the repo is dirty, `.beads/` is safe,
    /// and every recovered journal record belongs to this adapter.
    ///
    /// This is the safe replay path for workflows like bead claims or success
    /// reconciliation that must not accidentally publish another workflow's
    /// half-finished multi-step mutation sequence from the same repo.
    pub async fn sync_own_dirty_if_beads_healthy(
        &self,
        base_dir: &Path,
    ) -> Result<SyncIfDirtyOutcome, SyncIfDirtyHealthError> {
        let _operation_guard = self.operation_lock.lock().await;
        let _repo_guard = self.acquire_repo_operation_lock()?;
        if self.adapter.working_dir.is_none() {
            if !self.has_pending_mutations() {
                tracing::debug!(
                    operation = "sync_own_dirty_if_beads_healthy",
                    outcome = "skipped",
                    "skipping guarded br sync because no local mutations are pending"
                );
                return Ok(SyncIfDirtyOutcome::Clean);
            }

            self.ensure_healthy_beads_for_pending_sync(base_dir)?;
            return self
                .sync_flush_locked()
                .await
                .map(|output| SyncIfDirtyOutcome::Flushed {
                    output,
                    flushed_mutations: 1,
                    recovered_status_updates: Vec::new(),
                })
                .map_err(Into::into);
        }

        let snapshot = self.refresh_pending_mutation_state_locked()?;
        if !snapshot.is_dirty() {
            tracing::debug!(
                operation = "sync_own_dirty_if_beads_healthy",
                outcome = "skipped",
                "skipping guarded br sync because no local mutations are pending"
            );
            return Ok(SyncIfDirtyOutcome::Clean);
        }

        self.ensure_healthy_beads_for_pending_sync(base_dir)?;
        self.ensure_current_adapter_owns_pending_snapshot(&snapshot, "br sync --flush-only")?;
        self.sync_flush_locked()
            .await
            .map(|output| Self::flushed_sync_if_dirty_outcome(&snapshot, output))
            .map_err(Into::into)
    }

    /// Import upstream JSONL changes into the local workspace without
    /// publishing any local mutations.
    ///
    /// Use this after `git pull`, merge, or other external `.beads/` updates.
    /// The method refuses to run while local unsynced mutations are pending,
    /// including mutations recovered from `.beads/.br-unsynced-mutations`
    /// after a restart, so imported state cannot be mixed with a dirty local
    /// mutation set. It also reuses the beads export health check so missing,
    /// unreadable, or conflict-marked `.beads/issues.jsonl` state is rejected
    /// before the import subprocess can run.
    pub async fn sync_import(&self) -> Result<BrOutput, BrError> {
        let _operation_guard = self.operation_lock.lock().await;
        let _repo_guard = self.acquire_repo_operation_lock()?;
        self.ensure_synced_locked()?;
        self.ensure_healthy_beads_for_import()?;

        let cmd = BrCommand::sync_import();
        let start = Instant::now();
        let result = self.adapter.exec_mutation(&cmd).await;
        let duration_ms = start.elapsed().as_millis() as u64;

        match &result {
            Ok(_) => {
                tracing::info!(
                    operation = "sync_import",
                    outcome = "success",
                    duration_ms = duration_ms,
                    "br sync import completed"
                );
            }
            Err(err) => {
                let stderr_content = match err {
                    BrError::BrExitError { stderr, .. } => stderr.as_str(),
                    _ => "",
                };
                tracing::warn!(
                    operation = "sync_import",
                    outcome = "failure",
                    duration_ms = duration_ms,
                    stderr = stderr_content,
                    "br sync import failed"
                );
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command as StdCommand;
    use std::sync::Arc;
    use tokio::sync::Notify;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

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
    fn command_builder_sync_import() -> Result<(), Box<dyn std::error::Error>> {
        let cmd = BrCommand::sync_import();
        let args = cmd.build_args();
        assert_eq!(args, vec!["sync", "--import-only"]);
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
        commands: std::sync::Arc<std::sync::Mutex<Vec<Vec<String>>>>,
    }

    impl MockRunner {
        fn new(responses: Vec<Result<BrOutput, BrError>>) -> Self {
            Self {
                responses: std::sync::Mutex::new(responses),
                commands: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
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

        fn command_log(&self) -> std::sync::Arc<std::sync::Mutex<Vec<Vec<String>>>> {
            std::sync::Arc::clone(&self.commands)
        }
    }

    impl ProcessRunner for MockRunner {
        async fn run(
            &self,
            args: Vec<String>,
            _timeout: Duration,
            _working_dir: Option<&std::path::Path>,
        ) -> Result<BrOutput, BrError> {
            self.commands
                .lock()
                .expect("mock command log poisoned")
                .push(args);
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

    #[cfg(unix)]
    #[test]
    fn check_available_rejects_non_zero_version_exit() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let fake_br = tmp.path().join("fake-br");
        std::fs::write(
            &fake_br,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  echo broken >&2\n  exit 7\nfi\nexit 0\n",
        )?;
        let mut permissions = std::fs::metadata(&fake_br)?.permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_br, permissions)?;

        let adapter = BrAdapter {
            runner: OsProcessRunner::with_binary(fake_br),
            read_timeout: DEFAULT_READ_TIMEOUT,
            mutation_timeout: DEFAULT_MUTATION_TIMEOUT,
            working_dir: None,
        };

        let error = adapter
            .check_available()
            .expect_err("non-zero --version exit must be unavailable");
        assert!(matches!(error, BrError::BrExitError { exit_code: 7, .. }));
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn check_available_times_out_when_version_probe_hangs() -> Result<(), Box<dyn std::error::Error>>
    {
        let tmp = tempfile::tempdir()?;
        let fake_br = tmp.path().join("fake-br");
        std::fs::write(
            &fake_br,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  sleep 5\n  exit 0\nfi\nexit 0\n",
        )?;
        let mut permissions = std::fs::metadata(&fake_br)?.permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_br, permissions)?;

        let adapter = BrAdapter {
            runner: OsProcessRunner::with_binary(fake_br),
            read_timeout: DEFAULT_READ_TIMEOUT,
            mutation_timeout: DEFAULT_MUTATION_TIMEOUT,
            working_dir: None,
        };

        let error = adapter
            .check_available_with_timeout(Duration::from_millis(50))
            .expect_err("hung --version probe must time out");
        assert!(matches!(error, BrError::BrTimeout { .. }));
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

    fn make_mutation_adapter_in(
        base_dir: &std::path::Path,
        responses: Vec<Result<BrOutput, BrError>>,
    ) -> (
        BrMutationAdapter<MockRunner>,
        std::sync::Arc<std::sync::Mutex<Vec<Vec<String>>>>,
    ) {
        let runner = MockRunner::new(responses);
        let command_log = runner.command_log();
        let adapter = BrAdapter::with_runner(runner).with_working_dir(base_dir.to_path_buf());
        (BrMutationAdapter::with_adapter(adapter), command_log)
    }

    fn write_issues_file(
        base_dir: &std::path::Path,
        contents: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let beads_dir = base_dir.join(".beads");
        std::fs::create_dir_all(&beads_dir)?;
        std::fs::write(beads_dir.join("issues.jsonl"), contents)?;
        Ok(())
    }

    fn write_pending_mutation_record(
        base_dir: &std::path::Path,
        adapter_id: &str,
        operation: &str,
        bead_id: Option<&str>,
        status: Option<&str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let records_dir = base_dir.join(PENDING_MUTATIONS_DIR);
        std::fs::create_dir_all(&records_dir)?;
        let record = PendingMutationRecord::new(adapter_id.to_owned(), operation, bead_id, status);
        std::fs::write(
            records_dir.join(format!("{adapter_id}.json")),
            serde_json::to_vec(&record)?,
        )?;
        Ok(())
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

    #[tokio::test]
    async fn sync_if_dirty_skips_when_clean() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockRunner::new(Vec::new());
        let command_log = runner.command_log();
        let adapter = BrAdapter::with_runner(runner);
        let ma = BrMutationAdapter::with_adapter(adapter);

        let result = ma.sync_if_dirty().await?;

        assert!(result.is_clean());
        assert!(
            command_log.lock().expect("command log").is_empty(),
            "clean sync_if_dirty should not invoke br"
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_if_dirty_flushes_when_dirty() -> Result<(), Box<dyn std::error::Error>> {
        let runner = MockRunner::new(vec![
            MockRunner::success("updated"),
            MockRunner::success("synced"),
        ]);
        let command_log = runner.command_log();
        let adapter = BrAdapter::with_runner(runner);
        let ma = BrMutationAdapter::with_adapter(adapter);

        ma.update_bead_status("bead-1", "in_progress").await?;
        let result = ma.sync_if_dirty().await?;

        assert!(matches!(result, SyncIfDirtyOutcome::Flushed { .. }));
        assert!(!ma.has_pending_mutations());
        let commands = command_log.lock().expect("command log");
        assert!(commands.contains(&vec![
            "update".to_owned(),
            "bead-1".to_owned(),
            "--status=in_progress".to_owned(),
        ]));
        assert!(commands.contains(&vec!["sync".to_owned(), "--flush-only".to_owned()]));
        Ok(())
    }

    #[tokio::test]
    async fn sync_if_dirty_rechecks_repo_pending_state_after_adapter_construction(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let (clean_adapter, clean_log) =
            make_mutation_adapter_in(tmp.path(), vec![MockRunner::success("synced")]);
        assert!(
            !clean_adapter.has_pending_mutations(),
            "fresh adapter should start clean before another writer mutates"
        );

        let (dirty_adapter, _) =
            make_mutation_adapter_in(tmp.path(), vec![MockRunner::success("updated")]);
        dirty_adapter
            .update_bead_status("bead-1", "in_progress")
            .await?;

        let result = clean_adapter.sync_if_dirty().await?;

        assert!(matches!(result, SyncIfDirtyOutcome::Flushed { .. }));
        assert!(result.includes_update_status("bead-1", "in_progress"));
        assert_eq!(
            *clean_log.lock().expect("command log"),
            vec![vec!["sync".to_owned(), "--flush-only".to_owned()]],
            "clean adapter should re-read repo state and flush recovered work"
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_if_dirty_tracks_recovered_update_status_by_adapter_id(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let claim_adapter_id = "project-claim-owner";
        let (claim_adapter, _) = {
            let runner = MockRunner::new(vec![MockRunner::success("updated")]);
            let adapter = BrAdapter::with_runner(runner).with_working_dir(tmp.path().to_path_buf());
            (
                BrMutationAdapter::with_adapter_id(adapter, claim_adapter_id),
                (),
            )
        };
        claim_adapter
            .update_bead_status("bead-1", "in_progress")
            .await?;

        let observer_runner = MockRunner::new(vec![MockRunner::success("synced")]);
        let observer_log = observer_runner.command_log();
        let observer_adapter =
            BrAdapter::with_runner(observer_runner).with_working_dir(tmp.path().to_path_buf());
        let observer = BrMutationAdapter::with_adapter(observer_adapter);

        let result = observer.sync_if_dirty().await?;

        assert!(matches!(result, SyncIfDirtyOutcome::Flushed { .. }));
        assert!(result.includes_update_status("bead-1", "in_progress"));
        assert!(result.includes_owned_update_status(claim_adapter_id, "bead-1", "in_progress"));
        assert!(!result.includes_owned_update_status("different-owner", "bead-1", "in_progress"));
        assert_eq!(
            *observer_log.lock().expect("command log"),
            vec![vec!["sync".to_owned(), "--flush-only".to_owned()]],
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_own_dirty_if_beads_healthy_rejects_foreign_pending_records_without_flushing(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues_file(tmp.path(), "{\"id\":\"bead-1\"}\n")?;
        let foreign_record = tmp.path().join(PENDING_MUTATIONS_DIR).join("foreign.json");
        std::fs::create_dir_all(
            foreign_record
                .parent()
                .expect("pending mutation journal must have a parent dir"),
        )?;
        std::fs::write(
            &foreign_record,
            r#"{"adapter_id":"other-workflow","operation":"create_bead","bead_id":"bead-2","status":null}"#,
        )?;

        let (adapter, command_log) =
            make_mutation_adapter_in(tmp.path(), vec![MockRunner::success("synced")]);

        let error = adapter
            .sync_own_dirty_if_beads_healthy(tmp.path())
            .await
            .expect_err("foreign pending records must block an owned-only replay sync");

        match error {
            SyncIfDirtyHealthError::UnsafeBeadsState { details } => {
                assert!(
                    details.contains("another local bead workflow still has pending `create_bead`"),
                    "details should explain the foreign pending mutation: {details}"
                );
            }
            other => panic!("expected unsafe-beads-state error, got {other:?}"),
        }
        assert!(
            command_log.lock().expect("command log").is_empty(),
            "owned-only replay sync must not invoke br when a foreign journal record is present"
        );
        assert!(
            foreign_record.exists(),
            "blocking the foreign replay must leave the journal record in place"
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_own_dirty_if_beads_healthy_rejects_legacy_pending_marker_without_flushing(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues_file(tmp.path(), "{\"id\":\"bead-1\"}\n")?;
        let legacy_marker = tmp.path().join(LEGACY_PENDING_MUTATIONS_MARKER);
        std::fs::create_dir_all(
            legacy_marker
                .parent()
                .expect("legacy marker path must have a parent dir"),
        )?;
        std::fs::write(&legacy_marker, "pending\n")?;

        let (adapter, command_log) =
            make_mutation_adapter_in(tmp.path(), vec![MockRunner::success("synced")]);

        let error = adapter
            .sync_own_dirty_if_beads_healthy(tmp.path())
            .await
            .expect_err("legacy marker ownership is unknown and must block owned-only replay");

        match error {
            SyncIfDirtyHealthError::UnsafeBeadsState { details } => {
                assert!(
                    details.contains("legacy pending br mutation marker"),
                    "details should explain the legacy-marker ownership failure: {details}"
                );
            }
            other => panic!("expected unsafe-beads-state error, got {other:?}"),
        }
        assert!(
            command_log.lock().expect("command log").is_empty(),
            "owned-only replay sync must not invoke br when only the legacy marker is present"
        );
        assert!(
            legacy_marker.exists(),
            "blocking the legacy replay must leave the marker in place"
        );
        Ok(())
    }

    struct BlockingMutationRunner {
        command_log: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
        mutation_started: Arc<Notify>,
        allow_mutation_finish: Arc<Notify>,
    }

    impl ProcessRunner for BlockingMutationRunner {
        async fn run(
            &self,
            args: Vec<String>,
            _timeout: Duration,
            _working_dir: Option<&std::path::Path>,
        ) -> Result<BrOutput, BrError> {
            self.command_log
                .lock()
                .expect("command log")
                .push(args.clone());

            if args.first().map(String::as_str) == Some("update") {
                self.mutation_started.notify_waiters();
                self.allow_mutation_finish.notified().await;
                return MockRunner::success("updated");
            }

            if args == vec!["sync".to_owned(), "--flush-only".to_owned()] {
                return MockRunner::success("synced");
            }

            panic!("unexpected command: {args:?}");
        }
    }

    #[tokio::test]
    async fn mutation_and_flush_are_serialized_around_pending_marker(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues_file(tmp.path(), "{\"id\":\"bead-1\"}\n")?;
        let command_log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mutation_started = Arc::new(Notify::new());
        let allow_mutation_finish = Arc::new(Notify::new());
        let adapter = BrAdapter::with_runner(BlockingMutationRunner {
            command_log: Arc::clone(&command_log),
            mutation_started: Arc::clone(&mutation_started),
            allow_mutation_finish: Arc::clone(&allow_mutation_finish),
        })
        .with_working_dir(tmp.path().to_path_buf());
        let mutation_adapter = Arc::new(BrMutationAdapter::with_adapter(adapter));

        let update_adapter = Arc::clone(&mutation_adapter);
        let update_task = tokio::spawn(async move {
            update_adapter
                .update_bead_status("bead-1", "in_progress")
                .await
        });

        mutation_started.notified().await;

        let flush_adapter = Arc::clone(&mutation_adapter);
        let flush_task = tokio::spawn(async move { flush_adapter.sync_flush().await });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            command_log.lock().expect("command log").len(),
            1,
            "sync_flush must wait until the in-flight mutation finishes"
        );

        allow_mutation_finish.notify_waiters();
        update_task.await??;
        flush_task.await??;

        assert!(
            !mutation_adapter.has_pending_mutations(),
            "flush should leave the adapter clean after the serialized mutation"
        );
        assert!(
            !tmp.path().join(PENDING_MUTATIONS_DIR).exists(),
            "flush should remove the pending mutation journal once the serialized mutation is synced"
        );
        assert_eq!(
            *command_log.lock().expect("command log"),
            vec![
                vec![
                    "update".to_owned(),
                    "bead-1".to_owned(),
                    "--status=in_progress".to_owned(),
                ],
                vec!["sync".to_owned(), "--flush-only".to_owned()],
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_import_rejects_dirty_adapter_without_invoking_br(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let (ma, command_log) = make_mutation_adapter_in(
            tmp.path(),
            vec![
                MockRunner::success("updated"),
                MockRunner::success("imported"),
            ],
        );

        ma.update_bead_status("bead-1", "in_progress").await?;
        let error = ma.sync_import().await.expect_err("dirty import must fail");

        assert!(error
            .to_string()
            .contains("pending local br mutations detected"));
        let commands = command_log.lock().expect("command log");
        assert_eq!(
            commands.len(),
            1,
            "sync_import should not spawn br when dirty"
        );
        assert_eq!(
            commands[0],
            vec![
                "update".to_owned(),
                "bead-1".to_owned(),
                "--status=in_progress".to_owned(),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_import_rechecks_repo_pending_state_after_adapter_construction(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues_file(tmp.path(), "{\"id\":\"bead-1\"}\n")?;

        let (clean_adapter, clean_log) =
            make_mutation_adapter_in(tmp.path(), vec![MockRunner::success("imported")]);
        assert!(
            !clean_adapter.has_pending_mutations(),
            "adapter should start clean before another writer mutates"
        );

        let (dirty_adapter, _) =
            make_mutation_adapter_in(tmp.path(), vec![MockRunner::success("updated")]);
        dirty_adapter
            .update_bead_status("bead-1", "in_progress")
            .await?;

        let error = clean_adapter
            .sync_import()
            .await
            .expect_err("import must re-read repo state and block on later mutations");
        assert!(error
            .to_string()
            .contains("pending local br mutations detected"));
        assert!(
            clean_log.lock().expect("command log").is_empty(),
            "sync_import should not invoke br when repo became dirty after adapter construction"
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_import_rejects_missing_beads_export_without_invoking_br(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let (ma, command_log) =
            make_mutation_adapter_in(tmp.path(), vec![MockRunner::success("imported")]);

        let error = ma
            .sync_import()
            .await
            .expect_err("missing issues.jsonl must block import");

        assert!(error.to_string().contains("missing .beads/issues.jsonl"));
        assert!(
            command_log.lock().expect("command log").is_empty(),
            "health-gated sync_import should not invoke br when beads export is missing"
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_import_rejects_conflicted_beads_export_without_invoking_br(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues_file(
            tmp.path(),
            "<<<<<<< HEAD\n{\"id\":\"bead-1\"}\n=======\n{\"id\":\"bead-2\"}\n>>>>>>> branch\n",
        )?;
        let (ma, command_log) =
            make_mutation_adapter_in(tmp.path(), vec![MockRunner::success("imported")]);

        let error = ma
            .sync_import()
            .await
            .expect_err("conflicted issues.jsonl must block import");

        assert!(error.to_string().contains("detected git conflict markers"));
        assert!(
            command_log.lock().expect("command log").is_empty(),
            "health-gated sync_import should not invoke br when beads export is conflicted"
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_import_rejects_malformed_beads_export_without_invoking_br(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues_file(tmp.path(), "{\"id\":\"bead-1\"}\n{\"id\": }\n")?;
        let (ma, command_log) =
            make_mutation_adapter_in(tmp.path(), vec![MockRunner::success("imported")]);

        let error = ma
            .sync_import()
            .await
            .expect_err("malformed issues.jsonl must block import");

        assert!(error
            .to_string()
            .contains("malformed .beads/issues.jsonl line 2"));
        assert!(
            command_log.lock().expect("command log").is_empty(),
            "health-gated sync_import should not invoke br when beads export is malformed"
        );
        Ok(())
    }

    #[tokio::test]
    async fn sync_import_runs_when_beads_export_is_healthy(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues_file(tmp.path(), "{\"id\":\"bead-1\"}\n")?;
        let (ma, command_log) =
            make_mutation_adapter_in(tmp.path(), vec![MockRunner::success("imported")]);

        let result = ma.sync_import().await?;

        assert_eq!(result.stdout, "imported");
        assert_eq!(
            *command_log.lock().expect("command log"),
            vec![vec!["sync".to_owned(), "--import-only".to_owned()]]
        );
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sync_import_uses_configured_br_binary_for_health_check(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues_file(tmp.path(), "{\"id\":\"bead-1\"}\n")?;
        let fake_br = tmp.path().join("custom-br");
        std::fs::write(
            &fake_br,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  exit 0\nfi\nif [ \"$1\" = \"sync\" ] && [ \"$2\" = \"--import-only\" ]; then\n  echo imported-via-custom-binary\n  exit 0\nfi\necho unexpected \"$@\" >&2\nexit 99\n",
        )?;
        let mut permissions = std::fs::metadata(&fake_br)?.permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_br, permissions)?;

        let adapter = BrAdapter {
            runner: OsProcessRunner::with_binary(fake_br),
            read_timeout: DEFAULT_READ_TIMEOUT,
            mutation_timeout: DEFAULT_MUTATION_TIMEOUT,
            working_dir: Some(tmp.path().to_path_buf()),
        };
        let mutation = BrMutationAdapter::with_adapter(adapter);

        let output = mutation.sync_import().await?;

        assert_eq!(output.stdout.trim(), "imported-via-custom-binary");
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sync_if_dirty_when_beads_healthy_uses_configured_br_binary_for_health_check(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        write_issues_file(tmp.path(), "{\"id\":\"bead-1\"}\n")?;
        let fake_br = tmp.path().join("custom-br");
        std::fs::write(
            &fake_br,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then\n  exit 0\nfi\nif [ \"$1\" = \"sync\" ] && [ \"$2\" = \"--flush-only\" ]; then\n  echo synced-via-custom-binary\n  exit 0\nfi\necho unexpected \"$@\" >&2\nexit 99\n",
        )?;
        let mut permissions = std::fs::metadata(&fake_br)?.permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&fake_br, permissions)?;

        let adapter_id = "configured-binary-owner";
        write_pending_mutation_record(
            tmp.path(),
            adapter_id,
            "update_bead_status",
            Some("bead-1"),
            Some("in_progress"),
        )?;
        let adapter = BrAdapter {
            runner: OsProcessRunner::with_binary(fake_br),
            read_timeout: DEFAULT_READ_TIMEOUT,
            mutation_timeout: DEFAULT_MUTATION_TIMEOUT,
            working_dir: Some(tmp.path().to_path_buf()),
        };
        let mutation = BrMutationAdapter::with_adapter_id(adapter, adapter_id);

        let outcome = mutation
            .sync_if_dirty_when_beads_healthy(tmp.path())
            .await?;

        match outcome {
            SyncIfDirtyOutcome::Flushed {
                output,
                flushed_mutations,
                ..
            } => {
                assert_eq!(output.stdout.trim(), "synced-via-custom-binary");
                assert_eq!(flushed_mutations, 1);
            }
            SyncIfDirtyOutcome::Clean => panic!("expected configured binary to flush dirty state"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn sync_import_recovers_pending_mutation_marker_after_restart(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;

        let (first_adapter, _) =
            make_mutation_adapter_in(tmp.path(), vec![MockRunner::success("updated")]);
        first_adapter
            .update_bead_status("bead-1", "in_progress")
            .await?;

        let marker_path = first_adapter
            .pending_mutation_record_path()
            .expect("record path should exist for working-dir adapters");
        assert!(
            marker_path.is_file(),
            "pending mutation record should persist to disk"
        );

        let (restarted_adapter, command_log) =
            make_mutation_adapter_in(tmp.path(), vec![MockRunner::success("imported")]);
        assert!(
            restarted_adapter.has_pending_mutations(),
            "fresh adapter should recover pending state from marker"
        );

        let error = restarted_adapter
            .sync_import()
            .await
            .expect_err("restarted import must fail while marker is present");
        assert!(error
            .to_string()
            .contains("pending local br mutations detected"));
        assert!(
            command_log.lock().expect("command log").is_empty(),
            "guarded sync_import should not invoke br after restart"
        );
        Ok(())
    }

    #[test]
    fn pending_mutation_artifacts_are_ignored_by_git_add_beads(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        std::fs::write(
            tmp.path().join(".gitignore"),
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/.gitignore"))?,
        )?;

        let marker_path = tmp.path().join(LEGACY_PENDING_MUTATIONS_MARKER);
        std::fs::create_dir_all(
            marker_path
                .parent()
                .expect("pending mutation marker must have a parent dir"),
        )?;
        std::fs::write(&marker_path, "pending\n")?;
        let journal_path = tmp.path().join(PENDING_MUTATIONS_DIR).join("adapter.json");
        std::fs::create_dir_all(
            journal_path
                .parent()
                .expect("pending mutation journal must have a parent dir"),
        )?;
        std::fs::write(
            &journal_path,
            serde_json::to_vec(&PendingMutationRecord::new(
                "adapter".to_owned(),
                "update_bead_status",
                Some("bead-1"),
                Some("in_progress"),
            ))?,
        )?;
        let lock_path = tmp.path().join(REPO_OPERATION_LOCK);
        std::fs::write(&lock_path, "")?;

        let init = StdCommand::new("git")
            .args(["init", "-q"])
            .current_dir(tmp.path())
            .status()?;
        assert!(init.success(), "git init should succeed");

        let add = StdCommand::new("git")
            .args(["add", ".beads"])
            .current_dir(tmp.path())
            .status()?;
        assert!(add.success(), "git add .beads should succeed");

        let staged = StdCommand::new("git")
            .args(["diff", "--cached", "--name-only", "--", ".beads"])
            .current_dir(tmp.path())
            .output()?;
        assert!(staged.status.success(), "git diff --cached should succeed");
        assert!(
            String::from_utf8_lossy(&staged.stdout).trim().is_empty(),
            "ignored pending-mutation artifacts must not be staged by git add .beads"
        );
        Ok(())
    }

    struct CleanupFailureRunner {
        record_path: PathBuf,
    }

    impl ProcessRunner for CleanupFailureRunner {
        async fn run(
            &self,
            _args: Vec<String>,
            _timeout: Duration,
            _working_dir: Option<&std::path::Path>,
        ) -> Result<BrOutput, BrError> {
            std::fs::remove_file(&self.record_path)?;
            std::fs::create_dir(&self.record_path)?;
            Err(BrError::BrExitError {
                exit_code: 23,
                stdout: String::new(),
                stderr: "mutation failed".to_owned(),
                command: "br update bead-1 --status=in_progress".to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn failed_mutation_preserves_original_error_when_marker_cleanup_fails(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tmp = tempfile::tempdir()?;
        let record_path = tmp.path().join(PENDING_MUTATIONS_DIR).join("cleanup.json");
        let adapter = BrAdapter::with_runner(CleanupFailureRunner {
            record_path: record_path.clone(),
        })
        .with_working_dir(tmp.path().to_path_buf());
        let mut mutation_adapter = BrMutationAdapter::with_adapter(adapter);
        mutation_adapter.adapter_id = "cleanup".to_owned();

        let error = mutation_adapter
            .update_bead_status("bead-1", "in_progress")
            .await
            .expect_err("mutation should still fail");

        assert!(matches!(error, BrError::BrExitError { exit_code: 23, .. }));
        assert!(
            record_path.is_dir(),
            "cleanup sabotage should leave a directory behind"
        );
        assert!(
            !mutation_adapter.has_pending_mutations(),
            "failed mutation should not mark the adapter dirty"
        );
        Ok(())
    }
}
