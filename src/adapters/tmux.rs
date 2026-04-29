use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
#[cfg(unix)]
use nix::errno::Errno;
#[cfg(unix)]
use nix::sys::signal::Signal;
#[cfg(unix)]
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::adapters::fs::FileSystem;
use crate::adapters::process_backend::{
    classify_exit_failure_with_output, extract_stdout_error,
    stdout_for_exit_failure_classification, truncate_str_tail, ChildOutput, ProcessBackendAdapter,
    STDERR_EXHAUSTION_SCAN_LIMIT,
};
use crate::contexts::agent_execution::model::{
    InvocationContract, InvocationEnvelope, InvocationRequest,
};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::contexts::project_run_record::model::{LogLevel, RuntimeLogEntry};
use crate::shared::domain::{BackendFamily, FailureClass, ResolvedBackendTarget, SessionPolicy};
use crate::shared::error::{AppError, AppResult};

const SESSION_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CANCEL_GRACE_PERIOD: Duration = Duration::from_millis(500);
const SIGNAL_TARGET_WAIT: Duration = Duration::from_millis(250);
const SIGNAL_TARGET_POLL_INTERVAL: Duration = Duration::from_millis(25);
const ACTIVE_SESSION_STATE_FILE: &str = "runtime/active-tmux-session.json";
const OPENROUTER_TMUX_UNSUPPORTED_DETAILS: &str =
    "execution.mode = \"tmux\" is not supported for OpenRouter-backed invocations; OpenRouter does not spawn an attachable local process. Use execution.mode = \"direct\"";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveTmuxSession {
    pub invocation_id: String,
    pub session_name: String,
    pub recorded_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Clone)]
pub struct TmuxAdapter {
    process: ProcessBackendAdapter,
    active_sessions: Arc<Mutex<HashMap<String, Arc<ManagedTmuxSession>>>>,
    stream_output: bool,
    /// Resolved path (or bare name) for the tmux binary used by instance
    /// methods.  When explicit search paths are set, this is an absolute
    /// [`PathBuf`]; otherwise it is the bare name `"tmux"` which relies on
    /// ambient PATH lookup at spawn time.
    tmux_binary: PathBuf,
}

impl TmuxAdapter {
    pub fn new(process: ProcessBackendAdapter, stream_output: bool) -> Result<Self, String> {
        let tmux_binary = Self::resolve_tmux_binary(&process)?;
        Ok(Self {
            process,
            active_sessions: Arc::new(Mutex::new(HashMap::new())),
            stream_output,
            tmux_binary,
        })
    }

    /// Resolve the tmux binary by delegating to
    /// [`ProcessBackendAdapter::resolve_binary`].
    ///
    /// When the adapter has explicit search paths, the resolved path is
    /// absolute; when no explicit paths are set, the bare name `"tmux"` is
    /// returned so the OS performs ambient PATH lookup at spawn time.
    fn resolve_tmux_binary(process: &ProcessBackendAdapter) -> Result<PathBuf, String> {
        process.resolve_binary("tmux").map_err(|e| e.to_string())
    }

    /// Check tmux availability using explicit search paths.
    pub fn check_tmux_available_in(search_paths: &[std::path::PathBuf]) -> AppResult<()> {
        ProcessBackendAdapter::ensure_binary_available("tmux", "tmux", search_paths)
    }

    /// Verify that the cached `tmux_binary` is still available.
    ///
    /// When the adapter was constructed with explicit search paths, the cached
    /// binary is an absolute path — verify it is still an executable file.
    /// When the adapter uses the default bare name (`"tmux"`), fall back to
    /// scanning the system PATH via [`check_tmux_available_in`] so that the
    /// same `ensure_binary_available` diagnostics (not-found vs
    /// found-but-not-executable) are preserved.
    fn verify_tmux_available(&self) -> AppResult<()> {
        if self.process.has_explicit_search_paths() {
            if ProcessBackendAdapter::is_executable_file(&self.tmux_binary) {
                return Ok(());
            }
            return Err(AppError::BackendUnavailable {
                backend: "tmux".to_owned(),
                details: format!(
                    "resolved tmux binary '{}' is no longer available or executable",
                    self.tmux_binary.display()
                ),
                failure_class: None,
            });
        }
        Self::check_tmux_available_in(&self.process.effective_search_paths())
    }

    pub fn session_name(
        project_id: &str,
        invocation_id: &str,
        project_root: &std::path::Path,
    ) -> String {
        // Include a hash of the project root to namespace sessions per
        // workspace, preventing collisions in multi-repo daemon mode.
        let path_hash = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut h = DefaultHasher::new();
            project_root.hash(&mut h);
            format!("{:08x}", h.finish() as u32)
        };
        format!(
            "rb-{}-{}-{}",
            path_hash,
            sanitize_session_segment(project_id),
            sanitize_session_segment(invocation_id)
        )
    }

    pub fn check_tmux_available() -> AppResult<()> {
        ProcessBackendAdapter::ensure_binary_available(
            "tmux",
            "tmux",
            &ProcessBackendAdapter::system_path_entries(),
        )
    }

    fn ensure_supported_backend(
        backend: &ResolvedBackendTarget,
        contract: Option<&InvocationContract>,
    ) -> AppResult<()> {
        if backend.backend.family != BackendFamily::OpenRouter {
            return Ok(());
        }

        match contract {
            Some(contract) => Err(AppError::CapabilityMismatch {
                backend: backend.backend.family.to_string(),
                contract_id: contract.label(),
                details: OPENROUTER_TMUX_UNSUPPORTED_DETAILS.to_owned(),
            }),
            None => Err(AppError::BackendUnavailable {
                backend: backend.backend.family.to_string(),
                details: OPENROUTER_TMUX_UNSUPPORTED_DETAILS.to_owned(),
                failure_class: None,
            }),
        }
    }

    pub fn session_exists(session_name: &str) -> AppResult<bool> {
        if Self::check_tmux_available().is_err() {
            // Return false when tmux is unavailable — the session may still
            // exist but we can't verify. Callers that need to preserve state
            // (like `run attach`) should check tmux availability separately
            // before making destructive decisions.
            return Ok(false);
        }

        let status = StdCommand::new("tmux")
            .args(["has-session", "-t", session_name])
            .status()?;
        Ok(status.success())
    }

    pub fn attach_to_session(session_name: &str) -> AppResult<()> {
        Self::check_tmux_available()?;
        let status = StdCommand::new("tmux")
            .args(["attach-session", "-t", session_name])
            .status()?;
        if status.success() {
            Ok(())
        } else {
            Err(AppError::BackendUnavailable {
                backend: "tmux".to_owned(),
                details: format!("session '{session_name}' is not available for attachment"),
                failure_class: None,
            })
        }
    }

    pub fn read_active_session(project_root: &Path) -> AppResult<Option<ActiveTmuxSession>> {
        let path = Self::active_session_state_path(project_root);
        let contents = match fs::read_to_string(&path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        serde_json::from_str(&contents)
            .map(Some)
            .map_err(|error| AppError::CorruptRecord {
                file: path.display().to_string(),
                details: format!("invalid active tmux session state: {error}"),
            })
    }

    pub fn clear_active_session(project_root: &Path, invocation_id: &str) -> AppResult<()> {
        let Some(current) = Self::read_active_session(project_root)? else {
            return Ok(());
        };

        if current.invocation_id != invocation_id {
            return Ok(());
        }

        match fs::remove_file(Self::active_session_state_path(project_root)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn active_session_state_path(project_root: &Path) -> PathBuf {
        project_root.join(ACTIVE_SESSION_STATE_FILE)
    }

    fn record_active_session(
        project_root: &Path,
        invocation_id: &str,
        session_name: &str,
    ) -> AppResult<()> {
        let state = ActiveTmuxSession {
            invocation_id: invocation_id.to_owned(),
            session_name: session_name.to_owned(),
            recorded_at: Utc::now(),
        };
        let contents = serde_json::to_string_pretty(&state)?;
        FileSystem::write_atomic(&Self::active_session_state_path(project_root), &contents)
    }

    async fn register_session(&self, invocation_id: &str, session: Arc<ManagedTmuxSession>) {
        let mut sessions = self.active_sessions.lock().await;
        sessions.insert(invocation_id.to_owned(), session);
    }

    async fn take_active_session(&self, invocation_id: &str) -> Option<Arc<ManagedTmuxSession>> {
        let mut sessions = self.active_sessions.lock().await;
        sessions.remove(invocation_id)
    }

    async fn remove_session_if_same(&self, invocation_id: &str, session: &Arc<ManagedTmuxSession>) {
        let mut sessions = self.active_sessions.lock().await;
        if sessions
            .get(invocation_id)
            .is_some_and(|current| Arc::ptr_eq(current, session))
        {
            sessions.remove(invocation_id);
        }
    }

    pub(crate) async fn has_session(&self, session_name: &str) -> AppResult<bool> {
        match Command::new(&self.tmux_binary)
            .args(["has-session", "-t", session_name])
            .status()
            .await
        {
            Ok(status) => Ok(status.success()),
            Err(error) => Err(AppError::BackendUnavailable {
                backend: "tmux".to_owned(),
                details: format!("failed to query tmux session '{session_name}': {error}"),
                failure_class: None,
            }),
        }
    }

    async fn kill_session(&self, session_name: &str) -> AppResult<()> {
        let output = Command::new(&self.tmux_binary)
            .args(["kill-session", "-t", session_name])
            .output()
            .await
            .map_err(|error| AppError::InvocationFailed {
                backend: "tmux".to_owned(),
                contract_id: session_name.to_owned(),
                failure_class: FailureClass::TransportFailure,
                details: format!("failed to kill tmux session '{session_name}': {error}"),
            })?;

        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        if is_missing_tmux_session(&stderr) {
            return Ok(());
        }

        Err(AppError::InvocationFailed {
            backend: "tmux".to_owned(),
            contract_id: session_name.to_owned(),
            failure_class: FailureClass::TransportFailure,
            details: if stderr.is_empty() {
                format!(
                    "tmux kill-session failed for '{session_name}' with status {}",
                    output.status
                )
            } else {
                format!("failed to kill tmux session '{session_name}': {stderr}")
            },
        })
    }

    async fn wait_for_session_shutdown(&self, session: &ManagedTmuxSession) -> AppResult<()> {
        loop {
            if read_exit_code(&session.exit_status_path)?.is_some()
                || !self.has_session(&session.session_name).await?
            {
                return Ok(());
            }

            tokio::time::sleep(SESSION_POLL_INTERVAL).await;
        }
    }

    async fn wait_for_signal_target(&self, session: &ManagedTmuxSession) -> AppResult<Option<i32>> {
        let deadline = tokio::time::Instant::now() + SIGNAL_TARGET_WAIT;

        loop {
            if let Some(pid) = session.read_signal_pid()? {
                return Ok(Some(pid));
            }

            if read_exit_code(&session.exit_status_path)?.is_some()
                || !self.has_session(&session.session_name).await?
                || tokio::time::Instant::now() >= deadline
            {
                return Ok(None);
            }

            tokio::time::sleep(SIGNAL_TARGET_POLL_INTERVAL).await;
        }
    }

    async fn wait_for_session_exit(
        &self,
        session: &ManagedTmuxSession,
        stdout_tail: &mut CaptureTail,
        stderr_tail: &mut CaptureTail,
    ) -> AppResult<i32> {
        loop {
            if self.stream_output {
                stdout_tail.flush_new_lines(&session.project_root).await?;
                stderr_tail.flush_new_lines(&session.project_root).await?;
            }

            if let Some(exit_code) = read_exit_code(&session.exit_status_path)? {
                // Flush one more time after seeing the exit marker so the
                // final stdout/stderr chunk is captured before returning.
                if self.stream_output {
                    stdout_tail.flush_new_lines(&session.project_root).await?;
                    stderr_tail.flush_new_lines(&session.project_root).await?;
                }
                return Ok(exit_code);
            }

            if !self.has_session(&session.session_name).await? {
                tokio::time::sleep(SESSION_POLL_INTERVAL).await;
                if let Some(exit_code) = read_exit_code(&session.exit_status_path)? {
                    return Ok(exit_code);
                }
                return Err(AppError::InvocationFailed {
                    backend: "tmux".to_owned(),
                    contract_id: session.session_name.clone(),
                    failure_class: FailureClass::TransportFailure,
                    details: format!(
                        "tmux session '{}' exited before recording a backend exit status",
                        session.session_name
                    ),
                });
            }

            tokio::time::sleep(SESSION_POLL_INTERVAL).await;
        }
    }

    async fn start_tmux_session(&self, session: &ManagedTmuxSession) -> AppResult<()> {
        let command = format!(
            "exec {}",
            shell_escape(&session.wrapper_path.to_string_lossy())
        );
        let output = Command::new(&self.tmux_binary)
            .args([
                "new-session",
                "-d",
                "-s",
                &session.session_name,
                "-x",
                "200",
                "-y",
                "50",
                &command,
            ])
            .output()
            .await?;
        if output.status.success() {
            append_runtime_log(
                &session.project_root,
                "tmux.lifecycle",
                &format!("session created: {}", session.session_name),
            )?;
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        Err(AppError::InvocationFailed {
            backend: "tmux".to_owned(),
            contract_id: session.session_name.clone(),
            failure_class: FailureClass::TransportFailure,
            details: if stderr.is_empty() {
                format!("failed to create tmux session '{}'", session.session_name)
            } else {
                stderr
            },
        })
    }

    async fn finalize_captured_output(
        &self,
        session: &ManagedTmuxSession,
        stdout_tail: &mut CaptureTail,
        stderr_tail: &mut CaptureTail,
    ) -> AppResult<()> {
        if self.stream_output {
            stdout_tail.flush_final_partial(&session.project_root)?;
            stderr_tail.flush_final_partial(&session.project_root)?;
            return Ok(());
        }

        flush_capture_file(
            &session.project_root,
            &session.stdout_path,
            "backend.stdout",
        )?;
        flush_capture_file(
            &session.project_root,
            &session.stderr_path,
            "backend.stderr",
        )?;
        Ok(())
    }
}

impl AgentExecutionPort for TmuxAdapter {
    fn enforces_timeout(&self) -> bool {
        true
    }

    async fn check_capability(
        &self,
        backend: &ResolvedBackendTarget,
        contract: &InvocationContract,
    ) -> AppResult<()> {
        Self::ensure_supported_backend(backend, Some(contract))?;
        self.process.check_capability(backend, contract).await
    }

    async fn check_availability(&self, backend: &ResolvedBackendTarget) -> AppResult<()> {
        Self::ensure_supported_backend(backend, None)?;
        self.verify_tmux_available()?;
        self.process.check_availability(backend).await
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        self.check_capability(&request.resolved_target, &request.contract)
            .await?;
        self.verify_tmux_available()?;

        let prepared = self.process.build_command(&request).await?;
        let project_name = request
            .project_root
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("workspace");
        let session_name =
            Self::session_name(project_name, &request.invocation_id, &request.project_root);
        let session = Arc::new(ManagedTmuxSession::new(
            &request,
            session_name,
            prepared.args(),
            prepared.binary(),
            prepared.stdin_payload(),
        )?);
        let mut stdout_tail = CaptureTail::new(session.stdout_path.clone(), "backend.stdout");
        let mut stderr_tail = CaptureTail::new(session.stderr_path.clone(), "backend.stderr");

        self.register_session(&request.invocation_id, session.clone())
            .await;

        if let Err(error) = self.start_tmux_session(&session).await {
            self.remove_session_if_same(&request.invocation_id, &session)
                .await;
            prepared.cleanup().await;
            session.cleanup();
            return Err(error);
        }

        if let Err(error) = Self::record_active_session(
            &request.project_root,
            &request.invocation_id,
            &session.session_name,
        ) {
            let _ = self.kill_session(&session.session_name).await;
            self.remove_session_if_same(&request.invocation_id, &session)
                .await;
            prepared.cleanup().await;
            session.cleanup();
            return Err(error);
        }

        let exit_code = match tokio::time::timeout(
            request.timeout,
            self.wait_for_session_exit(&session, &mut stdout_tail, &mut stderr_tail),
        )
        .await
        {
            Ok(Ok(exit_code)) => exit_code,
            Ok(Err(error)) => {
                let _ = self.kill_session(&session.session_name).await;
                self.remove_session_if_same(&request.invocation_id, &session)
                    .await;
                let _ = Self::clear_active_session(&request.project_root, &request.invocation_id);
                prepared.cleanup().await;
                session.cleanup();
                return Err(error);
            }
            Err(_elapsed) => {
                // Graceful shutdown: SIGTERM → grace period → SIGKILL, matching
                // the cancel() flow so the child has a chance to flush output.
                let cancel_failed = if let Err(cancel_err) =
                    self.cancel(&request.invocation_id).await
                {
                    // cancel() already removed the session from tracking.
                    // Attempt a direct kill as last resort, then clean up
                    // the session's on-disk state.
                    let _ = append_runtime_log(
                        &request.project_root,
                        "tmux.lifecycle",
                        &format!(
                            "timeout cleanup failed for invocation {}: {cancel_err}; \
                                 attempting direct kill-session fallback",
                            request.invocation_id,
                        ),
                    );
                    let _ = self.kill_session(&session.session_name).await;
                    let _ =
                        Self::clear_active_session(&request.project_root, &request.invocation_id);
                    session.cleanup();
                    true
                } else {
                    false
                };
                prepared
                    .preserve_failed_artifacts(&request, "tmux session timed out")
                    .await;
                let binary_display = prepared.binary().display().to_string();
                // If cancel failed, report as TransportFailure (teardown was
                // not confirmed) so the retry loop knows cleanup was unclean.
                let failure_class = if cancel_failed {
                    FailureClass::TransportFailure
                } else {
                    FailureClass::Timeout
                };
                return Err(AppError::InvocationFailed {
                    backend: request.resolved_target.backend.family.to_string(),
                    contract_id: request.contract.label(),
                    failure_class,
                    details: format!(
                        "{binary_display} exceeded timeout of {}s",
                        request.timeout.as_secs()
                    ),
                });
            }
        };

        if let Err(error) = self
            .finalize_captured_output(&session, &mut stdout_tail, &mut stderr_tail)
            .await
        {
            let _ = self.kill_session(&session.session_name).await;
            self.remove_session_if_same(&request.invocation_id, &session)
                .await;
            let _ = Self::clear_active_session(&request.project_root, &request.invocation_id);
            prepared.cleanup().await;
            session.cleanup();
            return Err(error);
        }

        let output = ChildOutput {
            status: exit_status_from_code(exit_code),
            stdout: tokio::fs::read(&session.stdout_path)
                .await
                .unwrap_or_default(),
            stderr: tokio::fs::read(&session.stderr_path)
                .await
                .unwrap_or_default(),
        };

        self.remove_session_if_same(&request.invocation_id, &session)
            .await;
        let _ = self.kill_session(&session.session_name).await;
        let _ = Self::clear_active_session(&request.project_root, &request.invocation_id);
        session.cleanup();

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);

            // Retry once without session resume if Claude returned a stale
            // session error, matching ProcessBackendAdapter::invoke() behavior.
            if request.resolved_target.backend.family == BackendFamily::Claude
                && stderr.contains("No conversation found with session ID")
                && request.prior_session.is_some()
            {
                let mut retry_request = request.clone();
                retry_request.prior_session = None;
                retry_request.session_policy = SessionPolicy::NewSession;
                retry_request.attempt_number += 1;
                return Box::pin(self.invoke(retry_request)).await;
            }

            prepared.cleanup().await;
            let stdout_error = extract_stdout_error(&output.stdout);
            let stdout_text = String::from_utf8_lossy(&output.stdout);
            let stdout_for_class =
                stdout_for_exit_failure_classification(stdout_error.as_deref(), &stdout_text);
            // Narrow stderr to its tail — codex backends may echo
            // user prompts at the start of stderr.
            let stderr_for_class = truncate_str_tail(&stderr, STDERR_EXHAUSTION_SCAN_LIMIT);
            let failure_class = classify_exit_failure_with_output(
                output.status,
                stderr_for_class,
                stdout_for_class,
            );
            return Err(AppError::InvocationFailed {
                backend: request.resolved_target.backend.family.to_string(),
                contract_id: request.contract.label(),
                failure_class,
                details: format!(
                    "{} exited with code {}{}",
                    prepared.binary().display(),
                    exit_code,
                    if stderr.is_empty() {
                        String::new()
                    } else {
                        format!(": {stderr}")
                    }
                ),
            });
        }

        prepared.finish(&request, output).await
    }

    async fn cancel(&self, invocation_id: &str) -> AppResult<()> {
        let Some(session) = self.take_active_session(invocation_id).await else {
            return Ok(());
        };

        if self.wait_for_signal_target(&session).await?.is_some() {
            session
                .send_sigterm()
                .map_err(|error| AppError::InvocationFailed {
                    backend: "tmux".to_owned(),
                    contract_id: session.session_name.clone(),
                    failure_class: FailureClass::TransportFailure,
                    details: format!(
                        "failed to send SIGTERM to tmux session '{}': {error}",
                        session.session_name
                    ),
                })?;
        }

        let wait_result = tokio::time::timeout(
            CANCEL_GRACE_PERIOD,
            self.wait_for_session_shutdown(&session),
        )
        .await;

        match wait_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                if self.wait_for_signal_target(&session).await?.is_some() {
                    session
                        .send_sigkill()
                        .map_err(|error| AppError::InvocationFailed {
                            backend: "tmux".to_owned(),
                            contract_id: session.session_name.clone(),
                            failure_class: FailureClass::TransportFailure,
                            details: format!(
                                "failed to send SIGKILL to tmux session '{}': {error}",
                                session.session_name
                            ),
                        })?;
                }

                match tokio::time::timeout(
                    CANCEL_GRACE_PERIOD,
                    self.wait_for_session_shutdown(&session),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => return Err(error),
                    Err(_) => {
                        self.kill_session(&session.session_name).await?;
                    }
                }
            }
        }

        append_runtime_log(
            &session.project_root,
            "tmux.lifecycle",
            &format!("session cleaned up: {}", session.session_name),
        )?;
        Self::clear_active_session(&session.project_root, invocation_id)?;
        session.cleanup();
        Ok(())
    }
}

struct ManagedTmuxSession {
    session_name: String,
    project_root: PathBuf,
    wrapper_path: PathBuf,
    stdin_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    exit_status_path: PathBuf,
    signal_pid_path: PathBuf,
}

impl ManagedTmuxSession {
    fn new(
        request: &InvocationRequest,
        session_name: String,
        args: &[String],
        binary: &Path,
        stdin_payload: &str,
    ) -> AppResult<Self> {
        let temp_dir = request.project_root.join("runtime/temp");
        fs::create_dir_all(&temp_dir)?;

        let stdin_path = temp_dir.join(format!("{}.tmux.stdin", request.invocation_id));
        let stdout_path = temp_dir.join(format!("{}.tmux.stdout", request.invocation_id));
        let stderr_path = temp_dir.join(format!("{}.tmux.stderr", request.invocation_id));
        let exit_status_path = temp_dir.join(format!("{}.tmux.exit", request.invocation_id));
        let signal_pid_path = temp_dir.join(format!("{}.tmux.pid", request.invocation_id));
        let wrapper_path = temp_dir.join(format!("{}.tmux.sh", request.invocation_id));

        fs::write(&stdin_path, stdin_payload)?;
        fs::write(
            &wrapper_path,
            build_wrapper_script(
                &request.working_dir,
                binary,
                args,
                &stdin_path,
                &stdout_path,
                &stderr_path,
                &exit_status_path,
                &signal_pid_path,
            )?,
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut permissions = fs::metadata(&wrapper_path)?.permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&wrapper_path, permissions)?;
        }

        Ok(Self {
            session_name,
            project_root: request.project_root.clone(),
            wrapper_path,
            stdin_path,
            stdout_path,
            stderr_path,
            exit_status_path,
            signal_pid_path,
        })
    }

    #[cfg(unix)]
    fn send_sigterm(&self) -> AppResult<()> {
        self.send_signal(Signal::SIGTERM)
    }

    #[cfg(not(unix))]
    fn send_sigterm(&self) -> AppResult<()> {
        Err(AppError::BackendUnavailable {
            backend: "tmux".to_owned(),
            details: "tmux signal delivery requires unix".to_owned(),
            failure_class: None,
        })
    }

    #[cfg(unix)]
    fn send_sigkill(&self) -> AppResult<()> {
        self.send_signal(Signal::SIGKILL)
    }

    #[cfg(not(unix))]
    fn send_sigkill(&self) -> AppResult<()> {
        Err(AppError::BackendUnavailable {
            backend: "tmux".to_owned(),
            details: "tmux signal delivery requires unix".to_owned(),
            failure_class: None,
        })
    }

    #[cfg(unix)]
    fn send_signal(&self, signal: Signal) -> AppResult<()> {
        let Some(pid) = self.read_signal_pid()? else {
            return Ok(());
        };

        match signal_tmux_target(pid, signal) {
            Ok(()) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn read_signal_pid(&self) -> AppResult<Option<i32>> {
        let contents = match fs::read_to_string(&self.signal_pid_path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let trimmed = contents.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }

        let pid = trimmed
            .parse::<i32>()
            .map_err(|error| AppError::CorruptRecord {
                file: self.signal_pid_path.display().to_string(),
                details: format!("invalid tmux signal pid: {error}"),
            })?;
        if pid <= 0 {
            return Err(AppError::CorruptRecord {
                file: self.signal_pid_path.display().to_string(),
                details: format!("invalid tmux signal pid '{pid}': expected a positive pid"),
            });
        }

        Ok(Some(pid))
    }

    fn cleanup(&self) {
        for path in [
            &self.wrapper_path,
            &self.stdin_path,
            &self.stdout_path,
            &self.stderr_path,
            &self.exit_status_path,
            &self.signal_pid_path,
        ] {
            let _ = fs::remove_file(path);
        }
    }
}

struct CaptureTail {
    path: PathBuf,
    source: &'static str,
    offset: usize,
    partial_line: String,
}

impl CaptureTail {
    fn new(path: PathBuf, source: &'static str) -> Self {
        Self {
            path,
            source,
            offset: 0,
            partial_line: String::new(),
        }
    }

    async fn flush_new_lines(&mut self, project_root: &Path) -> AppResult<()> {
        let contents = match tokio::fs::read_to_string(&self.path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        self.consume_contents(project_root, &contents)
    }

    fn flush_final_partial(&mut self, project_root: &Path) -> AppResult<()> {
        if self.partial_line.is_empty() {
            return Ok(());
        }

        append_runtime_log(project_root, self.source, &self.partial_line)?;
        self.partial_line.clear();
        Ok(())
    }

    fn consume_contents(&mut self, project_root: &Path, contents: &str) -> AppResult<()> {
        if contents.len() <= self.offset {
            return Ok(());
        }

        let new_chunk = &contents[self.offset..];
        self.offset = contents.len();
        self.partial_line.push_str(new_chunk);

        while let Some(newline_index) = self.partial_line.find('\n') {
            let line = self.partial_line[..newline_index].trim_end_matches('\r');
            if !line.is_empty() {
                append_runtime_log(project_root, self.source, line)?;
            }
            self.partial_line.drain(..=newline_index);
        }

        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
fn build_wrapper_script(
    working_dir: &Path,
    binary: &Path,
    args: &[String],
    stdin_path: &Path,
    stdout_path: &Path,
    stderr_path: &Path,
    exit_status_path: &Path,
    signal_pid_path: &Path,
) -> AppResult<String> {
    // Validate that the binary path is valid UTF-8 so that we produce a
    // correct shell script.  `to_string_lossy()` would silently replace
    // non-UTF-8 bytes with U+FFFD, yielding a broken command.
    let binary_str = binary
        .to_str()
        .ok_or_else(|| AppError::BackendUnavailable {
            backend: "tmux".to_owned(),
            details: format!(
            "binary path '{}' contains non-UTF-8 bytes and cannot be embedded in a shell script",
            binary.display()
        ),
            failure_class: None,
        })?;

    let command = std::iter::once(shell_escape(binary_str))
        .chain(args.iter().map(|arg| shell_escape(arg)))
        .collect::<Vec<_>>()
        .join(" ");
    // Issue #188 follow-up: the original wrapper used `mkfifo` + two
    // background `tee FILE < FIFO` processes to capture stdout/stderr
    // while keeping the streams visible to anyone attaching to the tmux
    // session. That pattern relies on `tee` looping until EOF on the
    // FIFO. uutils-coreutils tee 0.8.0 (the default tee on NixOS and
    // some other distros that ship uutils) closes its FIFO reader
    // before the producer is finished — observed in practice as the
    // FIFO returning EOF after a single read on the reporter's
    // workspace; the public uutils tracker also has buffering-related
    // deviations from GNU tee in the same release line. Either way,
    // the producer's next write to the FIFO gets SIGPIPE and the child
    // exits 141 — which surfaced as the "codex exited with code 101"
    // loop reported in issue #188. See the PR body for the shell-only
    // repro that does not involve ralph or codex.
    //
    // The fix is to drop the FIFO+tee dance entirely. We redirect the
    // child's stdout/stderr directly to files (load-bearing — ralph's
    // CaptureTail reads these files through `wait_for_session_exit`,
    // not through the tmux pty) and use `tail -f` to mirror the live
    // output to the tmux pty for `run attach` users. `tail -f` on a
    // regular growing file works correctly under both GNU and uutils
    // coreutils, so no FIFO is involved in the streaming path.
    //
    // On the cleanup path: after the child exits we sleep briefly so
    // the inotify-backed tail can flush its final read before the
    // post-wait `kill`. We deliberately do NOT pass `--pid="$child_pid"`
    // because that flag is GNU-only — BSD tail (macOS, NetBSD) does
    // not support it and would error out, leaving tmux-attach users
    // with no streaming at all on those platforms. With inotify the
    // grace is mostly idle (events fire on each child write); on
    // polling-mode tails the grace may not cover a full polling
    // cycle (~1s by default), so the final lines may be lost from
    // the tmux pane. The captured stdout/stderr files are unaffected
    // because the child writes them directly via shell redirection.
    Ok(format!(
        "#!/usr/bin/env bash\nset +e\nset -m\ncd {cwd}\nrm -f {exit_status} {signal_pid}\ntrap 'rm -f {signal_pid}' EXIT\n: > {stdout}\n: > {stderr}\n(\n  printf '%s' \"$BASHPID\" > {signal_pid}\n  exec {command} < {stdin} > {stdout} 2> {stderr}\n) &\nchild_pid=$!\ntail -n +1 -f {stdout} 2>/dev/null &\ntail_stdout_pid=$!\ntail -n +1 -f {stderr} >&2 2>/dev/null &\ntail_stderr_pid=$!\nwait \"$child_pid\"\nstatus=$?\nsleep 0.5\nkill \"$tail_stdout_pid\" \"$tail_stderr_pid\" 2>/dev/null\nwait \"$tail_stdout_pid\" 2>/dev/null\nwait \"$tail_stderr_pid\" 2>/dev/null\nprintf '%s' \"$status\" > {exit_status}\nexit \"$status\"\n",
        cwd = shell_escape(&working_dir.to_string_lossy()),
        stdin = shell_escape(&stdin_path.to_string_lossy()),
        stdout = shell_escape(&stdout_path.to_string_lossy()),
        stderr = shell_escape(&stderr_path.to_string_lossy()),
        exit_status = shell_escape(&exit_status_path.to_string_lossy()),
        signal_pid = shell_escape(&signal_pid_path.to_string_lossy()),
    ))
}

fn flush_capture_file(project_root: &Path, path: &Path, source: &str) -> AppResult<()> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    flush_lines(project_root, source, &contents)
}

fn flush_lines(project_root: &Path, source: &str, contents: &str) -> AppResult<()> {
    let mut saw_line = false;
    for line in contents.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() {
            continue;
        }
        saw_line = true;
        append_runtime_log(project_root, source, line)?;
    }

    if !saw_line && !contents.trim().is_empty() {
        append_runtime_log(project_root, source, contents.trim())?;
    }

    Ok(())
}

fn append_runtime_log(project_root: &Path, source: &str, message: &str) -> AppResult<()> {
    if message.is_empty() {
        return Ok(());
    }

    let log_dir = project_root.join("runtime/logs");
    fs::create_dir_all(&log_dir)?;
    let log_path = log_dir.join("run.ndjson");
    let entry = RuntimeLogEntry {
        timestamp: Utc::now(),
        level: LogLevel::Info,
        source: source.to_owned(),
        message: message.to_owned(),
    };
    let line = serde_json::to_string(&entry)?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    use std::io::Write;
    writeln!(file, "{line}")?;
    Ok(())
}

fn read_exit_code(path: &Path) -> AppResult<Option<i32>> {
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let trimmed = contents.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    trimmed
        .parse::<i32>()
        .map(Some)
        .map_err(|error| AppError::CorruptRecord {
            file: path.display().to_string(),
            details: format!("invalid tmux exit code: {error}"),
        })
}

fn is_missing_tmux_session(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();
    stderr.contains("can't find session") || stderr.contains("no server running")
}

#[cfg(unix)]
fn signal_tmux_target(pid: i32, signal: Signal) -> std::io::Result<()> {
    let group_result = nix::sys::signal::kill(Pid::from_raw(-pid), signal);
    let pid_result = nix::sys::signal::kill(Pid::from_raw(pid), signal);

    match (group_result, pid_result) {
        (Ok(()), _) | (_, Ok(())) => Ok(()),
        (Err(Errno::ESRCH), Err(Errno::ESRCH)) => Ok(()),
        (Err(Errno::ESRCH), Err(errno)) | (Err(errno), Err(Errno::ESRCH)) => {
            Err(std::io::Error::from_raw_os_error(errno as i32))
        }
        (Err(errno), _) => Err(std::io::Error::from_raw_os_error(errno as i32)),
    }
}

fn sanitize_session_segment(value: &str) -> String {
    value
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '-',
        })
        .collect()
}

fn shell_escape(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

#[cfg(unix)]
fn exit_status_from_code(code: i32) -> std::process::ExitStatus {
    use std::os::unix::process::ExitStatusExt;

    std::process::ExitStatus::from_raw(code << 8)
}

#[cfg(not(unix))]
fn exit_status_from_code(_code: i32) -> std::process::ExitStatus {
    unreachable!("tmux execution currently targets unix platforms")
}

#[cfg(all(test, unix))]
mod wrapper_script_tests {
    //! Issue #188 follow-up regression tests for `build_wrapper_script`.
    //!
    //! The original wrapper used `mkfifo` + `tee FILE < FIFO` to capture
    //! stdout/stderr while still streaming to the tmux pty. That pattern
    //! relies on `tee` looping until EOF on the FIFO. uutils-coreutils tee
    //! 0.8.0 (the default `tee` on NixOS and any system that ships uutils
    //! in lieu of GNU coreutils) reads the FIFO once and exits. The child's
    //! next write to the FIFO then gets `SIGPIPE` and the child exits 141,
    //! which surfaced as the `code 101 transport_failure` retry loop in
    //! issue #188.
    //!
    //! These tests exercise the wrapper script with a fake codex that
    //! mimics the failing pattern: write output, sleep (FIFO would be idle
    //! here), write more, exit cleanly. The wrapper must capture **all**
    //! lines and report exit 0. They run as part of the normal cargo test
    //! run; they don't require a tmux binary because they execute the
    //! wrapper script directly under bash.

    use super::build_wrapper_script;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::process::Command;
    use tempfile::tempdir;

    fn write_executable(path: &Path, contents: &str) {
        fs::write(path, contents).expect("write fake binary");
        let mut permissions = fs::metadata(path).expect("stat fake binary").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod fake binary");
    }

    /// Fake child that emits output, sleeps, emits more output, then
    /// exits cleanly. Matches the codex CLI shape that triggered #188:
    /// banner → reasoning (idle on stdout) → final response.
    ///
    /// Uses `#!/bin/sh` (POSIX-only) so the test runs in restricted
    /// environments like the Nix build sandbox where `/usr/bin/env`
    /// and bash may not be on $PATH for child exec.
    fn write_fake_codex(bin: &Path) {
        write_executable(
            bin,
            r#"#!/bin/sh
echo "OpenAI Codex banner"
sleep 1
echo "thinking..."
sleep 1
echo "final response"
exit 0
"#,
        );
    }

    fn make_wrapper(working_dir: &Path, binary: &Path, paths: &WrapperPaths) -> std::path::PathBuf {
        let script = build_wrapper_script(
            working_dir,
            binary,
            &[],
            &paths.stdin,
            &paths.stdout,
            &paths.stderr,
            &paths.exit_status,
            &paths.signal_pid,
        )
        .expect("build wrapper script");
        let wrapper_path = working_dir.join("wrapper.sh");
        write_executable(&wrapper_path, &script);
        wrapper_path
    }

    struct WrapperPaths {
        stdin: std::path::PathBuf,
        stdout: std::path::PathBuf,
        stderr: std::path::PathBuf,
        exit_status: std::path::PathBuf,
        signal_pid: std::path::PathBuf,
    }

    impl WrapperPaths {
        fn new(dir: &Path) -> Self {
            let stdin = dir.join("stdin.txt");
            fs::write(&stdin, "test prompt\n").expect("write stdin");
            Self {
                stdin,
                stdout: dir.join("stdout"),
                stderr: dir.join("stderr"),
                exit_status: dir.join("exit"),
                signal_pid: dir.join("pid"),
            }
        }
    }

    #[test]
    fn wrapper_captures_all_output_across_sleeps_under_uutils_tee() {
        // Regression test for issue #188: the wrapper must capture every
        // line of child output even when the system `tee` is uutils
        // 0.8.0, which reads a FIFO once and exits. Before the fix, the
        // wrapper used `tee FILE < FIFO` and only the first line landed
        // in the file before SIGPIPE killed the child. After the fix the
        // wrapper redirects directly to the file and uses `tail -f` for
        // tmux pty streaming, so the FIFO+tee race no longer exists.
        let dir = tempdir().expect("create temp dir");
        let bin = dir.path().join("fake-codex");
        write_fake_codex(&bin);
        let paths = WrapperPaths::new(dir.path());
        let wrapper = make_wrapper(dir.path(), &bin, &paths);

        let output = Command::new("bash")
            .arg(&wrapper)
            .output()
            .expect("run wrapper");

        assert_eq!(
            output.status.code(),
            Some(0),
            "wrapper must exit 0; stderr was: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let captured_stdout = fs::read_to_string(&paths.stdout).expect("read captured stdout");
        let captured_lines: Vec<&str> = captured_stdout.lines().collect();
        assert_eq!(
            captured_lines,
            vec!["OpenAI Codex banner", "thinking...", "final response"],
            "wrapper must capture all 3 child stdout lines (uutils tee \
             0.8.0 reads the FIFO once and exits, which would lose every \
             line after the first under the old wrapper)"
        );

        let recorded_status =
            fs::read_to_string(&paths.exit_status).expect("read exit-status file");
        assert_eq!(
            recorded_status, "0",
            "exit_status file must record the child's clean exit"
        );
    }

    #[test]
    fn wrapper_propagates_child_failure_exit_code() {
        // Sanity: a child that writes to stdout then exits non-zero must
        // surface its exit code, not get masked into 0 by the wrapper's
        // own exit handling.
        let dir = tempdir().expect("create temp dir");
        let bin = dir.path().join("failing-fake");
        write_executable(
            &bin,
            r#"#!/bin/sh
echo "starting"
sleep 1
echo "about to fail"
exit 7
"#,
        );
        let paths = WrapperPaths::new(dir.path());
        let wrapper = make_wrapper(dir.path(), &bin, &paths);

        let output = Command::new("bash")
            .arg(&wrapper)
            .output()
            .expect("run wrapper");

        assert_eq!(output.status.code(), Some(7));
        let recorded_status =
            fs::read_to_string(&paths.exit_status).expect("read exit-status file");
        assert_eq!(recorded_status, "7");
        let captured_stdout = fs::read_to_string(&paths.stdout).expect("read captured stdout");
        assert!(
            captured_stdout.contains("starting") && captured_stdout.contains("about to fail"),
            "captured stdout should contain both lines emitted before \
             the non-zero exit; got: {captured_stdout:?}"
        );
    }

    #[test]
    fn wrapper_records_signal_pid_and_honors_external_termination() {
        // The signal_pid file is the load-bearing handle ralph uses to
        // SIGTERM/SIGKILL the child via `cancel()`. The fix must not
        // regress that contract. We spawn a long-lived child, wait until
        // signal_pid is written, signal that pid, and assert the wrapper
        // exits with the SIGTERM-derived status.
        use std::thread;
        use std::time::{Duration, Instant};

        let dir = tempdir().expect("create temp dir");
        let bin = dir.path().join("long-fake");
        write_executable(
            &bin,
            r#"#!/bin/sh
echo "alive"
# Sleep long enough that the test can read signal_pid and signal us.
sleep 30
"#,
        );
        let paths = WrapperPaths::new(dir.path());
        let wrapper = make_wrapper(dir.path(), &bin, &paths);

        let mut child = Command::new("bash")
            .arg(&wrapper)
            .spawn()
            .expect("spawn wrapper");

        // Poll for signal_pid to appear and contain a positive PID. The
        // wrapper writes it inside the child subshell after fork, so it
        // takes a few ms after spawn.
        let deadline = Instant::now() + Duration::from_secs(5);
        let signal_pid: u32 = loop {
            if let Ok(raw) = fs::read_to_string(&paths.signal_pid) {
                if let Ok(pid) = raw.trim().parse::<u32>() {
                    if pid > 0 {
                        break pid;
                    }
                }
            }
            assert!(
                Instant::now() < deadline,
                "signal_pid file was never populated with a positive PID"
            );
            thread::sleep(Duration::from_millis(25));
        };

        // SIGTERM the recorded pid, then wait for the wrapper to exit.
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(signal_pid as i32),
            nix::sys::signal::Signal::SIGTERM,
        )
        .expect("kill(SIGTERM) on signal_pid");

        let status = child.wait().expect("wait for wrapper");
        assert!(
            !status.success(),
            "wrapper must surface non-zero exit after the child is SIGTERM'd: {status:?}"
        );

        // The wrapper writes the child's exit status to exit_status. A
        // SIGTERM'd child exits with status 128 + SIGTERM (15) = 143.
        let recorded = fs::read_to_string(&paths.exit_status).expect("read exit-status");
        assert_eq!(
            recorded.trim(),
            "143",
            "exit_status must record 128+SIGTERM for SIGTERM'd child"
        );
    }
}
