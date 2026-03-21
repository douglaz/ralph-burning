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
use crate::adapters::process_backend::{ChildOutput, ProcessBackendAdapter};
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
}

impl TmuxAdapter {
    pub fn new(process: ProcessBackendAdapter, stream_output: bool) -> Self {
        Self {
            process,
            active_sessions: Arc::new(Mutex::new(HashMap::new())),
            stream_output,
        }
    }

    pub fn session_name(project_id: &str, invocation_id: &str, project_root: &std::path::Path) -> String {
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
        ProcessBackendAdapter::ensure_binary_available("tmux", "tmux")
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
            }),
        }
    }

    pub fn session_exists(session_name: &str) -> AppResult<bool> {
        if let Err(_) = Self::check_tmux_available() {
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

    async fn has_session(&self, session_name: &str) -> AppResult<bool> {
        match Command::new("tmux")
            .args(["has-session", "-t", session_name])
            .status()
            .await
        {
            Ok(status) => Ok(status.success()),
            Err(error) => Err(AppError::BackendUnavailable {
                backend: "tmux".to_owned(),
                details: format!("failed to query tmux session '{session_name}': {error}"),
            }),
        }
    }

    async fn kill_session(&self, session_name: &str) -> AppResult<()> {
        let output = Command::new("tmux")
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
        let output = Command::new("tmux")
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
        Self::check_tmux_available()?;
        self.process.check_availability(backend).await
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        self.check_capability(&request.resolved_target, &request.contract)
            .await?;
        Self::check_tmux_available()?;

        let prepared = self.process.build_command(&request).await?;
        let project_name = request
            .project_root
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("workspace");
        let session_name = Self::session_name(project_name, &request.invocation_id, &request.project_root);
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

        let exit_code = match self
            .wait_for_session_exit(&session, &mut stdout_tail, &mut stderr_tail)
            .await
        {
            Ok(exit_code) => exit_code,
            Err(error) => {
                let _ = self.kill_session(&session.session_name).await;
                self.remove_session_if_same(&request.invocation_id, &session)
                    .await;
                let _ = Self::clear_active_session(&request.project_root, &request.invocation_id);
                prepared.cleanup().await;
                session.cleanup();
                return Err(error);
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
            return Err(AppError::InvocationFailed {
                backend: request.resolved_target.backend.family.to_string(),
                contract_id: request.contract.label(),
                failure_class: FailureClass::TransportFailure,
                details: format!(
                    "{} exited with code {}{}",
                    prepared.binary(),
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
    stdout_pipe_path: PathBuf,
    stderr_pipe_path: PathBuf,
    exit_status_path: PathBuf,
    signal_pid_path: PathBuf,
}

impl ManagedTmuxSession {
    fn new(
        request: &InvocationRequest,
        session_name: String,
        args: &[String],
        binary: &str,
        stdin_payload: &str,
    ) -> AppResult<Self> {
        let temp_dir = request.project_root.join("runtime/temp");
        fs::create_dir_all(&temp_dir)?;

        let stdin_path = temp_dir.join(format!("{}.tmux.stdin", request.invocation_id));
        let stdout_path = temp_dir.join(format!("{}.tmux.stdout", request.invocation_id));
        let stderr_path = temp_dir.join(format!("{}.tmux.stderr", request.invocation_id));
        let stdout_pipe_path = temp_dir.join(format!("{}.tmux.stdout.pipe", request.invocation_id));
        let stderr_pipe_path = temp_dir.join(format!("{}.tmux.stderr.pipe", request.invocation_id));
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
                &stdout_pipe_path,
                &stderr_pipe_path,
                &exit_status_path,
                &signal_pid_path,
            ),
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
            stdout_pipe_path,
            stderr_pipe_path,
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
            &self.stdout_pipe_path,
            &self.stderr_pipe_path,
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

fn build_wrapper_script(
    working_dir: &Path,
    binary: &str,
    args: &[String],
    stdin_path: &Path,
    stdout_path: &Path,
    stderr_path: &Path,
    stdout_pipe_path: &Path,
    stderr_pipe_path: &Path,
    exit_status_path: &Path,
    signal_pid_path: &Path,
) -> String {
    let command = std::iter::once(shell_escape(binary))
        .chain(args.iter().map(|arg| shell_escape(arg)))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "#!/usr/bin/env bash\nset +e\nset -m\ncd {cwd}\nrm -f {exit_status} {signal_pid} {stdout_pipe} {stderr_pipe}\nmkfifo {stdout_pipe} {stderr_pipe}\ntrap 'rm -f {signal_pid} {stdout_pipe} {stderr_pipe}' EXIT\ntee {stdout} < {stdout_pipe} &\nstdout_tee_pid=$!\ntee {stderr} < {stderr_pipe} >&2 &\nstderr_tee_pid=$!\n(\n  printf '%s' \"$BASHPID\" > {signal_pid}\n  exec {command} < {stdin} > {stdout_pipe} 2> {stderr_pipe}\n) &\nchild_pid=$!\nwait \"$child_pid\"\nstatus=$?\nwait \"$stdout_tee_pid\"\nwait \"$stderr_tee_pid\"\nprintf '%s' \"$status\" > {exit_status}\nexit \"$status\"\n",
        cwd = shell_escape(&working_dir.to_string_lossy()),
        stdin = shell_escape(&stdin_path.to_string_lossy()),
        stdout = shell_escape(&stdout_path.to_string_lossy()),
        stderr = shell_escape(&stderr_path.to_string_lossy()),
        stdout_pipe = shell_escape(&stdout_pipe_path.to_string_lossy()),
        stderr_pipe = shell_escape(&stderr_pipe_path.to_string_lossy()),
        exit_status = shell_escape(&exit_status_path.to_string_lossy()),
        signal_pid = shell_escape(&signal_pid_path.to_string_lossy()),
    )
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
