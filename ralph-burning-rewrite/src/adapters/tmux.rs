use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::adapters::openrouter_backend::OpenRouterBackendAdapter;
use crate::adapters::process_backend::{ChildOutput, ProcessBackendAdapter};
use crate::contexts::agent_execution::model::{
    InvocationContract, InvocationEnvelope, InvocationRequest,
};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::contexts::project_run_record::model::{LogLevel, RuntimeLogEntry};
use crate::shared::domain::{BackendFamily, FailureClass, ResolvedBackendTarget};
use crate::shared::error::{AppError, AppResult};

const SESSION_POLL_INTERVAL: Duration = Duration::from_millis(100);
const CANCEL_GRACE_PERIOD: Duration = Duration::from_millis(500);

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

    pub fn session_name(project_id: &str, invocation_id: &str) -> String {
        format!(
            "rb-{}-{}",
            sanitize_session_segment(project_id),
            sanitize_session_segment(invocation_id)
        )
    }

    pub fn check_tmux_available() -> AppResult<()> {
        ProcessBackendAdapter::ensure_binary_available("tmux", "tmux")
    }

    pub fn session_exists(session_name: &str) -> AppResult<bool> {
        if let Err(error) = Self::check_tmux_available() {
            return match error {
                AppError::BackendUnavailable { .. } => Ok(false),
                other => Err(other),
            };
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
        match Command::new("tmux")
            .args(["kill-session", "-t", session_name])
            .status()
            .await
        {
            Ok(status) if status.success() => Ok(()),
            Ok(_) => Ok(()),
            Err(error) => Err(AppError::InvocationFailed {
                backend: "tmux".to_owned(),
                contract_id: session_name.to_owned(),
                failure_class: FailureClass::TransportFailure,
                details: format!("failed to kill tmux session '{session_name}': {error}"),
            }),
        }
    }

    async fn send_ctrl_c(&self, session_name: &str) -> AppResult<()> {
        match Command::new("tmux")
            .args(["send-keys", "-t", session_name, "C-c"])
            .status()
            .await
        {
            Ok(status) if status.success() => Ok(()),
            Ok(_) => Ok(()),
            Err(error) => Err(AppError::InvocationFailed {
                backend: "tmux".to_owned(),
                contract_id: session_name.to_owned(),
                failure_class: FailureClass::TransportFailure,
                details: format!("failed to send Ctrl-C to tmux session '{session_name}': {error}"),
            }),
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
                return Ok(exit_code);
            }

            if !self.has_session(&session.session_name).await? {
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
        match backend.backend.family {
            BackendFamily::OpenRouter => {
                OpenRouterBackendAdapter::new()
                    .check_capability(backend, contract)
                    .await
            }
            _ => self.process.check_capability(backend, contract).await,
        }
    }

    async fn check_availability(&self, backend: &ResolvedBackendTarget) -> AppResult<()> {
        match backend.backend.family {
            BackendFamily::OpenRouter => {
                OpenRouterBackendAdapter::new()
                    .check_availability(backend)
                    .await
            }
            _ => {
                Self::check_tmux_available()?;
                self.process.check_availability(backend).await
            }
        }
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        self.check_capability(&request.resolved_target, &request.contract)
            .await?;

        if request.resolved_target.backend.family == BackendFamily::OpenRouter {
            return OpenRouterBackendAdapter::new().invoke(request).await;
        }

        Self::check_tmux_available()?;

        let prepared = self.process.build_command(&request).await?;
        let project_name = request
            .project_root
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("workspace");
        let session_name = Self::session_name(project_name, &request.invocation_id);
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

        let exit_code = match self
            .wait_for_session_exit(&session, &mut stdout_tail, &mut stderr_tail)
            .await
        {
            Ok(exit_code) => exit_code,
            Err(error) => {
                let _ = self.kill_session(&session.session_name).await;
                self.remove_session_if_same(&request.invocation_id, &session)
                    .await;
                prepared.cleanup().await;
                session.cleanup();
                return Err(error);
            }
        };

        self.finalize_captured_output(&session, &mut stdout_tail, &mut stderr_tail)
            .await?;

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
        session.cleanup();

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
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

        let _ = self.send_ctrl_c(&session.session_name).await;
        let wait_result = tokio::time::timeout(CANCEL_GRACE_PERIOD, async {
            loop {
                if read_exit_code(&session.exit_status_path)?.is_some()
                    || !self.has_session(&session.session_name).await?
                {
                    return Ok::<(), AppError>(());
                }
                tokio::time::sleep(SESSION_POLL_INTERVAL).await;
            }
        })
        .await;

        match wait_result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                self.kill_session(&session.session_name).await?;
            }
        }

        append_runtime_log(
            &session.project_root,
            "tmux.lifecycle",
            &format!("session cleaned up: {}", session.session_name),
        )?;
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
        let exit_status_path = temp_dir.join(format!("{}.tmux.exit", request.invocation_id));
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
            exit_status_path,
        })
    }

    fn cleanup(&self) {
        for path in [
            &self.wrapper_path,
            &self.stdin_path,
            &self.stdout_path,
            &self.stderr_path,
            &self.exit_status_path,
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
    exit_status_path: &Path,
) -> String {
    let command = std::iter::once(shell_escape(binary))
        .chain(args.iter().map(|arg| shell_escape(arg)))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "#!/usr/bin/env bash\nset +e\ncd {cwd}\n{command} < {stdin} > >(tee {stdout}) 2> >(tee {stderr} >&2)\nstatus=$?\nprintf '%s' \"$status\" > {exit_status}\nexit \"$status\"\n",
        cwd = shell_escape(&working_dir.to_string_lossy()),
        stdin = shell_escape(&stdin_path.to_string_lossy()),
        stdout = shell_escape(&stdout_path.to_string_lossy()),
        stderr = shell_escape(&stderr_path.to_string_lossy()),
        exit_status = shell_escape(&exit_status_path.to_string_lossy()),
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
