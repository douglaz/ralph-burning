use std::collections::HashMap;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::contexts::agent_execution::model::{
    InvocationContract, InvocationEnvelope, InvocationMetadata, InvocationRequest,
    RawOutputReference, TokenCounts,
};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::shared::domain::{BackendFamily, FailureClass, ResolvedBackendTarget, SessionPolicy};
use crate::shared::error::{AppError, AppResult};

const CHILD_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const CANCEL_GRACE_PERIOD: Duration = Duration::from_millis(500);

pub(crate) struct PreparedCommand {
    binary: String,
    args: Vec<String>,
    stdin_payload: String,
    response_decoder: ResponseDecoder,
}

impl PreparedCommand {
    pub(crate) fn binary(&self) -> &str {
        &self.binary
    }

    pub(crate) fn args(&self) -> &[String] {
        &self.args
    }

    pub(crate) fn stdin_payload(&self) -> &str {
        &self.stdin_payload
    }

    pub(crate) async fn cleanup(&self) {
        match &self.response_decoder {
            ResponseDecoder::Claude { .. } => {}
            ResponseDecoder::Codex {
                schema_path,
                message_path,
                ..
            } => best_effort_cleanup(Some(schema_path), message_path).await,
        }
    }

    pub(crate) async fn finish(
        self,
        request: &InvocationRequest,
        output: ChildOutput,
    ) -> AppResult<InvocationEnvelope> {
        match self.response_decoder {
            ResponseDecoder::Claude { session_resuming } => {
                let stdout_text = String::from_utf8_lossy(&output.stdout).into_owned();

                let envelope: ClaudeEnvelope =
                    serde_json::from_str(&stdout_text).map_err(|error| {
                        ProcessBackendAdapter::invocation_failed(
                            request,
                            FailureClass::SchemaValidationFailure,
                            format!("invalid Claude envelope JSON: {error}"),
                        )
                    })?;

                let parsed_payload = if let Some(structured) = envelope.structured_output {
                    structured
                } else if !envelope.result.trim().is_empty() {
                    // result is non-empty: try direct parse, then extract embedded JSON
                    serde_json::from_str(&envelope.result)
                        .or_else(|_| extract_json_from_text(&envelope.result))
                        .map_err(|error| {
                            ProcessBackendAdapter::invocation_failed(
                                request,
                                FailureClass::SchemaValidationFailure,
                                format!(
                                    "invalid Claude result JSON: {error} \
                                 (contract: {}, result_len: {})",
                                    request.contract.label(),
                                    envelope.result.len(),
                                ),
                            )
                        })?
                } else {
                    // Both structured_output and result are empty.
                    // Last resort: try to find JSON in the raw stdout beyond the envelope.
                    // Guard: reject if the extracted object is just the Claude envelope
                    // itself (has `result` + `session_id` keys), which would happen when
                    // stdout contains only the envelope and no separate payload.
                    extract_json_from_text(&stdout_text)
                        .ok()
                        .filter(|val| !looks_like_claude_envelope(val))
                        .ok_or_else(|| {
                            ProcessBackendAdapter::invocation_failed(
                                request,
                                FailureClass::SchemaValidationFailure,
                                format!(
                                    "Claude returned empty result with no structured_output \
                                     (contract: {}, stdout_len: {}, session_policy: {:?})",
                                    request.contract.label(),
                                    output.stdout.len(),
                                    request.session_policy,
                                ),
                            )
                        })?
                };

                let session_id = envelope.session_id.or_else(|| {
                    if session_resuming {
                        request.prior_session.as_ref().map(|s| s.session_id.clone())
                    } else {
                        None
                    }
                });

                Ok(InvocationEnvelope {
                    raw_output_reference: RawOutputReference::Inline(stdout_text),
                    parsed_payload,
                    metadata: InvocationMetadata {
                        invocation_id: request.invocation_id.clone(),
                        duration: Duration::from_millis(0),
                        token_counts: TokenCounts::default(),
                        backend_used: request.resolved_target.backend.clone(),
                        model_used: request.resolved_target.model.clone(),
                        attempt_number: request.attempt_number,
                        session_id,
                        session_reused: session_resuming,
                    },
                    timestamp: Utc::now(),
                })
            }
            ResponseDecoder::Codex {
                schema_path,
                message_path,
                session_resuming,
            } => {
                let last_message_text = match tokio::fs::read_to_string(&message_path).await {
                    Ok(text) => text,
                    Err(error) => {
                        best_effort_cleanup(Some(&schema_path), &message_path).await;
                        return Err(ProcessBackendAdapter::invocation_failed(
                            request,
                            FailureClass::TransportFailure,
                            format!("failed to read codex last-message file: {error}"),
                        ));
                    }
                };

                let parsed_payload = match serde_json::from_str(&last_message_text) {
                    Ok(value) => value,
                    Err(error) => {
                        best_effort_cleanup(Some(&schema_path), &message_path).await;
                        return Err(ProcessBackendAdapter::invocation_failed(
                            request,
                            FailureClass::SchemaValidationFailure,
                            format!("invalid Codex last-message JSON: {error}"),
                        ));
                    }
                };

                best_effort_cleanup(Some(&schema_path), &message_path).await;

                let session_id = if session_resuming {
                    request.prior_session.as_ref().map(|s| s.session_id.clone())
                } else {
                    None
                };

                Ok(InvocationEnvelope {
                    raw_output_reference: RawOutputReference::Inline(last_message_text),
                    parsed_payload,
                    metadata: InvocationMetadata {
                        invocation_id: request.invocation_id.clone(),
                        duration: Duration::from_millis(0),
                        token_counts: TokenCounts::default(),
                        backend_used: request.resolved_target.backend.clone(),
                        model_used: request.resolved_target.model.clone(),
                        attempt_number: request.attempt_number,
                        session_id,
                        session_reused: session_resuming,
                    },
                    timestamp: Utc::now(),
                })
            }
        }
    }
}

enum ResponseDecoder {
    Claude {
        session_resuming: bool,
    },
    Codex {
        schema_path: std::path::PathBuf,
        message_path: std::path::PathBuf,
        session_resuming: bool,
    },
}

#[derive(Clone, Default)]
pub struct ProcessBackendAdapter {
    pub active_children: Arc<Mutex<HashMap<String, Arc<ManagedChild>>>>,
}

pub struct ManagedChild {
    state: Mutex<ManagedChildState>,
}

enum ManagedChildState {
    Running(Child),
    Exited(ExitStatus),
}

impl ManagedChild {
    fn new(child: Child) -> Self {
        Self {
            state: Mutex::new(ManagedChildState::Running(child)),
        }
    }

    async fn pid(&self) -> Option<u32> {
        let state = self.state.lock().await;
        match &*state {
            ManagedChildState::Running(child) => child.id(),
            ManagedChildState::Exited(_) => None,
        }
    }

    async fn send_sigterm(&self) -> std::io::Result<()> {
        let Some(pid) = self.pid().await else {
            return Ok(());
        };

        #[cfg(unix)]
        {
            let pid = i32::try_from(pid).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("process id {pid} exceeds libc::pid_t range"),
                )
            })?;
            match nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid),
                nix::sys::signal::Signal::SIGTERM,
            ) {
                Ok(()) => Ok(()),
                Err(nix::errno::Errno::ESRCH) => Ok(()),
                Err(errno) => Err(std::io::Error::from_raw_os_error(errno as i32)),
            }
        }

        #[cfg(not(unix))]
        {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "SIGTERM delivery requires unix",
            ))
        }
    }

    async fn send_sigkill(&self) -> std::io::Result<()> {
        let Some(pid) = self.pid().await else {
            return Ok(());
        };

        #[cfg(unix)]
        {
            let pid = i32::try_from(pid).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("process id {pid} exceeds libc::pid_t range"),
                )
            })?;
            match nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid),
                nix::sys::signal::Signal::SIGKILL,
            ) {
                Ok(()) => Ok(()),
                Err(nix::errno::Errno::ESRCH) => Ok(()),
                Err(errno) => Err(std::io::Error::from_raw_os_error(errno as i32)),
            }
        }

        #[cfg(not(unix))]
        {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "SIGKILL delivery requires unix",
            ))
        }
    }

    async fn wait(&self) -> std::io::Result<ExitStatus> {
        loop {
            let maybe_status = {
                let mut state = self.state.lock().await;
                match &mut *state {
                    // Poll without holding the child mutex across a long wait
                    // so cancel() can still acquire the handle and signal it.
                    ManagedChildState::Running(child) => match child.try_wait()? {
                        Some(status) => {
                            *state = ManagedChildState::Exited(status);
                            Some(status)
                        }
                        None => None,
                    },
                    ManagedChildState::Exited(status) => return Ok(*status),
                }
            };

            if let Some(status) = maybe_status {
                return Ok(status);
            }

            tokio::time::sleep(CHILD_WAIT_POLL_INTERVAL).await;
        }
    }
}

impl ProcessBackendAdapter {
    pub fn new() -> Self {
        Self {
            active_children: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn register_child(&self, invocation_id: &str, child: Arc<ManagedChild>) {
        let mut children = self.active_children.lock().await;
        children.insert(invocation_id.to_owned(), child);
    }

    async fn take_active_child(&self, invocation_id: &str) -> Option<Arc<ManagedChild>> {
        let mut children = self.active_children.lock().await;
        children.remove(invocation_id)
    }

    async fn remove_child_if_same(&self, invocation_id: &str, child: &Arc<ManagedChild>) {
        let mut children = self.active_children.lock().await;
        if children
            .get(invocation_id)
            .is_some_and(|current| Arc::ptr_eq(current, child))
        {
            children.remove(invocation_id);
        }
    }

    fn binary_name(backend: BackendFamily) -> Option<&'static str> {
        match backend {
            BackendFamily::Claude => Some("claude"),
            BackendFamily::Codex => Some("codex"),
            BackendFamily::OpenRouter | BackendFamily::Stub => None,
        }
    }

    fn capability_mismatch(
        backend: &ResolvedBackendTarget,
        contract: &InvocationContract,
        details: impl Into<String>,
    ) -> AppError {
        AppError::CapabilityMismatch {
            backend: backend.backend.family.to_string(),
            contract_id: contract.label(),
            details: details.into(),
        }
    }

    /// Build the stdin payload from prompt + context.
    fn assemble_stdin(request: &InvocationRequest) -> String {
        let contract_label = request.contract.label();
        let role = request.role.display_name();
        let prompt = &request.payload.prompt;

        let mut input = String::new();
        input.push_str(&format!("Contract: {contract_label}\n"));
        input.push_str(&format!("Role: {role}\n\n"));
        input.push_str(prompt);
        input.push('\n');

        if !request.payload.context.is_null() {
            input.push_str("\n--- Context JSON ---\n");
            input.push_str(
                &serde_json::to_string_pretty(&request.payload.context)
                    .unwrap_or_else(|_| request.payload.context.to_string()),
            );
            input.push('\n');
        }

        let schema_json = request.contract.json_schema_value();
        let schema_json =
            serde_json::to_string_pretty(&schema_json).unwrap_or_else(|_| "{}".to_owned());

        input.push_str("\nReturn ONLY valid JSON matching the following schema:\n");
        input.push_str(&schema_json);
        input.push('\n');

        input
    }

    pub(crate) async fn build_command(
        &self,
        request: &InvocationRequest,
    ) -> AppResult<PreparedCommand> {
        match request.resolved_target.backend.family {
            BackendFamily::Claude => {
                let model_id = &request.resolved_target.model.model_id;
                let mut schema_value = request.contract.json_schema_value();
                enforce_strict_mode_schema(&mut schema_value);
                let schema_json = serde_json::to_string(&schema_value)
                    .unwrap_or_else(|_| "{}".to_owned());
                let session_resuming = matches!(request.session_policy, SessionPolicy::ReuseIfAllowed)
                    && request.prior_session.is_some();

                let mut args = vec![
                    "-p".to_owned(),
                    "--output-format".to_owned(),
                    "json".to_owned(),
                    "--model".to_owned(),
                    model_id.clone(),
                    "--permission-mode".to_owned(),
                    "acceptEdits".to_owned(),
                    "--allowedTools".to_owned(),
                    "Bash,Edit,Write,Read,Glob,Grep".to_owned(),
                    "--json-schema".to_owned(),
                    schema_json,
                ];

                if session_resuming {
                    if let Some(ref session) = request.prior_session {
                        args.push("--resume".to_owned());
                        args.push(session.session_id.clone());
                    }
                }

                Ok(PreparedCommand {
                    binary: "claude".to_owned(),
                    args,
                    stdin_payload: Self::assemble_stdin(request),
                    response_decoder: ResponseDecoder::Claude { session_resuming },
                })
            }
            BackendFamily::Codex => {
                let model_id = &request.resolved_target.model.model_id;
                let session_resuming = matches!(request.session_policy, SessionPolicy::ReuseIfAllowed)
                    && request.prior_session.is_some();

                let temp_dir = request.project_root.join("runtime/temp");
                let _ = tokio::fs::create_dir_all(&temp_dir).await;

                let schema_path = temp_dir.join(format!("{}.schema.json", request.invocation_id));
                let message_path =
                    temp_dir.join(format!("{}.last-message.json", request.invocation_id));

                let mut schema_value = request.contract.json_schema_value();
                enforce_strict_mode_schema(&mut schema_value);
                let schema_json = serde_json::to_string_pretty(&schema_value)
                    .unwrap_or_else(|_| "{}".to_owned());

                if let Err(error) = tokio::fs::write(&schema_path, &schema_json).await {
                    best_effort_cleanup(Some(&schema_path), &message_path).await;
                    return Err(Self::invocation_failed(
                        request,
                        FailureClass::TransportFailure,
                        format!("failed to write schema file: {error}"),
                    ));
                }

                let args = if session_resuming {
                    let session = request
                        .prior_session
                        .as_ref()
                        .expect("session_resuming requires a prior session");
                    Self::codex_resume_args(
                        model_id,
                        &schema_path,
                        &message_path,
                        &session.session_id,
                    )
                } else {
                    Self::codex_new_session_args(model_id, &schema_path, &message_path)
                };

                Ok(PreparedCommand {
                    binary: "codex".to_owned(),
                    args,
                    stdin_payload: Self::assemble_stdin(request),
                    response_decoder: ResponseDecoder::Codex {
                        schema_path,
                        message_path,
                        session_resuming,
                    },
                })
            }
            _ => Err(Self::capability_mismatch(
                &request.resolved_target,
                &request.contract,
                "ProcessBackendAdapter currently supports only claude and codex; self-hosted workflow runs require default_backend=claude or default_backend=codex",
            )),
        }
    }

    fn invocation_failed(
        request: &InvocationRequest,
        failure_class: FailureClass,
        details: String,
    ) -> AppError {
        AppError::InvocationFailed {
            backend: request.resolved_target.backend.family.to_string(),
            contract_id: request.contract.label(),
            failure_class,
            details,
        }
    }

    pub(crate) fn ensure_binary_available(binary_name: &str, backend: &str) -> AppResult<()> {
        let path_entries = std::env::var_os("PATH")
            .map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
            .unwrap_or_default();

        #[cfg(unix)]
        let mut non_executable_candidate = None;

        for candidate in path_entries
            .into_iter()
            .map(|entry| entry.join(binary_name))
        {
            let Ok(metadata) = std::fs::metadata(&candidate) else {
                continue;
            };
            if !metadata.is_file() {
                continue;
            }

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;

                if metadata.permissions().mode() & 0o111 != 0 {
                    return Ok(());
                }

                if non_executable_candidate.is_none() {
                    non_executable_candidate = Some(candidate);
                }
                continue;
            }

            #[cfg(not(unix))]
            {
                return Ok(());
            }
        }

        #[cfg(unix)]
        if let Some(candidate) = non_executable_candidate {
            return Err(AppError::BackendUnavailable {
                backend: backend.to_owned(),
                details: format!(
                    "required binary '{binary_name}' was found at '{}' but is not executable; fix the file permissions or install a working executable on PATH",
                    candidate.display()
                ),
            });
        }

        Err(AppError::BackendUnavailable {
            backend: backend.to_owned(),
            details: format!("required binary '{binary_name}' was not found on PATH"),
        })
    }

    /// Spawn a command, write stdin, register the child handle before I/O,
    /// read captured stdout/stderr, reap the child, and then deregister it.
    async fn spawn_and_wait(
        &self,
        request: &InvocationRequest,
        binary: &str,
        args: &[String],
        stdin_payload: &str,
    ) -> AppResult<ChildOutput> {
        let mut command = Command::new(binary);
        command
            .args(args)
            .current_dir(&request.working_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let mut child = command.spawn().map_err(|error| {
            Self::invocation_failed(
                request,
                FailureClass::TransportFailure,
                format!("failed to spawn {binary}: {error}"),
            )
        })?;

        let stdin_handle = child.stdin.take();
        let mut stdout_handle = child.stdout.take();
        let mut stderr_handle = child.stderr.take();
        let active_child = Arc::new(ManagedChild::new(child));
        self.register_child(&request.invocation_id, active_child.clone())
            .await;
        let stdin_bytes = stdin_payload.as_bytes().to_vec();

        let stdin_future = async move {
            if let Some(mut stdin) = stdin_handle {
                stdin.write_all(&stdin_bytes).await?;
                stdin.shutdown().await?;
            }
            Ok::<(), std::io::Error>(())
        };
        let stdout_future = async move {
            let mut buf = Vec::new();
            if let Some(ref mut handle) = stdout_handle {
                handle.read_to_end(&mut buf).await?;
            }
            Ok::<Vec<u8>, std::io::Error>(buf)
        };
        let stderr_future = async move {
            let mut buf = Vec::new();
            if let Some(ref mut handle) = stderr_handle {
                handle.read_to_end(&mut buf).await?;
            }
            Ok::<Vec<u8>, std::io::Error>(buf)
        };

        let (stdin_result, stdout_result, stderr_result) =
            tokio::join!(stdin_future, stdout_future, stderr_future);
        let status_result = active_child.wait().await;
        self.remove_child_if_same(&request.invocation_id, &active_child)
            .await;

        let stderr_text = stderr_result
            .as_ref()
            .map(|stderr| String::from_utf8_lossy(stderr).into_owned())
            .unwrap_or_default();

        if let Err(error) = stdin_result {
            return Err(Self::invocation_failed(
                request,
                FailureClass::TransportFailure,
                format!(
                    "failed to write stdin to {binary}: {error}{}",
                    if stderr_text.is_empty() {
                        String::new()
                    } else {
                        format!(": {stderr_text}")
                    }
                ),
            ));
        }

        let stdout = stdout_result.map_err(|error| {
            Self::invocation_failed(
                request,
                FailureClass::TransportFailure,
                format!("failed to read stdout from {binary}: {error}"),
            )
        })?;

        let stderr = stderr_result.map_err(|error| {
            Self::invocation_failed(
                request,
                FailureClass::TransportFailure,
                format!("failed to read stderr from {binary}: {error}"),
            )
        })?;

        match status_result {
            Ok(status) => Ok(ChildOutput {
                status,
                stdout,
                stderr,
            }),
            Err(error) => Err(Self::invocation_failed(
                request,
                FailureClass::TransportFailure,
                format!("failed to wait on {binary} process: {error}"),
            )),
        }
    }

    fn codex_new_session_args(
        model_id: &str,
        schema_path: &Path,
        message_path: &Path,
    ) -> Vec<String> {
        vec![
            "exec".to_owned(),
            "--dangerously-bypass-approvals-and-sandbox".to_owned(),
            "--skip-git-repo-check".to_owned(),
            "--model".to_owned(),
            model_id.to_owned(),
            "--output-schema".to_owned(),
            schema_path.to_string_lossy().into_owned(),
            "--output-last-message".to_owned(),
            message_path.to_string_lossy().into_owned(),
            "-".to_owned(),
        ]
    }

    fn codex_resume_args(
        model_id: &str,
        schema_path: &Path,
        message_path: &Path,
        session_id: &str,
    ) -> Vec<String> {
        vec![
            "exec".to_owned(),
            "resume".to_owned(),
            "--dangerously-bypass-approvals-and-sandbox".to_owned(),
            "--skip-git-repo-check".to_owned(),
            "--model".to_owned(),
            model_id.to_owned(),
            "--output-schema".to_owned(),
            schema_path.to_string_lossy().into_owned(),
            "--output-last-message".to_owned(),
            message_path.to_string_lossy().into_owned(),
            session_id.to_owned(),
            "-".to_owned(),
        ]
    }
}

impl AgentExecutionPort for ProcessBackendAdapter {
    async fn check_capability(
        &self,
        backend: &ResolvedBackendTarget,
        contract: &InvocationContract,
    ) -> AppResult<()> {
        match (backend.backend.family, contract) {
            (
                BackendFamily::Claude | BackendFamily::Codex,
                InvocationContract::Stage(_) | InvocationContract::Requirements { .. } | InvocationContract::Panel { .. },
            ) => Ok(()),
            (BackendFamily::OpenRouter | BackendFamily::Stub, _) => {
                Err(Self::capability_mismatch(
                    backend,
                    contract,
                    "ProcessBackendAdapter currently supports only claude and codex; self-hosted workflow runs require default_backend=claude or default_backend=codex",
                ))
            }
        }
    }

    async fn check_availability(&self, backend: &ResolvedBackendTarget) -> AppResult<()> {
        let Some(binary_name) = Self::binary_name(backend.backend.family) else {
            return Err(AppError::BackendUnavailable {
                backend: backend.backend.family.to_string(),
                details: "ProcessBackendAdapter availability checks are only supported for claude and codex".to_owned(),
            });
        };
        Self::ensure_binary_available(binary_name, backend.backend.family.as_str())
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        self.check_capability(&request.resolved_target, &request.contract)
            .await?;
        let prepared = self.build_command(&request).await?;
        let output = match self
            .spawn_and_wait(
                &request,
                prepared.binary(),
                prepared.args(),
                prepared.stdin_payload(),
            )
            .await
        {
            Ok(output) => output,
            Err(error) => {
                prepared.cleanup().await;
                return Err(error);
            }
        };

        match output.status {
            status if !status.success() => {
                let stderr = String::from_utf8_lossy(&output.stderr);

                // Stale session recovery: if Claude fails with "No conversation
                // found" while resuming a session, retry once without --resume.
                if request.prior_session.is_some()
                    && stderr.contains("No conversation found with session ID")
                {
                    prepared.cleanup().await;
                    let mut fresh_request = request.clone();
                    fresh_request.prior_session = None;
                    let fresh_prepared = self.build_command(&fresh_request).await?;
                    let fresh_output = match self
                        .spawn_and_wait(
                            &fresh_request,
                            fresh_prepared.binary(),
                            fresh_prepared.args(),
                            fresh_prepared.stdin_payload(),
                        )
                        .await
                    {
                        Ok(output) => output,
                        Err(error) => {
                            fresh_prepared.cleanup().await;
                            return Err(error);
                        }
                    };
                    if !fresh_output.status.success() {
                        let fresh_stderr = String::from_utf8_lossy(&fresh_output.stderr);
                        let fresh_stdout_error = extract_stdout_error(&fresh_output.stdout);
                        let code = fresh_output
                            .status
                            .code()
                            .map_or("signal".to_owned(), |c| c.to_string());
                        fresh_prepared.cleanup().await;
                        let detail = match (fresh_stderr.is_empty(), fresh_stdout_error) {
                            (false, Some(out)) => format!(": {fresh_stderr}; stdout error: {out}"),
                            (false, None) => format!(": {fresh_stderr}"),
                            (true, Some(out)) => format!(": {out}"),
                            (true, None) => String::new(),
                        };
                        return Err(Self::invocation_failed(
                            &fresh_request,
                            FailureClass::TransportFailure,
                            format!(
                                "{} exited with code {code}{detail}",
                                fresh_prepared.binary(),
                            ),
                        ));
                    }
                    return fresh_prepared.finish(&fresh_request, fresh_output).await;
                }

                let stdout_error = extract_stdout_error(&output.stdout);
                let code = status.code().map_or("signal".to_owned(), |c| c.to_string());
                prepared.cleanup().await;
                let detail = match (stderr.is_empty(), stdout_error) {
                    (false, Some(out)) => format!(": {stderr}; stdout error: {out}"),
                    (false, None) => format!(": {stderr}"),
                    (true, Some(out)) => format!(": {out}"),
                    (true, None) => String::new(),
                };
                Err(Self::invocation_failed(
                    &request,
                    FailureClass::TransportFailure,
                    format!("{} exited with code {code}{detail}", prepared.binary(),),
                ))
            }
            _ => prepared.finish(&request, output).await,
        }
    }

    async fn cancel(&self, invocation_id: &str) -> AppResult<()> {
        let Some(active_child) = self.take_active_child(invocation_id).await else {
            return Ok(());
        };

        active_child
            .send_sigterm()
            .await
            .map_err(|error| AppError::InvocationFailed {
                backend: "process".to_owned(),
                contract_id: invocation_id.to_owned(),
                failure_class: FailureClass::TransportFailure,
                details: format!("failed to send SIGTERM to invocation '{invocation_id}': {error}"),
            })?;

        match tokio::time::timeout(CANCEL_GRACE_PERIOD, active_child.wait()).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(AppError::InvocationFailed {
                backend: "process".to_owned(),
                contract_id: invocation_id.to_owned(),
                failure_class: FailureClass::TransportFailure,
                details: format!("failed to reap invocation '{invocation_id}': {error}"),
            }),
            Err(_) => {
                spawn_background_reap(invocation_id.to_owned(), active_child);
                Ok(())
            }
        }
    }
}

#[derive(Deserialize)]
struct ClaudeEnvelope {
    #[serde(default)]
    result: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    structured_output: Option<serde_json::Value>,
}

pub(crate) struct ChildOutput {
    pub(crate) status: ExitStatus,
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
}

/// Try to extract an error message from Claude's stdout JSON envelope.
/// Returns `Some(detail)` if stdout contains JSON with `is_error: true`.
fn extract_stdout_error(stdout: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(stdout).ok()?;
    if value.get("is_error")?.as_bool()? {
        value
            .get("result")
            .and_then(|r| r.as_str())
            .map(|s| s.to_owned())
    } else {
        None
    }
}

fn spawn_background_reap(invocation_id: String, child: Arc<ManagedChild>) {
    tokio::spawn(async move {
        let _ = child.send_sigkill().await;
        let _ = child.wait().await;
        drop(invocation_id);
    });
}

async fn best_effort_cleanup(schema_path: Option<&Path>, message_path: &Path) {
    if let Some(schema_path) = schema_path {
        let _ = tokio::fs::remove_file(schema_path).await;
    }
    let _ = tokio::fs::remove_file(message_path).await;
}

/// Recursively enforce OpenAI strict-mode schema requirements:
/// 1. Inject `"additionalProperties": false` on every object schema.
/// 2. Ensure `"required"` includes every key from `"properties"` — strict mode
///    rejects schemas where a property key is missing from the required array.
///
/// This is needed because `schemars` honours `#[serde(default)]` by omitting
/// the field from `required`, which is correct for general JSON Schema but
/// violates OpenAI's strict-mode contract.
pub(crate) fn enforce_strict_mode_schema(value: &mut serde_json::Value) {
    if let serde_json::Value::Object(map) = value {
        let is_object = map.get("type").and_then(|t| t.as_str()) == Some("object");
        if is_object {
            // 1. additionalProperties: false
            if !map.contains_key("additionalProperties") {
                map.insert(
                    "additionalProperties".to_owned(),
                    serde_json::Value::Bool(false),
                );
            }

            // 2. Ensure required contains every property key
            if let Some(serde_json::Value::Object(props_map)) = map.get("properties") {
                let all_keys: Vec<serde_json::Value> = props_map
                    .keys()
                    .map(|k| serde_json::Value::String(k.clone()))
                    .collect();
                match map.get_mut("required") {
                    Some(serde_json::Value::Array(required)) => {
                        for key in &all_keys {
                            if !required.contains(key) {
                                required.push(key.clone());
                            }
                        }
                    }
                    _ => {
                        map.insert("required".to_owned(), serde_json::Value::Array(all_keys));
                    }
                }
            }
        }
        // Recurse into properties
        if let Some(serde_json::Value::Object(props_map)) = map.get_mut("properties") {
            for prop_value in props_map.values_mut() {
                enforce_strict_mode_schema(prop_value);
            }
        }
        // Recurse into definitions
        if let Some(serde_json::Value::Object(defs_map)) = map.get_mut("definitions") {
            for def_value in defs_map.values_mut() {
                enforce_strict_mode_schema(def_value);
            }
        }
        // Recurse into items (for array types)
        if let Some(items) = map.get_mut("items") {
            enforce_strict_mode_schema(items);
        }
    }
}

/// Returns `true` if the given JSON value looks like a Claude CLI envelope
/// (contains `result` and/or `session_id` top-level keys) rather than a
/// contract payload. Used to guard the empty-result fallback from accidentally
/// recovering the envelope itself as the payload.
fn looks_like_claude_envelope(val: &serde_json::Value) -> bool {
    let Some(obj) = val.as_object() else {
        return false;
    };
    // The Claude envelope has `result`, `session_id`, and optionally
    // `structured_output`. If we see at least two of these three keys,
    // this is almost certainly the envelope, not a contract payload.
    let envelope_keys = ["result", "session_id", "structured_output"];
    let matches = envelope_keys
        .iter()
        .filter(|k| obj.contains_key(**k))
        .count();
    matches >= 2
}

/// Try to extract a JSON object from text that may contain surrounding prose or
/// markdown fencing. This handles cases where the Claude CLI returns the payload
/// inside `result` wrapped in markdown code blocks or conversational text.
fn extract_json_from_text(text: &str) -> Result<serde_json::Value, serde_json::Error> {
    // 1. Try direct parse first (already attempted by caller, but cheap).
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(text) {
        if val.is_object() {
            return Ok(val);
        }
    }

    // 2. Try extracting from ```json ... ``` fenced blocks.
    if let Some(start) = text.find("```json") {
        let after_fence = &text[start + 7..];
        if let Some(end) = after_fence.find("```") {
            let candidate = after_fence[..end].trim();
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(candidate) {
                if val.is_object() {
                    return Ok(val);
                }
            }
        }
    }

    // 3. Find the first balanced `{...}` substring.
    if let Some(obj_start) = text.find('{') {
        let mut depth = 0i32;
        let mut in_string = false;
        let mut escape_next = false;
        for (i, ch) in text[obj_start..].char_indices() {
            if escape_next {
                escape_next = false;
                continue;
            }
            match ch {
                '\\' if in_string => escape_next = true,
                '"' => in_string = !in_string,
                '{' if !in_string => depth += 1,
                '}' if !in_string => {
                    depth -= 1;
                    if depth == 0 {
                        let candidate = &text[obj_start..obj_start + i + 1];
                        if let Ok(val) = serde_json::from_str::<serde_json::Value>(candidate) {
                            if val.is_object() {
                                return Ok(val);
                            }
                        }
                        break;
                    }
                }
                _ => {}
            }
        }
    }

    // Nothing found — return an error via a failed parse of the original text.
    serde_json::from_str::<serde_json::Value>(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn enforce_strict_mode_adds_missing_required_fields() {
        // Simulates a schema generated by schemars for a struct with
        // #[serde(default)] fields — `follow_ups` is in properties but
        // NOT in the required array.
        let mut schema = json!({
            "type": "object",
            "properties": {
                "outcome": { "type": "string" },
                "evidence": { "type": "array", "items": { "type": "string" } },
                "findings": { "type": "array", "items": { "type": "string" } },
                "follow_ups": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["outcome", "evidence", "findings"]
        });

        enforce_strict_mode_schema(&mut schema);

        let required = schema["required"].as_array().unwrap();
        let required_strings: Vec<&str> = required.iter().map(|v| v.as_str().unwrap()).collect();
        assert!(
            required_strings.contains(&"follow_ups"),
            "follow_ups should be added to required; got: {required_strings:?}"
        );
        assert!(required_strings.contains(&"outcome"));
        assert!(required_strings.contains(&"evidence"));
        assert!(required_strings.contains(&"findings"));
        assert_eq!(schema["additionalProperties"], json!(false));
    }

    #[test]
    fn enforce_strict_mode_creates_required_when_absent() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "value": { "type": "integer" }
            }
        });

        enforce_strict_mode_schema(&mut schema);

        let required = schema["required"].as_array().unwrap();
        assert_eq!(required.len(), 2);
        assert_eq!(schema["additionalProperties"], json!(false));
    }

    #[test]
    fn enforce_strict_mode_recurses_into_nested_objects() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "source": {
                    "type": "object",
                    "properties": {
                        "mode": { "type": "string" },
                        "run_id": { "type": "string" },
                        "quick_revisions": { "type": "integer" }
                    },
                    "required": ["mode", "run_id"]
                }
            },
            "required": ["source"]
        });

        enforce_strict_mode_schema(&mut schema);

        let nested = &schema["properties"]["source"];
        let nested_required = nested["required"].as_array().unwrap();
        let nested_strings: Vec<&str> = nested_required
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert!(
            nested_strings.contains(&"quick_revisions"),
            "quick_revisions should be added to nested required; got: {nested_strings:?}"
        );
        assert_eq!(nested["additionalProperties"], json!(false));
    }

    #[test]
    fn extract_json_from_text_direct_parse() {
        let text = r#"{"project_id": "test-123", "flow": "standard"}"#;
        let val = extract_json_from_text(text).unwrap();
        assert_eq!(val["project_id"], "test-123");
    }

    #[test]
    fn extract_json_from_text_markdown_fenced() {
        let text = "Here is the output:\n```json\n{\"project_id\": \"test-456\"}\n```\nDone.";
        let val = extract_json_from_text(text).unwrap();
        assert_eq!(val["project_id"], "test-456");
    }

    #[test]
    fn extract_json_from_text_embedded_object() {
        let text =
            "The result is: {\"outcome\": \"approved\", \"evidence\": [\"ok\"]} and that's it.";
        let val = extract_json_from_text(text).unwrap();
        assert_eq!(val["outcome"], "approved");
    }

    #[test]
    fn extract_json_from_text_no_json_returns_error() {
        let text = "No JSON here at all.";
        assert!(extract_json_from_text(text).is_err());
    }

    #[test]
    fn looks_like_claude_envelope_detects_envelope() {
        let envelope = json!({
            "result": "",
            "session_id": "sess-abc123",
            "structured_output": null
        });
        assert!(looks_like_claude_envelope(&envelope));
    }

    #[test]
    fn looks_like_claude_envelope_detects_partial_envelope() {
        // Two of three envelope keys → still detected
        let partial = json!({
            "result": "some text",
            "session_id": "sess-xyz"
        });
        assert!(looks_like_claude_envelope(&partial));
    }

    #[test]
    fn looks_like_claude_envelope_rejects_contract_payload() {
        let payload = json!({
            "outcome": "approved",
            "evidence": ["test passed"],
            "findings": []
        });
        assert!(!looks_like_claude_envelope(&payload));
    }

    #[test]
    fn looks_like_claude_envelope_rejects_single_key_overlap() {
        // A payload that happens to have a "result" key but nothing else
        // from the envelope signature should NOT be rejected.
        let payload = json!({
            "result": "approved",
            "score": 95
        });
        assert!(!looks_like_claude_envelope(&payload));
    }

    // ── Integration-style tests for the full Claude finish() fallback ────

    use crate::contexts::agent_execution::model::{
        CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
    };
    use crate::shared::domain::{BackendFamily, BackendRole, ResolvedBackendTarget, SessionPolicy};
    use std::os::unix::process::ExitStatusExt;
    use std::path::PathBuf;

    fn make_test_request() -> InvocationRequest {
        InvocationRequest {
            invocation_id: "test-inv-001".to_owned(),
            project_root: PathBuf::from("/tmp/test"),
            working_dir: PathBuf::from("/tmp/test"),
            contract: InvocationContract::Requirements {
                label: "requirements:project_seed".to_owned(),
            },
            role: BackendRole::Planner,
            resolved_target: ResolvedBackendTarget::new(BackendFamily::Claude, "claude-test"),
            payload: InvocationPayload {
                prompt: "test".to_owned(),
                context: json!({}),
            },
            timeout: Duration::from_secs(60),
            cancellation_token: CancellationToken::new(),
            session_policy: SessionPolicy::NewSession,
            prior_session: None,
            attempt_number: 1,
        }
    }

    fn make_child_output(stdout: &str) -> ChildOutput {
        ChildOutput {
            status: ExitStatus::from_raw(0),
            stdout: stdout.as_bytes().to_vec(),
            stderr: Vec::new(),
        }
    }

    #[tokio::test]
    async fn finish_claude_result_raw_json_extraction() {
        // When structured_output is null and result contains raw JSON (no
        // markdown fencing), the fallback should parse it directly.
        let envelope = json!({
            "result": "{\"outcome\": \"approved\", \"evidence\": [\"test passed\"]}",
            "session_id": "sess-test-001",
            "structured_output": null
        });
        let stdout = envelope.to_string();
        let output = make_child_output(&stdout);

        let prepared = PreparedCommand {
            binary: "claude".to_owned(),
            args: vec![],
            stdin_payload: String::new(),
            response_decoder: ResponseDecoder::Claude {
                session_resuming: false,
            },
        };

        let request = make_test_request();
        let result = prepared.finish(&request, output).await.unwrap();
        assert_eq!(result.parsed_payload["outcome"], "approved");
    }

    #[tokio::test]
    async fn finish_claude_empty_result_rejects_envelope_only() {
        // When stdout contains ONLY the envelope (no separate payload),
        // the fallback should fail rather than returning the envelope.
        let envelope = json!({
            "result": "",
            "session_id": "sess-test-002",
            "structured_output": null
        });
        let stdout = envelope.to_string();
        let output = make_child_output(&stdout);

        let prepared = PreparedCommand {
            binary: "claude".to_owned(),
            args: vec![],
            stdin_payload: String::new(),
            response_decoder: ResponseDecoder::Claude {
                session_resuming: false,
            },
        };

        let request = make_test_request();
        let result = prepared.finish(&request, output).await;

        assert!(
            result.is_err(),
            "should fail when only envelope is in stdout"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("empty result"),
            "error should mention empty result: {err}"
        );
    }

    #[tokio::test]
    async fn finish_claude_structured_output_preferred() {
        // When structured_output is present, it should be used directly
        // regardless of result content.
        let envelope = json!({
            "result": "some conversational text",
            "session_id": "sess-test-003",
            "structured_output": {
                "outcome": "approved",
                "evidence": ["test passed"]
            }
        });
        let stdout = envelope.to_string();
        let output = make_child_output(&stdout);

        let prepared = PreparedCommand {
            binary: "claude".to_owned(),
            args: vec![],
            stdin_payload: String::new(),
            response_decoder: ResponseDecoder::Claude {
                session_resuming: false,
            },
        };

        let request = make_test_request();
        let result = prepared.finish(&request, output).await.unwrap();
        assert_eq!(result.parsed_payload["outcome"], "approved");
    }

    #[tokio::test]
    async fn finish_claude_result_json_extraction() {
        // When structured_output is null but result contains JSON embedded in
        // markdown fencing, the fallback should extract it.
        let envelope = json!({
            "result": "Here is the output:\n```json\n{\"outcome\": \"approved\", \"evidence\": [\"ok\"]}\n```\nDone.",
            "session_id": "sess-test-004",
            "structured_output": null
        });
        let stdout = envelope.to_string();
        let output = make_child_output(&stdout);

        let prepared = PreparedCommand {
            binary: "claude".to_owned(),
            args: vec![],
            stdin_payload: String::new(),
            response_decoder: ResponseDecoder::Claude {
                session_resuming: false,
            },
        };

        let request = make_test_request();
        let result = prepared.finish(&request, output).await.unwrap();
        assert_eq!(result.parsed_payload["outcome"], "approved");
    }
}
