use std::collections::HashMap;
use std::path::Path;
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
#[cfg(unix)]
use nix::errno::Errno;
#[cfg(unix)]
use nix::sys::signal::{self, Signal};
#[cfg(unix)]
use nix::unistd::Pid;
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
/// Grace period for confirming process teardown after timeout-triggered kill.
const TEARDOWN_GRACE_PERIOD: Duration = Duration::from_secs(2);

pub(crate) struct PreparedCommand {
    binary: std::path::PathBuf,
    args: Vec<String>,
    stdin_payload: String,
    response_decoder: ResponseDecoder,
    env_overrides: Vec<(String, String)>,
}

impl PreparedCommand {
    pub(crate) fn binary(&self) -> &std::path::Path {
        &self.binary
    }

    pub(crate) fn args(&self) -> &[String] {
        &self.args
    }

    pub(crate) fn stdin_payload(&self) -> &str {
        &self.stdin_payload
    }

    pub(crate) fn env_overrides(&self) -> &[(String, String)] {
        &self.env_overrides
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

    /// Preserve backend artifacts (schema/message temp files) into
    /// `runtime/failed` without requiring child process output.
    /// Used for timeout and spawn failures where no ChildOutput is available.
    pub(crate) async fn preserve_failed_artifacts(
        &self,
        request: &InvocationRequest,
        reason: &str,
    ) {
        let failed_dir = request.project_root.join("runtime/failed");
        let _ = tokio::fs::create_dir_all(&failed_dir).await;

        match &self.response_decoder {
            ResponseDecoder::Claude { .. } => {}
            ResponseDecoder::Codex {
                schema_path,
                message_path,
                ..
            } => {
                let failed_schema_path =
                    failed_dir.join(format!("{}.schema.json", request.invocation_id));
                let failed_message_path =
                    failed_dir.join(format!("{}.last-message.json", request.invocation_id));
                best_effort_move_file(schema_path, &failed_schema_path).await;
                best_effort_move_file(message_path, &failed_message_path).await;
            }
        }

        let failed_raw_path = failed_dir.join(format!("{}.failed.raw", request.invocation_id));
        let _ = tokio::fs::write(failed_raw_path, format!("[no child output — {reason}]\n")).await;
    }

    // Failure cleanup preserves backend artifacts for operator inspection.
    pub(crate) async fn cleanup_failed_invocation(
        &self,
        request: &InvocationRequest,
        output: &ChildOutput,
    ) {
        let failed_dir = request.project_root.join("runtime/failed");
        let _ = tokio::fs::create_dir_all(&failed_dir).await;

        match &self.response_decoder {
            ResponseDecoder::Claude { .. } => {}
            ResponseDecoder::Codex {
                schema_path,
                message_path,
                ..
            } => {
                let failed_schema_path =
                    failed_dir.join(format!("{}.schema.json", request.invocation_id));
                let failed_message_path =
                    failed_dir.join(format!("{}.last-message.json", request.invocation_id));
                best_effort_move_file(schema_path, &failed_schema_path).await;
                best_effort_move_file(message_path, &failed_message_path).await;
            }
        }

        let failed_raw_path = failed_dir.join(format!("{}.failed.raw", request.invocation_id));
        let _ = tokio::fs::write(
            failed_raw_path,
            format_failed_raw_output(&output.stdout, &output.stderr),
        )
        .await;
    }

    pub(crate) async fn finish(
        self,
        request: &InvocationRequest,
        output: ChildOutput,
    ) -> AppResult<InvocationEnvelope> {
        match &self.response_decoder {
            ResponseDecoder::Claude {
                session_resuming, ..
            } => {
                let session_resuming = *session_resuming;
                let stdout_text = String::from_utf8_lossy(&output.stdout).into_owned();

                let envelope: ClaudeEnvelope = match serde_json::from_str(&stdout_text) {
                    Ok(val) => val,
                    Err(error) => {
                        self.cleanup_failed_invocation(request, &output).await;
                        return Err(ProcessBackendAdapter::invocation_failed(
                            request,
                            FailureClass::SchemaValidationFailure,
                            format!("invalid Claude envelope JSON: {error}"),
                        ));
                    }
                };

                let parsed_payload = if let Some(structured) = envelope.structured_output {
                    structured
                } else if !envelope.result.trim().is_empty() {
                    // result is non-empty: try direct parse, then extract embedded JSON
                    match serde_json::from_str(&envelope.result)
                        .or_else(|_| extract_json_from_text(&envelope.result))
                    {
                        Ok(val) => val,
                        Err(error) => {
                            self.cleanup_failed_invocation(request, &output).await;
                            return Err(ProcessBackendAdapter::invocation_failed(
                                request,
                                FailureClass::SchemaValidationFailure,
                                format!(
                                    "invalid Claude result JSON: {error} \
                                 (contract: {}, result_len: {})",
                                    request.contract.label(),
                                    envelope.result.len(),
                                ),
                            ));
                        }
                    }
                } else {
                    // Both structured_output and result are empty.
                    // Last resort: try to find JSON in the raw stdout beyond the envelope.
                    // Guard: reject if the extracted object is just the Claude envelope
                    // itself (has `result` + `session_id` keys), which would happen when
                    // stdout contains only the envelope and no separate payload.
                    match extract_json_from_text(&stdout_text)
                        .ok()
                        .filter(|val| !looks_like_claude_envelope(val))
                    {
                        Some(val) => val,
                        None => {
                            self.cleanup_failed_invocation(request, &output).await;
                            return Err(ProcessBackendAdapter::invocation_failed(
                                request,
                                FailureClass::SchemaValidationFailure,
                                format!(
                                    "Claude returned empty result with no structured_output \
                                     (contract: {}, stdout_len: {}, session_policy: {:?})",
                                    request.contract.label(),
                                    output.stdout.len(),
                                    request.session_policy,
                                ),
                            ));
                        }
                    }
                };

                let session_id = envelope.session_id.or_else(|| {
                    if session_resuming {
                        request.prior_session.as_ref().map(|s| s.session_id.clone())
                    } else {
                        None
                    }
                });

                let token_counts = match envelope.usage {
                    Some(ref u) => {
                        let prompt = u.input_tokens;
                        let completion = u.output_tokens;
                        let total = match (prompt, completion) {
                            (Some(p), Some(c)) => p.checked_add(c),
                            _ => None,
                        };
                        TokenCounts {
                            prompt_tokens: prompt,
                            completion_tokens: completion,
                            total_tokens: total,
                            cache_read_tokens: u.cache_read_input_tokens,
                            cache_creation_tokens: u.cache_creation_input_tokens,
                        }
                    }
                    None => TokenCounts::default(),
                };

                Ok(InvocationEnvelope {
                    raw_output_reference: RawOutputReference::Inline(stdout_text),
                    parsed_payload,
                    metadata: InvocationMetadata {
                        invocation_id: request.invocation_id.clone(),
                        duration: Duration::from_millis(0),
                        token_counts,
                        backend_used: request.resolved_target.backend.clone(),
                        model_used: request.resolved_target.model.clone(),
                        adapter_reported_backend: None,
                        adapter_reported_model: None,
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
                let session_resuming = *session_resuming;
                let last_message_text = match tokio::fs::read_to_string(message_path).await {
                    Ok(text) => text,
                    Err(error) => {
                        self.cleanup_failed_invocation(request, &output).await;
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
                        self.cleanup_failed_invocation(request, &output).await;
                        return Err(ProcessBackendAdapter::invocation_failed(
                            request,
                            FailureClass::SchemaValidationFailure,
                            format!("invalid Codex last-message JSON: {error}"),
                        ));
                    }
                };

                best_effort_cleanup(Some(schema_path.as_path()), message_path.as_path()).await;

                let session_id = if session_resuming {
                    request.prior_session.as_ref().map(|s| s.session_id.clone())
                } else {
                    None
                };

                let token_counts = extract_codex_usage_from_stdout(&output.stdout);

                Ok(InvocationEnvelope {
                    raw_output_reference: RawOutputReference::Inline(last_message_text),
                    parsed_payload,
                    metadata: InvocationMetadata {
                        invocation_id: request.invocation_id.clone(),
                        duration: Duration::from_millis(0),
                        token_counts,
                        backend_used: request.resolved_target.backend.clone(),
                        model_used: request.resolved_target.model.clone(),
                        adapter_reported_backend: None,
                        adapter_reported_model: None,
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
    search_paths: Option<Vec<std::path::PathBuf>>,
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

    #[cfg(unix)]
    async fn send_signal(&self, signal: Signal) -> std::io::Result<()> {
        let Some(pid) = self.pid().await else {
            return Ok(());
        };

        let pid = i32::try_from(pid).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("process id {pid} exceeds libc::pid_t range"),
            )
        })?;

        match signal::kill(Pid::from_raw(pid), signal) {
            Ok(()) => Ok(()),
            Err(Errno::ESRCH) => Ok(()),
            Err(errno) => Err(std::io::Error::from_raw_os_error(errno as i32)),
        }
    }

    async fn send_sigterm(&self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            self.send_signal(Signal::SIGTERM).await
        }

        #[cfg(not(unix))]
        {
            if self.pid().await.is_none() {
                return Ok(());
            }

            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "SIGTERM delivery requires unix",
            ))
        }
    }

    async fn send_sigkill(&self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            self.send_signal(Signal::SIGKILL).await
        }

        #[cfg(not(unix))]
        {
            if self.pid().await.is_none() {
                return Ok(());
            }

            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "SIGKILL delivery requires unix",
            ))
        }
    }

    /// Cross-platform forceful kill.  Uses SIGKILL on Unix; on non-Unix
    /// falls back to tokio's `Child::kill()` which calls
    /// `TerminateProcess` on Windows.
    async fn force_kill(&self) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            self.send_sigkill().await
        }

        #[cfg(not(unix))]
        {
            let mut state = self.state.lock().await;
            match &mut *state {
                ManagedChildState::Running(child) => child.kill().await,
                ManagedChildState::Exited(_) => Ok(()),
            }
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
            search_paths: None,
        }
    }

    /// Create an adapter that resolves binaries from the given explicit
    /// search paths instead of the process-global `PATH`.  Relative paths
    /// are resolved against the current working directory at construction
    /// time so that later `current_dir` changes in child processes do not
    /// alter binary lookup.  When explicit paths are set, [`resolve_binary`]
    /// returns an error if no executable match is found — it never silently
    /// falls back to ambient PATH.
    pub fn with_search_paths(paths: Vec<std::path::PathBuf>) -> Self {
        let absolute_paths = Self::absolutize_paths(paths);
        Self {
            active_children: Arc::new(Mutex::new(HashMap::new())),
            search_paths: Some(absolute_paths),
        }
    }

    /// Convert a list of paths to absolute paths.  Relative entries are
    /// joined against `std::env::current_dir()`.
    ///
    /// # Panics
    ///
    /// Panics if the process working directory is inaccessible (deleted or
    /// insufficient permissions).  This is unrecoverable — if `current_dir()`
    /// fails, the entire process is in a broken state.
    fn absolutize_paths(paths: Vec<std::path::PathBuf>) -> Vec<std::path::PathBuf> {
        let has_relative = paths.iter().any(|p| !p.is_absolute());
        let cwd = if has_relative {
            Some(std::env::current_dir().expect(
                "current_dir() failed — process working directory is inaccessible; \
                 cannot absolutize relative search paths",
            ))
        } else {
            None
        };
        paths
            .into_iter()
            .map(|p| {
                if p.is_absolute() {
                    p
                } else {
                    cwd.as_ref().expect("cwd resolved above").join(p)
                }
            })
            .collect()
    }

    /// Whether this adapter was constructed with explicit search paths.
    pub(crate) fn has_explicit_search_paths(&self) -> bool {
        self.search_paths.is_some()
    }

    /// Return the search paths this adapter uses for binary resolution.
    /// If explicit search paths were provided via `with_search_paths`, those
    /// are returned; otherwise falls back to the process-global `PATH`.
    pub(crate) fn effective_search_paths(&self) -> Vec<std::path::PathBuf> {
        match &self.search_paths {
            Some(paths) => paths.clone(),
            None => Self::system_path_entries(),
        }
    }

    /// Resolve a binary name to its absolute path using this adapter's search
    /// paths.  When no explicit search paths are set, returns the bare binary
    /// name (relying on OS PATH lookup at spawn time).
    ///
    /// When explicit search paths are configured, the returned [`PathBuf`] is
    /// always absolute (because `with_search_paths` absolutizes its inputs).
    /// Uses the same executable-semantics as [`ensure_binary_available`]: on
    /// unix a candidate must be a regular file **and** have at least one
    /// execute bit set (`mode & 0o111 != 0`).  On Windows, `PATHEXT`
    /// extensions (`.exe`, `.cmd`, etc.) are probed when the bare name does
    /// not match.  Returns [`AppError::BackendUnavailable`] if no executable
    /// match is found — never silently falls back to ambient PATH resolution.
    pub(crate) fn resolve_binary(&self, binary_name: &str) -> AppResult<std::path::PathBuf> {
        if let Some(ref paths) = self.search_paths {
            for dir in paths {
                if let Some(found) = Self::probe_executable(dir, binary_name) {
                    return Ok(found);
                }
            }
            return Err(AppError::BackendUnavailable {
                backend: binary_name.to_owned(),
                details: format!(
                    "required binary '{binary_name}' not found (or not executable) in explicit search paths"
                ),
                failure_class: Some(FailureClass::BinaryNotFound),
            });
        }
        Ok(std::path::PathBuf::from(binary_name))
    }

    /// Probe a single directory for an executable named `binary_name`.
    ///
    /// Checks the exact name first, then (on Windows) tries each `PATHEXT`
    /// extension so that e.g. `claude` finds `claude.exe`.
    fn probe_executable(dir: &std::path::Path, binary_name: &str) -> Option<std::path::PathBuf> {
        let candidate = dir.join(binary_name);
        if Self::is_executable_file(&candidate) {
            return Some(candidate);
        }
        #[cfg(windows)]
        {
            for ext in Self::pathext_extensions() {
                let with_ext = dir.join(format!("{binary_name}{ext}"));
                if Self::is_executable_file(&with_ext) {
                    return Some(with_ext);
                }
            }
        }
        None
    }

    /// Return the list of executable extensions from the `PATHEXT` env var.
    #[cfg(windows)]
    fn pathext_extensions() -> Vec<String> {
        std::env::var("PATHEXT")
            .unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_owned())
            .split(';')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_owned())
            .collect()
    }

    /// Check whether `path` is a regular file that is executable.
    ///
    /// On unix this mirrors the permission check in
    /// [`ensure_binary_available`] (`mode & 0o111 != 0`).  On non-unix
    /// platforms any regular file is considered executable.
    pub(crate) fn is_executable_file(path: &std::path::Path) -> bool {
        let Ok(metadata) = std::fs::metadata(path) else {
            return false;
        };
        if !metadata.is_file() {
            return false;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            metadata.permissions().mode() & 0o111 != 0
        }
        #[cfg(not(unix))]
        {
            true
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
            BackendFamily::Codex | BackendFamily::OpenRouter => Some("codex"),
            BackendFamily::Stub => None,
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
                inline_schema_refs(&mut schema_value);
                let schema_json =
                    serde_json::to_string(&schema_value).unwrap_or_else(|_| "{}".to_owned());
                let session_resuming =
                    matches!(request.session_policy, SessionPolicy::ReuseIfAllowed)
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
                    binary: self.resolve_binary("claude")?,
                    args,
                    stdin_payload: Self::assemble_stdin(request),
                    response_decoder: ResponseDecoder::Claude { session_resuming },
                    env_overrides: Vec::new(),
                })
            }
            BackendFamily::Codex => {
                // Resolve the binary before writing temp files so that a
                // missing binary does not leave orphaned schema artifacts.
                let binary = self.resolve_binary("codex")?;

                let model_id = &request.resolved_target.model.model_id;
                let session_resuming =
                    matches!(request.session_policy, SessionPolicy::ReuseIfAllowed)
                        && request.prior_session.is_some();

                let temp_dir = request.project_root.join("runtime/temp");
                let _ = tokio::fs::create_dir_all(&temp_dir).await;

                let schema_path = temp_dir.join(format!("{}.schema.json", request.invocation_id));
                let message_path =
                    temp_dir.join(format!("{}.last-message.json", request.invocation_id));

                let mut schema_value = request.contract.json_schema_value();
                enforce_strict_mode_schema(&mut schema_value);
                inline_schema_refs(&mut schema_value);
                let schema_json =
                    serde_json::to_string_pretty(&schema_value).unwrap_or_else(|_| "{}".to_owned());

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
                    binary,
                    args,
                    stdin_payload: Self::assemble_stdin(request),
                    response_decoder: ResponseDecoder::Codex {
                        schema_path,
                        message_path,
                        session_resuming,
                    },
                    env_overrides: Vec::new(),
                })
            }
            BackendFamily::OpenRouter => {
                // Route through codex CLI with OpenRouter as the provider.
                // Resolve the binary before writing temp files so that a
                // missing binary does not leave orphaned schema artifacts.
                let binary = self.resolve_binary("codex").map_err(|e| match e {
                    AppError::BackendUnavailable {
                        details,
                        failure_class,
                        ..
                    } => AppError::BackendUnavailable {
                        backend: "openrouter".to_owned(),
                        details,
                        failure_class,
                    },
                    other => other,
                })?;

                let api_key = std::env::var("OPENROUTER_API_KEY")
                    .ok()
                    .filter(|k| !k.trim().is_empty())
                    .ok_or_else(|| {
                        Self::invocation_failed(
                            request,
                            FailureClass::BinaryNotFound,
                            "OPENROUTER_API_KEY is not set".to_owned(),
                        )
                    })?;

                let model_id = &request.resolved_target.model.model_id;
                let session_resuming =
                    matches!(request.session_policy, SessionPolicy::ReuseIfAllowed)
                        && request.prior_session.is_some();

                let temp_dir = request.project_root.join("runtime/temp");
                let _ = tokio::fs::create_dir_all(&temp_dir).await;

                let schema_path = temp_dir.join(format!("{}.schema.json", request.invocation_id));
                let message_path =
                    temp_dir.join(format!("{}.last-message.json", request.invocation_id));

                let mut schema_value = request.contract.json_schema_value();
                enforce_strict_mode_schema(&mut schema_value);
                inline_schema_refs(&mut schema_value);
                let schema_json =
                    serde_json::to_string_pretty(&schema_value).unwrap_or_else(|_| "{}".to_owned());

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
                    binary,
                    args,
                    stdin_payload: Self::assemble_stdin(request),
                    response_decoder: ResponseDecoder::Codex {
                        schema_path,
                        message_path,
                        session_resuming,
                    },
                    env_overrides: vec![
                        (
                            "OPENAI_BASE_URL".to_owned(),
                            "https://openrouter.ai/api/v1".to_owned(),
                        ),
                        ("OPENAI_API_KEY".to_owned(), api_key),
                    ],
                })
            }
            _ => Err(Self::capability_mismatch(
                &request.resolved_target,
                &request.contract,
                "ProcessBackendAdapter does not support this backend family",
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

    /// Return the directories from the system `PATH` environment variable.
    pub(crate) fn system_path_entries() -> Vec<std::path::PathBuf> {
        std::env::var_os("PATH")
            .map(|path| std::env::split_paths(&path).collect())
            .unwrap_or_default()
    }

    pub(crate) fn ensure_binary_available(
        binary_name: &str,
        backend: &str,
        search_paths: &[std::path::PathBuf],
    ) -> AppResult<()> {
        #[cfg(unix)]
        let mut non_executable_candidate = None;

        for dir in search_paths {
            // Probe bare name first, then PATHEXT extensions on Windows.
            for candidate in Self::candidate_names(dir, binary_name) {
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
        }

        #[cfg(unix)]
        if let Some(candidate) = non_executable_candidate {
            return Err(AppError::BackendUnavailable {
                backend: backend.to_owned(),
                details: format!(
                    "required binary '{binary_name}' was found at '{}' but is not executable; fix the file permissions or install a working executable on PATH",
                    candidate.display()
                ),
                failure_class: Some(FailureClass::BinaryNotFound),
            });
        }

        Err(AppError::BackendUnavailable {
            backend: backend.to_owned(),
            details: format!("required binary '{binary_name}' was not found on PATH"),
            failure_class: Some(FailureClass::BinaryNotFound),
        })
    }

    /// Generate candidate paths for a binary in a single directory: the bare
    /// name, plus each `PATHEXT` extension on Windows.
    fn candidate_names(dir: &std::path::Path, binary_name: &str) -> Vec<std::path::PathBuf> {
        #[allow(unused_mut)]
        let mut names = vec![dir.join(binary_name)];
        #[cfg(windows)]
        {
            for ext in Self::pathext_extensions() {
                names.push(dir.join(format!("{binary_name}{ext}")));
            }
        }
        names
    }

    /// Spawn a command, write stdin, register the child handle before I/O,
    /// read captured stdout/stderr, reap the child, and then deregister it.
    async fn spawn_and_wait(
        &self,
        request: &InvocationRequest,
        binary: &std::path::Path,
        args: &[String],
        stdin_payload: &str,
        env_overrides: &[(String, String)],
    ) -> AppResult<ChildOutput> {
        let mut command = Command::new(binary);
        command
            .args(args)
            .current_dir(&request.working_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        for (key, value) in env_overrides {
            command.env(key, value);
        }

        let binary_display = binary.display();
        let mut child = command.spawn().map_err(|error| {
            // spawn() returns ENOENT for several reasons: missing binary,
            // missing working directory, or missing script interpreter.
            // Only classify as BinaryNotFound when the binary itself is
            // confirmed missing; other NotFound causes are retryable.
            let failure_class = if error.kind() == std::io::ErrorKind::NotFound && !binary.exists()
            {
                FailureClass::BinaryNotFound
            } else {
                FailureClass::TransportFailure
            };
            Self::invocation_failed(
                request,
                failure_class,
                format!("failed to spawn {binary_display}: {error}"),
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

        let timeout_duration = request.timeout;
        let io_and_wait = async {
            let (stdin_result, stdout_result, stderr_result) =
                tokio::join!(stdin_future, stdout_future, stderr_future);
            let status_result = active_child.wait().await;
            (stdin_result, stdout_result, stderr_result, status_result)
        };

        let (stdin_result, stdout_result, stderr_result, status_result) =
            match tokio::time::timeout(timeout_duration, io_and_wait).await {
                Ok(results) => results,
                Err(_elapsed) => {
                    // Timeout fired — kill the child and confirm teardown
                    // before releasing the tracking handle.
                    let teardown_confirmed =
                        confirm_teardown(&active_child, TEARDOWN_GRACE_PERIOD).await;
                    self.remove_child_if_same(&request.invocation_id, &active_child)
                        .await;
                    let failure_class = if teardown_confirmed {
                        FailureClass::Timeout
                    } else {
                        // Kill was not confirmed — the hung process may still
                        // be alive, so report TransportFailure to signal an
                        // unclean timeout.
                        FailureClass::TransportFailure
                    };
                    return Err(Self::invocation_failed(
                        request,
                        failure_class,
                        format!(
                            "{binary_display} exceeded timeout of {}s{}",
                            timeout_duration.as_secs(),
                            if teardown_confirmed {
                                ""
                            } else {
                                " (teardown not confirmed)"
                            },
                        ),
                    ));
                }
            };

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
                    "failed to write stdin to {binary_display}: {error}{}",
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
                format!("failed to read stdout from {binary_display}: {error}"),
            )
        })?;

        let stderr = stderr_result.map_err(|error| {
            Self::invocation_failed(
                request,
                FailureClass::TransportFailure,
                format!("failed to read stderr from {binary_display}: {error}"),
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
                format!("failed to wait on {binary_display} process: {error}"),
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
    fn enforces_timeout(&self) -> bool {
        true
    }

    async fn check_capability(
        &self,
        backend: &ResolvedBackendTarget,
        contract: &InvocationContract,
    ) -> AppResult<()> {
        match (backend.backend.family, contract) {
            (
                BackendFamily::Claude | BackendFamily::Codex | BackendFamily::OpenRouter,
                InvocationContract::Stage(_)
                | InvocationContract::Requirements { .. }
                | InvocationContract::Panel { .. },
            ) => Ok(()),
            (BackendFamily::Stub, _) => Err(Self::capability_mismatch(
                backend,
                contract,
                "ProcessBackendAdapter does not support the stub backend",
            )),
        }
    }

    async fn check_availability(&self, backend: &ResolvedBackendTarget) -> AppResult<()> {
        let Some(binary_name) = Self::binary_name(backend.backend.family) else {
            return Err(AppError::BackendUnavailable {
                backend: backend.backend.family.to_string(),
                details: "ProcessBackendAdapter availability checks are only supported for claude, codex, and openrouter".to_owned(),
                failure_class: Some(FailureClass::BinaryNotFound),
            });
        };
        Self::ensure_binary_available(
            binary_name,
            backend.backend.family.as_str(),
            &self.effective_search_paths(),
        )?;
        // A missing API key is a fatal infrastructure problem (like a missing
        // binary) that won't resolve between retry attempts.  BinaryNotFound
        // is reused here as the terminal "infrastructure prerequisite missing"
        // signal rather than introducing a dedicated variant.
        if backend.backend.family == BackendFamily::OpenRouter
            && std::env::var("OPENROUTER_API_KEY")
                .unwrap_or_default()
                .trim()
                .is_empty()
        {
            return Err(AppError::BackendUnavailable {
                backend: "openrouter".to_owned(),
                details: "OPENROUTER_API_KEY is not set".to_owned(),
                failure_class: Some(FailureClass::BinaryNotFound),
            });
        }
        Ok(())
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
                prepared.env_overrides(),
            )
            .await
        {
            Ok(output) => output,
            Err(error) => {
                if error.failure_class() == Some(FailureClass::Timeout) {
                    prepared
                        .preserve_failed_artifacts(&request, "process timed out")
                        .await;
                } else {
                    prepared.cleanup().await;
                }
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
                    // Do NOT preserve failure artifacts here — this is an internal
                    // retry, not a terminal failure. If the retry succeeds, we don't
                    // want stale artifacts left in runtime/failed. Cleanup deletes the
                    // original attempt's temp files normally.
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
                            fresh_prepared.env_overrides(),
                        )
                        .await
                    {
                        Ok(output) => output,
                        Err(error) => {
                            if error.failure_class() == Some(FailureClass::Timeout) {
                                fresh_prepared
                                    .preserve_failed_artifacts(
                                        &fresh_request,
                                        "process timed out (stale session retry)",
                                    )
                                    .await;
                            } else {
                                fresh_prepared.cleanup().await;
                            }
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
                        let failure_class = classify_exit_failure(fresh_output.status);
                        fresh_prepared
                            .cleanup_failed_invocation(&fresh_request, &fresh_output)
                            .await;
                        let detail = match (fresh_stderr.is_empty(), fresh_stdout_error) {
                            (false, Some(out)) => format!(": {fresh_stderr}; stdout error: {out}"),
                            (false, None) => format!(": {fresh_stderr}"),
                            (true, Some(out)) => format!(": {out}"),
                            (true, None) => String::new(),
                        };
                        return Err(Self::invocation_failed(
                            &fresh_request,
                            failure_class,
                            format!(
                                "{} exited with code {code}{detail}",
                                fresh_prepared.binary().display(),
                            ),
                        ));
                    }
                    return fresh_prepared.finish(&fresh_request, fresh_output).await;
                }

                let stdout_error = extract_stdout_error(&output.stdout);
                let code = status.code().map_or("signal".to_owned(), |c| c.to_string());
                let failure_class = classify_exit_failure(status);
                prepared.cleanup_failed_invocation(&request, &output).await;
                let detail = match (stderr.is_empty(), stdout_error) {
                    (false, Some(out)) => format!(": {stderr}; stdout error: {out}"),
                    (false, None) => format!(": {stderr}"),
                    (true, Some(out)) => format!(": {out}"),
                    (true, None) => String::new(),
                };
                Err(Self::invocation_failed(
                    &request,
                    failure_class,
                    format!(
                        "{} exited with code {code}{detail}",
                        prepared.binary().display(),
                    ),
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
struct ClaudeUsage {
    #[serde(alias = "tokens_in")]
    input_tokens: Option<u32>,
    #[serde(alias = "tokens_out")]
    output_tokens: Option<u32>,
    #[serde(alias = "cached_in")]
    cache_read_input_tokens: Option<u32>,
    cache_creation_input_tokens: Option<u32>,
}

#[derive(Deserialize)]
struct ClaudeEnvelope {
    #[serde(default)]
    result: String,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    structured_output: Option<serde_json::Value>,
    #[serde(default)]
    usage: Option<ClaudeUsage>,
}

pub(crate) struct ChildOutput {
    pub(crate) status: ExitStatus,
    pub(crate) stdout: Vec<u8>,
    pub(crate) stderr: Vec<u8>,
}

/// Extract token usage from Codex CLI NDJSON stdout.
///
/// Codex emits newline-delimited JSON events to stdout. We scan lines in
/// reverse looking for the last event containing a `usage` object with at
/// least one recognized token field (`input_tokens`, `output_tokens`, or
/// `cached_input_tokens`). Lines where `usage` is null, non-object, or
/// contains none of the recognized fields are skipped so they don't shadow
/// a valid earlier record.
fn extract_codex_usage_from_stdout(stdout: &[u8]) -> TokenCounts {
    let text = String::from_utf8_lossy(stdout);
    for line in text.lines().rev() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let Some(usage) = value.get("usage").filter(|u| u.is_object()) else {
            continue;
        };
        let input = usage
            .get("input_tokens")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok());
        let output = usage
            .get("output_tokens")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok());
        let cached = usage
            .get("cached_input_tokens")
            .and_then(|v| v.as_u64())
            .and_then(|v| u32::try_from(v).ok());
        if input.is_none() && output.is_none() && cached.is_none() {
            continue;
        }
        let total = match (input, output) {
            (Some(i), Some(o)) => i.checked_add(o),
            _ => None,
        };
        return TokenCounts {
            prompt_tokens: input,
            completion_tokens: output,
            total_tokens: total,
            cache_read_tokens: cached,
            cache_creation_tokens: None,
        };
    }
    TokenCounts::default()
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

/// Classify process exit status into the appropriate failure class.
///
/// Exit code 127 conventionally means "command not found" and is treated as
/// `BinaryNotFound` (terminal) so the retry loop does not waste attempts on
/// a missing binary.  All other non-zero exits and signal kills map to
/// `TransportFailure` (retryable).
pub(crate) fn classify_exit_failure(status: ExitStatus) -> FailureClass {
    match status.code() {
        Some(127) => FailureClass::BinaryNotFound,
        _ => FailureClass::TransportFailure,
    }
}

fn spawn_background_reap(invocation_id: String, child: Arc<ManagedChild>) {
    tokio::spawn(async move {
        let _ = child.force_kill().await;
        let _ = child.wait().await;
        drop(invocation_id);
    });
}

/// Attempt to force-kill the child and wait for exit within `grace_period`.
/// Returns `true` if teardown was confirmed, `false` if kill or wait failed.
async fn confirm_teardown(child: &Arc<ManagedChild>, grace_period: Duration) -> bool {
    if child.force_kill().await.is_err() {
        return false;
    }
    tokio::time::timeout(grace_period, child.wait())
        .await
        .is_ok_and(|r| r.is_ok())
}

async fn best_effort_cleanup(schema_path: Option<&Path>, message_path: &Path) {
    if let Some(schema_path) = schema_path {
        let _ = tokio::fs::remove_file(schema_path).await;
    }
    let _ = tokio::fs::remove_file(message_path).await;
}

async fn best_effort_move_file(source: &Path, destination: &Path) {
    match tokio::fs::rename(source, destination).await {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => {
            if tokio::fs::copy(source, destination).await.is_ok() {
                let _ = tokio::fs::remove_file(source).await;
            }
        }
    }
}

fn format_failed_raw_output(stdout: &[u8], stderr: &[u8]) -> Vec<u8> {
    let mut combined = Vec::with_capacity(stdout.len() + stderr.len() + 32);
    combined.extend_from_slice(b"=== stdout ===\n");
    combined.extend_from_slice(stdout);
    if !combined.ends_with(b"\n") {
        combined.push(b'\n');
    }
    combined.extend_from_slice(b"=== stderr ===\n");
    combined.extend_from_slice(stderr);
    if !combined.ends_with(b"\n") {
        combined.push(b'\n');
    }
    combined
}

/// Rewrite `{"type": ["T", "null"], ...props}` → `{"anyOf": [{"type": "T", ...props}, {"type": "null"}]}`.
/// Leaves schemas with single-string `type` (e.g., `"array"`, `"integer"`) unchanged.
fn normalize_nullable_type_array(map: &mut serde_json::Map<String, serde_json::Value>) {
    let has_null_in_type_array = match map.get("type") {
        Some(serde_json::Value::Array(arr)) => arr.iter().any(|v| v.as_str() == Some("null")),
        _ => return,
    };
    if !has_null_in_type_array {
        return;
    }

    // Extract type array
    let types = match map.remove("type") {
        Some(serde_json::Value::Array(arr)) => arr,
        _ => return,
    };
    let non_null_types: Vec<serde_json::Value> = types
        .into_iter()
        .filter(|v| v.as_str() != Some("null"))
        .collect();

    // Build non-null variant: carry over remaining schema properties (format, minimum, etc.)
    let mut non_null_variant = std::mem::take(map);

    // Strip schema-wide annotations that don't belong on the non-null variant.
    // schemars 0.8 emits `"default": null` for defaulted `Option<T>` fields — placing
    // that on the non-null arm would produce `{"type":"string","default":null}` which
    // is not the intended strict-mode shape.
    if non_null_variant.get("default") == Some(&serde_json::Value::Null) {
        non_null_variant.remove("default");
    }

    if non_null_types.len() == 1 {
        non_null_variant.insert(
            "type".to_owned(),
            non_null_types.into_iter().next().unwrap(),
        );
        map.insert(
            "anyOf".to_owned(),
            serde_json::Value::Array(vec![
                serde_json::Value::Object(non_null_variant),
                serde_json::json!({"type": "null"}),
            ]),
        );
    } else {
        // Multiple non-null types: create a separate anyOf arm per type to
        // avoid producing `{"type": ["string", "integer"]}` which violates
        // strict mode's scalar-type requirement.
        let mut arms: Vec<serde_json::Value> = non_null_types
            .into_iter()
            .map(|t| {
                let mut arm = non_null_variant.clone();
                arm.insert("type".to_owned(), t);
                serde_json::Value::Object(arm)
            })
            .collect();
        arms.push(serde_json::json!({"type": "null"}));
        map.insert("anyOf".to_owned(), serde_json::Value::Array(arms));
    }
}

/// Recursively enforce OpenAI strict-mode schema requirements:
/// 1. Normalize schemars nullable type arrays (`["T", "null"]`) into `anyOf` format.
/// 2. Inject `"additionalProperties": false` on every object schema.
/// 3. Ensure `"required"` includes every key from `"properties"` — strict mode
///    rejects schemas where a property key is missing from the required array.
/// 4. Recurse into `anyOf`/`oneOf`/`allOf` composition arrays.
///
/// This is needed because `schemars` honours `#[serde(default)]` by omitting
/// the field from `required`, which is correct for general JSON Schema but
/// violates OpenAI's strict-mode contract. Additionally, `schemars` 0.8
/// represents `Option<T>` as `{"type": ["T", "null"]}` which is incompatible
/// with OpenAI strict mode's requirement for single-string `type` values.
pub(crate) fn enforce_strict_mode_schema(value: &mut serde_json::Value) {
    if let serde_json::Value::Object(map) = value {
        // Convert type arrays like ["string", "null"] to anyOf format
        normalize_nullable_type_array(map);

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
        // Recurse into anyOf/oneOf/allOf composition arrays
        for keyword in ["anyOf", "oneOf", "allOf"] {
            if let Some(serde_json::Value::Array(variants)) = map.get_mut(keyword) {
                for variant in variants.iter_mut() {
                    enforce_strict_mode_schema(variant);
                }
            }
        }
    }
}

/// Resolve all `{"$ref": "#/definitions/X"}` references in-place by substituting
/// the referenced definition object, then remove the top-level `definitions` key.
///
/// This is called after `enforce_strict_mode_schema` so that inlined definitions
/// are already strict-mode-compliant. The function handles nested refs transitively
/// and leaves unresolvable refs (missing target) unchanged.
pub(crate) fn inline_schema_refs(value: &mut serde_json::Value) {
    let Some(map) = value.as_object_mut() else {
        return;
    };
    // Check that `definitions` is present and is an object before removing it.
    // Non-object `definitions` (e.g. array, string, number) must leave the
    // schema completely unchanged (AC 6).
    let Some(definitions) = map
        .get("definitions")
        .and_then(serde_json::Value::as_object)
        .cloned()
    else {
        return;
    };
    map.remove("definitions");
    let mut expanding = std::collections::HashSet::new();
    resolve_refs(value, &definitions, &mut expanding);
}

fn resolve_refs(
    node: &mut serde_json::Value,
    definitions: &serde_json::Map<String, serde_json::Value>,
    expanding: &mut std::collections::HashSet<String>,
) {
    if let serde_json::Value::Object(map) = node {
        // Check if this node is a `$ref` object: exactly one key `"$ref"` with a
        // string value starting with `"#/definitions/"`.
        if map.len() == 1 {
            if let Some(serde_json::Value::String(ref_str)) = map.get("$ref") {
                if let Some(name) = ref_str.strip_prefix("#/definitions/") {
                    // Cycle guard: if this definition is already being expanded
                    // up the call stack, leave the $ref unresolved to prevent
                    // infinite recursion (spec: recursive refs left unresolved).
                    if expanding.contains(name) {
                        return;
                    }
                    if let Some(def) = definitions.get(name) {
                        let mut replacement = def.clone();
                        expanding.insert(name.to_owned());
                        resolve_refs(&mut replacement, definitions, expanding);
                        expanding.remove(name);
                        *node = replacement;
                        return;
                    }
                }
            }
        }
        // Recurse into all values
        for val in map.values_mut() {
            resolve_refs(val, definitions, expanding);
        }
    } else if let serde_json::Value::Array(arr) = node {
        for item in arr.iter_mut() {
            resolve_refs(item, definitions, expanding);
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
    use tempfile::tempdir;

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
    fn enforce_strict_mode_normalizes_nullable_type_array() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "rationale": { "type": ["string", "null"] }
            },
            "required": ["name"]
        });

        enforce_strict_mode_schema(&mut schema);

        let required = schema["required"].as_array().unwrap();
        let required_strings: Vec<&str> = required.iter().map(|v| v.as_str().unwrap()).collect();
        assert!(required_strings.contains(&"rationale"));
        assert!(required_strings.contains(&"name"));

        // rationale should be rewritten to anyOf format
        let rationale = &schema["properties"]["rationale"];
        assert!(
            rationale.get("type").is_none(),
            "type key should be removed"
        );
        let any_of = rationale["anyOf"].as_array().unwrap();
        assert_eq!(any_of.len(), 2);
        assert_eq!(any_of[0], json!({"type": "string"}));
        assert_eq!(any_of[1], json!({"type": "null"}));
    }

    #[test]
    fn enforce_strict_mode_preserves_format_on_nullable_integer() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "count": { "type": ["integer", "null"], "format": "uint32", "minimum": 0 }
            },
            "required": []
        });

        enforce_strict_mode_schema(&mut schema);

        let count = &schema["properties"]["count"];
        let any_of = count["anyOf"].as_array().unwrap();
        assert_eq!(any_of.len(), 2);
        assert_eq!(any_of[0]["type"], "integer");
        assert_eq!(any_of[0]["format"], "uint32");
        assert_eq!(any_of[0]["minimum"], 0);
        assert_eq!(any_of[1], json!({"type": "null"}));
    }

    #[test]
    fn enforce_strict_mode_normalizes_nullable_multi_type_array() {
        // Regression: a type array with multiple non-null types like
        // ["string", "integer", "null"] must NOT produce {"type": ["string", "integer"]}
        // in the non-null arm — that still violates strict mode's scalar-type rule.
        // Instead, each non-null type gets its own anyOf arm.
        let mut schema = json!({
            "type": "object",
            "properties": {
                "value": { "type": ["string", "integer", "null"], "format": "custom" }
            },
            "required": []
        });

        enforce_strict_mode_schema(&mut schema);

        let value = &schema["properties"]["value"];
        assert!(value.get("type").is_none(), "type key should be removed");
        let any_of = value["anyOf"].as_array().unwrap();
        assert_eq!(
            any_of.len(),
            3,
            "should have one arm per non-null type plus null"
        );
        assert_eq!(any_of[0]["type"], "string");
        assert_eq!(any_of[0]["format"], "custom");
        assert_eq!(any_of[1]["type"], "integer");
        assert_eq!(any_of[1]["format"], "custom");
        assert_eq!(any_of[2], json!({"type": "null"}));
    }

    #[test]
    fn enforce_strict_mode_leaves_non_nullable_defaults_unchanged() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "outcome": { "type": "string" },
                "follow_ups": { "type": "array", "items": { "type": "string" } },
                "version": { "type": "integer" }
            },
            "required": ["outcome"]
        });

        enforce_strict_mode_schema(&mut schema);

        let required = schema["required"].as_array().unwrap();
        let required_strings: Vec<&str> = required.iter().map(|v| v.as_str().unwrap()).collect();
        assert!(required_strings.contains(&"follow_ups"));
        assert!(required_strings.contains(&"version"));

        // follow_ups schema unchanged — no anyOf, no null
        assert_eq!(schema["properties"]["follow_ups"]["type"], "array");
        assert!(schema["properties"]["follow_ups"].get("anyOf").is_none());

        // version schema unchanged
        assert_eq!(schema["properties"]["version"]["type"], "integer");
        assert!(schema["properties"]["version"].get("anyOf").is_none());
    }

    #[test]
    fn enforce_strict_mode_recurses_into_one_of() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "producer": {
                    "oneOf": [
                        {
                            "type": "object",
                            "properties": {
                                "type": { "type": "string" },
                                "model": { "type": "string" }
                            },
                            "required": ["type"]
                        },
                        {
                            "type": "object",
                            "properties": {
                                "type": { "type": "string" },
                                "path": { "type": "string" }
                            },
                            "required": ["type"]
                        }
                    ]
                }
            },
            "required": ["producer"]
        });

        enforce_strict_mode_schema(&mut schema);

        let one_of = schema["properties"]["producer"]["oneOf"]
            .as_array()
            .unwrap();
        for (i, variant) in one_of.iter().enumerate() {
            assert_eq!(
                variant["additionalProperties"],
                json!(false),
                "variant {i} should have additionalProperties: false"
            );
            let req = variant["required"].as_array().unwrap();
            assert!(
                req.len() == 2,
                "variant {i} should have all properties in required, got {req:?}"
            );
        }
    }

    #[test]
    fn enforce_strict_mode_final_review_proposal_round_trip() {
        // Simulates the schemars output for FinalReviewProposalPayload
        let mut schema = json!({
            "type": "object",
            "definitions": {
                "FinalReviewProposal": {
                    "type": "object",
                    "properties": {
                        "body": { "type": "string" },
                        "rationale": { "default": null, "type": ["string", "null"] }
                    },
                    "required": ["body"]
                }
            },
            "properties": {
                "amendments": {
                    "type": "array",
                    "items": { "$ref": "#/definitions/FinalReviewProposal" }
                }
            },
            "required": ["amendments"]
        });

        enforce_strict_mode_schema(&mut schema);

        // Definition should be enforced
        let def = &schema["definitions"]["FinalReviewProposal"];
        assert_eq!(def["additionalProperties"], json!(false));

        let def_required = def["required"].as_array().unwrap();
        let def_req_strings: Vec<&str> = def_required.iter().map(|v| v.as_str().unwrap()).collect();
        assert!(def_req_strings.contains(&"body"));
        assert!(def_req_strings.contains(&"rationale"));

        // rationale should be anyOf, not type array
        let rationale = &def["properties"]["rationale"];
        assert!(rationale.get("type").is_none());
        let any_of = rationale["anyOf"].as_array().unwrap();
        assert_eq!(any_of[0], json!({"type": "string"}));
        assert_eq!(any_of[1], json!({"type": "null"}));
    }

    #[test]
    fn enforce_strict_mode_strips_default_null_from_non_null_variant() {
        // Regression: schemars 0.8 emits `"default": null` alongside type arrays
        // for defaulted `Option<T>` fields. The non-null variant must NOT carry
        // `"default": null` — that would produce `{"type":"string","default":null}`.
        let mut schema = json!({
            "type": "object",
            "properties": {
                "rationale": {
                    "default": null,
                    "type": ["string", "null"]
                }
            },
            "required": []
        });

        enforce_strict_mode_schema(&mut schema);

        let rationale = &schema["properties"]["rationale"];
        let any_of = rationale["anyOf"].as_array().unwrap();
        assert_eq!(any_of.len(), 2);
        // Non-null variant must NOT have "default": null
        assert!(
            any_of[0].get("default").is_none(),
            "non-null variant should not carry 'default: null'; got: {}",
            any_of[0]
        );
        assert_eq!(any_of[0], json!({"type": "string"}));
        assert_eq!(any_of[1], json!({"type": "null"}));
        // "default" should not appear at wrapper level either
        assert!(rationale.get("default").is_none());
    }

    #[test]
    fn enforce_strict_mode_preserves_non_null_default() {
        // A non-null default (e.g., "default": 0 on a nullable integer) should
        // be preserved on the non-null variant since it's meaningful.
        let mut schema = json!({
            "type": "object",
            "properties": {
                "count": {
                    "default": 0,
                    "type": ["integer", "null"],
                    "format": "uint32"
                }
            },
            "required": []
        });

        enforce_strict_mode_schema(&mut schema);

        let count = &schema["properties"]["count"];
        let any_of = count["anyOf"].as_array().unwrap();
        assert_eq!(any_of[0]["type"], "integer");
        assert_eq!(any_of[0]["default"], 0);
        assert_eq!(any_of[0]["format"], "uint32");
    }

    // ── inline_schema_refs tests ──────────────────────────────────────────────

    #[test]
    fn inline_schema_refs_resolves_simple_ref() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "action": { "$ref": "#/definitions/Action" }
            },
            "definitions": {
                "Action": {
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" }
                    },
                    "additionalProperties": false,
                    "required": ["name"]
                }
            }
        });

        inline_schema_refs(&mut schema);

        // $ref replaced with the definition body
        assert_eq!(
            schema["properties"]["action"],
            json!({
                "type": "object",
                "properties": { "name": { "type": "string" } },
                "additionalProperties": false,
                "required": ["name"]
            })
        );
        // definitions key removed
        assert!(schema.get("definitions").is_none());
    }

    #[test]
    fn inline_schema_refs_resolves_nested_refs() {
        let mut schema = json!({
            "type": "object",
            "properties": {
                "outer": { "$ref": "#/definitions/Outer" }
            },
            "definitions": {
                "Outer": {
                    "type": "object",
                    "properties": {
                        "inner": { "$ref": "#/definitions/Inner" }
                    },
                    "additionalProperties": false,
                    "required": ["inner"]
                },
                "Inner": {
                    "type": "object",
                    "properties": {
                        "value": { "type": "integer" }
                    },
                    "additionalProperties": false,
                    "required": ["value"]
                }
            }
        });

        inline_schema_refs(&mut schema);

        // Both refs resolved transitively
        assert_eq!(
            schema["properties"]["outer"]["properties"]["inner"],
            json!({
                "type": "object",
                "properties": { "value": { "type": "integer" } },
                "additionalProperties": false,
                "required": ["value"]
            })
        );
        assert!(schema.get("definitions").is_none());
    }

    #[test]
    fn inline_schema_refs_no_op_without_refs() {
        let original = json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" },
                "count": { "type": "integer" }
            },
            "required": ["name", "count"],
            "additionalProperties": false
        });
        let mut schema = original.clone();

        inline_schema_refs(&mut schema);

        assert_eq!(schema, original);
    }

    #[test]
    fn inline_schema_refs_handles_edge_cases() {
        // Sub-case 1: $ref to missing definition — node left unchanged
        let mut schema = json!({
            "type": "object",
            "properties": {
                "item": { "$ref": "#/definitions/Missing" }
            },
            "definitions": {
                "Other": { "type": "string" }
            }
        });
        inline_schema_refs(&mut schema);
        assert_eq!(
            schema["properties"]["item"],
            json!({ "$ref": "#/definitions/Missing" })
        );
        assert!(schema.get("definitions").is_none());

        // Sub-case 2: definitions is a non-object — schema must be completely unchanged
        let mut schema = json!({
            "type": "object",
            "properties": { "x": { "type": "string" } },
            "definitions": 42
        });
        let original = schema.clone();
        inline_schema_refs(&mut schema);
        assert_eq!(schema, original);

        // Sub-case 3: definitions present but no $ref anywhere
        let mut schema = json!({
            "type": "object",
            "properties": { "y": { "type": "boolean" } },
            "definitions": {
                "Unused": { "type": "string" }
            }
        });
        inline_schema_refs(&mut schema);
        assert!(schema.get("definitions").is_none());
        assert_eq!(schema["properties"]["y"], json!({ "type": "boolean" }));
    }

    #[test]
    fn inline_schema_refs_handles_self_referential_definition() {
        // A definition that references itself should not cause infinite recursion.
        // The self-referential $ref should be left unresolved.
        let mut schema = json!({
            "type": "object",
            "properties": {
                "tree": { "$ref": "#/definitions/Node" }
            },
            "definitions": {
                "Node": {
                    "type": "object",
                    "properties": {
                        "value": { "type": "string" },
                        "child": { "$ref": "#/definitions/Node" }
                    }
                }
            }
        });
        inline_schema_refs(&mut schema);

        // Top-level $ref should be resolved to the Node definition body
        assert!(schema.get("definitions").is_none());
        assert_eq!(schema["properties"]["tree"]["type"], "object");
        assert_eq!(
            schema["properties"]["tree"]["properties"]["value"],
            json!({ "type": "string" })
        );
        // The nested self-reference should be left as an unresolved $ref
        assert_eq!(
            schema["properties"]["tree"]["properties"]["child"],
            json!({ "$ref": "#/definitions/Node" })
        );
    }

    #[test]
    fn inline_schema_refs_handles_mutually_recursive_definitions() {
        // Two definitions that reference each other should not cause infinite recursion.
        let mut schema = json!({
            "type": "object",
            "properties": {
                "a": { "$ref": "#/definitions/A" }
            },
            "definitions": {
                "A": {
                    "type": "object",
                    "properties": {
                        "b": { "$ref": "#/definitions/B" }
                    }
                },
                "B": {
                    "type": "object",
                    "properties": {
                        "a": { "$ref": "#/definitions/A" }
                    }
                }
            }
        });
        inline_schema_refs(&mut schema);

        assert!(schema.get("definitions").is_none());
        // A is resolved
        assert_eq!(schema["properties"]["a"]["type"], "object");
        // B inside A is resolved
        assert_eq!(
            schema["properties"]["a"]["properties"]["b"]["type"],
            "object"
        );
        // A inside B is left unresolved (cycle: A -> B -> A)
        assert_eq!(
            schema["properties"]["a"]["properties"]["b"]["properties"]["a"],
            json!({ "$ref": "#/definitions/A" })
        );
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

    fn make_codex_test_request(project_root: PathBuf) -> InvocationRequest {
        InvocationRequest {
            invocation_id: "test-inv-001".to_owned(),
            working_dir: project_root.clone(),
            project_root,
            contract: InvocationContract::Requirements {
                label: "requirements:project_seed".to_owned(),
            },
            role: BackendRole::Implementer,
            resolved_target: ResolvedBackendTarget::new(BackendFamily::Codex, "codex-test"),
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

    fn make_failed_child_output(stdout: &str, stderr: &str) -> ChildOutput {
        ChildOutput {
            status: ExitStatus::from_raw(1 << 8),
            stdout: stdout.as_bytes().to_vec(),
            stderr: stderr.as_bytes().to_vec(),
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
            binary: "claude".into(),
            args: vec![],
            stdin_payload: String::new(),
            response_decoder: ResponseDecoder::Claude {
                session_resuming: false,
            },
            env_overrides: Vec::new(),
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
            binary: "claude".into(),
            args: vec![],
            stdin_payload: String::new(),
            response_decoder: ResponseDecoder::Claude {
                session_resuming: false,
            },
            env_overrides: Vec::new(),
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
            binary: "claude".into(),
            args: vec![],
            stdin_payload: String::new(),
            response_decoder: ResponseDecoder::Claude {
                session_resuming: false,
            },
            env_overrides: Vec::new(),
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
            binary: "claude".into(),
            args: vec![],
            stdin_payload: String::new(),
            response_decoder: ResponseDecoder::Claude {
                session_resuming: false,
            },
            env_overrides: Vec::new(),
        };

        let request = make_test_request();
        let result = prepared.finish(&request, output).await.unwrap();
        assert_eq!(result.parsed_payload["outcome"], "approved");
    }

    #[tokio::test]
    async fn cleanup_failed_invocation_moves_codex_artifacts_and_writes_raw_output() {
        let project_dir = tempdir().unwrap();
        let runtime_temp = project_dir.path().join("runtime/temp");
        std::fs::create_dir_all(&runtime_temp).unwrap();

        let schema_path = runtime_temp.join("test-inv-001.schema.json");
        let message_path = runtime_temp.join("test-inv-001.last-message.json");
        std::fs::write(&schema_path, "{\"type\":\"object\"}").unwrap();
        std::fs::write(&message_path, "{\"outcome\":\"failed\"}").unwrap();

        let prepared = PreparedCommand {
            binary: "codex".into(),
            args: vec![],
            stdin_payload: String::new(),
            response_decoder: ResponseDecoder::Codex {
                schema_path: schema_path.clone(),
                message_path: message_path.clone(),
                session_resuming: false,
            },
            env_overrides: Vec::new(),
        };

        let request = make_codex_test_request(project_dir.path().to_path_buf());
        let output = make_failed_child_output("stdout-body", "stderr-body");
        prepared.cleanup_failed_invocation(&request, &output).await;

        let failed_dir = project_dir.path().join("runtime/failed");
        assert!(!schema_path.exists());
        assert!(!message_path.exists());
        assert_eq!(
            std::fs::read_to_string(failed_dir.join("test-inv-001.schema.json")).unwrap(),
            "{\"type\":\"object\"}"
        );
        assert_eq!(
            std::fs::read_to_string(failed_dir.join("test-inv-001.last-message.json")).unwrap(),
            "{\"outcome\":\"failed\"}"
        );
        assert_eq!(
            std::fs::read_to_string(failed_dir.join("test-inv-001.failed.raw")).unwrap(),
            "=== stdout ===\nstdout-body\n=== stderr ===\nstderr-body\n"
        );
    }

    #[tokio::test]
    async fn cleanup_failed_invocation_keeps_codex_temp_files_if_move_fails() {
        let project_dir = tempdir().unwrap();
        let runtime_temp = project_dir.path().join("runtime/temp");
        let failed_path = project_dir.path().join("runtime/failed");
        std::fs::create_dir_all(&runtime_temp).unwrap();
        std::fs::create_dir_all(failed_path.parent().unwrap()).unwrap();
        std::fs::write(&failed_path, "not-a-directory").unwrap();

        let schema_path = runtime_temp.join("test-inv-001.schema.json");
        let message_path = runtime_temp.join("test-inv-001.last-message.json");
        std::fs::write(&schema_path, "{\"type\":\"object\"}").unwrap();
        std::fs::write(&message_path, "{\"outcome\":\"failed\"}").unwrap();

        let prepared = PreparedCommand {
            binary: "codex".into(),
            args: vec![],
            stdin_payload: String::new(),
            response_decoder: ResponseDecoder::Codex {
                schema_path: schema_path.clone(),
                message_path: message_path.clone(),
                session_resuming: false,
            },
            env_overrides: Vec::new(),
        };

        let request = make_codex_test_request(project_dir.path().to_path_buf());
        let output = make_failed_child_output("stdout-body", "stderr-body");
        prepared.cleanup_failed_invocation(&request, &output).await;

        assert!(schema_path.exists());
        assert!(message_path.exists());
        assert!(!failed_path.join("test-inv-001.failed.raw").exists());
    }

    #[tokio::test]
    async fn cleanup_deletes_codex_temp_files_on_success() {
        let project_dir = tempdir().unwrap();
        let runtime_temp = project_dir.path().join("runtime/temp");
        std::fs::create_dir_all(&runtime_temp).unwrap();

        let schema_path = runtime_temp.join("test-inv-001.schema.json");
        let message_path = runtime_temp.join("test-inv-001.last-message.json");
        std::fs::write(&schema_path, "{\"type\":\"object\"}").unwrap();
        std::fs::write(&message_path, "{\"outcome\":\"ok\"}").unwrap();

        let prepared = PreparedCommand {
            binary: "codex".into(),
            args: vec![],
            stdin_payload: String::new(),
            response_decoder: ResponseDecoder::Codex {
                schema_path: schema_path.clone(),
                message_path: message_path.clone(),
                session_resuming: false,
            },
            env_overrides: Vec::new(),
        };

        prepared.cleanup().await;

        assert!(!schema_path.exists());
        assert!(!message_path.exists());
        assert!(!project_dir.path().join("runtime/failed").exists());
    }

    // ── Integration: build_command produces ref-free schemas ─────────────

    /// Recursively assert that a JSON value contains no `$ref` keys.
    fn assert_no_refs(value: &serde_json::Value, path: &str) {
        match value {
            serde_json::Value::Object(map) => {
                assert!(!map.contains_key("$ref"), "found $ref at {path}");
                for (k, v) in map {
                    assert_no_refs(v, &format!("{path}.{k}"));
                }
            }
            serde_json::Value::Array(arr) => {
                for (i, v) in arr.iter().enumerate() {
                    assert_no_refs(v, &format!("{path}[{i}]"));
                }
            }
            _ => {}
        }
    }

    #[tokio::test]
    async fn build_command_claude_schema_has_no_refs_or_definitions() {
        let request = make_test_request();
        let adapter = ProcessBackendAdapter::new();
        let prepared = adapter.build_command(&request).await.unwrap();

        // The --json-schema arg is the last arg in the args list
        let schema_idx = prepared
            .args
            .iter()
            .position(|a| a == "--json-schema")
            .expect("--json-schema flag should be present");
        let schema_json = &prepared.args[schema_idx + 1];
        let schema: serde_json::Value =
            serde_json::from_str(schema_json).expect("schema should be valid JSON");

        assert!(
            schema.get("definitions").is_none(),
            "top-level definitions should be removed"
        );
        assert_no_refs(&schema, "root");
    }

    #[tokio::test]
    async fn build_command_codex_schema_file_has_no_refs_or_definitions() {
        let project_dir = tempdir().unwrap();
        let request = make_codex_test_request(project_dir.path().to_path_buf());
        let adapter = ProcessBackendAdapter::new();
        let prepared = adapter.build_command(&request).await.unwrap();

        // Extract schema file path from --output-schema arg
        let schema_idx = prepared
            .args
            .iter()
            .position(|a| a == "--output-schema")
            .expect("--output-schema flag should be present");
        let schema_path = &prepared.args[schema_idx + 1];
        let schema_json = std::fs::read_to_string(schema_path).expect("schema file should exist");
        let schema: serde_json::Value =
            serde_json::from_str(&schema_json).expect("schema should be valid JSON");

        assert!(
            schema.get("definitions").is_none(),
            "top-level definitions should be removed"
        );
        assert_no_refs(&schema, "root");
    }

    // ── Claude envelope usage tests ─────────────────────────────────────────

    #[test]
    fn claude_envelope_with_usage_parses_token_counts() {
        let envelope_json = r#"{
            "result": "hello",
            "session_id": "sess-1",
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_read_input_tokens": 80,
                "cache_creation_input_tokens": 20
            }
        }"#;
        let envelope: ClaudeEnvelope = serde_json::from_str(envelope_json).unwrap();
        let usage = envelope.usage.unwrap();
        assert_eq!(usage.input_tokens, Some(100));
        assert_eq!(usage.output_tokens, Some(50));
        assert_eq!(usage.cache_read_input_tokens, Some(80));
        assert_eq!(usage.cache_creation_input_tokens, Some(20));
    }

    #[test]
    fn claude_envelope_without_usage_deserializes() {
        let envelope_json = r#"{"result": "hello", "session_id": "sess-1"}"#;
        let envelope: ClaudeEnvelope = serde_json::from_str(envelope_json).unwrap();
        assert!(envelope.usage.is_none());
        assert_eq!(envelope.result, "hello");
    }

    #[test]
    fn claude_envelope_usage_with_aliases() {
        let envelope_json = r#"{
            "result": "",
            "usage": {
                "tokens_in": 200,
                "tokens_out": 75,
                "cached_in": 150
            }
        }"#;
        let envelope: ClaudeEnvelope = serde_json::from_str(envelope_json).unwrap();
        let usage = envelope.usage.unwrap();
        assert_eq!(usage.input_tokens, Some(200));
        assert_eq!(usage.output_tokens, Some(75));
        assert_eq!(usage.cache_read_input_tokens, Some(150));
        assert_eq!(usage.cache_creation_input_tokens, None);
    }

    // ── Codex NDJSON usage extraction tests ─────────────────────────────────

    #[test]
    fn codex_usage_extracts_from_ndjson_stdout() {
        let stdout = br#"{"type":"message.start"}
{"type":"turn.completed","usage":{"input_tokens":500,"output_tokens":120,"cached_input_tokens":300}}
"#;
        let counts = extract_codex_usage_from_stdout(stdout);
        assert_eq!(counts.prompt_tokens, Some(500));
        assert_eq!(counts.completion_tokens, Some(120));
        assert_eq!(counts.total_tokens, Some(620));
        assert_eq!(counts.cache_read_tokens, Some(300));
        assert_eq!(counts.cache_creation_tokens, None);
    }

    #[test]
    fn codex_usage_returns_default_for_empty_stdout() {
        let counts = extract_codex_usage_from_stdout(b"");
        assert_eq!(counts, TokenCounts::default());
    }

    #[test]
    fn codex_usage_picks_last_event_with_usage() {
        let stdout = br#"{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":10,"cached_input_tokens":50}}
{"type":"turn.completed","usage":{"input_tokens":600,"output_tokens":200,"cached_input_tokens":400}}
"#;
        let counts = extract_codex_usage_from_stdout(stdout);
        assert_eq!(counts.prompt_tokens, Some(600));
        assert_eq!(counts.completion_tokens, Some(200));
        assert_eq!(counts.cache_read_tokens, Some(400));
    }

    #[test]
    fn codex_usage_skips_lines_without_usage() {
        let stdout = br#"{"type":"message.start"}
{"type":"content.delta","text":"hi"}
{"type":"turn.completed","usage":{"input_tokens":42,"output_tokens":7}}
"#;
        let counts = extract_codex_usage_from_stdout(stdout);
        assert_eq!(counts.prompt_tokens, Some(42));
        assert_eq!(counts.completion_tokens, Some(7));
        assert_eq!(counts.total_tokens, Some(49));
        assert_eq!(counts.cache_read_tokens, None);
    }

    #[test]
    fn codex_usage_skips_null_and_empty_usage_objects() {
        // A trailing event with `"usage": null` or `"usage": {}` must not
        // shadow a valid earlier usage record.
        let stdout = br#"{"type":"turn.completed","usage":{"input_tokens":500,"output_tokens":120,"cached_input_tokens":300}}
{"type":"done","usage":null}
{"type":"cleanup","usage":{}}
"#;
        let counts = extract_codex_usage_from_stdout(stdout);
        assert_eq!(counts.prompt_tokens, Some(500));
        assert_eq!(counts.completion_tokens, Some(120));
        assert_eq!(counts.total_tokens, Some(620));
        assert_eq!(counts.cache_read_tokens, Some(300));
    }
}
