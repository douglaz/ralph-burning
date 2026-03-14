use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::contexts::agent_execution::model::{
    InvocationContract, InvocationEnvelope, InvocationMetadata, InvocationRequest,
    RawOutputReference, TokenCounts,
};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::shared::domain::{BackendFamily, FailureClass, ResolvedBackendTarget, SessionPolicy};
use crate::shared::error::{AppError, AppResult};

#[derive(Clone, Default)]
pub struct ProcessBackendAdapter {
    pub active_children: Arc<Mutex<HashMap<String, u32>>>,
}

impl ProcessBackendAdapter {
    pub fn new() -> Self {
        Self {
            active_children: Arc::new(Mutex::new(HashMap::new())),
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

        let schema_json = request
            .contract
            .stage_contract()
            .map(|sc| {
                serde_json::to_string_pretty(&sc.json_schema())
                    .unwrap_or_else(|_| "{}".to_owned())
            })
            .unwrap_or_else(|| "{}".to_owned());

        input.push_str("\nReturn ONLY valid JSON matching the following schema:\n");
        input.push_str(&schema_json);
        input.push('\n');

        input
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

    /// Spawn a command, write stdin, register the child PID, wait for exit,
    /// read captured stdout/stderr, and remove from active_children on every
    /// path. The child is owned locally so `wait()` never holds the mutex.
    async fn spawn_and_wait(
        &self,
        request: &InvocationRequest,
        binary: &str,
        args: &[String],
        stdin_payload: &str,
    ) -> AppResult<ChildOutput> {
        let mut child = Command::new(binary)
            .args(args)
            .current_dir(&request.working_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                Self::invocation_failed(
                    request,
                    FailureClass::TransportFailure,
                    format!("failed to spawn {binary}: {error}"),
                )
            })?;

        // Write stdin and take stdout/stderr handles before registering
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(stdin_payload.as_bytes()).await;
            let _ = stdin.shutdown().await;
        }

        let mut stdout_handle = child.stdout.take();
        let mut stderr_handle = child.stderr.take();

        // Register child PID in active_children before awaiting.
        // The Child itself stays local so wait() is lock-free.
        if let Some(pid) = child.id() {
            let mut children = self.active_children.lock().await;
            children.insert(request.invocation_id.clone(), pid);
        }

        // Read stdout/stderr concurrently with waiting for the child
        let stdout_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(ref mut handle) = stdout_handle {
                let _ = handle.read_to_end(&mut buf).await;
            }
            buf
        });
        let stderr_task = tokio::spawn(async move {
            let mut buf = Vec::new();
            if let Some(ref mut handle) = stderr_handle {
                let _ = handle.read_to_end(&mut buf).await;
            }
            buf
        });

        // Wait for exit — no lock held, so cancel() can send SIGTERM
        let status = child.wait().await;

        // Remove from active_children on every exit path
        {
            let mut children = self.active_children.lock().await;
            children.remove(&request.invocation_id);
        }

        let stdout = stdout_task.await.unwrap_or_default();
        let stderr = stderr_task.await.unwrap_or_default();

        match status {
            Ok(s) => Ok(ChildOutput {
                status: Some(s),
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

    async fn invoke_claude(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        let model_id = &request.resolved_target.model.model_id;
        let schema_json = request
            .contract
            .stage_contract()
            .map(|sc| {
                serde_json::to_string(&sc.json_schema()).unwrap_or_else(|_| "{}".to_owned())
            })
            .unwrap_or_else(|| "{}".to_owned());

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

        let session_resuming = matches!(request.session_policy, SessionPolicy::ReuseIfAllowed)
            && request.prior_session.is_some();

        if session_resuming {
            if let Some(ref session) = request.prior_session {
                args.push("--resume".to_owned());
                args.push(session.session_id.clone());
            }
        }

        let stdin_payload = Self::assemble_stdin(&request);
        let output = self
            .spawn_and_wait(&request, "claude", &args, &stdin_payload)
            .await?;

        // Check exit status
        match output.status {
            Some(s) if !s.success() => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let code = s.code().map_or("signal".to_owned(), |c| c.to_string());
                return Err(Self::invocation_failed(
                    &request,
                    FailureClass::TransportFailure,
                    format!(
                        "claude exited with code {code}{}",
                        if stderr.is_empty() {
                            String::new()
                        } else {
                            format!(": {stderr}")
                        }
                    ),
                ));
            }
            None => {
                // Process was cancelled
                return Err(Self::invocation_failed(
                    &request,
                    FailureClass::TransportFailure,
                    "claude process was cancelled".to_owned(),
                ));
            }
            _ => {}
        }

        let stdout_text = String::from_utf8_lossy(&output.stdout).into_owned();

        // Double-parse: outer envelope, then result string as JSON
        let envelope: ClaudeEnvelope = serde_json::from_str(&stdout_text).map_err(|error| {
            Self::invocation_failed(
                &request,
                FailureClass::SchemaValidationFailure,
                format!("invalid Claude envelope JSON: {error}"),
            )
        })?;

        let parsed_payload: serde_json::Value =
            serde_json::from_str(&envelope.result).map_err(|error| {
                Self::invocation_failed(
                    &request,
                    FailureClass::SchemaValidationFailure,
                    format!("invalid Claude result JSON: {error}"),
                )
            })?;

        let session_id = envelope.session_id.or_else(|| {
            if session_resuming {
                request
                    .prior_session
                    .as_ref()
                    .map(|s| s.session_id.clone())
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

    async fn invoke_codex(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        let model_id = &request.resolved_target.model.model_id;

        // Create temp directory
        let temp_dir = request.project_root.join("runtime/temp");
        let _ = tokio::fs::create_dir_all(&temp_dir).await;

        let schema_path = temp_dir.join(format!("{}.schema.json", request.invocation_id));
        let message_path = temp_dir.join(format!("{}.last-message.json", request.invocation_id));

        // Write schema file
        let schema_json = request
            .contract
            .stage_contract()
            .map(|sc| {
                serde_json::to_string_pretty(&sc.json_schema())
                    .unwrap_or_else(|_| "{}".to_owned())
            })
            .unwrap_or_else(|| "{}".to_owned());

        tokio::fs::write(&schema_path, &schema_json)
            .await
            .map_err(|error| {
                Self::invocation_failed(
                    &request,
                    FailureClass::TransportFailure,
                    format!("failed to write schema file: {error}"),
                )
            })?;

        let session_resuming = matches!(request.session_policy, SessionPolicy::ReuseIfAllowed)
            && request.prior_session.is_some();

        let mut args: Vec<String> = Vec::new();
        args.push("exec".to_owned());

        if session_resuming {
            args.push("resume".to_owned());
        }

        args.extend([
            "--dangerously-bypass-approvals-and-sandbox".to_owned(),
            "--skip-git-repo-check".to_owned(),
            "--model".to_owned(),
            model_id.clone(),
            "--output-schema".to_owned(),
            schema_path.to_string_lossy().into_owned(),
            "--output-last-message".to_owned(),
            message_path.to_string_lossy().into_owned(),
        ]);

        if session_resuming {
            if let Some(ref session) = request.prior_session {
                args.push(session.session_id.clone());
            }
        }

        // Trailing `-` for stdin input
        args.push("-".to_owned());

        let stdin_payload = Self::assemble_stdin(&request);
        let output = match self
            .spawn_and_wait(&request, "codex", &args, &stdin_payload)
            .await
        {
            Ok(o) => o,
            Err(error) => {
                best_effort_cleanup(&schema_path, &message_path).await;
                return Err(error);
            }
        };

        // Check exit status
        match output.status {
            Some(s) if !s.success() => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let code = s.code().map_or("signal".to_owned(), |c| c.to_string());
                best_effort_cleanup(&schema_path, &message_path).await;
                return Err(Self::invocation_failed(
                    &request,
                    FailureClass::TransportFailure,
                    format!(
                        "codex exited with code {code}{}",
                        if stderr.is_empty() {
                            String::new()
                        } else {
                            format!(": {stderr}")
                        }
                    ),
                ));
            }
            None => {
                best_effort_cleanup(&schema_path, &message_path).await;
                return Err(Self::invocation_failed(
                    &request,
                    FailureClass::TransportFailure,
                    "codex process was cancelled".to_owned(),
                ));
            }
            _ => {}
        }

        // Read last-message file
        let last_message_text = match tokio::fs::read_to_string(&message_path).await {
            Ok(text) => text,
            Err(error) => {
                best_effort_cleanup(&schema_path, &message_path).await;
                return Err(Self::invocation_failed(
                    &request,
                    FailureClass::SchemaValidationFailure,
                    format!("failed to read codex last-message file: {error}"),
                ));
            }
        };

        let parsed_payload: serde_json::Value = match serde_json::from_str(&last_message_text) {
            Ok(v) => v,
            Err(error) => {
                best_effort_cleanup(&schema_path, &message_path).await;
                return Err(Self::invocation_failed(
                    &request,
                    FailureClass::SchemaValidationFailure,
                    format!("invalid Codex last-message JSON: {error}"),
                ));
            }
        };

        // Best-effort cleanup of temp files on success
        best_effort_cleanup(&schema_path, &message_path).await;

        let session_id = if session_resuming {
            request
                .prior_session
                .as_ref()
                .map(|s| s.session_id.clone())
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

impl AgentExecutionPort for ProcessBackendAdapter {
    async fn check_capability(
        &self,
        backend: &ResolvedBackendTarget,
        contract: &InvocationContract,
    ) -> AppResult<()> {
        match (backend.backend.family, contract) {
            (BackendFamily::Claude | BackendFamily::Codex, InvocationContract::Stage(_)) => Ok(()),
            (_, InvocationContract::Requirements { .. }) => Err(Self::capability_mismatch(
                backend,
                contract,
                "ProcessBackendAdapter currently supports workflow stage invocations only",
            )),
            (BackendFamily::OpenRouter | BackendFamily::Stub, _) => Err(Self::capability_mismatch(
                backend,
                contract,
                "ProcessBackendAdapter currently supports only claude and codex; self-hosted workflow runs require default_backend=claude or default_backend=codex",
            )),
        }
    }

    async fn check_availability(&self, backend: &ResolvedBackendTarget) -> AppResult<()> {
        let Some(binary_name) = Self::binary_name(backend.backend.family) else {
            return Err(AppError::BackendUnavailable {
                backend: backend.backend.family.to_string(),
                details: "ProcessBackendAdapter availability checks are only supported for claude and codex".to_owned(),
            });
        };

        let output = Command::new("which")
            .arg(binary_name)
            .output()
            .await
            .map_err(|error| AppError::BackendUnavailable {
                backend: backend.backend.family.to_string(),
                details: format!("failed to probe PATH for '{binary_name}': {error}"),
            })?;

        if output.status.success() {
            Ok(())
        } else {
            Err(AppError::BackendUnavailable {
                backend: backend.backend.family.to_string(),
                details: format!("required binary '{binary_name}' was not found on PATH"),
            })
        }
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        // Enforce the same stage-only / supported-family rules as preflight
        // so that callers bypassing check_capability still get CapabilityMismatch.
        self.check_capability(&request.resolved_target, &request.contract)
            .await?;

        match request.resolved_target.backend.family {
            BackendFamily::Claude => self.invoke_claude(request).await,
            BackendFamily::Codex => self.invoke_codex(request).await,
            // Unreachable after check_capability, but defensive:
            _ => Err(Self::capability_mismatch(
                &request.resolved_target,
                &request.contract,
                "ProcessBackendAdapter currently supports only claude and codex; self-hosted workflow runs require default_backend=claude or default_backend=codex",
            )),
        }
    }

    async fn cancel(&self, invocation_id: &str) -> AppResult<()> {
        // Remove the PID from the map first, then drop the lock before
        // sending SIGTERM so we never hold the mutex across blocking work.
        let pid = {
            let mut children = self.active_children.lock().await;
            match children.remove(invocation_id) {
                Some(pid) => pid,
                None => return Ok(()),
            }
        };

        // Send SIGTERM synchronously (kill is a near-instant syscall wrapper).
        let kill_result = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .output();

        match kill_result {
            Ok(_) => {
                // Both success and "already exited" non-zero are fine.
                Ok(())
            }
            Err(error) => Err(AppError::InvocationFailed {
                backend: "process".to_owned(),
                contract_id: invocation_id.to_owned(),
                failure_class: FailureClass::TransportFailure,
                details: format!(
                    "failed to send SIGTERM to invocation '{invocation_id}': {error}"
                ),
            }),
        }
    }
}

/// Claude outer envelope shape — only the fields we need.
#[derive(Deserialize)]
struct ClaudeEnvelope {
    result: String,
    #[serde(default)]
    session_id: Option<String>,
}

struct ChildOutput {
    status: Option<std::process::ExitStatus>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

async fn best_effort_cleanup(schema_path: &Path, message_path: &Path) {
    let _ = tokio::fs::remove_file(schema_path).await;
    let _ = tokio::fs::remove_file(message_path).await;
}
