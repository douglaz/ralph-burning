use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use chrono::Utc;
use tempfile::tempdir;

use ralph_burning::adapters::process_backend::ProcessBackendAdapter;
use ralph_burning::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
    RawOutputReference,
};
use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
use ralph_burning::contexts::agent_execution::session::SessionMetadata;
use ralph_burning::contexts::workflow_composition::contracts::contract_for_stage;
use ralph_burning::shared::domain::{
    BackendFamily, BackendRole, FailureClass, ResolvedBackendTarget, SessionPolicy, StageId,
};
use ralph_burning::shared::error::AppError;

static PATH_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn lock_path_mutex() -> std::sync::MutexGuard<'static, ()> {
    PATH_MUTEX.lock().unwrap_or_else(|e| e.into_inner())
}

fn request_fixture(backend_family: BackendFamily) -> (tempfile::TempDir, InvocationRequest) {
    let temp_dir = tempdir().expect("create temp dir");
    // Create runtime/temp for codex temp files
    fs::create_dir_all(temp_dir.path().join("runtime/temp")).expect("create runtime/temp");

    let request = InvocationRequest {
        invocation_id: format!("process-{}", backend_family.as_str()),
        project_root: temp_dir.path().to_path_buf(),
        working_dir: temp_dir.path().to_path_buf(),
        contract: InvocationContract::Stage(contract_for_stage(StageId::Planning)),
        role: BackendRole::Planner,
        resolved_target: ResolvedBackendTarget::new(
            backend_family,
            backend_family.default_model_id(),
        ),
        payload: InvocationPayload {
            prompt: "Process adapter placeholder prompt".to_owned(),
            context: serde_json::json!({"stage": "planning"}),
        },
        timeout: Duration::from_secs(10),
        cancellation_token: CancellationToken::new(),
        session_policy: SessionPolicy::NewSession,
        prior_session: None,
        attempt_number: 1,
    };
    (temp_dir, request)
}

fn write_executable(path: &std::path::Path, contents: &str) {
    fs::write(path, contents).expect("write executable");
    let mut permissions = fs::metadata(path).expect("stat executable").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("chmod executable");
}

struct PathGuard {
    original: Option<std::ffi::OsString>,
}

impl PathGuard {
    fn prepend(dir: &std::path::Path) -> Self {
        let original = std::env::var_os("PATH");
        let new_path = match &original {
            Some(existing) => {
                let mut paths = std::env::split_paths(existing).collect::<Vec<_>>();
                paths.insert(0, dir.to_path_buf());
                std::env::join_paths(paths).expect("join paths")
            }
            None => dir.as_os_str().to_owned(),
        };
        std::env::set_var("PATH", &new_path);
        Self { original }
    }
}

impl Drop for PathGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(value) => std::env::set_var("PATH", value),
            None => std::env::remove_var("PATH"),
        }
    }
}

/// Write a fake claude script that outputs an envelope file and logs args/stdin.
fn write_fake_claude(bin_dir: &std::path::Path, envelope_file: &std::path::Path) {
    let envelope_path = envelope_file.to_string_lossy();
    write_executable(
        &bin_dir.join("claude"),
        &format!(
            r#"#!/bin/sh
echo "$@" > "$PWD/claude-args.txt"
cat > "$PWD/claude-stdin.txt"
cat "{envelope_path}"
"#
        ),
    );
}

/// Build a valid Claude envelope JSON string and write it to a file.
fn write_claude_envelope(
    path: &std::path::Path,
    result_json: &serde_json::Value,
    session_id: Option<&str>,
) {
    let result_string = serde_json::to_string(result_json).unwrap();
    let mut envelope = serde_json::json!({
        "type": "result",
        "result": result_string,
    });
    if let Some(sid) = session_id {
        envelope["session_id"] = serde_json::json!(sid);
    }
    fs::write(path, serde_json::to_string(&envelope).unwrap()).expect("write envelope file");
}

fn planning_payload() -> serde_json::Value {
    serde_json::json!({
        "problem_framing": "test plan",
        "assumptions_or_open_questions": ["none"],
        "proposed_work": [{"order": 1, "summary": "do it", "details": "details"}],
        "readiness": {"ready": true, "risks": []}
    })
}

/// Write a fake codex script that writes last-message from a prepared file.
fn write_fake_codex(bin_dir: &std::path::Path, payload_file: &std::path::Path) {
    let payload_path = payload_file.to_string_lossy();
    write_executable(
        &bin_dir.join("codex"),
        &format!(
            r#"#!/bin/sh
echo "$@" > "$PWD/codex-args.txt"
cat > "$PWD/codex-stdin.txt"
# Parse --output-last-message path from args
msg_path=""
next_is_msg=0
for arg in "$@"; do
    if [ "$next_is_msg" = "1" ]; then
        msg_path="$arg"
        next_is_msg=0
    fi
    if [ "$arg" = "--output-last-message" ]; then
        next_is_msg=1
    fi
done
if [ -n "$msg_path" ]; then
    cp "{payload_path}" "$msg_path"
fi
"#
        ),
    );
}

// ── Capability checks ───────────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn process_backend_accepts_stage_contracts_for_claude_and_codex() {
    let adapter = ProcessBackendAdapter::new();

    for backend_family in [BackendFamily::Claude, BackendFamily::Codex] {
        let (_dir, request) = request_fixture(backend_family);
        adapter
            .check_capability(&request.resolved_target, &request.contract)
            .await
            .expect("supported capability");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn process_backend_rejects_unsupported_backend_families_and_requirements() {
    let adapter = ProcessBackendAdapter::new();
    let (_dir, openrouter_request) = request_fixture(BackendFamily::OpenRouter);

    let openrouter_error = adapter
        .check_capability(&openrouter_request.resolved_target, &openrouter_request.contract)
        .await
        .expect_err("openrouter should be rejected");
    match &openrouter_error {
        AppError::CapabilityMismatch { details, .. } => {
            assert!(
                details.contains("only claude and codex"),
                "openrouter mismatch detail should mention claude and codex: {details}"
            );
            assert!(
                details.contains("default_backend=claude")
                    || details.contains("default_backend=codex"),
                "openrouter mismatch detail should mention default_backend: {details}"
            );
        }
        other => panic!("expected CapabilityMismatch, got: {other:?}"),
    }

    let (_dir, mut requirements_request) = request_fixture(BackendFamily::Claude);
    requirements_request.contract = InvocationContract::Requirements {
        label: "requirements:question_set".to_owned(),
    };
    let requirements_error = adapter
        .check_capability(
            &requirements_request.resolved_target,
            &requirements_request.contract,
        )
        .await
        .expect_err("requirements should be rejected");
    assert!(matches!(
        requirements_error,
        AppError::CapabilityMismatch { .. }
    ));
}

// ── Availability checks ─────────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn process_backend_reports_missing_binary_as_backend_unavailable() {
    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);
    let bin_dir = tempdir().expect("create temp dir");
    let which_path = bin_dir.path().join("which");
    let _env_lock = lock_path_mutex();
    // Only set path to our dir with a fake `which` that always fails
    let original_path = std::env::var_os("PATH");
    std::env::set_var("PATH", bin_dir.path());

    write_executable(&which_path, "#!/bin/sh\nexit 1\n");

    let error = adapter
        .check_availability(&request.resolved_target)
        .await
        .expect_err("missing binary should fail");

    // Restore PATH
    match original_path {
        Some(v) => std::env::set_var("PATH", v),
        None => std::env::remove_var("PATH"),
    }

    assert!(matches!(error, AppError::BackendUnavailable { .. }));
}

// ── Claude command construction ─────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn claude_command_construction_and_double_parse() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, request) = request_fixture(BackendFamily::Claude);

    // Write envelope file in the working directory
    let envelope_file = request.working_dir.join("claude-envelope.json");
    write_claude_envelope(&envelope_file, &planning_payload(), Some("ses-abc123"));
    write_fake_claude(bin_dir.path(), &envelope_file);

    let adapter = ProcessBackendAdapter::new();
    let envelope = adapter.invoke(request.clone()).await.expect("invoke should succeed");

    // Verify double-parse: parsed_payload should be the inner JSON object
    assert_eq!(envelope.parsed_payload["problem_framing"], "test plan");

    // Verify raw output is the full stdout (outer envelope)
    assert!(matches!(
        &envelope.raw_output_reference,
        RawOutputReference::Inline(text) if text.contains("session_id")
    ));

    // Verify metadata
    assert_eq!(envelope.metadata.invocation_id, request.invocation_id);
    assert_eq!(envelope.metadata.session_id.as_deref(), Some("ses-abc123"));
    assert!(!envelope.metadata.session_reused);

    // Verify command args contain expected flags
    let args_file = request.working_dir.join("claude-args.txt");
    let args_text = fs::read_to_string(&args_file).expect("read args");
    assert!(args_text.contains("-p"), "should have -p flag");
    assert!(
        args_text.contains("--output-format json"),
        "should have --output-format json"
    );
    assert!(
        args_text.contains(&format!(
            "--model {}",
            BackendFamily::Claude.default_model_id()
        )),
        "should have --model flag"
    );
    assert!(
        args_text.contains("--permission-mode acceptEdits"),
        "should have --permission-mode"
    );
    assert!(
        args_text.contains("--allowedTools Bash,Edit,Write,Read,Glob,Grep"),
        "should have --allowedTools"
    );
    assert!(
        args_text.contains("--json-schema"),
        "should have --json-schema"
    );
    // Should NOT have --resume for new session
    assert!(
        !args_text.contains("--resume"),
        "should not have --resume for new session"
    );

    // Verify stdin contains prompt, context, and schema reference
    let stdin_file = request.working_dir.join("claude-stdin.txt");
    let stdin_text = fs::read_to_string(&stdin_file).expect("read stdin");
    assert!(
        stdin_text.contains("Process adapter placeholder prompt"),
        "stdin should contain the prompt"
    );
    assert!(
        stdin_text.contains("Context JSON"),
        "stdin should contain Context JSON section"
    );
    assert!(
        stdin_text.contains("Return ONLY valid JSON"),
        "stdin should contain schema instruction"
    );
}

// ── Claude resume flag ──────────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn claude_resume_flag_added_when_session_available() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, mut request) = request_fixture(BackendFamily::Claude);
    request.session_policy = SessionPolicy::ReuseIfAllowed;
    request.prior_session = Some(SessionMetadata {
        role: BackendRole::Planner,
        backend_family: BackendFamily::Claude,
        model_id: BackendFamily::Claude.default_model_id().to_owned(),
        session_id: "ses-prior-123".to_owned(),
        created_at: Utc::now(),
        last_used_at: Utc::now(),
        invocation_count: 1,
    });

    let envelope_file = request.working_dir.join("claude-envelope.json");
    write_claude_envelope(&envelope_file, &planning_payload(), Some("ses-resumed"));
    write_fake_claude(bin_dir.path(), &envelope_file);

    let adapter = ProcessBackendAdapter::new();
    let envelope = adapter
        .invoke(request.clone())
        .await
        .expect("invoke should succeed");

    // Verify resume metadata
    assert!(envelope.metadata.session_reused);
    // The envelope returned ses-resumed, so that should be the session_id
    assert_eq!(envelope.metadata.session_id.as_deref(), Some("ses-resumed"));

    // Verify --resume flag was passed
    let args_file = request.working_dir.join("claude-args.txt");
    let args_text = fs::read_to_string(&args_file).expect("read args");
    assert!(
        args_text.contains("--resume ses-prior-123"),
        "should have --resume with session id: {args_text}"
    );
}

// ── Codex command construction ──────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn codex_command_construction_and_temp_files() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, request) = request_fixture(BackendFamily::Codex);

    // Write payload file for fake codex to copy
    let payload_file = request.working_dir.join("codex-payload.json");
    fs::write(&payload_file, serde_json::to_string(&planning_payload()).unwrap())
        .expect("write payload");
    write_fake_codex(bin_dir.path(), &payload_file);

    let adapter = ProcessBackendAdapter::new();
    let envelope = adapter
        .invoke(request.clone())
        .await
        .expect("invoke should succeed");

    // Verify parsed payload
    assert_eq!(envelope.parsed_payload["problem_framing"], "test plan");

    // Verify raw output is the last-message content
    assert!(matches!(
        &envelope.raw_output_reference,
        RawOutputReference::Inline(text) if text.contains("test plan")
    ));

    // Verify metadata
    assert_eq!(envelope.metadata.invocation_id, request.invocation_id);
    assert!(envelope.metadata.session_id.is_none());
    assert!(!envelope.metadata.session_reused);

    // Verify command args
    let args_file = request.working_dir.join("codex-args.txt");
    let args_text = fs::read_to_string(&args_file).expect("read args");
    assert!(args_text.contains("exec"), "should have exec subcommand");
    assert!(
        args_text.contains("--dangerously-bypass-approvals-and-sandbox"),
        "should have bypass flag"
    );
    assert!(
        args_text.contains("--skip-git-repo-check"),
        "should have skip-git flag"
    );
    assert!(
        args_text.contains(&format!(
            "--model {}",
            BackendFamily::Codex.default_model_id()
        )),
        "should have --model flag"
    );
    assert!(
        args_text.contains("--output-schema"),
        "should have --output-schema"
    );
    assert!(
        args_text.contains("--output-last-message"),
        "should have --output-last-message"
    );
    // Should NOT have resume for new session
    assert!(
        !args_text.starts_with("exec resume"),
        "should not have resume for new session: {args_text}"
    );

    // Schema temp file should be cleaned up
    let schema_path = request
        .project_root
        .join(format!("runtime/temp/{}.schema.json", request.invocation_id));
    assert!(
        !schema_path.exists(),
        "schema temp file should be cleaned up after success"
    );
}

// ── Codex resume ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn codex_resume_command_construction() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, mut request) = request_fixture(BackendFamily::Codex);
    request.session_policy = SessionPolicy::ReuseIfAllowed;
    request.prior_session = Some(SessionMetadata {
        role: BackendRole::Implementer,
        backend_family: BackendFamily::Codex,
        model_id: BackendFamily::Codex.default_model_id().to_owned(),
        session_id: "codex-ses-456".to_owned(),
        created_at: Utc::now(),
        last_used_at: Utc::now(),
        invocation_count: 1,
    });

    let payload_file = request.working_dir.join("codex-payload.json");
    fs::write(
        &payload_file,
        serde_json::to_string(&planning_payload()).unwrap(),
    )
    .expect("write payload");
    write_fake_codex(bin_dir.path(), &payload_file);

    let adapter = ProcessBackendAdapter::new();
    let envelope = adapter
        .invoke(request.clone())
        .await
        .expect("invoke should succeed");

    assert!(envelope.metadata.session_reused);
    assert_eq!(
        envelope.metadata.session_id.as_deref(),
        Some("codex-ses-456")
    );

    let args_file = request.working_dir.join("codex-args.txt");
    let args_text = fs::read_to_string(&args_file).expect("read args");
    assert!(
        args_text.starts_with("exec resume"),
        "should have 'exec resume' for session resuming: {args_text}"
    );
    assert!(
        args_text.contains("codex-ses-456"),
        "should contain session id: {args_text}"
    );
}

// ── OpenRouter capability mismatch detail text ──────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn openrouter_capability_mismatch_detail_text() {
    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::OpenRouter);

    let error = adapter
        .check_capability(&request.resolved_target, &request.contract)
        .await
        .expect_err("openrouter should fail");

    match error {
        AppError::CapabilityMismatch { details, .. } => {
            assert!(
                details.contains("ProcessBackendAdapter"),
                "detail should mention ProcessBackendAdapter: {details}"
            );
            assert!(
                details.contains("claude") && details.contains("codex"),
                "detail should mention supported families: {details}"
            );
            assert!(
                details.contains("default_backend=claude")
                    || details.contains("default_backend=codex"),
                "detail should mention default_backend config: {details}"
            );
        }
        other => panic!("expected CapabilityMismatch, got: {other:?}"),
    }
}

// ── Cancellation sends SIGTERM ──────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn cancellation_sends_sigterm_to_long_running_child() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    // Fake claude that sleeps for a long time.
    // Use `exec` so SIGTERM reaches sleep directly (shell won't mask it).
    write_executable(
        &bin_dir.path().join("claude"),
        "#!/bin/sh\ncat > /dev/null\nexec sleep 300\n",
    );

    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);
    let invocation_id = request.invocation_id.clone();

    // Spawn invoke in background
    let adapter_clone = adapter.clone();
    let handle = tokio::spawn(async move { adapter_clone.invoke(request).await });

    // Wait a moment for the child to register
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify child is registered
    {
        let children = adapter.active_children.lock().await;
        assert!(
            children.contains_key(&invocation_id),
            "child should be registered in active_children"
        );
    }

    // Cancel
    adapter
        .cancel(&invocation_id)
        .await
        .expect("cancel should succeed");

    // Verify child is removed from active_children
    {
        let children = adapter.active_children.lock().await;
        assert!(
            !children.contains_key(&invocation_id),
            "child should be removed from active_children after cancel"
        );
    }

    // The invoke task should complete (with an error since the process was killed)
    let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(result.is_ok(), "invoke should complete after cancel");
}

// ── active_children cleanup after success ───────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn active_children_cleanup_after_success() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, request) = request_fixture(BackendFamily::Claude);

    let envelope_file = request.working_dir.join("claude-envelope.json");
    write_claude_envelope(&envelope_file, &planning_payload(), Some("ses-ok"));
    write_fake_claude(bin_dir.path(), &envelope_file);

    let adapter = ProcessBackendAdapter::new();
    let invocation_id = request.invocation_id.clone();

    adapter.invoke(request).await.expect("invoke should succeed");

    let children = adapter.active_children.lock().await;
    assert!(
        !children.contains_key(&invocation_id),
        "active_children should be empty after successful invoke"
    );
}

// ── active_children cleanup after failure ───────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn active_children_cleanup_after_failure() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    // Fake claude that exits with error
    write_executable(
        &bin_dir.path().join("claude"),
        "#!/bin/sh\ncat > /dev/null\necho 'something went wrong' >&2\nexit 1\n",
    );

    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);
    let invocation_id = request.invocation_id.clone();

    let error = adapter
        .invoke(request)
        .await
        .expect_err("failing invoke should error");

    assert!(matches!(
        error,
        AppError::InvocationFailed {
            failure_class: FailureClass::TransportFailure,
            ..
        }
    ));

    let children = adapter.active_children.lock().await;
    assert!(
        !children.contains_key(&invocation_id),
        "active_children should be empty after failed invoke"
    );
}

// ── Schema validation failure on bad Claude envelope ────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn claude_invalid_envelope_returns_schema_validation_failure() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, request) = request_fixture(BackendFamily::Claude);

    // Write invalid JSON to envelope file
    let envelope_file = request.working_dir.join("claude-envelope.json");
    fs::write(&envelope_file, "not json at all").expect("write invalid envelope");
    write_fake_claude(bin_dir.path(), &envelope_file);

    let adapter = ProcessBackendAdapter::new();

    let error = adapter
        .invoke(request)
        .await
        .expect_err("invalid envelope should fail");

    assert!(matches!(
        error,
        AppError::InvocationFailed {
            failure_class: FailureClass::SchemaValidationFailure,
            ..
        }
    ));
}

// ── Schema validation failure on bad Claude result JSON ─────────────────────

#[tokio::test(flavor = "current_thread")]
async fn claude_invalid_result_json_returns_schema_validation_failure() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, request) = request_fixture(BackendFamily::Claude);

    // Valid envelope but result is not valid JSON
    let envelope_file = request.working_dir.join("claude-envelope.json");
    let envelope = serde_json::json!({
        "type": "result",
        "result": "this is not json",
        "session_id": "ses-bad"
    });
    fs::write(&envelope_file, serde_json::to_string(&envelope).unwrap())
        .expect("write bad-result envelope");

    write_fake_claude(bin_dir.path(), &envelope_file);

    let adapter = ProcessBackendAdapter::new();

    let error = adapter
        .invoke(request)
        .await
        .expect_err("invalid result JSON should fail");

    assert!(matches!(
        error,
        AppError::InvocationFailed {
            failure_class: FailureClass::SchemaValidationFailure,
            ..
        }
    ));
}

// ── Schema validation failure on bad Codex last-message JSON ────────────────

#[tokio::test(flavor = "current_thread")]
async fn codex_invalid_last_message_returns_schema_validation_failure() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, request) = request_fixture(BackendFamily::Codex);

    // Write invalid JSON to be copied as last-message
    let payload_file = request.working_dir.join("codex-payload.json");
    fs::write(&payload_file, "not valid json").expect("write invalid payload");
    write_fake_codex(bin_dir.path(), &payload_file);

    let adapter = ProcessBackendAdapter::new();

    let error = adapter
        .invoke(request)
        .await
        .expect_err("invalid last-message should fail");

    assert!(matches!(
        error,
        AppError::InvocationFailed {
            failure_class: FailureClass::SchemaValidationFailure,
            ..
        }
    ));
}

// ── invoke() rejects Requirements contract with CapabilityMismatch ──────────

#[tokio::test(flavor = "current_thread")]
async fn invoke_rejects_requirements_contract_with_capability_mismatch() {
    let adapter = ProcessBackendAdapter::new();
    let (_dir, mut request) = request_fixture(BackendFamily::Claude);
    request.contract = InvocationContract::Requirements {
        label: "requirements:question_set".to_owned(),
    };

    let error = adapter
        .invoke(request)
        .await
        .expect_err("requirements should be rejected at invoke");

    match error {
        AppError::CapabilityMismatch { details, .. } => {
            assert!(
                details.contains("stage"),
                "should mention stage-only support: {details}"
            );
        }
        other => panic!("expected CapabilityMismatch, got: {other:?}"),
    }
}

// ── invoke() rejects OpenRouter with CapabilityMismatch ─────────────────────

#[tokio::test(flavor = "current_thread")]
async fn invoke_rejects_openrouter_with_capability_mismatch() {
    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::OpenRouter);

    let error = adapter
        .invoke(request)
        .await
        .expect_err("openrouter should be rejected at invoke");

    match error {
        AppError::CapabilityMismatch { details, .. } => {
            assert!(
                details.contains("only claude and codex"),
                "should mention supported families: {details}"
            );
            assert!(
                details.contains("default_backend=claude")
                    || details.contains("default_backend=codex"),
                "should mention default_backend config: {details}"
            );
        }
        other => panic!("expected CapabilityMismatch, got: {other:?}"),
    }
}

// ── Cancel is noop for unknown invocations ──────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn process_backend_cancel_is_noop_when_invocation_is_unknown() {
    let adapter = ProcessBackendAdapter::new();
    adapter
        .cancel("missing-process-invocation")
        .await
        .expect("missing invocation should cancel cleanly");
}

// ── Stdin payload assembly includes all required parts ──────────────────────

#[tokio::test(flavor = "current_thread")]
async fn stdin_payload_includes_contract_role_prompt_context_and_schema() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, mut request) = request_fixture(BackendFamily::Claude);
    request.payload.context = serde_json::json!({"test_key": "test_value"});

    let envelope_file = request.working_dir.join("claude-envelope.json");
    write_claude_envelope(&envelope_file, &planning_payload(), None);
    write_fake_claude(bin_dir.path(), &envelope_file);

    let adapter = ProcessBackendAdapter::new();
    adapter
        .invoke(request.clone())
        .await
        .expect("should succeed");

    let stdin_file = request.working_dir.join("claude-stdin.txt");
    let stdin_text = fs::read_to_string(&stdin_file).expect("read stdin");

    // Contract label
    assert!(
        stdin_text.contains("Contract: planning"),
        "should contain contract label: {stdin_text}"
    );
    // Role
    assert!(
        stdin_text.contains("Role: Planner"),
        "should contain role: {stdin_text}"
    );
    // Prompt
    assert!(
        stdin_text.contains("Process adapter placeholder prompt"),
        "should contain prompt"
    );
    // Context JSON section
    assert!(
        stdin_text.contains("Context JSON"),
        "should contain Context JSON header"
    );
    assert!(
        stdin_text.contains("test_value"),
        "should contain context values"
    );
    // Schema instruction
    assert!(
        stdin_text.contains("Return ONLY valid JSON"),
        "should contain schema instruction"
    );
}

// ── Null context omits Context JSON section ─────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn stdin_payload_omits_context_section_when_null() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, mut request) = request_fixture(BackendFamily::Claude);
    request.payload.context = serde_json::Value::Null;

    let envelope_file = request.working_dir.join("claude-envelope.json");
    write_claude_envelope(&envelope_file, &planning_payload(), None);
    write_fake_claude(bin_dir.path(), &envelope_file);

    let adapter = ProcessBackendAdapter::new();
    adapter
        .invoke(request.clone())
        .await
        .expect("should succeed");

    let stdin_file = request.working_dir.join("claude-stdin.txt");
    let stdin_text = fs::read_to_string(&stdin_file).expect("read stdin");

    assert!(
        !stdin_text.contains("Context JSON"),
        "should NOT contain Context JSON header when context is null"
    );
}

// ── Transport failure includes stderr ───────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn transport_failure_includes_stderr_text() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    write_executable(
        &bin_dir.path().join("claude"),
        "#!/bin/sh\ncat > /dev/null\necho 'detailed error message' >&2\nexit 42\n",
    );

    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);

    let error = adapter.invoke(request).await.expect_err("should fail");

    match error {
        AppError::InvocationFailed {
            failure_class: FailureClass::TransportFailure,
            details,
            ..
        } => {
            assert!(
                details.contains("42"),
                "should contain exit code: {details}"
            );
            assert!(
                details.contains("detailed error message"),
                "should contain stderr: {details}"
            );
        }
        other => panic!("expected TransportFailure, got: {other:?}"),
    }
}
