use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use chrono::Utc;
use tempfile::tempdir;

use ralph_burning::adapters::process_backend::ProcessBackendAdapter;
use ralph_burning::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest, RawOutputReference,
};
use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
use ralph_burning::contexts::agent_execution::session::SessionMetadata;
use ralph_burning::contexts::workflow_composition::contracts::contract_for_stage;
use ralph_burning::shared::domain::{
    BackendFamily, BackendRole, FailureClass, ResolvedBackendTarget, SessionPolicy, StageId,
};
use ralph_burning::shared::error::AppError;

use super::env_test_support::{lock_path_mutex, PathGuard};

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

fn read_logged_args(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .expect("read args")
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

fn process_exists(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn process_is_running(pid: u32) -> bool {
    let Ok(stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };

    let Some((_, rest)) = stat.rsplit_once(") ") else {
        return false;
    };

    !rest.starts_with('Z')
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

/// Write a fake codex script that exits successfully without producing
/// --output-last-message so the adapter exercises file-read error handling.
fn write_fake_codex_without_last_message(bin_dir: &std::path::Path) {
    write_executable(
        &bin_dir.join("codex"),
        r#"#!/bin/sh
echo "$@" > "$PWD/codex-args.txt"
cat > "$PWD/codex-stdin.txt"
"#,
    );
}

fn write_fake_codex_with_large_stdout_before_stdin(
    bin_dir: &std::path::Path,
    payload: &serde_json::Value,
) {
    let payload_json = serde_json::to_string(payload).expect("serialize payload");
    write_executable(
        &bin_dir.join("codex"),
        &format!(
            r#"#!/bin/sh
msg_path=""
next_is_msg=0
for arg in "$@"; do
    if [ "$next_is_msg" = "1" ]; then
        msg_path="$arg"
        next_is_msg=0
    elif [ "$arg" = "--output-last-message" ]; then
        next_is_msg=1
    fi
done
i=0
while [ "$i" -lt 8192 ]; do
    printf '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
    i=$((i + 1))
done
cat > /dev/null
if [ -n "$msg_path" ]; then
    printf '%s' '{payload_json}' > "$msg_path"
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
async fn process_backend_rejects_unsupported_backend_families() {
    let adapter = ProcessBackendAdapter::new();
    let (_dir, openrouter_request) = request_fixture(BackendFamily::OpenRouter);

    let openrouter_error = adapter
        .check_capability(
            &openrouter_request.resolved_target,
            &openrouter_request.contract,
        )
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
}

#[tokio::test(flavor = "current_thread")]
async fn process_backend_accepts_requirements_contracts_for_claude_and_codex() {
    let adapter = ProcessBackendAdapter::new();

    for family in [BackendFamily::Claude, BackendFamily::Codex] {
        let (_dir, mut request) = request_fixture(family);
        request.contract = InvocationContract::Requirements {
            label: "requirements:question_set".to_owned(),
        };
        adapter
            .check_capability(&request.resolved_target, &request.contract)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "requirements should be accepted for {}: {e}",
                    family.as_str()
                )
            });
    }
}

#[tokio::test(flavor = "current_thread")]
async fn process_backend_rejects_requirements_for_unsupported_families() {
    let adapter = ProcessBackendAdapter::new();

    let (_dir, mut openrouter_request) = request_fixture(BackendFamily::OpenRouter);
    openrouter_request.contract = InvocationContract::Requirements {
        label: "requirements:question_set".to_owned(),
    };
    let error = adapter
        .check_capability(
            &openrouter_request.resolved_target,
            &openrouter_request.contract,
        )
        .await
        .expect_err("openrouter requirements should be rejected");
    assert!(matches!(error, AppError::CapabilityMismatch { .. }));
}

// ── Availability checks ─────────────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn process_backend_reports_missing_binary_as_backend_unavailable() {
    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);
    let bin_dir = tempdir().expect("create temp dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::replace(bin_dir.path());

    let error = adapter
        .check_availability(&request.resolved_target)
        .await
        .expect_err("missing binary should fail");

    assert!(matches!(error, AppError::BackendUnavailable { .. }));
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn process_backend_reports_non_executable_binary_as_backend_unavailable() {
    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);
    let bin_dir = tempdir().expect("create temp dir");
    let binary_path = bin_dir.path().join("claude");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::replace(bin_dir.path());

    fs::write(&binary_path, "#!/bin/sh\nexit 0\n").expect("write non-executable binary");
    let mut permissions = fs::metadata(&binary_path)
        .expect("stat non-executable binary")
        .permissions();
    permissions.set_mode(0o644);
    fs::set_permissions(&binary_path, permissions).expect("chmod non-executable binary");

    let error = adapter
        .check_availability(&request.resolved_target)
        .await
        .expect_err("non-executable binary should fail");

    match error {
        AppError::BackendUnavailable { details, .. } => {
            assert!(
                details.contains(&binary_path.display().to_string()),
                "error should include the candidate path: {details}"
            );
            assert!(
                details.contains("not executable"),
                "error should explain the permission problem: {details}"
            );
        }
        other => panic!("expected BackendUnavailable, got: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn availability_works_without_which_binary() {
    let adapter = ProcessBackendAdapter::new();
    let bin_dir = tempdir().expect("create temp dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::replace(bin_dir.path());

    write_executable(&bin_dir.path().join("codex"), "#!/bin/sh\nexit 0\n");

    let (_dir, codex_request) = request_fixture(BackendFamily::Codex);
    adapter
        .check_availability(&codex_request.resolved_target)
        .await
        .expect("codex on PATH should be available even without `which`");

    let (_dir, claude_request) = request_fixture(BackendFamily::Claude);
    let error = adapter
        .check_availability(&claude_request.resolved_target)
        .await
        .expect_err("missing claude binary should still fail");

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
    let envelope = adapter
        .invoke(request.clone())
        .await
        .expect("invoke should succeed");

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
    let schema_path = request.project_root.join(format!(
        "runtime/temp/{}.schema.json",
        request.invocation_id
    ));
    let message_path = request.project_root.join(format!(
        "runtime/temp/{}.last-message.json",
        request.invocation_id
    ));

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

    let args_file = request.working_dir.join("codex-args.txt");
    let args = read_logged_args(&args_file);
    assert_eq!(
        args,
        vec![
            "exec".to_owned(),
            "--dangerously-bypass-approvals-and-sandbox".to_owned(),
            "--skip-git-repo-check".to_owned(),
            "--model".to_owned(),
            BackendFamily::Codex.default_model_id().to_owned(),
            "--output-schema".to_owned(),
            schema_path.to_string_lossy().into_owned(),
            "--output-last-message".to_owned(),
            message_path.to_string_lossy().into_owned(),
            "-".to_owned(),
        ]
    );

    assert!(
        !schema_path.exists(),
        "schema temp file should be cleaned up after success"
    );
    assert!(
        !message_path.exists(),
        "last-message temp file should be cleaned up after success"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn codex_new_session_returns_session_id() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, request) = request_fixture(BackendFamily::Codex);
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

    assert!(envelope.metadata.session_id.is_none());
    assert!(!envelope.metadata.session_reused);
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
    let schema_path = request.project_root.join(format!(
        "runtime/temp/{}.schema.json",
        request.invocation_id
    ));
    let message_path = request.project_root.join(format!(
        "runtime/temp/{}.last-message.json",
        request.invocation_id
    ));

    assert!(envelope.metadata.session_reused);
    assert_eq!(
        envelope.metadata.session_id.as_deref(),
        Some("codex-ses-456")
    );

    let args_file = request.working_dir.join("codex-args.txt");
    let args = read_logged_args(&args_file);
    assert_eq!(
        args,
        vec![
            "exec".to_owned(),
            "resume".to_owned(),
            "--dangerously-bypass-approvals-and-sandbox".to_owned(),
            "--skip-git-repo-check".to_owned(),
            "--model".to_owned(),
            BackendFamily::Codex.default_model_id().to_owned(),
            "--output-schema".to_owned(),
            schema_path.to_string_lossy().into_owned(),
            "--output-last-message".to_owned(),
            message_path.to_string_lossy().into_owned(),
            "codex-ses-456".to_owned(),
            "-".to_owned(),
        ]
    );
    assert!(
        !schema_path.exists(),
        "resume flow should not leave a schema temp file behind"
    );
    assert!(
        !message_path.exists(),
        "resume flow should not leave a last-message temp file behind"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn codex_large_stdout_before_reading_stdin_does_not_deadlock() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, mut request) = request_fixture(BackendFamily::Codex);
    request.payload.prompt = "x".repeat(1024 * 1024);
    write_fake_codex_with_large_stdout_before_stdin(bin_dir.path(), &planning_payload());

    let adapter = ProcessBackendAdapter::new();
    let result = tokio::time::timeout(Duration::from_secs(15), adapter.invoke(request)).await;
    let envelope = result
        .expect("invoke should complete without deadlocking")
        .expect("invoke should succeed");

    assert_eq!(envelope.parsed_payload["problem_framing"], "test plan");
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
async fn cancellation_reaps_long_running_child_before_returning() {
    let bin_dir = tempdir().expect("create bin dir");
    let pid_dir = tempdir().expect("create pid dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());
    let pid_file = pid_dir.path().join("claude-child.pid");

    write_executable(
        &bin_dir.path().join("claude"),
        &format!(
            "#!/bin/sh\necho \"$$\" > \"{}\"\ncat > /dev/null\nexec sleep 300\n",
            pid_file.to_string_lossy()
        ),
    );

    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);
    let invocation_id = request.invocation_id.clone();

    // Spawn invoke in background
    let adapter_clone = adapter.clone();
    let handle = tokio::spawn(async move { adapter_clone.invoke(request).await });

    for _ in 0..40 {
        if pid_file.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let pid: u32 = fs::read_to_string(&pid_file)
        .expect("read child pid")
        .trim()
        .parse()
        .expect("parse child pid");
    assert!(
        process_exists(pid),
        "child should still exist before cancel"
    );

    {
        let children = adapter.active_children.lock().await;
        assert!(
            children.contains_key(&invocation_id),
            "child should be registered in active_children"
        );
    }

    adapter
        .cancel(&invocation_id)
        .await
        .expect("cancel should succeed");
    assert!(
        !process_exists(pid),
        "child should be reaped before cancel returns"
    );

    {
        let children = adapter.active_children.lock().await;
        assert!(
            !children.contains_key(&invocation_id),
            "child should be removed from active_children after cancel"
        );
    }

    let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(result.is_ok(), "invoke should complete after cancel");
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_returns_promptly_when_child_ignores_sigterm() {
    let bin_dir = tempdir().expect("create bin dir");
    let pid_dir = tempdir().expect("create pid dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());
    let pid_file = pid_dir.path().join("claude-ignore-term.pid");

    write_executable(
        &bin_dir.path().join("claude"),
        &format!(
            "#!/bin/sh\ntrap '' TERM\necho \"$$\" > \"{}\"\ncat > /dev/null\nexec sleep 300\n",
            pid_file.to_string_lossy()
        ),
    );

    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);
    let invocation_id = request.invocation_id.clone();

    let adapter_clone = adapter.clone();
    let handle = tokio::spawn(async move { adapter_clone.invoke(request).await });

    for _ in 0..40 {
        if pid_file.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let pid: u32 = fs::read_to_string(&pid_file)
        .expect("read child pid")
        .trim()
        .parse()
        .expect("parse child pid");
    assert!(
        process_is_running(pid),
        "child should still be running before cancel"
    );

    let started = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(2), adapter.cancel(&invocation_id))
        .await
        .expect("cancel should return within the bounded grace period")
        .expect("cancel should succeed");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "cancel should return promptly for a SIGTERM-ignoring child"
    );

    {
        let children = adapter.active_children.lock().await;
        assert!(
            !children.contains_key(&invocation_id),
            "child should be removed from active_children before cancel returns"
        );
    }

    for _ in 0..40 {
        if !process_is_running(pid) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !process_is_running(pid),
        "background reap task should eventually force-kill the child"
    );

    let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(
        result.is_ok(),
        "invoke should complete after background reap"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_child_handle_uses_kill_on_drop_safety_net() {
    let bin_dir = tempdir().expect("create bin dir");
    let pid_dir = tempdir().expect("create pid dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());
    let pid_file = pid_dir.path().join("claude-kill-on-drop.pid");

    write_executable(
        &bin_dir.path().join("claude"),
        &format!(
            "#!/bin/sh\necho \"$$\" > \"{}\"\ncat > /dev/null\nexec sleep 300\n",
            pid_file.to_string_lossy()
        ),
    );

    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);
    let adapter_clone = adapter.clone();
    let handle = tokio::spawn(async move { adapter_clone.invoke(request).await });

    for _ in 0..40 {
        if pid_file.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let pid: u32 = fs::read_to_string(&pid_file)
        .expect("read child pid")
        .trim()
        .parse()
        .expect("parse child pid");
    assert!(
        process_is_running(pid),
        "child should be running before dropping the handle"
    );

    handle.abort();
    let join_error = handle.await.expect_err("invoke task should be cancelled");
    assert!(
        join_error.is_cancelled(),
        "invoke task should abort cleanly"
    );
    assert!(
        process_is_running(pid),
        "dropping the invoke task alone should not kill the child while the adapter still holds it"
    );

    drop(adapter);

    for _ in 0..40 {
        if !process_is_running(pid) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !process_is_running(pid),
        "dropping the last child handle should terminate the child via kill_on_drop(true)"
    );
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

    adapter
        .invoke(request)
        .await
        .expect("invoke should succeed");

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

// ── Transport failure on missing Codex last-message output ──────────────────

#[tokio::test(flavor = "current_thread")]
async fn codex_missing_last_message_returns_transport_failure() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, request) = request_fixture(BackendFamily::Codex);
    write_fake_codex_without_last_message(bin_dir.path());

    let adapter = ProcessBackendAdapter::new();

    let error = adapter
        .invoke(request)
        .await
        .expect_err("missing last-message should fail");

    match error {
        AppError::InvocationFailed {
            failure_class: FailureClass::TransportFailure,
            details,
            ..
        } => {
            assert!(
                details.contains("failed to read codex last-message file"),
                "should report the read failure: {details}"
            );
        }
        other => panic!("expected TransportFailure for missing last-message, got: {other:?}"),
    }
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

    match error {
        AppError::InvocationFailed {
            failure_class: FailureClass::SchemaValidationFailure,
            details,
            ..
        } => {
            assert!(
                details.contains("invalid Codex last-message JSON"),
                "should report the parse failure: {details}"
            );
        }
        other => {
            panic!("expected SchemaValidationFailure for invalid last-message JSON, got: {other:?}")
        }
    }
}

// ── invoke() routes Requirements contract through structured output path ─────

#[tokio::test(flavor = "current_thread")]
async fn invoke_requirements_contract_via_claude() {
    let adapter = ProcessBackendAdapter::new();
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let payload_json = serde_json::json!({
        "questions": [
            {"id": "q1", "prompt": "What language?", "required": true}
        ]
    });

    // Write pre-baked envelope to a file and use the same write_fake_claude
    // pattern that existing tests use (reads stdin, outputs envelope file).
    let (_dir, mut request) = request_fixture(BackendFamily::Claude);
    let envelope_file = request.working_dir.join("req-claude-envelope.json");
    {
        let envelope = serde_json::json!({
            "type": "result",
            "result": "",
            "session_id": "req-session-1",
            "structured_output": payload_json,
        });
        fs::write(&envelope_file, serde_json::to_string(&envelope).unwrap())
            .expect("write envelope file");
    }
    write_fake_claude(bin_dir.path(), &envelope_file);

    request.contract = InvocationContract::Requirements {
        label: "requirements:question_set".to_owned(),
    };

    let result = adapter
        .invoke(request)
        .await
        .expect("requirements invoke should succeed");
    assert_eq!(result.parsed_payload, payload_json);
    assert_eq!(result.metadata.session_id.as_deref(), Some("req-session-1"));
}

#[tokio::test(flavor = "current_thread")]
async fn invoke_requirements_contract_via_codex() {
    let adapter = ProcessBackendAdapter::new();
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let payload_json = serde_json::json!({
        "questions": [
            {"id": "q1", "prompt": "Describe the feature", "required": true}
        ]
    });

    // Write pre-baked payload to a file and use the same write_fake_codex
    // pattern that existing tests use.
    let (_dir, mut request) = request_fixture(BackendFamily::Codex);
    let payload_file = request.working_dir.join("req-codex-payload.json");
    fs::write(&payload_file, serde_json::to_string(&payload_json).unwrap())
        .expect("write payload file");
    write_fake_codex(bin_dir.path(), &payload_file);

    request.contract = InvocationContract::Requirements {
        label: "requirements:question_set".to_owned(),
    };

    let result = adapter
        .invoke(request)
        .await
        .expect("codex requirements invoke should succeed");
    assert_eq!(result.parsed_payload, payload_json);
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

// ── Cancellation during stdin piping ─────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn cancellation_during_stdin_blocks_still_delivers_sigterm() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    // Fake claude that does NOT read stdin, so write_all blocks until the
    // pipe buffer fills or the process exits. This simulates the window
    // between spawn and stdin completion.
    write_executable(
        &bin_dir.path().join("claude"),
        "#!/bin/sh\nexec sleep 300\n",
    );

    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);
    let invocation_id = request.invocation_id.clone();

    let adapter_clone = adapter.clone();
    let handle = tokio::spawn(async move { adapter_clone.invoke(request).await });

    // Wait for child to be registered (it should be registered immediately
    // after spawn, before stdin writing starts)
    tokio::time::sleep(Duration::from_millis(500)).await;

    {
        let children = adapter.active_children.lock().await;
        assert!(
            children.contains_key(&invocation_id),
            "child should be registered before stdin completes"
        );
    }

    // Cancel should deliver SIGTERM even though stdin piping may be blocked
    adapter
        .cancel(&invocation_id)
        .await
        .expect("cancel should succeed");

    {
        let children = adapter.active_children.lock().await;
        assert!(
            !children.contains_key(&invocation_id),
            "child should be removed from active_children after cancel"
        );
    }

    let result = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(result.is_ok(), "invoke should complete after cancel");
}

// ── Codex schema-write failure cleanup ──────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn codex_schema_write_failure_cleans_up_partial_files() {
    let adapter = ProcessBackendAdapter::new();

    let (_dir, request) = request_fixture(BackendFamily::Codex);

    // Remove runtime/temp directory and replace it with a regular file so
    // the schema write fails (works even when running as root, unlike
    // read-only directory permissions).
    let temp_dir = request.project_root.join("runtime/temp");
    let _ = fs::remove_dir_all(&temp_dir);
    fs::write(&temp_dir, b"not a directory").expect("create blocking file");

    let error = adapter
        .invoke(request.clone())
        .await
        .expect_err("schema write should fail");

    match error {
        AppError::InvocationFailed {
            failure_class: FailureClass::TransportFailure,
            details,
            ..
        } => {
            assert!(
                details.contains("schema file"),
                "should mention schema file failure: {details}"
            );
        }
        other => panic!("expected TransportFailure for schema write, got: {other:?}"),
    }
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
