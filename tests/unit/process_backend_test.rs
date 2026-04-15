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

/// Recursively assert that a JSON value contains no `"$ref"` keys anywhere.
fn assert_no_ref_keys(value: &serde_json::Value, path: &str) {
    match value {
        serde_json::Value::Object(map) => {
            assert!(!map.contains_key("$ref"), "found $ref key at {path}");
            for (key, val) in map {
                assert_no_ref_keys(val, &format!("{path}.{key}"));
            }
        }
        serde_json::Value::Array(arr) => {
            for (i, val) in arr.iter().enumerate() {
                assert_no_ref_keys(val, &format!("{path}[{i}]"));
            }
        }
        _ => {}
    }
}

fn parse_schema_from_stdin(stdin_text: &str) -> serde_json::Value {
    let marker = "Return ONLY valid JSON matching the following schema:\n";
    let schema_text = stdin_text
        .split_once(marker)
        .map(|(_, tail)| tail.trim())
        .expect("stdin should contain schema marker");
    serde_json::from_str(schema_text).expect("stdin schema should be valid JSON")
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

async fn wait_for_condition(
    timeout: Duration,
    poll_interval: Duration,
    mut condition: impl FnMut() -> bool,
) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if condition() {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "condition was not met within {:?}",
            timeout
        );
        tokio::time::sleep(poll_interval).await;
    }
}

/// Write a fake claude script that outputs an envelope file and logs args/stdin.
/// Also extracts the `--json-schema` argument value to `claude-json-schema.json`.
fn write_fake_claude(bin_dir: &std::path::Path, envelope_file: &std::path::Path) {
    let envelope_path = envelope_file.to_string_lossy();
    write_executable(
        &bin_dir.join("claude"),
        &format!(
            r#"#!/bin/sh
echo "$@" > "$PWD/claude-args.txt"
# Extract --json-schema value to a separate file for structured parsing
next_is_schema=0
for arg in "$@"; do
    if [ "$next_is_schema" = "1" ]; then
        printf '%s' "$arg" > "$PWD/claude-json-schema.json"
        next_is_schema=0
    fi
    if [ "$arg" = "--json-schema" ]; then
        next_is_schema=1
    fi
done
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
/// Also copies the `--output-schema` file to `codex-schema-captured.json`.
fn write_fake_codex(bin_dir: &std::path::Path, payload_file: &std::path::Path) {
    let payload_path = payload_file.to_string_lossy();
    write_executable(
        &bin_dir.join("codex"),
        &format!(
            r#"#!/bin/sh
echo "$@" > "$PWD/codex-args.txt"
cat > "$PWD/codex-stdin.txt"
# Parse --output-last-message and --output-schema paths from args
msg_path=""
schema_path=""
next_is_msg=0
next_is_schema=0
for arg in "$@"; do
    if [ "$next_is_msg" = "1" ]; then
        msg_path="$arg"
        next_is_msg=0
    fi
    if [ "$next_is_schema" = "1" ]; then
        schema_path="$arg"
        next_is_schema=0
    fi
    if [ "$arg" = "--output-last-message" ]; then
        next_is_msg=1
    fi
    if [ "$arg" = "--output-schema" ]; then
        next_is_schema=1
    fi
done
if [ -n "$msg_path" ]; then
    cp "{payload_path}" "$msg_path"
fi
if [ -n "$schema_path" ] && [ -f "$schema_path" ]; then
    cp "$schema_path" "$PWD/codex-schema-captured.json"
fi
"#
        ),
    );
}

/// Write a fake codex script that writes last-message from a prepared file and
/// emits the provided stdout verbatim. This lets tests verify that the adapter
/// preserves usage-bearing NDJSON in the raw output artifact while still
/// parsing the structured payload from `--output-last-message`.
fn write_fake_codex_with_stdout(
    bin_dir: &std::path::Path,
    payload_file: &std::path::Path,
    stdout_text: &str,
) {
    let payload_path = payload_file.to_string_lossy();
    write_executable(
        &bin_dir.join("codex"),
        &format!(
            r#"#!/bin/sh
echo "$@" > "$PWD/codex-args.txt"
cat > "$PWD/codex-stdin.txt"
# Parse --output-last-message and --output-schema paths from args
msg_path=""
schema_path=""
next_is_msg=0
next_is_schema=0
for arg in "$@"; do
    if [ "$next_is_msg" = "1" ]; then
        msg_path="$arg"
        next_is_msg=0
    fi
    if [ "$next_is_schema" = "1" ]; then
        schema_path="$arg"
        next_is_schema=0
    fi
    if [ "$arg" = "--output-last-message" ]; then
        next_is_msg=1
    fi
    if [ "$arg" = "--output-schema" ]; then
        next_is_schema=1
    fi
done
if [ -n "$msg_path" ]; then
    cp "{payload_path}" "$msg_path"
fi
if [ -n "$schema_path" ] && [ -f "$schema_path" ]; then
    cp "$schema_path" "$PWD/codex-schema-captured.json"
fi
printf '%s\n' '{stdout_text}'
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

fn write_failing_codex(
    bin_dir: &std::path::Path,
    payload_file: &std::path::Path,
    stdout_text: &str,
    stderr_text: &str,
    exit_code: i32,
) {
    let payload_path = payload_file.to_string_lossy();
    write_executable(
        &bin_dir.join("codex"),
        &format!(
            r#"#!/bin/sh
echo "$@" > "$PWD/codex-args.txt"
cat > "$PWD/codex-stdin.txt"
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
if [ -n "$msg_path" ]; then
    cp "{payload_path}" "$msg_path"
fi
printf '%s\n' '{stdout_text}'
printf '%s\n' '{stderr_text}' >&2
exit {exit_code}
"#
        ),
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
async fn process_backend_accepts_openrouter_capability() {
    let adapter = ProcessBackendAdapter::new();
    let (_dir, openrouter_request) = request_fixture(BackendFamily::OpenRouter);

    adapter
        .check_capability(
            &openrouter_request.resolved_target,
            &openrouter_request.contract,
        )
        .await
        .expect("openrouter should be accepted for capability check");
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
async fn process_backend_accepts_requirements_for_openrouter() {
    let adapter = ProcessBackendAdapter::new();

    let (_dir, mut openrouter_request) = request_fixture(BackendFamily::OpenRouter);
    openrouter_request.contract = InvocationContract::Requirements {
        label: "requirements:question_set".to_owned(),
    };
    adapter
        .check_capability(
            &openrouter_request.resolved_target,
            &openrouter_request.contract,
        )
        .await
        .expect("openrouter requirements should be accepted");
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

    // Parse the actual --json-schema JSON value and validate schema structure
    let schema_file = request.working_dir.join("claude-json-schema.json");
    let schema_text = fs::read_to_string(&schema_file)
        .expect("fake claude should have captured --json-schema value");
    let schema: serde_json::Value =
        serde_json::from_str(&schema_text).expect("--json-schema value should be valid JSON");
    assert!(
        !schema
            .as_object()
            .expect("schema should be an object")
            .contains_key("definitions"),
        "schema should not have top-level definitions key"
    );
    assert_no_ref_keys(&schema, "schema");
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
    let stdin_schema = parse_schema_from_stdin(&stdin_text);
    assert_eq!(
        stdin_schema, schema,
        "Claude stdin schema should match the wrapped transport schema exactly"
    );
    assert!(
        stdin_schema.pointer("/properties/data").is_some(),
        "Claude stdin schema should include the top-level data wrapper"
    );
    assert!(
        stdin_schema.pointer("/properties/__rb_wrapped").is_none(),
        "Claude stdin schema should not require a wrapper sentinel"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn claude_suffix_model_adds_reasoning_effort_flag() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, mut request) = request_fixture(BackendFamily::Claude);
    request.resolved_target =
        ResolvedBackendTarget::new(BackendFamily::Claude, "claude-opus-4-6-max");

    let envelope_file = request.working_dir.join("claude-envelope.json");
    write_claude_envelope(
        &envelope_file,
        &planning_payload(),
        Some("ses-claude-effort"),
    );
    write_fake_claude(bin_dir.path(), &envelope_file);

    let adapter = ProcessBackendAdapter::new();
    adapter
        .invoke(request.clone())
        .await
        .expect("invoke should succeed");

    let args_text =
        fs::read_to_string(request.working_dir.join("claude-args.txt")).expect("read args");
    assert!(
        args_text.contains("--model claude-opus-4-6"),
        "should strip the effort suffix from --model: {args_text}"
    );
    assert!(
        !args_text.contains("--model claude-opus-4-6-max"),
        "should not pass the suffixed model id through to the CLI: {args_text}"
    );
    assert!(
        args_text.contains("--effort max"),
        "should pass Claude effort as a dedicated flag: {args_text}"
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

#[tokio::test(flavor = "current_thread")]
async fn claude_resume_suffix_model_adds_reasoning_effort_flag() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, mut request) = request_fixture(BackendFamily::Claude);
    request.resolved_target =
        ResolvedBackendTarget::new(BackendFamily::Claude, "claude-opus-4-6-max");
    request.session_policy = SessionPolicy::ReuseIfAllowed;
    request.prior_session = Some(SessionMetadata {
        role: BackendRole::Planner,
        backend_family: BackendFamily::Claude,
        model_id: "claude-opus-4-6-max".to_owned(),
        session_id: "ses-prior-123".to_owned(),
        created_at: Utc::now(),
        last_used_at: Utc::now(),
        invocation_count: 1,
    });

    let envelope_file = request.working_dir.join("claude-envelope.json");
    write_claude_envelope(&envelope_file, &planning_payload(), Some("ses-resumed"));
    write_fake_claude(bin_dir.path(), &envelope_file);

    let adapter = ProcessBackendAdapter::new();
    adapter
        .invoke(request.clone())
        .await
        .expect("invoke should succeed");

    let args_text =
        fs::read_to_string(request.working_dir.join("claude-args.txt")).expect("read args");
    assert!(
        args_text.contains("--model claude-opus-4-6"),
        "should strip the effort suffix from --model on resume: {args_text}"
    );
    assert!(
        args_text.contains("--effort max"),
        "should keep the Claude effort flag on resume: {args_text}"
    );
    assert!(
        args_text.contains("--resume ses-prior-123"),
        "should still resume the prior session: {args_text}"
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
    write_fake_codex_with_stdout(
        bin_dir.path(),
        &payload_file,
        r#"{"type":"turn.completed","usage":{"input_tokens":1200,"output_tokens":350,"cached_input_tokens":800}}"#,
    );

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

    // Verify raw output preserves Codex stdout NDJSON, including usage.
    assert!(matches!(
        &envelope.raw_output_reference,
        RawOutputReference::Inline(text)
            if text.contains("\"turn.completed\"")
                && text.contains("\"cached_input_tokens\":800")
                && !text.contains("problem_framing")
    ));

    // Verify metadata
    assert_eq!(envelope.metadata.invocation_id, request.invocation_id);
    assert!(envelope.metadata.session_id.is_none());
    assert!(!envelope.metadata.session_reused);
    assert_eq!(envelope.metadata.token_counts.prompt_tokens, Some(1200));
    assert_eq!(envelope.metadata.token_counts.completion_tokens, Some(350));
    assert_eq!(envelope.metadata.token_counts.total_tokens, Some(1550));
    assert_eq!(envelope.metadata.token_counts.cache_read_tokens, Some(800));

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

    // Verify the schema file contents captured by fake codex before cleanup
    let captured_schema_file = request.working_dir.join("codex-schema-captured.json");
    let schema_text = fs::read_to_string(&captured_schema_file)
        .expect("fake codex should have captured --output-schema file");
    let schema: serde_json::Value =
        serde_json::from_str(&schema_text).expect("schema file should be valid JSON");
    assert!(
        !schema
            .as_object()
            .expect("schema should be an object")
            .contains_key("definitions"),
        "schema should not have top-level definitions key"
    );
    assert_no_ref_keys(&schema, "schema");
    let stdin_file = request.working_dir.join("codex-stdin.txt");
    let stdin_text = fs::read_to_string(&stdin_file).expect("read stdin");
    let stdin_schema = parse_schema_from_stdin(&stdin_text);
    assert_eq!(
        stdin_schema, schema,
        "Codex stdin schema should match the processed transport schema exactly"
    );
    assert!(
        stdin_schema.pointer("/properties/data").is_none(),
        "Non-Claude stdin schema should not include Claude's data wrapper"
    );
    assert!(
        stdin_schema.pointer("/properties/__rb_wrapped").is_none(),
        "Non-Claude stdin schema should not include Claude's wrapper sentinel"
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

#[tokio::test(flavor = "current_thread")]
async fn codex_new_session_suffix_model_adds_reasoning_effort_override() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, mut request) = request_fixture(BackendFamily::Codex);
    request.resolved_target =
        ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.3-codex-spark-xhigh");
    let payload_file = request.working_dir.join("codex-payload.json");
    fs::write(
        &payload_file,
        serde_json::to_string(&planning_payload()).unwrap(),
    )
    .expect("write payload");
    write_fake_codex(bin_dir.path(), &payload_file);

    let adapter = ProcessBackendAdapter::new();
    adapter
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

    let args_file = request.working_dir.join("codex-args.txt");
    let args = read_logged_args(&args_file);
    assert_eq!(
        args,
        vec![
            "exec".to_owned(),
            "--dangerously-bypass-approvals-and-sandbox".to_owned(),
            "--skip-git-repo-check".to_owned(),
            "--model".to_owned(),
            "gpt-5.3-codex-spark".to_owned(),
            "-c".to_owned(),
            "model_reasoning_effort=\"xhigh\"".to_owned(),
            "--output-schema".to_owned(),
            schema_path.to_string_lossy().into_owned(),
            "--output-last-message".to_owned(),
            message_path.to_string_lossy().into_owned(),
            "-".to_owned(),
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn codex_new_session_fast_suffix_adds_service_tier_override() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, mut request) = request_fixture(BackendFamily::Codex);
    request.resolved_target =
        ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.3-codex-spark-xhigh-fast");
    let payload_file = request.working_dir.join("codex-payload.json");
    fs::write(
        &payload_file,
        serde_json::to_string(&planning_payload()).unwrap(),
    )
    .expect("write payload");
    write_fake_codex(bin_dir.path(), &payload_file);

    let adapter = ProcessBackendAdapter::new();
    adapter
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

    let args_file = request.working_dir.join("codex-args.txt");
    let args = read_logged_args(&args_file);
    assert_eq!(
        args,
        vec![
            "exec".to_owned(),
            "--dangerously-bypass-approvals-and-sandbox".to_owned(),
            "--skip-git-repo-check".to_owned(),
            "--model".to_owned(),
            "gpt-5.3-codex-spark".to_owned(),
            "-c".to_owned(),
            "model_reasoning_effort=\"xhigh\"".to_owned(),
            "-c".to_owned(),
            "service_tier=\"fast\"".to_owned(),
            "--output-schema".to_owned(),
            schema_path.to_string_lossy().into_owned(),
            "--output-last-message".to_owned(),
            message_path.to_string_lossy().into_owned(),
            "-".to_owned(),
        ]
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
async fn codex_resume_suffix_model_adds_reasoning_effort_override() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, mut request) = request_fixture(BackendFamily::Codex);
    request.resolved_target = ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.4-xhigh");
    request.session_policy = SessionPolicy::ReuseIfAllowed;
    request.prior_session = Some(SessionMetadata {
        role: BackendRole::Implementer,
        backend_family: BackendFamily::Codex,
        model_id: "gpt-5.4-xhigh".to_owned(),
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
    adapter
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
            "gpt-5.4".to_owned(),
            "-c".to_owned(),
            "model_reasoning_effort=\"xhigh\"".to_owned(),
            "--output-schema".to_owned(),
            schema_path.to_string_lossy().into_owned(),
            "--output-last-message".to_owned(),
            message_path.to_string_lossy().into_owned(),
            "codex-ses-456".to_owned(),
            "-".to_owned(),
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn codex_resume_fast_suffix_adds_service_tier_override() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, mut request) = request_fixture(BackendFamily::Codex);
    request.resolved_target = ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.4-fast");
    request.session_policy = SessionPolicy::ReuseIfAllowed;
    request.prior_session = Some(SessionMetadata {
        role: BackendRole::Implementer,
        backend_family: BackendFamily::Codex,
        model_id: "gpt-5.4-fast".to_owned(),
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
    adapter
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
            "gpt-5.4".to_owned(),
            "-c".to_owned(),
            "service_tier=\"fast\"".to_owned(),
            "--output-schema".to_owned(),
            schema_path.to_string_lossy().into_owned(),
            "--output-last-message".to_owned(),
            message_path.to_string_lossy().into_owned(),
            "codex-ses-456".to_owned(),
            "-".to_owned(),
        ]
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

// ── OpenRouter capability is now accepted ────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn openrouter_capability_accepted_by_process_adapter() {
    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::OpenRouter);

    adapter
        .check_capability(&request.resolved_target, &request.contract)
        .await
        .expect("openrouter should be accepted by process adapter");
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

#[tokio::test(flavor = "current_thread")]
async fn codex_nonzero_exit_moves_temp_files_to_runtime_failed() {
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
    write_failing_codex(
        bin_dir.path(),
        &payload_file,
        "backend stdout failure",
        "backend stderr failure",
        17,
    );

    let adapter = ProcessBackendAdapter::new();
    let error = adapter
        .invoke(request.clone())
        .await
        .expect_err("should fail");

    match error {
        AppError::InvocationFailed {
            failure_class: FailureClass::TransportFailure,
            details,
            ..
        } => {
            assert!(
                details.contains("17"),
                "should include exit code: {details}"
            );
            assert!(
                details.contains("backend stderr failure"),
                "should include stderr detail: {details}"
            );
        }
        other => panic!("expected TransportFailure, got: {other:?}"),
    }

    let temp_schema_path = request.project_root.join(format!(
        "runtime/temp/{}.schema.json",
        request.invocation_id
    ));
    let temp_message_path = request.project_root.join(format!(
        "runtime/temp/{}.last-message.json",
        request.invocation_id
    ));
    let failed_dir = request.project_root.join("runtime/failed");
    let failed_schema_path = failed_dir.join(format!("{}.schema.json", request.invocation_id));
    let failed_message_path =
        failed_dir.join(format!("{}.last-message.json", request.invocation_id));
    let failed_raw_path = failed_dir.join(format!("{}.failed.raw", request.invocation_id));

    assert!(
        !temp_schema_path.exists(),
        "schema temp file should be moved out of runtime/temp on failure"
    );
    assert!(
        !temp_message_path.exists(),
        "last-message temp file should be moved out of runtime/temp on failure"
    );
    assert!(
        failed_schema_path.is_file(),
        "schema file should be preserved under runtime/failed"
    );
    assert!(
        failed_message_path.is_file(),
        "last-message file should be preserved under runtime/failed"
    );
    assert!(
        failed_raw_path.is_file(),
        "raw failure output should be written under runtime/failed"
    );

    let failed_message =
        fs::read_to_string(&failed_message_path).expect("read failed last-message");
    assert!(
        failed_message.contains("test plan"),
        "preserved last-message should contain the codex payload"
    );

    let failed_raw = fs::read_to_string(&failed_raw_path).expect("read failed raw output");
    assert!(
        failed_raw.contains("=== stdout ===") && failed_raw.contains("backend stdout failure"),
        "raw failure output should contain stdout: {failed_raw}"
    );
    assert!(
        failed_raw.contains("=== stderr ===") && failed_raw.contains("backend stderr failure"),
        "raw failure output should contain stderr: {failed_raw}"
    );
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

// ── invoke() for OpenRouter fails gracefully without API key ─────────────────

#[tokio::test(flavor = "current_thread")]
async fn invoke_openrouter_without_api_key_returns_terminal_failure() {
    let _lock = super::env_test_support::lock_openrouter_key_mutex();
    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::OpenRouter);

    let _key_guard = super::env_test_support::OpenRouterKeyGuard::remove();

    let error = adapter
        .invoke(request)
        .await
        .expect_err("openrouter invoke without API key should fail");

    match &error {
        AppError::InvocationFailed {
            failure_class,
            details,
            ..
        } => {
            assert_eq!(
                *failure_class,
                FailureClass::BinaryNotFound,
                "missing API key should be terminal (BinaryNotFound), got: {failure_class:?}"
            );
            assert!(
                details.contains("OPENROUTER_API_KEY"),
                "should mention missing API key: {details}"
            );
        }
        other => panic!("expected InvocationFailed, got: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn invoke_openrouter_with_blank_api_key_returns_terminal_failure() {
    let _lock = super::env_test_support::lock_openrouter_key_mutex();
    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::OpenRouter);

    let _key_guard = super::env_test_support::OpenRouterKeyGuard::set("");

    let error = adapter
        .invoke(request)
        .await
        .expect_err("openrouter invoke with blank API key should fail");

    match &error {
        AppError::InvocationFailed {
            failure_class,
            details,
            ..
        } => {
            assert_eq!(
                *failure_class,
                FailureClass::BinaryNotFound,
                "blank API key should be terminal (BinaryNotFound), got: {failure_class:?}"
            );
            assert!(
                details.contains("OPENROUTER_API_KEY"),
                "should mention missing API key: {details}"
            );
        }
        other => panic!("expected InvocationFailed, got: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn invoke_openrouter_with_whitespace_only_api_key_returns_terminal_failure() {
    let _lock = super::env_test_support::lock_openrouter_key_mutex();
    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::OpenRouter);

    let _key_guard = super::env_test_support::OpenRouterKeyGuard::set("   ");

    let error = adapter
        .invoke(request)
        .await
        .expect_err("openrouter invoke with whitespace-only API key should fail");

    match &error {
        AppError::InvocationFailed {
            failure_class,
            details,
            ..
        } => {
            assert_eq!(
                *failure_class,
                FailureClass::BinaryNotFound,
                "whitespace-only API key should be terminal (BinaryNotFound), got: {failure_class:?}"
            );
            assert!(
                details.contains("OPENROUTER_API_KEY"),
                "should mention missing API key: {details}"
            );
        }
        other => panic!("expected InvocationFailed, got: {other:?}"),
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
    let stdin_schema = parse_schema_from_stdin(&stdin_text);
    assert_eq!(
        stdin_schema.pointer("/properties/__rb_wrapped"),
        None,
        "Claude stdin schema should not include the legacy wrapper sentinel"
    );
    assert!(
        stdin_schema.pointer("/properties/data").is_some(),
        "Claude stdin schema should describe the same wrapped data contract as transport"
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
    wait_for_condition(Duration::from_secs(1), Duration::from_millis(25), || {
        adapter
            .active_children
            .try_lock()
            .map(|children| children.contains_key(&invocation_id))
            .unwrap_or(false)
    })
    .await;

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

#[tokio::test(flavor = "current_thread")]
async fn nonzero_exit_writes_failed_raw_output_for_claude() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    write_executable(
        &bin_dir.path().join("claude"),
        "#!/bin/sh\ncat > /dev/null\necho 'claude stdout failure'\necho 'claude stderr failure' >&2\nexit 9\n",
    );

    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);

    let error = adapter
        .invoke(request.clone())
        .await
        .expect_err("should fail");

    assert!(matches!(
        error,
        AppError::InvocationFailed {
            failure_class: FailureClass::TransportFailure,
            ..
        }
    ));

    let failed_raw_path = request
        .project_root
        .join("runtime/failed")
        .join(format!("{}.failed.raw", request.invocation_id));
    assert!(
        failed_raw_path.is_file(),
        "raw failure output should be written for claude exits"
    );

    let failed_raw = fs::read_to_string(&failed_raw_path).expect("read failed raw output");
    assert!(
        failed_raw.contains("claude stdout failure"),
        "raw failure output should contain stdout: {failed_raw}"
    );
    assert!(
        failed_raw.contains("claude stderr failure"),
        "raw failure output should contain stderr: {failed_raw}"
    );
}

// ── Stdout JSON error extraction ─────────────────────────────────────────────

#[tokio::test(flavor = "current_thread")]
async fn stdout_json_error_extracted_on_nonzero_exit() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    // Fake claude that writes an is_error JSON envelope to stdout and exits non-zero
    write_executable(
        &bin_dir.path().join("claude"),
        "#!/bin/sh\ncat > /dev/null\necho '{\"is_error\": true, \"result\": \"model overloaded\"}'\nexit 1\n",
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
                details.contains("model overloaded"),
                "should contain stdout error message: {details}"
            );
        }
        other => panic!("expected TransportFailure, got: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn stdout_json_error_combined_with_stderr() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    // Fake claude that writes both stderr and a stdout JSON error
    write_executable(
        &bin_dir.path().join("claude"),
        "#!/bin/sh\ncat > /dev/null\necho 'stderr message' >&2\necho '{\"is_error\": true, \"result\": \"rate limited\"}'\nexit 1\n",
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
                details.contains("stderr message"),
                "should contain stderr: {details}"
            );
            assert!(
                details.contains("stdout error: rate limited"),
                "should contain stdout error: {details}"
            );
        }
        other => panic!("expected TransportFailure, got: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_stdout_json_falls_back_to_stderr() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    // Fake claude that writes invalid JSON to stdout and an error to stderr
    write_executable(
        &bin_dir.path().join("claude"),
        "#!/bin/sh\ncat > /dev/null\necho 'not json at all'\necho 'fallback error' >&2\nexit 1\n",
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
                details.contains("fallback error"),
                "should contain stderr when stdout JSON is invalid: {details}"
            );
            assert!(
                !details.contains("stdout error"),
                "should not contain stdout error label when JSON is invalid: {details}"
            );
        }
        other => panic!("expected TransportFailure, got: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn stdout_plain_text_exhaustion_without_stderr_is_backend_exhausted() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    write_executable(
        &bin_dir.path().join("claude"),
        "#!/bin/sh\ncat > /dev/null\necho 'Your account has insufficient credits remaining.'\nexit 1\n",
    );

    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);

    let error = adapter.invoke(request).await.expect_err("should fail");

    match error {
        AppError::InvocationFailed {
            failure_class: FailureClass::BackendExhausted,
            details,
            ..
        } => {
            assert!(
                details.contains("insufficient credits"),
                "should retain stdout tail detail: {details}"
            );
        }
        other => panic!("expected BackendExhausted, got: {other:?}"),
    }
}

// ── Failure artifact preservation for finish()-path errors ───────────────────

#[tokio::test(flavor = "current_thread")]
async fn claude_invalid_envelope_preserves_failure_artifacts() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, request) = request_fixture(BackendFamily::Claude);

    let envelope_file = request.working_dir.join("claude-envelope.json");
    fs::write(&envelope_file, "not json at all").expect("write invalid envelope");
    write_fake_claude(bin_dir.path(), &envelope_file);

    let adapter = ProcessBackendAdapter::new();
    let _ = adapter
        .invoke(request.clone())
        .await
        .expect_err("invalid envelope should fail");

    let failed_dir = request.project_root.join("runtime/failed");
    let failed_raw_path = failed_dir.join(format!("{}.failed.raw", request.invocation_id));
    assert!(
        failed_raw_path.is_file(),
        "raw failure output should be preserved for Claude invalid envelope"
    );
    let raw = fs::read_to_string(&failed_raw_path).expect("read failed raw");
    assert!(
        raw.contains("not json at all"),
        "preserved raw should contain the invalid stdout: {raw}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn claude_invalid_result_preserves_failure_artifacts() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, request) = request_fixture(BackendFamily::Claude);

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
    let _ = adapter
        .invoke(request.clone())
        .await
        .expect_err("invalid result JSON should fail");

    let failed_dir = request.project_root.join("runtime/failed");
    let failed_raw_path = failed_dir.join(format!("{}.failed.raw", request.invocation_id));
    assert!(
        failed_raw_path.is_file(),
        "raw failure output should be preserved for Claude invalid result"
    );
    let raw = fs::read_to_string(&failed_raw_path).expect("read failed raw");
    assert!(
        raw.contains("this is not json"),
        "preserved raw should contain the invalid result in stdout: {raw}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn codex_missing_last_message_preserves_failure_artifacts() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, request) = request_fixture(BackendFamily::Codex);
    write_fake_codex_without_last_message(bin_dir.path());

    let adapter = ProcessBackendAdapter::new();
    let _ = adapter
        .invoke(request.clone())
        .await
        .expect_err("missing last-message should fail");

    let failed_dir = request.project_root.join("runtime/failed");
    let failed_schema_path = failed_dir.join(format!("{}.schema.json", request.invocation_id));
    let failed_raw_path = failed_dir.join(format!("{}.failed.raw", request.invocation_id));
    assert!(
        failed_schema_path.is_file(),
        "schema file should be preserved under runtime/failed for missing last-message"
    );
    assert!(
        failed_raw_path.is_file(),
        "raw failure output should be preserved for missing last-message"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn codex_invalid_last_message_preserves_failure_artifacts() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let (_dir, request) = request_fixture(BackendFamily::Codex);

    let payload_file = request.working_dir.join("codex-payload.json");
    fs::write(&payload_file, "not valid json").expect("write invalid payload");
    write_fake_codex(bin_dir.path(), &payload_file);

    let adapter = ProcessBackendAdapter::new();
    let _ = adapter
        .invoke(request.clone())
        .await
        .expect_err("invalid last-message should fail");

    let failed_dir = request.project_root.join("runtime/failed");
    let failed_schema_path = failed_dir.join(format!("{}.schema.json", request.invocation_id));
    let failed_message_path =
        failed_dir.join(format!("{}.last-message.json", request.invocation_id));
    let failed_raw_path = failed_dir.join(format!("{}.failed.raw", request.invocation_id));
    assert!(
        failed_schema_path.is_file(),
        "schema file should be preserved under runtime/failed for invalid last-message"
    );
    assert!(
        failed_message_path.is_file(),
        "last-message file should be preserved under runtime/failed for invalid last-message"
    );
    assert!(
        failed_raw_path.is_file(),
        "raw failure output should be preserved for invalid last-message"
    );
    let failed_message =
        fs::read_to_string(&failed_message_path).expect("read failed last-message");
    assert!(
        failed_message.contains("not valid json"),
        "preserved last-message should contain the invalid payload: {failed_message}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn claude_envelope_with_usage_populates_token_counts() {
    let adapter = ProcessBackendAdapter::new();
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let payload_json = serde_json::json!({
        "questions": [
            {"id": "q1", "prompt": "What?", "required": true}
        ]
    });

    let (_dir, mut request) = request_fixture(BackendFamily::Claude);
    let envelope_file = request.working_dir.join("usage-claude-envelope.json");
    {
        let envelope = serde_json::json!({
            "type": "result",
            "result": "",
            "session_id": "usage-sess-1",
            "structured_output": payload_json,
            "usage": {
                "input_tokens": 1200,
                "output_tokens": 350,
                "cache_read_input_tokens": 800,
                "cache_creation_input_tokens": 100
            }
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
        .expect("invoke should succeed");
    assert_eq!(result.metadata.token_counts.prompt_tokens, Some(1200));
    assert_eq!(result.metadata.token_counts.completion_tokens, Some(350));
    assert_eq!(result.metadata.token_counts.total_tokens, Some(1550));
    assert_eq!(result.metadata.token_counts.cache_read_tokens, Some(800));
    assert_eq!(
        result.metadata.token_counts.cache_creation_tokens,
        Some(100)
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn spawn_and_wait_timeout_returns_timeout_failure() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    // Script that sleeps forever (will be killed by timeout).
    write_executable(
        &bin_dir.path().join("claude"),
        "#!/bin/sh\ncat > /dev/null\nsleep 99999\n",
    );

    let adapter = ProcessBackendAdapter::new();
    let (project_dir, mut request) = request_fixture(BackendFamily::Claude);
    let invocation_id = request.invocation_id.clone();
    // Set a very short timeout (will fire instantly with paused time).
    request.timeout = Duration::from_secs(2);

    let error = adapter.invoke(request).await.expect_err("should fail");

    match error {
        AppError::InvocationFailed {
            failure_class,
            details,
            ..
        } => {
            assert!(
                matches!(
                    failure_class,
                    FailureClass::Timeout | FailureClass::TransportFailure
                ),
                "timeout path should surface Timeout or teardown TransportFailure, got {failure_class:?}"
            );
            assert!(
                details.contains("exceeded timeout"),
                "should mention timeout: {details}"
            );
            if failure_class == FailureClass::TransportFailure {
                assert!(
                    details.contains("teardown not confirmed"),
                    "unclean timeout should explain why it was downgraded: {details}"
                );
            }
        }
        other => panic!("expected Timeout, got: {other:?}"),
    }

    // Verify artifact preservation: the .failed.raw sentinel file should
    // exist in runtime/failed with a timeout reason.
    let failed_raw = project_dir
        .path()
        .join("runtime/failed")
        .join(format!("{invocation_id}.failed.raw"));
    let contents = fs::read_to_string(&failed_raw).unwrap_or_else(|err| {
        panic!("expected {failed_raw:?} to exist after timeout, but got: {err}")
    });
    assert!(
        contents.contains("process timed out"),
        "failed.raw should mention timeout reason: {contents}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn exit_code_127_returns_binary_not_found() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    write_executable(
        &bin_dir.path().join("claude"),
        "#!/bin/sh\ncat > /dev/null\nexit 127\n",
    );

    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);

    let error = adapter.invoke(request).await.expect_err("should fail");

    // Exit code 127 conventionally means "command not found" and is
    // classified as BinaryNotFound (terminal) to avoid wasting retries.
    match error {
        AppError::InvocationFailed {
            failure_class: FailureClass::BinaryNotFound,
            details,
            ..
        } => {
            assert!(
                details.contains("127"),
                "should contain exit code 127: {details}"
            );
        }
        other => panic!("expected BinaryNotFound for exit 127, got: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn spawn_missing_binary_returns_binary_not_found() {
    let empty_dir = tempdir().expect("create empty dir");
    let _env_lock = lock_path_mutex();
    // Replace PATH entirely with an empty directory so the binary cannot be found.
    let _path_guard = PathGuard::replace(empty_dir.path());

    let adapter = ProcessBackendAdapter::new();
    let (_dir, request) = request_fixture(BackendFamily::Claude);

    let error = adapter.invoke(request).await.expect_err("should fail");

    // When the binary truly does not exist, spawn() returns ErrorKind::NotFound
    // which is classified as BinaryNotFound (non-retryable / terminal).
    match error {
        AppError::InvocationFailed {
            failure_class: FailureClass::BinaryNotFound,
            details,
            ..
        } => {
            assert!(
                details.contains("failed to spawn"),
                "should mention spawn failure: {details}"
            );
        }
        other => panic!("expected BinaryNotFound for missing binary, got: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn clean_timeout_teardown_yields_timeout_without_warning() {
    // Verify that when a killable process times out, confirm_teardown
    // succeeds and the error is classified as Timeout (not TransportFailure)
    // with no "teardown not confirmed" warning in the details.
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    write_executable(
        &bin_dir.path().join("claude"),
        "#!/bin/sh\ncat > /dev/null\nsleep 99999\n",
    );

    let adapter = ProcessBackendAdapter::new();
    let (_dir, mut request) = request_fixture(BackendFamily::Claude);
    request.timeout = Duration::from_secs(2);

    let error = adapter.invoke(request).await.expect_err("should timeout");

    match error {
        AppError::InvocationFailed {
            failure_class,
            details,
            ..
        } => {
            assert_eq!(
                failure_class,
                FailureClass::Timeout,
                "clean teardown should yield Timeout, got {failure_class:?}"
            );
            assert!(
                !details.contains("teardown not confirmed"),
                "clean teardown should not warn about unconfirmed teardown: {details}"
            );
        }
        other => panic!("expected InvocationFailed, got: {other:?}"),
    }
}

/// Verify that timeout kills the entire process group, not just the direct
/// child PID.  The stub script spawns a background child that writes a PID
/// file, then sleeps.  After the adapter times out, we check that the
/// background child is also dead.
#[cfg(unix)]
#[tokio::test]
async fn timeout_kills_entire_process_group() {
    let bin_dir = tempdir().expect("create bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());

    let pid_dir = tempdir().expect("create pid dir");
    let child_pid_file = pid_dir.path().join("child.pid");
    let child_pid_path = child_pid_file.display().to_string();

    // Script: spawns a background child that writes its PID and sleeps,
    // then the parent also sleeps.  Both should be killed by process group
    // kill on timeout.
    let script = format!(
        r#"#!/bin/sh
cat > /dev/null
(echo $$ > "{child_pid_path}" && sleep 99999) &
sleep 99999
"#,
    );

    write_executable(&bin_dir.path().join("claude"), &script);

    let adapter = ProcessBackendAdapter::new();
    let (_dir, mut request) = request_fixture(BackendFamily::Claude);
    request.timeout = Duration::from_secs(2);

    let _error = adapter.invoke(request).await.expect_err("should timeout");

    // Read the background child's PID and verify it's no longer running.
    let child_pid_str = std::fs::read_to_string(&child_pid_file)
        .expect("child PID file should exist — background child was spawned");
    let child_pid: i32 = child_pid_str
        .trim()
        .parse()
        .expect("child PID file should contain an integer");

    wait_for_condition(Duration::from_secs(1), Duration::from_millis(25), || {
        !nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(child_pid),
            None, // signal 0
        )
        .is_ok()
    })
    .await;

    // signal 0 checks existence without actually signaling.
    let child_alive = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(child_pid),
        None, // signal 0
    )
    .is_ok();

    assert!(
        !child_alive,
        "background child (pid {child_pid}) should have been killed by process group signal"
    );
}
