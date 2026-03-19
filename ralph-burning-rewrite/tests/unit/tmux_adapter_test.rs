use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

use tempfile::tempdir;

use ralph_burning::adapters::process_backend::ProcessBackendAdapter;
use ralph_burning::adapters::tmux::TmuxAdapter;
use ralph_burning::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest, RawOutputReference,
};
use ralph_burning::contexts::agent_execution::service::{
    AgentExecutionPort, AgentExecutionService, RawOutputPort,
};
use ralph_burning::contexts::agent_execution::session::{PersistedSessions, SessionStorePort};
use ralph_burning::contexts::workflow_composition::contracts::contract_for_stage;
use ralph_burning::shared::domain::{
    BackendFamily, BackendRole, ResolvedBackendTarget, SessionPolicy, StageId,
};
use ralph_burning::shared::error::AppError;

use super::env_test_support::{lock_path_mutex, PathGuard};

#[derive(Clone, Copy)]
struct InlineRawOutputStore;

impl RawOutputPort for InlineRawOutputStore {
    fn persist_raw_output(
        &self,
        _project_root: &Path,
        _invocation_id: &str,
        contents: &str,
    ) -> ralph_burning::shared::error::AppResult<RawOutputReference> {
        Ok(RawOutputReference::Inline(contents.to_owned()))
    }
}

#[derive(Clone, Copy)]
struct NoopSessionStore;

impl SessionStorePort for NoopSessionStore {
    fn load_sessions(
        &self,
        _project_root: &Path,
    ) -> ralph_burning::shared::error::AppResult<PersistedSessions> {
        Ok(PersistedSessions::empty())
    }

    fn save_sessions(
        &self,
        _project_root: &Path,
        _sessions: &PersistedSessions,
    ) -> ralph_burning::shared::error::AppResult<()> {
        Ok(())
    }
}

fn request_fixture(invocation_id: &str) -> (tempfile::TempDir, InvocationRequest) {
    let temp_dir = tempdir().expect("create temp dir");
    fs::create_dir_all(temp_dir.path().join("runtime/temp")).expect("create runtime/temp");

    let request = InvocationRequest {
        invocation_id: invocation_id.to_owned(),
        project_root: temp_dir.path().to_path_buf(),
        working_dir: temp_dir.path().to_path_buf(),
        contract: InvocationContract::Stage(contract_for_stage(StageId::Planning)),
        role: BackendRole::Planner,
        resolved_target: ResolvedBackendTarget::new(
            BackendFamily::Claude,
            BackendFamily::Claude.default_model_id(),
        ),
        payload: InvocationPayload {
            prompt: "Tmux adapter prompt".to_owned(),
            context: serde_json::json!({"stage": "planning"}),
        },
        timeout: Duration::from_secs(5),
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

fn planning_payload() -> serde_json::Value {
    serde_json::json!({
        "problem_framing": "tmux plan",
        "assumptions_or_open_questions": ["none"],
        "proposed_work": [{"order": 1, "summary": "do it", "details": "details"}],
        "readiness": {"ready": true, "risks": []}
    })
}

fn write_claude_envelope(path: &std::path::Path, result_json: &serde_json::Value) {
    let envelope = serde_json::json!({
        "type": "result",
        "result": "{}",
        "structured_output": result_json,
        "session_id": "ses-tmux"
    });
    fs::write(path, serde_json::to_string(&envelope).unwrap()).expect("write envelope");
}

fn write_fake_claude(bin_dir: &std::path::Path, envelope_file: &std::path::Path) {
    let envelope_path = envelope_file.to_string_lossy();
    write_executable(
        &bin_dir.join("claude"),
        &format!(
            r#"#!/usr/bin/env bash
echo "$@" > "$PWD/claude-args.txt"
cat > "$PWD/claude-stdin.txt"
cat "{envelope_path}"
"#
        ),
    );
}

fn write_sleeping_claude(bin_dir: &std::path::Path) {
    write_executable(
        &bin_dir.join("claude"),
        r#"#!/usr/bin/env bash
trap 'exit 130' INT TERM
while true; do
  sleep 0.1
done
"#,
    );
}

fn write_sigterm_ignoring_claude(
    bin_dir: &std::path::Path,
    term_file: &std::path::Path,
    int_file: &std::path::Path,
) {
    write_executable(
        &bin_dir.join("claude"),
        &format!(
            r#"#!/usr/bin/env bash
trap 'printf "term\n" >> "{term_file}"' TERM
trap 'printf "int\n" >> "{int_file}"; exit 130' INT
while true; do
  sleep 0.1
done
"#,
            term_file = term_file.to_string_lossy(),
            int_file = int_file.to_string_lossy(),
        ),
    );
}

fn write_fake_tmux(bin_dir: &std::path::Path, state_dir: &std::path::Path) {
    let state = state_dir.to_string_lossy();
    write_executable(
        &bin_dir.join("tmux"),
        &format!(
            r#"#!/usr/bin/env bash
set -eu
STATE_DIR="{state}"
mkdir -p "$STATE_DIR"
cmd="$1"
shift

pid_file() {{
  printf '%s/%s.pid' "$STATE_DIR" "$1"
}}

live_session() {{
  local session="$1"
  local file
  file="$(pid_file "$session")"
  if [ ! -f "$file" ]; then
    return 1
  fi
  local pid
  pid="$(cat "$file")"
  if kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  rm -f "$file"
  return 1
}}

case "$cmd" in
  new-session)
    session=""
    shell_cmd=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        -d)
          shift
          ;;
        -s)
          session="$2"
          shift 2
          ;;
        -x|-y|-c)
          shift 2
          ;;
        *)
          shell_cmd="$1"
          shift
          ;;
      esac
    done
    printf '%s' "$shell_cmd" > "$STATE_DIR/last-command.txt"
    setsid bash -c "$shell_cmd" >/dev/null 2>&1 &
    echo "$!" > "$(pid_file "$session")"
    ;;
  has-session)
    session="$2"
    if live_session "$session"; then
      exit 0
    fi
    exit 1
    ;;
  kill-session)
    session="$2"
    file="$(pid_file "$session")"
    if [ -f "$file" ]; then
      pid="$(cat "$file")"
      kill -TERM "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
      sleep 0.1
      kill -KILL "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
      rm -f "$file"
    fi
    ;;
  send-keys)
    session="$2"
    file="$(pid_file "$session")"
    if [ -f "$file" ]; then
      pid="$(cat "$file")"
      kill -INT "-$pid" 2>/dev/null || kill -INT "$pid" 2>/dev/null || true
    fi
    ;;
  attach-session)
    session="$2"
    if live_session "$session"; then
      printf 'attached:%s\n' "$session"
      exit 0
    fi
    exit 1
    ;;
  *)
    exit 1
    ;;
esac
"#
        ),
    );
}

fn session_name_for_request(request: &InvocationRequest) -> String {
    let project_name = request
        .project_root
        .file_name()
        .and_then(|value| value.to_str())
        .expect("project dir name");
    TmuxAdapter::session_name(project_name, &request.invocation_id)
}

fn read_active_session(
    request: &InvocationRequest,
) -> Option<ralph_burning::adapters::tmux::ActiveTmuxSession> {
    TmuxAdapter::read_active_session(&request.project_root).expect("read active session state")
}

#[test]
fn tmux_session_name_is_deterministic() {
    assert_eq!(
        "rb-alpha-run-1-planning-c1-a1-cr1",
        TmuxAdapter::session_name("alpha", "run-1-planning-c1-a1-cr1")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn tmux_adapter_matches_direct_process_output() {
    let bin_dir = tempdir().expect("create bin dir");
    let state_dir = tempdir().expect("create state dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());
    std::env::set_var("FAKE_TMUX_STATE_DIR", state_dir.path());

    let (_direct_dir, direct_request) = request_fixture("direct-claude");
    let envelope_file = direct_request.working_dir.join("claude-envelope.json");
    write_claude_envelope(&envelope_file, &planning_payload());
    write_fake_claude(bin_dir.path(), &envelope_file);
    write_fake_tmux(bin_dir.path(), state_dir.path());

    let direct = ProcessBackendAdapter::new()
        .invoke(direct_request.clone())
        .await
        .expect("direct invoke");

    let (_tmux_dir, tmux_request) = request_fixture("tmux-claude");
    let envelope_file = tmux_request.working_dir.join("claude-envelope.json");
    write_claude_envelope(&envelope_file, &planning_payload());

    let tmux = TmuxAdapter::new(ProcessBackendAdapter::new(), true)
        .invoke(tmux_request.clone())
        .await
        .expect("tmux invoke");

    assert_eq!(direct.parsed_payload, tmux.parsed_payload);
    assert_eq!(direct.raw_output_reference, tmux.raw_output_reference);
    assert_eq!(tmux.metadata.session_id.as_deref(), Some("ses-tmux"));

    let stdin =
        fs::read_to_string(tmux_request.working_dir.join("claude-stdin.txt")).expect("read stdin");
    let args =
        fs::read_to_string(tmux_request.working_dir.join("claude-args.txt")).expect("read args");
    assert!(stdin.contains("Tmux adapter prompt"));
    assert!(args.contains("--output-format json"));
}

#[tokio::test(flavor = "current_thread")]
async fn tmux_adapter_cancel_cleans_up_session_and_allows_attach_while_running() {
    let bin_dir = tempdir().expect("create bin dir");
    let state_dir = tempdir().expect("create state dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());
    std::env::set_var("FAKE_TMUX_STATE_DIR", state_dir.path());

    write_sleeping_claude(bin_dir.path());
    write_fake_tmux(bin_dir.path(), state_dir.path());

    let (_dir, request) = request_fixture("tmux-cancel");
    let session_name = session_name_for_request(&request);
    let adapter = TmuxAdapter::new(ProcessBackendAdapter::new(), true);

    let invocation_id = request.invocation_id.clone();
    let join = tokio::spawn({
        let adapter = adapter.clone();
        let request = request.clone();
        async move { adapter.invoke(request).await }
    });

    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        TmuxAdapter::session_exists(&session_name).expect("query session"),
        "session should exist while invocation is running"
    );
    let active_session = read_active_session(&request).expect("active session should be recorded");
    assert_eq!(active_session.invocation_id, invocation_id);
    assert_eq!(active_session.session_name, session_name);
    TmuxAdapter::attach_to_session(&session_name).expect("attach should succeed");

    adapter
        .cancel(&invocation_id)
        .await
        .expect("cancel invocation");
    let _ = join.await.expect("join invoke task");

    assert!(
        !TmuxAdapter::session_exists(&session_name).expect("query session"),
        "session should be cleaned up after cancel"
    );
    assert!(
        read_active_session(&request).is_none(),
        "active session state should be cleared after cancel"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn tmux_adapter_cancel_uses_sigterm_before_sigkill_when_backend_ignores_term() {
    let bin_dir = tempdir().expect("create bin dir");
    let state_dir = tempdir().expect("create state dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());
    std::env::set_var("FAKE_TMUX_STATE_DIR", state_dir.path());

    let term_file = state_dir.path().join("sigterm.marker");
    let int_file = state_dir.path().join("sigint.marker");
    write_sigterm_ignoring_claude(bin_dir.path(), &term_file, &int_file);
    write_fake_tmux(bin_dir.path(), state_dir.path());

    let (_dir, request) = request_fixture("tmux-signal-cancel");
    let session_name = session_name_for_request(&request);
    let adapter = TmuxAdapter::new(ProcessBackendAdapter::new(), true);
    let invocation_id = request.invocation_id.clone();

    let join = tokio::spawn({
        let adapter = adapter.clone();
        let request = request.clone();
        async move { adapter.invoke(request).await }
    });

    for _ in 0..20 {
        if TmuxAdapter::session_exists(&session_name).expect("query session") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    adapter
        .cancel(&invocation_id)
        .await
        .expect("cancel invocation");
    let _ = join.await.expect("join invoke task");

    for _ in 0..20 {
        if term_file.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert!(
        term_file.exists(),
        "cancel should deliver SIGTERM before forcing cleanup"
    );
    assert!(
        !int_file.exists(),
        "cancel should not deliver SIGINT via tmux send-keys"
    );
    assert!(
        !TmuxAdapter::session_exists(&session_name).expect("query session"),
        "session should be cleaned up after forced cancel"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn tmux_adapter_timeout_cleans_up_session_with_sigterm_then_sigkill() {
    let bin_dir = tempdir().expect("create bin dir");
    let state_dir = tempdir().expect("create state dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::prepend(bin_dir.path());
    std::env::set_var("FAKE_TMUX_STATE_DIR", state_dir.path());

    let term_file = state_dir.path().join("sigterm-timeout.marker");
    let int_file = state_dir.path().join("sigint-timeout.marker");
    write_sigterm_ignoring_claude(bin_dir.path(), &term_file, &int_file);
    write_fake_tmux(bin_dir.path(), state_dir.path());

    let (_dir, mut request) = request_fixture("tmux-timeout");
    request.timeout = Duration::from_millis(200);
    let session_name = session_name_for_request(&request);

    let service = AgentExecutionService::new(
        TmuxAdapter::new(ProcessBackendAdapter::new(), true),
        InlineRawOutputStore,
        NoopSessionStore,
    );

    let error = service
        .invoke(request.clone())
        .await
        .expect_err("timeout expected");
    match error {
        AppError::InvocationTimeout { .. } => {}
        other => panic!("expected InvocationTimeout, got {other:?}"),
    }

    for _ in 0..20 {
        if term_file.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        !TmuxAdapter::session_exists(&session_name).expect("query session"),
        "timeout should clean up the tmux session"
    );
    assert!(
        term_file.exists(),
        "timeout should deliver SIGTERM before forcing cleanup"
    );
    assert!(
        !int_file.exists(),
        "timeout should not deliver SIGINT via tmux control keys"
    );
    assert!(
        read_active_session(&request).is_none(),
        "active session state should be cleared after timeout"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn tmux_availability_check_fails_cleanly_when_binary_missing() {
    let adapter = TmuxAdapter::new(ProcessBackendAdapter::new(), false);
    let (_dir, request) = request_fixture("tmux-missing");
    let empty_dir = tempdir().expect("create empty bin dir");
    let _env_lock = lock_path_mutex();
    let _path_guard = PathGuard::replace(empty_dir.path());

    let error = adapter
        .check_availability(&request.resolved_target)
        .await
        .expect_err("tmux should be unavailable");

    match error {
        AppError::BackendUnavailable { backend, details } => {
            assert_eq!(backend, "tmux");
            assert!(details.contains("tmux"));
        }
        other => panic!("expected BackendUnavailable, got {other:?}"),
    }
}
