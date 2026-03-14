use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;

use tempfile::tempdir;

use ralph_burning::adapters::process_backend::ProcessBackendAdapter;
use ralph_burning::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
use ralph_burning::contexts::workflow_composition::contracts::contract_for_stage;
use ralph_burning::shared::domain::{
    BackendFamily, BackendRole, FailureClass, ResolvedBackendTarget, SessionPolicy, StageId,
};
use ralph_burning::shared::error::AppError;

static PATH_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn request_fixture(backend_family: BackendFamily) -> InvocationRequest {
    let temp_dir = tempdir().expect("create temp dir");

    InvocationRequest {
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
        timeout: Duration::from_secs(1),
        cancellation_token: CancellationToken::new(),
        session_policy: SessionPolicy::NewSession,
        prior_session: None,
        attempt_number: 1,
    }
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
    fn set(path: &std::path::Path) -> Self {
        let original = std::env::var_os("PATH");
        std::env::set_var("PATH", path);
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

#[tokio::test(flavor = "current_thread")]
async fn process_backend_accepts_stage_contracts_for_claude_and_codex() {
    let adapter = ProcessBackendAdapter::new();

    for backend_family in [BackendFamily::Claude, BackendFamily::Codex] {
        let request = request_fixture(backend_family);
        adapter
            .check_capability(&request.resolved_target, &request.contract)
            .await
            .expect("supported capability");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn process_backend_rejects_unsupported_backend_families_and_requirements() {
    let adapter = ProcessBackendAdapter::new();
    let openrouter_request = request_fixture(BackendFamily::OpenRouter);

    let openrouter_error = adapter
        .check_capability(&openrouter_request.resolved_target, &openrouter_request.contract)
        .await
        .expect_err("openrouter should be rejected");
    assert!(matches!(openrouter_error, AppError::CapabilityMismatch { .. }));

    let mut requirements_request = request_fixture(BackendFamily::Claude);
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

#[tokio::test(flavor = "current_thread")]
async fn process_backend_reports_missing_binary_as_backend_unavailable() {
    let adapter = ProcessBackendAdapter::new();
    let request = request_fixture(BackendFamily::Claude);
    let temp_dir = tempdir().expect("create temp dir");
    let which_path = temp_dir.path().join("which");
    let _env_lock = PATH_MUTEX.lock().expect("path mutex poisoned");
    let _path_guard = PathGuard::set(temp_dir.path());

    write_executable(&which_path, "#!/bin/sh\nexit 1\n");

    let error = adapter
        .check_availability(&request.resolved_target)
        .await
        .expect_err("missing binary should fail");

    assert!(matches!(error, AppError::BackendUnavailable { .. }));
}

#[tokio::test(flavor = "current_thread")]
async fn process_backend_invoke_returns_placeholder_transport_failure() {
    let adapter = ProcessBackendAdapter::new();
    let request = request_fixture(BackendFamily::Claude);

    let error = adapter
        .invoke(request)
        .await
        .expect_err("placeholder invoke should fail");

    assert!(matches!(
        error,
        AppError::InvocationFailed {
            failure_class: FailureClass::TransportFailure,
            ..
        }
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn process_backend_cancel_is_noop_when_invocation_is_unknown() {
    let adapter = ProcessBackendAdapter::new();
    adapter
        .cancel("missing-process-invocation")
        .await
        .expect("missing invocation should cancel cleanly");
}
