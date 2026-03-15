use std::time::Duration;

use chrono::{TimeZone, Utc};
use serde_json::Value;
use tempfile::tempdir;

use ralph_burning::adapters::stub_backend::StubBackendAdapter;
use ralph_burning::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use ralph_burning::contexts::agent_execution::service::AgentExecutionPort;
use ralph_burning::contexts::agent_execution::session::SessionMetadata;
use ralph_burning::contexts::workflow_composition::contracts::contract_for_stage;
use ralph_burning::shared::domain::{BackendFamily, BackendRole, SessionPolicy, StageId};
use ralph_burning::shared::error::AppError;

fn request_fixture(stage_id: StageId) -> InvocationRequest {
    let temp_dir = tempdir().expect("create temp dir");
    InvocationRequest {
        invocation_id: format!("stub-{}", stage_id.as_str()),
        project_root: temp_dir.path().to_path_buf(),
        working_dir: temp_dir.path().to_path_buf(),
        contract: InvocationContract::Stage(contract_for_stage(stage_id)),
        role: BackendRole::for_stage(stage_id),
        resolved_target: BackendRole::for_stage(stage_id).default_target(),
        payload: InvocationPayload {
            prompt: format!("Stub prompt for {}", stage_id.as_str()),
            context: serde_json::json!({"stage": stage_id.as_str()}),
        },
        timeout: Duration::from_secs(1),
        cancellation_token: CancellationToken::new(),
        session_policy: SessionPolicy::NewSession,
        prior_session: None,
        attempt_number: 1,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stub_backend_satisfies_agent_execution_port_for_all_stage_contracts() {
    let adapter = StubBackendAdapter::default();

    for stage_id in StageId::ALL {
        let request = request_fixture(stage_id);
        adapter
            .check_capability(&request.resolved_target, &request.contract)
            .await
            .expect("capability check");
        adapter
            .check_availability(&request.resolved_target)
            .await
            .expect("availability check");

        let envelope = adapter.invoke(request.clone()).await.expect("invoke stub");
        let raw = envelope
            .raw_output_reference
            .inline_contents()
            .expect("inline raw output");
        let parsed: Value = serde_json::from_str(raw).expect("parse canned JSON");

        request
            .contract
            .stage_contract()
            .expect("stage contract")
            .evaluate(&parsed)
            .expect("stage contract must accept stub payload");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stub_backend_reports_capability_failure_for_disabled_stage() {
    let adapter = StubBackendAdapter::default().without_stage_support(StageId::Planning);
    let request = request_fixture(StageId::Planning);

    let error = adapter
        .check_capability(&request.resolved_target, &request.contract)
        .await
        .expect_err("capability failure");

    assert!(matches!(error, AppError::CapabilityMismatch { .. }));
}

#[tokio::test(flavor = "multi_thread")]
async fn stub_backend_reuses_prior_session_when_role_and_backend_allow_it() {
    let adapter = StubBackendAdapter::default();
    let timestamp = Utc
        .with_ymd_and_hms(2026, 3, 11, 21, 0, 0)
        .single()
        .expect("valid timestamp");
    let mut request = request_fixture(StageId::Implementation);
    request.session_policy = SessionPolicy::ReuseIfAllowed;
    request.prior_session = Some(SessionMetadata {
        role: BackendRole::Implementer,
        backend_family: BackendFamily::Codex,
        model_id: "gpt-5.4".to_owned(),
        session_id: "existing-session".to_owned(),
        created_at: timestamp,
        last_used_at: timestamp,
        invocation_count: 1,
    });

    let envelope = adapter
        .invoke(request)
        .await
        .expect("invoke with session reuse");

    assert!(envelope.metadata.session_reused);
    assert_eq!(
        envelope.metadata.session_id.as_deref(),
        Some("existing-session")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stub_backend_cancel_is_callable() {
    let adapter = StubBackendAdapter::default();
    adapter.cancel("stub-cancel").await.expect("cancel stub");
    assert_eq!(
        adapter.cancelled_invocations(),
        vec!["stub-cancel".to_owned()]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stub_backend_transient_failure_succeeds_after_fail_count() {
    let adapter = StubBackendAdapter::default().with_transient_failure(StageId::Implementation, 1);
    let request = request_fixture(StageId::Implementation);

    let first = adapter
        .invoke(request.clone())
        .await
        .expect_err("first failure");
    assert!(matches!(first, AppError::InvocationFailed { .. }));

    let second = adapter.invoke(request).await.expect("second success");
    let raw = second
        .raw_output_reference
        .inline_contents()
        .expect("inline raw output");
    let parsed: Value = serde_json::from_str(raw).expect("parse canned JSON");
    assert_eq!(
        parsed["change_summary"].as_str(),
        Some("Stub execution output for Implementation")
    );
}
