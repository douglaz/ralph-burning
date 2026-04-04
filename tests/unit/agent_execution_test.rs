use std::fs;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use serde_json::{json, Value};
use tempfile::tempdir;

use ralph_burning::adapters::fs::{FsRawOutputStore, FsSessionStore};
use ralph_burning::adapters::stub_backend::StubBackendAdapter;
use ralph_burning::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationEnvelope, InvocationMetadata,
    InvocationPayload, InvocationRequest, RawOutputReference, TokenCounts,
};
use ralph_burning::contexts::agent_execution::service::{
    AgentExecutionPort, AgentExecutionService, BackendResolver, BackendSelectionConfig,
};
use ralph_burning::contexts::agent_execution::session::{PersistedSessions, SessionMetadata};
use ralph_burning::contexts::workflow_composition::contracts::contract_for_stage;
use ralph_burning::shared::domain::{
    BackendFamily, BackendRole, FailureClass, ModelSpec, ResolvedBackendTarget, SessionPolicy,
    StageId,
};
use ralph_burning::shared::error::{AppError, AppResult};
use ralph_burning::test_support::logging::log_capture;

fn project_root_fixture(root: &std::path::Path) -> std::path::PathBuf {
    let project_root = root.join("project-alpha");
    fs::create_dir_all(project_root.join("runtime/backend")).expect("create runtime/backend");
    let sessions = serde_json::to_string_pretty(&PersistedSessions::empty())
        .expect("serialize empty sessions");
    fs::write(project_root.join("sessions.json"), sessions).expect("write sessions.json");
    project_root
}

#[allow(clippy::too_many_arguments)]
fn request_fixture(
    project_root: &std::path::Path,
    invocation_id: &str,
    stage_id: StageId,
    role: BackendRole,
    resolved_target: ResolvedBackendTarget,
    timeout: Duration,
    session_policy: SessionPolicy,
    cancellation_token: CancellationToken,
) -> InvocationRequest {
    InvocationRequest {
        invocation_id: invocation_id.to_owned(),
        project_root: project_root.to_path_buf(),
        working_dir: project_root.to_path_buf(),
        contract: InvocationContract::Stage(contract_for_stage(stage_id)),
        role,
        resolved_target,
        payload: InvocationPayload {
            prompt: format!("Produce structured output for {}", stage_id.as_str()),
            context: json!({"stage": stage_id.as_str()}),
        },
        timeout,
        cancellation_token,
        session_policy,
        prior_session: None,
        attempt_number: 1,
    }
}

fn metadata_for_request(request: &InvocationRequest) -> InvocationMetadata {
    InvocationMetadata {
        invocation_id: request.invocation_id.clone(),
        duration: Duration::from_millis(0),
        token_counts: TokenCounts::default(),
        backend_used: request.resolved_target.backend.clone(),
        model_used: request.resolved_target.model.clone(),
        adapter_reported_backend: None,
        adapter_reported_model: None,
        attempt_number: request.attempt_number,
        session_id: None,
        session_reused: false,
    }
}

struct MalformedOutputAdapter;

impl AgentExecutionPort for MalformedOutputAdapter {
    async fn check_capability(
        &self,
        _backend: &ResolvedBackendTarget,
        _contract: &InvocationContract,
    ) -> AppResult<()> {
        Ok(())
    }

    async fn check_availability(&self, _backend: &ResolvedBackendTarget) -> AppResult<()> {
        Ok(())
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        Ok(InvocationEnvelope {
            raw_output_reference: RawOutputReference::Inline("not-json".to_owned()),
            parsed_payload: Value::Null,
            metadata: metadata_for_request(&request),
            timestamp: Utc::now(),
        })
    }

    async fn cancel(&self, _invocation_id: &str) -> AppResult<()> {
        Ok(())
    }
}

struct DomainInvalidAdapter;

impl AgentExecutionPort for DomainInvalidAdapter {
    async fn check_capability(
        &self,
        _backend: &ResolvedBackendTarget,
        _contract: &InvocationContract,
    ) -> AppResult<()> {
        Ok(())
    }

    async fn check_availability(&self, _backend: &ResolvedBackendTarget) -> AppResult<()> {
        Ok(())
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        let raw = json!({
            "problem_framing": "   ",
            "assumptions_or_open_questions": [],
            "proposed_work": [],
            "readiness": {"ready": false, "risks": []}
        });

        Ok(InvocationEnvelope {
            raw_output_reference: RawOutputReference::Inline(
                serde_json::to_string_pretty(&raw).expect("serialize raw"),
            ),
            parsed_payload: Value::Null,
            metadata: metadata_for_request(&request),
            timestamp: Utc::now(),
        })
    }

    async fn cancel(&self, _invocation_id: &str) -> AppResult<()> {
        Ok(())
    }
}

struct TransportErrorAdapter;

impl AgentExecutionPort for TransportErrorAdapter {
    async fn check_capability(
        &self,
        _backend: &ResolvedBackendTarget,
        _contract: &InvocationContract,
    ) -> AppResult<()> {
        Ok(())
    }

    async fn check_availability(&self, _backend: &ResolvedBackendTarget) -> AppResult<()> {
        Ok(())
    }

    async fn invoke(&self, _request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        Err(std::io::Error::other("transport failed").into())
    }

    async fn cancel(&self, _invocation_id: &str) -> AppResult<()> {
        Ok(())
    }
}

#[test]
fn backend_resolver_honors_precedence_chain() {
    let resolver = BackendResolver::new();
    let workspace = BackendSelectionConfig {
        backend_family: Some(BackendFamily::OpenRouter),
        model_id: Some("workspace-model".to_owned()),
    };
    let project = BackendSelectionConfig {
        backend_family: Some(BackendFamily::Codex),
        model_id: Some("project-model".to_owned()),
    };
    let explicit = ModelSpec::new(BackendFamily::Claude, "explicit-model");

    let resolved = resolver
        .resolve(
            BackendRole::Reviewer,
            Some(explicit.clone()),
            Some(&project),
            Some(&workspace),
        )
        .expect("resolve target");

    assert_eq!(resolved.backend.family, BackendFamily::Claude);
    assert_eq!(resolved.model.backend_family, BackendFamily::Claude);
    assert_eq!(resolved.model.model_id, explicit.model_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn service_constructs_envelope_and_persists_raw_output() {
    let temp_dir = tempdir().expect("create temp dir");
    let project_root = project_root_fixture(temp_dir.path());

    let adapter = StubBackendAdapter::default();
    let service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
    let request = request_fixture(
        &project_root,
        "invoke-1",
        StageId::Planning,
        BackendRole::Planner,
        BackendRole::Planner.default_target(),
        Duration::from_secs(1),
        SessionPolicy::NewSession,
        CancellationToken::new(),
    );

    let envelope = service.invoke(request).await.expect("invoke service");
    let stored_path = envelope
        .raw_output_reference
        .stored_path()
        .expect("stored path");

    assert!(stored_path.exists());
    assert!(stored_path.ends_with("runtime/backend/invoke-1.raw"));
    assert_eq!(envelope.parsed_payload["readiness"]["ready"], json!(true));
    assert_eq!(envelope.metadata.attempt_number, 1);
    assert_eq!(envelope.metadata.backend_used.family, BackendFamily::Claude);
    assert_eq!(envelope.metadata.model_used.model_id, "claude-opus-4-6");
    assert!(envelope.metadata.token_counts.total_tokens.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn service_returns_capability_mismatch_when_backend_cannot_support_stage() {
    let temp_dir = tempdir().expect("create temp dir");
    let project_root = project_root_fixture(temp_dir.path());
    let adapter = StubBackendAdapter::default().without_stage_support(StageId::Planning);
    let service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
    let request = request_fixture(
        &project_root,
        "invoke-capability",
        StageId::Planning,
        BackendRole::Planner,
        BackendRole::Planner.default_target(),
        Duration::from_secs(1),
        SessionPolicy::NewSession,
        CancellationToken::new(),
    );

    let error = service
        .invoke(request)
        .await
        .expect_err("capability mismatch");

    assert!(matches!(error, AppError::CapabilityMismatch { .. }));
    assert_eq!(
        error.failure_class(),
        Some(FailureClass::DomainValidationFailure)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn service_returns_backend_unavailable_as_transport_failure() {
    let temp_dir = tempdir().expect("create temp dir");
    let project_root = project_root_fixture(temp_dir.path());
    let adapter = StubBackendAdapter::default().unavailable();
    let service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
    let request = request_fixture(
        &project_root,
        "invoke-unavailable",
        StageId::Planning,
        BackendRole::Planner,
        BackendRole::Planner.default_target(),
        Duration::from_secs(1),
        SessionPolicy::NewSession,
        CancellationToken::new(),
    );

    let error = service
        .invoke(request)
        .await
        .expect_err("backend unavailable");

    // The stub adapter returns BackendUnavailable with failure_class: None,
    // which defaults to TransportFailure (retryable).  Adapters that detect
    // fatal infrastructure issues (missing binary, missing API key) set
    // failure_class: Some(BinaryNotFound) to make it terminal.
    assert!(matches!(error, AppError::BackendUnavailable { .. }));
    assert_eq!(error.failure_class(), Some(FailureClass::TransportFailure));
}

#[tokio::test(flavor = "multi_thread")]
async fn service_times_out_and_requests_backend_cancellation() {
    let temp_dir = tempdir().expect("create temp dir");
    let project_root = project_root_fixture(temp_dir.path());
    let adapter = StubBackendAdapter::default().with_delay(Duration::from_millis(100));
    let adapter_handle = adapter.clone();
    let service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
    let request = request_fixture(
        &project_root,
        "invoke-timeout",
        StageId::Planning,
        BackendRole::Planner,
        BackendRole::Planner.default_target(),
        Duration::from_millis(10),
        SessionPolicy::NewSession,
        CancellationToken::new(),
    );

    let error = service.invoke(request).await.expect_err("timeout");

    assert!(matches!(error, AppError::InvocationTimeout { .. }));
    assert_eq!(error.failure_class(), Some(FailureClass::Timeout));
    assert_eq!(
        adapter_handle.cancelled_invocations(),
        vec!["invoke-timeout".to_owned()]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn service_propagates_cancellation_without_retry() {
    let temp_dir = tempdir().expect("create temp dir");
    let project_root = project_root_fixture(temp_dir.path());
    let adapter = StubBackendAdapter::default().with_delay(Duration::from_millis(100));
    let service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
    let cancellation_token = CancellationToken::new();
    let cancel_handle = cancellation_token.clone();
    let request = request_fixture(
        &project_root,
        "invoke-cancel",
        StageId::Planning,
        BackendRole::Planner,
        BackendRole::Planner.default_target(),
        Duration::from_secs(1),
        SessionPolicy::NewSession,
        cancellation_token,
    );

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        cancel_handle.cancel();
    });

    let error = service
        .invoke(request)
        .await
        .expect_err("cancelled invocation");

    assert!(matches!(error, AppError::InvocationCancelled { .. }));
    assert_eq!(error.failure_class(), Some(FailureClass::Cancellation));
}

#[tokio::test(flavor = "multi_thread")]
async fn service_maps_transport_failures_from_invoke() {
    let temp_dir = tempdir().expect("create temp dir");
    let project_root = project_root_fixture(temp_dir.path());
    let service =
        AgentExecutionService::new(TransportErrorAdapter, FsRawOutputStore, FsSessionStore);
    let request = request_fixture(
        &project_root,
        "invoke-transport",
        StageId::Planning,
        BackendRole::Planner,
        BackendRole::Planner.default_target(),
        Duration::from_secs(1),
        SessionPolicy::NewSession,
        CancellationToken::new(),
    );

    let error = service
        .invoke(request)
        .await
        .expect_err("transport failure");

    assert!(matches!(error, AppError::InvocationFailed { .. }));
    assert_eq!(error.failure_class(), Some(FailureClass::TransportFailure));
}

#[tokio::test(flavor = "multi_thread")]
async fn service_maps_schema_failures_and_still_persists_raw_output() {
    let temp_dir = tempdir().expect("create temp dir");
    let project_root = project_root_fixture(temp_dir.path());
    let service =
        AgentExecutionService::new(MalformedOutputAdapter, FsRawOutputStore, FsSessionStore);
    let request = request_fixture(
        &project_root,
        "invoke-schema",
        StageId::Planning,
        BackendRole::Planner,
        BackendRole::Planner.default_target(),
        Duration::from_secs(1),
        SessionPolicy::NewSession,
        CancellationToken::new(),
    );

    let error = service.invoke(request).await.expect_err("schema failure");

    assert!(matches!(error, AppError::InvocationFailed { .. }));
    assert_eq!(
        error.failure_class(),
        Some(FailureClass::SchemaValidationFailure)
    );

    let raw_output = fs::read_to_string(project_root.join("runtime/backend/invoke-schema.raw"))
        .expect("read stored raw output");
    assert_eq!(raw_output, "not-json");
}

#[tokio::test(flavor = "multi_thread")]
async fn service_maps_domain_validation_failures() {
    let temp_dir = tempdir().expect("create temp dir");
    let project_root = project_root_fixture(temp_dir.path());
    let service =
        AgentExecutionService::new(DomainInvalidAdapter, FsRawOutputStore, FsSessionStore);
    let request = request_fixture(
        &project_root,
        "invoke-domain",
        StageId::Planning,
        BackendRole::Planner,
        BackendRole::Planner.default_target(),
        Duration::from_secs(1),
        SessionPolicy::NewSession,
        CancellationToken::new(),
    );

    let error = service.invoke(request).await.expect_err("domain failure");

    assert!(matches!(error, AppError::InvocationFailed { .. }));
    assert_eq!(
        error.failure_class(),
        Some(FailureClass::DomainValidationFailure)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn service_reuses_supported_sessions_and_persists_updated_metadata() {
    let temp_dir = tempdir().expect("create temp dir");
    let project_root = project_root_fixture(temp_dir.path());
    let timestamp = Utc
        .with_ymd_and_hms(2026, 3, 11, 21, 0, 0)
        .single()
        .expect("valid timestamp");
    let sessions = PersistedSessions {
        sessions: vec![SessionMetadata {
            role: BackendRole::Implementer,
            backend_family: BackendFamily::Claude,
            model_id: "claude-opus-4-6".to_owned(),
            session_id: "existing-session".to_owned(),
            created_at: timestamp,
            last_used_at: timestamp,
            invocation_count: 1,
        }],
    };
    fs::write(
        project_root.join("sessions.json"),
        serde_json::to_string_pretty(&sessions).expect("serialize sessions"),
    )
    .expect("write sessions");

    let service = AgentExecutionService::new(
        StubBackendAdapter::default(),
        FsRawOutputStore,
        FsSessionStore,
    );
    let request = request_fixture(
        &project_root,
        "invoke-session-reuse",
        StageId::Implementation,
        BackendRole::Implementer,
        BackendRole::Implementer.default_target(),
        Duration::from_secs(1),
        SessionPolicy::ReuseIfAllowed,
        CancellationToken::new(),
    );

    let envelope = service
        .invoke(request)
        .await
        .expect("invoke with session reuse");

    assert!(envelope.metadata.session_reused);
    assert_eq!(
        envelope.metadata.session_id.as_deref(),
        Some("existing-session")
    );

    let persisted: PersistedSessions = serde_json::from_str(
        &fs::read_to_string(project_root.join("sessions.json")).expect("read sessions.json"),
    )
    .expect("deserialize sessions");
    assert_eq!(persisted.sessions.len(), 1);
    assert_eq!(persisted.sessions[0].invocation_count, 2);
    assert_eq!(persisted.sessions[0].session_id, "existing-session");
}

#[tokio::test(flavor = "multi_thread")]
async fn service_does_not_reuse_sessions_for_roles_that_disallow_it() {
    let temp_dir = tempdir().expect("create temp dir");
    let project_root = project_root_fixture(temp_dir.path());
    let timestamp = Utc
        .with_ymd_and_hms(2026, 3, 11, 21, 0, 0)
        .single()
        .expect("valid timestamp");
    let sessions = PersistedSessions {
        sessions: vec![SessionMetadata {
            role: BackendRole::Planner,
            backend_family: BackendFamily::Claude,
            model_id: "claude-opus-4-6".to_owned(),
            session_id: "planner-session".to_owned(),
            created_at: timestamp,
            last_used_at: timestamp,
            invocation_count: 1,
        }],
    };
    fs::write(
        project_root.join("sessions.json"),
        serde_json::to_string_pretty(&sessions).expect("serialize sessions"),
    )
    .expect("write sessions");

    let service = AgentExecutionService::new(
        StubBackendAdapter::default(),
        FsRawOutputStore,
        FsSessionStore,
    );
    let request = request_fixture(
        &project_root,
        "invoke-no-session-reuse",
        StageId::Planning,
        BackendRole::Planner,
        BackendRole::Planner.default_target(),
        Duration::from_secs(1),
        SessionPolicy::ReuseIfAllowed,
        CancellationToken::new(),
    );

    let envelope = service.invoke(request).await.expect("invoke without reuse");

    assert!(!envelope.metadata.session_reused);
    assert_eq!(
        envelope.metadata.session_id.as_deref(),
        None,
        "planner sessions must not be reused"
    );

    let persisted: PersistedSessions = serde_json::from_str(
        &fs::read_to_string(project_root.join("sessions.json")).expect("read sessions.json"),
    )
    .expect("deserialize sessions");
    assert_eq!(persisted.sessions.len(), 1);
    assert_eq!(persisted.sessions[0].session_id, "planner-session");
    assert_eq!(persisted.sessions[0].invocation_count, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn service_emits_invocation_completed_trace_with_token_fields() {
    let capture = log_capture();

    let temp_dir = tempdir().expect("create temp dir");
    let project_root = project_root_fixture(temp_dir.path());
    let adapter = StubBackendAdapter::default();
    let service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
    let request = request_fixture(
        &project_root,
        "trace-invoke-1",
        StageId::Planning,
        BackendRole::Planner,
        BackendRole::Planner.default_target(),
        Duration::from_secs(1),
        SessionPolicy::NewSession,
        CancellationToken::new(),
    );

    capture
        .in_scope_async(async {
            let _envelope = service.invoke(request).await.expect("invoke succeeds");
        })
        .await;

    let event = capture.assert_event_has_fields(&[
        ("invocation_id", "trace-invoke-1"),
        ("message", "invocation completed"),
    ]);

    for field in [
        "invocation_id=",
        "backend=",
        "model=",
        "attempt=",
        "duration_ms=",
        "prompt_tokens=",
        "completion_tokens=",
        "cache_read_tokens=",
        "cache_creation_tokens=",
        "tokens_reported=",
        "cache_reported=",
        "session_reused=",
    ] {
        let key = field.trim_end_matches('=');
        assert!(
            event.field(key).is_some(),
            "missing field '{key}' in trace event: {event:?}"
        );
    }

    assert!(
        event.field("tokens_reported") == Some("true"),
        "expected tokens_reported=true in trace event: {event:?}"
    );
    assert!(
        event.field("cache_reported") == Some("false"),
        "expected cache_reported=false in trace event: {event:?}"
    );
}
