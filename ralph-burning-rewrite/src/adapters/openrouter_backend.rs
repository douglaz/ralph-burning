use std::time::Duration;

use chrono::Utc;
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::contexts::agent_execution::model::{
    InvocationContract, InvocationEnvelope, InvocationMetadata, InvocationRequest,
    RawOutputReference, TokenCounts,
};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::shared::domain::{BackendFamily, FailureClass, ResolvedBackendTarget};
use crate::shared::error::{AppError, AppResult};

const DEFAULT_OPENROUTER_BASE_URL: &str = "https://openrouter.ai";
const MODELS_PATH: &str = "/api/v1/models";
const CHAT_COMPLETIONS_PATH: &str = "/api/v1/chat/completions";
const AVAILABILITY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct OpenRouterBackendAdapter {
    base_url: String,
}

impl Default for OpenRouterBackendAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenRouterBackendAdapter {
    pub fn new() -> Self {
        Self {
            base_url: normalize_base_url(
                std::env::var("OPENROUTER_BASE_URL")
                    .unwrap_or_else(|_| DEFAULT_OPENROUTER_BASE_URL.to_owned()),
            ),
        }
    }

    #[cfg(test)]
    fn with_base_url(base_url: impl Into<String>) -> Self {
        Self {
            base_url: normalize_base_url(base_url.into()),
        }
    }

    fn models_url(&self) -> String {
        format!("{}{}", self.base_url, MODELS_PATH)
    }

    fn chat_completions_url(&self) -> String {
        format!("{}{}", self.base_url, CHAT_COMPLETIONS_PATH)
    }

    fn api_key() -> AppResult<String> {
        match std::env::var("OPENROUTER_API_KEY") {
            Ok(value) if !value.trim().is_empty() => Ok(value),
            _ => Err(AppError::BackendUnavailable {
                backend: BackendFamily::OpenRouter.to_string(),
                details: "OPENROUTER_API_KEY is not set".to_owned(),
            }),
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

    fn invocation_failed(
        request: &InvocationRequest,
        failure_class: FailureClass,
        details: impl Into<String>,
    ) -> AppError {
        AppError::InvocationFailed {
            backend: request.resolved_target.backend.family.to_string(),
            contract_id: request.contract.label(),
            failure_class,
            details: details.into(),
        }
    }

    fn build_client(timeout: Duration) -> AppResult<reqwest::Client> {
        reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| AppError::InvocationFailed {
                backend: BackendFamily::OpenRouter.to_string(),
                contract_id: "client_init".to_owned(),
                failure_class: FailureClass::TransportFailure,
                details: format!("failed to build HTTP client: {error}"),
            })
    }

    fn system_message(contract: &InvocationContract) -> String {
        format!(
            "You are the OpenRouter backend adapter for ralph-burning. Return only valid JSON matching the '{}' contract. Do not emit markdown fences or prose outside the JSON object.",
            contract.label()
        )
    }

    fn user_message(request: &InvocationRequest) -> String {
        let mut content = String::new();
        content.push_str(&format!("Contract: {}\n", request.contract.label()));
        content.push_str(&format!("Role: {}\n\n", request.role.display_name()));
        content.push_str(&request.payload.prompt);
        content.push('\n');

        if !request.payload.context.is_null() {
            content.push_str("\n--- Context JSON ---\n");
            content.push_str(
                &serde_json::to_string_pretty(&request.payload.context)
                    .unwrap_or_else(|_| request.payload.context.to_string()),
            );
            content.push('\n');
        }

        content
    }

    fn request_body(request: &InvocationRequest) -> Value {
        json!({
            "model": request.resolved_target.model.model_id,
            "messages": [
                {
                    "role": "system",
                    "content": Self::system_message(&request.contract),
                },
                {
                    "role": "user",
                    "content": Self::user_message(request),
                }
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "stage_output",
                    "strict": true,
                    "schema": request.contract.json_schema_value(),
                }
            }
        })
    }

    async fn send_probe(&self, api_key: &str) -> AppResult<()> {
        let client = reqwest::Client::builder()
            .timeout(AVAILABILITY_TIMEOUT)
            .build()
            .map_err(|error| AppError::BackendUnavailable {
                backend: BackendFamily::OpenRouter.to_string(),
                details: format!("failed to build HTTP client: {error}"),
            })?;

        let response = client
            .get(self.models_url())
            .bearer_auth(api_key)
            .send()
            .await
            .map_err(|error| AppError::BackendUnavailable {
                backend: BackendFamily::OpenRouter.to_string(),
                details: format!("availability probe failed: {error}"),
            })?;

        if response.status().is_success() {
            return Ok(());
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(AppError::BackendUnavailable {
            backend: BackendFamily::OpenRouter.to_string(),
            details: format_http_error_details(status, &body),
        })
    }

    async fn send_completion(
        &self,
        request: &InvocationRequest,
        api_key: &str,
    ) -> AppResult<(StatusCode, String)> {
        if request.cancellation_token.is_cancelled() {
            return Err(AppError::InvocationCancelled {
                backend: request.resolved_target.backend.family.to_string(),
                contract_id: request.contract.label(),
            });
        }

        let client = Self::build_client(request.timeout).map_err(|error| match error {
            AppError::InvocationFailed { details, .. } => {
                Self::invocation_failed(request, FailureClass::TransportFailure, details)
            }
            other => {
                Self::invocation_failed(request, FailureClass::TransportFailure, other.to_string())
            }
        })?;
        let request_body = Self::request_body(request);

        let http_future = async {
            let response = client
                .post(self.chat_completions_url())
                .bearer_auth(api_key)
                .json(&request_body)
                .send()
                .await
                .map_err(|error| map_reqwest_error(request, error, "request failed"))?;

            let status = response.status();
            let body = response.text().await.map_err(|error| {
                map_reqwest_error(request, error, "failed to read response body")
            })?;

            Ok((status, body))
        };

        tokio::select! {
            _ = request.cancellation_token.cancelled() => Err(AppError::InvocationCancelled {
                backend: request.resolved_target.backend.family.to_string(),
                contract_id: request.contract.label(),
            }),
            result = http_future => result,
        }
    }

    fn parse_success_response(
        request: &InvocationRequest,
        response_body: String,
    ) -> AppResult<InvocationEnvelope> {
        let parsed: ChatCompletionResponse =
            serde_json::from_str(&response_body).map_err(|error| {
                Self::invocation_failed(
                    request,
                    FailureClass::SchemaValidationFailure,
                    format!("invalid OpenRouter response JSON: {error}"),
                )
            })?;

        let Some(choice) = parsed.choices.into_iter().next() else {
            return Err(Self::invocation_failed(
                request,
                FailureClass::SchemaValidationFailure,
                "OpenRouter response did not contain any choices",
            ));
        };

        let parsed_payload =
            extract_choice_payload(&choice.message.content).map_err(|details| {
                Self::invocation_failed(request, FailureClass::SchemaValidationFailure, details)
            })?;

        Ok(InvocationEnvelope {
            raw_output_reference: RawOutputReference::Inline(response_body),
            parsed_payload,
            metadata: InvocationMetadata {
                invocation_id: request.invocation_id.clone(),
                duration: Duration::from_millis(0),
                token_counts: TokenCounts {
                    prompt_tokens: parsed.usage.as_ref().and_then(|usage| usage.prompt_tokens),
                    completion_tokens: parsed
                        .usage
                        .as_ref()
                        .and_then(|usage| usage.completion_tokens),
                    total_tokens: parsed.usage.as_ref().and_then(|usage| usage.total_tokens),
                },
                backend_used: request.resolved_target.backend.clone(),
                model_used: request.resolved_target.model.clone(),
                attempt_number: request.attempt_number,
                session_id: None,
                session_reused: false,
            },
            timestamp: Utc::now(),
        })
    }

    fn map_http_error(request: &InvocationRequest, status: StatusCode, body: &str) -> AppError {
        let details = format_http_error_details(status, body);
        match status.as_u16() {
            401 | 403 => AppError::BackendUnavailable {
                backend: request.resolved_target.backend.family.to_string(),
                details,
            },
            429 => Self::invocation_failed(
                request,
                FailureClass::TransportFailure,
                format!("OpenRouter rate limit: {details}"),
            ),
            500..=599 => Self::invocation_failed(
                request,
                FailureClass::TransportFailure,
                format!("OpenRouter server failure: {details}"),
            ),
            _ => Self::invocation_failed(
                request,
                FailureClass::TransportFailure,
                format!("OpenRouter request failed: {details}"),
            ),
        }
    }
}

impl AgentExecutionPort for OpenRouterBackendAdapter {
    async fn check_capability(
        &self,
        backend: &ResolvedBackendTarget,
        contract: &InvocationContract,
    ) -> AppResult<()> {
        if backend.backend.family != BackendFamily::OpenRouter {
            return Err(Self::capability_mismatch(
                backend,
                contract,
                "OpenRouterBackendAdapter only supports OpenRouter targets",
            ));
        }

        Ok(())
    }

    async fn check_availability(&self, backend: &ResolvedBackendTarget) -> AppResult<()> {
        if backend.backend.family != BackendFamily::OpenRouter {
            return Err(AppError::BackendUnavailable {
                backend: backend.backend.family.to_string(),
                details:
                    "OpenRouterBackendAdapter availability checks only support openrouter targets"
                        .to_owned(),
            });
        }

        let api_key = Self::api_key()?;
        self.send_probe(&api_key).await
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        self.check_capability(&request.resolved_target, &request.contract)
            .await?;

        let api_key = Self::api_key()?;
        let (status, response_body) = self.send_completion(&request, &api_key).await?;
        if !status.is_success() {
            return Err(Self::map_http_error(&request, status, &response_body));
        }

        Self::parse_success_response(&request, response_body)
    }

    async fn cancel(&self, _invocation_id: &str) -> AppResult<()> {
        Ok(())
    }
}

fn normalize_base_url(base_url: String) -> String {
    base_url.trim_end_matches('/').to_owned()
}

fn map_reqwest_error(
    request: &InvocationRequest,
    error: reqwest::Error,
    context: &str,
) -> AppError {
    let failure_class = if error.is_timeout() {
        FailureClass::Timeout
    } else {
        FailureClass::TransportFailure
    };

    OpenRouterBackendAdapter::invocation_failed(
        request,
        failure_class,
        format!("{context}: {error}"),
    )
}

fn extract_choice_payload(content: &Value) -> Result<Value, String> {
    match content {
        Value::String(text) => serde_json::from_str(text)
            .map_err(|error| format!("assistant content was not valid JSON: {error}")),
        Value::Object(_) => Ok(content.clone()),
        Value::Array(parts) => {
            let text = parts
                .iter()
                .filter_map(|part| part.get("text").and_then(Value::as_str))
                .collect::<String>();
            if text.is_empty() {
                return Err("assistant content array did not contain any text parts".to_owned());
            }
            serde_json::from_str(&text)
                .map_err(|error| format!("assistant content text was not valid JSON: {error}"))
        }
        other => Err(format!(
            "assistant content had unsupported shape: {}",
            other
        )),
    }
}

fn format_http_error_details(status: StatusCode, body: &str) -> String {
    let body = body.trim();
    let body_details = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(extract_error_message)
        .filter(|message| !message.is_empty())
        .unwrap_or_else(|| {
            if body.is_empty() {
                "empty response body".to_owned()
            } else {
                body.to_owned()
            }
        });

    format!("HTTP {}: {}", status.as_u16(), body_details)
}

fn extract_error_message(body: Value) -> Option<String> {
    if let Some(message) = body
        .get("error")
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
    {
        return Some(message.to_owned());
    }

    body.get("message")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
    usage: Option<OpenRouterUsage>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    content: Value,
}

#[derive(Debug, Deserialize)]
struct OpenRouterUsage {
    prompt_tokens: Option<u32>,
    completion_tokens: Option<u32>,
    total_tokens: Option<u32>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Arc, Mutex};
    use std::thread;

    use tempfile::tempdir;

    use super::*;
    use crate::adapters::process_backend::ProcessBackendAdapter;
    use crate::adapters::BackendAdapter;
    use crate::contexts::agent_execution::model::{
        CancellationToken, InvocationContract, InvocationPayload,
    };
    use crate::contexts::agent_execution::service::AgentExecutionPort;
    use crate::contexts::workflow_composition::contracts::contract_for_stage;
    use crate::shared::domain::{BackendRole, SessionPolicy, StageId};

    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[derive(Clone)]
    struct ResponsePlan {
        status: u16,
        body: String,
        content_type: &'static str,
        delay: Duration,
    }

    impl ResponsePlan {
        fn json(status: u16, body: Value) -> Self {
            Self {
                status,
                body: serde_json::to_string(&body).expect("serialize mock response"),
                content_type: "application/json",
                delay: Duration::ZERO,
            }
        }

        fn text(status: u16, body: &str) -> Self {
            Self {
                status,
                body: body.to_owned(),
                content_type: "text/plain",
                delay: Duration::ZERO,
            }
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = delay;
            self
        }
    }

    #[derive(Debug, Clone)]
    struct RecordedRequest {
        method: String,
        path: String,
        headers: HashMap<String, String>,
        body: String,
    }

    impl RecordedRequest {
        fn json_body(&self) -> Value {
            serde_json::from_str(&self.body).expect("request body should be valid JSON")
        }
    }

    struct MockHttpServer {
        base_url: String,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        handle: Option<thread::JoinHandle<()>>,
    }

    impl MockHttpServer {
        fn start(plans: Vec<ResponsePlan>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
            let address = listener.local_addr().expect("mock server address");
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_clone = Arc::clone(&requests);

            let handle = thread::spawn(move || {
                for plan in plans {
                    let (mut stream, _) = listener.accept().expect("accept request");
                    let request = read_http_request(&mut stream).expect("read HTTP request");
                    requests_clone
                        .lock()
                        .expect("requests lock poisoned")
                        .push(request);

                    if !plan.delay.is_zero() {
                        thread::sleep(plan.delay);
                    }

                    let response = format!(
                        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        plan.status,
                        reason_phrase(plan.status),
                        plan.content_type,
                        plan.body.len(),
                        plan.body
                    );
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.flush();
                }
            });

            Self {
                base_url: format!("http://{}", address),
                requests,
                handle: Some(handle),
            }
        }

        fn requests(&self) -> Vec<RecordedRequest> {
            self.requests
                .lock()
                .expect("requests lock poisoned")
                .clone()
        }
    }

    impl Drop for MockHttpServer {
        fn drop(&mut self) {
            if let Some(handle) = self.handle.take() {
                handle.join().expect("join mock server thread");
            }
        }
    }

    fn request_fixture(
        backend_family: BackendFamily,
        model_id: &str,
    ) -> (tempfile::TempDir, InvocationRequest) {
        let temp_dir = tempdir().expect("create temp dir");
        let project_root = temp_dir.path().join("project-alpha");
        fs::create_dir_all(project_root.join("runtime/backend")).expect("create runtime/backend");
        fs::write(project_root.join("sessions.json"), r#"{"sessions":[]}"#)
            .expect("write sessions.json");

        let request = InvocationRequest {
            invocation_id: "openrouter-invoke-1".to_owned(),
            project_root: project_root.clone(),
            working_dir: project_root,
            contract: InvocationContract::Requirements {
                label: "requirements:question_set".to_owned(),
            },
            role: BackendRole::Planner,
            resolved_target: ResolvedBackendTarget::new(backend_family, model_id),
            payload: InvocationPayload {
                prompt: "Generate clarifying questions".to_owned(),
                context: json!({"idea": "Build a backend"}),
            },
            timeout: Duration::from_millis(200),
            cancellation_token: CancellationToken::new(),
            session_policy: SessionPolicy::NewSession,
            prior_session: None,
            attempt_number: 1,
        };

        (temp_dir, request)
    }

    fn set_openrouter_env(base_url: &str) {
        std::env::set_var("OPENROUTER_API_KEY", "test-openrouter-key");
        std::env::set_var("OPENROUTER_BASE_URL", base_url);
    }

    fn clear_openrouter_env() {
        std::env::remove_var("OPENROUTER_API_KEY");
        std::env::remove_var("OPENROUTER_BASE_URL");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn availability_probe_requires_api_key_and_calls_models_endpoint() {
        let _env_guard = ENV_MUTEX.lock().expect("env lock");
        clear_openrouter_env();
        let server = MockHttpServer::start(vec![ResponsePlan::json(
            200,
            json!({"data": [{"id": "model-1"}]}),
        )]);
        let adapter = OpenRouterBackendAdapter::with_base_url(server.base_url.clone());
        let (_dir, request) = request_fixture(BackendFamily::OpenRouter, "openai/gpt-5");

        let missing_key = adapter
            .check_availability(&request.resolved_target)
            .await
            .expect_err("missing API key should fail");
        assert!(matches!(missing_key, AppError::BackendUnavailable { .. }));

        set_openrouter_env(&server.base_url);
        adapter
            .check_availability(&request.resolved_target)
            .await
            .expect("availability probe should succeed");

        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "GET");
        assert_eq!(requests[0].path, MODELS_PATH);
        assert_eq!(
            requests[0]
                .headers
                .get("authorization")
                .expect("authorization header"),
            "Bearer test-openrouter-key"
        );
        clear_openrouter_env();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invoke_serializes_request_and_maps_usage_tokens() {
        let _env_guard = ENV_MUTEX.lock().expect("env lock");
        let server = MockHttpServer::start(vec![ResponsePlan::json(
            200,
            json!({
                "choices": [{
                    "message": {
                        "content": "{\"questions\":[{\"id\":\"q1\",\"prompt\":\"What should it do?\",\"rationale\":\"Scope\",\"required\":true}]}"
                    }
                }],
                "usage": {
                    "prompt_tokens": 11,
                    "completion_tokens": 7,
                    "total_tokens": 18
                }
            }),
        )]);
        set_openrouter_env(&server.base_url);
        let adapter = OpenRouterBackendAdapter::with_base_url(server.base_url.clone());
        let (_dir, request) =
            request_fixture(BackendFamily::OpenRouter, "anthropic/claude-3.5-sonnet");

        let envelope = adapter
            .invoke(request.clone())
            .await
            .expect("invoke succeeds");
        assert_eq!(envelope.parsed_payload["questions"][0]["id"], json!("q1"));
        assert_eq!(envelope.metadata.token_counts.prompt_tokens, Some(11));
        assert_eq!(envelope.metadata.token_counts.completion_tokens, Some(7));
        assert_eq!(envelope.metadata.token_counts.total_tokens, Some(18));
        assert_eq!(envelope.metadata.session_id, None);
        assert!(!envelope.metadata.session_reused);

        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        let body = requests[0].json_body();
        assert_eq!(body["model"], json!("anthropic/claude-3.5-sonnet"));
        assert_eq!(body["messages"].as_array().expect("messages").len(), 2);
        assert_eq!(body["response_format"]["type"], json!("json_object"));
        assert_eq!(
            body["response_format"]["json_schema"],
            request.contract.json_schema_value()
        );
        clear_openrouter_env();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invoke_maps_http_errors_and_malformed_bodies() {
        let _env_guard = ENV_MUTEX.lock().expect("env lock");
        let (_dir, request) = request_fixture(BackendFamily::OpenRouter, "openai/gpt-5");

        let unauthorized_server = MockHttpServer::start(vec![ResponsePlan::json(
            401,
            json!({"error": {"message": "invalid API key"}}),
        )]);
        set_openrouter_env(&unauthorized_server.base_url);
        let adapter = OpenRouterBackendAdapter::with_base_url(unauthorized_server.base_url.clone());
        let unauthorized = adapter
            .invoke(request.clone())
            .await
            .expect_err("401 should fail");
        assert!(matches!(unauthorized, AppError::BackendUnavailable { .. }));

        let rate_limited_server = MockHttpServer::start(vec![ResponsePlan::json(
            429,
            json!({"error": {"message": "too many requests"}}),
        )]);
        set_openrouter_env(&rate_limited_server.base_url);
        let adapter = OpenRouterBackendAdapter::with_base_url(rate_limited_server.base_url.clone());
        let rate_limited = adapter
            .invoke(request.clone())
            .await
            .expect_err("429 should fail");
        match rate_limited {
            AppError::InvocationFailed {
                failure_class,
                details,
                ..
            } => {
                assert_eq!(failure_class, FailureClass::TransportFailure);
                assert!(details.contains("rate limit"));
            }
            other => panic!("expected transport failure, got: {other:?}"),
        }

        let malformed_server =
            MockHttpServer::start(vec![ResponsePlan::text(200, "not-json-response-body")]);
        set_openrouter_env(&malformed_server.base_url);
        let adapter = OpenRouterBackendAdapter::with_base_url(malformed_server.base_url.clone());
        let malformed = adapter
            .invoke(request)
            .await
            .expect_err("malformed body should fail");
        match malformed {
            AppError::InvocationFailed { failure_class, .. } => {
                assert_eq!(failure_class, FailureClass::SchemaValidationFailure);
            }
            other => panic!("expected schema failure, got: {other:?}"),
        }
        clear_openrouter_env();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invoke_honors_timeout_and_cancellation() {
        let _env_guard = ENV_MUTEX.lock().expect("env lock");
        let timeout_server = MockHttpServer::start(vec![ResponsePlan::json(
            200,
            json!({
                "choices": [{
                    "message": {
                        "content": "{\"questions\":[]}"
                    }
                }]
            }),
        )
        .with_delay(Duration::from_millis(250))]);
        set_openrouter_env(&timeout_server.base_url);
        let adapter = OpenRouterBackendAdapter::with_base_url(timeout_server.base_url.clone());
        let (_dir, timeout_request) = request_fixture(BackendFamily::OpenRouter, "openai/gpt-5");

        let timeout_error = adapter
            .invoke(timeout_request)
            .await
            .expect_err("timeout should fail");
        match timeout_error {
            AppError::InvocationFailed { failure_class, .. } => {
                assert_eq!(failure_class, FailureClass::Timeout);
            }
            other => panic!("expected timeout failure, got: {other:?}"),
        }

        let cancellation_server = MockHttpServer::start(vec![ResponsePlan::json(
            200,
            json!({
                "choices": [{
                    "message": {
                        "content": "{\"questions\":[]}"
                    }
                }]
            }),
        )
        .with_delay(Duration::from_millis(250))]);
        set_openrouter_env(&cancellation_server.base_url);
        let adapter = OpenRouterBackendAdapter::with_base_url(cancellation_server.base_url.clone());
        let (_dir, mut cancel_request) = request_fixture(BackendFamily::OpenRouter, "openai/gpt-5");
        cancel_request.timeout = Duration::from_secs(1);
        let token = cancel_request.cancellation_token.clone();

        let invoke_task = tokio::spawn(async move { adapter.invoke(cancel_request).await });
        tokio::time::sleep(Duration::from_millis(25)).await;
        token.cancel();

        let cancellation_error = invoke_task
            .await
            .expect("join invoke task")
            .expect_err("cancellation should fail");
        assert!(matches!(
            cancellation_error,
            AppError::InvocationCancelled { .. }
        ));
        clear_openrouter_env();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn backend_adapter_process_variant_dispatches_openrouter_targets() {
        let _env_guard = ENV_MUTEX.lock().expect("env lock");
        let server = MockHttpServer::start(vec![
            ResponsePlan::json(200, json!({"data": [{"id": "model-1"}]})),
            ResponsePlan::json(
                200,
                json!({
                    "choices": [{
                        "message": {
                            "content": "{\"questions\":[]}"
                        }
                    }],
                    "usage": {
                        "prompt_tokens": 1,
                        "completion_tokens": 1,
                        "total_tokens": 2
                    }
                }),
            ),
        ]);
        set_openrouter_env(&server.base_url);

        let adapter = BackendAdapter::Process(ProcessBackendAdapter::new());
        let (_dir, request) =
            request_fixture(BackendFamily::OpenRouter, "anthropic/claude-3.5-sonnet");

        adapter
            .check_availability(&request.resolved_target)
            .await
            .expect("process variant should dispatch OpenRouter availability");
        let envelope = adapter
            .invoke(request)
            .await
            .expect("invoke through process variant");
        assert_eq!(envelope.parsed_payload["questions"], json!([]));

        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].path, MODELS_PATH);
        assert_eq!(requests[1].path, CHAT_COMPLETIONS_PATH);
        clear_openrouter_env();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn check_capability_accepts_workflow_and_requirements_contracts() {
        let adapter = OpenRouterBackendAdapter::new();
        let (_dir, mut request) = request_fixture(BackendFamily::OpenRouter, "openai/gpt-5");

        adapter
            .check_capability(&request.resolved_target, &request.contract)
            .await
            .expect("requirements contract should be supported");

        request.contract = InvocationContract::Stage(contract_for_stage(StageId::Planning));
        adapter
            .check_capability(&request.resolved_target, &request.contract)
            .await
            .expect("stage contract should be supported");
    }

    fn read_http_request(stream: &mut TcpStream) -> std::io::Result<RecordedRequest> {
        stream.set_read_timeout(Some(Duration::from_secs(3)))?;
        let mut buffer = Vec::new();
        let mut headers_end = None;
        let mut content_length = 0usize;
        let mut chunk = [0u8; 1024];

        loop {
            let bytes_read = stream.read(&mut chunk)?;
            if bytes_read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..bytes_read]);

            if headers_end.is_none() {
                if let Some(position) = find_header_terminator(&buffer) {
                    headers_end = Some(position);
                    content_length = parse_content_length(&buffer[..position])?;
                }
            }

            if let Some(position) = headers_end {
                if buffer.len() >= position + content_length {
                    break;
                }
            }
        }

        let headers_end = headers_end.expect("headers should be present");
        let headers_text = String::from_utf8_lossy(&buffer[..headers_end]);
        let mut lines = headers_text.lines();
        let request_line = lines.next().expect("request line");
        let mut request_line_parts = request_line.split_whitespace();
        let method = request_line_parts.next().unwrap_or_default().to_owned();
        let path = request_line_parts.next().unwrap_or_default().to_owned();

        let mut headers = HashMap::new();
        for line in lines {
            if let Some((key, value)) = line.split_once(':') {
                headers.insert(key.trim().to_ascii_lowercase(), value.trim().to_owned());
            }
        }

        let body = String::from_utf8_lossy(&buffer[headers_end..headers_end + content_length])
            .into_owned();

        Ok(RecordedRequest {
            method,
            path,
            headers,
            body,
        })
    }

    fn find_header_terminator(buffer: &[u8]) -> Option<usize> {
        buffer
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|position| position + 4)
    }

    fn parse_content_length(headers: &[u8]) -> std::io::Result<usize> {
        let headers_text = String::from_utf8_lossy(headers);
        for line in headers_text.lines() {
            if let Some((key, value)) = line.split_once(':') {
                if key.eq_ignore_ascii_case("content-length") {
                    return value
                        .trim()
                        .parse::<usize>()
                        .map_err(|error| std::io::Error::other(error.to_string()));
                }
            }
        }

        Ok(0)
    }

    fn reason_phrase(status: u16) -> &'static str {
        match status {
            200 => "OK",
            401 => "Unauthorized",
            403 => "Forbidden",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            _ => "OK",
        }
    }
}
