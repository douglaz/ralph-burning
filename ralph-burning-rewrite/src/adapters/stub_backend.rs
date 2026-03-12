use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use chrono::Utc;
use serde_json::{json, Value};

use crate::contexts::agent_execution::model::{
    InvocationEnvelope, InvocationMetadata, InvocationRequest, RawOutputReference, TokenCounts,
};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::shared::domain::{FailureClass, SessionPolicy, StageId};
use crate::shared::error::{AppError, AppResult};

#[derive(Clone)]
pub struct StubBackendAdapter {
    delay: Duration,
    available: bool,
    unsupported_stages: HashSet<StageId>,
    /// Stages that pass preflight but fail during invocation (for testing).
    fail_invoke_stages: HashSet<StageId>,
    transient_failure_limits: HashMap<StageId, u32>,
    transient_failure_counters: HashMap<StageId, Arc<AtomicU32>>,
    stage_payload_overrides: Arc<Mutex<HashMap<StageId, Vec<Value>>>>,
    stage_payload_counters: HashMap<StageId, Arc<AtomicU32>>,
    cancelled_invocations: Arc<Mutex<Vec<String>>>,
}

impl Default for StubBackendAdapter {
    fn default() -> Self {
        Self {
            delay: Duration::from_millis(0),
            available: true,
            unsupported_stages: HashSet::new(),
            fail_invoke_stages: HashSet::new(),
            transient_failure_limits: HashMap::new(),
            transient_failure_counters: HashMap::new(),
            stage_payload_overrides: Arc::new(Mutex::new(HashMap::new())),
            stage_payload_counters: HashMap::new(),
            cancelled_invocations: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl StubBackendAdapter {
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    pub fn unavailable(mut self) -> Self {
        self.available = false;
        self
    }

    pub fn without_stage_support(mut self, stage_id: StageId) -> Self {
        self.unsupported_stages.insert(stage_id);
        self
    }

    /// Configure a stage to pass preflight but fail during invocation.
    pub fn with_invoke_failure(mut self, stage_id: StageId) -> Self {
        self.fail_invoke_stages.insert(stage_id);
        self
    }

    /// Configure a stage to fail the first N invocations, then succeed.
    pub fn with_transient_failure(mut self, stage_id: StageId, fail_count: u32) -> Self {
        self.transient_failure_limits.insert(stage_id, fail_count);
        self.transient_failure_counters
            .insert(stage_id, Arc::new(AtomicU32::new(0)));
        self
    }

    pub fn with_stage_payload(self, stage_id: StageId, payload: Value) -> Self {
        self.with_stage_payload_sequence(stage_id, vec![payload])
    }

    pub fn with_stage_payload_sequence(mut self, stage_id: StageId, payloads: Vec<Value>) -> Self {
        self.stage_payload_overrides
            .lock()
            .expect("stage payload override lock poisoned")
            .insert(stage_id, payloads);
        self.stage_payload_counters
            .insert(stage_id, Arc::new(AtomicU32::new(0)));
        self
    }

    pub fn cancelled_invocations(&self) -> Vec<String> {
        self.cancelled_invocations
            .lock()
            .expect("cancelled invocation lock poisoned")
            .clone()
    }

    fn payload_for_stage(&self, stage_id: StageId) -> Value {
        let overrides = self
            .stage_payload_overrides
            .lock()
            .expect("stage payload override lock poisoned");
        let Some(payloads) = overrides.get(&stage_id) else {
            return canned_payload_for_stage(stage_id);
        };
        let Some(counter) = self.stage_payload_counters.get(&stage_id) else {
            return payloads
                .last()
                .cloned()
                .unwrap_or_else(|| canned_payload_for_stage(stage_id));
        };
        let idx = counter.fetch_add(1, Ordering::SeqCst) as usize;
        payloads
            .get(idx)
            .cloned()
            .or_else(|| payloads.last().cloned())
            .unwrap_or_else(|| canned_payload_for_stage(stage_id))
    }
}

impl AgentExecutionPort for StubBackendAdapter {
    async fn check_capability(
        &self,
        backend: &crate::shared::domain::ResolvedBackendTarget,
        stage_contract: &crate::contexts::workflow_composition::contracts::StageContract,
    ) -> AppResult<()> {
        if self.unsupported_stages.contains(&stage_contract.stage_id)
            || !backend.supports_stage(stage_contract.stage_id)
        {
            return Err(AppError::CapabilityMismatch {
                backend: backend.backend.family.to_string(),
                stage_id: stage_contract.stage_id,
                details: "stub adapter does not support the requested stage".to_owned(),
            });
        }

        Ok(())
    }

    async fn check_availability(
        &self,
        backend: &crate::shared::domain::ResolvedBackendTarget,
    ) -> AppResult<()> {
        if self.available {
            Ok(())
        } else {
            Err(AppError::BackendUnavailable {
                backend: backend.backend.family.to_string(),
                details: "stub adapter configured as unavailable".to_owned(),
            })
        }
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        if request.cancellation_token.is_cancelled() {
            return Err(AppError::InvocationCancelled {
                backend: request.resolved_target.backend.family.to_string(),
                stage_id: request.stage_contract.stage_id,
            });
        }

        if let Some(limit) = self
            .transient_failure_limits
            .get(&request.stage_contract.stage_id)
        {
            let counter = self
                .transient_failure_counters
                .get(&request.stage_contract.stage_id)
                .expect("transient failure counter missing");
            let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= *limit {
                return Err(AppError::InvocationFailed {
                    backend: request.resolved_target.backend.family.to_string(),
                    stage_id: request.stage_contract.stage_id,
                    failure_class: FailureClass::TransportFailure,
                    details: format!(
                        "stub adapter configured to fail invocation {attempt}/{limit} for this stage"
                    ),
                });
            }
        }

        if self
            .fail_invoke_stages
            .contains(&request.stage_contract.stage_id)
        {
            return Err(AppError::InvocationFailed {
                backend: request.resolved_target.backend.family.to_string(),
                stage_id: request.stage_contract.stage_id,
                failure_class: FailureClass::TransportFailure,
                details: "stub adapter configured to fail invocation for this stage".to_owned(),
            });
        }

        if self.delay > Duration::from_millis(0) {
            tokio::select! {
                _ = request.cancellation_token.cancelled() => {
                    return Err(AppError::InvocationCancelled {
                        backend: request.resolved_target.backend.family.to_string(),
                        stage_id: request.stage_contract.stage_id,
                    });
                }
                _ = tokio::time::sleep(self.delay) => {}
            }
        }

        let payload = self.payload_for_stage(request.stage_contract.stage_id);
        let raw_output = serde_json::to_string_pretty(&payload)?;
        let prompt_token_count = count_tokens(&request.payload.prompt);
        let completion_token_count = count_tokens(&raw_output);
        let prompt_tokens = Some(prompt_token_count);
        let completion_tokens = Some(completion_token_count);
        let total_tokens = Some(prompt_token_count + completion_token_count);

        let supports_reuse =
            request.role.allows_session_reuse() && request.resolved_target.supports_session_reuse();
        let session_reused = supports_reuse
            && matches!(request.session_policy, SessionPolicy::ReuseIfAllowed)
            && request.prior_session.is_some();
        let session_id = if supports_reuse {
            request
                .prior_session
                .as_ref()
                .map(|session| session.session_id.clone())
                .filter(|_| session_reused)
                .or_else(|| {
                    Some(format!(
                        "stub-{}-{}",
                        request.role.as_str(),
                        request.stage_contract.stage_id.as_str()
                    ))
                })
        } else {
            None
        };

        Ok(InvocationEnvelope {
            raw_output_reference: RawOutputReference::Inline(raw_output),
            parsed_payload: serde_json::Value::Null,
            metadata: InvocationMetadata {
                invocation_id: request.invocation_id,
                duration: Duration::from_millis(0),
                token_counts: TokenCounts {
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                },
                backend_used: request.resolved_target.backend,
                model_used: request.resolved_target.model,
                attempt_number: request.attempt_number,
                session_id,
                session_reused,
            },
            timestamp: Utc::now(),
        })
    }

    async fn cancel(&self, invocation_id: &str) -> AppResult<()> {
        self.cancelled_invocations
            .lock()
            .expect("cancelled invocation lock poisoned")
            .push(invocation_id.to_owned());
        Ok(())
    }
}

fn count_tokens(contents: &str) -> u32 {
    contents.split_whitespace().count() as u32
}

fn canned_payload_for_stage(stage_id: StageId) -> serde_json::Value {
    match stage_id {
        StageId::PromptReview | StageId::Planning | StageId::DocsPlan | StageId::CiPlan => json!({
            "problem_framing": format!("Stub planning output for {}", stage_id.display_name()),
            "assumptions_or_open_questions": [
                format!("Assumption captured for {}", stage_id.as_str())
            ],
            "proposed_work": [
                {
                    "order": 1,
                    "summary": format!("Complete {}", stage_id.display_name()),
                    "details": "Deterministic stub work item."
                }
            ],
            "readiness": {
                "ready": true,
                "risks": []
            }
        }),
        StageId::Implementation
        | StageId::PlanAndImplement
        | StageId::ApplyFixes
        | StageId::DocsUpdate
        | StageId::CiUpdate => json!({
            "change_summary": format!("Stub execution output for {}", stage_id.display_name()),
            "steps": [
                {
                    "order": 1,
                    "description": format!("Execute {}", stage_id.as_str()),
                    "status": "completed"
                }
            ],
            "validation_evidence": ["stub adapter deterministic evidence"],
            "outstanding_risks": []
        }),
        StageId::Qa
        | StageId::Review
        | StageId::CompletionPanel
        | StageId::AcceptanceQa
        | StageId::FinalReview
        | StageId::DocsValidation
        | StageId::CiValidation => json!({
            "outcome": "approved",
            "evidence": [format!("Stub validation output for {}", stage_id.display_name())],
            "findings_or_gaps": [],
            "follow_up_or_amendments": []
        }),
    }
}
