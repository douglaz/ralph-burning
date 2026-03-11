use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use crate::contexts::agent_execution::model::{
    InvocationEnvelope, InvocationMetadata, InvocationRequest, RawOutputReference, TokenCounts,
};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::shared::domain::{SessionPolicy, StageId};
use crate::shared::error::{AppError, AppResult};

#[derive(Clone)]
pub struct StubBackendAdapter {
    delay: Duration,
    available: bool,
    unsupported_stages: HashSet<StageId>,
    cancelled_invocations: Arc<Mutex<Vec<String>>>,
}

impl Default for StubBackendAdapter {
    fn default() -> Self {
        Self {
            delay: Duration::from_millis(0),
            available: true,
            unsupported_stages: HashSet::new(),
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

    pub fn cancelled_invocations(&self) -> Vec<String> {
        self.cancelled_invocations
            .lock()
            .expect("cancelled invocation lock poisoned")
            .clone()
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

        let payload = canned_payload_for_stage(request.stage_contract.stage_id);
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
