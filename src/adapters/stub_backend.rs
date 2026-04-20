use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use serde_json::{json, Value};

use crate::contexts::agent_execution::model::{
    InvocationContract, InvocationEnvelope, InvocationMetadata, InvocationRequest,
    RawOutputReference, TokenCounts,
};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::shared::domain::{FailureClass, ResolvedBackendTarget, SessionPolicy, StageId};
use crate::shared::error::{AppError, AppResult};

/// A recorded invocation target, captured by `StubBackendAdapter` for test assertions.
#[derive(Debug, Clone)]
pub struct RecordedInvocation {
    pub contract_label: String,
    pub resolved_target: ResolvedBackendTarget,
}

#[derive(Clone)]
pub struct StubBackendAdapter {
    delay: Duration,
    available: bool,
    unsupported_stages: HashSet<StageId>,
    /// Stages that pass preflight but fail during invocation (for testing).
    fail_invoke_stages: HashSet<StageId>,
    transient_failure_limits: HashMap<StageId, u32>,
    transient_failure_counters: HashMap<StageId, Arc<AtomicU32>>,
    /// Stages that succeed the first N invocations, then fail.
    delayed_failure_limits: HashMap<StageId, u32>,
    delayed_failure_counters: HashMap<StageId, Arc<AtomicU32>>,
    stage_payload_overrides: Arc<Mutex<HashMap<StageId, Vec<Value>>>>,
    stage_payload_counters: HashMap<StageId, Arc<AtomicU32>>,
    /// Contract-label-keyed payload overrides for non-stage contracts.
    label_payload_overrides: Arc<Mutex<HashMap<String, Vec<Value>>>>,
    label_payload_counters: HashMap<String, Arc<AtomicU32>>,
    cancelled_invocations: Arc<Mutex<Vec<String>>>,
    recorded_invocations: Arc<Mutex<Vec<RecordedInvocation>>>,
    /// Panel round tracking: all panel members within the same stage invocation
    /// group share the same sequence index. Stores (stage_id, current_index).
    last_panel_stage_index: Arc<Mutex<Option<(StageId, usize)>>>,
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
            delayed_failure_limits: HashMap::new(),
            delayed_failure_counters: HashMap::new(),
            stage_payload_overrides: Arc::new(Mutex::new(HashMap::new())),
            stage_payload_counters: HashMap::new(),
            label_payload_overrides: Arc::new(Mutex::new(HashMap::new())),
            label_payload_counters: HashMap::new(),
            cancelled_invocations: Arc::new(Mutex::new(Vec::new())),
            recorded_invocations: Arc::new(Mutex::new(Vec::new())),
            last_panel_stage_index: Arc::new(Mutex::new(None)),
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

    /// Configure a stage to succeed the first N invocations, then fail.
    pub fn with_delayed_failure(mut self, stage_id: StageId, succeed_count: u32) -> Self {
        self.delayed_failure_limits.insert(stage_id, succeed_count);
        self.delayed_failure_counters
            .insert(stage_id, Arc::new(AtomicU32::new(0)));
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

    /// Configure a contract-label-keyed payload for non-stage invocations (e.g. requirements).
    pub fn with_label_payload(self, label: impl Into<String>, payload: Value) -> Self {
        self.with_label_payload_sequence(label, vec![payload])
    }

    /// Configure a contract-label-keyed payload sequence for non-stage invocations.
    pub fn with_label_payload_sequence(
        mut self,
        label: impl Into<String>,
        payloads: Vec<Value>,
    ) -> Self {
        let label = label.into();
        self.label_payload_overrides
            .lock()
            .expect("label payload override lock poisoned")
            .insert(label.clone(), payloads);
        self.label_payload_counters
            .insert(label, Arc::new(AtomicU32::new(0)));
        self
    }

    pub fn cancelled_invocations(&self) -> Vec<String> {
        self.cancelled_invocations
            .lock()
            .expect("cancelled invocation lock poisoned")
            .clone()
    }

    /// Return a clone of the recorded invocation targets for test assertions.
    pub fn recorded_invocations(&self) -> Vec<RecordedInvocation> {
        self.recorded_invocations
            .lock()
            .expect("recorded invocation lock poisoned")
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

    fn payload_for_label(&self, label: &str) -> Value {
        let overrides = self
            .label_payload_overrides
            .lock()
            .expect("label payload override lock poisoned");
        let Some(payloads) = overrides.get(label) else {
            return canned_requirements_payload(label);
        };
        let Some(counter) = self.label_payload_counters.get(label) else {
            return payloads
                .last()
                .cloned()
                .unwrap_or_else(|| canned_requirements_payload(label));
        };
        let idx = counter.fetch_add(1, Ordering::SeqCst) as usize;
        payloads
            .get(idx)
            .cloned()
            .or_else(|| payloads.last().cloned())
            .unwrap_or_else(|| canned_requirements_payload(label))
    }

    /// Return the stage override payload for a panel invocation using round-based
    /// indexing: all panel members within the same stage invocation group share
    /// the same sequence index. The counter only advances on the FIRST panel
    /// member invocation for each new round.
    fn panel_payload_for_stage(&self, stage_id: StageId) -> Value {
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

        // Check if we're in the same panel group (same stage_id) as the last
        // panel invocation. If so, reuse the same index. Otherwise, advance.
        let mut last = self
            .last_panel_stage_index
            .lock()
            .expect("last panel stage index lock poisoned");
        let idx = match *last {
            Some((last_stage, last_idx)) if last_stage == stage_id => last_idx,
            _ => {
                let new_idx = counter.fetch_add(1, Ordering::SeqCst) as usize;
                *last = Some((stage_id, new_idx));
                new_idx
            }
        };

        payloads
            .get(idx)
            .cloned()
            .or_else(|| payloads.last().cloned())
            .unwrap_or_else(|| canned_payload_for_stage(stage_id))
    }

    fn payload_for_request(&self, request: &InvocationRequest) -> Value {
        match &request.contract {
            InvocationContract::Stage(sc) => {
                // Clear panel group tracking on non-panel invocations so that
                // the next panel invocation starts a new group.
                *self
                    .last_panel_stage_index
                    .lock()
                    .expect("last panel stage index lock poisoned") = None;
                self.payload_for_stage(sc.stage_id)
            }
            InvocationContract::Requirements { label } => {
                *self
                    .last_panel_stage_index
                    .lock()
                    .expect("last panel stage index lock poisoned") = None;
                self.payload_for_label(label)
            }
            InvocationContract::Panel { stage_id, role } => {
                let contract_label = request.contract.label();
                {
                    let overrides = self
                        .label_payload_overrides
                        .lock()
                        .expect("label payload override lock poisoned");
                    if overrides.contains_key(&contract_label) {
                        drop(overrides);
                        return self.payload_for_label(&contract_label);
                    }
                }
                // Check for stage-level overrides first (preserves existing test seam).
                {
                    let overrides = self
                        .stage_payload_overrides
                        .lock()
                        .expect("stage payload override lock poisoned");
                    if overrides.contains_key(stage_id) {
                        drop(overrides);
                        // Use round-based indexing so all panel members in the
                        // same round get the same payload.
                        let raw = self.panel_payload_for_stage(*stage_id);
                        // Translate old validation format to panel format if needed.
                        return translate_to_panel_payload(
                            *stage_id,
                            role,
                            raw,
                            &request.payload.prompt,
                        );
                    }
                }
                canned_panel_payload(*stage_id, role, &request.payload.prompt)
            }
        }
    }
}

impl AgentExecutionPort for StubBackendAdapter {
    async fn check_capability(
        &self,
        backend: &crate::shared::domain::ResolvedBackendTarget,
        contract: &InvocationContract,
    ) -> AppResult<()> {
        match contract {
            InvocationContract::Stage(sc) => {
                if self.unsupported_stages.contains(&sc.stage_id)
                    || !backend.supports_stage(sc.stage_id)
                {
                    return Err(AppError::CapabilityMismatch {
                        backend: backend.backend.family.to_string(),
                        contract_id: sc.stage_id.to_string(),
                        details: "stub adapter does not support the requested stage".to_owned(),
                    });
                }
            }
            InvocationContract::Requirements { .. } | InvocationContract::Panel { .. } => {
                // Requirements and panel contracts are always supported by the stub.
            }
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
                failure_class: None,
            })
        }
    }

    async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
        let contract_label = request.contract.label();

        // Record the invocation target for test assertions.
        self.recorded_invocations
            .lock()
            .expect("recorded invocation lock poisoned")
            .push(RecordedInvocation {
                contract_label: contract_label.clone(),
                resolved_target: request.resolved_target.clone(),
            });

        if request.cancellation_token.is_cancelled() {
            return Err(AppError::InvocationCancelled {
                backend: request.resolved_target.backend.family.to_string(),
                contract_id: contract_label,
            });
        }

        // Transient failure simulation (stage contracts only)
        if let Some(stage_id) = request.contract.stage_id() {
            if let Some(limit) = self.transient_failure_limits.get(&stage_id) {
                let counter = self
                    .transient_failure_counters
                    .get(&stage_id)
                    .expect("transient failure counter missing");
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt <= *limit {
                    return Err(AppError::InvocationFailed {
                        backend: request.resolved_target.backend.family.to_string(),
                        contract_id: contract_label,
                        failure_class: FailureClass::TransportFailure,
                        details: format!(
                            "stub adapter configured to fail invocation {attempt}/{limit} for this stage"
                        ),
                    });
                }
            }

            // Delayed failure: succeed the first N invocations, then fail.
            if let Some(limit) = self.delayed_failure_limits.get(&stage_id) {
                let counter = self
                    .delayed_failure_counters
                    .get(&stage_id)
                    .expect("delayed failure counter missing");
                let attempt = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt > *limit {
                    return Err(AppError::InvocationFailed {
                        backend: request.resolved_target.backend.family.to_string(),
                        contract_id: contract_label,
                        failure_class: FailureClass::TransportFailure,
                        details: format!(
                            "stub adapter configured to fail after {limit} successful invocations (attempt {attempt})"
                        ),
                    });
                }
            }

            if self.fail_invoke_stages.contains(&stage_id) {
                return Err(AppError::InvocationFailed {
                    backend: request.resolved_target.backend.family.to_string(),
                    contract_id: contract_label,
                    failure_class: FailureClass::TransportFailure,
                    details: "stub adapter configured to fail invocation for this stage".to_owned(),
                });
            }
        }

        if self.delay > Duration::from_millis(0) {
            tokio::select! {
                _ = request.cancellation_token.cancelled() => {
                    return Err(AppError::InvocationCancelled {
                        backend: request.resolved_target.backend.family.to_string(),
                        contract_id: contract_label,
                    });
                }
                _ = tokio::time::sleep(self.delay) => {}
            }
        }

        let payload = self.payload_for_request(&request);
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
                .or_else(|| Some(format!("stub-{}-{}", request.role.as_str(), contract_label)))
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
                    cache_read_tokens: None,
                    cache_creation_tokens: None,
                },
                backend_used: request.resolved_target.backend,
                model_used: request.resolved_target.model,
                adapter_reported_backend: None,
                adapter_reported_model: None,
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

/// Translate old-format stage override payloads to panel-specific format when needed.
/// This allows existing test overrides using the validation schema (`"outcome"`, etc.)
/// to work transparently with the new panel dispatch path.
fn translate_to_panel_payload(stage_id: StageId, role: &str, raw: Value, prompt: &str) -> Value {
    match (stage_id, role) {
        (StageId::CompletionPanel, "completer") => {
            if raw.get("vote_complete").is_some() {
                return raw; // already in panel format
            }
            let vote_complete = raw
                .get("outcome")
                .and_then(|v| v.as_str())
                .map(|outcome| outcome == "approved")
                .unwrap_or(false);
            let evidence = raw.get("evidence").cloned().unwrap_or(json!([]));
            let remaining_work = raw
                .get("follow_up_or_amendments")
                .cloned()
                .unwrap_or(json!([]));
            json!({
                "vote_complete": vote_complete,
                "evidence": evidence,
                "remaining_work": remaining_work
            })
        }
        (StageId::PromptReview, "refiner") => {
            if raw.get("refined_prompt").is_some() {
                return raw; // already in panel format
            }
            let summary = raw
                .get("problem_framing")
                .and_then(|v| v.as_str())
                .unwrap_or("No changes")
                .to_string();
            let refined_prompt = extract_canonical_prompt_from_review_prompt(prompt)
                .unwrap_or_else(|| "Stub refined prompt text.".to_owned());
            json!({
                "refined_prompt": refined_prompt,
                "refinement_summary": summary,
                "improvements": []
            })
        }
        (StageId::PromptReview, "validator") => {
            if raw.get("accepted").is_some() {
                return raw; // already in panel format
            }
            // Honour old-format readiness.ready field: if explicitly false, reject.
            let accepted = raw
                .get("readiness")
                .and_then(|r| r.get("ready"))
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let concerns = raw
                .get("readiness")
                .and_then(|r| r.get("risks"))
                .cloned()
                .unwrap_or(json!([]));
            json!({
                "accepted": accepted,
                "evidence": ["Stub validation: translated from old format."],
                "concerns": concerns
            })
        }
        (StageId::FinalReview, "reviewer") => {
            if raw.get("amendments").is_some() {
                return raw;
            }
            let amendments = raw
                .get("follow_up_or_amendments")
                .and_then(|value| value.as_array())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|value| value.as_str().map(|body| json!({ "body": body })))
                .collect::<Vec<_>>();
            json!({
                "summary": "Stub final-review proposal translated from validation payload.",
                "amendments": amendments,
            })
        }
        (StageId::FinalReview, "voter") => {
            if raw.get("votes").is_some() {
                return raw;
            }
            let amendment_ids = extract_final_review_amendment_ids(prompt);
            let decision = "accept";
            json!({
                "summary": "Stub final-review votes translated from validation payload.",
                "votes": amendment_ids
                    .into_iter()
                    .map(|amendment_id| json!({
                        "amendment_id": amendment_id,
                        "decision": decision,
                        "rationale": "Stub reviewer/planner vote.",
                    }))
                    .collect::<Vec<_>>(),
            })
        }
        (StageId::FinalReview, "arbiter") => {
            if raw.get("rulings").is_some() {
                return raw;
            }
            let amendment_ids = extract_final_review_amendment_ids(prompt);
            json!({
                "summary": "Stub final-review arbiter translated from validation payload.",
                "rulings": amendment_ids
                    .into_iter()
                    .map(|amendment_id| json!({
                        "amendment_id": amendment_id,
                        "decision": "accept",
                        "rationale": "Stub arbiter accepts the disputed amendment.",
                    }))
                    .collect::<Vec<_>>(),
            })
        }
        _ => raw,
    }
}

fn extract_final_review_amendment_ids(prompt: &str) -> Vec<String> {
    prompt
        .lines()
        .filter_map(|line| line.strip_prefix("## Amendment: "))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn extract_canonical_prompt_from_review_prompt(prompt: &str) -> Option<String> {
    let marker = crate::contexts::project_run_record::task_prompt_contract::contract_marker();
    let marker_index = prompt.find(&marker)?;
    let trailing = &prompt[marker_index..];
    let end_marker = "\n\n## Authoritative JSON Schema";
    let end_index = trailing.find(end_marker).unwrap_or(trailing.len());
    let candidate = trailing[..end_index].trim_end();
    if crate::contexts::project_run_record::task_prompt_contract::validate_canonical_prompt_shape(
        candidate,
    )
    .is_ok()
    {
        Some(candidate.to_owned())
    } else {
        None
    }
}

/// Deterministic canned payloads for panel contracts (prompt-review and completion).
fn canned_panel_payload(stage_id: StageId, role: &str, prompt: &str) -> serde_json::Value {
    match (stage_id, role) {
        (StageId::PromptReview, "refiner") => json!({
            "refined_prompt": extract_canonical_prompt_from_review_prompt(prompt)
                .unwrap_or_else(|| "Stub refined prompt text.".to_owned()),
            "refinement_summary": "No substantive changes needed.",
            "improvements": []
        }),
        (StageId::PromptReview, "validator") => json!({
            "accepted": true,
            "evidence": ["Stub validation: prompt is acceptable."],
            "concerns": []
        }),
        (StageId::CompletionPanel, "completer") => json!({
            "vote_complete": true,
            "evidence": ["Stub completion vote: work is complete."],
            "remaining_work": []
        }),
        (StageId::FinalReview, "reviewer") => json!({
            "summary": "Stub final-review pass found no amendments.",
            "amendments": []
        }),
        (StageId::FinalReview, "voter") => json!({
            "summary": "Stub final-review voter pass.",
            "votes": []
        }),
        (StageId::FinalReview, "arbiter") => json!({
            "summary": "Stub final-review arbiter pass.",
            "rulings": []
        }),
        _ => canned_payload_for_stage(stage_id),
    }
}

/// Deterministic canned payloads for requirements contracts.
fn canned_requirements_payload(label: &str) -> serde_json::Value {
    if label.contains("question_set") {
        json!({
            "questions": []
        })
    } else if label.contains("requirements_draft") {
        json!({
            "problem_summary": "Stub requirements draft",
            "goals": ["Deliver the feature"],
            "non_goals": [],
            "constraints": [],
            "acceptance_criteria": ["Feature works as specified"],
            "risks_or_open_questions": [],
            "recommended_flow": "standard"
        })
    } else if label.contains("requirements_review") {
        json!({
            "outcome": "approved",
            "evidence": ["Stub review evidence"],
            "findings": [],
            "follow_ups": []
        })
    } else if label.contains("project_seed") {
        json!({
            "version": 2,
            "project_id": "stub-project",
            "project_name": "Stub Project",
            "flow": "standard",
            "prompt_body": "Stub prompt body for the project.",
            "handoff_summary": "Stub handoff summary.",
            "follow_ups": []
        })
    } else if label.contains("milestone_bundle") {
        json!({
            "schema_version": 1,
            "identity": {
                "id": "ms-stub",
                "name": "Stub Milestone"
            },
            "executive_summary": "Stub milestone plan summary.",
            "goals": ["Deliver the milestone"],
            "non_goals": [],
            "constraints": [],
            "acceptance_map": [{
                "id": "AC-1",
                "description": "Milestone can be executed from structured plan output.",
                "covered_by": ["ms-stub.bead-1"]
            }],
            "workstreams": [{
                "name": "Planning",
                "description": "Stub workstream",
                "beads": [{
                    "bead_id": "ms-stub.bead-1",
                    "title": "Stub bead",
                    "description": "Carry the milestone plan into execution.",
                    "bead_type": "task",
                    "priority": 1,
                    "labels": ["phase1"],
                    "depends_on": [],
                    "acceptance_criteria": ["AC-1"],
                    "flow_override": null
                }]
            }],
            "default_flow": "minimal",
            "agents_guidance": "Follow the milestone plan."
        })
    } else if label.contains("ideation") {
        json!({
            "themes": ["Core feature delivery"],
            "key_concepts": ["Structured pipeline"],
            "initial_scope": "Stub ideation scope for the project.",
            "open_questions": []
        })
    } else if label.contains("research") {
        json!({
            "findings": [{
                "area": "Technical context",
                "summary": "Stub research finding",
                "relevance": "Directly relevant"
            }],
            "constraints_discovered": [],
            "prior_art": [],
            "technical_context": "Stub technical context for research."
        })
    } else if label.contains("synthesis") {
        json!({
            "problem_summary": "Stub synthesis summary",
            "goals": ["Deliver the feature"],
            "non_goals": [],
            "constraints": [],
            "acceptance_criteria": ["Feature works as specified"],
            "risks_or_open_questions": [],
            "recommended_flow": "standard"
        })
    } else if label.contains("implementation_spec") {
        json!({
            "architecture_overview": "Stub architecture overview.",
            "components": [{
                "name": "Core",
                "responsibility": "Main component",
                "interfaces": ["public API"]
            }],
            "integration_points": [],
            "migration_notes": []
        })
    } else if label.contains("gap_analysis") {
        json!({
            "gaps": [],
            "coverage_assessment": "Stub coverage assessment — no gaps found.",
            "blocking_gaps": []
        })
    } else if label.contains("validation") {
        json!({
            "outcome": "pass",
            "evidence": ["Stub validation evidence"],
            "blocking_issues": [],
            "missing_information": []
        })
    } else {
        json!({
            "stub": true,
            "label": label
        })
    }
}
