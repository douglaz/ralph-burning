#![forbid(unsafe_code)]

//! Final-review panel orchestration.
//!
//! The panel flow is:
//! 1. Collect reviewer amendment proposals.
//! 2. Normalize and deduplicate amendments by canonical body hash.
//! 3. Short-circuit completion if no amendments remain.
//! 4. Ask reviewers to vote on each amendment.
//! 5. Compute per-amendment consensus.
//! 6. Ask the arbiter to resolve only disputed amendments.
//! 7. Return the final accepted amendment set for either completion or restart.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use chrono::Utc;
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::adapters::process_backend::{
    is_timeout_related, is_transient_codex_failure, processed_contract_schema_value,
};
use crate::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use crate::contexts::agent_execution::policy::FinalReviewPanelResolution;
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::AgentExecutionService;
use crate::contexts::project_run_record::journal;
use crate::contexts::project_run_record::model::{
    ArtifactRecord, LogLevel, PayloadRecord, RuntimeLogEntry,
};
use crate::contexts::project_run_record::service::{
    JournalStorePort, PayloadArtifactWritePort, RuntimeLogWritePort,
};
use crate::contexts::project_run_record::task_prompt_contract;
use crate::contexts::workflow_composition::panel_contracts::{
    AmendmentClassification, FinalReviewAggregatePayload, FinalReviewAmendmentSource,
    FinalReviewArbiterPayload, FinalReviewCanonicalAmendment, FinalReviewProposalPayload,
    FinalReviewVoteDecision, FinalReviewVotePayload, RecordKind, RecordProducer,
};
use crate::contexts::workflow_composition::renderers;
use crate::contexts::workflow_composition::retry_policy::{
    apply_test_retry_policy_overrides, RetryPolicy,
};
use crate::contexts::workflow_composition::review_classification;
use crate::contexts::workspace_governance::template_catalog;
use crate::shared::domain::{
    BackendPolicyRole, BackendRole, FailureClass, ProjectId, ResolvedBackendTarget, RunId,
    SessionPolicy, StageCursor, StageId,
};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalAmendment {
    pub amendment_id: String,
    pub normalized_body: String,
    pub sources: Vec<FinalReviewAmendmentSource>,
    /// When set, this amendment is a planned-elsewhere finding.
    pub mapped_to_bead_id: Option<String>,
    /// Three-way classification for routing.
    pub classification: AmendmentClassification,
    /// Reviewer-provided rationale preserved for downstream routing.
    pub rationale: Option<String>,
    /// Title to use when routing an accepted propose-new-bead amendment.
    pub proposed_title: Option<String>,
    /// Scope summary to use when routing an accepted propose-new-bead amendment.
    pub proposed_scope: Option<String>,
    /// Severity used to derive bead priority for accepted propose-new-bead amendments.
    pub severity: Option<review_classification::Severity>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalReviewConsensusStatus {
    Accepted,
    Rejected,
    Disputed,
}

pub struct FinalReviewResult {
    pub aggregate_payload: Value,
    pub aggregate_artifact: String,
    pub final_accepted_amendments: Vec<FinalReviewAcceptedAmendment>,
    pub restart_required: bool,
    pub force_completed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalReviewAcceptedAmendment {
    pub amendment_id: String,
    pub normalized_body: String,
    pub sources: Vec<FinalReviewAmendmentSource>,
    /// When set, this amendment is classified as "planned-elsewhere".
    pub mapped_to_bead_id: Option<String>,
    /// Three-way classification for routing.
    pub classification: AmendmentClassification,
    /// Reviewer-provided rationale preserved for downstream routing.
    pub rationale: Option<String>,
    /// Title to use when routing an accepted propose-new-bead amendment.
    pub proposed_title: Option<String>,
    /// Scope summary to use when routing an accepted propose-new-bead amendment.
    pub proposed_scope: Option<String>,
    /// Severity used to derive bead priority for accepted propose-new-bead amendments.
    pub severity: Option<review_classification::Severity>,
}

struct ReviewerProposalRecord {
    member_index: usize,
    reviewer_id: String,
    required: bool,
    backend_family: String,
    model_id: String,
    target: ResolvedBackendTarget,
    payload: FinalReviewProposalPayload,
}

struct FinalReviewMemberInvocationSuccess {
    payload: Value,
    producer: RecordProducer,
    retry_count: u32,
}

struct FinalReviewMemberInvocationFailure {
    error: AppError,
    retry_count: u32,
}

pub(crate) fn final_review_reviewer_id(member_index: usize) -> String {
    format!("reviewer-{}", member_index + 1)
}

fn final_review_vote_id(member_index: usize) -> String {
    format!("vote-{}", member_index + 1)
}

fn panel_member_identity_from_producer(
    producer: &RecordProducer,
    target: &ResolvedBackendTarget,
    contract_id: &str,
    details: &str,
) -> AppResult<(String, String)> {
    let (backend_family, model_id) = super::require_agent_record_producer(
        producer,
        target.backend.family.as_str(),
        contract_id,
        details,
    )?;
    Ok((backend_family.to_owned(), model_id.to_owned()))
}

pub(crate) fn final_review_retry_policy(_role: BackendPolicyRole) -> RetryPolicy {
    let policy = apply_test_retry_policy_overrides(RetryPolicy::default_policy());
    #[cfg(test)]
    {
        policy.with_no_backoff()
    }
    #[cfg(not(test))]
    {
        policy
    }
}

fn final_review_contract_id(panel_role: &str) -> String {
    format!("final_review:{panel_role}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinalReviewRetryExhaustionKind {
    AvailabilityProbe,
    Invocation,
}

fn rewrite_transient_retry_exhaustion_error(
    error: AppError,
    reviewer_id: &str,
    target: &ResolvedBackendTarget,
    retry_count: u32,
) -> AppError {
    let prefix = format!(
        "{reviewer_id} ({}/{}) exhausted {retry_count} transient retries: ",
        target.backend.family.as_str(),
        target.model.model_id.as_str()
    );
    match error {
        AppError::InvocationFailed {
            backend,
            contract_id,
            failure_class,
            details,
        } => AppError::InvocationFailed {
            backend,
            contract_id,
            failure_class,
            details: format!("{prefix}{details}"),
        },
        AppError::InvocationTimeout {
            backend,
            contract_id,
            timeout_ms,
            details,
        } => AppError::InvocationTimeout {
            backend,
            contract_id,
            timeout_ms,
            details: format!("{prefix}{details}"),
        },
        AppError::BackendUnavailable {
            backend,
            details,
            failure_class,
        } => AppError::BackendUnavailable {
            backend,
            details: format!("{prefix}{details}"),
            failure_class: Some(failure_class.unwrap_or(FailureClass::TransportFailure)),
        },
        other => other,
    }
}

fn final_review_retry_exhaustion_kind(error: &AppError) -> Option<FinalReviewRetryExhaustionKind> {
    match error {
        AppError::InvocationFailed {
            contract_id,
            details,
            ..
        }
        | AppError::InvocationTimeout {
            contract_id,
            details,
            ..
        } if contract_id.starts_with("final_review:")
            && details.contains(" exhausted ")
            && details.contains(" transient retries:") =>
        {
            Some(FinalReviewRetryExhaustionKind::Invocation)
        }
        AppError::BackendUnavailable { details, .. }
            if details.contains(" exhausted ") && details.contains(" transient retries:") =>
        {
            Some(FinalReviewRetryExhaustionKind::AvailabilityProbe)
        }
        _ => None,
    }
}

pub(crate) fn is_final_review_invocation_retry_exhaustion_error(error: &AppError) -> bool {
    matches!(
        final_review_retry_exhaustion_kind(error),
        Some(FinalReviewRetryExhaustionKind::Invocation)
    )
}

pub(crate) fn is_final_review_availability_retry_exhaustion_error(error: &AppError) -> bool {
    matches!(
        final_review_retry_exhaustion_kind(error),
        Some(FinalReviewRetryExhaustionKind::AvailabilityProbe)
    )
}

pub(crate) fn final_review_retry_failure_class(error: &AppError) -> Option<FailureClass> {
    if is_timeout_related(error) {
        Some(FailureClass::Timeout)
    } else if is_transient_codex_failure(error) {
        Some(FailureClass::TransportFailure)
    } else {
        None
    }
}

pub(crate) async fn check_final_review_availability_with_retry<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    target: &ResolvedBackendTarget,
    role: BackendPolicyRole,
    reviewer_id: &str,
    panel_role: &str,
    cancellation_token: CancellationToken,
) -> AppResult<u32>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    check_final_review_availability_with_retry_on_adapter(
        agent_service.adapter(),
        target,
        role,
        reviewer_id,
        panel_role,
        cancellation_token,
    )
    .await
}

pub(crate) async fn check_final_review_availability_with_retry_on_adapter<A>(
    adapter: &A,
    target: &ResolvedBackendTarget,
    role: BackendPolicyRole,
    reviewer_id: &str,
    panel_role: &str,
    cancellation_token: CancellationToken,
) -> AppResult<u32>
where
    A: AgentExecutionPort,
{
    let retry_policy = final_review_retry_policy(role);
    let contract_id = final_review_contract_id(panel_role);
    let mut retry_count = 0;

    for probe_attempt in 1.. {
        if cancellation_token.is_cancelled() {
            return Err(AppError::InvocationCancelled {
                backend: target.backend.family.to_string(),
                contract_id: contract_id.clone(),
            });
        }

        match adapter.check_availability(target).await {
            Ok(()) => return Ok(retry_count),
            Err(error) => {
                if matches!(error.failure_class(), Some(FailureClass::Cancellation)) {
                    return Err(error);
                }
                let Some(retry_failure_class) = final_review_retry_failure_class(&error) else {
                    return Err(error);
                };

                retry_count += 1;
                let max_attempts = retry_policy.max_attempts(retry_failure_class).max(1);
                if probe_attempt >= max_attempts {
                    return Err(rewrite_transient_retry_exhaustion_error(
                        error,
                        reviewer_id,
                        target,
                        retry_count,
                    ));
                }

                let backoff = retry_policy.backoff_for_attempt(probe_attempt);
                tracing::warn!(
                    reviewer_id = reviewer_id,
                    panel_role = panel_role,
                    backend = %target.backend.family.as_str(),
                    model = %target.model.model_id.as_str(),
                    contract_id = contract_id,
                    retry_count = retry_count,
                    max_attempts = max_attempts,
                    backoff_ms = backoff.as_millis().min(u128::from(u64::MAX)) as u64,
                    error = %error,
                    "final_review transient availability failure; retrying"
                );
                if !backoff.is_zero() {
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        () = cancellation_token.cancelled() => {
                            return Err(AppError::InvocationCancelled {
                                backend: target.backend.family.to_string(),
                                contract_id: contract_id.clone(),
                            });
                        }
                    }
                }
            }
        }
    }

    Err(AppError::InvocationFailed {
        backend: target.backend.family.to_string(),
        contract_id,
        failure_class: FailureClass::TransportFailure,
        details: "final-review availability retry loop terminated without a probe result"
            .to_owned(),
    })
}

pub(crate) fn is_terminal_final_review_contract_failure(error: &AppError) -> bool {
    match error {
        AppError::InvocationFailed {
            contract_id,
            failure_class,
            ..
        } => {
            contract_id.starts_with("final_review:")
                && matches!(
                    failure_class,
                    FailureClass::SchemaValidationFailure | FailureClass::DomainValidationFailure
                )
        }
        AppError::CapabilityMismatch { contract_id, .. } => {
            contract_id.starts_with("final_review:")
        }
        _ => false,
    }
}

pub fn normalize_amendment_body(body: &str) -> String {
    let normalized_newlines = body.replace("\r\n", "\n").replace('\r', "\n");
    let trimmed = normalized_newlines.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    trimmed
        .split('\n')
        .map(|line| line.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn canonical_amendment_id(completion_round: u32, body: &str) -> String {
    let normalized = normalize_amendment_body(body);
    let digest = Sha256::digest(normalized.as_bytes());
    let hash = format!("{:x}", digest);
    format!("fr-{completion_round}-{}", &hash[..8])
}

pub fn consensus_status(
    accept_count: usize,
    total_votes: usize,
    threshold: f64,
) -> FinalReviewConsensusStatus {
    if total_votes == 0 {
        return FinalReviewConsensusStatus::Rejected;
    }

    let ratio = accept_count as f64 / total_votes as f64;
    if ratio >= threshold {
        FinalReviewConsensusStatus::Accepted
    } else if accept_count == 0 {
        FinalReviewConsensusStatus::Rejected
    } else {
        FinalReviewConsensusStatus::Disputed
    }
}

/// Normalize a `mapped_to_bead_id` value: trim whitespace and convert
/// empty/whitespace-only strings to `None` so they cannot suppress restarts.
fn normalize_mapped_to_bead_id(id: Option<&String>) -> Option<String> {
    id.and_then(|s| {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    })
}

fn mapped_to_bead_id_is_allowed(allowed_planned_elsewhere_ids: &HashSet<String>, id: &str) -> bool {
    !allowed_planned_elsewhere_ids.is_empty() && allowed_planned_elsewhere_ids.contains(id.trim())
}

fn normalize_optional_text(value: Option<&String>) -> Option<String> {
    value.and_then(|text| {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    })
}

fn normalize_propose_new_bead_fields(
    proposal: &super::panel_contracts::FinalReviewProposal,
    classification: AmendmentClassification,
) -> (
    AmendmentClassification,
    Option<String>,
    Option<String>,
    Option<review_classification::Severity>,
    Option<String>,
) {
    if classification != AmendmentClassification::ProposeNewBead {
        return (classification, None, None, None, None);
    }

    let proposed_title = normalize_optional_text(proposal.proposed_title.as_ref());
    let proposed_scope = normalize_optional_text(proposal.proposed_scope.as_ref());
    let rationale = normalize_optional_text(proposal.rationale.as_ref());
    let severity = proposal.severity;

    if proposed_title.is_some()
        && proposed_scope.is_some()
        && severity.is_some()
        && rationale.is_some()
    {
        (
            classification,
            proposed_title,
            proposed_scope,
            severity,
            rationale,
        )
    } else {
        (AmendmentClassification::FixNow, None, None, None, None)
    }
}

/// Infer classification from proposal fields. When an explicit `classification`
/// is set, use it. Otherwise fall back to the legacy `mapped_to_bead_id`
/// heuristic: present → planned-elsewhere, absent → fix-now.
fn infer_classification(
    proposal: &super::panel_contracts::FinalReviewProposal,
) -> AmendmentClassification {
    if let Some(c) = proposal.classification {
        return c;
    }
    if proposal.mapped_to_bead_id.is_some() {
        AmendmentClassification::PlannedElsewhere
    } else {
        AmendmentClassification::FixNow
    }
}

fn merge_final_review_amendments(
    completion_round: u32,
    proposals: &[ReviewerProposalRecord],
) -> Vec<CanonicalAmendment> {
    let mut by_body: HashMap<String, CanonicalAmendment> = HashMap::new();
    let mut order: Vec<String> = Vec::new();

    for proposal in proposals {
        for amendment in &proposal.payload.amendments {
            let normalized_body = normalize_amendment_body(&amendment.body);
            if normalized_body.is_empty() {
                continue;
            }

            let amendment_id = canonical_amendment_id(completion_round, &normalized_body);
            let source = FinalReviewAmendmentSource {
                reviewer_id: proposal.reviewer_id.clone(),
                backend_family: proposal.backend_family.clone(),
                model_id: proposal.model_id.clone(),
            };

            let incoming_mapped = normalize_mapped_to_bead_id(amendment.mapped_to_bead_id.as_ref());
            let inferred_classification = infer_classification(amendment);
            let (
                incoming_classification,
                incoming_proposed_title,
                incoming_proposed_scope,
                incoming_severity,
                incoming_rationale,
            ) = normalize_propose_new_bead_fields(amendment, inferred_classification);

            match by_body.get_mut(&normalized_body) {
                Some(existing) => {
                    if !existing.sources.contains(&source) {
                        existing.sources.push(source);
                    }
                    // Conservative merge for classification:
                    // - If reviewers disagree on classification, fall back to
                    //   fix-now (the most conservative option).
                    if existing.classification != incoming_classification {
                        existing.classification = AmendmentClassification::FixNow;
                        existing.mapped_to_bead_id = None;
                        existing.rationale = None;
                        existing.proposed_title = None;
                        existing.proposed_scope = None;
                        existing.severity = None;
                    }
                    // Conservative merge for planned-elsewhere target:
                    // - If ANY reviewer says "not planned-elsewhere" (None),
                    //   clear the field so the amendment is treated as regular.
                    // - If reviewers disagree on WHICH bead owns it, clear the
                    //   field (conflicting targets → treat as regular).
                    match (&existing.mapped_to_bead_id, &incoming_mapped) {
                        (Some(_), None) | (None, Some(_)) => {
                            existing.mapped_to_bead_id = None;
                            existing.classification = AmendmentClassification::FixNow;
                            existing.rationale = None;
                            existing.proposed_title = None;
                            existing.proposed_scope = None;
                            existing.severity = None;
                        }
                        (Some(existing_target), Some(new_target))
                            if existing_target != new_target =>
                        {
                            existing.mapped_to_bead_id = None;
                            existing.classification = AmendmentClassification::FixNow;
                            existing.rationale = None;
                            existing.proposed_title = None;
                            existing.proposed_scope = None;
                            existing.severity = None;
                        }
                        _ => {
                            // Agreement: both None or both same Some.
                        }
                    }

                    if existing.classification == AmendmentClassification::ProposeNewBead
                        && (existing.proposed_title.as_ref() != incoming_proposed_title.as_ref()
                            || existing.proposed_scope.as_ref() != incoming_proposed_scope.as_ref()
                            || existing.severity != incoming_severity
                            || existing.rationale.as_ref() != incoming_rationale.as_ref())
                    {
                        existing.classification = AmendmentClassification::FixNow;
                        existing.mapped_to_bead_id = None;
                        existing.rationale = None;
                        existing.proposed_title = None;
                        existing.proposed_scope = None;
                        existing.severity = None;
                    }
                }
                None => {
                    order.push(normalized_body.clone());
                    by_body.insert(
                        normalized_body.clone(),
                        CanonicalAmendment {
                            amendment_id,
                            normalized_body,
                            sources: vec![source],
                            mapped_to_bead_id: incoming_mapped,
                            classification: incoming_classification,
                            rationale: incoming_rationale,
                            proposed_title: incoming_proposed_title,
                            proposed_scope: incoming_proposed_scope,
                            severity: incoming_severity,
                        },
                    );
                }
            }
        }
    }

    order
        .into_iter()
        .filter_map(|body| by_body.remove(&body))
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_final_review_panel<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    artifact_write: &dyn PayloadArtifactWritePort,
    log_write: &dyn RuntimeLogWritePort,
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    project_root: &Path,
    backend_working_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    seq: &mut u64,
    cursor: &StageCursor,
    panel: &FinalReviewPanelResolution,
    min_reviewers: usize,
    probe_exhausted_count: usize,
    consensus_threshold: f64,
    max_restarts: u32,
    final_review_restart_count: u32,
    prompt_reference: &str,
    rollback_count: u32,
    reviewer_timeout_for_backend: &dyn Fn(crate::shared::domain::BackendFamily) -> Duration,
    arbiter_timeout: Duration,
    cancellation_token: CancellationToken,
) -> AppResult<FinalReviewResult>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    let stage_id = StageId::FinalReview;
    let prompt_path = project_root.join(prompt_reference);
    let project_prompt =
        std::fs::read_to_string(&prompt_path).map_err(|e| AppError::CorruptRecord {
            file: prompt_path.display().to_string(),
            details: format!("failed to read prompt for final review: {e}"),
        })?;

    // --- Proposal phase: prepare all reviewers first, then emit started events ---
    // We collect all prep data before emitting any started events so that a
    // prep failure (e.g. build_reviewer_prompt) does not leave orphaned
    // started events for reviewers that were never invoked.
    let mut proposal_preps = Vec::new();
    for (idx, member) in panel.reviewers.iter().enumerate() {
        let reviewer_id = final_review_reviewer_id(idx);
        let reviewer_prompt = build_reviewer_prompt(
            &project_prompt,
            &member.target,
            backend_working_dir,
            base_dir,
            Some(project_id),
        )?;
        proposal_preps.push((idx, member, reviewer_id, reviewer_prompt));
    }
    // All preps succeeded — now emit started events for the entire batch.
    for (_, member, reviewer_id, _) in &proposal_preps {
        append_panel_member_started_event(
            journal_store,
            base_dir,
            project_id,
            seq,
            run_id,
            cursor,
            "proposal",
            reviewer_id,
            "reviewer",
            &member.target,
        )?;
        append_panel_member_runtime_log(
            log_write,
            base_dir,
            project_id,
            "started",
            "proposal",
            reviewer_id,
            "reviewer",
            &member.target,
            None,
            None,
            None,
        );
    }

    // --- Proposal phase: invoke all reviewers concurrently ---
    let reviewer_retry_policy = final_review_retry_policy(BackendPolicyRole::FinalReviewer);
    let mut proposal_futures = FuturesUnordered::new();
    for (prep_idx, (_, member, reviewer_id, reviewer_prompt)) in proposal_preps.iter().enumerate() {
        let cancellation_token = cancellation_token.clone();
        let target = member.target.clone();
        let reviewer_id = reviewer_id.clone();
        let reviewer_prompt = reviewer_prompt.clone();
        let retry_policy = reviewer_retry_policy.clone();
        let timeout = reviewer_timeout_for_backend(member.target.backend.family);
        proposal_futures.push(async move {
            let started_at = Instant::now();
            let result = invoke_final_review_member(
                agent_service,
                project_root,
                backend_working_dir,
                run_id,
                stage_id,
                cursor,
                &target,
                BackendRole::Reviewer,
                &reviewer_id,
                "reviewer",
                &reviewer_id,
                reviewer_prompt,
                Value::Null,
                timeout,
                cancellation_token,
                &retry_policy,
            )
            .await;
            (prep_idx, started_at, result)
        });
    }

    // --- Proposal phase: process results sequentially ---
    // We must drain ALL futures before returning any error so that every
    // reviewer that emitted a `started` event also emits a `completed` event.
    let mut reviewer_records = Vec::new();
    let mut proposal_total_exhausted: usize = 0;
    let mut last_proposal_exhaustion_error: Option<AppError> = None;
    let mut first_required_proposal_failure: Option<(usize, AppError)> = None;
    let mut first_optional_proposal_failure: Option<(usize, AppError)> = None;
    let mut deferred_processing_error: Option<AppError> = None;
    while let Some((prep_idx, started_at, reviewer_payload)) = proposal_futures.next().await {
        let (idx, member, reviewer_id, _) = &proposal_preps[prep_idx];
        let idx = *idx;
        let (reviewer_payload, producer, retry_count) = match reviewer_payload {
            Ok(payload) => (payload.payload, payload.producer, payload.retry_count),
            // BackendExhausted is handled first regardless of required/optional
            // status — an exhausted backend is skipped gracefully.
            Err(FinalReviewMemberInvocationFailure { error, retry_count })
                if error
                    .failure_class()
                    .is_some_and(|fc| fc == FailureClass::BackendExhausted) =>
            {
                proposal_total_exhausted += 1;
                tracing::warn!(
                    reviewer = %reviewer_id,
                    "reviewer unavailable (backend exhausted), proceeding with remaining reviewers"
                );
                append_panel_member_completed_event(
                    journal_store,
                    base_dir,
                    project_id,
                    seq,
                    run_id,
                    cursor,
                    "proposal",
                    reviewer_id,
                    "reviewer",
                    &member.target,
                    started_at.elapsed(),
                    "failed_exhausted",
                    0,
                )?;
                append_panel_member_runtime_log_with_retry_count(
                    log_write,
                    base_dir,
                    project_id,
                    "completed",
                    "proposal",
                    reviewer_id,
                    "reviewer",
                    &member.target,
                    Some(started_at.elapsed()),
                    Some("failed_exhausted"),
                    Some(0),
                    Some(retry_count),
                );
                last_proposal_exhaustion_error = Some(error);
                continue;
            }
            Err(FinalReviewMemberInvocationFailure { error, retry_count }) if !member.required => {
                append_panel_member_completed_event(
                    journal_store,
                    base_dir,
                    project_id,
                    seq,
                    run_id,
                    cursor,
                    "proposal",
                    reviewer_id,
                    "reviewer",
                    &member.target,
                    started_at.elapsed(),
                    "failed_optional",
                    0,
                )?;
                append_panel_member_runtime_log_with_retry_count(
                    log_write,
                    base_dir,
                    project_id,
                    "completed",
                    "proposal",
                    reviewer_id,
                    "reviewer",
                    &member.target,
                    Some(started_at.elapsed()),
                    Some("failed_optional"),
                    Some(0),
                    Some(retry_count),
                );
                match &first_optional_proposal_failure {
                    None => {
                        first_optional_proposal_failure = Some((idx, error));
                    }
                    Some((prev_idx, _)) if idx < *prev_idx => {
                        first_optional_proposal_failure = Some((idx, error));
                    }
                    _ => {}
                }
                continue;
            }
            Err(FinalReviewMemberInvocationFailure { error, retry_count }) => {
                append_panel_member_completed_event(
                    journal_store,
                    base_dir,
                    project_id,
                    seq,
                    run_id,
                    cursor,
                    "proposal",
                    reviewer_id,
                    "reviewer",
                    &member.target,
                    started_at.elapsed(),
                    "failed",
                    0,
                )?;
                append_panel_member_runtime_log_with_retry_count(
                    log_write,
                    base_dir,
                    project_id,
                    "completed",
                    "proposal",
                    reviewer_id,
                    "reviewer",
                    &member.target,
                    Some(started_at.elapsed()),
                    Some("failed"),
                    Some(0),
                    Some(retry_count),
                );
                {
                    let is_cancellation = error
                        .failure_class()
                        .is_some_and(|fc| fc == FailureClass::Cancellation);
                    match &first_required_proposal_failure {
                        None => {
                            first_required_proposal_failure = Some((idx, error));
                            if !is_cancellation {
                                cancellation_token.cancel();
                            }
                        }
                        // Only replace with a lower-index error if it is a
                        // real failure, not a synthetic cancellation induced
                        // by our own cancel() call above.
                        Some((prev_idx, _)) if idx < *prev_idx && !is_cancellation => {
                            first_required_proposal_failure = Some((idx, error));
                        }
                        _ => {}
                    }
                }
                continue;
            }
        };
        let (backend_family, model_id) = match panel_member_identity_from_producer(
            &producer,
            &member.target,
            "final_review:reviewer",
            "final-review reviewer invocations must produce agent metadata",
        ) {
            Ok(v) => v,
            Err(e) => {
                if deferred_processing_error.is_none() {
                    deferred_processing_error = Some(e);
                    cancellation_token.cancel();
                }
                continue;
            }
        };

        let proposal: FinalReviewProposalPayload =
            match serde_json::from_value(reviewer_payload.clone()) {
                Ok(v) => v,
                Err(e) => {
                    let duration = started_at.elapsed();
                    let _ = append_panel_member_completed_event_with_identity(
                        journal_store,
                        base_dir,
                        project_id,
                        seq,
                        run_id,
                        cursor,
                        "proposal",
                        reviewer_id,
                        "reviewer",
                        &backend_family,
                        &model_id,
                        duration,
                        "failed_schema_validation",
                        0,
                    );
                    append_panel_member_runtime_log_with_identity_and_retry_count(
                        log_write,
                        base_dir,
                        project_id,
                        "completed",
                        "proposal",
                        reviewer_id,
                        "reviewer",
                        &backend_family,
                        &model_id,
                        Some(duration),
                        Some("failed_schema_validation"),
                        Some(0),
                        Some(retry_count),
                    );
                    if deferred_processing_error.is_none() {
                        deferred_processing_error = Some(AppError::InvocationFailed {
                            backend: member.target.backend.family.to_string(),
                            contract_id: "final_review:reviewer".to_owned(),
                            failure_class:
                                crate::shared::domain::FailureClass::SchemaValidationFailure,
                            details: format!("final-review proposal schema validation failed: {e}"),
                        });
                    }
                    continue;
                }
            };

        let duration = started_at.elapsed();
        if let Err(e) = append_panel_member_completed_event_with_identity(
            journal_store,
            base_dir,
            project_id,
            seq,
            run_id,
            cursor,
            "proposal",
            reviewer_id,
            "reviewer",
            &backend_family,
            &model_id,
            duration,
            "proposed_amendments",
            proposal.amendments.len(),
        ) {
            if deferred_processing_error.is_none() {
                deferred_processing_error = Some(e);
            }
            continue;
        }
        append_panel_member_runtime_log_with_identity_and_retry_count(
            log_write,
            base_dir,
            project_id,
            "completed",
            "proposal",
            reviewer_id,
            "reviewer",
            &backend_family,
            &model_id,
            Some(duration),
            Some("proposed_amendments"),
            Some(proposal.amendments.len()),
            Some(retry_count),
        );
        let artifact = renderers::render_final_review_proposal(&proposal, &producer.to_string());
        if let Err(e) = persist_supporting_record(
            artifact_write,
            base_dir,
            project_id,
            run_id,
            cursor,
            stage_id,
            rollback_count,
            &reviewer_payload,
            &artifact,
            producer,
            reviewer_id.as_str(),
        ) {
            if deferred_processing_error.is_none() {
                deferred_processing_error = Some(e);
            }
            continue;
        }

        reviewer_records.push(ReviewerProposalRecord {
            member_index: idx,
            reviewer_id: reviewer_id.clone(),
            required: member.required,
            backend_family,
            model_id,
            target: member.target.clone(),
            payload: proposal,
        });
    }

    // Now that all futures have been drained, propagate deferred errors.
    // Required invocation failures take priority over processing errors.
    // We pick the lowest-index (configured panel order) failure for
    // deterministic retry/hard-fail decisions regardless of finish order.
    if let Some((_, error)) = first_required_proposal_failure {
        return Err(error);
    }
    if let Some(error) = deferred_processing_error {
        return Err(error);
    }

    // Restore deterministic panel order (FuturesUnordered yields in
    // completion order which is latency-dependent).
    reviewer_records.sort_by_key(|r| r.member_index);

    let all_proposal_exhausted = probe_exhausted_count + proposal_total_exhausted;
    let effective_proposal_min = min_reviewers
        .min(
            panel
                .reviewers
                .len()
                .saturating_sub(proposal_total_exhausted),
        )
        .max(1);
    if reviewer_records.len() < effective_proposal_min {
        // Propagate the most specific error so the engine's
        // failure-class-aware handling can apply (e.g., retry decisions).
        // Priority: optional failure (preserves FailureClass) > exhaustion > generic.
        if let Some((_, error)) = first_optional_proposal_failure {
            return Err(error);
        }
        if let Some(exhaustion_error) = last_proposal_exhaustion_error {
            return Err(exhaustion_error);
        }
        return Err(AppError::InsufficientPanelMembers {
            panel: "final_review".to_owned(),
            resolved: reviewer_records.len(),
            minimum: effective_proposal_min,
        });
    }

    let mut amendments = merge_final_review_amendments(cursor.completion_round, &reviewer_records);
    if amendments.is_empty() {
        let aggregate = FinalReviewAggregatePayload {
            restart_required: false,
            force_completed: false,
            total_reviewers: reviewer_records.len(),
            total_proposed_amendments: reviewer_records
                .iter()
                .map(|record| record.payload.amendments.len())
                .sum(),
            unique_amendment_count: 0,
            accepted_amendment_ids: Vec::new(),
            rejected_amendment_ids: Vec::new(),
            disputed_amendment_ids: Vec::new(),
            amendments: Vec::new(),
            final_accepted_amendments: Vec::new(),
            final_review_restart_count,
            max_restarts,
            summary: "No final-review amendments were proposed.".to_owned(),
            exhausted_count: all_proposal_exhausted,
            probe_exhausted_count,
            effective_min_reviewers: effective_proposal_min,
        };
        let aggregate_payload = serde_json::to_value(&aggregate)?;
        let aggregate_artifact = renderers::render_final_review_aggregate(&aggregate);
        return Ok(FinalReviewResult {
            aggregate_payload,
            aggregate_artifact,
            final_accepted_amendments: Vec::new(),
            restart_required: false,
            force_completed: false,
        });
    }

    // Validate mapped_to_bead_id BEFORE building voter/arbiter prompts.
    // Fail closed: if no adjacent planned-elsewhere IDs are present in the
    // prompt, strip ALL mapped_to_bead_id values so they cannot mislead the
    // voting panel.
    let allowed_planned_elsewhere_ids =
        task_prompt_contract::extract_planned_elsewhere_routing_bead_ids(&project_prompt);
    for amendment in &mut amendments {
        if let Some(ref mapped_to) = amendment.mapped_to_bead_id {
            if !mapped_to_bead_id_is_allowed(&allowed_planned_elsewhere_ids, mapped_to) {
                amendment.mapped_to_bead_id = None;
            }
        }
    }

    // --- Vote phase: prepare and emit started events ---
    let amendments_context = json!({
        "amendments": amendments
            .iter()
            .map(|amendment| {
                json!({
                    "amendment_id": amendment.amendment_id,
                    "body": amendment.normalized_body,
                })
            })
            .collect::<Vec<_>>(),
    });
    // Collect all vote preps before emitting started events (same rationale
    // as the proposal phase: avoid orphaned started events on prep failure).
    let mut vote_preps = Vec::new();
    for reviewer in reviewer_records.iter() {
        let vote_prompt = build_voter_prompt(
            "Final Review Votes",
            &amendments,
            &reviewer.target,
            base_dir,
            Some(project_id),
        )?;
        vote_preps.push((reviewer, vote_prompt));
    }
    for (reviewer, _) in &vote_preps {
        append_panel_member_started_event(
            journal_store,
            base_dir,
            project_id,
            seq,
            run_id,
            cursor,
            "vote",
            &reviewer.reviewer_id,
            "reviewer",
            &reviewer.target,
        )?;
        append_panel_member_runtime_log(
            log_write,
            base_dir,
            project_id,
            "started",
            "vote",
            &reviewer.reviewer_id,
            "reviewer",
            &reviewer.target,
            None,
            None,
            None,
        );
    }

    // --- Vote phase: invoke all voters concurrently ---
    let reviewer_retry_policy = final_review_retry_policy(BackendPolicyRole::FinalReviewer);
    let mut vote_futures = FuturesUnordered::new();
    for (prep_idx, (reviewer, vote_prompt)) in vote_preps.iter().enumerate() {
        let cancellation_token = cancellation_token.clone();
        let target = reviewer.target.clone();
        let reviewer_id = reviewer.reviewer_id.clone();
        let vote_prompt = vote_prompt.clone();
        let retry_policy = reviewer_retry_policy.clone();
        let amendments_context = amendments_context.clone();
        let timeout = reviewer_timeout_for_backend(reviewer.target.backend.family);
        vote_futures.push(async move {
            let started_at = Instant::now();
            let result = invoke_final_review_member(
                agent_service,
                project_root,
                backend_working_dir,
                run_id,
                stage_id,
                cursor,
                &target,
                BackendRole::Reviewer,
                &reviewer_id,
                "voter",
                &reviewer_id,
                vote_prompt,
                amendments_context,
                timeout,
                cancellation_token,
                &retry_policy,
            )
            .await;
            (prep_idx, started_at, result)
        });
    }

    // --- Vote phase: process results sequentially ---
    let mut reviewer_votes = Vec::new();
    let mut vote_total_exhausted: usize = 0;
    let mut last_vote_exhaustion_error: Option<AppError> = None;
    let mut first_required_vote_failure: Option<(usize, AppError)> = None;
    let mut first_optional_vote_failure: Option<(usize, AppError)> = None;
    let mut deferred_vote_processing_error: Option<AppError> = None;
    while let Some((prep_idx, started_at, vote_payload)) = vote_futures.next().await {
        let (reviewer, _) = &vote_preps[prep_idx];
        let (vote_payload, producer, retry_count) = match vote_payload {
            Ok(payload) => (payload.payload, payload.producer, payload.retry_count),
            // BackendExhausted is handled first regardless of required/optional
            // status — an exhausted backend is skipped gracefully.
            Err(FinalReviewMemberInvocationFailure { error, retry_count })
                if error
                    .failure_class()
                    .is_some_and(|fc| fc == FailureClass::BackendExhausted) =>
            {
                vote_total_exhausted += 1;
                tracing::warn!(
                    reviewer = %reviewer.reviewer_id,
                    "reviewer unavailable during vote (backend exhausted), proceeding with remaining reviewers"
                );
                append_panel_member_completed_event(
                    journal_store,
                    base_dir,
                    project_id,
                    seq,
                    run_id,
                    cursor,
                    "vote",
                    &reviewer.reviewer_id,
                    "reviewer",
                    &reviewer.target,
                    started_at.elapsed(),
                    "failed_exhausted",
                    0,
                )?;
                append_panel_member_runtime_log_with_retry_count(
                    log_write,
                    base_dir,
                    project_id,
                    "completed",
                    "vote",
                    &reviewer.reviewer_id,
                    "reviewer",
                    &reviewer.target,
                    Some(started_at.elapsed()),
                    Some("failed_exhausted"),
                    Some(0),
                    Some(retry_count),
                );
                last_vote_exhaustion_error = Some(error);
                continue;
            }
            Err(FinalReviewMemberInvocationFailure { error, retry_count })
                if !reviewer.required =>
            {
                append_panel_member_completed_event(
                    journal_store,
                    base_dir,
                    project_id,
                    seq,
                    run_id,
                    cursor,
                    "vote",
                    &reviewer.reviewer_id,
                    "reviewer",
                    &reviewer.target,
                    started_at.elapsed(),
                    "failed_optional",
                    0,
                )?;
                append_panel_member_runtime_log_with_retry_count(
                    log_write,
                    base_dir,
                    project_id,
                    "completed",
                    "vote",
                    &reviewer.reviewer_id,
                    "reviewer",
                    &reviewer.target,
                    Some(started_at.elapsed()),
                    Some("failed_optional"),
                    Some(0),
                    Some(retry_count),
                );
                match &first_optional_vote_failure {
                    None => {
                        first_optional_vote_failure = Some((reviewer.member_index, error));
                    }
                    Some((prev_idx, _)) if reviewer.member_index < *prev_idx => {
                        first_optional_vote_failure = Some((reviewer.member_index, error));
                    }
                    _ => {}
                }
                continue;
            }
            Err(FinalReviewMemberInvocationFailure { error, retry_count }) => {
                append_panel_member_completed_event(
                    journal_store,
                    base_dir,
                    project_id,
                    seq,
                    run_id,
                    cursor,
                    "vote",
                    &reviewer.reviewer_id,
                    "reviewer",
                    &reviewer.target,
                    started_at.elapsed(),
                    "failed",
                    0,
                )?;
                append_panel_member_runtime_log_with_retry_count(
                    log_write,
                    base_dir,
                    project_id,
                    "completed",
                    "vote",
                    &reviewer.reviewer_id,
                    "reviewer",
                    &reviewer.target,
                    Some(started_at.elapsed()),
                    Some("failed"),
                    Some(0),
                    Some(retry_count),
                );
                {
                    let is_cancellation = error
                        .failure_class()
                        .is_some_and(|fc| fc == FailureClass::Cancellation);
                    match &first_required_vote_failure {
                        None => {
                            first_required_vote_failure = Some((reviewer.member_index, error));
                            if !is_cancellation {
                                cancellation_token.cancel();
                            }
                        }
                        Some((prev_idx, _))
                            if reviewer.member_index < *prev_idx && !is_cancellation =>
                        {
                            first_required_vote_failure = Some((reviewer.member_index, error));
                        }
                        _ => {}
                    }
                }
                continue;
            }
        };
        let (backend_family, model_id) = match panel_member_identity_from_producer(
            &producer,
            &reviewer.target,
            "final_review:voter",
            "final-review reviewer vote invocations must produce agent metadata",
        ) {
            Ok(v) => v,
            Err(e) => {
                if deferred_vote_processing_error.is_none() {
                    deferred_vote_processing_error = Some(e);
                }
                continue;
            }
        };

        let votes: FinalReviewVotePayload = match serde_json::from_value(vote_payload.clone()) {
            Ok(v) => v,
            Err(e) => {
                let duration = started_at.elapsed();
                let _ = append_panel_member_completed_event_with_identity(
                    journal_store,
                    base_dir,
                    project_id,
                    seq,
                    run_id,
                    cursor,
                    "vote",
                    &reviewer.reviewer_id,
                    "reviewer",
                    &backend_family,
                    &model_id,
                    duration,
                    "failed_schema_validation",
                    0,
                );
                append_panel_member_runtime_log_with_identity_and_retry_count(
                    log_write,
                    base_dir,
                    project_id,
                    "completed",
                    "vote",
                    &reviewer.reviewer_id,
                    "reviewer",
                    &backend_family,
                    &model_id,
                    Some(duration),
                    Some("failed_schema_validation"),
                    Some(0),
                    Some(retry_count),
                );
                if deferred_vote_processing_error.is_none() {
                    deferred_vote_processing_error = Some(AppError::InvocationFailed {
                        backend: reviewer.target.backend.family.to_string(),
                        contract_id: "final_review:voter".to_owned(),
                        failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                        details: format!("final-review vote schema validation failed: {e}"),
                    });
                }
                continue;
            }
        };
        if let Err(error) = validate_vote_payload(&votes, &amendments, &reviewer.target) {
            let duration = started_at.elapsed();
            let _ = append_panel_member_completed_event_with_identity(
                journal_store,
                base_dir,
                project_id,
                seq,
                run_id,
                cursor,
                "vote",
                &reviewer.reviewer_id,
                "reviewer",
                &backend_family,
                &model_id,
                duration,
                "failed_domain_validation",
                0,
            );
            append_panel_member_runtime_log_with_identity_and_retry_count(
                log_write,
                base_dir,
                project_id,
                "completed",
                "vote",
                &reviewer.reviewer_id,
                "reviewer",
                &backend_family,
                &model_id,
                Some(duration),
                Some("failed_domain_validation"),
                Some(0),
                Some(retry_count),
            );
            if deferred_vote_processing_error.is_none() {
                deferred_vote_processing_error = Some(error);
            }
            continue;
        }
        let duration = started_at.elapsed();
        if let Err(e) = append_panel_member_completed_event_with_identity(
            journal_store,
            base_dir,
            project_id,
            seq,
            run_id,
            cursor,
            "vote",
            &reviewer.reviewer_id,
            "reviewer",
            &backend_family,
            &model_id,
            duration,
            "reviewer_votes_recorded",
            votes.votes.len(),
        ) {
            if deferred_vote_processing_error.is_none() {
                deferred_vote_processing_error = Some(e);
            }
            continue;
        }
        append_panel_member_runtime_log_with_identity_and_retry_count(
            log_write,
            base_dir,
            project_id,
            "completed",
            "vote",
            &reviewer.reviewer_id,
            "reviewer",
            &backend_family,
            &model_id,
            Some(duration),
            Some("reviewer_votes_recorded"),
            Some(votes.votes.len()),
            Some(retry_count),
        );

        let artifact = renderers::render_final_review_vote(
            "Final Review Votes",
            &votes,
            &producer.to_string(),
        );
        let vote_id = final_review_vote_id(reviewer.member_index);
        if let Err(e) = persist_supporting_record(
            artifact_write,
            base_dir,
            project_id,
            run_id,
            cursor,
            stage_id,
            rollback_count,
            &vote_payload,
            &artifact,
            producer,
            &vote_id,
        ) {
            if deferred_vote_processing_error.is_none() {
                deferred_vote_processing_error = Some(e);
            }
            continue;
        }

        reviewer_votes.push((reviewer.member_index, votes));
    }

    // Now that all vote futures have been drained, propagate deferred errors.
    // Pick lowest-index failure for deterministic behavior (same as proposals).
    if let Some((_, error)) = first_required_vote_failure {
        return Err(error);
    }
    if let Some(error) = deferred_vote_processing_error {
        return Err(error);
    }

    // Restore deterministic panel order for votes (same as proposals).
    reviewer_votes.sort_by_key(|(idx, _)| *idx);
    let reviewer_votes: Vec<FinalReviewVotePayload> =
        reviewer_votes.into_iter().map(|(_, v)| v).collect();

    // Reduce vote quorum only when exhaustion makes the configured
    // minimum impossible.  reviewer_records is the post-proposal panel;
    // vote_total_exhausted is the invocation-time exhaustion during voting.
    let effective_vote_min = min_reviewers
        .min(reviewer_records.len().saturating_sub(vote_total_exhausted))
        .max(1);
    if reviewer_votes.len() < effective_vote_min {
        // Propagate the most specific error so the engine's
        // failure-class-aware handling can apply (e.g., retry decisions).
        if let Some((_, error)) = first_optional_vote_failure {
            return Err(error);
        }
        if let Some(exhaustion_error) = last_vote_exhaustion_error {
            return Err(exhaustion_error);
        }
        return Err(AppError::InsufficientPanelMembers {
            panel: "final_review_vote".to_owned(),
            resolved: reviewer_votes.len(),
            minimum: effective_vote_min,
        });
    }

    let mut accepted_ids = Vec::new();
    let mut rejected_ids = Vec::new();
    let mut disputed_ids = Vec::new();

    for amendment in &amendments {
        let accept_count = reviewer_votes
            .iter()
            .filter_map(|payload| {
                payload
                    .votes
                    .iter()
                    .find(|vote| vote.amendment_id == amendment.amendment_id)
            })
            .filter(|vote| vote.decision == FinalReviewVoteDecision::Accept)
            .count();
        match consensus_status(accept_count, reviewer_votes.len(), consensus_threshold) {
            FinalReviewConsensusStatus::Accepted => {
                accepted_ids.push(amendment.amendment_id.clone());
            }
            FinalReviewConsensusStatus::Rejected => {
                rejected_ids.push(amendment.amendment_id.clone());
            }
            FinalReviewConsensusStatus::Disputed => {
                disputed_ids.push(amendment.amendment_id.clone());
            }
        }
    }

    let mut final_accepted_ids = accepted_ids.clone();
    if !disputed_ids.is_empty() {
        let disputed_set: HashMap<String, &CanonicalAmendment> = amendments
            .iter()
            .filter(|amendment| disputed_ids.contains(&amendment.amendment_id))
            .map(|amendment| (amendment.amendment_id.clone(), amendment))
            .collect();

        let arbiter_prompt = build_arbiter_prompt(
            &disputed_set,
            &reviewer_votes,
            &panel.arbiter,
            base_dir,
            Some(project_id),
        )?;
        append_panel_member_started_event(
            journal_store,
            base_dir,
            project_id,
            seq,
            run_id,
            cursor,
            "arbitrate",
            "arbiter",
            "arbiter",
            &panel.arbiter,
        )?;
        append_panel_member_runtime_log(
            log_write,
            base_dir,
            project_id,
            "started",
            "arbitrate",
            "arbiter",
            "arbiter",
            &panel.arbiter,
            None,
            None,
            None,
        );
        let arbiter_retry_policy = final_review_retry_policy(BackendPolicyRole::Arbiter);
        let arbiter_started_at = Instant::now();
        let arbiter_payload = invoke_final_review_member(
            agent_service,
            project_root,
            backend_working_dir,
            run_id,
            stage_id,
            cursor,
            &panel.arbiter,
            BackendRole::Reviewer,
            "arbiter",
            "arbiter",
            "arbiter",
            arbiter_prompt,
            json!({
                "disputed_amendments": disputed_set
                    .values()
                    .map(|amendment| {
                        json!({
                            "amendment_id": amendment.amendment_id,
                            "body": amendment.normalized_body,
                        })
                    })
                    .collect::<Vec<_>>(),
            }),
            arbiter_timeout,
            cancellation_token,
            &arbiter_retry_policy,
        )
        .await;
        let (arbiter_payload, producer, arbiter_retry_count) = match arbiter_payload {
            Ok(payload) => (payload.payload, payload.producer, payload.retry_count),
            Err(FinalReviewMemberInvocationFailure { error, retry_count }) => {
                append_panel_member_completed_event(
                    journal_store,
                    base_dir,
                    project_id,
                    seq,
                    run_id,
                    cursor,
                    "arbitrate",
                    "arbiter",
                    "arbiter",
                    &panel.arbiter,
                    arbiter_started_at.elapsed(),
                    "failed",
                    0,
                )?;
                append_panel_member_runtime_log_with_retry_count(
                    log_write,
                    base_dir,
                    project_id,
                    "completed",
                    "arbitrate",
                    "arbiter",
                    "arbiter",
                    &panel.arbiter,
                    Some(arbiter_started_at.elapsed()),
                    Some("failed"),
                    Some(0),
                    Some(retry_count),
                );
                return Err(error);
            }
        };
        let (arbiter_backend_family, arbiter_model_id) = panel_member_identity_from_producer(
            &producer,
            &panel.arbiter,
            "final_review:arbiter",
            "final-review arbiter invocations must produce agent metadata",
        )?;

        let arbiter: FinalReviewArbiterPayload = serde_json::from_value(arbiter_payload.clone())
            .map_err(|e| {
                let duration = arbiter_started_at.elapsed();
                let _ = append_panel_member_completed_event_with_identity(
                    journal_store,
                    base_dir,
                    project_id,
                    seq,
                    run_id,
                    cursor,
                    "arbitrate",
                    "arbiter",
                    "arbiter",
                    &arbiter_backend_family,
                    &arbiter_model_id,
                    duration,
                    "failed_schema_validation",
                    0,
                );
                append_panel_member_runtime_log_with_identity_and_retry_count(
                    log_write,
                    base_dir,
                    project_id,
                    "completed",
                    "arbitrate",
                    "arbiter",
                    "arbiter",
                    &arbiter_backend_family,
                    &arbiter_model_id,
                    Some(duration),
                    Some("failed_schema_validation"),
                    Some(0),
                    Some(arbiter_retry_count),
                );
                AppError::InvocationFailed {
                    backend: panel.arbiter.backend.family.to_string(),
                    contract_id: "final_review:arbiter".to_owned(),
                    failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                    details: format!("final-review arbiter schema validation failed: {e}"),
                }
            })?;
        validate_arbiter_payload(&arbiter, &disputed_ids, &panel.arbiter).inspect_err(|_| {
            let duration = arbiter_started_at.elapsed();
            let _ = append_panel_member_completed_event_with_identity(
                journal_store,
                base_dir,
                project_id,
                seq,
                run_id,
                cursor,
                "arbitrate",
                "arbiter",
                "arbiter",
                &arbiter_backend_family,
                &arbiter_model_id,
                duration,
                "failed_domain_validation",
                0,
            );
            append_panel_member_runtime_log_with_identity_and_retry_count(
                log_write,
                base_dir,
                project_id,
                "completed",
                "arbitrate",
                "arbiter",
                "arbiter",
                &arbiter_backend_family,
                &arbiter_model_id,
                Some(duration),
                Some("failed_domain_validation"),
                Some(0),
                Some(arbiter_retry_count),
            );
        })?;
        let arbiter_duration = arbiter_started_at.elapsed();
        append_panel_member_completed_event_with_identity(
            journal_store,
            base_dir,
            project_id,
            seq,
            run_id,
            cursor,
            "arbitrate",
            "arbiter",
            "arbiter",
            &arbiter_backend_family,
            &arbiter_model_id,
            arbiter_duration,
            "arbiter_rulings_recorded",
            arbiter.rulings.len(),
        )?;
        append_panel_member_runtime_log_with_identity_and_retry_count(
            log_write,
            base_dir,
            project_id,
            "completed",
            "arbitrate",
            "arbiter",
            "arbiter",
            &arbiter_backend_family,
            &arbiter_model_id,
            Some(arbiter_duration),
            Some("arbiter_rulings_recorded"),
            Some(arbiter.rulings.len()),
            Some(arbiter_retry_count),
        );
        let artifact = renderers::render_final_review_arbiter(&arbiter, &producer.to_string());
        persist_supporting_record(
            artifact_write,
            base_dir,
            project_id,
            run_id,
            cursor,
            stage_id,
            rollback_count,
            &arbiter_payload,
            &artifact,
            producer,
            "arbiter",
        )?;

        for ruling in arbiter.rulings {
            if ruling.decision == FinalReviewVoteDecision::Accept {
                final_accepted_ids.push(ruling.amendment_id);
            }
        }
    }

    final_accepted_ids.sort();
    final_accepted_ids.dedup();

    let mut final_accepted_amendments: Vec<CanonicalAmendment> = amendments
        .iter()
        .filter(|amendment| final_accepted_ids.contains(&amendment.amendment_id))
        .cloned()
        .collect();

    // Defense-in-depth: re-validate mapped_to_bead_id on the accepted subset
    // before the restart decision. The primary validation already ran on
    // `amendments` before voter/arbiter prompts, but this guards against any
    // code path that could re-introduce an invalid mapping.
    for amendment in &mut final_accepted_amendments {
        if let Some(ref mapped_to) = amendment.mapped_to_bead_id {
            if !mapped_to_bead_id_is_allowed(&allowed_planned_elsewhere_ids, mapped_to) {
                amendment.mapped_to_bead_id = None;
                // If the mapped bead ID was invalid, downgrade to fix-now
                // since we can't route a planned-elsewhere without a target.
                if amendment.classification == AmendmentClassification::PlannedElsewhere {
                    amendment.classification = AmendmentClassification::FixNow;
                }
            }
        }
    }

    let total_all_exhausted =
        probe_exhausted_count + proposal_total_exhausted + vote_total_exhausted;
    let effective_min = min_reviewers
        .min(reviewer_records.len().saturating_sub(vote_total_exhausted))
        .max(1);

    let has_restart_triggering = final_accepted_amendments
        .iter()
        .any(|a| a.classification.triggers_restart());
    if has_restart_triggering && final_review_restart_count >= max_restarts {
        let aggregate = FinalReviewAggregatePayload {
            restart_required: false,
            force_completed: true,
            total_reviewers: reviewer_votes.len(),
            total_proposed_amendments: reviewer_records
                .iter()
                .map(|record| record.payload.amendments.len())
                .sum(),
            unique_amendment_count: amendments.len(),
            accepted_amendment_ids: accepted_ids.clone(),
            rejected_amendment_ids: rejected_ids.clone(),
            disputed_amendment_ids: disputed_ids.clone(),
            amendments: amendments.iter().map(canonical_to_payload).collect(),
            final_accepted_amendments: final_accepted_amendments
                .iter()
                .map(canonical_to_payload)
                .collect(),
            final_review_restart_count,
            max_restarts,
            summary: format!(
                "Final-review restart cap reached ({final_review_restart_count}/{max_restarts}); force-completing instead of restarting with {} accepted amendment(s).",
                final_accepted_amendments.len()
            ),
            exhausted_count: total_all_exhausted,
            probe_exhausted_count,
            effective_min_reviewers: effective_min,
        };
        let aggregate_payload = serde_json::to_value(&aggregate)?;
        let aggregate_artifact = renderers::render_final_review_aggregate(&aggregate);

        return Ok(FinalReviewResult {
            aggregate_payload,
            aggregate_artifact,
            final_accepted_amendments: final_accepted_amendments
                .into_iter()
                .map(|amendment| FinalReviewAcceptedAmendment {
                    amendment_id: amendment.amendment_id,
                    normalized_body: amendment.normalized_body,
                    sources: amendment.sources,
                    mapped_to_bead_id: amendment.mapped_to_bead_id,
                    classification: amendment.classification,
                    rationale: amendment.rationale,
                    proposed_title: amendment.proposed_title,
                    proposed_scope: amendment.proposed_scope,
                    severity: amendment.severity,
                })
                .collect(),
            restart_required: false,
            force_completed: true,
        });
    }

    let needs_restart = final_accepted_amendments
        .iter()
        .any(|a| a.classification.triggers_restart());
    let aggregate = FinalReviewAggregatePayload {
        restart_required: needs_restart,
        force_completed: false,
        total_reviewers: reviewer_votes.len(),
        total_proposed_amendments: reviewer_records
            .iter()
            .map(|record| record.payload.amendments.len())
            .sum(),
        unique_amendment_count: amendments.len(),
        accepted_amendment_ids: accepted_ids,
        rejected_amendment_ids: rejected_ids,
        disputed_amendment_ids: disputed_ids,
        amendments: amendments.iter().map(canonical_to_payload).collect(),
        final_accepted_amendments: final_accepted_amendments
            .iter()
            .map(canonical_to_payload)
            .collect(),
        final_review_restart_count: if needs_restart {
            final_review_restart_count.saturating_add(1)
        } else {
            final_review_restart_count
        },
        max_restarts,
        summary: if final_accepted_amendments.is_empty() {
            "Final review accepted no amendments.".to_owned()
        } else if needs_restart {
            format!(
                "Final review accepted {} amendment(s); restart required.",
                final_accepted_amendments.len()
            )
        } else {
            format!(
                "Final review accepted {} amendment(s), none require restart; no restart required.",
                final_accepted_amendments.len()
            )
        },
        exhausted_count: total_all_exhausted,
        probe_exhausted_count,
        effective_min_reviewers: effective_min,
    };
    let aggregate_payload = serde_json::to_value(&aggregate)?;
    let aggregate_artifact = renderers::render_final_review_aggregate(&aggregate);

    Ok(FinalReviewResult {
        aggregate_payload,
        aggregate_artifact,
        final_accepted_amendments: final_accepted_amendments
            .into_iter()
            .map(|amendment| FinalReviewAcceptedAmendment {
                amendment_id: amendment.amendment_id,
                normalized_body: amendment.normalized_body,
                sources: amendment.sources,
                mapped_to_bead_id: amendment.mapped_to_bead_id,
                classification: amendment.classification,
                rationale: amendment.rationale,
                proposed_title: amendment.proposed_title,
                proposed_scope: amendment.proposed_scope,
                severity: amendment.severity,
            })
            .collect(),
        restart_required: needs_restart,
        force_completed: false,
    })
}

#[allow(clippy::too_many_arguments)]
fn append_panel_member_started_event(
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    seq: &mut u64,
    run_id: &RunId,
    cursor: &StageCursor,
    phase: &str,
    reviewer_id: &str,
    role: &str,
    target: &ResolvedBackendTarget,
) -> AppResult<()> {
    *seq += 1;
    let event = journal::reviewer_started_event(
        *seq,
        Utc::now(),
        run_id,
        StageId::FinalReview,
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round,
        "final_review",
        phase,
        reviewer_id,
        role,
        target.backend.family.as_str(),
        target.model.model_id.as_str(),
    );
    let line = journal::serialize_event(&event)?;
    if let Err(error) = journal_store.append_event(base_dir, project_id, &line) {
        *seq -= 1;
        return Err(AppError::StageCommitFailed {
            stage_id: StageId::FinalReview,
            details: format!(
                "failed to persist reviewer_started for final_review {role} {reviewer_id}: {error}"
            ),
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_panel_member_completed_event(
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    seq: &mut u64,
    run_id: &RunId,
    cursor: &StageCursor,
    phase: &str,
    reviewer_id: &str,
    role: &str,
    target: &ResolvedBackendTarget,
    duration: Duration,
    outcome: &str,
    amendment_count: usize,
) -> AppResult<()> {
    append_panel_member_completed_event_with_identity(
        journal_store,
        base_dir,
        project_id,
        seq,
        run_id,
        cursor,
        phase,
        reviewer_id,
        role,
        target.backend.family.as_str(),
        target.model.model_id.as_str(),
        duration,
        outcome,
        amendment_count,
    )
}

#[allow(clippy::too_many_arguments)]
fn append_panel_member_completed_event_with_identity(
    journal_store: &dyn JournalStorePort,
    base_dir: &Path,
    project_id: &ProjectId,
    seq: &mut u64,
    run_id: &RunId,
    cursor: &StageCursor,
    phase: &str,
    reviewer_id: &str,
    role: &str,
    backend_family: &str,
    model_id: &str,
    duration: Duration,
    outcome: &str,
    amendment_count: usize,
) -> AppResult<()> {
    *seq += 1;
    let event = journal::reviewer_completed_event(
        *seq,
        Utc::now(),
        run_id,
        StageId::FinalReview,
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round,
        "final_review",
        phase,
        reviewer_id,
        role,
        backend_family,
        model_id,
        duration.as_millis().min(u128::from(u64::MAX)) as u64,
        outcome,
        amendment_count,
    );
    let line = journal::serialize_event(&event)?;
    if let Err(error) = journal_store.append_event(base_dir, project_id, &line) {
        *seq -= 1;
        return Err(AppError::StageCommitFailed {
            stage_id: StageId::FinalReview,
            details: format!(
                "failed to persist reviewer_completed for final_review {role} {reviewer_id}: {error}"
            ),
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_panel_member_runtime_log(
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    state: &str,
    phase: &str,
    reviewer_id: &str,
    role: &str,
    target: &ResolvedBackendTarget,
    duration: Option<Duration>,
    outcome: Option<&str>,
    amendment_count: Option<usize>,
) {
    append_panel_member_runtime_log_with_identity_and_retry_count(
        log_write,
        base_dir,
        project_id,
        state,
        phase,
        reviewer_id,
        role,
        target.backend.family.as_str(),
        target.model.model_id.as_str(),
        duration,
        outcome,
        amendment_count,
        None,
    );
}

#[allow(clippy::too_many_arguments)]
fn append_panel_member_runtime_log_with_retry_count(
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    state: &str,
    phase: &str,
    reviewer_id: &str,
    role: &str,
    target: &ResolvedBackendTarget,
    duration: Option<Duration>,
    outcome: Option<&str>,
    amendment_count: Option<usize>,
    retry_count: Option<u32>,
) {
    append_panel_member_runtime_log_with_identity_and_retry_count(
        log_write,
        base_dir,
        project_id,
        state,
        phase,
        reviewer_id,
        role,
        target.backend.family.as_str(),
        target.model.model_id.as_str(),
        duration,
        outcome,
        amendment_count,
        retry_count,
    );
}

#[allow(clippy::too_many_arguments)]
fn append_panel_member_runtime_log_with_identity_and_retry_count(
    log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    state: &str,
    phase: &str,
    reviewer_id: &str,
    role: &str,
    backend_family: &str,
    model_id: &str,
    duration: Option<Duration>,
    outcome: Option<&str>,
    amendment_count: Option<usize>,
    retry_count: Option<u32>,
) {
    let mut message = format!(
        "final_review {state}: role={role} phase={phase} reviewer_id={reviewer_id} backend={backend_family} model={model_id}",
    );
    if let Some(duration) = duration {
        message.push_str(&format!(
            " duration_ms={}",
            duration.as_millis().min(u128::from(u64::MAX)) as u64
        ));
    }
    if let Some(outcome) = outcome {
        message.push_str(&format!(" outcome={outcome}"));
    }
    if let Some(amendment_count) = amendment_count {
        message.push_str(&format!(" amendment_count={amendment_count}"));
    }
    if let Some(retry_count) = retry_count {
        message.push_str(&format!(" retry_count={retry_count}"));
    }
    let _ = log_write.append_runtime_log(
        base_dir,
        project_id,
        &RuntimeLogEntry {
            timestamp: Utc::now(),
            level: LogLevel::Info,
            source: "final_review".to_owned(),
            message,
        },
    );
}

fn canonical_to_payload(amendment: &CanonicalAmendment) -> FinalReviewCanonicalAmendment {
    FinalReviewCanonicalAmendment {
        amendment_id: amendment.amendment_id.clone(),
        normalized_body: amendment.normalized_body.clone(),
        sources: amendment.sources.clone(),
        mapped_to_bead_id: amendment.mapped_to_bead_id.clone(),
        classification: amendment.classification,
        rationale: amendment.rationale.clone(),
        proposed_title: amendment.proposed_title.clone(),
        proposed_scope: amendment.proposed_scope.clone(),
        severity: amendment.severity,
    }
}

fn build_reviewer_prompt(
    project_prompt: &str,
    backend_target: &ResolvedBackendTarget,
    backend_working_dir: &Path,
    base_dir: &Path,
    project_id: Option<&ProjectId>,
) -> AppResult<String> {
    let schema_str = serde_json::to_string_pretty(&processed_contract_schema_value(
        &InvocationContract::Panel {
            stage_id: StageId::FinalReview,
            role: "reviewer".to_owned(),
        },
        backend_target.backend.family,
    ))?;
    let task_prompt_contract_block =
        task_prompt_contract::stage_consumer_guidance_for_prompt(project_prompt);
    let planned_elsewhere_ids =
        task_prompt_contract::extract_planned_elsewhere_routing_bead_ids(project_prompt);
    let classification_guidance_block =
        review_classification::render_classification_guidance(&planned_elsewhere_ids, true);
    let review_scope_guidance = build_review_scope_guidance(backend_working_dir, project_id);
    template_catalog::resolve_and_render(
        "final_review_reviewer",
        base_dir,
        project_id,
        &[
            ("review_scope_guidance", &review_scope_guidance),
            ("task_prompt_contract", task_prompt_contract_block.as_str()),
            ("classification_guidance", &classification_guidance_block),
            ("project_prompt", project_prompt),
            ("json_schema", &schema_str),
        ],
    )
}

fn run_git_stdout(repo_root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .current_dir(repo_root)
        .env("LC_ALL", "C")
        .env("LANG", "C")
        .env_remove("LANGUAGE")
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if stdout.is_empty() {
        None
    } else {
        Some(stdout)
    }
}

fn review_base_ref(repo_root: &Path) -> Option<String> {
    run_git_stdout(
        repo_root,
        &[
            "symbolic-ref",
            "--quiet",
            "--short",
            "refs/remotes/origin/HEAD",
        ],
    )
    .or_else(|| {
        ["origin/main", "origin/master", "main", "master"]
            .into_iter()
            .find(|candidate| {
                run_git_stdout(repo_root, &["rev-parse", "--verify", candidate]).is_some()
            })
            .map(str::to_owned)
    })
}

fn review_fork_point(repo_root: &Path, base_ref: &str) -> Option<String> {
    run_git_stdout(repo_root, &["merge-base", "HEAD", base_ref])
}

fn build_review_scope_guidance(
    backend_working_dir: &Path,
    project_id: Option<&ProjectId>,
) -> String {
    let mut guidance = String::from(
        "## Review Scope\n\nReview the full branch state, not just the current unstaged worktree diff.",
    );

    if let Some(base_ref) = review_base_ref(backend_working_dir) {
        if let Some(fork_point) = review_fork_point(backend_working_dir, &base_ref) {
            guidance.push_str(&format!(
                "\n\nFor this checkout, compare against `{base_ref}` from fork point `{fork_point}`:\n- `git log --stat {fork_point}..HEAD`\n- `git diff {fork_point}..HEAD`\n- `git diff`\n- `git status --short`\n\nRead the changed implementation files end-to-end after scoping the branch delta."
            ));
        } else {
            guidance.push_str(&format!(
                "\n\nThe base ref resolved to `{base_ref}`, but the fork point could not be computed automatically. Do NOT rely on plain `git diff` alone. Determine the correct fork point manually, then review:\n- `git log --stat <fork-point>..HEAD`\n- `git diff <fork-point>..HEAD`\n- `git diff`\n- `git status --short`"
            ));
        }
    } else {
        guidance.push_str(
            "\n\nThe default branch could not be resolved automatically in this checkout. Do NOT rely on plain `git diff` alone. Determine the repository's canonical base branch/ref manually, then review:\n- `git log --stat <fork-point>..HEAD`\n- `git diff <fork-point>..HEAD`\n- `git diff`\n- `git status --short`",
        );
    }

    if let Some(project_id) = project_id {
        guidance.push_str(&format!(
            "\n\nIgnore live orchestration runtime state under `.ralph-burning/projects/{}/` for this active run. Those files are generated by the workflow itself, not product code. Other intentional checked-in fixtures under `.ralph-burning/` may still be in scope when this task explicitly edits them.",
            project_id.as_str()
        ));
    }

    guidance
}

fn build_voter_prompt(
    title: &str,
    amendments: &[CanonicalAmendment],
    backend_target: &ResolvedBackendTarget,
    base_dir: &Path,
    project_id: Option<&ProjectId>,
) -> AppResult<String> {
    let schema_str = serde_json::to_string_pretty(&processed_contract_schema_value(
        &InvocationContract::Panel {
            stage_id: StageId::FinalReview,
            role: "voter".to_owned(),
        },
        backend_target.backend.family,
    ))?;
    let amendment_text = amendments
        .iter()
        .map(|amendment| {
            let pe_note = amendment
                .mapped_to_bead_id
                .as_ref()
                .map(|id| {
                    format!(
                        "\n\n**Classification: planned-elsewhere** — mapped to bead `{id}`. \
                         If accepted, this amendment will NOT trigger a restart; the concern \
                         will be recorded as already covered by the mapped-to bead."
                    )
                })
                .unwrap_or_default();
            format!(
                "## Amendment: {}\n\n{}{}",
                amendment.amendment_id, amendment.normalized_body, pe_note
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    template_catalog::resolve_and_render(
        "final_review_voter",
        base_dir,
        project_id,
        &[
            ("title", title),
            ("amendments", &amendment_text),
            ("json_schema", &schema_str),
        ],
    )
}

fn build_arbiter_prompt(
    disputed_amendments: &HashMap<String, &CanonicalAmendment>,
    reviewer_votes: &[FinalReviewVotePayload],
    backend_target: &ResolvedBackendTarget,
    base_dir: &Path,
    project_id: Option<&ProjectId>,
) -> AppResult<String> {
    let schema_str = serde_json::to_string_pretty(&processed_contract_schema_value(
        &InvocationContract::Panel {
            stage_id: StageId::FinalReview,
            role: "arbiter".to_owned(),
        },
        backend_target.backend.family,
    ))?;
    let amendment_text = disputed_amendments
        .values()
        .map(|amendment| {
            let pe_note = amendment
                .mapped_to_bead_id
                .as_ref()
                .map(|id| {
                    format!(
                        "\n\n**Classification: planned-elsewhere** — mapped to bead `{id}`. \
                         If accepted, this amendment will NOT trigger a restart; the concern \
                         will be recorded as already covered by the mapped-to bead."
                    )
                })
                .unwrap_or_default();
            format!(
                "## Amendment: {}\n\n{}{}",
                amendment.amendment_id, amendment.normalized_body, pe_note
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let reviewer_str = serde_json::to_string_pretty(reviewer_votes)?;
    template_catalog::resolve_and_render(
        "final_review_arbiter",
        base_dir,
        project_id,
        &[
            ("amendments", &amendment_text),
            ("reviewer_votes", &reviewer_str),
            ("json_schema", &schema_str),
        ],
    )
}

fn validate_vote_payload(
    payload: &FinalReviewVotePayload,
    amendments: &[CanonicalAmendment],
    target: &ResolvedBackendTarget,
) -> AppResult<()> {
    let expected_ids: Vec<&str> = amendments
        .iter()
        .map(|amendment| amendment.amendment_id.as_str())
        .collect();
    let actual_ids: Vec<&str> = payload
        .votes
        .iter()
        .map(|vote| vote.amendment_id.as_str())
        .collect();
    if actual_ids.len() != expected_ids.len()
        || !expected_ids.iter().all(|id| actual_ids.contains(id))
    {
        return Err(AppError::InvocationFailed {
            backend: target.backend.family.to_string(),
            contract_id: "final_review:voter".to_owned(),
            failure_class: crate::shared::domain::FailureClass::DomainValidationFailure,
            details: "vote payload must cover every amendment exactly once".to_owned(),
        });
    }
    Ok(())
}

fn validate_arbiter_payload(
    payload: &FinalReviewArbiterPayload,
    disputed_ids: &[String],
    target: &ResolvedBackendTarget,
) -> AppResult<()> {
    let actual_ids: Vec<&str> = payload
        .rulings
        .iter()
        .map(|ruling| ruling.amendment_id.as_str())
        .collect();
    if actual_ids.len() != disputed_ids.len()
        || !disputed_ids
            .iter()
            .all(|id| actual_ids.contains(&id.as_str()))
    {
        return Err(AppError::InvocationFailed {
            backend: target.backend.family.to_string(),
            contract_id: "final_review:arbiter".to_owned(),
            failure_class: crate::shared::domain::FailureClass::DomainValidationFailure,
            details: "arbiter payload must resolve every disputed amendment exactly once"
                .to_owned(),
        });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn invoke_final_review_member_once<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    project_root: &Path,
    backend_working_dir: &Path,
    run_id: &RunId,
    stage_id: StageId,
    cursor: &StageCursor,
    target: &ResolvedBackendTarget,
    role: BackendRole,
    member_key: &str,
    panel_role: &str,
    prompt: String,
    context: Value,
    timeout: Duration,
    cancellation_token: CancellationToken,
    invocation_attempt: u32,
) -> AppResult<(Value, RecordProducer)>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    let invocation_id = final_review_invocation_id(
        run_id,
        stage_id,
        cursor,
        panel_role,
        member_key,
        invocation_attempt,
    );

    let request = InvocationRequest {
        invocation_id,
        project_root: project_root.to_path_buf(),
        working_dir: backend_working_dir.to_path_buf(),
        contract: InvocationContract::Panel {
            stage_id,
            role: panel_role.to_owned(),
        },
        role,
        resolved_target: target.clone(),
        payload: InvocationPayload { prompt, context },
        timeout,
        cancellation_token,
        session_policy: SessionPolicy::NewSession,
        prior_session: None,
        attempt_number: cursor.attempt,
    };

    let envelope = agent_service.invoke(request).await?;
    let producer = super::agent_record_producer(&envelope.metadata);
    Ok((envelope.parsed_payload, producer))
}

#[allow(clippy::too_many_arguments)]
async fn invoke_final_review_member<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    project_root: &Path,
    backend_working_dir: &Path,
    run_id: &RunId,
    stage_id: StageId,
    cursor: &StageCursor,
    target: &ResolvedBackendTarget,
    role: BackendRole,
    member_key: &str,
    panel_role: &str,
    reviewer_id: &str,
    prompt: String,
    context: Value,
    timeout: Duration,
    cancellation_token: CancellationToken,
    retry_policy: &RetryPolicy,
) -> Result<FinalReviewMemberInvocationSuccess, FinalReviewMemberInvocationFailure>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    let contract_id = final_review_contract_id(panel_role);
    let mut retry_count = 0;

    for invocation_attempt in 1.. {
        if cancellation_token.is_cancelled() {
            return Err(FinalReviewMemberInvocationFailure {
                error: AppError::InvocationCancelled {
                    backend: target.backend.family.to_string(),
                    contract_id: contract_id.clone(),
                },
                retry_count,
            });
        }

        let prompt = prompt.clone();
        let context = context.clone();
        let cancellation_token = cancellation_token.clone();
        match invoke_final_review_member_once(
            agent_service,
            project_root,
            backend_working_dir,
            run_id,
            stage_id,
            cursor,
            target,
            role,
            member_key,
            panel_role,
            prompt,
            context,
            timeout,
            cancellation_token.clone(),
            invocation_attempt,
        )
        .await
        {
            Ok((payload, producer)) => {
                return Ok(FinalReviewMemberInvocationSuccess {
                    payload,
                    producer,
                    retry_count,
                });
            }
            Err(error) => {
                if matches!(error.failure_class(), Some(FailureClass::Cancellation)) {
                    return Err(FinalReviewMemberInvocationFailure { error, retry_count });
                }
                let Some(retry_failure_class) = final_review_retry_failure_class(&error) else {
                    return Err(FinalReviewMemberInvocationFailure { error, retry_count });
                };

                retry_count += 1;
                let max_attempts = retry_policy.max_attempts(retry_failure_class).max(1);
                if invocation_attempt >= max_attempts {
                    return Err(FinalReviewMemberInvocationFailure {
                        error: rewrite_transient_retry_exhaustion_error(
                            error,
                            reviewer_id,
                            target,
                            retry_count,
                        ),
                        retry_count,
                    });
                }

                let backoff = retry_policy.backoff_for_attempt(invocation_attempt);
                tracing::warn!(
                    reviewer_id = reviewer_id,
                    backend = %target.backend.family.as_str(),
                    model = %target.model.model_id.as_str(),
                    contract_id = contract_id,
                    retry_count = retry_count,
                    max_attempts = max_attempts,
                    backoff_ms = backoff.as_millis().min(u128::from(u64::MAX)) as u64,
                    error = %error,
                    "final_review transient invocation failure; retrying"
                );
                if !backoff.is_zero() {
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        () = cancellation_token.cancelled() => {
                            return Err(FinalReviewMemberInvocationFailure {
                                error: AppError::InvocationCancelled {
                                    backend: target.backend.family.to_string(),
                                    contract_id: contract_id.clone(),
                                },
                                retry_count,
                            });
                        }
                    }
                }
            }
        }
    }

    Err(FinalReviewMemberInvocationFailure {
        error: AppError::InvocationFailed {
            backend: target.backend.family.to_string(),
            contract_id,
            failure_class: FailureClass::TransportFailure,
            details: "final-review retry loop terminated without an invocation result".to_owned(),
        },
        retry_count,
    })
}

fn final_review_invocation_id(
    run_id: &RunId,
    stage_id: StageId,
    cursor: &StageCursor,
    panel_role: &str,
    member_key: &str,
    invocation_attempt: u32,
) -> String {
    let base = format!(
        "{}-{}-{panel_role}-{member_key}-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );
    if invocation_attempt <= 1 {
        base
    } else {
        format!("{base}-r{invocation_attempt}")
    }
}

#[allow(clippy::too_many_arguments)]
fn persist_supporting_record(
    artifact_write: &dyn PayloadArtifactWritePort,
    base_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    cursor: &StageCursor,
    stage_id: StageId,
    rollback_count: u32,
    payload: &Value,
    artifact_content: &str,
    producer: RecordProducer,
    suffix: &str,
) -> AppResult<()> {
    let now = Utc::now();
    let base_id = format!(
        "{}-{}-{suffix}-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );
    let payload_id = if rollback_count == 0 {
        format!("{base_id}-payload")
    } else {
        format!("{base_id}-rb{rollback_count}-payload")
    };
    let artifact_id = if rollback_count == 0 {
        format!("{base_id}-artifact")
    } else {
        format!("{base_id}-rb{rollback_count}-artifact")
    };

    let payload_record = PayloadRecord {
        payload_id: payload_id.clone(),
        stage_id,
        cycle: cursor.cycle,
        attempt: cursor.attempt,
        created_at: now,
        payload: payload.clone(),
        record_kind: RecordKind::StageSupporting,
        producer: Some(producer.clone()),
        completion_round: cursor.completion_round,
    };
    let artifact_record = ArtifactRecord {
        artifact_id,
        payload_id,
        stage_id,
        created_at: now,
        content: artifact_content.to_owned(),
        record_kind: RecordKind::StageSupporting,
        producer: Some(producer),
        completion_round: cursor.completion_round,
    };

    artifact_write.write_payload_artifact_pair(
        base_dir,
        project_id,
        &payload_record,
        &artifact_record,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use tempfile::tempdir;

    use crate::adapters::fs::{
        FileSystem, FsArtifactStore, FsJournalStore, FsPayloadArtifactWriteStore, FsProjectStore,
        FsRawOutputStore, FsRuntimeLogStore, FsRuntimeLogWriteStore, FsSessionStore,
    };
    use crate::contexts::agent_execution::model::{
        InvocationEnvelope, InvocationMetadata, InvocationRequest, RawOutputReference, TokenCounts,
    };
    use crate::contexts::agent_execution::policy::ResolvedPanelMember;
    use crate::contexts::agent_execution::service::AgentExecutionPort;
    use crate::contexts::agent_execution::AgentExecutionService;
    use crate::contexts::project_run_record::model::JournalEventType;
    use crate::contexts::project_run_record::service::{
        self, ArtifactStorePort, CreateProjectInput, PayloadArtifactWritePort, RuntimeLogStorePort,
    };
    use crate::contexts::workspace_governance;
    use crate::shared::domain::{BackendFamily, FlowPreset};
    use crate::shared::error::AppError;

    use super::*;
    use crate::contexts::workflow_composition::panel_contracts::FinalReviewProposal;

    static RETRY_POLICY_ENV_MUTEX: Mutex<()> = Mutex::new(());

    struct EnvVarGuard {
        key: &'static str,
        original: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let original = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, original }
        }

        fn remove(key: &'static str) -> Self {
            let original = std::env::var_os(key);
            std::env::remove_var(key);
            Self { key, original }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match self.original.take() {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    #[derive(Clone, Debug)]
    enum PlannedInvocationFailure {
        Transport(&'static str),
        Timeout(&'static str),
        DomainValidation(&'static str),
    }

    #[cfg(feature = "test-stub")]
    #[test]
    fn final_review_retry_policy_honours_fail_invoke_stage_override() {
        let _env_lock = RETRY_POLICY_ENV_MUTEX
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let _fail_stage = EnvVarGuard::set("RALPH_BURNING_TEST_FAIL_INVOKE_STAGE", "final_review");
        let _explicit_override = EnvVarGuard::remove("RALPH_BURNING_TEST_DISABLE_RETRY_BACKOFF");

        let policy = final_review_retry_policy(BackendPolicyRole::FinalReviewer);

        assert_eq!(policy.backoff_for_attempt(1), Duration::ZERO);
        assert_eq!(policy.max_attempts(FailureClass::TransportFailure), 5);
        assert_eq!(policy.max_attempts(FailureClass::Timeout), 3);
    }

    #[test]
    fn final_review_retry_failure_class_ignores_terminal_invocation_failures() {
        let misleading_domain_validation = AppError::InvocationFailed {
            backend: "codex".to_owned(),
            contract_id: "final_review:reviewer".to_owned(),
            failure_class: FailureClass::DomainValidationFailure,
            details:
                "proposal payload omitted amendment ids; validator note referenced HTTP 503 from a previous run"
                    .to_owned(),
        };
        let exhausted_invocation = AppError::InvocationFailed {
            backend: "codex".to_owned(),
            contract_id: "final_review:reviewer".to_owned(),
            failure_class: FailureClass::BackendExhausted,
            details: "Rate limit reached, try again at 3:03 PM".to_owned(),
        };
        let timeout_domain_validation = AppError::InvocationFailed {
            backend: "codex".to_owned(),
            contract_id: "final_review:reviewer".to_owned(),
            failure_class: FailureClass::DomainValidationFailure,
            details: "validator note: process exceeded timeout during a prior run".to_owned(),
        };
        let disconnected_transport_failure = AppError::InvocationFailed {
            backend: "codex".to_owned(),
            contract_id: "final_review:reviewer".to_owned(),
            failure_class: FailureClass::TransportFailure,
            details: "ERROR: stream disconnected before completion".to_owned(),
        };

        assert_eq!(
            final_review_retry_failure_class(&misleading_domain_validation),
            None
        );
        assert_eq!(
            final_review_retry_failure_class(&exhausted_invocation),
            None
        );
        assert_eq!(
            final_review_retry_failure_class(&timeout_domain_validation),
            None
        );
        assert_eq!(
            final_review_retry_failure_class(&disconnected_transport_failure),
            Some(FailureClass::TransportFailure)
        );
    }

    #[test]
    fn final_review_retry_exhaustion_helpers_distinguish_invocation_from_availability() {
        let invocation_exhaustion = AppError::InvocationFailed {
            backend: "codex".to_owned(),
            contract_id: "final_review:reviewer".to_owned(),
            failure_class: FailureClass::TransportFailure,
            details: "reviewer-1 (codex/gpt-5.5-xhigh) exhausted 5 transient retries: ERROR: stream disconnected before completion".to_owned(),
        };
        let availability_exhaustion = AppError::BackendUnavailable {
            backend: "codex".to_owned(),
            details: "reviewer-1 (codex/gpt-5.5-xhigh) exhausted 5 transient retries: stream disconnected before completion".to_owned(),
            failure_class: Some(FailureClass::TransportFailure),
        };

        assert!(is_final_review_invocation_retry_exhaustion_error(
            &invocation_exhaustion
        ));
        assert!(!is_final_review_invocation_retry_exhaustion_error(
            &availability_exhaustion
        ));
        assert!(!is_final_review_availability_retry_exhaustion_error(
            &invocation_exhaustion
        ));
        assert!(is_final_review_availability_retry_exhaustion_error(
            &availability_exhaustion
        ));
    }

    #[derive(Clone, Default)]
    struct RecordingFinalReviewAdapter {
        availability_checks: Arc<Mutex<Vec<String>>>,
        requests: Arc<Mutex<Vec<(String, String)>>>,
        proposal_failures: Arc<HashSet<String>>,
        vote_failures: Arc<HashSet<String>>,
        /// Members whose proposal invocations fail with BackendExhausted.
        proposal_exhausted: Arc<HashSet<String>>,
        scripted_availability_failures: Arc<Mutex<HashMap<String, VecDeque<&'static str>>>>,
        scripted_proposal_failures: Arc<Mutex<HashMap<String, VecDeque<PlannedInvocationFailure>>>>,
        scripted_vote_failures: Arc<Mutex<HashMap<String, VecDeque<PlannedInvocationFailure>>>>,
        scripted_arbiter_failures: Arc<Mutex<HashMap<String, VecDeque<PlannedInvocationFailure>>>>,
        invocation_delays: Arc<HashMap<String, Duration>>,
        actual_targets: Arc<HashMap<String, ResolvedBackendTarget>>,
        deferred_template_override: Arc<Mutex<Option<DeferredTemplateOverride>>>,
    }

    #[derive(Clone)]
    struct DeferredTemplateOverride {
        invocation_id_fragment: String,
        template_id: String,
        content: String,
    }

    impl RecordingFinalReviewAdapter {
        fn with_proposal_failure(member_key: &str) -> Self {
            Self {
                proposal_failures: Arc::new(HashSet::from([member_key.to_owned()])),
                ..Default::default()
            }
        }

        fn with_vote_failure(member_key: &str) -> Self {
            Self {
                vote_failures: Arc::new(HashSet::from([member_key.to_owned()])),
                ..Default::default()
            }
        }

        fn with_proposal_exhausted(keys: &[&str]) -> Self {
            Self {
                proposal_exhausted: Arc::new(keys.iter().map(|k| (*k).to_owned()).collect()),
                ..Default::default()
            }
        }

        fn with_scripted_proposal_failures(
            member_key: &str,
            failures: Vec<PlannedInvocationFailure>,
        ) -> Self {
            Self {
                scripted_proposal_failures: Arc::new(Mutex::new(HashMap::from([(
                    member_key.to_owned(),
                    failures.into(),
                )]))),
                ..Default::default()
            }
        }

        fn with_scripted_availability_failures(
            target_model_id: &str,
            failures: Vec<&'static str>,
        ) -> Self {
            Self {
                scripted_availability_failures: Arc::new(Mutex::new(HashMap::from([(
                    target_model_id.to_owned(),
                    failures.into(),
                )]))),
                ..Default::default()
            }
        }

        fn with_scripted_vote_failures(
            member_key: &str,
            failures: Vec<PlannedInvocationFailure>,
        ) -> Self {
            Self {
                scripted_vote_failures: Arc::new(Mutex::new(HashMap::from([(
                    member_key.to_owned(),
                    failures.into(),
                )]))),
                ..Default::default()
            }
        }

        fn with_invocation_delays(entries: &[(&str, Duration)]) -> Self {
            Self {
                invocation_delays: Arc::new(
                    entries
                        .iter()
                        .map(|(fragment, delay)| ((*fragment).to_owned(), *delay))
                        .collect(),
                ),
                ..Default::default()
            }
        }

        fn with_actual_target(member_key: &str, backend: BackendFamily, model_id: &str) -> Self {
            Self {
                actual_targets: Arc::new(HashMap::from([(
                    member_key.to_owned(),
                    ResolvedBackendTarget::new(backend, model_id),
                )])),
                ..Default::default()
            }
        }

        fn with_template_override_after_invocation(
            invocation_id_fragment: &str,
            template_id: &str,
            content: &str,
        ) -> Self {
            Self {
                deferred_template_override: Arc::new(Mutex::new(Some(DeferredTemplateOverride {
                    invocation_id_fragment: invocation_id_fragment.to_owned(),
                    template_id: template_id.to_owned(),
                    content: content.to_owned(),
                }))),
                ..Default::default()
            }
        }

        fn invocation_ids_for(&self, contract_label: &str) -> Vec<String> {
            self.requests
                .lock()
                .expect("recording adapter lock poisoned")
                .iter()
                .filter(|(label, _)| label == contract_label)
                .map(|(_, invocation_id)| invocation_id.clone())
                .collect()
        }

        fn availability_checks_for(&self, model_id: &str) -> usize {
            self.availability_checks
                .lock()
                .expect("recording adapter lock poisoned")
                .iter()
                .filter(|recorded_model_id| recorded_model_id.as_str() == model_id)
                .count()
        }

        fn all_invocation_ids(&self) -> Vec<String> {
            self.requests
                .lock()
                .expect("recording adapter lock poisoned")
                .iter()
                .map(|(_, invocation_id)| invocation_id.clone())
                .collect()
        }

        fn pop_scripted_availability_failure(
            plans: &Mutex<HashMap<String, VecDeque<&'static str>>>,
            backend: &ResolvedBackendTarget,
        ) -> Option<AppError> {
            let mut plans = plans.lock().expect("recording adapter lock poisoned");
            let key = backend.model.model_id.as_str();
            let (details, should_remove) = {
                let queue = plans.get_mut(key)?;
                let details = queue.pop_front()?;
                let should_remove = queue.is_empty();
                (details, should_remove)
            };
            if should_remove {
                plans.remove(key);
            }
            Some(AppError::BackendUnavailable {
                backend: backend.backend.family.to_string(),
                details: details.to_owned(),
                failure_class: None,
            })
        }

        fn pop_scripted_failure(
            plans: &Mutex<HashMap<String, VecDeque<PlannedInvocationFailure>>>,
            request: &InvocationRequest,
        ) -> Option<AppError> {
            let mut plans = plans.lock().expect("recording adapter lock poisoned");
            let member_key = plans
                .keys()
                .find(|member_key| request.invocation_id.contains(member_key.as_str()))
                .cloned()?;
            let (failure, should_remove) = {
                let queue = plans
                    .get_mut(&member_key)
                    .expect("scripted failure queue should exist");
                let failure = queue.pop_front()?;
                let should_remove = queue.is_empty();
                (failure, should_remove)
            };
            if should_remove {
                plans.remove(&member_key);
            }

            let failure_class = match failure {
                PlannedInvocationFailure::Transport(_) => FailureClass::TransportFailure,
                PlannedInvocationFailure::Timeout(_) => FailureClass::Timeout,
                PlannedInvocationFailure::DomainValidation(_) => {
                    FailureClass::DomainValidationFailure
                }
            };
            let details = match failure {
                PlannedInvocationFailure::Transport(details)
                | PlannedInvocationFailure::Timeout(details)
                | PlannedInvocationFailure::DomainValidation(details) => details.to_owned(),
            };
            match failure_class {
                FailureClass::Timeout => Some(AppError::InvocationTimeout {
                    backend: request.resolved_target.backend.family.to_string(),
                    contract_id: request.contract.label(),
                    timeout_ms: 1_000,
                    details,
                }),
                _ => Some(AppError::InvocationFailed {
                    backend: request.resolved_target.backend.family.to_string(),
                    contract_id: request.contract.label(),
                    failure_class,
                    details,
                }),
            }
        }
    }

    impl AgentExecutionPort for RecordingFinalReviewAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &InvocationContract,
        ) -> AppResult<()> {
            Ok(())
        }

        async fn check_availability(&self, backend: &ResolvedBackendTarget) -> AppResult<()> {
            self.availability_checks
                .lock()
                .expect("recording adapter lock poisoned")
                .push(backend.model.model_id.to_owned());
            if let Some(error) = Self::pop_scripted_availability_failure(
                self.scripted_availability_failures.as_ref(),
                backend,
            ) {
                return Err(error);
            }
            Ok(())
        }

        async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
            let contract_label = request.contract.label();
            self.requests
                .lock()
                .expect("recording adapter lock poisoned")
                .push((contract_label.clone(), request.invocation_id.clone()));

            if contract_label == "final_review:reviewer" {
                if let Some(error) =
                    Self::pop_scripted_failure(self.scripted_proposal_failures.as_ref(), &request)
                {
                    return Err(error);
                }
            }
            if contract_label == "final_review:arbiter" {
                if let Some(error) =
                    Self::pop_scripted_failure(self.scripted_arbiter_failures.as_ref(), &request)
                {
                    return Err(error);
                }
            }
            if contract_label == "final_review:voter" {
                if let Some(error) =
                    Self::pop_scripted_failure(self.scripted_vote_failures.as_ref(), &request)
                {
                    return Err(error);
                }
            }

            if contract_label == "final_review:reviewer"
                && self
                    .proposal_failures
                    .iter()
                    .any(|member_key| request.invocation_id.contains(member_key))
            {
                return Err(AppError::InvocationFailed {
                    backend: request.resolved_target.backend.family.to_string(),
                    contract_id: contract_label,
                    failure_class: crate::shared::domain::FailureClass::TransportFailure,
                    details: "optional reviewer failed during proposals".to_owned(),
                });
            }
            if contract_label == "final_review:reviewer"
                && self
                    .proposal_exhausted
                    .iter()
                    .any(|member_key| request.invocation_id.contains(member_key))
            {
                return Err(AppError::InvocationFailed {
                    backend: request.resolved_target.backend.family.to_string(),
                    contract_id: contract_label,
                    failure_class: crate::shared::domain::FailureClass::BackendExhausted,
                    details: "reviewer backend exhausted (credits/quota)".to_owned(),
                });
            }
            if contract_label == "final_review:voter"
                && self
                    .vote_failures
                    .iter()
                    .any(|member_key| request.invocation_id.contains(member_key))
            {
                return Err(AppError::InvocationFailed {
                    backend: request.resolved_target.backend.family.to_string(),
                    contract_id: contract_label,
                    failure_class: crate::shared::domain::FailureClass::TransportFailure,
                    details: "optional reviewer failed during voting".to_owned(),
                });
            }
            if let Some(delay) = self.invocation_delays.iter().find_map(|(fragment, delay)| {
                request.invocation_id.contains(fragment).then_some(*delay)
            }) {
                tokio::time::sleep(delay).await;
            }

            let amendment_id = canonical_amendment_id(1, "tighten wording");
            let actual_target = self.actual_targets.iter().find_map(|(member_key, target)| {
                request
                    .invocation_id
                    .contains(member_key)
                    .then(|| target.clone())
            });
            let backend_used = actual_target
                .as_ref()
                .map(|target| target.backend.clone())
                .unwrap_or_else(|| request.resolved_target.backend.clone());
            let model_used = actual_target
                .as_ref()
                .map(|target| target.model.clone())
                .unwrap_or_else(|| request.resolved_target.model.clone());
            let deferred_template_override = {
                let mut guard = self
                    .deferred_template_override
                    .lock()
                    .expect("recording adapter lock poisoned");
                match guard.as_ref() {
                    Some(override_spec)
                        if request
                            .invocation_id
                            .contains(&override_spec.invocation_id_fragment) =>
                    {
                        guard.take()
                    }
                    _ => None,
                }
            };
            let payload = match contract_label.as_str() {
                "final_review:reviewer" => json!({
                    "summary": "proposal",
                    "amendments": [
                        {
                            "body": "tighten wording",
                            "rationale": "worth doing"
                        }
                    ]
                }),
                "final_review:voter" => json!({
                    "summary": "vote",
                    "votes": [
                        {
                            "amendment_id": amendment_id,
                            "decision": "accept",
                            "rationale": "agree"
                        }
                    ]
                }),
                "final_review:arbiter" => json!({
                    "summary": "arbiter",
                    "rulings": []
                }),
                other => panic!("unexpected contract label: {other}"),
            };

            if let Some(override_spec) = deferred_template_override {
                write_project_template_override(
                    &request.project_root,
                    &override_spec.template_id,
                    &override_spec.content,
                )?;
            }

            Ok(InvocationEnvelope {
                raw_output_reference: RawOutputReference::Inline("{}".to_owned()),
                parsed_payload: payload,
                metadata: InvocationMetadata {
                    invocation_id: request.invocation_id,
                    duration: Duration::from_millis(1),
                    token_counts: TokenCounts::default(),
                    backend_used,
                    model_used,
                    adapter_reported_backend: None,
                    adapter_reported_model: None,
                    attempt_number: request.attempt_number,
                    session_id: None,
                    session_reused: false,
                },
                timestamp: Utc::now(),
            })
        }

        async fn cancel(&self, _invocation_id: &str) -> AppResult<()> {
            Ok(())
        }
    }

    fn setup_project(base_dir: &Path, project_name: &str) -> ProjectId {
        workspace_governance::initialize_workspace(base_dir, Utc::now()).expect("workspace init");
        let project_id = ProjectId::new(project_name).expect("project id");
        service::create_project(
            &FsProjectStore,
            &FsJournalStore,
            base_dir,
            CreateProjectInput {
                id: project_id.clone(),
                name: project_name.to_owned(),
                flow: FlowPreset::Standard,
                prompt_path: "prompt.md".to_owned(),
                prompt_contents: "# Prompt\n\nTest prompt.\n".to_owned(),
                prompt_hash: FileSystem::prompt_hash("# Prompt\n\nTest prompt.\n"),
                created_at: Utc::now(),
                task_source: None,
            },
        )
        .expect("project creation");
        project_id
    }

    fn project_root(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
        base_dir
            .join(".ralph-burning")
            .join("projects")
            .join(project_id.as_str())
    }

    fn write_workspace_template_override(base_dir: &Path, template_id: &str, content: &str) {
        let template_dir = base_dir.join(".ralph-burning").join("templates");
        std::fs::create_dir_all(&template_dir).expect("workspace template dir");
        std::fs::write(template_dir.join(format!("{template_id}.md")), content)
            .expect("workspace template override");
    }

    fn write_project_template_override(
        project_root: &Path,
        template_id: &str,
        content: &str,
    ) -> AppResult<()> {
        let template_dir = project_root.join("templates");
        std::fs::create_dir_all(&template_dir)?;
        std::fs::write(template_dir.join(format!("{template_id}.md")), content)?;
        Ok(())
    }

    #[test]
    fn build_reviewer_prompt_surfaces_task_prompt_contract_guidance() {
        let tmp = tempdir().expect("tempdir");
        let prompt = build_reviewer_prompt(
            "<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            &ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                "claude-test",
            ),
            tmp.path(),
            tmp.path(),
            None,
        )
        .expect("reviewer prompt");

        assert!(prompt.contains("## Task Prompt Contract"));
        assert!(prompt.contains("## Already Planned Elsewhere"));
        assert!(prompt.contains("## Review Scope"));
    }

    #[test]
    fn build_reviewer_prompt_allows_nearby_work_ids_for_planned_elsewhere_mapping() {
        let tmp = tempdir().expect("tempdir");
        let project_prompt = format!(
            "{}\n# Ralph Task Prompt\n\n- Milestone: `9ni`\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\n### Siblings\n- `9ni.6.5` [open] Adjacent prompt routing\n  Scope: Owns review routing for adjacent findings.\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nNo explicit planned-elsewhere items were supplied.\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            task_prompt_contract::contract_marker()
        );

        let prompt = build_reviewer_prompt(
            &project_prompt,
            &ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                "claude-test",
            ),
            tmp.path(),
            tmp.path(),
            None,
        )
        .expect("reviewer prompt");

        assert!(prompt.contains("\"Already Planned Elsewhere\" or \"Nearby work\""));
        assert!(prompt.contains("- `9ni.6.5`"));
        assert!(prompt.contains("- `6.5`"));
        assert!(!prompt.contains("unlikely to apply"));
    }

    #[test]
    fn final_review_mapping_allowlist_accepts_nearby_work_ids() {
        let project_prompt = format!(
            "{}\n# Ralph Task Prompt\n\n- Milestone: `9ni`\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\n### Direct dependents\n- `9ni.6.5` [open] Consume nearby routing\n  Scope: Owns adjacent review finding routing.\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nNo explicit planned-elsewhere items were supplied.\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            task_prompt_contract::contract_marker()
        );
        let allowed =
            task_prompt_contract::extract_planned_elsewhere_routing_bead_ids(&project_prompt);

        assert!(mapped_to_bead_id_is_allowed(&allowed, "9ni.6.5"));
        assert!(mapped_to_bead_id_is_allowed(&allowed, "6.5"));
        assert!(mapped_to_bead_id_is_allowed(&allowed, " 9ni.6.5 "));
        assert!(!mapped_to_bead_id_is_allowed(&allowed, "9ni.6.7"));
    }

    #[test]
    fn final_review_mapping_allowlist_rejects_foreign_nearby_short_alias() {
        let project_prompt = format!(
            "{}\n# Ralph Task Prompt\n\n- Milestone: `current-ms`\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\n### Related work\n- `other-ms.bead-4` [open] Foreign related routing\n  Scope: Owns adjacent review finding routing.\n- `current-ms.bead-5` [open] Local related routing\n  Scope: Owns adjacent review finding routing.\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nNo explicit planned-elsewhere items were supplied.\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            task_prompt_contract::contract_marker()
        );
        let allowed =
            task_prompt_contract::extract_planned_elsewhere_routing_bead_ids(&project_prompt);

        assert!(mapped_to_bead_id_is_allowed(&allowed, "other-ms.bead-4"));
        assert!(!mapped_to_bead_id_is_allowed(&allowed, "bead-4"));
        assert!(mapped_to_bead_id_is_allowed(&allowed, "current-ms.bead-5"));
        assert!(mapped_to_bead_id_is_allowed(&allowed, "bead-5"));
    }

    #[test]
    fn build_reviewer_prompt_allows_legacy_override_without_task_prompt_contract_placeholder() {
        let tmp = tempdir().expect("tempdir");
        write_workspace_template_override(
            tmp.path(),
            "final_review_reviewer",
            "LEGACY REVIEWER\n\n{{project_prompt}}\n\n{{json_schema}}",
        );

        let prompt = build_reviewer_prompt(
            "# Prompt\n\nGeneric.",
            &ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                "claude-test",
            ),
            tmp.path(),
            tmp.path(),
            None,
        )
        .expect("reviewer prompt");

        assert!(prompt.starts_with("LEGACY REVIEWER"));
        assert!(prompt.contains("# Prompt"));
    }

    fn run_git(repo_root: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .current_dir(repo_root)
            .args(args)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    #[test]
    fn build_reviewer_prompt_includes_branch_range_and_runtime_state_exclusion() {
        let tmp = tempdir().expect("tempdir");
        let repo_root = tmp.path();
        run_git(repo_root, &["init", "-b", "main"]);
        run_git(repo_root, &["config", "user.name", "Test User"]);
        run_git(repo_root, &["config", "user.email", "test@example.com"]);
        std::fs::write(repo_root.join("tracked.txt"), "base\n").expect("write base file");
        run_git(repo_root, &["add", "tracked.txt"]);
        run_git(repo_root, &["commit", "-m", "base"]);
        let fork_point = run_git(repo_root, &["rev-parse", "HEAD"]);
        run_git(repo_root, &["checkout", "-b", "feature/final-review-scope"]);
        std::fs::write(repo_root.join("tracked.txt"), "base\nchange\n").expect("write change");
        run_git(repo_root, &["commit", "-am", "change"]);

        let project_id = ProjectId::new("scope-demo").expect("project id");
        let prompt = build_reviewer_prompt(
            "# Prompt\n\nGeneric.",
            &ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                "claude-test",
            ),
            repo_root,
            repo_root,
            Some(&project_id),
        )
        .expect("reviewer prompt");

        assert!(prompt.contains(&format!(
            "compare against `main` from fork point `{fork_point}`"
        )));
        assert!(prompt.contains(&format!("git diff {fork_point}..HEAD")));
        assert!(prompt.contains("git status --short"));
        assert!(prompt.contains(".ralph-burning/projects/scope-demo/"));
    }

    #[test]
    fn build_reviewer_prompt_falls_back_when_base_ref_cannot_be_resolved() {
        let tmp = tempdir().expect("tempdir");
        let prompt = build_reviewer_prompt(
            "# Prompt\n\nGeneric.",
            &ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                "claude-test",
            ),
            tmp.path(),
            tmp.path(),
            None,
        )
        .expect("reviewer prompt");

        assert!(prompt.contains("Do NOT rely on plain `git diff` alone"));
        assert!(prompt
            .contains("The default branch could not be resolved automatically in this checkout."));
        assert!(prompt.contains("git diff <fork-point>..HEAD"));
        assert!(prompt.contains("git diff"));
        assert!(prompt.contains("git status --short"));
    }

    #[test]
    fn build_reviewer_prompt_falls_back_when_fork_point_cannot_be_computed() {
        let tmp = tempdir().expect("tempdir");
        let repo_root = tmp.path();
        run_git(repo_root, &["init", "-b", "main"]);
        run_git(repo_root, &["config", "user.name", "Test User"]);
        run_git(repo_root, &["config", "user.email", "test@example.com"]);
        std::fs::write(repo_root.join("tracked.txt"), "base\n").expect("write base file");
        run_git(repo_root, &["add", "tracked.txt"]);
        run_git(repo_root, &["commit", "-m", "base"]);
        run_git(
            repo_root,
            &["checkout", "--orphan", "feature/unrelated-history"],
        );
        std::fs::remove_file(repo_root.join("tracked.txt")).expect("remove inherited file");
        std::fs::write(repo_root.join("tracked.txt"), "orphan\n").expect("write orphan file");
        run_git(repo_root, &["add", "tracked.txt"]);
        run_git(repo_root, &["commit", "-m", "orphan"]);

        let prompt = build_reviewer_prompt(
            "# Prompt\n\nGeneric.",
            &ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                "claude-test",
            ),
            repo_root,
            repo_root,
            None,
        )
        .expect("reviewer prompt");

        assert!(prompt.contains(
            "The base ref resolved to `main`, but the fork point could not be computed automatically."
        ));
        assert!(prompt.contains("Do NOT rely on plain `git diff` alone"));
        assert!(prompt.contains("git diff <fork-point>..HEAD"));
        assert!(prompt.contains("git status --short"));
    }

    fn proposal_record(reviewer_id: &str, body: &str) -> ReviewerProposalRecord {
        ReviewerProposalRecord {
            member_index: reviewer_id
                .rsplit_once('-')
                .and_then(|(_, value)| value.parse::<usize>().ok())
                .unwrap_or(1)
                .saturating_sub(1),
            reviewer_id: reviewer_id.to_owned(),
            required: true,
            backend_family: "claude".to_owned(),
            model_id: format!("model-{reviewer_id}"),
            target: ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                format!("model-{reviewer_id}"),
            ),
            payload: FinalReviewProposalPayload {
                summary: format!("proposal from {reviewer_id}"),
                amendments: vec![FinalReviewProposal {
                    body: body.to_owned(),
                    rationale: None,
                    mapped_to_bead_id: None,
                    classification: None,
                    proposed_title: None,
                    proposed_scope: None,
                    severity: None,
                }],
            },
        }
    }

    struct AlwaysFailPayloadArtifactWriteStore;

    impl PayloadArtifactWritePort for AlwaysFailPayloadArtifactWriteStore {
        fn write_payload_artifact_pair(
            &self,
            _base_dir: &Path,
            _project_id: &ProjectId,
            _payload: &PayloadRecord,
            _artifact: &ArtifactRecord,
        ) -> AppResult<()> {
            Err(AppError::Io(std::io::Error::other(
                "simulated payload/artifact write failure",
            )))
        }

        fn remove_payload_artifact_pair(
            &self,
            _base_dir: &Path,
            _project_id: &ProjectId,
            _payload_id: &str,
            _artifact_id: &str,
        ) -> AppResult<()> {
            Ok(())
        }
    }

    #[test]
    fn normalize_amendment_body_collapses_whitespace_but_preserves_newlines() {
        let raw = "  tighten\t the wording \r\n  keep\tthis  line \r final line   ";
        assert_eq!(
            normalize_amendment_body(raw),
            "tighten the wording\nkeep this line\nfinal line"
        );
    }

    #[test]
    fn canonical_amendment_id_is_stable_for_equivalent_bodies() {
        let left = canonical_amendment_id(2, "  tighten  wording \r\n");
        let right = canonical_amendment_id(2, "tighten wording\n");
        assert_eq!(left, right);
        assert!(left.starts_with("fr-2-"));
    }

    #[test]
    fn merge_final_review_amendments_deduplicates_and_preserves_sources() {
        let merged = merge_final_review_amendments(
            1,
            &[
                proposal_record("reviewer-a", " tighten   wording "),
                proposal_record("reviewer-b", "tighten wording"),
            ],
        );

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].normalized_body, "tighten wording");
        assert_eq!(merged[0].sources.len(), 2);
        assert_eq!(
            merged[0].amendment_id,
            canonical_amendment_id(1, "tighten wording")
        );
    }

    #[test]
    fn merge_final_review_amendments_keeps_duplicate_backend_slots_distinct() {
        let shared_model = "claude-opus".to_owned();
        let merged = merge_final_review_amendments(
            1,
            &[
                ReviewerProposalRecord {
                    member_index: 0,
                    reviewer_id: final_review_reviewer_id(0),
                    required: true,
                    backend_family: "claude".to_owned(),
                    model_id: shared_model.clone(),
                    target: ResolvedBackendTarget::new(
                        crate::shared::domain::BackendFamily::Claude,
                        shared_model.clone(),
                    ),
                    payload: FinalReviewProposalPayload {
                        summary: "slot one".to_owned(),
                        amendments: vec![FinalReviewProposal {
                            body: " tighten wording ".to_owned(),
                            rationale: None,
                            mapped_to_bead_id: None,
                            classification: None,
                            proposed_title: None,
                            proposed_scope: None,
                            severity: None,
                        }],
                    },
                },
                ReviewerProposalRecord {
                    member_index: 1,
                    reviewer_id: final_review_reviewer_id(1),
                    required: true,
                    backend_family: "claude".to_owned(),
                    model_id: shared_model,
                    target: ResolvedBackendTarget::new(
                        crate::shared::domain::BackendFamily::Claude,
                        "claude-opus",
                    ),
                    payload: FinalReviewProposalPayload {
                        summary: "slot two".to_owned(),
                        amendments: vec![FinalReviewProposal {
                            body: "tighten wording".to_owned(),
                            rationale: None,
                            mapped_to_bead_id: None,
                            classification: None,
                            proposed_title: None,
                            proposed_scope: None,
                            severity: None,
                        }],
                    },
                },
            ],
        );

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].sources.len(), 2);
        assert_eq!(merged[0].sources[0].reviewer_id, "reviewer-1");
        assert_eq!(merged[0].sources[1].reviewer_id, "reviewer-2");
        assert_eq!(merged[0].sources[0].model_id, "claude-opus");
        assert_eq!(merged[0].sources[1].model_id, "claude-opus");
    }

    fn proposal_record_with_mapping(
        reviewer_id: &str,
        body: &str,
        mapped_to_bead_id: Option<&str>,
    ) -> ReviewerProposalRecord {
        ReviewerProposalRecord {
            member_index: reviewer_id
                .rsplit_once('-')
                .and_then(|(_, value)| value.parse::<usize>().ok())
                .unwrap_or(1)
                .saturating_sub(1),
            reviewer_id: reviewer_id.to_owned(),
            required: true,
            backend_family: "claude".to_owned(),
            model_id: format!("model-{reviewer_id}"),
            target: ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                format!("model-{reviewer_id}"),
            ),
            payload: FinalReviewProposalPayload {
                summary: format!("proposal from {reviewer_id}"),
                amendments: vec![FinalReviewProposal {
                    body: body.to_owned(),
                    rationale: None,
                    mapped_to_bead_id: mapped_to_bead_id.map(|s| s.to_owned()),
                    classification: None,
                    proposed_title: None,
                    proposed_scope: None,
                    severity: None,
                }],
            },
        }
    }

    fn proposal_record_propose_new_bead(
        reviewer_id: &str,
        body: &str,
        proposed_title: &str,
        proposed_scope: &str,
        severity: review_classification::Severity,
        rationale: &str,
    ) -> ReviewerProposalRecord {
        ReviewerProposalRecord {
            member_index: reviewer_id
                .rsplit_once('-')
                .and_then(|(_, value)| value.parse::<usize>().ok())
                .unwrap_or(1)
                .saturating_sub(1),
            reviewer_id: reviewer_id.to_owned(),
            required: true,
            backend_family: "claude".to_owned(),
            model_id: format!("model-{reviewer_id}"),
            target: ResolvedBackendTarget::new(
                crate::shared::domain::BackendFamily::Claude,
                format!("model-{reviewer_id}"),
            ),
            payload: FinalReviewProposalPayload {
                summary: format!("proposal from {reviewer_id}"),
                amendments: vec![FinalReviewProposal {
                    body: body.to_owned(),
                    rationale: Some(rationale.to_owned()),
                    mapped_to_bead_id: None,
                    classification: Some(AmendmentClassification::ProposeNewBead),
                    proposed_title: Some(proposed_title.to_owned()),
                    proposed_scope: Some(proposed_scope.to_owned()),
                    severity: Some(severity),
                }],
            },
        }
    }

    #[test]
    fn merge_planned_elsewhere_unanimous_preserves_mapping() {
        // Both reviewers agree the amendment is planned-elsewhere to the same bead.
        let merged = merge_final_review_amendments(
            1,
            &[
                proposal_record_with_mapping("reviewer-a", "fix error handling", Some("bead-42")),
                proposal_record_with_mapping("reviewer-b", "fix error handling", Some("bead-42")),
            ],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].mapped_to_bead_id.as_deref(),
            Some("bead-42"),
            "unanimous planned-elsewhere should preserve mapping"
        );
    }

    #[test]
    fn merge_planned_elsewhere_disagreement_clears_mapping() {
        // Reviewer A says planned-elsewhere, reviewer B says regular amendment.
        let merged = merge_final_review_amendments(
            1,
            &[
                proposal_record_with_mapping("reviewer-a", "fix error handling", Some("bead-42")),
                proposal_record_with_mapping("reviewer-b", "fix error handling", None),
            ],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].mapped_to_bead_id, None,
            "disagreement on planned-elsewhere should clear mapping"
        );
    }

    #[test]
    fn merge_planned_elsewhere_conflicting_targets_clears_mapping() {
        // Both say planned-elsewhere but point to different beads.
        let merged = merge_final_review_amendments(
            1,
            &[
                proposal_record_with_mapping("reviewer-a", "fix error handling", Some("bead-42")),
                proposal_record_with_mapping("reviewer-b", "fix error handling", Some("bead-99")),
            ],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].mapped_to_bead_id, None,
            "conflicting targets should clear mapping"
        );
    }

    #[test]
    fn merge_first_none_then_some_clears_mapping() {
        // Reviewer A proposes as regular, reviewer B says planned-elsewhere.
        // Conservative: any disagreement clears the mapping.
        let merged = merge_final_review_amendments(
            1,
            &[
                proposal_record_with_mapping("reviewer-a", "fix error handling", None),
                proposal_record_with_mapping("reviewer-b", "fix error handling", Some("bead-42")),
            ],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].mapped_to_bead_id, None,
            "first-None then-Some should clear mapping (disagreement)"
        );
    }

    #[test]
    fn merge_propose_new_bead_unanimous_preserves_creation_metadata() {
        let merged = merge_final_review_amendments(
            1,
            &[
                proposal_record_propose_new_bead(
                    "reviewer-a",
                    "missing telemetry for retries",
                    "Add retry telemetry",
                    "Instrument retry loops with counters and histograms",
                    review_classification::Severity::Medium,
                    "No existing bead covers retry observability",
                ),
                proposal_record_propose_new_bead(
                    "reviewer-b",
                    "missing telemetry for retries",
                    "Add retry telemetry",
                    "Instrument retry loops with counters and histograms",
                    review_classification::Severity::Medium,
                    "No existing bead covers retry observability",
                ),
            ],
        );

        assert_eq!(merged.len(), 1);
        assert_eq!(
            merged[0].classification,
            AmendmentClassification::ProposeNewBead
        );
        assert_eq!(
            merged[0].proposed_title.as_deref(),
            Some("Add retry telemetry")
        );
        assert_eq!(
            merged[0].proposed_scope.as_deref(),
            Some("Instrument retry loops with counters and histograms")
        );
        assert_eq!(
            merged[0].severity,
            Some(review_classification::Severity::Medium)
        );
        assert_eq!(
            merged[0].rationale.as_deref(),
            Some("No existing bead covers retry observability")
        );
    }

    #[test]
    fn merge_propose_new_bead_metadata_disagreement_falls_back_to_fix_now() {
        let merged = merge_final_review_amendments(
            1,
            &[
                proposal_record_propose_new_bead(
                    "reviewer-a",
                    "missing telemetry for retries",
                    "Add retry telemetry",
                    "Instrument retry loops with counters and histograms",
                    review_classification::Severity::Medium,
                    "No existing bead covers retry observability",
                ),
                proposal_record_propose_new_bead(
                    "reviewer-b",
                    "missing telemetry for retries",
                    "Add retry-loop tracing",
                    "Instrument retry loops with structured tracing",
                    review_classification::Severity::Medium,
                    "Need better diagnostics",
                ),
            ],
        );

        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].classification, AmendmentClassification::FixNow);
        assert_eq!(merged[0].proposed_title, None);
        assert_eq!(merged[0].proposed_scope, None);
        assert_eq!(merged[0].severity, None);
        assert_eq!(merged[0].rationale, None);
    }

    #[test]
    fn consensus_status_applies_accept_reject_and_dispute_rules() {
        assert_eq!(
            consensus_status(2, 3, 0.66),
            FinalReviewConsensusStatus::Accepted
        );
        assert_eq!(
            consensus_status(0, 3, 0.66),
            FinalReviewConsensusStatus::Rejected
        );
        assert_eq!(
            consensus_status(1, 2, 0.75),
            FinalReviewConsensusStatus::Disputed
        );
    }

    #[tokio::test]
    async fn final_review_reviewer_prompt_failure_does_not_emit_started_telemetry() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-reviewer-prompt-failure");
        write_workspace_template_override(base_dir, "final_review_reviewer", "broken reviewer");
        let adapter = RecordingFinalReviewAdapter::default();
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-reviewer-prompt-failure").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-1-model"),
                    required: true,
                    configured_index: 0,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Codex, "reviewer-2-model"),
                    required: true,
                    configured_index: 1,
                },
            ],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let error = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            2,
            0,
            1.0,
            0,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("reviewer prompt failure should abort final review");

        assert!(
            matches!(error, AppError::MalformedTemplate { .. }),
            "expected malformed template error, got: {error:?}"
        );
        assert!(adapter
            .invocation_ids_for("final_review:reviewer")
            .is_empty());

        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        assert!(!events.iter().any(|event| {
            matches!(
                event.event_type,
                JournalEventType::ReviewerStarted | JournalEventType::ReviewerCompleted
            )
        }));

        let runtime_logs = FsRuntimeLogStore
            .read_runtime_logs(base_dir, &project_id)
            .expect("runtime logs");
        assert!(runtime_logs.iter().all(|entry| {
            !entry.message.contains("final_review started:")
                && !entry.message.contains("final_review completed:")
        }));
    }

    #[tokio::test]
    async fn final_review_voter_prompt_failure_does_not_emit_vote_started_telemetry() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-voter-prompt-failure");
        write_workspace_template_override(base_dir, "final_review_voter", "broken voter");
        let adapter = RecordingFinalReviewAdapter::default();
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-voter-prompt-failure").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-1-model"),
                required: true,
                configured_index: 0,
            }],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let error = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            1,
            0,
            1.0,
            0,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("voter prompt failure should abort final review");

        assert!(
            matches!(error, AppError::MalformedTemplate { .. }),
            "expected malformed template error, got: {error:?}"
        );
        assert_eq!(adapter.invocation_ids_for("final_review:reviewer").len(), 1);
        assert!(adapter.invocation_ids_for("final_review:voter").is_empty());

        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        assert!(!events.iter().any(|event| {
            matches!(
                event.event_type,
                JournalEventType::ReviewerStarted | JournalEventType::ReviewerCompleted
            ) && event.details["phase"] == "vote"
        }));

        let runtime_logs = FsRuntimeLogStore
            .read_runtime_logs(base_dir, &project_id)
            .expect("runtime logs");
        assert!(runtime_logs
            .iter()
            .all(|entry| !entry.message.contains("phase=vote")));
    }

    #[tokio::test]
    async fn final_review_reviewer_vote_prompt_failure_does_not_emit_member_started_telemetry() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-reviewer-vote-prompt-failure");
        let adapter = RecordingFinalReviewAdapter::with_template_override_after_invocation(
            "reviewer-reviewer-1",
            "final_review_voter",
            "broken voter",
        );
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-reviewer-vote-prompt-failure").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-1-model"),
                required: true,
                configured_index: 0,
            }],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let error = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            1,
            0,
            1.0,
            0,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("reviewer vote prompt failure should abort final review");

        assert!(
            matches!(error, AppError::MalformedTemplate { .. }),
            "expected malformed template error, got: {error:?}"
        );
        assert_eq!(
            adapter.invocation_ids_for("final_review:voter").len(),
            0,
            "vote prompt construction should fail before any reviewer vote invocation"
        );

        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        assert!(!events.iter().any(|event| {
            matches!(
                event.event_type,
                JournalEventType::ReviewerStarted | JournalEventType::ReviewerCompleted
            ) && event.details["phase"] == "vote"
                && event.details["reviewer_id"] == "reviewer-2"
        }));

        let runtime_logs = FsRuntimeLogStore
            .read_runtime_logs(base_dir, &project_id)
            .expect("runtime logs");
        assert!(runtime_logs.iter().all(|entry| {
            !(entry.message.contains("phase=vote")
                && entry.message.contains("reviewer_id=reviewer-2"))
        }));
    }

    #[tokio::test]
    async fn final_review_arbiter_prompt_failure_does_not_emit_started_telemetry() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-arbiter-prompt-failure");
        write_workspace_template_override(base_dir, "final_review_arbiter", "broken arbiter");
        let adapter = RecordingFinalReviewAdapter::default();
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-arbiter-prompt-failure").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-1-model"),
                required: true,
                configured_index: 0,
            }],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let error = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            1,
            0,
            1.1,
            0,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("arbiter prompt failure should abort final review");

        assert!(
            matches!(error, AppError::MalformedTemplate { .. }),
            "expected malformed template error, got: {error:?}"
        );
        assert!(adapter
            .invocation_ids_for("final_review:arbiter")
            .is_empty());

        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        assert!(!events.iter().any(|event| {
            matches!(
                event.event_type,
                JournalEventType::ReviewerStarted | JournalEventType::ReviewerCompleted
            ) && event.details["phase"] == "arbitrate"
                && event.details["reviewer_id"] == "arbiter"
        }));

        let runtime_logs = FsRuntimeLogStore
            .read_runtime_logs(base_dir, &project_id)
            .expect("runtime logs");
        assert!(runtime_logs.iter().all(|entry| {
            !(entry.message.contains("phase=arbitrate")
                && entry.message.contains("reviewer_id=arbiter"))
        }));
    }

    #[tokio::test]
    async fn final_review_votes_only_with_successful_proposal_reviewers_and_uses_unique_ids() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-sticky-reviewers");
        let adapter = RecordingFinalReviewAdapter::with_proposal_failure("reviewer-3");
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-1-model"),
                    required: true,
                    configured_index: 0,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Codex, "reviewer-2-model"),
                    required: true,
                    configured_index: 1,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(
                        BackendFamily::OpenRouter,
                        "reviewer-3-model",
                    ),
                    required: false,
                    configured_index: 2,
                },
            ],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let result = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &crate::adapters::fs::FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            2,
            0,
            0.66,
            2,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .expect("final review should succeed");

        assert!(result.restart_required);
        assert!(!result.final_accepted_amendments.is_empty());

        let vote_ids = adapter.invocation_ids_for("final_review:voter");
        assert_eq!(vote_ids.len(), 2, "two successful reviewers should vote");
        assert!(
            vote_ids
                .iter()
                .all(|invocation_id| !invocation_id.contains("reviewer-3")),
            "optional reviewer skipped during proposals must not re-enter voting: {vote_ids:?}"
        );

        let all_ids = adapter.all_invocation_ids();
        let unique_ids = all_ids.iter().cloned().collect::<HashSet<_>>();
        assert_eq!(
            all_ids.len(),
            unique_ids.len(),
            "every final-review invocation id must be distinct: {all_ids:?}"
        );

        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        let reviewer_started: Vec<_> = events
            .iter()
            .filter(|event| {
                event.event_type
                    == crate::contexts::project_run_record::model::JournalEventType::ReviewerStarted
            })
            .collect();
        let reviewer_completed: Vec<_> = events
            .iter()
            .filter(|event| event.event_type == crate::contexts::project_run_record::model::JournalEventType::ReviewerCompleted)
            .collect();
        assert!(
            reviewer_started.len() >= 4,
            "expected proposal and reviewer vote starts"
        );
        assert_eq!(
            reviewer_started.len(),
            reviewer_completed.len(),
            "every panel member start should have a matching completion"
        );
        assert!(reviewer_completed.iter().any(|event| {
            event.details["reviewer_id"] == "reviewer-1"
                && event.details["phase"] == "proposal"
                && event.details["outcome"] == "proposed_amendments"
        }));
    }

    #[tokio::test]
    async fn final_review_emits_reviewer_completion_events_in_finish_order() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-completion-order");
        let adapter = RecordingFinalReviewAdapter::with_invocation_delays(&[
            ("reviewer-reviewer-1", Duration::from_millis(40)),
            ("reviewer-reviewer-2", Duration::from_millis(5)),
            ("voter-reviewer-1", Duration::from_millis(40)),
            ("voter-reviewer-2", Duration::from_millis(5)),
        ]);
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-completion-order").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-1-model"),
                    required: true,
                    configured_index: 0,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-2-model"),
                    required: true,
                    configured_index: 1,
                },
            ],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &crate::adapters::fs::FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            2,
            0,
            0.66,
            2,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .expect("final review should succeed");

        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        let proposal_completions: Vec<_> = events
            .iter()
            .filter(|event| {
                event.event_type
                    == crate::contexts::project_run_record::model::JournalEventType::ReviewerCompleted
                    && event.details["phase"] == "proposal"
                    && event.details["role"] == "reviewer"
                    && event.details["outcome"] == "proposed_amendments"
            })
            .map(|event| event.details["reviewer_id"].as_str().unwrap().to_owned())
            .collect();
        let vote_completions: Vec<_> = events
            .iter()
            .filter(|event| {
                event.event_type
                    == crate::contexts::project_run_record::model::JournalEventType::ReviewerCompleted
                    && event.details["phase"] == "vote"
                    && event.details["role"] == "reviewer"
                    && event.details["outcome"] == "reviewer_votes_recorded"
            })
            .map(|event| event.details["reviewer_id"].as_str().unwrap().to_owned())
            .collect();

        assert_eq!(
            proposal_completions,
            vec!["reviewer-2".to_owned(), "reviewer-1".to_owned()],
            "proposal completion events should follow actual finish order"
        );
        assert_eq!(
            vote_completions,
            vec!["reviewer-2".to_owned(), "reviewer-1".to_owned()],
            "vote completion events should follow actual finish order"
        );
    }

    #[tokio::test]
    async fn final_review_completion_telemetry_persists_even_when_supporting_record_write_fails() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-persist-failure-telemetry");
        let adapter = RecordingFinalReviewAdapter::default();
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-persist-failure").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-1-model"),
                required: true,
                configured_index: 0,
            }],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let error = execute_final_review_panel(
            &agent_service,
            &AlwaysFailPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            1,
            0,
            1.0,
            0,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("supporting record persistence should fail");

        assert!(
            matches!(error, AppError::Io(_)),
            "expected write failure, got: {error:?}"
        );

        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        assert!(events.iter().any(|event| {
            event.event_type
                == crate::contexts::project_run_record::model::JournalEventType::ReviewerCompleted
                && event.details["reviewer_id"] == "reviewer-1"
                && event.details["phase"] == "proposal"
                && event.details["outcome"] == "proposed_amendments"
        }));

        let runtime_logs = FsRuntimeLogStore
            .read_runtime_logs(base_dir, &project_id)
            .expect("runtime logs");
        assert!(runtime_logs.iter().any(|entry| {
            entry.message.contains("final_review completed:")
                && entry.message.contains("reviewer_id=reviewer-1")
                && entry.message.contains("phase=proposal")
                && entry.message.contains("outcome=proposed_amendments")
        }));
    }

    #[tokio::test]
    async fn final_review_uses_effective_producer_identity_for_sources_and_completion_events() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-effective-producer-identity");
        let adapter = RecordingFinalReviewAdapter::with_actual_target(
            "reviewer-1",
            BackendFamily::OpenRouter,
            "openai/gpt-4.1",
        );
        let agent_service = AgentExecutionService::new(adapter, FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-effective-identity").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-1-model"),
                required: true,
                configured_index: 0,
            }],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let result = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            1,
            0,
            1.0,
            0,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .expect("final review should succeed");

        assert_eq!(result.final_accepted_amendments.len(), 1);
        assert_eq!(
            result.final_accepted_amendments[0].sources[0].backend_family,
            "openrouter"
        );
        assert_eq!(
            result.final_accepted_amendments[0].sources[0].model_id,
            "openai/gpt-4.1"
        );

        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        assert!(events.iter().any(|event| {
            event.event_type
                == crate::contexts::project_run_record::model::JournalEventType::ReviewerCompleted
                && event.details["reviewer_id"] == "reviewer-1"
                && event.details["phase"] == "proposal"
                && event.details["backend_family"] == "openrouter"
                && event.details["model_id"] == "openai/gpt-4.1"
        }));
    }

    #[tokio::test]
    async fn final_review_retries_transient_reviewer_failure_and_records_single_success() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-transient-reviewer-success");
        let adapter = RecordingFinalReviewAdapter::with_scripted_proposal_failures(
            "reviewer-1",
            vec![PlannedInvocationFailure::Transport(
                "ERROR: stream disconnected before completion",
            )],
        );
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-transient-success").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.5-xhigh"),
                required: true,
                configured_index: 0,
            }],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let result = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            1,
            0,
            1.0,
            0,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .expect("transient codex transport failure should succeed on retry");

        assert!(
            !result.aggregate_artifact.is_empty(),
            "stage should complete with an aggregate result"
        );
        assert_eq!(adapter.invocation_ids_for("final_review:reviewer").len(), 2);

        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        let proposal_started: Vec<_> = events
            .iter()
            .filter(|event| {
                event.event_type == JournalEventType::ReviewerStarted
                    && event.details["phase"] == "proposal"
                    && event.details["reviewer_id"] == "reviewer-1"
            })
            .collect();
        let proposal_completed: Vec<_> = events
            .iter()
            .filter(|event| {
                event.event_type == JournalEventType::ReviewerCompleted
                    && event.details["phase"] == "proposal"
                    && event.details["reviewer_id"] == "reviewer-1"
                    && event.details["outcome"] == "proposed_amendments"
            })
            .collect();
        assert_eq!(
            proposal_started.len(),
            1,
            "retry loop must not add started events"
        );
        assert_eq!(
            proposal_completed.len(),
            1,
            "retry loop must emit one terminal completion event"
        );

        let runtime_logs = FsRuntimeLogStore
            .read_runtime_logs(base_dir, &project_id)
            .expect("runtime logs");
        assert!(runtime_logs.iter().any(|entry| {
            entry.message.contains("phase=proposal")
                && entry.message.contains("reviewer_id=reviewer-1")
                && entry.message.contains("outcome=proposed_amendments")
                && entry.message.contains("retry_count=1")
        }));
    }

    #[tokio::test]
    async fn final_review_fails_after_exhausting_transient_reviewer_retries() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-transient-reviewer-exhausted");
        let adapter = RecordingFinalReviewAdapter::with_scripted_proposal_failures(
            "reviewer-1",
            vec![
                PlannedInvocationFailure::Transport("ERROR: stream disconnected before completion",);
                5
            ],
        );
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-transient-exhausted").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.5-xhigh"),
                required: true,
                configured_index: 0,
            }],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let error = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            1,
            0,
            1.0,
            0,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("persistent transient failures should bubble out");

        let message = error.to_string();
        assert!(message.contains("exhausted 5 transient retries"));
        assert!(message.contains("stream disconnected before completion"));
        assert_eq!(adapter.invocation_ids_for("final_review:reviewer").len(), 5);

        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        let proposal_started: Vec<_> = events
            .iter()
            .filter(|event| {
                event.event_type == JournalEventType::ReviewerStarted
                    && event.details["phase"] == "proposal"
                    && event.details["reviewer_id"] == "reviewer-1"
            })
            .collect();
        let proposal_completed: Vec<_> = events
            .iter()
            .filter(|event| {
                event.event_type == JournalEventType::ReviewerCompleted
                    && event.details["phase"] == "proposal"
                    && event.details["reviewer_id"] == "reviewer-1"
                    && event.details["outcome"] == "failed"
            })
            .collect();
        assert_eq!(proposal_started.len(), 1);
        assert_eq!(proposal_completed.len(), 1);

        let runtime_logs = FsRuntimeLogStore
            .read_runtime_logs(base_dir, &project_id)
            .expect("runtime logs");
        assert!(runtime_logs.iter().any(|entry| {
            entry.message.contains("phase=proposal")
                && entry.message.contains("reviewer_id=reviewer-1")
                && entry.message.contains("outcome=failed")
                && entry.message.contains("retry_count=5")
        }));
    }

    #[tokio::test]
    async fn final_review_timeout_retries_stop_at_timeout_policy_limit() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-timeout-reviewer-exhausted");
        let adapter = RecordingFinalReviewAdapter::with_scripted_proposal_failures(
            "reviewer-1",
            vec![
                PlannedInvocationFailure::Timeout("reviewer proposal timed out before completion",);
                3
            ],
        );
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-timeout-exhausted").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.5-xhigh"),
                required: true,
                configured_index: 0,
            }],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let error = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            1,
            0,
            1.0,
            0,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("persistent timeout failures should stop at the timeout retry limit");

        let message = error.to_string();
        assert!(message.contains("exhausted 3 transient retries"));
        assert!(message.contains("timed out before completion"));
        assert_eq!(adapter.invocation_ids_for("final_review:reviewer").len(), 3);

        let runtime_logs = FsRuntimeLogStore
            .read_runtime_logs(base_dir, &project_id)
            .expect("runtime logs");
        assert!(runtime_logs.iter().any(|entry| {
            entry.message.contains("phase=proposal")
                && entry.message.contains("reviewer_id=reviewer-1")
                && entry.message.contains("outcome=failed")
                && entry.message.contains("retry_count=3")
        }));
    }

    #[tokio::test]
    async fn final_review_does_not_retry_non_transient_domain_validation_failures() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-domain-validation-no-retry");
        let adapter = RecordingFinalReviewAdapter::with_scripted_proposal_failures(
            "reviewer-1",
            vec![PlannedInvocationFailure::DomainValidation(
                "proposal payload omitted required amendment ids; validator note referenced HTTP 503 in earlier logs",
            )],
        );
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-domain-validation").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.5-xhigh"),
                    required: false,
                    configured_index: 0,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-2-model"),
                    required: true,
                    configured_index: 1,
                },
            ],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let result = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            1,
            0,
            1.0,
            0,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .expect("optional domain-validation failure should fall through to remaining reviewers");

        assert!(
            !result.aggregate_artifact.is_empty(),
            "remaining reviewer should carry the stage to completion"
        );
        let reviewer_attempts: Vec<_> = adapter
            .invocation_ids_for("final_review:reviewer")
            .into_iter()
            .filter(|invocation_id| invocation_id.contains("reviewer-1"))
            .collect();
        assert_eq!(
            reviewer_attempts.len(),
            1,
            "non-transient domain validation failures must not be retried"
        );

        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        assert!(events.iter().any(|event| {
            event.event_type == JournalEventType::ReviewerCompleted
                && event.details["phase"] == "proposal"
                && event.details["reviewer_id"] == "reviewer-1"
                && event.details["outcome"] == "failed_optional"
        }));

        let runtime_logs = FsRuntimeLogStore
            .read_runtime_logs(base_dir, &project_id)
            .expect("runtime logs");
        assert!(runtime_logs.iter().any(|entry| {
            entry.message.contains("phase=proposal")
                && entry.message.contains("reviewer_id=reviewer-1")
                && entry.message.contains("outcome=failed_optional")
                && entry.message.contains("retry_count=0")
        }));
    }

    #[tokio::test]
    async fn final_review_retries_transient_reviewer_availability_probe_failures() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-reviewer-availability-retry");
        let adapter = RecordingFinalReviewAdapter::with_scripted_availability_failures(
            "gpt-5.5-xhigh",
            vec!["availability probe failed: connection reset by peer"],
        );
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-reviewer-availability").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.5-xhigh"),
                required: true,
                configured_index: 0,
            }],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let result = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            1,
            0,
            1.0,
            0,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .expect("transient availability probe failures should retry and succeed");

        assert!(
            !result.aggregate_artifact.is_empty(),
            "stage should complete after the availability retry"
        );
        assert!(
            adapter.availability_checks_for("gpt-5.5-xhigh") >= 2,
            "proposal availability should be rechecked after the transient failure"
        );
        assert_eq!(adapter.invocation_ids_for("final_review:reviewer").len(), 1);

        let runtime_logs = FsRuntimeLogStore
            .read_runtime_logs(base_dir, &project_id)
            .expect("runtime logs");
        assert!(runtime_logs.iter().any(|entry| {
            entry.message.contains("phase=proposal")
                && entry.message.contains("reviewer_id=reviewer-1")
                && entry.message.contains("outcome=proposed_amendments")
                && entry.message.contains("retry_count=1")
        }));
    }

    #[tokio::test]
    async fn final_review_retries_transient_reviewer_rate_limit_probe_failures() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-reviewer-availability-rate-limit-retry");
        let adapter = RecordingFinalReviewAdapter::with_scripted_availability_failures(
            "gpt-5.5-xhigh",
            vec!["HTTP 429: Too Many Requests"],
        );
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id =
            RunId::new("run-final-review-reviewer-availability-rate-limit").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::OpenRouter, "gpt-5.5-xhigh"),
                required: true,
                configured_index: 0,
            }],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let result = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            1,
            0,
            1.0,
            0,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .expect("transient 429 availability probe failures should retry and succeed");

        assert!(
            !result.aggregate_artifact.is_empty(),
            "stage should complete after the transient 429 availability retry"
        );
        assert!(
            adapter.availability_checks_for("gpt-5.5-xhigh") >= 2,
            "proposal availability should be rechecked after the transient 429 failure"
        );
        assert_eq!(adapter.invocation_ids_for("final_review:reviewer").len(), 1);

        let runtime_logs = FsRuntimeLogStore
            .read_runtime_logs(base_dir, &project_id)
            .expect("runtime logs");
        assert!(runtime_logs.iter().any(|entry| {
            entry.message.contains("phase=proposal")
                && entry.message.contains("reviewer_id=reviewer-1")
                && entry.message.contains("outcome=proposed_amendments")
                && entry.message.contains("retry_count=1")
        }));
    }

    #[tokio::test]
    async fn final_review_retries_transient_vote_timeouts_and_records_single_success() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-transient-vote-timeout");
        let adapter = RecordingFinalReviewAdapter::with_scripted_vote_failures(
            "reviewer-1",
            vec![PlannedInvocationFailure::Timeout(
                "reviewer vote timed out before completion",
            )],
        );
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-vote-timeout").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![ResolvedPanelMember {
                target: ResolvedBackendTarget::new(BackendFamily::Codex, "gpt-5.5-xhigh"),
                required: true,
                configured_index: 0,
            }],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let result = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            1,
            0,
            1.0,
            0,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .expect("transient vote timeout should succeed on retry");

        assert!(
            !result.final_accepted_amendments.is_empty(),
            "stage should complete after a retried vote timeout"
        );
        assert_eq!(adapter.invocation_ids_for("final_review:reviewer").len(), 1);
        assert_eq!(adapter.invocation_ids_for("final_review:voter").len(), 2);

        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        let vote_started: Vec<_> = events
            .iter()
            .filter(|event| {
                event.event_type == JournalEventType::ReviewerStarted
                    && event.details["phase"] == "vote"
                    && event.details["reviewer_id"] == "reviewer-1"
            })
            .collect();
        let vote_completed: Vec<_> = events
            .iter()
            .filter(|event| {
                event.event_type == JournalEventType::ReviewerCompleted
                    && event.details["phase"] == "vote"
                    && event.details["reviewer_id"] == "reviewer-1"
                    && event.details["outcome"] == "reviewer_votes_recorded"
            })
            .collect();
        assert_eq!(
            vote_started.len(),
            1,
            "retry loop must not add vote started events"
        );
        assert_eq!(
            vote_completed.len(),
            1,
            "retry loop must emit one terminal vote completion event"
        );

        let runtime_logs = FsRuntimeLogStore
            .read_runtime_logs(base_dir, &project_id)
            .expect("runtime logs");
        assert!(runtime_logs.iter().any(|entry| {
            entry.message.contains("phase=vote")
                && entry.message.contains("reviewer_id=reviewer-1")
                && entry.message.contains("outcome=reviewer_votes_recorded")
                && entry.message.contains("retry_count=1")
        }));
    }

    #[tokio::test]
    async fn final_review_skips_optional_vote_failures_when_min_reviewers_still_hold() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-optional-vote-failure");
        let adapter = RecordingFinalReviewAdapter::with_vote_failure("reviewer-3");
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-vote-failure").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-1-model"),
                    required: true,
                    configured_index: 0,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Codex, "reviewer-2-model"),
                    required: true,
                    configured_index: 1,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(
                        BackendFamily::OpenRouter,
                        "reviewer-3-model",
                    ),
                    required: false,
                    configured_index: 2,
                },
            ],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let result = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &crate::adapters::fs::FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            2,
            0,
            0.66,
            2,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .expect("final review should tolerate optional vote failures");

        assert!(result.restart_required);
        assert_eq!(result.aggregate_payload["total_reviewers"], json!(2));
        let vote_ids = adapter.invocation_ids_for("final_review:voter");
        assert_eq!(
            vote_ids.len(),
            7,
            "two successful voters plus five retries for the optional transient failure should be recorded"
        );
        assert_eq!(
            vote_ids
                .iter()
                .filter(|invocation_id| invocation_id.contains("reviewer-3"))
                .count(),
            5,
            "optional vote failures should exhaust the transient retry budget before degrading"
        );
    }

    #[tokio::test]
    async fn final_review_supporting_artifacts_persist_agent_producer_metadata() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-producer-metadata");
        let agent_service = AgentExecutionService::new(
            RecordingFinalReviewAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let run_id = RunId::new("run-final-review-producer").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-1-model"),
                    required: true,
                    configured_index: 0,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Codex, "reviewer-2-model"),
                    required: true,
                    configured_index: 1,
                },
            ],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &crate::adapters::fs::FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            2,
            0,
            0.5,
            2,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .expect("final review should succeed");

        let artifacts = FsArtifactStore
            .list_artifacts(base_dir, &project_id)
            .unwrap();
        let supporting_final_review_artifacts: Vec<_> = artifacts
            .into_iter()
            .filter(|record| {
                record.stage_id == StageId::FinalReview
                    && record.record_kind == RecordKind::StageSupporting
            })
            .collect();

        assert!(
            supporting_final_review_artifacts.len() >= 4,
            "expected reviewer proposals and reviewer vote artifacts"
        );
        assert!(
            supporting_final_review_artifacts.iter().all(|record| matches!(
                &record.producer,
                Some(RecordProducer::Agent {
                    requested_backend_family,
                    requested_model_id,
                    ..
                }) if !requested_backend_family.is_empty() && !requested_model_id.is_empty()
            )),
            "final-review supporting artifacts must persist agent producer metadata: {supporting_final_review_artifacts:?}"
        );
    }

    #[tokio::test]
    async fn final_review_supporting_artifact_ids_align_with_reviewer_ids() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-supporting-artifact-ids");
        let agent_service = AgentExecutionService::new(
            RecordingFinalReviewAdapter::default(),
            FsRawOutputStore,
            FsSessionStore,
        );
        let run_id = RunId::new("run-final-review-artifact-ids").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-1-model"),
                    required: true,
                    configured_index: 0,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Codex, "reviewer-2-model"),
                    required: true,
                    configured_index: 1,
                },
            ],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &crate::adapters::fs::FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            2,
            0,
            0.5,
            2,
            0,
            "prompt.md",
            0,
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .expect("final review should succeed");

        let artifacts = FsArtifactStore
            .list_artifacts(base_dir, &project_id)
            .expect("list artifacts");
        let supporting_ids: Vec<_> = artifacts
            .into_iter()
            .filter(|record| {
                record.stage_id == StageId::FinalReview
                    && record.record_kind == RecordKind::StageSupporting
            })
            .map(|record| record.artifact_id)
            .collect();

        assert!(
            supporting_ids
                .iter()
                .any(|id| id.contains("-final_review-reviewer-1-")),
            "proposal artifact should align with reviewer-1: {supporting_ids:?}"
        );
        assert!(
            supporting_ids
                .iter()
                .any(|id| id.contains("-final_review-reviewer-2-")),
            "proposal artifact should align with reviewer-2: {supporting_ids:?}"
        );
        assert!(
            supporting_ids
                .iter()
                .any(|id| id.contains("-final_review-vote-1-")),
            "vote artifact should align with reviewer 1: {supporting_ids:?}"
        );
        assert!(
            supporting_ids
                .iter()
                .any(|id| id.contains("-final_review-vote-2-")),
            "vote artifact should align with reviewer 2: {supporting_ids:?}"
        );
        assert!(
            supporting_ids
                .iter()
                .all(|id| !id.contains("-final_review-reviewer-0-") && !id.contains("-final_review-vote-0-")),
            "final-review supporting artifacts must not use zero-based reviewer ids: {supporting_ids:?}"
        );
    }

    #[tokio::test]
    async fn final_review_degrades_when_one_reviewer_backend_exhausted() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-one-exhausted");
        let adapter = RecordingFinalReviewAdapter::with_proposal_exhausted(&["reviewer-1"]);
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-fr-one-exhausted").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        // Two required reviewers, min_reviewers=2. One exhausts.
        // With quorum reduction, effective min becomes 1, so panel succeeds.
        let panel = FinalReviewPanelResolution {
            reviewers: vec![
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Codex, "reviewer-1-model"),
                    required: true,
                    configured_index: 0,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-2-model"),
                    required: true,
                    configured_index: 1,
                },
            ],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let result = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            2,    // min_reviewers
            0,    // probe_exhausted_count
            0.66, // acceptance_threshold
            2,    // total_reviewers
            0,    // rollback_count
            "prompt.md",
            0, // skip_final_validation_rounds
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await;

        assert!(
            result.is_ok(),
            "panel should succeed with one exhausted reviewer: {}",
            result.err().map_or("ok".to_owned(), |e| e.to_string())
        );
        // Verify the exhausted reviewer was logged in journal details
        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        let exhausted_events: Vec<_> = events
            .iter()
            .filter(|e| {
                matches!(e.event_type, JournalEventType::ReviewerCompleted)
                    && e.details.get("outcome").and_then(|s| s.as_str()) == Some("failed_exhausted")
            })
            .collect();
        assert_eq!(
            exhausted_events.len(),
            1,
            "expected exactly one failed_exhausted reviewer event"
        );
    }

    #[tokio::test]
    async fn final_review_fails_when_all_reviewers_exhausted() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-all-exhausted");
        let adapter =
            RecordingFinalReviewAdapter::with_proposal_exhausted(&["reviewer-1", "reviewer-2"]);
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-fr-all-exhausted").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            reviewers: vec![
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Codex, "reviewer-0-model"),
                    required: true,
                    configured_index: 0,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "reviewer-1-model"),
                    required: true,
                    configured_index: 1,
                },
            ],
            arbiter: ResolvedBackendTarget::new(BackendFamily::Claude, "arbiter-model"),
        };

        let error = execute_final_review_panel(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            &FsJournalStore,
            base_dir,
            &project_root(base_dir, &project_id),
            base_dir,
            &project_id,
            &run_id,
            &mut seq,
            &cursor,
            &panel,
            2,    // min_reviewers
            0,    // probe_exhausted_count
            0.66, // acceptance_threshold
            2,    // total_reviewers
            0,    // rollback_count
            "prompt.md",
            0, // skip_final_validation_rounds
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("panel should fail when all reviewers are exhausted");

        assert!(
            error
                .failure_class()
                .is_some_and(|fc| fc == FailureClass::BackendExhausted),
            "error should be BackendExhausted: {error:?}"
        );
    }
}
