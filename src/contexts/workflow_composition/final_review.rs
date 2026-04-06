#![forbid(unsafe_code)]

//! Final-review panel orchestration.
//!
//! The panel flow is:
//! 1. Collect reviewer amendment proposals.
//! 2. Normalize and deduplicate amendments by canonical body hash.
//! 3. Short-circuit completion if no amendments remain.
//! 4. Ask the planner for per-amendment positions.
//! 5. Ask reviewers to vote on each amendment.
//! 6. Compute per-amendment consensus.
//! 7. Ask the arbiter to resolve only disputed amendments.
//! 8. Return the final accepted amendment set for either completion or restart.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use chrono::Utc;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::adapters::process_backend::processed_contract_schema_value;
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
    FinalReviewAggregatePayload, FinalReviewAmendmentSource, FinalReviewArbiterPayload,
    FinalReviewCanonicalAmendment, FinalReviewProposalPayload, FinalReviewVoteDecision,
    FinalReviewVotePayload, RecordKind, RecordProducer,
};
use crate::contexts::workflow_composition::renderers;
use crate::contexts::workspace_governance::template_catalog;
use crate::shared::domain::{
    BackendRole, FailureClass, ProjectId, ResolvedBackendTarget, RunId, SessionPolicy, StageCursor,
    StageId,
};
use crate::shared::error::{AppError, AppResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalAmendment {
    pub amendment_id: String,
    pub normalized_body: String,
    pub sources: Vec<FinalReviewAmendmentSource>,
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

fn final_review_reviewer_id(member_index: usize) -> String {
    format!("reviewer-{}", member_index + 1)
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

            match by_body.get_mut(&normalized_body) {
                Some(existing) => {
                    if !existing.sources.contains(&source) {
                        existing.sources.push(source);
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
    planner_timeout: Duration,
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

    let mut reviewer_records = Vec::new();
    let mut proposal_total_exhausted: usize = 0;
    let mut last_proposal_exhaustion_error: Option<AppError> = None;
    for (idx, member) in panel.reviewers.iter().enumerate() {
        let reviewer_id = final_review_reviewer_id(idx);
        let reviewer_prompt =
            build_reviewer_prompt(&project_prompt, &member.target, base_dir, Some(project_id))?;
        append_panel_member_started_event(
            journal_store,
            base_dir,
            project_id,
            seq,
            run_id,
            cursor,
            "proposal",
            &reviewer_id,
            "reviewer",
            &member.target,
        )?;
        append_panel_member_runtime_log(
            log_write,
            base_dir,
            project_id,
            "started",
            "proposal",
            &reviewer_id,
            "reviewer",
            &member.target,
            None,
            None,
            None,
        );
        let started_at = Instant::now();
        let reviewer_payload = invoke_final_review_member(
            agent_service,
            project_root,
            backend_working_dir,
            run_id,
            stage_id,
            cursor,
            &member.target,
            BackendRole::Reviewer,
            &reviewer_id,
            "reviewer",
            reviewer_prompt,
            Value::Null,
            reviewer_timeout_for_backend(member.target.backend.family),
            cancellation_token.clone(),
        )
        .await;

        let (reviewer_payload, producer) = match reviewer_payload {
            Ok(payload) => payload,
            // BackendExhausted is handled first regardless of required/optional
            // status — an exhausted backend is skipped gracefully.
            Err(error)
                if error
                    .failure_class()
                    .is_some_and(|fc| fc == FailureClass::BackendExhausted) =>
            {
                // Track all exhausted members — both required and optional
                // count toward quorum reduction because resolve_panel_backends
                // counts all resolved members toward the configured minimum.
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
                    &reviewer_id,
                    "reviewer",
                    &member.target,
                    started_at.elapsed(),
                    "failed_exhausted",
                    0,
                )?;
                append_panel_member_runtime_log(
                    log_write,
                    base_dir,
                    project_id,
                    "completed",
                    "proposal",
                    &reviewer_id,
                    "reviewer",
                    &member.target,
                    Some(started_at.elapsed()),
                    Some("failed_exhausted"),
                    Some(0),
                );
                // Reduce effective quorum only when exhaustion makes the
                // configured minimum impossible (remaining < min).
                let effective_min = min_reviewers
                    .min(
                        panel
                            .reviewers
                            .len()
                            .saturating_sub(proposal_total_exhausted),
                    )
                    .max(1);
                if reviewer_records.len() + panel.reviewers.len().saturating_sub(idx + 1)
                    < effective_min
                {
                    last_proposal_exhaustion_error = Some(error);
                    break;
                }
                continue;
            }
            Err(error) if !member.required => {
                append_panel_member_completed_event(
                    journal_store,
                    base_dir,
                    project_id,
                    seq,
                    run_id,
                    cursor,
                    "proposal",
                    &reviewer_id,
                    "reviewer",
                    &member.target,
                    started_at.elapsed(),
                    "failed_optional",
                    0,
                )?;
                append_panel_member_runtime_log(
                    log_write,
                    base_dir,
                    project_id,
                    "completed",
                    "proposal",
                    &reviewer_id,
                    "reviewer",
                    &member.target,
                    Some(started_at.elapsed()),
                    Some("failed_optional"),
                    Some(0),
                );
                // Use the reduced quorum: only reduce when exhaustion
                // makes the configured minimum impossible.
                let effective_optional_min = min_reviewers
                    .min(
                        panel
                            .reviewers
                            .len()
                            .saturating_sub(proposal_total_exhausted),
                    )
                    .max(1);
                if reviewer_records.len() + panel.reviewers.len().saturating_sub(idx + 1)
                    < effective_optional_min
                {
                    tracing::warn!(
                        reviewer = idx,
                        backend = %member.target.backend.family,
                        successful = reviewer_records.len(),
                        remaining = panel.reviewers.len().saturating_sub(idx + 1),
                        exhausted = proposal_total_exhausted,
                        effective_min = effective_optional_min,
                        "proposal quorum shortfall: optional failure + exhaustion makes minimum unreachable"
                    );
                    // Propagate the original error so its FailureClass is
                    // preserved for the engine's retry decision.
                    return Err(error);
                }
                continue;
            }
            Err(error) => {
                append_panel_member_completed_event(
                    journal_store,
                    base_dir,
                    project_id,
                    seq,
                    run_id,
                    cursor,
                    "proposal",
                    &reviewer_id,
                    "reviewer",
                    &member.target,
                    started_at.elapsed(),
                    "failed",
                    0,
                )?;
                append_panel_member_runtime_log(
                    log_write,
                    base_dir,
                    project_id,
                    "completed",
                    "proposal",
                    &reviewer_id,
                    "reviewer",
                    &member.target,
                    Some(started_at.elapsed()),
                    Some("failed"),
                    Some(0),
                );
                return Err(error);
            }
        };
        let (backend_family, model_id) = panel_member_identity_from_producer(
            &producer,
            &member.target,
            "final_review:reviewer",
            "final-review reviewer invocations must produce agent metadata",
        )?;

        let proposal: FinalReviewProposalPayload = serde_json::from_value(reviewer_payload.clone())
            .map_err(|e| {
                let duration = started_at.elapsed();
                let _ = append_panel_member_completed_event_with_identity(
                    journal_store,
                    base_dir,
                    project_id,
                    seq,
                    run_id,
                    cursor,
                    "proposal",
                    &reviewer_id,
                    "reviewer",
                    &backend_family,
                    &model_id,
                    duration,
                    "failed_schema_validation",
                    0,
                );
                append_panel_member_runtime_log_with_identity(
                    log_write,
                    base_dir,
                    project_id,
                    "completed",
                    "proposal",
                    &reviewer_id,
                    "reviewer",
                    &backend_family,
                    &model_id,
                    Some(duration),
                    Some("failed_schema_validation"),
                    Some(0),
                );
                AppError::InvocationFailed {
                    backend: member.target.backend.family.to_string(),
                    contract_id: "final_review:reviewer".to_owned(),
                    failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                    details: format!("final-review proposal schema validation failed: {e}"),
                }
            })?;

        let duration = started_at.elapsed();
        append_panel_member_completed_event_with_identity(
            journal_store,
            base_dir,
            project_id,
            seq,
            run_id,
            cursor,
            "proposal",
            &reviewer_id,
            "reviewer",
            &backend_family,
            &model_id,
            duration,
            "proposed_amendments",
            proposal.amendments.len(),
        )?;
        append_panel_member_runtime_log_with_identity(
            log_write,
            base_dir,
            project_id,
            "completed",
            "proposal",
            &reviewer_id,
            "reviewer",
            &backend_family,
            &model_id,
            Some(duration),
            Some("proposed_amendments"),
            Some(proposal.amendments.len()),
        );
        let artifact = renderers::render_final_review_proposal(&proposal, &producer.to_string());
        persist_supporting_record(
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
            &format!("reviewer-{idx}"),
        )?;

        reviewer_records.push(ReviewerProposalRecord {
            member_index: idx,
            reviewer_id,
            required: member.required,
            backend_family,
            model_id,
            target: member.target.clone(),
            payload: proposal,
        });
    }

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
        // When the shortfall is entirely due to backend exhaustion,
        // propagate the last BackendExhausted error so the engine's
        // failure-class-aware handling can apply (e.g., non-retryable).
        if let Some(exhaustion_error) = last_proposal_exhaustion_error {
            return Err(exhaustion_error);
        }
        return Err(AppError::InsufficientPanelMembers {
            panel: "final_review".to_owned(),
            resolved: reviewer_records.len(),
            minimum: effective_proposal_min,
        });
    }

    let amendments = merge_final_review_amendments(cursor.completion_round, &reviewer_records);
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

    let planner_prompt = build_voter_prompt(
        "Planner Positions",
        &amendments,
        None,
        &panel.planner,
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
        "vote",
        "planner",
        "planner",
        &panel.planner,
    )?;
    append_panel_member_runtime_log(
        log_write,
        base_dir,
        project_id,
        "started",
        "vote",
        "planner",
        "planner",
        &panel.planner,
        None,
        None,
        None,
    );
    let planner_started_at = Instant::now();
    let planner_vote_payload = invoke_final_review_member(
        agent_service,
        project_root,
        backend_working_dir,
        run_id,
        stage_id,
        cursor,
        &panel.planner,
        BackendRole::Planner,
        "planner",
        "voter",
        planner_prompt,
        json!({
            "amendments": amendments
                .iter()
                .map(|amendment| {
                    json!({
                        "amendment_id": amendment.amendment_id,
                        "body": amendment.normalized_body,
                    })
                })
                .collect::<Vec<_>>(),
        }),
        planner_timeout,
        cancellation_token.clone(),
    )
    .await;
    let (planner_vote_payload, planner_producer) = match planner_vote_payload {
        Ok(payload) => payload,
        Err(error) => {
            append_panel_member_completed_event(
                journal_store,
                base_dir,
                project_id,
                seq,
                run_id,
                cursor,
                "vote",
                "planner",
                "planner",
                &panel.planner,
                planner_started_at.elapsed(),
                "failed",
                0,
            )?;
            append_panel_member_runtime_log(
                log_write,
                base_dir,
                project_id,
                "completed",
                "vote",
                "planner",
                "planner",
                &panel.planner,
                Some(planner_started_at.elapsed()),
                Some("failed"),
                Some(0),
            );
            return Err(error);
        }
    };
    let (planner_backend_family, planner_model_id) = panel_member_identity_from_producer(
        &planner_producer,
        &panel.planner,
        "final_review:voter",
        "final-review planner invocations must produce agent metadata",
    )?;
    let planner_votes: FinalReviewVotePayload =
        serde_json::from_value(planner_vote_payload.clone()).map_err(|e| {
            let duration = planner_started_at.elapsed();
            let _ = append_panel_member_completed_event_with_identity(
                journal_store,
                base_dir,
                project_id,
                seq,
                run_id,
                cursor,
                "vote",
                "planner",
                "planner",
                &planner_backend_family,
                &planner_model_id,
                duration,
                "failed_schema_validation",
                0,
            );
            append_panel_member_runtime_log_with_identity(
                log_write,
                base_dir,
                project_id,
                "completed",
                "vote",
                "planner",
                "planner",
                &planner_backend_family,
                &planner_model_id,
                Some(duration),
                Some("failed_schema_validation"),
                Some(0),
            );
            AppError::InvocationFailed {
                backend: panel.planner.backend.family.to_string(),
                contract_id: "final_review:voter".to_owned(),
                failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                details: format!("planner positions schema validation failed: {e}"),
            }
        })?;
    validate_vote_payload(&planner_votes, &amendments, &panel.planner).map_err(|error| {
        let duration = planner_started_at.elapsed();
        let _ = append_panel_member_completed_event_with_identity(
            journal_store,
            base_dir,
            project_id,
            seq,
            run_id,
            cursor,
            "vote",
            "planner",
            "planner",
            &planner_backend_family,
            &planner_model_id,
            duration,
            "failed_domain_validation",
            0,
        );
        append_panel_member_runtime_log_with_identity(
            log_write,
            base_dir,
            project_id,
            "completed",
            "vote",
            "planner",
            "planner",
            &planner_backend_family,
            &planner_model_id,
            Some(duration),
            Some("failed_domain_validation"),
            Some(0),
        );
        error
    })?;
    let planner_duration = planner_started_at.elapsed();
    append_panel_member_completed_event_with_identity(
        journal_store,
        base_dir,
        project_id,
        seq,
        run_id,
        cursor,
        "vote",
        "planner",
        "planner",
        &planner_backend_family,
        &planner_model_id,
        planner_duration,
        "planner_positions_recorded",
        planner_votes.votes.len(),
    )?;
    append_panel_member_runtime_log_with_identity(
        log_write,
        base_dir,
        project_id,
        "completed",
        "vote",
        "planner",
        "planner",
        &planner_backend_family,
        &planner_model_id,
        Some(planner_duration),
        Some("planner_positions_recorded"),
        Some(planner_votes.votes.len()),
    );
    let planner_artifact = renderers::render_final_review_vote(
        "Final Review Planner Positions",
        &planner_votes,
        &planner_producer.to_string(),
    );
    persist_supporting_record(
        artifact_write,
        base_dir,
        project_id,
        run_id,
        cursor,
        stage_id,
        rollback_count,
        &planner_vote_payload,
        &planner_artifact,
        planner_producer,
        "planner-positions",
    )?;

    let planner_positions: HashMap<String, FinalReviewVoteDecision> = planner_votes
        .votes
        .iter()
        .map(|vote| (vote.amendment_id.clone(), vote.decision))
        .collect();

    let mut reviewer_votes = Vec::new();
    let mut vote_total_exhausted: usize = 0;
    let mut last_vote_exhaustion_error: Option<AppError> = None;
    for (idx, reviewer) in reviewer_records.iter().enumerate() {
        let vote_prompt = build_voter_prompt(
            "Final Review Votes",
            &amendments,
            Some(&planner_votes),
            &reviewer.target,
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
        let started_at = Instant::now();
        let vote_payload = invoke_final_review_member(
            agent_service,
            project_root,
            backend_working_dir,
            run_id,
            stage_id,
            cursor,
            &reviewer.target,
            BackendRole::Reviewer,
            &reviewer.reviewer_id,
            "voter",
            vote_prompt,
            json!({
                "amendments": amendments
                    .iter()
                    .map(|amendment| {
                        json!({
                            "amendment_id": amendment.amendment_id,
                            "body": amendment.normalized_body,
                            "planner_position": planner_positions
                                .get(&amendment.amendment_id)
                                .map(|decision| decision.to_string()),
                        })
                    })
                    .collect::<Vec<_>>(),
            }),
            reviewer_timeout_for_backend(reviewer.target.backend.family),
            cancellation_token.clone(),
        )
        .await;

        let (vote_payload, producer) = match vote_payload {
            Ok(payload) => payload,
            // BackendExhausted is handled first regardless of required/optional
            // status — an exhausted backend is skipped gracefully.
            Err(error)
                if error
                    .failure_class()
                    .is_some_and(|fc| fc == FailureClass::BackendExhausted) =>
            {
                // Track all exhausted members — both required and optional
                // count toward quorum reduction (see proposal phase comment).
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
                append_panel_member_runtime_log(
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
                );
                let effective_min = min_reviewers
                    .min(reviewer_records.len().saturating_sub(vote_total_exhausted))
                    .max(1);
                if reviewer_votes.len() + reviewer_records.len().saturating_sub(idx + 1)
                    < effective_min
                {
                    last_vote_exhaustion_error = Some(error);
                    break;
                }
                continue;
            }
            Err(error) if !reviewer.required => {
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
                append_panel_member_runtime_log(
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
                );
                // Use the reduced quorum: only reduce when exhaustion
                // makes the configured minimum impossible.
                let effective_optional_min = min_reviewers
                    .min(reviewer_records.len().saturating_sub(vote_total_exhausted))
                    .max(1);
                if reviewer_votes.len() + reviewer_records.len().saturating_sub(idx + 1)
                    < effective_optional_min
                {
                    tracing::warn!(
                        reviewer = idx,
                        backend = %reviewer.target.backend.family,
                        successful_votes = reviewer_votes.len(),
                        remaining = reviewer_records.len().saturating_sub(idx + 1),
                        exhausted = vote_total_exhausted,
                        effective_min = effective_optional_min,
                        "vote quorum shortfall: optional failure + exhaustion makes minimum unreachable"
                    );
                    // Propagate the original error so its FailureClass is
                    // preserved for the engine's retry decision.
                    return Err(error);
                }
                continue;
            }
            Err(error) => {
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
                append_panel_member_runtime_log(
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
                );
                return Err(error);
            }
        };
        let (backend_family, model_id) = panel_member_identity_from_producer(
            &producer,
            &reviewer.target,
            "final_review:voter",
            "final-review reviewer vote invocations must produce agent metadata",
        )?;

        let votes: FinalReviewVotePayload =
            serde_json::from_value(vote_payload.clone()).map_err(|e| {
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
                append_panel_member_runtime_log_with_identity(
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
                );
                AppError::InvocationFailed {
                    backend: reviewer.target.backend.family.to_string(),
                    contract_id: "final_review:voter".to_owned(),
                    failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                    details: format!("final-review vote schema validation failed: {e}"),
                }
            })?;
        validate_vote_payload(&votes, &amendments, &reviewer.target).map_err(|error| {
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
            append_panel_member_runtime_log_with_identity(
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
            );
            error
        })?;
        let duration = started_at.elapsed();
        append_panel_member_completed_event_with_identity(
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
        )?;
        append_panel_member_runtime_log_with_identity(
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
        );

        let artifact = renderers::render_final_review_vote(
            "Final Review Votes",
            &votes,
            &producer.to_string(),
        );
        persist_supporting_record(
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
            &format!("vote-{}", reviewer.member_index),
        )?;

        reviewer_votes.push(votes);
    }

    // Reduce vote quorum only when exhaustion makes the configured
    // minimum impossible.  reviewer_records is the post-proposal panel;
    // vote_total_exhausted is the invocation-time exhaustion during voting.
    let effective_vote_min = min_reviewers
        .min(reviewer_records.len().saturating_sub(vote_total_exhausted))
        .max(1);
    if reviewer_votes.len() < effective_vote_min {
        // When the shortfall is entirely due to backend exhaustion,
        // propagate the last BackendExhausted error so the engine's
        // failure-class-aware handling can apply (e.g., non-retryable).
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
            &planner_votes,
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
        )
        .await;
        let (arbiter_payload, producer) = match arbiter_payload {
            Ok(payload) => payload,
            Err(error) => {
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
                append_panel_member_runtime_log(
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
                append_panel_member_runtime_log_with_identity(
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
                );
                AppError::InvocationFailed {
                    backend: panel.arbiter.backend.family.to_string(),
                    contract_id: "final_review:arbiter".to_owned(),
                    failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                    details: format!("final-review arbiter schema validation failed: {e}"),
                }
            })?;
        validate_arbiter_payload(&arbiter, &disputed_ids, &panel.arbiter).map_err(|error| {
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
            append_panel_member_runtime_log_with_identity(
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
            );
            error
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
        append_panel_member_runtime_log_with_identity(
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

    let final_accepted_amendments: Vec<CanonicalAmendment> = amendments
        .iter()
        .filter(|amendment| final_accepted_ids.contains(&amendment.amendment_id))
        .cloned()
        .collect();

    let total_all_exhausted =
        probe_exhausted_count + proposal_total_exhausted + vote_total_exhausted;
    let effective_min = min_reviewers
        .min(reviewer_records.len().saturating_sub(vote_total_exhausted))
        .max(1);

    if !final_accepted_amendments.is_empty() && final_review_restart_count >= max_restarts {
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
                })
                .collect(),
            restart_required: false,
            force_completed: true,
        });
    }

    let aggregate = FinalReviewAggregatePayload {
        restart_required: !final_accepted_amendments.is_empty(),
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
        final_review_restart_count: if final_accepted_amendments.is_empty() {
            final_review_restart_count
        } else {
            final_review_restart_count.saturating_add(1)
        },
        max_restarts,
        summary: if final_accepted_amendments.is_empty() {
            "Final review accepted no amendments.".to_owned()
        } else {
            format!(
                "Final review accepted {} amendment(s); restart required.",
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
            })
            .collect(),
        restart_required: !aggregate.final_accepted_amendments.is_empty(),
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
    append_panel_member_runtime_log_with_identity(
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
    );
}

#[allow(clippy::too_many_arguments)]
fn append_panel_member_runtime_log_with_identity(
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
    }
}

fn build_reviewer_prompt(
    project_prompt: &str,
    backend_target: &ResolvedBackendTarget,
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
    template_catalog::resolve_and_render(
        "final_review_reviewer",
        base_dir,
        project_id,
        &[
            ("task_prompt_contract", task_prompt_contract_block.as_str()),
            ("project_prompt", project_prompt),
            ("json_schema", &schema_str),
        ],
    )
}

fn build_voter_prompt(
    title: &str,
    amendments: &[CanonicalAmendment],
    planner_votes: Option<&FinalReviewVotePayload>,
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
            format!(
                "## Amendment: {}\n\n{}",
                amendment.amendment_id, amendment.normalized_body
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let planner_section = planner_votes
        .map(|payload| {
            format!(
                "\n\n## Planner Positions\n\n```json\n{}\n```",
                serde_json::to_string_pretty(payload).unwrap_or_default()
            )
        })
        .unwrap_or_default();
    template_catalog::resolve_and_render(
        "final_review_voter",
        base_dir,
        project_id,
        &[
            ("title", title),
            ("amendments", &amendment_text),
            ("planner_positions", &planner_section),
            ("json_schema", &schema_str),
        ],
    )
}

fn build_arbiter_prompt(
    disputed_amendments: &HashMap<String, &CanonicalAmendment>,
    planner_votes: &FinalReviewVotePayload,
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
            format!(
                "## Amendment: {}\n\n{}",
                amendment.amendment_id, amendment.normalized_body
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let planner_str = serde_json::to_string_pretty(planner_votes)?;
    let reviewer_str = serde_json::to_string_pretty(reviewer_votes)?;
    template_catalog::resolve_and_render(
        "final_review_arbiter",
        base_dir,
        project_id,
        &[
            ("amendments", &amendment_text),
            ("planner_positions", &planner_str),
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
    prompt: String,
    context: Value,
    timeout: Duration,
    cancellation_token: CancellationToken,
) -> AppResult<(Value, RecordProducer)>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    let invocation_id =
        final_review_invocation_id(run_id, stage_id, cursor, panel_role, member_key);

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

fn final_review_invocation_id(
    run_id: &RunId,
    stage_id: StageId,
    cursor: &StageCursor,
    panel_role: &str,
    member_key: &str,
) -> String {
    format!(
        "{}-{}-{panel_role}-{member_key}-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    )
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
    use std::collections::{HashMap, HashSet};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use tempfile::tempdir;

    use crate::adapters::fs::{
        FileSystem, FsArtifactStore, FsJournalStore, FsPayloadArtifactWriteStore, FsProjectStore,
        FsRawOutputStore, FsRuntimeLogStore, FsRuntimeLogWriteStore, FsSessionStore,
    };
    use crate::contexts::agent_execution::model::{
        InvocationEnvelope, InvocationMetadata, RawOutputReference, TokenCounts,
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

    #[derive(Clone, Default)]
    struct RecordingFinalReviewAdapter {
        requests: Arc<Mutex<Vec<(String, String)>>>,
        proposal_failures: Arc<HashSet<String>>,
        vote_failures: Arc<HashSet<String>>,
        /// Members whose proposal invocations fail with BackendExhausted.
        proposal_exhausted: Arc<HashSet<String>>,
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

        fn all_invocation_ids(&self) -> Vec<String> {
            self.requests
                .lock()
                .expect("recording adapter lock poisoned")
                .iter()
                .map(|(_, invocation_id)| invocation_id.clone())
                .collect()
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

        async fn check_availability(&self, _backend: &ResolvedBackendTarget) -> AppResult<()> {
            Ok(())
        }

        async fn invoke(&self, request: InvocationRequest) -> AppResult<InvocationEnvelope> {
            let contract_label = request.contract.label();
            self.requests
                .lock()
                .expect("recording adapter lock poisoned")
                .push((contract_label.clone(), request.invocation_id.clone()));

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
            None,
        )
        .expect("reviewer prompt");

        assert!(prompt.contains("## Task Prompt Contract"));
        assert!(prompt.contains("## Already Planned Elsewhere"));
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
            None,
        )
        .expect("reviewer prompt");

        assert!(prompt.starts_with("LEGACY REVIEWER"));
        assert!(prompt.contains("# Prompt"));
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
            planner: ResolvedBackendTarget::new(BackendFamily::Claude, "planner-model"),
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
            Duration::from_secs(1),
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
    async fn final_review_planner_prompt_failure_does_not_emit_vote_started_telemetry() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-planner-prompt-failure");
        write_workspace_template_override(base_dir, "final_review_voter", "broken voter");
        let adapter = RecordingFinalReviewAdapter::default();
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-final-review-planner-prompt-failure").expect("run id");
        let cursor = StageCursor::new(StageId::FinalReview, 1, 1, 1).expect("cursor");
        let mut seq = FsJournalStore
            .read_journal(base_dir, &project_id)
            .expect("journal")
            .len() as u64;
        let panel = FinalReviewPanelResolution {
            planner: ResolvedBackendTarget::new(BackendFamily::Claude, "planner-model"),
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
            Duration::from_secs(1),
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("planner prompt failure should abort final review");

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
        assert!(runtime_logs.iter().all(|entry| {
            !(entry.message.contains("phase=vote") && entry.message.contains("reviewer_id=planner"))
        }));
    }

    #[tokio::test]
    async fn final_review_reviewer_vote_prompt_failure_does_not_emit_member_started_telemetry() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let project_id = setup_project(base_dir, "fr-reviewer-vote-prompt-failure");
        let adapter = RecordingFinalReviewAdapter::with_template_override_after_invocation(
            "voter-planner",
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
            planner: ResolvedBackendTarget::new(BackendFamily::Claude, "planner-model"),
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
            Duration::from_secs(1),
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
            1,
            "planner vote should succeed before reviewer vote prompt construction fails"
        );

        let events = FsJournalStore.read_journal(base_dir, &project_id).unwrap();
        assert!(!events.iter().any(|event| {
            matches!(
                event.event_type,
                JournalEventType::ReviewerStarted | JournalEventType::ReviewerCompleted
            ) && event.details["phase"] == "vote"
                && event.details["reviewer_id"] == "reviewer-1"
        }));

        let runtime_logs = FsRuntimeLogStore
            .read_runtime_logs(base_dir, &project_id)
            .expect("runtime logs");
        assert!(runtime_logs.iter().all(|entry| {
            !(entry.message.contains("phase=vote")
                && entry.message.contains("reviewer_id=reviewer-1"))
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
            planner: ResolvedBackendTarget::new(BackendFamily::Claude, "planner-model"),
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
            Duration::from_secs(1),
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
            planner: ResolvedBackendTarget::new(BackendFamily::Claude, "planner-model"),
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
            Duration::from_secs(1),
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .expect("final review should succeed");

        assert!(result.restart_required);
        assert!(!result.final_accepted_amendments.is_empty());

        let vote_ids = adapter.invocation_ids_for("final_review:voter");
        assert_eq!(vote_ids.len(), 3, "planner plus two successful reviewers");
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
            reviewer_started.len() >= 5,
            "expected proposal, planner, and reviewer vote starts"
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
            planner: ResolvedBackendTarget::new(BackendFamily::Claude, "planner-model"),
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
            Duration::from_secs(1),
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
            planner: ResolvedBackendTarget::new(BackendFamily::Claude, "planner-model"),
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
            Duration::from_secs(1),
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
            planner: ResolvedBackendTarget::new(BackendFamily::Claude, "planner-model"),
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
            Duration::from_secs(1),
            &|_| Duration::from_secs(1),
            Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .expect("final review should tolerate optional vote failures");

        assert!(result.restart_required);
        assert_eq!(result.aggregate_payload["total_reviewers"], json!(2));
        assert_eq!(
            adapter.invocation_ids_for("final_review:voter").len(),
            4,
            "planner plus three reviewer vote attempts should be recorded"
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
            planner: ResolvedBackendTarget::new(BackendFamily::Claude, "planner-model"),
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
            Duration::from_secs(1),
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
            supporting_final_review_artifacts.len() >= 5,
            "expected reviewer proposals, planner positions, and vote artifacts"
        );
        assert!(
            supporting_final_review_artifacts.iter().all(|record| matches!(
                &record.producer,
                Some(RecordProducer::Agent {
                    backend_family,
                    model_id,
                    ..
                }) if !backend_family.is_empty() && !model_id.is_empty()
            )),
            "final-review supporting artifacts must persist agent producer metadata: {supporting_final_review_artifacts:?}"
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
            planner: ResolvedBackendTarget::new(BackendFamily::Claude, "planner-model"),
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
            Duration::from_secs(1),
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
            planner: ResolvedBackendTarget::new(BackendFamily::Claude, "planner-model"),
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
            Duration::from_secs(1),
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
