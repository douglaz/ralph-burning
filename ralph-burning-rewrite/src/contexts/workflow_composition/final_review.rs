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
use std::time::Duration;

use chrono::Utc;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use crate::contexts::agent_execution::policy::{
    FinalReviewPanelResolution,
};
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::AgentExecutionService;
use crate::contexts::project_run_record::model::{ArtifactRecord, PayloadRecord};
use crate::contexts::project_run_record::service::{PayloadArtifactWritePort, RuntimeLogWritePort};
use crate::contexts::workflow_composition::panel_contracts::{
    FinalReviewAggregatePayload, FinalReviewAmendmentSource, FinalReviewArbiterPayload,
    FinalReviewCanonicalAmendment, FinalReviewProposalPayload, FinalReviewVoteDecision,
    FinalReviewVotePayload, RecordKind, RecordProducer,
};
use crate::contexts::workflow_composition::renderers;
use crate::shared::domain::{
    BackendRole, ProjectId, ResolvedBackendTarget, RunId, SessionPolicy, StageCursor, StageId,
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
    pub final_accepted_amendments: Vec<CanonicalAmendment>,
    pub restart_required: bool,
    pub force_completed: bool,
}

struct ReviewerProposalRecord {
    reviewer_id: String,
    backend_family: String,
    model_id: String,
    payload: FinalReviewProposalPayload,
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
    _log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_root: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    cursor: &StageCursor,
    panel: &FinalReviewPanelResolution,
    planner_target: &ResolvedBackendTarget,
    min_reviewers: usize,
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

    if final_review_restart_count >= max_restarts {
        let aggregate = FinalReviewAggregatePayload {
            restart_required: false,
            force_completed: true,
            total_reviewers: panel.reviewers.len(),
            total_proposed_amendments: 0,
            unique_amendment_count: 0,
            accepted_amendment_ids: Vec::new(),
            rejected_amendment_ids: Vec::new(),
            disputed_amendment_ids: Vec::new(),
            amendments: Vec::new(),
            final_accepted_amendments: Vec::new(),
            final_review_restart_count,
            max_restarts,
            summary: format!(
                "Final-review restart cap reached ({final_review_restart_count}/{max_restarts}); force-completing."
            ),
        };
        let aggregate_payload = serde_json::to_value(&aggregate)?;
        let aggregate_artifact = renderers::render_final_review_aggregate(&aggregate);
        return Ok(FinalReviewResult {
            aggregate_payload,
            aggregate_artifact,
            final_accepted_amendments: Vec::new(),
            restart_required: false,
            force_completed: true,
        });
    }

    let mut reviewer_records = Vec::new();
    for (idx, member) in panel.reviewers.iter().enumerate() {
        let reviewer_payload = invoke_final_review_member(
            agent_service,
            project_root,
            run_id,
            stage_id,
            cursor,
            &member.target,
            BackendRole::Reviewer,
            "reviewer",
            build_reviewer_prompt(&project_prompt)?,
            Value::Null,
            reviewer_timeout_for_backend(member.target.backend.family),
            cancellation_token.clone(),
        )
        .await;

        let reviewer_payload = match reviewer_payload {
            Ok(payload) => payload,
            Err(error) if !member.required => {
                if reviewer_records.len() + panel.reviewers.len().saturating_sub(idx + 1)
                    < min_reviewers
                {
                    return Err(error);
                }
                continue;
            }
            Err(error) => return Err(error),
        };

        let proposal: FinalReviewProposalPayload =
            serde_json::from_value(reviewer_payload.clone()).map_err(|e| {
                AppError::InvocationFailed {
                    backend: member.target.backend.family.to_string(),
                    contract_id: "final_review:reviewer".to_owned(),
                    failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                    details: format!("final-review proposal schema validation failed: {e}"),
                }
            })?;

        let reviewer_id = format!(
            "reviewer-{}:{}",
            member.target.backend.family, member.target.model.model_id
        );
        let producer = RecordProducer::Agent {
            backend_family: member.target.backend.family.to_string(),
            model_id: member.target.model.model_id.clone(),
        };
        let artifact =
            renderers::render_final_review_proposal(&proposal, &producer.to_string());
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
            reviewer_id,
            backend_family: member.target.backend.family.to_string(),
            model_id: member.target.model.model_id.clone(),
            payload: proposal,
        });
    }

    if reviewer_records.len() < min_reviewers {
        return Err(AppError::InsufficientPanelMembers {
            panel: "final_review".to_owned(),
            resolved: reviewer_records.len(),
            minimum: min_reviewers,
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

    let planner_vote_payload = invoke_final_review_member(
        agent_service,
        project_root,
        run_id,
        stage_id,
        cursor,
        planner_target,
        BackendRole::Planner,
        "voter",
        build_voter_prompt("Planner Positions", &amendments, None)?,
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
    .await?;
    let planner_votes: FinalReviewVotePayload =
        serde_json::from_value(planner_vote_payload.clone()).map_err(|e| {
            AppError::InvocationFailed {
                backend: planner_target.backend.family.to_string(),
                contract_id: "final_review:voter".to_owned(),
                failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                details: format!("planner positions schema validation failed: {e}"),
            }
        })?;
    validate_vote_payload(&planner_votes, &amendments, planner_target)?;
    let planner_producer = RecordProducer::Agent {
        backend_family: planner_target.backend.family.to_string(),
        model_id: planner_target.model.model_id.clone(),
    };
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
    for (idx, member) in panel.reviewers.iter().enumerate() {
        let vote_payload = invoke_final_review_member(
            agent_service,
            project_root,
            run_id,
            stage_id,
            cursor,
            &member.target,
            BackendRole::Reviewer,
            "voter",
            build_voter_prompt("Final Review Votes", &amendments, Some(&planner_votes))?,
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
            reviewer_timeout_for_backend(member.target.backend.family),
            cancellation_token.clone(),
        )
        .await;

        let vote_payload = match vote_payload {
            Ok(payload) => payload,
            Err(error) if !member.required => continue,
            Err(error) => return Err(error),
        };

        let votes: FinalReviewVotePayload =
            serde_json::from_value(vote_payload.clone()).map_err(|e| {
                AppError::InvocationFailed {
                    backend: member.target.backend.family.to_string(),
                    contract_id: "final_review:voter".to_owned(),
                    failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                    details: format!("final-review vote schema validation failed: {e}"),
                }
            })?;
        validate_vote_payload(&votes, &amendments, &member.target)?;

        let producer = RecordProducer::Agent {
            backend_family: member.target.backend.family.to_string(),
            model_id: member.target.model.model_id.clone(),
        };
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
            &format!("vote-{idx}"),
        )?;

        reviewer_votes.push(votes);
    }

    if reviewer_votes.len() < min_reviewers {
        return Err(AppError::InsufficientPanelMembers {
            panel: "final_review_vote".to_owned(),
            resolved: reviewer_votes.len(),
            minimum: min_reviewers,
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

        let arbiter_payload = invoke_final_review_member(
            agent_service,
            project_root,
            run_id,
            stage_id,
            cursor,
            &panel.arbiter,
            BackendRole::Reviewer,
            "arbiter",
            build_arbiter_prompt(&disputed_set, &planner_votes, &reviewer_votes)?,
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
        .await?;

        let arbiter: FinalReviewArbiterPayload =
            serde_json::from_value(arbiter_payload.clone()).map_err(|e| {
                AppError::InvocationFailed {
                    backend: panel.arbiter.backend.family.to_string(),
                    contract_id: "final_review:arbiter".to_owned(),
                    failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                    details: format!("final-review arbiter schema validation failed: {e}"),
                }
            })?;
        validate_arbiter_payload(&arbiter, &disputed_ids, &panel.arbiter)?;
        let producer = RecordProducer::Agent {
            backend_family: panel.arbiter.backend.family.to_string(),
            model_id: panel.arbiter.model.model_id.clone(),
        };
        let artifact =
            renderers::render_final_review_arbiter(&arbiter, &producer.to_string());
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
    };
    let aggregate_payload = serde_json::to_value(&aggregate)?;
    let aggregate_artifact = renderers::render_final_review_aggregate(&aggregate);

    Ok(FinalReviewResult {
        aggregate_payload,
        aggregate_artifact,
        final_accepted_amendments,
        restart_required: !aggregate.final_accepted_amendments.is_empty(),
        force_completed: false,
    })
}

fn canonical_to_payload(amendment: &CanonicalAmendment) -> FinalReviewCanonicalAmendment {
    FinalReviewCanonicalAmendment {
        amendment_id: amendment.amendment_id.clone(),
        normalized_body: amendment.normalized_body.clone(),
        sources: amendment.sources.clone(),
    }
}

fn build_reviewer_prompt(project_prompt: &str) -> AppResult<String> {
    let schema = super::panel_contracts::panel_json_schema(StageId::FinalReview, "reviewer");
    Ok(format!(
        "# Final Review Proposals\n\n## Project Prompt\n\n{project_prompt}\n\n## JSON Schema\n\n```json\n{}\n```",
        serde_json::to_string_pretty(&schema)?
    ))
}

fn build_voter_prompt(
    title: &str,
    amendments: &[CanonicalAmendment],
    planner_votes: Option<&FinalReviewVotePayload>,
) -> AppResult<String> {
    let schema = super::panel_contracts::panel_json_schema(StageId::FinalReview, "voter");
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
    Ok(format!(
        "# {title}\n\n{amendment_text}{planner_section}\n\n## JSON Schema\n\n```json\n{}\n```",
        serde_json::to_string_pretty(&schema)?
    ))
}

fn build_arbiter_prompt(
    disputed_amendments: &HashMap<String, &CanonicalAmendment>,
    planner_votes: &FinalReviewVotePayload,
    reviewer_votes: &[FinalReviewVotePayload],
) -> AppResult<String> {
    let schema = super::panel_contracts::panel_json_schema(StageId::FinalReview, "arbiter");
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
    Ok(format!(
        "# Final Review Arbiter\n\n{amendment_text}\n\n## Planner Positions\n\n```json\n{}\n```\n\n## Reviewer Votes\n\n```json\n{}\n```\n\n## JSON Schema\n\n```json\n{}\n```",
        serde_json::to_string_pretty(planner_votes)?,
        serde_json::to_string_pretty(reviewer_votes)?,
        serde_json::to_string_pretty(&schema)?
    ))
}

fn validate_vote_payload(
    payload: &FinalReviewVotePayload,
    amendments: &[CanonicalAmendment],
    target: &ResolvedBackendTarget,
) -> AppResult<()> {
    let expected_ids: Vec<&str> = amendments.iter().map(|amendment| amendment.amendment_id.as_str()).collect();
    let actual_ids: Vec<&str> = payload.votes.iter().map(|vote| vote.amendment_id.as_str()).collect();
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
    run_id: &RunId,
    stage_id: StageId,
    cursor: &StageCursor,
    target: &ResolvedBackendTarget,
    role: BackendRole,
    panel_role: &str,
    prompt: String,
    context: Value,
    timeout: Duration,
    cancellation_token: CancellationToken,
) -> AppResult<Value>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    let invocation_id = format!(
        "{}-{}-{panel_role}-c{}-a{}-cr{}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );

    let request = InvocationRequest {
        invocation_id,
        project_root: project_root.to_path_buf(),
        working_dir: project_root.to_path_buf(),
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
    Ok(envelope.parsed_payload)
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
    use super::*;
    use crate::contexts::workflow_composition::panel_contracts::FinalReviewProposal;

    fn proposal_record(reviewer_id: &str, body: &str) -> ReviewerProposalRecord {
        ReviewerProposalRecord {
            reviewer_id: reviewer_id.to_owned(),
            backend_family: "claude".to_owned(),
            model_id: format!("model-{reviewer_id}"),
            payload: FinalReviewProposalPayload {
                summary: format!("proposal from {reviewer_id}"),
                amendments: vec![FinalReviewProposal {
                    body: body.to_owned(),
                    rationale: None,
                }],
            },
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
        assert_eq!(merged[0].amendment_id, canonical_amendment_id(1, "tighten wording"));
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
}
