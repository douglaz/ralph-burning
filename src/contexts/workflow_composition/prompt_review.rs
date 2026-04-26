#![forbid(unsafe_code)]

//! Prompt-review refiner-plus-validator orchestration.
//!
//! Implements the prompt-review panel workflow:
//! 1. Run the refiner to produce a refined prompt.
//! 2. Persist the refinement result as `StageSupporting`.
//! 3. Run validators against the refined prompt.
//! 4. Persist each validator result as `StageSupporting`.
//! 5. If any validator rejects, collect concerns and feed them back to the
//!    refiner for revision (up to `max_refinement_retries` times).
//! 6. If executed validators < `min_reviewers`, prompt review fails.
//! 7. On success: persist `prompt.original.md`, replace `prompt.md`,
//!    recompute prompt hash, and persist one canonical `StagePrimary` record.

use std::path::Path;

use chrono::Utc;
use serde_json::Value;

use std::time::Duration;

use crate::adapters::process_backend::processed_contract_schema_value;
use crate::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use crate::contexts::agent_execution::policy::PromptReviewPanelResolution;
use crate::contexts::agent_execution::service::AgentExecutionPort;
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::AgentExecutionService;
use crate::contexts::project_run_record::model::{ArtifactRecord, PayloadRecord};
use crate::contexts::project_run_record::service::{PayloadArtifactWritePort, RuntimeLogWritePort};
use crate::contexts::project_run_record::task_prompt_contract;
use crate::contexts::workflow_composition::panel_contracts::{
    PromptRefinementPayload, PromptReviewDecision, PromptReviewPrimaryPayload,
    PromptValidationPayload, RecordKind, RecordProducer,
};
use crate::contexts::workflow_composition::renderers;
use crate::contexts::workspace_governance::template_catalog;
use crate::shared::domain::{
    BackendFamily, BackendRole, FailureClass, ProjectId, ResolvedBackendTarget, RunId,
    SessionPolicy, StageCursor, StageId,
};
use crate::shared::error::{AppError, AppResult};

/// Result of a successful prompt review execution.
pub struct PromptReviewResult {
    /// The primary payload for journal persistence.
    pub primary_payload: Value,
    /// The primary artifact text.
    pub primary_artifact: String,
    /// The original prompt text (before refinement) for writing prompt.original.md.
    pub original_prompt: String,
    /// The refined prompt text for replacing prompt.md.
    pub refined_prompt: String,
}

/// Execute the prompt-review panel workflow.
///
/// Returns `Ok(PromptReviewResult)` on success (all validators accept, min_reviewers met).
/// Returns `Err` on failure. Supporting records are already persisted regardless of outcome.
///
/// When validators reject the refined prompt and `max_refinement_retries > 0`, the
/// rejection concerns are fed back to the refiner for revision. This retry loop
/// repeats up to `max_refinement_retries` times before failing.
#[allow(clippy::too_many_arguments)]
pub async fn execute_prompt_review<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    artifact_write: &dyn PayloadArtifactWritePort,
    _log_write: &dyn RuntimeLogWritePort,
    base_dir: &Path,
    project_root: &Path,
    backend_working_dir: &Path,
    project_id: &ProjectId,
    run_id: &RunId,
    cursor: &StageCursor,
    panel: &PromptReviewPanelResolution,
    min_reviewers: usize,
    max_refinement_retries: u32,
    prompt_reference: &str,
    rollback_count: u32,
    refiner_timeout: Duration,
    timeout_for_backend: &dyn Fn(BackendFamily) -> Duration,
    cancellation_token: CancellationToken,
) -> AppResult<PromptReviewResult>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    let stage_id = StageId::PromptReview;
    let prompt_path = project_root.join(prompt_reference);
    let original_prompt =
        std::fs::read_to_string(&prompt_path).map_err(|e| AppError::CorruptRecord {
            file: prompt_path.display().to_string(),
            details: format!("failed to read prompt for review: {e}"),
        })?;

    let refiner_target = &panel.refiner;
    let mut prior_concerns: Option<String> = None;

    // The outer loop covers the initial attempt (round 0) plus up to
    // max_refinement_retries additional rounds. On each round the refiner
    // receives any accumulated validator concerns from the previous round.
    let total_rounds = 1 + max_refinement_retries;
    for round in 0..total_rounds {
        let retry_suffix = if round == 0 {
            String::new()
        } else {
            format!("-retry{round}")
        };

        // ── Step 1: Run the refiner ────────────────────────────────────────
        // Always supply prior_concerns (empty string on round 0) so templates
        // that require the placeholder can render without error.
        let concerns_str = prior_concerns.as_deref().unwrap_or("");
        let extra_values: Vec<(&str, &str)> = vec![("prior_concerns", concerns_str)];
        let (refinement_payload, refiner_producer) = invoke_panel_member(
            agent_service,
            base_dir,
            project_root,
            backend_working_dir,
            run_id,
            stage_id,
            cursor,
            refiner_target,
            &original_prompt,
            &original_prompt,
            "refiner",
            refiner_timeout,
            cancellation_token.clone(),
            Some(project_id),
            &extra_values,
            &retry_suffix,
        )
        .await?;

        let refinement: PromptRefinementPayload =
            serde_json::from_value(refinement_payload.clone()).map_err(|e| {
                AppError::InvocationFailed {
                    backend: refiner_target.backend.family.to_string(),
                    contract_id: "prompt_review:refiner".to_owned(),
                    failure_class: crate::shared::domain::FailureClass::SchemaValidationFailure,
                    details: format!("refiner output schema validation failed: {e}"),
                }
            })?;

        // ── Step 2: Persist refinement as StageSupporting ──────────────────
        let refiner_artifact = renderers::render_prompt_refinement(
            stage_id,
            &refinement,
            &refiner_producer.to_string(),
        );
        persist_supporting_record(
            artifact_write,
            base_dir,
            project_id,
            run_id,
            cursor,
            stage_id,
            rollback_count,
            &refinement_payload,
            &refiner_artifact,
            refiner_producer,
            &format!("refinement{retry_suffix}"),
        )?;

        // ── Step 3: Invoke validators ──────────────────────────────────────
        // Unlike completion/final-review, prompt-review does NOT degrade on
        // BackendExhausted — any validator error aborts the stage.
        let mut executed_count = 0usize;
        let mut accept_count = 0usize;
        let mut reject_count = 0usize;
        let mut collected_concerns: Vec<String> = Vec::new();

        for (i, member) in panel.validators.iter().enumerate() {
            let validator_target = &member.target;
            let validator_timeout = timeout_for_backend(validator_target.backend.family);
            let invocation_result = invoke_panel_member(
                agent_service,
                base_dir,
                project_root,
                backend_working_dir,
                run_id,
                stage_id,
                cursor,
                validator_target,
                &refinement.refined_prompt,
                &original_prompt,
                "validator",
                validator_timeout,
                cancellation_token.clone(),
                Some(project_id),
                &[],
                &retry_suffix,
            )
            .await;

            let (validation_payload, validator_producer) = match invocation_result {
                Ok(result) => result,
                Err(error) => return Err(error),
            };

            let validation: PromptValidationPayload =
                serde_json::from_value(validation_payload.clone()).map_err(|e| {
                    AppError::InvocationFailed {
                        backend: validator_target.backend.family.to_string(),
                        contract_id: "prompt_review:validator".to_owned(),
                        failure_class: FailureClass::SchemaValidationFailure,
                        details: format!("validator output schema validation failed: {e}"),
                    }
                })?;

            let validator_artifact = renderers::render_prompt_validation(
                stage_id,
                &validation,
                &validator_producer.to_string(),
            );
            persist_supporting_record(
                artifact_write,
                base_dir,
                project_id,
                run_id,
                cursor,
                stage_id,
                rollback_count,
                &validation_payload,
                &validator_artifact,
                validator_producer,
                &format!("validator-{i}{retry_suffix}"),
            )?;

            executed_count += 1;
            if validation.accepted {
                accept_count += 1;
            } else {
                reject_count += 1;
                collected_concerns.extend(validation.concerns);
            }
        }

        let contract_drift_concerns =
            canonical_contract_drift_concerns(&original_prompt, &refinement.refined_prompt);
        if !contract_drift_concerns.is_empty() {
            collected_concerns.extend(contract_drift_concerns);
        }

        // ── Step 4: Enforce min_reviewers ──────────────────────────────────
        if executed_count < min_reviewers {
            return Err(AppError::InsufficientPanelMembers {
                panel: "prompt_review".to_owned(),
                resolved: executed_count,
                minimum: min_reviewers,
            });
        }

        // ── Step 5: Check acceptance ───────────────────────────────────────
        if reject_count == 0 && collected_concerns.is_empty() {
            // Success: all validators accepted.
            let refined = refinement.refined_prompt.clone();
            let primary = PromptReviewPrimaryPayload {
                decision: PromptReviewDecision::Accepted,
                refined_prompt: refinement.refined_prompt,
                executed_reviewers: executed_count,
                accept_count,
                reject_count,
                refinement_summary: refinement.refinement_summary,
            };

            let primary_payload = serde_json::to_value(&primary)?;
            let primary_artifact = renderers::render_prompt_review_decision(stage_id, &primary);

            return Ok(PromptReviewResult {
                primary_payload,
                primary_artifact,
                original_prompt,
                refined_prompt: refined,
            });
        }

        // Validators rejected. If retries remain, feed concerns back to the
        // refiner; otherwise fall through to the final rejection error.
        let is_last_round = round + 1 >= total_rounds;
        if is_last_round {
            let summary = if reject_count == 0 {
                format!(
                    "the refined prompt still violated the canonical bead task prompt contract \
                     after {} refinement retries",
                    max_refinement_retries
                )
            } else {
                format!(
                    "{reject_count} of {executed_count} validators rejected the refined prompt \
                     (after {} refinement retries)",
                    max_refinement_retries
                )
            };
            let mut detail_parts = vec![summary];
            if !collected_concerns.is_empty() {
                detail_parts.push(format!(
                    "remaining concerns: {}",
                    collected_concerns.join("; ")
                ));
            }
            return Err(AppError::PromptReviewRejected {
                details: detail_parts.join("; "),
            });
        }

        // Format concerns for the next refiner invocation.
        prior_concerns = Some(format_prior_concerns(&collected_concerns));
    }

    // Unreachable: the loop always returns from within.
    unreachable!("refinement retry loop must return")
}

/// Format collected validator concerns into a section for the refiner template.
fn format_prior_concerns(concerns: &[String]) -> String {
    let mut section =
        String::from("## Prior Validator Concerns\n\nThe previous refinement was rejected. Address these concerns:\n\n");
    for (i, concern) in concerns.iter().enumerate() {
        section.push_str(&format!("{}. {}\n", i + 1, concern));
    }
    section
}

fn canonical_contract_drift_concerns(original_prompt: &str, refined_prompt: &str) -> Vec<String> {
    if !task_prompt_contract::prompt_uses_contract(original_prompt) {
        return Vec::new();
    }

    let original_uses_current_contract =
        task_prompt_contract::validate_current_canonical_prompt_shape(original_prompt).is_ok();
    let original_is_legacy_v1_contract = !original_uses_current_contract
        && task_prompt_contract::validate_canonical_prompt_shape(original_prompt).is_ok();
    let validation_result = if original_is_legacy_v1_contract {
        match task_prompt_contract::validate_canonical_prompt_shape(refined_prompt) {
            Ok(()) if task_prompt_contract::validate_current_canonical_prompt_shape(refined_prompt)
                .is_ok() =>
            {
                Err(vec![
                    "legacy v1 prompt refinements must not add `## Nearby work`; that section is graph-derived builder output".to_owned(),
                ])
            }
            other => other,
        }
    } else {
        task_prompt_contract::validate_current_canonical_prompt_shape(refined_prompt)
    };

    let validation_passed = validation_result.is_ok();
    let mut concerns = match validation_result {
        Ok(()) => Vec::new(),
        Err(errors) => vec![format!(
            "Preserve the canonical bead task prompt contract exactly: {}",
            errors.join("; ")
        )],
    };

    let original_nearby_work = nearby_work_section_body(original_prompt);
    let refined_nearby_work = nearby_work_section_body(refined_prompt);
    if validation_passed {
        if original_nearby_work.is_some() && original_nearby_work != refined_nearby_work {
            concerns.push(
                "Preserve graph-derived `## Nearby work` verbatim; prompt review must not add, remove, or rewrite nearby bead IDs".to_owned(),
            );
        } else if original_nearby_work.is_none() && refined_nearby_work.is_some() {
            concerns.push(
                "Do not add graph-derived `## Nearby work` when the original contract-marked prompt did not contain that section".to_owned(),
            );
        }
    }

    concerns
}

fn nearby_work_section_body(prompt: &str) -> Option<&str> {
    canonical_section_body(prompt, task_prompt_contract::SECTION_NEARBY_WORK)
}

fn canonical_section_body<'a>(prompt: &'a str, section_title: &str) -> Option<&'a str> {
    let section_header = format!("## {section_title}");
    let mut active_fence = None;
    let mut offset = 0usize;
    let mut body_start = None;

    for segment in prompt.split_inclusive('\n') {
        let line = segment.trim_end_matches('\n').trim_end_matches('\r');
        let heading_line = line.trim_end();

        if let Some(opening) = active_fence {
            if crate::contexts::project_run_record::fence_util::closes_fence(line, opening) {
                active_fence = None;
            }
            offset += segment.len();
            continue;
        }

        if let Some(opening) =
            crate::contexts::project_run_record::fence_util::opening_fence_delimiter(line)
        {
            active_fence = Some(opening);
            offset += segment.len();
            continue;
        }

        if body_start.is_some() && heading_line.starts_with("## ") {
            return body_start.map(|start| &prompt[start..offset]);
        }

        if heading_line == section_header {
            body_start = Some(offset + segment.len());
        }

        offset += segment.len();
    }

    if offset < prompt.len() {
        let segment = &prompt[offset..];
        let line = segment.trim_end_matches('\r');
        let heading_line = line.trim_end();
        if body_start.is_some() && heading_line.starts_with("## ") {
            return body_start.map(|start| &prompt[start..offset]);
        }
        if heading_line == section_header {
            body_start = Some(prompt.len());
        }
    }

    body_start.map(|start| &prompt[start..])
}

fn build_prompt_review_member_prompt(
    base_dir: &Path,
    project_id: Option<&ProjectId>,
    prompt_text: &str,
    contract_reference_prompt: &str,
    role_label: &str,
    schema_str: &str,
    extra_values: &[(&str, &str)],
) -> AppResult<String> {
    let template_id = if role_label == "refiner" {
        "prompt_review_refiner"
    } else {
        "prompt_review_validator"
    };
    let task_prompt_contract_block =
        task_prompt_contract::prompt_review_consumer_guidance_for_prompt(contract_reference_prompt);

    let mut values: Vec<(&str, &str)> = vec![
        ("role_label", role_label),
        ("prompt_text", prompt_text),
        ("task_prompt_contract", task_prompt_contract_block.as_str()),
        ("json_schema", schema_str),
    ];
    values.extend_from_slice(extra_values);

    template_catalog::resolve_and_render(template_id, base_dir, project_id, &values)
}

/// Invoke a single panel member (refiner or validator) and return the raw parsed payload.
#[allow(clippy::too_many_arguments)]
async fn invoke_panel_member<A, R, S>(
    agent_service: &AgentExecutionService<A, R, S>,
    base_dir: &Path,
    project_root: &Path,
    backend_working_dir: &Path,
    run_id: &RunId,
    stage_id: StageId,
    cursor: &StageCursor,
    target: &ResolvedBackendTarget,
    prompt_text: &str,
    contract_reference_prompt: &str,
    role_label: &str,
    timeout: Duration,
    cancellation_token: CancellationToken,
    project_id: Option<&ProjectId>,
    extra_values: &[(&str, &str)],
    retry_suffix: &str,
) -> AppResult<(Value, RecordProducer)>
where
    A: AgentExecutionPort,
    R: crate::contexts::agent_execution::service::RawOutputPort,
    S: SessionStorePort,
{
    let invocation_id = format!(
        "{}-{}-{role_label}-c{}-a{}-cr{}{retry_suffix}",
        run_id.as_str(),
        stage_id.as_str(),
        cursor.cycle,
        cursor.attempt,
        cursor.completion_round
    );

    let schema_str = serde_json::to_string_pretty(&processed_contract_schema_value(
        &InvocationContract::Panel {
            stage_id,
            role: role_label.to_owned(),
        },
        target.backend.family,
    ))?;
    let prompt = build_prompt_review_member_prompt(
        base_dir,
        project_id,
        prompt_text,
        contract_reference_prompt,
        role_label,
        &schema_str,
        extra_values,
    )?;

    let request = InvocationRequest {
        invocation_id,
        project_root: project_root.to_path_buf(),
        working_dir: backend_working_dir.to_path_buf(),
        contract: InvocationContract::Panel {
            stage_id,
            role: role_label.to_owned(),
        },
        role: BackendRole::Planner,
        resolved_target: target.clone(),
        payload: InvocationPayload {
            prompt,
            context: Value::Null,
        },
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

/// Persist a supporting record (refinement or validation result).
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
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    const CANONICAL_PROMPT: &str = "<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH";
    const CURRENT_CANONICAL_PROMPT: &str = "<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->\n# Ralph Task Prompt\n\n## Milestone Summary\n\n- Milestone ID: ms-alpha\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\n### Related work\n- `ms-alpha.real` [open] Real nearby owner\n  Scope: Graph-derived context.\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH";

    #[test]
    fn build_prompt_review_member_prompt_surfaces_contract_guidance() {
        let tmp = tempdir().expect("tempdir");
        let prompt = build_prompt_review_member_prompt(
            tmp.path(),
            None,
            CANONICAL_PROMPT,
            CANONICAL_PROMPT,
            "refiner",
            "{}",
            &[],
        )
        .expect("render prompt");

        assert!(prompt.contains("## Task Prompt Contract"));
        assert!(prompt.contains("preserve the exact contract marker line"));
    }

    #[test]
    fn build_prompt_review_member_prompt_requires_nearby_work_verbatim_for_current_prompt() {
        let tmp = tempdir().expect("tempdir");
        let prompt = build_prompt_review_member_prompt(
            tmp.path(),
            None,
            CURRENT_CANONICAL_PROMPT,
            CURRENT_CANONICAL_PROMPT,
            "refiner",
            "{}",
            &[],
        )
        .expect("render prompt");

        assert!(prompt.contains("Preserve the graph-derived `Nearby work` body verbatim"));
    }

    #[test]
    fn build_prompt_review_member_prompt_omits_contract_guidance_for_generic_prompt() {
        let tmp = tempdir().expect("tempdir");
        let prompt = build_prompt_review_member_prompt(
            tmp.path(),
            None,
            "# Prompt\n\nGeneric.",
            "# Prompt\n\nGeneric.",
            "validator",
            "{}",
            &[],
        )
        .expect("render prompt");

        assert!(!prompt.contains("## Task Prompt Contract"));
    }

    #[test]
    fn build_prompt_review_member_prompt_keeps_contract_guidance_for_refined_prompt_without_marker()
    {
        let tmp = tempdir().expect("tempdir");
        let prompt = build_prompt_review_member_prompt(
            tmp.path(),
            None,
            "# Refined Prompt\n\nDropped the canonical marker by mistake.",
            CANONICAL_PROMPT,
            "validator",
            "{}",
            &[],
        )
        .expect("render prompt");

        assert!(prompt.contains("## Task Prompt Contract"));
        assert!(prompt.contains("preserve the exact contract marker line"));
        assert!(prompt.contains("# Refined Prompt"));
    }

    #[test]
    fn build_prompt_review_member_prompt_allows_legacy_override_without_task_prompt_contract_placeholder(
    ) {
        let tmp = tempdir().expect("tempdir");
        let template_dir = tmp.path().join(".ralph-burning").join("templates");
        fs::create_dir_all(&template_dir).expect("template dir");
        fs::write(
            template_dir.join("prompt_review_validator.md"),
            "LEGACY VALIDATOR\n\n{{role_label}}\n\n{{prompt_text}}\n\n{{json_schema}}",
        )
        .expect("template override");

        let prompt = build_prompt_review_member_prompt(
            tmp.path(),
            None,
            "# Prompt\n\nGeneric.",
            "# Prompt\n\nGeneric.",
            "validator",
            "{}",
            &[],
        )
        .expect("render prompt");

        assert!(prompt.starts_with("LEGACY VALIDATOR"));
        assert!(prompt.contains("# Prompt"));
    }

    #[test]
    fn canonical_contract_drift_becomes_retryable_concern() {
        let concerns = canonical_contract_drift_concerns(
            CANONICAL_PROMPT,
            "# Refined Prompt\n\nMissing the canonical sections.",
        );

        assert_eq!(concerns.len(), 1);
        assert!(concerns[0].contains("Preserve the canonical bead task prompt contract exactly"));
        assert!(concerns[0].contains("missing exact contract marker"));
    }

    #[test]
    fn canonical_contract_drift_still_applies_when_original_prompt_keeps_only_marker() {
        let concerns = canonical_contract_drift_concerns(
            "# Drifted Prompt\n\n<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->\n\n## Acceptance Criteria\n\nLater section only.",
            "# Refined Prompt\n\nNo canonical marker here.",
        );

        assert_eq!(concerns.len(), 1);
        assert!(concerns[0].contains("Preserve the canonical bead task prompt contract exactly"));
        assert!(concerns[0].contains("missing exact contract marker"));
    }

    #[test]
    fn canonical_contract_drift_rejects_malformed_contract_refinement_that_adds_nearby_work() {
        let concerns = canonical_contract_drift_concerns(
            "# Drifted Prompt\n\n<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->\n\n## Acceptance Criteria\n\nLater section only.",
            CURRENT_CANONICAL_PROMPT,
        );

        assert_eq!(concerns.len(), 1);
        assert!(concerns[0].contains("Do not add graph-derived `## Nearby work`"));
    }

    #[test]
    fn canonical_contract_drift_allows_legacy_v1_refinement_without_nearby_work() {
        let concerns = canonical_contract_drift_concerns(CANONICAL_PROMPT, CANONICAL_PROMPT);

        assert!(concerns.is_empty());
    }

    #[test]
    fn canonical_contract_drift_rejects_legacy_v1_refinement_that_adds_nearby_work() {
        let concerns = canonical_contract_drift_concerns(
            CANONICAL_PROMPT,
            "<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\n### Related work\n- `ms-alpha.fake` [open] Fabricated nearby owner\n  Scope: Not graph-derived.\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
        );

        assert_eq!(concerns.len(), 1);
        assert!(concerns[0].contains("must not add `## Nearby work`"));
    }

    #[test]
    fn canonical_contract_drift_flags_misplaced_top_level_marker() {
        let concerns = canonical_contract_drift_concerns(
            CANONICAL_PROMPT,
            "# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH\n\n<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->",
        );

        assert_eq!(concerns.len(), 1);
        assert!(concerns[0].contains("must appear before the canonical section block"));
    }

    #[test]
    fn canonical_contract_drift_flags_missing_nearby_work_in_refined_prompt() {
        let concerns = canonical_contract_drift_concerns(
            "<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Nearby work\n\nN\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH",
            CANONICAL_PROMPT,
        );

        assert_eq!(concerns.len(), 1);
        assert!(concerns[0].contains("missing section heading `## Nearby work`"));
    }

    #[test]
    fn canonical_contract_drift_flags_rewritten_nearby_work_body() {
        let refined = CURRENT_CANONICAL_PROMPT.replace("ms-alpha.real", "ms-alpha.fake");

        let concerns = canonical_contract_drift_concerns(CURRENT_CANONICAL_PROMPT, &refined);

        assert_eq!(concerns.len(), 1);
        assert!(concerns[0].contains("Preserve graph-derived `## Nearby work` verbatim"));
    }

    #[test]
    fn canonical_contract_drift_flags_rewritten_nearby_work_with_trailing_heading_space() {
        let original = CURRENT_CANONICAL_PROMPT.replace("## Nearby work", "## Nearby work ");
        let refined = original.replace("ms-alpha.real", "ms-alpha.fake");

        let concerns = canonical_contract_drift_concerns(&original, &refined);

        assert_eq!(concerns.len(), 1);
        assert!(concerns[0].contains("Preserve graph-derived `## Nearby work` verbatim"));
    }

    #[test]
    fn canonical_contract_drift_flags_extra_canonical_heading_after_agents_guidance() {
        let concerns = canonical_contract_drift_concerns(
            CANONICAL_PROMPT,
            "<!-- ralph-task-prompt-contract: bead_execution_prompt/1 -->\n# Ralph Task Prompt\n\n## Milestone Summary\n\nA\n\n## Current Bead Details\n\nB\n\n## Must-Do Scope\n\nC\n\n## Explicit Non-Goals\n\nD\n\n## Acceptance Criteria\n\nE\n\n## Already Planned Elsewhere\n\nF\n\n## Review Policy\n\nG\n\n## AGENTS / Repo Guidance\n\nH\n\n## Acceptance Criteria\n\nduplicate drift",
        );

        assert_eq!(concerns.len(), 1);
        assert!(concerns[0].contains("unexpected extra canonical heading `## Acceptance Criteria`"));
    }

    // ── Panel degradation tests ──────────────────────────────────────────

    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use serde_json::json;

    use crate::adapters::fs::{
        FileSystem, FsJournalStore, FsPayloadArtifactWriteStore, FsProjectStore, FsRawOutputStore,
        FsRuntimeLogWriteStore, FsSessionStore,
    };
    use crate::contexts::agent_execution::model::{
        InvocationEnvelope, InvocationMetadata, RawOutputReference, TokenCounts,
    };
    use crate::contexts::agent_execution::policy::ResolvedPanelMember;
    use crate::contexts::agent_execution::service::AgentExecutionPort;
    use crate::contexts::agent_execution::AgentExecutionService;
    use crate::contexts::project_run_record::service::{self, CreateProjectInput};
    use crate::contexts::workspace_governance;
    use crate::shared::domain::{
        BackendFamily, FailureClass, FlowPreset, RunId, StageCursor, StageId,
    };
    use crate::shared::error::AppError;

    #[derive(Clone, Default)]
    struct RecordingPromptReviewAdapter {
        requests: Arc<Mutex<Vec<String>>>,
        /// Validator indices whose invocations fail with BackendExhausted.
        exhausted_validator_indices: Arc<HashSet<usize>>,
    }

    impl RecordingPromptReviewAdapter {
        fn with_exhausted_validators(indices: &[usize]) -> Self {
            Self {
                exhausted_validator_indices: Arc::new(indices.iter().copied().collect()),
                ..Default::default()
            }
        }
    }

    impl AgentExecutionPort for RecordingPromptReviewAdapter {
        async fn check_capability(
            &self,
            _backend: &ResolvedBackendTarget,
            _contract: &crate::contexts::agent_execution::model::InvocationContract,
        ) -> crate::shared::error::AppResult<()> {
            Ok(())
        }

        async fn check_availability(
            &self,
            _backend: &ResolvedBackendTarget,
        ) -> crate::shared::error::AppResult<()> {
            Ok(())
        }

        async fn invoke(
            &self,
            request: crate::contexts::agent_execution::model::InvocationRequest,
        ) -> crate::shared::error::AppResult<InvocationEnvelope> {
            self.requests
                .lock()
                .expect("lock")
                .push(request.invocation_id.clone());

            // Check for exhausted validators by matching on model name,
            // since prompt_review invocation IDs don't include the validator
            // index (unlike completion which uses completer-{i}).
            for &idx in self.exhausted_validator_indices.iter() {
                let model_fragment = format!("validator-{idx}");
                if request
                    .resolved_target
                    .model
                    .model_id
                    .contains(&model_fragment)
                {
                    return Err(AppError::InvocationFailed {
                        backend: request.resolved_target.backend.family.to_string(),
                        contract_id: "prompt_review:validator".to_owned(),
                        failure_class: FailureClass::BackendExhausted,
                        details: "validator backend exhausted".to_owned(),
                    });
                }
            }

            // Determine role from invocation_id.
            let payload = if request.invocation_id.contains("refiner") {
                json!({
                    "refined_prompt": "# Refined Prompt\n\nImproved.",
                    "refinement_summary": "Improved clarity.",
                    "improvements": ["clarity"]
                })
            } else {
                // Validator: accept the prompt.
                json!({
                    "accepted": true,
                    "evidence": ["looks good"],
                    "concerns": []
                })
            };

            Ok(InvocationEnvelope {
                raw_output_reference: RawOutputReference::Inline("{}".to_owned()),
                parsed_payload: payload,
                metadata: InvocationMetadata {
                    invocation_id: request.invocation_id,
                    duration: Duration::from_millis(1),
                    token_counts: TokenCounts::default(),
                    backend_used: request.resolved_target.backend.clone(),
                    model_used: request.resolved_target.model.clone(),
                    adapter_reported_backend: None,
                    adapter_reported_model: None,
                    attempt_number: request.attempt_number,
                    session_id: None,
                    session_reused: false,
                },
                timestamp: Utc::now(),
            })
        }

        async fn cancel(&self, _invocation_id: &str) -> crate::shared::error::AppResult<()> {
            Ok(())
        }
    }

    fn setup_prompt_review_project(
        base_dir: &Path,
        project_name: &str,
    ) -> (crate::shared::domain::ProjectId, PathBuf) {
        workspace_governance::initialize_workspace(base_dir, Utc::now()).expect("workspace init");
        let project_id = crate::shared::domain::ProjectId::new(project_name).expect("project id");
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
        let project_root = base_dir
            .join(".ralph-burning")
            .join("projects")
            .join(project_id.as_str());
        (project_id, project_root)
    }

    #[tokio::test]
    async fn prompt_review_fails_when_one_validator_exhausted() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let (project_id, project_root) = setup_prompt_review_project(base_dir, "pr-one-exhausted");
        let adapter = RecordingPromptReviewAdapter::with_exhausted_validators(&[0]);
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-pr-one-exhausted").expect("run id");
        let cursor = StageCursor::new(StageId::PromptReview, 1, 1, 1).expect("cursor");

        let refiner_target = ResolvedBackendTarget::new(BackendFamily::Claude, "refiner-model");
        let panel = crate::contexts::agent_execution::policy::PromptReviewPanelResolution {
            refiner: refiner_target,
            validators: vec![
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Codex, "validator-0-model"),
                    required: true,
                    configured_index: 0,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "validator-1-model"),
                    required: true,
                    configured_index: 1,
                },
            ],
        };

        // Write prompt.md so the executor can read it.
        std::fs::write(project_root.join("prompt.md"), "# Prompt\n\nTest prompt.\n")
            .expect("write prompt");

        let result = execute_prompt_review(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            base_dir,
            &project_root,
            base_dir,
            &project_id,
            &run_id,
            &cursor,
            &panel,
            2, // min_reviewers
            0, // max_refinement_retries
            "prompt.md",
            0, // rollback_count
            Duration::from_secs(1),
            &|_| Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await;

        // Prompt-review does NOT degrade on BackendExhausted — the first
        // exhausted validator should immediately fail the stage.
        let error = result
            .err()
            .expect("prompt-review should fail when any validator is exhausted (no degradation)");
        assert!(
            error
                .failure_class()
                .is_some_and(|fc| fc == FailureClass::BackendExhausted),
            "error should carry BackendExhausted failure class: {error:?}"
        );
    }

    #[tokio::test]
    async fn prompt_review_fails_when_all_validators_exhausted() {
        let tmp = tempdir().expect("tempdir");
        let base_dir = tmp.path();
        let (project_id, project_root) = setup_prompt_review_project(base_dir, "pr-all-exhausted");
        let adapter = RecordingPromptReviewAdapter::with_exhausted_validators(&[0, 1]);
        let agent_service =
            AgentExecutionService::new(adapter.clone(), FsRawOutputStore, FsSessionStore);
        let run_id = RunId::new("run-pr-all-exhausted").expect("run id");
        let cursor = StageCursor::new(StageId::PromptReview, 1, 1, 1).expect("cursor");

        let refiner_target = ResolvedBackendTarget::new(BackendFamily::Claude, "refiner-model");
        let panel = crate::contexts::agent_execution::policy::PromptReviewPanelResolution {
            refiner: refiner_target,
            validators: vec![
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Codex, "validator-0-model"),
                    required: true,
                    configured_index: 0,
                },
                ResolvedPanelMember {
                    target: ResolvedBackendTarget::new(BackendFamily::Claude, "validator-1-model"),
                    required: true,
                    configured_index: 1,
                },
            ],
        };

        std::fs::write(project_root.join("prompt.md"), "# Prompt\n\nTest prompt.\n")
            .expect("write prompt");

        let error = execute_prompt_review(
            &agent_service,
            &FsPayloadArtifactWriteStore,
            &FsRuntimeLogWriteStore,
            base_dir,
            &project_root,
            base_dir,
            &project_id,
            &run_id,
            &cursor,
            &panel,
            2, // min_reviewers
            0, // max_refinement_retries
            "prompt.md",
            0, // rollback_count
            Duration::from_secs(1),
            &|_| Duration::from_secs(1),
            CancellationToken::new(),
        )
        .await
        .err()
        .expect("panel should fail when first validator is exhausted");

        // Prompt-review does not degrade — the first exhausted validator
        // immediately propagates BackendExhausted.
        assert!(
            error
                .failure_class()
                .is_some_and(|fc| fc == FailureClass::BackendExhausted),
            "error should carry BackendExhausted failure class: {error:?}"
        );
    }
}
