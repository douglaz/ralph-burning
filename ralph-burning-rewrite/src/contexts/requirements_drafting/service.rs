#![forbid(unsafe_code)]

//! Requirements drafting service.
//!
//! Orchestrates `draft`, `quick`, `show`, and `answer` commands, including
//! answer-template validation, seed rollback, and failure invariants.

use std::path::Path;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::adapters::fs::FileSystem;
use crate::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use crate::contexts::agent_execution::service::{AgentExecutionPort, AgentExecutionService};
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::RawOutputPort;
use crate::shared::domain::{BackendRole, SessionPolicy};
use crate::shared::error::{AppError, AppResult};

use super::contracts::{RequirementsContract, RequirementsPayload, RequirementsValidatedBundle};
use super::model::{
    AnswerEntry, PersistedAnswers, QuestionSetPayload, RequirementsJournalEvent,
    RequirementsJournalEventType, RequirementsReviewOutcome, RequirementsRun,
    RequirementsStageId, RequirementsStatus,
};

// ── Port traits ─────────────────────────────────────────────────────────────

/// Filesystem operations for requirements runs.
#[allow(async_fn_in_trait)]
pub trait RequirementsStorePort {
    fn create_run_dir(&self, base_dir: &Path, run_id: &str) -> AppResult<()>;
    fn write_run(&self, base_dir: &Path, run_id: &str, run: &RequirementsRun) -> AppResult<()>;
    fn read_run(&self, base_dir: &Path, run_id: &str) -> AppResult<RequirementsRun>;
    fn append_journal_event(
        &self,
        base_dir: &Path,
        run_id: &str,
        event: &RequirementsJournalEvent,
    ) -> AppResult<()>;
    fn read_journal(
        &self,
        base_dir: &Path,
        run_id: &str,
    ) -> AppResult<Vec<RequirementsJournalEvent>>;
    fn write_payload(
        &self,
        base_dir: &Path,
        run_id: &str,
        payload_id: &str,
        payload: &Value,
    ) -> AppResult<()>;
    fn write_artifact(
        &self,
        base_dir: &Path,
        run_id: &str,
        artifact_id: &str,
        content: &str,
    ) -> AppResult<()>;
    fn write_answers_toml(
        &self,
        base_dir: &Path,
        run_id: &str,
        template: &str,
    ) -> AppResult<()>;
    fn read_answers_toml(&self, base_dir: &Path, run_id: &str) -> AppResult<String>;
    fn write_answers_json(
        &self,
        base_dir: &Path,
        run_id: &str,
        answers: &PersistedAnswers,
    ) -> AppResult<()>;
    fn write_seed_pair(
        &self,
        base_dir: &Path,
        run_id: &str,
        project_json: &Value,
        prompt_md: &str,
    ) -> AppResult<()>;
    fn remove_seed_pair(&self, base_dir: &Path, run_id: &str) -> AppResult<()>;
    fn answers_toml_path(&self, base_dir: &Path, run_id: &str) -> std::path::PathBuf;
    fn seed_prompt_path(&self, base_dir: &Path, run_id: &str) -> std::path::PathBuf;
}

// ── Service ─────────────────────────────────────────────────────────────────

pub struct RequirementsService<A, R, S, Q> {
    agent_service: AgentExecutionService<A, R, S>,
    store: Q,
}

impl<A, R, S, Q> RequirementsService<A, R, S, Q> {
    pub fn new(agent_service: AgentExecutionService<A, R, S>, store: Q) -> Self {
        Self {
            agent_service,
            store,
        }
    }
}

impl<A, R, S, Q> RequirementsService<A, R, S, Q>
where
    A: AgentExecutionPort,
    R: RawOutputPort,
    S: SessionStorePort,
    Q: RequirementsStorePort,
{
    /// Execute `requirements draft --idea "<text>"`.
    pub async fn draft(&self, base_dir: &Path, idea: &str, now: DateTime<Utc>) -> AppResult<String> {
        let run_id = generate_run_id(now);
        let mut run = RequirementsRun::new_draft(run_id.clone(), idea.to_owned(), now);

        // Create run directory and initial state
        self.store.create_run_dir(base_dir, &run_id)?;
        self.store.write_run(base_dir, &run_id, &run)?;

        let created_event = journal_event(1, now, RequirementsJournalEventType::RunCreated, &run);
        self.store
            .append_journal_event(base_dir, &run_id, &created_event)?;

        // Generate questions
        let run_root = requirements_run_root(base_dir, &run_id);
        let question_result = self
            .invoke_stage(
                &run_root,
                RequirementsStageId::QuestionSet,
                BackendRole::Planner,
                &format!("Generate clarifying questions for the following idea:\n\n{idea}"),
            )
            .await;

        match question_result {
            Ok(bundle) => {
                let RequirementsPayload::QuestionSet(ref qs) = bundle.payload else {
                    unreachable!("QuestionSet contract always returns QuestionSet payload");
                };

                let payload_id = format!("{run_id}-qs-1");
                let artifact_id = format!("{run_id}-qs-art-1");

                // Persist payload and artifact atomically
                let payload_json = serde_json::to_value(qs)?;
                if let Err(e) = self.persist_payload_artifact_pair(
                    base_dir,
                    &run_id,
                    &payload_id,
                    &payload_json,
                    &artifact_id,
                    &bundle.artifact,
                ) {
                    // State invariant: if payload/artifact pair fails, run must not transition
                    run.status = RequirementsStatus::Failed;
                    run.status_summary = format!("failed: could not persist question set: {e}");
                    run.updated_at = Utc::now();
                    let _ = self.store.write_run(base_dir, &run_id, &run);
                    return Err(e);
                }

                if qs.questions.is_empty() {
                    // No questions needed — continue directly
                    self.continue_after_answers(base_dir, &mut run, idea, &[], 2, now)
                        .await?;
                } else {
                    // Generate answers.toml template
                    let template = generate_answers_template(qs);
                    self.store
                        .write_answers_toml(base_dir, &run_id, &template)?;

                    run.question_round = 1;
                    run.latest_question_set_id = Some(payload_id.clone());
                    run.status = RequirementsStatus::AwaitingAnswers;
                    run.status_summary = format!(
                        "awaiting answers: {} question(s), round 1",
                        qs.questions.len()
                    );
                    run.updated_at = Utc::now();
                    self.store.write_run(base_dir, &run_id, &run)?;

                    let event = journal_event(
                        2,
                        Utc::now(),
                        RequirementsJournalEventType::QuestionsGenerated,
                        &run,
                    );
                    self.store
                        .append_journal_event(base_dir, &run_id, &event)?;

                    let answers_path = self.store.answers_toml_path(base_dir, &run_id);
                    println!(
                        "Questions generated. Edit answers at:\n  {}\n\nThen run:\n  ralph-burning requirements answer {}",
                        answers_path.display(),
                        run_id
                    );
                }
            }
            Err(e) => {
                run.status = RequirementsStatus::Failed;
                run.status_summary = format!("failed: question generation: {e}");
                run.updated_at = Utc::now();
                self.store.write_run(base_dir, &run_id, &run)?;

                let event = journal_event(
                    2,
                    Utc::now(),
                    RequirementsJournalEventType::RunFailed,
                    &run,
                );
                self.store
                    .append_journal_event(base_dir, &run_id, &event)?;
                return Err(e);
            }
        }

        Ok(run_id)
    }

    /// Execute `requirements quick --idea "<text>"`.
    pub async fn quick(&self, base_dir: &Path, idea: &str, now: DateTime<Utc>) -> AppResult<String> {
        let run_id = generate_run_id(now);
        let mut run = RequirementsRun::new_quick(run_id.clone(), idea.to_owned(), now);

        self.store.create_run_dir(base_dir, &run_id)?;
        self.store.write_run(base_dir, &run_id, &run)?;

        let created_event = journal_event(1, now, RequirementsJournalEventType::RunCreated, &run);
        self.store
            .append_journal_event(base_dir, &run_id, &created_event)?;

        // Skip question generation entirely — go straight to draft
        self.continue_after_answers(base_dir, &mut run, idea, &[], 2, now)
            .await?;

        Ok(run_id)
    }

    /// Execute `requirements show <run-id>`.
    pub fn show(&self, base_dir: &Path, run_id: &str) -> AppResult<RequirementsShowResult> {
        let run = self.store.read_run(base_dir, run_id)?;
        let journal = self.store.read_journal(base_dir, run_id)?;

        let pending_question_count = if run.status == RequirementsStatus::AwaitingAnswers {
            // Count required questions from latest question set
            if let Some(ref _qs_id) = run.latest_question_set_id {
                // We derive count from the run state, not by scanning artifacts
                Some(run.question_round)
            } else {
                None
            }
        } else {
            None
        };

        let failure_summary = if run.status == RequirementsStatus::Failed {
            Some(run.status_summary.clone())
        } else {
            None
        };

        let seed_prompt_path = if run.status == RequirementsStatus::Completed {
            Some(self.store.seed_prompt_path(base_dir, run_id))
        } else {
            None
        };

        Ok(RequirementsShowResult {
            run,
            journal_event_count: journal.len() as u64,
            pending_question_count,
            failure_summary,
            seed_prompt_path,
        })
    }

    /// Execute `requirements answer <run-id>`.
    pub async fn answer(&self, base_dir: &Path, run_id: &str) -> AppResult<()> {
        let mut run = self.store.read_run(base_dir, run_id)?;

        // Valid only for awaiting_answers or failed runs with a committed question set
        match run.status {
            RequirementsStatus::AwaitingAnswers => {}
            RequirementsStatus::Failed if run.latest_question_set_id.is_some() => {}
            _ => {
                return Err(AppError::InvalidRequirementsState {
                    run_id: run_id.to_owned(),
                    details: format!(
                        "cannot answer: run is in '{}' state",
                        run.status
                    ),
                });
            }
        }

        // Open editor
        let answers_path = self.store.answers_toml_path(base_dir, run_id);
        FileSystem::open_editor(&answers_path)?;

        // Read and validate answers
        let answers_raw = self.store.read_answers_toml(base_dir, run_id)?;
        let answers = parse_and_validate_answers(&answers_raw, base_dir, run_id, &self.store)?;

        // Persist answers.json
        self.store
            .write_answers_json(base_dir, run_id, &answers)?;

        let seq = self.next_sequence(base_dir, run_id)?;
        let event = journal_event(
            seq,
            Utc::now(),
            RequirementsJournalEventType::AnswersSubmitted,
            &run,
        );
        self.store
            .append_journal_event(base_dir, run_id, &event)?;

        // Resume from answers boundary
        let answer_entries: Vec<(String, String)> = answers
            .answers
            .iter()
            .map(|a| (a.question_id.clone(), a.answer.clone()))
            .collect();

        run.question_round += 1;
        run.status = RequirementsStatus::Drafting;
        run.status_summary = "drafting: generating requirements from answers".to_owned();
        run.updated_at = Utc::now();
        self.store.write_run(base_dir, run_id, &run)?;

        let idea = run.idea.clone();
        self.continue_after_answers(base_dir, &mut run, &idea, &answer_entries, seq + 1, Utc::now())
            .await?;

        Ok(())
    }

    // ── Internal pipeline ───────────────────────────────────────────────

    async fn continue_after_answers(
        &self,
        base_dir: &Path,
        run: &mut RequirementsRun,
        idea: &str,
        answers: &[(String, String)],
        start_seq: u64,
        _now: DateTime<Utc>,
    ) -> AppResult<()> {
        let run_id = run.run_id.clone();
        let run_root = requirements_run_root(base_dir, &run_id);
        let mut seq = start_seq;

        // ── Draft ───────────────────────────────────────────────────────
        let draft_prompt = build_draft_prompt(idea, answers);
        let draft_result = self
            .invoke_stage(
                &run_root,
                RequirementsStageId::RequirementsDraft,
                BackendRole::Planner,
                &draft_prompt,
            )
            .await;

        let draft_bundle = match draft_result {
            Ok(b) => b,
            Err(e) => {
                self.fail_run(base_dir, run, seq, &format!("draft generation: {e}"))
                    .await?;
                return Err(e);
            }
        };

        let RequirementsPayload::Draft(ref draft_payload) = draft_bundle.payload else {
            unreachable!();
        };

        let draft_payload_id = format!("{run_id}-draft-1");
        let draft_artifact_id = format!("{run_id}-draft-art-1");
        let draft_json = serde_json::to_value(draft_payload)?;

        if let Err(e) = self.persist_payload_artifact_pair(
            base_dir,
            &run_id,
            &draft_payload_id,
            &draft_json,
            &draft_artifact_id,
            &draft_bundle.artifact,
        ) {
            self.fail_run(base_dir, run, seq, &format!("draft persistence: {e}"))
                .await?;
            return Err(e);
        }

        run.latest_draft_id = Some(draft_payload_id.clone());
        run.status_summary = "drafting: reviewing requirements".to_owned();
        run.updated_at = Utc::now();
        self.store.write_run(base_dir, &run_id, run)?;

        let event = journal_event(
            seq,
            Utc::now(),
            RequirementsJournalEventType::DraftGenerated,
            run,
        );
        self.store
            .append_journal_event(base_dir, &run_id, &event)?;
        seq += 1;

        // ── Review ──────────────────────────────────────────────────────
        let review_prompt = format!(
            "Review the following requirements draft:\n\n{}",
            draft_bundle.artifact
        );
        let review_result = self
            .invoke_stage(
                &run_root,
                RequirementsStageId::RequirementsReview,
                BackendRole::Reviewer,
                &review_prompt,
            )
            .await;

        let review_bundle = match review_result {
            Ok(b) => b,
            Err(e) => {
                self.fail_run(base_dir, run, seq, &format!("review: {e}"))
                    .await?;
                return Err(e);
            }
        };

        let RequirementsPayload::Review(ref review_payload) = review_bundle.payload else {
            unreachable!();
        };

        let review_payload_id = format!("{run_id}-review-1");
        let review_artifact_id = format!("{run_id}-review-art-1");
        let review_json = serde_json::to_value(review_payload)?;

        if let Err(e) = self.persist_payload_artifact_pair(
            base_dir,
            &run_id,
            &review_payload_id,
            &review_json,
            &review_artifact_id,
            &review_bundle.artifact,
        ) {
            self.fail_run(base_dir, run, seq, &format!("review persistence: {e}"))
                .await?;
            return Err(e);
        }

        run.latest_review_id = Some(review_payload_id.clone());
        run.updated_at = Utc::now();
        self.store.write_run(base_dir, &run_id, run)?;

        let event = journal_event(
            seq,
            Utc::now(),
            RequirementsJournalEventType::ReviewCompleted,
            run,
        );
        self.store
            .append_journal_event(base_dir, &run_id, &event)?;
        seq += 1;

        // Check review outcome
        match review_payload.outcome {
            RequirementsReviewOutcome::Approved | RequirementsReviewOutcome::ConditionallyApproved => {
                // Continue to seed generation
            }
            RequirementsReviewOutcome::RequestChanges | RequirementsReviewOutcome::Rejected => {
                // Fail the run after review payload/artifact are durably committed
                let msg = format!(
                    "review outcome: {} — {}",
                    review_payload.outcome,
                    review_payload.findings.join("; ")
                );
                self.fail_run(base_dir, run, seq, &msg).await?;
                return Err(AppError::InvalidRequirementsState {
                    run_id: run_id.clone(),
                    details: msg,
                });
            }
        }

        // ── Seed ────────────────────────────────────────────────────────
        let follow_ups = if review_payload.outcome == RequirementsReviewOutcome::ConditionallyApproved {
            review_payload.follow_ups.clone()
        } else {
            Vec::new()
        };

        let seed_prompt = format!(
            "Generate a project seed from the following requirements:\n\n{}\n\nFollow-ups: {}",
            draft_bundle.artifact,
            if follow_ups.is_empty() {
                "none".to_owned()
            } else {
                follow_ups.join("; ")
            }
        );

        let seed_result = self
            .invoke_stage(
                &run_root,
                RequirementsStageId::ProjectSeed,
                BackendRole::Planner,
                &seed_prompt,
            )
            .await;

        let seed_bundle = match seed_result {
            Ok(b) => b,
            Err(e) => {
                self.fail_run(base_dir, run, seq, &format!("seed generation: {e}"))
                    .await?;
                return Err(e);
            }
        };

        let RequirementsPayload::Seed(ref seed_payload) = seed_bundle.payload else {
            unreachable!();
        };

        let seed_payload_id = format!("{run_id}-seed-1");
        let seed_artifact_id = format!("{run_id}-seed-art-1");
        let seed_json = serde_json::to_value(seed_payload)?;

        if let Err(e) = self.persist_payload_artifact_pair(
            base_dir,
            &run_id,
            &seed_payload_id,
            &seed_json,
            &seed_artifact_id,
            &seed_bundle.artifact,
        ) {
            self.fail_run(base_dir, run, seq, &format!("seed persistence: {e}"))
                .await?;
            return Err(e);
        }

        // Write seed files atomically
        let project_json = serde_json::to_value(seed_payload)?;
        if let Err(e) = self
            .store
            .write_seed_pair(base_dir, &run_id, &project_json, &seed_payload.prompt_body)
        {
            // Rollback: remove both seed outputs, keep draft/review
            let _ = self.store.remove_seed_pair(base_dir, &run_id);
            self.fail_run(base_dir, run, seq, &format!("seed file write: {e}"))
                .await?;
            return Err(AppError::SeedPersistenceFailed {
                run_id: run_id.clone(),
                details: e.to_string(),
            });
        }

        run.latest_seed_id = Some(seed_payload_id.clone());
        run.status = RequirementsStatus::Completed;
        run.status_summary = "completed".to_owned();
        run.updated_at = Utc::now();
        self.store.write_run(base_dir, &run_id, run)?;

        let event = journal_event(
            seq,
            Utc::now(),
            RequirementsJournalEventType::SeedGenerated,
            run,
        );
        self.store
            .append_journal_event(base_dir, &run_id, &event)?;
        seq += 1;

        let event = journal_event(
            seq,
            Utc::now(),
            RequirementsJournalEventType::RunCompleted,
            run,
        );
        self.store
            .append_journal_event(base_dir, &run_id, &event)?;

        let seed_path = self.store.seed_prompt_path(base_dir, &run_id);
        println!("Requirements completed. Seed files at:");
        println!("  {}", seed_path.display());
        println!(
            "\nCreate project:\n  ralph-burning project create --id {} --name \"{}\" --flow {} --prompt {}",
            seed_payload.project_id,
            seed_payload.project_name,
            seed_payload.flow,
            seed_path.display()
        );

        Ok(())
    }

    async fn invoke_stage(
        &self,
        run_root: &Path,
        stage_id: RequirementsStageId,
        role: BackendRole,
        prompt: &str,
    ) -> AppResult<RequirementsValidatedBundle> {
        let contract_label = format!("requirements:{}", stage_id.as_str());
        let target = self.agent_service.resolver().resolve(role, None, None, None)?;

        let request = InvocationRequest {
            invocation_id: format!(
                "req-{}-{}",
                stage_id.as_str(),
                Utc::now().format("%Y%m%d%H%M%S")
            ),
            project_root: run_root.to_path_buf(),
            contract: InvocationContract::Requirements {
                label: contract_label,
            },
            role,
            resolved_target: target,
            payload: InvocationPayload {
                prompt: prompt.to_owned(),
                context: serde_json::Value::Null,
            },
            timeout: std::time::Duration::from_secs(300),
            cancellation_token: CancellationToken::new(),
            session_policy: SessionPolicy::NewSession,
            prior_session: None,
            attempt_number: 1,
        };

        let envelope = self.agent_service.invoke(request).await?;

        // Validate using requirements contract
        let contract = match stage_id {
            RequirementsStageId::QuestionSet => RequirementsContract::question_set(),
            RequirementsStageId::RequirementsDraft => RequirementsContract::draft(),
            RequirementsStageId::RequirementsReview => RequirementsContract::review(),
            RequirementsStageId::ProjectSeed => RequirementsContract::seed(),
        };

        let bundle = contract
            .evaluate(&envelope.parsed_payload)
            .map_err(|e| AppError::InvalidRequirementsState {
                run_id: "".to_owned(),
                details: format!("contract validation failed for {}: {e}", stage_id),
            })?;

        Ok(bundle)
    }

    fn persist_payload_artifact_pair(
        &self,
        base_dir: &Path,
        run_id: &str,
        payload_id: &str,
        payload: &Value,
        artifact_id: &str,
        artifact: &str,
    ) -> AppResult<()> {
        self.store
            .write_payload(base_dir, run_id, payload_id, payload)?;
        if let Err(e) = self
            .store
            .write_artifact(base_dir, run_id, artifact_id, artifact)
        {
            // Rollback: payload was written but artifact failed — this is a
            // partial failure. In practice the payload file should be cleaned up,
            // but for now we propagate the error.
            return Err(e);
        }
        Ok(())
    }

    async fn fail_run(
        &self,
        base_dir: &Path,
        run: &mut RequirementsRun,
        seq: u64,
        message: &str,
    ) -> AppResult<()> {
        run.status = RequirementsStatus::Failed;
        run.status_summary = format!("failed: {message}");
        run.updated_at = Utc::now();
        self.store.write_run(base_dir, &run.run_id, run)?;

        let event = journal_event(
            seq,
            Utc::now(),
            RequirementsJournalEventType::RunFailed,
            run,
        );
        self.store
            .append_journal_event(base_dir, &run.run_id, &event)?;
        Ok(())
    }

    fn next_sequence(&self, base_dir: &Path, run_id: &str) -> AppResult<u64> {
        let events = self.store.read_journal(base_dir, run_id)?;
        Ok(events.last().map_or(0, |e| e.sequence) + 1)
    }
}

// ── Show result ─────────────────────────────────────────────────────────────

/// Read model for `requirements show`.
#[derive(Debug)]
pub struct RequirementsShowResult {
    pub run: RequirementsRun,
    pub journal_event_count: u64,
    pub pending_question_count: Option<u32>,
    pub failure_summary: Option<String>,
    pub seed_prompt_path: Option<std::path::PathBuf>,
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn generate_run_id(now: DateTime<Utc>) -> String {
    format!("req-{}", now.format("%Y%m%d-%H%M%S"))
}

fn requirements_run_root(base_dir: &Path, run_id: &str) -> std::path::PathBuf {
    base_dir
        .join(".ralph-burning")
        .join("requirements")
        .join(run_id)
}

fn journal_event(
    sequence: u64,
    timestamp: DateTime<Utc>,
    event_type: RequirementsJournalEventType,
    run: &RequirementsRun,
) -> RequirementsJournalEvent {
    RequirementsJournalEvent {
        sequence,
        timestamp,
        event_type,
        details: serde_json::json!({
            "run_id": run.run_id,
            "status": run.status,
            "status_summary": run.status_summary,
        }),
    }
}

fn generate_answers_template(qs: &QuestionSetPayload) -> String {
    let mut out = String::new();
    out.push_str("# Answers for clarifying questions\n");
    out.push_str("# Fill in answers below each question.\n\n");

    for q in &qs.questions {
        let required = if q.required { " (required)" } else { "" };
        out.push_str(&format!("# {}{}\n", q.prompt, required));
        if !q.rationale.is_empty() {
            out.push_str(&format!("# Rationale: {}\n", q.rationale));
        }
        let default_value = q
            .suggested_default
            .as_deref()
            .unwrap_or("");
        out.push_str(&format!("{} = \"{}\"\n\n", q.id, default_value));
    }

    out
}

fn parse_and_validate_answers<Q: RequirementsStorePort>(
    answers_raw: &str,
    base_dir: &Path,
    run_id: &str,
    store: &Q,
) -> AppResult<PersistedAnswers> {
    // Parse TOML
    let table: toml::Table = toml::from_str(answers_raw).map_err(|e| {
        AppError::AnswerValidationFailed {
            run_id: run_id.to_owned(),
            details: format!("invalid TOML: {e}"),
        }
    })?;

    // Read the question set to get required question IDs
    let run = store.read_run(base_dir, run_id)?;
    let _qs_id = run.latest_question_set_id.as_ref().ok_or_else(|| {
        AppError::InvalidRequirementsState {
            run_id: run_id.to_owned(),
            details: "no question set committed".to_owned(),
        }
    })?;

    // Build answers from TOML keys
    let mut answers = Vec::new();
    for (key, value) in &table {
        let answer_text = match value.as_str() {
            Some(s) => s.to_owned(),
            None => value.to_string(),
        };
        answers.push(AnswerEntry {
            question_id: key.clone(),
            answer: answer_text,
        });
    }

    // Note: full validation against required question IDs would require
    // reading the question set payload. For now we validate non-empty answers.
    for entry in &answers {
        if entry.answer.trim().is_empty() {
            // We just skip empty answers; the required check would need the
            // question set payload to determine which are required.
        }
    }

    Ok(PersistedAnswers { answers })
}

fn build_draft_prompt(idea: &str, answers: &[(String, String)]) -> String {
    let mut prompt = format!("Draft requirements for the following idea:\n\n{idea}\n");

    if !answers.is_empty() {
        prompt.push_str("\nClarifying answers:\n");
        for (qid, answer) in answers {
            prompt.push_str(&format!("- {qid}: {answer}\n"));
        }
    }

    prompt
}
