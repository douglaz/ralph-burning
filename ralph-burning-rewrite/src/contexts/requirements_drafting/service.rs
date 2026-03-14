#![forbid(unsafe_code)]

//! Requirements drafting service.
//!
//! Orchestrates `draft`, `quick`, `show`, and `answer` commands, including
//! answer-template validation, seed rollback, and failure invariants.

use std::collections::HashSet;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::adapters::fs::FileSystem;
use crate::contexts::agent_execution::model::{
    CancellationToken, InvocationContract, InvocationPayload, InvocationRequest,
};
use crate::contexts::agent_execution::service::{
    AgentExecutionPort, AgentExecutionService, BackendSelectionConfig,
};
use crate::contexts::agent_execution::session::SessionStorePort;
use crate::contexts::agent_execution::RawOutputPort;
use crate::shared::domain::{BackendRole, FlowPreset, SessionPolicy};
use crate::shared::error::{AppError, AppResult};

use super::contracts::{RequirementsContract, RequirementsPayload};
use super::model::{
    AnswerEntry, PersistedAnswers, QuestionSetPayload, RequirementsJournalEvent,
    RequirementsJournalEventType, RequirementsReviewOutcome, RequirementsRun, RequirementsStageId,
    RequirementsStatus,
};
use super::renderers;

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
    fn read_payload(&self, base_dir: &Path, run_id: &str, payload_id: &str) -> AppResult<Value>;
    fn write_payload_artifact_pair_atomic(
        &self,
        base_dir: &Path,
        run_id: &str,
        payload_id: &str,
        payload: &Value,
        artifact_id: &str,
        artifact: &str,
    ) -> AppResult<()>;
    fn write_answers_toml(&self, base_dir: &Path, run_id: &str, template: &str) -> AppResult<()>;
    fn read_answers_toml(&self, base_dir: &Path, run_id: &str) -> AppResult<String>;
    fn write_answers_json(
        &self,
        base_dir: &Path,
        run_id: &str,
        answers: &PersistedAnswers,
    ) -> AppResult<()>;
    fn read_answers_json(&self, base_dir: &Path, run_id: &str) -> AppResult<PersistedAnswers>;
    fn write_seed_pair(
        &self,
        base_dir: &Path,
        run_id: &str,
        project_json: &Value,
        prompt_md: &str,
    ) -> AppResult<()>;
    fn remove_seed_pair(&self, base_dir: &Path, run_id: &str) -> AppResult<()>;
    fn remove_payload_artifact_pair(
        &self,
        base_dir: &Path,
        run_id: &str,
        payload_id: &str,
        artifact_id: &str,
    ) -> AppResult<()>;
    fn answers_toml_path(&self, base_dir: &Path, run_id: &str) -> std::path::PathBuf;
    fn seed_prompt_path(&self, base_dir: &Path, run_id: &str) -> std::path::PathBuf;
}

// ── Service ─────────────────────────────────────────────────────────────────

pub struct RequirementsService<A, R, S, Q> {
    agent_service: AgentExecutionService<A, R, S>,
    store: Q,
    workspace_defaults: Option<BackendSelectionConfig>,
}

impl<A, R, S, Q> RequirementsService<A, R, S, Q> {
    pub fn new(agent_service: AgentExecutionService<A, R, S>, store: Q) -> Self {
        Self {
            agent_service,
            store,
            workspace_defaults: None,
        }
    }

    pub fn with_workspace_defaults(mut self, defaults: BackendSelectionConfig) -> Self {
        self.workspace_defaults = Some(defaults);
        self
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
    pub async fn draft(
        &self,
        base_dir: &Path,
        idea: &str,
        now: DateTime<Utc>,
    ) -> AppResult<String> {
        let run_id = generate_run_id(now);
        let mut run = RequirementsRun::new_draft(run_id.clone(), idea.to_owned(), now);

        // Create run directory and initial state
        self.store.create_run_dir(base_dir, &run_id)?;
        self.store.write_run(base_dir, &run_id, &run)?;

        let created_event = journal_event(1, now, RequirementsJournalEventType::RunCreated, &run);
        if let Err(e) = self
            .store
            .append_journal_event(base_dir, &run_id, &created_event)
        {
            self.fail_run(
                base_dir,
                &mut run,
                1,
                &format!("journal append failed for run_created: {e}"),
            )
            .await?;
            return Err(e);
        }

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

                let previous_question_round = run.question_round;
                let previous_question_set_id = run.latest_question_set_id.clone();
                let question_round = next_question_round(&run);
                let payload_id = format!("{run_id}-qs-{question_round}");
                let artifact_id = format!("{run_id}-qs-art-{question_round}");

                // Persist payload and artifact atomically
                let payload_json = serde_json::to_value(qs)?;
                if let Err(e) = self.store.write_payload_artifact_pair_atomic(
                    base_dir,
                    &run_id,
                    &payload_id,
                    &payload_json,
                    &artifact_id,
                    &bundle.artifact,
                ) {
                    // State invariant: if payload/artifact pair fails, run must not
                    // transition and no partial files remain (handled by atomic write).
                    // Durably journal the failure so show() can report it.
                    run.status = RequirementsStatus::Failed;
                    run.status_summary = format!("failed: could not persist question set: {e}");
                    run.updated_at = Utc::now();
                    let _ = self.store.write_run(base_dir, &run_id, &run);
                    let fail_event =
                        journal_event(2, Utc::now(), RequirementsJournalEventType::RunFailed, &run);
                    let _ = self
                        .store
                        .append_journal_event(base_dir, &run_id, &fail_event);
                    return Err(e);
                }

                // Record the committed question-set boundary in canonical state and
                // journal immediately after the payload/artifact pair succeeds,
                // before branching into any downstream path.
                run.question_round = question_round;
                run.latest_question_set_id = Some(payload_id.clone());
                run.updated_at = Utc::now();
                self.store.write_run(base_dir, &run_id, &run)?;

                let qs_event = journal_event(
                    2,
                    Utc::now(),
                    RequirementsJournalEventType::QuestionsGenerated,
                    &run,
                );
                if let Err(e) = self
                    .store
                    .append_journal_event(base_dir, &run_id, &qs_event)
                {
                    // Roll back question payload/artifact and boundary marker
                    // so run.json does not reference deleted history.
                    let _ = self.store.remove_payload_artifact_pair(
                        base_dir,
                        &run_id,
                        &payload_id,
                        &artifact_id,
                    );
                    run.latest_question_set_id = previous_question_set_id;
                    run.question_round = previous_question_round;
                    self.fail_run(
                        base_dir,
                        &mut run,
                        2,
                        &format!("journal append failed for questions_generated: {e}"),
                    )
                    .await?;
                    return Err(e);
                }

                // Record the pending question count in canonical state for all
                // paths (including failure), so show() can report it without
                // scanning artifacts.
                let question_count = qs.questions.len() as u32;
                run.pending_question_count = Some(question_count);

                if qs.questions.is_empty() {
                    // No questions needed — continue directly
                    self.continue_after_answers(base_dir, &mut run, idea, &[], 3, now)
                        .await?;
                } else {
                    // Generate answers.toml template — fail run on write error
                    let template = generate_answers_template(qs);
                    if let Err(e) = self.store.write_answers_toml(base_dir, &run_id, &template) {
                        // Question payload/artifact and question-set boundary are already
                        // committed; fail the run but retain the committed question set.
                        // Durably journal the failure.
                        run.status = RequirementsStatus::Failed;
                        run.status_summary =
                            format!("failed: could not write answers template: {e}");
                        run.updated_at = Utc::now();
                        let _ = self.store.write_run(base_dir, &run_id, &run);
                        let fail_event = journal_event(
                            3,
                            Utc::now(),
                            RequirementsJournalEventType::RunFailed,
                            &run,
                        );
                        let _ = self
                            .store
                            .append_journal_event(base_dir, &run_id, &fail_event);
                        return Err(e);
                    }

                    run.status = RequirementsStatus::AwaitingAnswers;
                    run.status_summary = format!(
                        "awaiting answers: {} question(s), round {}",
                        question_count, question_round
                    );
                    run.updated_at = Utc::now();
                    self.store.write_run(base_dir, &run_id, &run)?;

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

                let event =
                    journal_event(2, Utc::now(), RequirementsJournalEventType::RunFailed, &run);
                self.store.append_journal_event(base_dir, &run_id, &event)?;
                return Err(e);
            }
        }

        Ok(run_id)
    }

    /// Execute `requirements quick --idea "<text>"`.
    pub async fn quick(
        &self,
        base_dir: &Path,
        idea: &str,
        now: DateTime<Utc>,
    ) -> AppResult<String> {
        let run_id = generate_run_id(now);
        let mut run = RequirementsRun::new_quick(run_id.clone(), idea.to_owned(), now);

        self.store.create_run_dir(base_dir, &run_id)?;
        self.store.write_run(base_dir, &run_id, &run)?;

        let created_event = journal_event(1, now, RequirementsJournalEventType::RunCreated, &run);
        if let Err(e) = self
            .store
            .append_journal_event(base_dir, &run_id, &created_event)
        {
            self.fail_run(
                base_dir,
                &mut run,
                1,
                &format!("journal append failed for run_created: {e}"),
            )
            .await?;
            return Err(e);
        }

        // Skip question generation entirely — go straight to draft
        self.continue_after_answers(base_dir, &mut run, idea, &[], 2, now)
            .await?;

        Ok(run_id)
    }

    /// Execute `requirements show <run-id>`.
    ///
    /// Derives its read model from `run.json` and `journal.ndjson`, not by
    /// scanning artifacts.
    pub fn show(&self, base_dir: &Path, run_id: &str) -> AppResult<RequirementsShowResult> {
        let run = self.store.read_run(base_dir, run_id)?;
        let journal = self.store.read_journal(base_dir, run_id)?;

        // Pending question count from canonical run state.
        // Only surface pending questions when the latest durable boundary is
        // still the committed question set. Once answers are durably stored or
        // downstream draft/review state exists, pending_question_count is
        // cleared to avoid reporting stale values.
        let pending_question_count = match run.status {
            RequirementsStatus::AwaitingAnswers => run.pending_question_count,
            RequirementsStatus::Failed => {
                // If a draft has already been committed past the question
                // boundary, questions are no longer pending.
                if run.latest_draft_id.is_some() {
                    None
                } else if journal
                    .iter()
                    .any(|e| e.event_type == RequirementsJournalEventType::AnswersSubmitted)
                {
                    // Answers were durably submitted — the question boundary
                    // has been crossed even though the run later failed.
                    None
                } else {
                    run.pending_question_count
                }
            }
            _ => None,
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

        // recommended_flow from canonical run state (set during draft generation)
        let recommended_flow = run.recommended_flow;

        Ok(RequirementsShowResult {
            run,
            journal_event_count: journal.len() as u64,
            pending_question_count,
            recommended_flow,
            failure_summary,
            seed_prompt_path,
        })
    }

    /// Execute `requirements answer <run-id>`.
    ///
    /// Valid only for runs in `awaiting_answers` status, or failed runs whose
    /// latest durable boundary is a committed question set (no draft or review
    /// has been committed past the question set, and no answers have been
    /// durably submitted).
    pub async fn answer(&self, base_dir: &Path, run_id: &str) -> AppResult<()> {
        let mut run = self.store.read_run(base_dir, run_id)?;

        // Durable-boundary gating: only allow answer when the latest committed
        // boundary is the question set. Once answers have been durably recorded
        // — either via AnswersSubmitted in the journal OR via a non-empty
        // answers.json on disk — the question set is no longer the latest
        // boundary and `answer` must be rejected.
        match run.status {
            RequirementsStatus::AwaitingAnswers => {
                // Defense-in-depth: verify answers haven't already been
                // durably submitted even in AwaitingAnswers state (can happen
                // if write_run() failed after answers.json was written).
                if self.answers_already_durably_stored(base_dir, run_id)? {
                    return Err(AppError::InvalidRequirementsState {
                        run_id: run_id.to_owned(),
                        details: "cannot answer: answers already durably submitted past question boundary".to_owned(),
                    });
                }
            }
            RequirementsStatus::Failed
                if run.latest_question_set_id.is_some() && run.latest_draft_id.is_none() =>
            {
                // Additional check: verify answers haven't already been
                // durably submitted past the question boundary.
                if self.answers_already_durably_stored(base_dir, run_id)? {
                    return Err(AppError::InvalidRequirementsState {
                        run_id: run_id.to_owned(),
                        details: "cannot answer: answers already durably submitted past question boundary".to_owned(),
                    });
                }
            }
            _ => {
                return Err(AppError::InvalidRequirementsState {
                    run_id: run_id.to_owned(),
                    details: format!(
                        "cannot answer: run is in '{}' state{}",
                        run.status,
                        if run.latest_draft_id.is_some() {
                            " (draft already committed past question boundary)"
                        } else {
                            ""
                        }
                    ),
                });
            }
        }

        // Open editor
        let answers_path = self.store.answers_toml_path(base_dir, run_id);
        FileSystem::open_editor(&answers_path)?;

        // Read and validate answers against the committed question set
        let answers_raw = self.store.read_answers_toml(base_dir, run_id)?;
        let answers = parse_and_validate_answers(&answers_raw, base_dir, run_id, &self.store)?;

        // Persist answers.json
        self.store.write_answers_json(base_dir, run_id, &answers)?;

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

        // Append AnswersSubmitted AFTER the run transition is durably committed.
        // This ensures that if write_run() fails, the journal does not contain
        // AnswersSubmitted while the run still appears resumable from the question
        // boundary.
        let seq = self.next_sequence(base_dir, run_id)?;
        let event = journal_event(
            seq,
            Utc::now(),
            RequirementsJournalEventType::AnswersSubmitted,
            &run,
        );
        if let Err(e) = self.store.append_journal_event(base_dir, run_id, &event) {
            self.fail_run(
                base_dir,
                &mut run,
                seq,
                &format!("journal append failed for answers_submitted: {e}"),
            )
            .await?;
            return Err(e);
        }

        let idea = run.idea.clone();
        self.continue_after_answers(
            base_dir,
            &mut run,
            &idea,
            &answer_entries,
            seq + 1,
            Utc::now(),
        )
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

        let question_round = effective_question_round(run);
        let draft_payload_id = format!("{run_id}-draft-{question_round}");
        let draft_artifact_id = format!("{run_id}-draft-art-{question_round}");
        let draft_json = serde_json::to_value(draft_payload)?;

        if let Err(e) = self.store.write_payload_artifact_pair_atomic(
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
        run.recommended_flow = Some(draft_payload.recommended_flow);
        run.status_summary = "drafting: reviewing requirements".to_owned();
        run.updated_at = Utc::now();
        self.store.write_run(base_dir, &run_id, run)?;

        let event = journal_event(
            seq,
            Utc::now(),
            RequirementsJournalEventType::DraftGenerated,
            run,
        );
        if let Err(e) = self.store.append_journal_event(base_dir, &run_id, &event) {
            // Roll back draft payload/artifact and boundary marker
            let _ = self.store.remove_payload_artifact_pair(
                base_dir,
                &run_id,
                &draft_payload_id,
                &draft_artifact_id,
            );
            run.latest_draft_id = None;
            run.recommended_flow = None;
            self.fail_run(
                base_dir,
                run,
                seq,
                &format!("journal append failed for draft_generated: {e}"),
            )
            .await?;
            return Err(e);
        }
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

        let review_payload_id = format!("{run_id}-review-{question_round}");
        let review_artifact_id = format!("{run_id}-review-art-{question_round}");
        let review_json = serde_json::to_value(review_payload)?;

        if let Err(e) = self.store.write_payload_artifact_pair_atomic(
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
        if let Err(e) = self.store.append_journal_event(base_dir, &run_id, &event) {
            // Roll back review payload/artifact and boundary marker
            let _ = self.store.remove_payload_artifact_pair(
                base_dir,
                &run_id,
                &review_payload_id,
                &review_artifact_id,
            );
            run.latest_review_id = None;
            self.fail_run(
                base_dir,
                run,
                seq,
                &format!("journal append failed for review_completed: {e}"),
            )
            .await?;
            return Err(e);
        }
        seq += 1;

        // Check review outcome
        match review_payload.outcome {
            RequirementsReviewOutcome::Approved
            | RequirementsReviewOutcome::ConditionallyApproved => {
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
        let follow_ups =
            if review_payload.outcome == RequirementsReviewOutcome::ConditionallyApproved {
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

        // Extract seed payload and merge review follow-ups for conditional approvals
        let mut seed_payload = match seed_bundle.payload {
            RequirementsPayload::Seed(p) => p,
            _ => unreachable!(),
        };

        if !follow_ups.is_empty() {
            // Merge review follow-ups into canonical seed metadata
            for fu in &follow_ups {
                if !seed_payload.follow_ups.contains(fu) {
                    seed_payload.follow_ups.push(fu.clone());
                }
            }
            // Append follow-ups to the rendered handoff summary
            seed_payload.handoff_summary = format!(
                "{}\n\nFollow-ups from conditional approval:\n{}",
                seed_payload.handoff_summary,
                follow_ups
                    .iter()
                    .map(|f| format!("- {f}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            );
        }

        // Re-render seed artifact with merged follow-ups
        let seed_artifact = renderers::render_project_seed(&seed_payload);

        let seed_payload_id = format!("{run_id}-seed-1");
        let seed_artifact_id = format!("{run_id}-seed-art-1");
        let seed_json = serde_json::to_value(&seed_payload)?;

        // Write seed files BEFORE committing history, so that if seed file
        // writes fail, no orphaned seed history payload/artifact remains.
        // Only the already committed draft/review history survives.
        let project_json = serde_json::to_value(&seed_payload)?;
        if let Err(e) =
            self.store
                .write_seed_pair(base_dir, &run_id, &project_json, &seed_payload.prompt_body)
        {
            // Terminal-transition ordering: persist the failed state BEFORE
            // cleaning up external side effects. This ensures that if the
            // process crashes after cleanup, canonical state is already
            // terminal rather than appearing non-terminal with missing files.
            self.fail_run(base_dir, run, seq, &format!("seed file write: {e}"))
                .await?;
            let _ = self.store.remove_seed_pair(base_dir, &run_id);
            return Err(AppError::SeedPersistenceFailed {
                run_id: run_id.clone(),
                details: e.to_string(),
            });
        }

        // Now commit the seed payload/artifact to history.
        if let Err(e) = self.store.write_payload_artifact_pair_atomic(
            base_dir,
            &run_id,
            &seed_payload_id,
            &seed_json,
            &seed_artifact_id,
            &seed_artifact,
        ) {
            // Terminal-transition ordering: persist the failed state BEFORE
            // cleaning up seed files, so canonical state is terminal even if
            // the process crashes between cleanup and state persistence.
            self.fail_run(
                base_dir,
                run,
                seq,
                &format!("seed history persistence: {e}"),
            )
            .await?;
            let _ = self.store.remove_seed_pair(base_dir, &run_id);
            return Err(e);
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
        if let Err(e) = self.store.append_journal_event(base_dir, &run_id, &event) {
            // Roll back seed payload/artifact, boundary marker, and seed files.
            // Persist Failed state BEFORE cleanup (terminal-transition ordering).
            let _ = self.store.remove_payload_artifact_pair(
                base_dir,
                &run_id,
                &seed_payload_id,
                &seed_artifact_id,
            );
            run.latest_seed_id = None;
            self.fail_run(
                base_dir,
                run,
                seq,
                &format!("journal append failed for seed_generated: {e}"),
            )
            .await?;
            let _ = self.store.remove_seed_pair(base_dir, &run_id);
            return Err(e);
        }
        seq += 1;

        let event = journal_event(
            seq,
            Utc::now(),
            RequirementsJournalEventType::RunCompleted,
            run,
        );
        // Best-effort: the run is already durably complete in run.json with all
        // seed files and history committed. A missing RunCompleted journal event
        // is non-ideal but does not affect run state consistency.
        let _ = self.store.append_journal_event(base_dir, &run_id, &event);

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
    ) -> AppResult<super::contracts::RequirementsValidatedBundle> {
        let contract_label = format!("requirements:{}", stage_id.as_str());
        let target = self.agent_service.resolver().resolve(
            role,
            None,
            None,
            self.workspace_defaults.as_ref(),
        )?;

        let request = InvocationRequest {
            invocation_id: format!(
                "req-{}-{}",
                stage_id.as_str(),
                Utc::now().format("%Y%m%d%H%M%S")
            ),
            project_root: run_root.to_path_buf(),
            working_dir: run_root.to_path_buf(),
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

        let bundle = contract.evaluate(&envelope.parsed_payload).map_err(|e| {
            AppError::InvalidRequirementsState {
                run_id: "".to_owned(),
                details: format!("contract validation failed for {}: {e}", stage_id),
            }
        })?;

        Ok(bundle)
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
        // Best-effort journal append: if the journal is failing, the run.json
        // Failed state is already committed and is the authoritative record.
        // This matches the workflow engine's fail_run() pattern (engine.rs:1755).
        let _ = self
            .store
            .append_journal_event(base_dir, &run.run_id, &event);
        Ok(())
    }

    /// Check whether answers have already been durably stored past the question
    /// boundary, by consulting both the journal (for AnswersSubmitted events) and
    /// the persisted answers.json (for non-empty answer content written before the
    /// run/journal transition could complete).
    fn answers_already_durably_stored(&self, base_dir: &Path, run_id: &str) -> AppResult<bool> {
        // Check journal first — authoritative event source.
        let journal = self.store.read_journal(base_dir, run_id)?;
        if journal
            .iter()
            .any(|e| e.event_type == RequirementsJournalEventType::AnswersSubmitted)
        {
            return Ok(true);
        }

        // Check answers.json content — covers the case where answers.json was
        // durably written but write_run()/journal append failed afterward.
        // If answers.json doesn't exist or can't be read, treat as no answers
        // stored (same as the initial empty state from create_run_dir).
        if let Ok(persisted) = self.store.read_answers_json(base_dir, run_id) {
            if !persisted.answers.is_empty() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn next_sequence(&self, base_dir: &Path, run_id: &str) -> AppResult<u64> {
        let events = self.store.read_journal(base_dir, run_id)?;
        Ok(events.last().map_or(0, |e| e.sequence) + 1)
    }
}

// ── Daemon-facing helpers ───────────────────────────────────────────────────

/// Read a requirements run status without needing the full service.
/// Used by the daemon to check whether a linked requirements run is complete.
pub fn read_requirements_run_status(
    store: &dyn RequirementsStorePort,
    base_dir: &Path,
    run_id: &str,
) -> AppResult<RequirementsRun> {
    store.read_run(base_dir, run_id)
}

/// Check if a requirements run has completed and produced a seed.
/// Returns `true` if the run is in `Completed` status.
pub fn is_requirements_run_complete(
    store: &dyn RequirementsStorePort,
    base_dir: &Path,
    run_id: &str,
) -> AppResult<bool> {
    let run = store.read_run(base_dir, run_id)?;
    Ok(run.status == RequirementsStatus::Completed)
}

/// Extract seed handoff data from a completed requirements run.
/// Returns the project seed payload and the seed prompt path.
pub fn extract_seed_handoff(
    store: &dyn RequirementsStorePort,
    base_dir: &Path,
    run_id: &str,
) -> AppResult<SeedHandoff> {
    let run = store.read_run(base_dir, run_id)?;
    if run.status != RequirementsStatus::Completed {
        return Err(AppError::RequirementsHandoffFailed {
            task_id: run_id.to_owned(),
            details: format!(
                "requirements run is in '{}' status, expected 'completed'",
                run.status
            ),
        });
    }

    let seed_id =
        run.latest_seed_id
            .as_deref()
            .ok_or_else(|| AppError::RequirementsHandoffFailed {
                task_id: run_id.to_owned(),
                details: "completed requirements run has no seed_id".to_owned(),
            })?;

    let seed_json = store.read_payload(base_dir, run_id, seed_id)?;
    let seed: super::model::ProjectSeedPayload =
        serde_json::from_value(seed_json).map_err(|e| AppError::RequirementsHandoffFailed {
            task_id: run_id.to_owned(),
            details: format!("failed to parse seed payload: {e}"),
        })?;

    let seed_prompt_path = store.seed_prompt_path(base_dir, run_id);

    Ok(SeedHandoff {
        project_id: seed.project_id,
        project_name: seed.project_name,
        flow: seed.flow,
        prompt_body: seed.prompt_body,
        prompt_path: seed_prompt_path,
        recommended_flow: run.recommended_flow,
    })
}

/// Seed handoff data extracted from a completed requirements run.
#[derive(Debug, Clone)]
pub struct SeedHandoff {
    pub project_id: String,
    pub project_name: String,
    pub flow: FlowPreset,
    pub prompt_body: String,
    pub prompt_path: std::path::PathBuf,
    pub recommended_flow: Option<FlowPreset>,
}

// ── Show result ─────────────────────────────────────────────────────────────

/// Read model for `requirements show`.
#[derive(Debug)]
pub struct RequirementsShowResult {
    pub run: RequirementsRun,
    pub journal_event_count: u64,
    pub pending_question_count: Option<u32>,
    pub recommended_flow: Option<FlowPreset>,
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

fn next_question_round(run: &RequirementsRun) -> u32 {
    run.question_round + 1
}

fn effective_question_round(run: &RequirementsRun) -> u32 {
    run.question_round.max(1)
}

fn generate_answers_template(qs: &QuestionSetPayload) -> String {
    let mut out = String::new();
    out.push_str("# Answers for clarifying questions\n");
    out.push_str("# Fill in answers below each question.\n\n");

    for q in &qs.questions {
        let required = if q.required { " (required)" } else { "" };
        // Write prompt as comment lines, handling embedded newlines so each
        // continuation line starts with `# `.
        let mut first_prompt_line = true;
        for line in q.prompt.lines() {
            if first_prompt_line {
                out.push_str(&format!("# {}{}\n", line, required));
                first_prompt_line = false;
            } else {
                out.push_str(&format!("# {}\n", line));
            }
        }
        // If prompt was empty or had no lines iterator output, ensure at least
        // one comment line is emitted.
        if first_prompt_line {
            out.push_str(&format!("# {}\n", required.trim()));
        }

        if !q.rationale.is_empty() {
            // Handle multi-line rationale in comments.
            let mut first = true;
            for line in q.rationale.lines() {
                if first {
                    out.push_str(&format!("# Rationale: {}\n", line));
                    first = false;
                } else {
                    out.push_str(&format!("#   {}\n", line));
                }
            }
        }

        let default_value = q.suggested_default.as_deref().unwrap_or("");
        // Escape the default value for TOML basic string safety.
        let escaped = toml_escape_basic_string(default_value);
        out.push_str(&format!("{} = \"{}\"\n\n", q.id, escaped));
    }

    out
}

/// Escape a string value for safe embedding in a TOML basic string (double-quoted).
fn toml_escape_basic_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000C}' => out.push_str("\\f"),
            _ => out.push(c),
        }
    }
    out
}

/// Validate answers against the committed question set payload.
///
/// Loads the question set from persisted payloads, rejects unknown answer keys,
/// and requires non-empty values for required questions.
fn parse_and_validate_answers<Q: RequirementsStorePort>(
    answers_raw: &str,
    base_dir: &Path,
    run_id: &str,
    store: &Q,
) -> AppResult<PersistedAnswers> {
    // Parse TOML
    let table: toml::Table =
        toml::from_str(answers_raw).map_err(|e| AppError::AnswerValidationFailed {
            run_id: run_id.to_owned(),
            details: format!("invalid TOML: {e}"),
        })?;

    // Load the committed question set payload
    let run = store.read_run(base_dir, run_id)?;
    let qs_id =
        run.latest_question_set_id
            .as_ref()
            .ok_or_else(|| AppError::InvalidRequirementsState {
                run_id: run_id.to_owned(),
                details: "no question set committed".to_owned(),
            })?;

    let qs_json = store.read_payload(base_dir, run_id, qs_id)?;
    let qs: QuestionSetPayload =
        serde_json::from_value(qs_json).map_err(|e| AppError::InvalidRequirementsState {
            run_id: run_id.to_owned(),
            details: format!("corrupt question set payload: {e}"),
        })?;

    // Build valid question ID set
    let valid_ids: HashSet<&str> = qs.questions.iter().map(|q| q.id.as_str()).collect();

    // Reject unknown answer keys
    for key in table.keys() {
        if !valid_ids.contains(key.as_str()) {
            return Err(AppError::AnswerValidationFailed {
                run_id: run_id.to_owned(),
                details: format!("unknown question ID: '{key}'"),
            });
        }
    }

    // Require non-empty answers for required questions
    for q in &qs.questions {
        if q.required {
            match table.get(&q.id) {
                None => {
                    return Err(AppError::AnswerValidationFailed {
                        run_id: run_id.to_owned(),
                        details: format!("required question '{}' has no answer", q.id),
                    });
                }
                Some(v) => {
                    let text = v.as_str().unwrap_or("");
                    if text.trim().is_empty() {
                        return Err(AppError::AnswerValidationFailed {
                            run_id: run_id.to_owned(),
                            details: format!("required question '{}' has an empty answer", q.id),
                        });
                    }
                }
            }
        }
    }

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
