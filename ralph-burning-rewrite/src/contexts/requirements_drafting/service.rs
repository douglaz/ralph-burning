#![forbid(unsafe_code)]

//! Requirements drafting service.
//!
//! Orchestrates `draft`, `quick`, `show`, and `answer` commands, including
//! the full-mode staged pipeline, quick-mode revision loop, cache-keyed
//! stage reuse, conditional question rounds, and versioned seed handoff.

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

use super::contracts::{RequirementsContract, RequirementsPayload, RequirementsValidatedBundle};
use super::model::{
    AnswerEntry, CommittedStageEntry, FullModeStage, PersistedAnswers, QuestionSetPayload,
    RequirementsJournalEvent, RequirementsJournalEventType, RequirementsReviewOutcome,
    RequirementsRun, RequirementsStageId, RequirementsStatus, SeedSourceMetadata,
    ValidationOutcome, PROJECT_SEED_VERSION, SUPPORTED_SEED_VERSIONS,
};
use super::renderers;

/// Maximum quick-mode revision cycles before forcing termination.
const MAX_QUICK_REVISIONS: u32 = 5;

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
    ///
    /// In full mode, runs the staged pipeline: ideation → research → synthesis →
    /// implementation_spec → gap_analysis → validation → project_seed. If validation
    /// finds blocking missing information, opens a question round; otherwise proceeds
    /// to seed generation.
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

        // Run the full-mode staged pipeline
        self.run_full_mode_pipeline(base_dir, &mut run, idea, &[], 2, now)
            .await?;

        Ok(run_id)
    }

    /// Execute `requirements quick --idea "<text>"`.
    ///
    /// Quick mode uses the existing draft/review contracts in a bounded revision
    /// loop with structured revision feedback and approval-based termination.
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

        // Skip question generation entirely — go through quick revision loop
        self.run_quick_mode_pipeline(base_dir, &mut run, idea, &[], 2, now)
            .await?;

        Ok(run_id)
    }

    /// Execute `requirements show <run-id>`.
    ///
    /// Derives its read model from `run.json` and `journal.ndjson`, not by
    /// scanning artifacts. Stage-aware progress reported from canonical state.
    pub fn show(&self, base_dir: &Path, run_id: &str) -> AppResult<RequirementsShowResult> {
        let run = self.store.read_run(base_dir, run_id)?;
        let journal = self.store.read_journal(base_dir, run_id)?;

        let pending_question_count = match run.status {
            RequirementsStatus::AwaitingAnswers => run.pending_question_count,
            RequirementsStatus::Failed => {
                if run.latest_draft_id.is_some() {
                    None
                } else if journal
                    .iter()
                    .any(|e| e.event_type == RequirementsJournalEventType::AnswersSubmitted)
                {
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
    /// latest durable boundary is a committed question set.
    pub async fn answer(&self, base_dir: &Path, run_id: &str) -> AppResult<()> {
        let mut run = self.store.read_run(base_dir, run_id)?;

        match run.status {
            RequirementsStatus::AwaitingAnswers => {
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

        // Snapshot pre-answer state so we can restore on journal failure.
        let pre_answer_question_round = run.question_round;
        let pre_answer_status = run.status;
        let pre_answer_summary = run.status_summary.clone();

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

        let seq = self.next_sequence(base_dir, run_id)?;
        let event = journal_event(
            seq,
            Utc::now(),
            RequirementsJournalEventType::AnswersSubmitted,
            &run,
        );
        if let Err(e) = self.store.append_journal_event(base_dir, run_id, &event) {
            // Restore pre-answer question boundary so the run remains resumable.
            // Remove the non-durable answers.json so answers_already_durably_stored()
            // does not treat a failed journal append as a crossed boundary.
            run.question_round = pre_answer_question_round;
            run.status = pre_answer_status;
            run.status_summary = pre_answer_summary;
            // Best-effort removal of the non-durable answers.json; the answers
            // template (answers.toml) remains so the operator can retry.
            let empty_answers = PersistedAnswers { answers: vec![] };
            let _ = self
                .store
                .write_answers_json(base_dir, run_id, &empty_answers);
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

        // For full mode, invalidate synthesis and downstream stages on answer,
        // preserving ideation and research history.
        if run.mode == super::model::RequirementsMode::Draft {
            for stage in FullModeStage::question_round_invalidated() {
                run.committed_stages.remove(stage.as_str());
            }
            run.updated_at = Utc::now();
            self.store.write_run(base_dir, run_id, &run)?;

            self.run_full_mode_pipeline(
                base_dir,
                &mut run,
                &idea,
                &answer_entries,
                seq + 1,
                Utc::now(),
            )
            .await?;
        } else {
            self.run_quick_mode_pipeline(
                base_dir,
                &mut run,
                &idea,
                &answer_entries,
                seq + 1,
                Utc::now(),
            )
            .await?;
        }

        Ok(())
    }

    // ── Full-mode staged pipeline ───────────────────────────────────────

    async fn run_full_mode_pipeline(
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

        // Build context from idea and answers
        let base_context = build_draft_prompt(idea, answers);

        // ── Ideation ────────────────────────────────────────────────────
        let ideation_cache_key =
            super::model::compute_stage_cache_key(FullModeStage::Ideation, &[&base_context]);
        let ideation_artifact = if let Some(cached) =
            self.try_reuse_stage(run, FullModeStage::Ideation, &ideation_cache_key)
        {
            run.last_transition_cached = true;
            let event = journal_event(
                seq,
                Utc::now(),
                RequirementsJournalEventType::StageReused,
                run,
            );
            if let Err(e) = self.store.append_journal_event(base_dir, &run_id, &event) {
                self.fail_run(
                    base_dir,
                    run,
                    seq,
                    &format!("journal append failed for stage_reused (ideation): {e}"),
                )
                .await?;
                return Err(e);
            }
            seq += 1;
            // Read the cached payload to get the artifact text
            let payload_json = self
                .store
                .read_payload(base_dir, &run_id, &cached.payload_id)?;
            let ideation: super::model::IdeationPayload =
                serde_json::from_value(payload_json).map_err(|e| {
                    AppError::InvalidRequirementsState {
                        run_id: run_id.clone(),
                        details: format!("corrupt cached ideation payload: {e}"),
                    }
                })?;
            renderers::render_ideation(&ideation)
        } else {
            run.current_stage = Some(FullModeStage::Ideation);
            run.status_summary = "drafting: ideation".to_owned();
            run.updated_at = Utc::now();
            self.store.write_run(base_dir, &run_id, run)?;

            let prompt = format!(
                "Explore themes and initial scope for the following idea:\n\n{base_context}"
            );
            let bundle = match self
                .invoke_stage(
                    &run_root,
                    RequirementsStageId::Ideation,
                    BackendRole::Planner,
                    &prompt,
                )
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    self.fail_run(base_dir, run, seq, &format!("ideation: {e}"))
                        .await?;
                    return Err(e);
                }
            };

            let artifact_text = bundle.artifact.clone();
            self.commit_full_mode_stage(
                base_dir,
                run,
                FullModeStage::Ideation,
                &bundle,
                &ideation_cache_key,
                &mut seq,
                run.recommended_flow,
            )
            .await?;
            artifact_text
        };

        // ── Research ────────────────────────────────────────────────────
        let research_cache_key = super::model::compute_stage_cache_key(
            FullModeStage::Research,
            &[&base_context, &ideation_artifact],
        );
        let research_artifact = if let Some(cached) =
            self.try_reuse_stage(run, FullModeStage::Research, &research_cache_key)
        {
            run.last_transition_cached = true;
            let event = journal_event(
                seq,
                Utc::now(),
                RequirementsJournalEventType::StageReused,
                run,
            );
            if let Err(e) = self.store.append_journal_event(base_dir, &run_id, &event) {
                self.fail_run(
                    base_dir,
                    run,
                    seq,
                    &format!("journal append failed for stage_reused (research): {e}"),
                )
                .await?;
                return Err(e);
            }
            seq += 1;
            let payload_json = self
                .store
                .read_payload(base_dir, &run_id, &cached.payload_id)?;
            let research: super::model::ResearchPayload =
                serde_json::from_value(payload_json).map_err(|e| {
                    AppError::InvalidRequirementsState {
                        run_id: run_id.clone(),
                        details: format!("corrupt cached research payload: {e}"),
                    }
                })?;
            renderers::render_research(&research)
        } else {
            run.current_stage = Some(FullModeStage::Research);
            run.status_summary = "drafting: research".to_owned();
            run.updated_at = Utc::now();
            self.store.write_run(base_dir, &run_id, run)?;

            let prompt = format!(
                "Research background and technical context for:\n\n{base_context}\n\nIdeation output:\n{ideation_artifact}"
            );
            let bundle = match self
                .invoke_stage(
                    &run_root,
                    RequirementsStageId::Research,
                    BackendRole::Planner,
                    &prompt,
                )
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    self.fail_run(base_dir, run, seq, &format!("research: {e}"))
                        .await?;
                    return Err(e);
                }
            };

            let artifact_text = bundle.artifact.clone();
            self.commit_full_mode_stage(
                base_dir,
                run,
                FullModeStage::Research,
                &bundle,
                &research_cache_key,
                &mut seq,
                run.recommended_flow,
            )
            .await?;
            artifact_text
        };

        // ── Synthesis ───────────────────────────────────────────────────
        let synthesis_cache_key = super::model::compute_stage_cache_key(
            FullModeStage::Synthesis,
            &[&base_context, &ideation_artifact, &research_artifact],
        );
        let synthesis_artifact = if let Some(cached) =
            self.try_reuse_stage(run, FullModeStage::Synthesis, &synthesis_cache_key)
        {
            run.last_transition_cached = true;
            let event = journal_event(
                seq,
                Utc::now(),
                RequirementsJournalEventType::StageReused,
                run,
            );
            if let Err(e) = self.store.append_journal_event(base_dir, &run_id, &event) {
                self.fail_run(
                    base_dir,
                    run,
                    seq,
                    &format!("journal append failed for stage_reused (synthesis): {e}"),
                )
                .await?;
                return Err(e);
            }
            seq += 1;
            let payload_json = self
                .store
                .read_payload(base_dir, &run_id, &cached.payload_id)?;
            let synthesis: super::model::SynthesisPayload =
                serde_json::from_value(payload_json).map_err(|e| {
                    AppError::InvalidRequirementsState {
                        run_id: run_id.clone(),
                        details: format!("corrupt cached synthesis payload: {e}"),
                    }
                })?;
            run.recommended_flow = Some(synthesis.recommended_flow);
            renderers::render_synthesis(&synthesis)
        } else {
            run.current_stage = Some(FullModeStage::Synthesis);
            run.status_summary = "drafting: synthesis".to_owned();
            run.updated_at = Utc::now();
            self.store.write_run(base_dir, &run_id, run)?;

            let prompt = format!(
                "Synthesize requirements from ideation and research:\n\nIdea: {base_context}\n\nIdeation:\n{ideation_artifact}\n\nResearch:\n{research_artifact}"
            );
            let bundle = match self
                .invoke_stage(
                    &run_root,
                    RequirementsStageId::Synthesis,
                    BackendRole::Planner,
                    &prompt,
                )
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    self.fail_run(base_dir, run, seq, &format!("synthesis: {e}"))
                        .await?;
                    return Err(e);
                }
            };

            // Capture recommended_flow before synthesis mutates it, for rollback.
            let pre_synthesis_flow = run.recommended_flow;
            if let RequirementsPayload::Synthesis(ref sp) = bundle.payload {
                run.recommended_flow = Some(sp.recommended_flow);
            }

            let artifact_text = bundle.artifact.clone();
            self.commit_full_mode_stage(
                base_dir,
                run,
                FullModeStage::Synthesis,
                &bundle,
                &synthesis_cache_key,
                &mut seq,
                pre_synthesis_flow,
            )
            .await?;
            artifact_text
        };

        // ── Implementation Spec ─────────────────────────────────────────
        let impl_spec_cache_key = super::model::compute_stage_cache_key(
            FullModeStage::ImplementationSpec,
            &[&synthesis_artifact],
        );
        let impl_spec_artifact = if let Some(cached) = self.try_reuse_stage(
            run,
            FullModeStage::ImplementationSpec,
            &impl_spec_cache_key,
        ) {
            run.last_transition_cached = true;
            let event = journal_event(
                seq,
                Utc::now(),
                RequirementsJournalEventType::StageReused,
                run,
            );
            if let Err(e) = self.store.append_journal_event(base_dir, &run_id, &event) {
                self.fail_run(
                    base_dir,
                    run,
                    seq,
                    &format!("journal append failed for stage_reused (implementation_spec): {e}"),
                )
                .await?;
                return Err(e);
            }
            seq += 1;
            let payload_json = self
                .store
                .read_payload(base_dir, &run_id, &cached.payload_id)?;
            let impl_spec: super::model::ImplementationSpecPayload =
                serde_json::from_value(payload_json).map_err(|e| {
                    AppError::InvalidRequirementsState {
                        run_id: run_id.clone(),
                        details: format!("corrupt cached impl_spec payload: {e}"),
                    }
                })?;
            renderers::render_implementation_spec(&impl_spec)
        } else {
            run.current_stage = Some(FullModeStage::ImplementationSpec);
            run.status_summary = "drafting: implementation spec".to_owned();
            run.updated_at = Utc::now();
            self.store.write_run(base_dir, &run_id, run)?;

            let prompt = format!(
                "Create an implementation specification from the synthesized requirements:\n\n{synthesis_artifact}"
            );
            let bundle = match self
                .invoke_stage(
                    &run_root,
                    RequirementsStageId::ImplementationSpec,
                    BackendRole::Planner,
                    &prompt,
                )
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    self.fail_run(base_dir, run, seq, &format!("implementation_spec: {e}"))
                        .await?;
                    return Err(e);
                }
            };

            let artifact_text = bundle.artifact.clone();
            self.commit_full_mode_stage(
                base_dir,
                run,
                FullModeStage::ImplementationSpec,
                &bundle,
                &impl_spec_cache_key,
                &mut seq,
                run.recommended_flow,
            )
            .await?;
            artifact_text
        };

        // ── Gap Analysis ────────────────────────────────────────────────
        let gap_cache_key = super::model::compute_stage_cache_key(
            FullModeStage::GapAnalysis,
            &[&synthesis_artifact, &impl_spec_artifact],
        );
        let gap_artifact = if let Some(cached) =
            self.try_reuse_stage(run, FullModeStage::GapAnalysis, &gap_cache_key)
        {
            run.last_transition_cached = true;
            let event = journal_event(
                seq,
                Utc::now(),
                RequirementsJournalEventType::StageReused,
                run,
            );
            if let Err(e) = self.store.append_journal_event(base_dir, &run_id, &event) {
                self.fail_run(
                    base_dir,
                    run,
                    seq,
                    &format!("journal append failed for stage_reused (gap_analysis): {e}"),
                )
                .await?;
                return Err(e);
            }
            seq += 1;
            let payload_json = self
                .store
                .read_payload(base_dir, &run_id, &cached.payload_id)?;
            let gap: super::model::GapAnalysisPayload =
                serde_json::from_value(payload_json).map_err(|e| {
                    AppError::InvalidRequirementsState {
                        run_id: run_id.clone(),
                        details: format!("corrupt cached gap_analysis payload: {e}"),
                    }
                })?;
            renderers::render_gap_analysis(&gap)
        } else {
            run.current_stage = Some(FullModeStage::GapAnalysis);
            run.status_summary = "drafting: gap analysis".to_owned();
            run.updated_at = Utc::now();
            self.store.write_run(base_dir, &run_id, run)?;

            let prompt = format!(
                "Analyze gaps between requirements and implementation spec:\n\nSynthesis:\n{synthesis_artifact}\n\nImplementation Spec:\n{impl_spec_artifact}"
            );
            let bundle = match self
                .invoke_stage(
                    &run_root,
                    RequirementsStageId::GapAnalysis,
                    BackendRole::Reviewer,
                    &prompt,
                )
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    self.fail_run(base_dir, run, seq, &format!("gap_analysis: {e}"))
                        .await?;
                    return Err(e);
                }
            };

            let artifact_text = bundle.artifact.clone();
            self.commit_full_mode_stage(
                base_dir,
                run,
                FullModeStage::GapAnalysis,
                &bundle,
                &gap_cache_key,
                &mut seq,
                run.recommended_flow,
            )
            .await?;
            artifact_text
        };

        // ── Validation ──────────────────────────────────────────────────
        let validation_cache_key = super::model::compute_stage_cache_key(
            FullModeStage::Validation,
            &[&synthesis_artifact, &impl_spec_artifact, &gap_artifact],
        );
        if let Some(cached) =
            self.try_reuse_stage(run, FullModeStage::Validation, &validation_cache_key)
        {
            run.last_transition_cached = true;
            let event = journal_event(
                seq,
                Utc::now(),
                RequirementsJournalEventType::StageReused,
                run,
            );
            if let Err(e) = self.store.append_journal_event(base_dir, &run_id, &event) {
                self.fail_run(
                    base_dir,
                    run,
                    seq,
                    &format!("journal append failed for stage_reused (validation): {e}"),
                )
                .await?;
                return Err(e);
            }
            seq += 1;

            // Still need to check cached validation outcome
            let payload_json = self
                .store
                .read_payload(base_dir, &run_id, &cached.payload_id)?;
            let validation: super::model::ValidationPayload =
                serde_json::from_value(payload_json).map_err(|e| {
                    AppError::InvalidRequirementsState {
                        run_id: run_id.clone(),
                        details: format!("corrupt cached validation payload: {e}"),
                    }
                })?;
            match validation.outcome {
                ValidationOutcome::NeedsQuestions => {
                    return self
                        .open_question_round(
                            base_dir,
                            run,
                            idea,
                            &validation.missing_information,
                            seq,
                        )
                        .await;
                }
                ValidationOutcome::Fail => {
                    let msg = format!(
                        "validation failed: {}",
                        validation.blocking_issues.join("; ")
                    );
                    self.fail_run(base_dir, run, seq, &msg).await?;
                    return Err(AppError::InvalidRequirementsState {
                        run_id: run_id.clone(),
                        details: msg,
                    });
                }
                ValidationOutcome::Pass => {}
            }
        } else {
            run.current_stage = Some(FullModeStage::Validation);
            run.status_summary = "drafting: validation".to_owned();
            run.updated_at = Utc::now();
            self.store.write_run(base_dir, &run_id, run)?;

            let prompt = format!(
                "Validate the requirements pipeline output:\n\nSynthesis:\n{synthesis_artifact}\n\nImplementation Spec:\n{impl_spec_artifact}\n\nGap Analysis:\n{gap_artifact}"
            );
            let bundle = match self
                .invoke_stage(
                    &run_root,
                    RequirementsStageId::Validation,
                    BackendRole::Reviewer,
                    &prompt,
                )
                .await
            {
                Ok(b) => b,
                Err(e) => {
                    self.fail_run(base_dir, run, seq, &format!("validation: {e}"))
                        .await?;
                    return Err(e);
                }
            };

            // Check validation outcome before committing
            let validation_outcome = match &bundle.payload {
                RequirementsPayload::Validation(v) => v.outcome,
                _ => unreachable!(),
            };
            let missing_info = match &bundle.payload {
                RequirementsPayload::Validation(v) => v.missing_information.clone(),
                _ => vec![],
            };

            self.commit_full_mode_stage(
                base_dir,
                run,
                FullModeStage::Validation,
                &bundle,
                &validation_cache_key,
                &mut seq,
                run.recommended_flow,
            )
            .await?;

            match validation_outcome {
                ValidationOutcome::NeedsQuestions => {
                    return self
                        .open_question_round(base_dir, run, idea, &missing_info, seq)
                        .await;
                }
                ValidationOutcome::Fail => {
                    let RequirementsPayload::Validation(ref v) = bundle.payload else {
                        unreachable!()
                    };
                    let msg = format!(
                        "validation failed: {}",
                        v.blocking_issues.join("; ")
                    );
                    self.fail_run(base_dir, run, seq, &msg).await?;
                    return Err(AppError::InvalidRequirementsState {
                        run_id: run_id.clone(),
                        details: msg,
                    });
                }
                ValidationOutcome::Pass => {}
            }
        };

        // ── Project Seed ────────────────────────────────────────────────
        self.generate_and_commit_seed(base_dir, run, &synthesis_artifact, &[], seq)
            .await
    }

    /// Open a question round when validation finds missing information.
    /// Preserves ideation and research; invalidates synthesis and downstream.
    async fn open_question_round(
        &self,
        base_dir: &Path,
        run: &mut RequirementsRun,
        idea: &str,
        missing_info: &[String],
        mut seq: u64,
    ) -> AppResult<()> {
        let run_id = run.run_id.clone();
        let run_root = requirements_run_root(base_dir, &run_id);

        // Generate questions about the missing information
        let question_prompt = format!(
            "Generate clarifying questions about the following missing information for the idea:\n\n{idea}\n\nMissing information:\n{}",
            missing_info.iter().map(|m| format!("- {m}")).collect::<Vec<_>>().join("\n")
        );

        let bundle = match self
            .invoke_stage(
                &run_root,
                RequirementsStageId::QuestionSet,
                BackendRole::Planner,
                &question_prompt,
            )
            .await
        {
            Ok(b) => b,
            Err(e) => {
                self.fail_run(
                    base_dir,
                    run,
                    seq,
                    &format!("question generation for validation: {e}"),
                )
                .await?;
                return Err(e);
            }
        };

        let RequirementsPayload::QuestionSet(ref qs) = bundle.payload else {
            unreachable!();
        };

        let question_round = next_question_round(run);
        let payload_id = format!("{run_id}-qs-{question_round}");
        let artifact_id = format!("{run_id}-qs-art-{question_round}");

        let payload_json = serde_json::to_value(qs)?;
        if let Err(e) = self.store.write_payload_artifact_pair_atomic(
            base_dir,
            &run_id,
            &payload_id,
            &payload_json,
            &artifact_id,
            &bundle.artifact,
        ) {
            self.fail_run(
                base_dir,
                run,
                seq,
                &format!("question persistence: {e}"),
            )
            .await?;
            return Err(e);
        }

        // Snapshot pre-question state for rollback on journal failure.
        let pre_question_committed_stages = run.committed_stages.clone();
        let pre_question_latest_qs_id = run.latest_question_set_id.clone();

        // Invalidate synthesis and downstream committed stages before pausing.
        // Ideation and research survive; synthesis onwards must be rerun after answers.
        for stage in FullModeStage::question_round_invalidated() {
            run.committed_stages.remove(stage.as_str());
        }

        // Do NOT set run.question_round here — it tracks completed rounds,
        // and this round is only completed when `answer()` increments it.
        run.latest_question_set_id = Some(payload_id.clone());
        run.updated_at = Utc::now();
        self.store.write_run(base_dir, &run_id, run)?;

        let qs_event = journal_event(
            seq,
            Utc::now(),
            RequirementsJournalEventType::QuestionRoundOpened,
            run,
        );
        if let Err(e) = self
            .store
            .append_journal_event(base_dir, &run_id, &qs_event)
        {
            // Restore pre-question state: committed_stages and latest_question_set_id.
            run.committed_stages = pre_question_committed_stages;
            run.latest_question_set_id = pre_question_latest_qs_id;
            // Remove the non-durable question-set payload/artifact pair.
            let _ = self.store.remove_payload_artifact_pair(
                base_dir,
                &run_id,
                &payload_id,
                &artifact_id,
            );
            self.fail_run(
                base_dir,
                run,
                seq,
                &format!("journal append failed for question_round_opened: {e}"),
            )
            .await?;
            return Err(e);
        }
        seq += 1;

        let question_count = qs.questions.len() as u32;
        run.pending_question_count = Some(question_count);

        if qs.questions.is_empty() {
            // No specific questions — but validation said we need info.
            // Fail the run since we can't generate useful questions.
            self.fail_run(
                base_dir,
                run,
                seq,
                "validation requires missing information but no questions could be generated",
            )
            .await?;
            return Err(AppError::InvalidRequirementsState {
                run_id: run_id.clone(),
                details: "validation requires missing information but no questions could be generated".to_owned(),
            });
        }

        // Generate answers template
        let template = generate_answers_template(qs);
        if let Err(e) = self.store.write_answers_toml(base_dir, &run_id, &template) {
            self.fail_run(
                base_dir,
                run,
                seq,
                &format!("could not write answers template: {e}"),
            )
            .await?;
            return Err(e);
        }

        run.status = RequirementsStatus::AwaitingAnswers;
        run.status_summary = format!(
            "awaiting answers: {} question(s), round {}",
            question_count, question_round
        );
        run.updated_at = Utc::now();
        self.store.write_run(base_dir, &run_id, run)?;

        let answers_path = self.store.answers_toml_path(base_dir, &run_id);
        println!(
            "Questions generated. Edit answers at:\n  {}\n\nThen run:\n  ralph-burning requirements answer {}",
            answers_path.display(),
            run_id
        );

        Ok(())
    }

    /// Commit a full-mode stage payload/artifact pair and record in run state.
    ///
    /// `prior_recommended_flow` is the value of `run.recommended_flow` before
    /// the pipeline block that calls this method made any mutations. If the
    /// journal append fails, this value is restored so `run.json` reflects
    /// only the last durable stage.
    async fn commit_full_mode_stage(
        &self,
        base_dir: &Path,
        run: &mut RequirementsRun,
        stage: FullModeStage,
        bundle: &RequirementsValidatedBundle,
        cache_key: &str,
        seq: &mut u64,
        prior_recommended_flow: Option<FlowPreset>,
    ) -> AppResult<()> {
        let run_id = run.run_id.clone();
        let round = effective_question_round(run);
        let payload_id = format!("{run_id}-{}-{round}", stage.as_str());
        let artifact_id = format!("{run_id}-{}-art-{round}", stage.as_str());

        let payload_json = match &bundle.payload {
            RequirementsPayload::Ideation(p) => serde_json::to_value(p)?,
            RequirementsPayload::Research(p) => serde_json::to_value(p)?,
            RequirementsPayload::Synthesis(p) => serde_json::to_value(p)?,
            RequirementsPayload::ImplementationSpec(p) => serde_json::to_value(p)?,
            RequirementsPayload::GapAnalysis(p) => serde_json::to_value(p)?,
            RequirementsPayload::Validation(p) => serde_json::to_value(p)?,
            _ => serde_json::to_value(&bundle.artifact)?,
        };

        if let Err(e) = self.store.write_payload_artifact_pair_atomic(
            base_dir,
            &run_id,
            &payload_id,
            &payload_json,
            &artifact_id,
            &bundle.artifact,
        ) {
            self.fail_run(
                base_dir,
                run,
                *seq,
                &format!("{} persistence: {e}", stage.as_str()),
            )
            .await?;
            return Err(e);
        }

        // Snapshot pre-stage run state for rollback on journal failure.
        // Note: the pipeline code may have already advanced current_stage
        // before calling this method, so we derive the prior current_stage
        // from committed_stages (which still reflects the last durable
        // boundary) rather than from run.current_stage.
        let prior_current_stage = FullModeStage::pipeline_order()
            .iter()
            .rev()
            .find(|s| run.committed_stages.contains_key(s.as_str()))
            .copied();

        run.committed_stages.insert(
            stage.as_str().to_owned(),
            CommittedStageEntry {
                payload_id: payload_id.clone(),
                artifact_id: artifact_id.clone(),
                cache_key: Some(cache_key.to_owned()),
            },
        );
        run.current_stage = Some(stage);
        run.last_transition_cached = false;
        run.updated_at = Utc::now();
        self.store.write_run(base_dir, &run_id, run)?;

        let event = journal_event(
            *seq,
            Utc::now(),
            RequirementsJournalEventType::StageCompleted,
            run,
        );
        if let Err(e) = self.store.append_journal_event(base_dir, &run_id, &event) {
            // Roll back the full stage commitment including run state fields.
            let _ = self.store.remove_payload_artifact_pair(
                base_dir,
                &run_id,
                &payload_id,
                &artifact_id,
            );
            run.committed_stages.remove(stage.as_str());
            run.current_stage = prior_current_stage;
            run.recommended_flow = prior_recommended_flow;
            self.fail_run(
                base_dir,
                run,
                *seq,
                &format!(
                    "journal append failed for stage_completed ({}): {e}",
                    stage.as_str()
                ),
            )
            .await?;
            return Err(e);
        }
        *seq += 1;
        Ok(())
    }

    /// Try to reuse a cached stage if the cache key matches.
    fn try_reuse_stage(
        &self,
        run: &RequirementsRun,
        stage: FullModeStage,
        expected_cache_key: &str,
    ) -> Option<CommittedStageEntry> {
        let entry = run.committed_stages.get(stage.as_str())?;
        if entry.cache_key.as_deref() == Some(expected_cache_key) {
            Some(entry.clone())
        } else {
            None
        }
    }

    // ── Quick-mode revision loop ────────────────────────────────────────

    async fn run_quick_mode_pipeline(
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

        let draft_prompt = build_draft_prompt(idea, answers);
        let mut last_draft_artifact: String;

        // Initial draft
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

        // Commit initial draft
        let question_round = effective_question_round(run);
        let mut revision = 0u32;
        {
            let RequirementsPayload::Draft(ref draft_payload) = draft_bundle.payload else {
                unreachable!();
            };
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
        }
        last_draft_artifact = draft_bundle.artifact.clone();

        // Revision loop: review → possibly revise → review again
        loop {
            // Review the current draft
            let review_prompt = format!(
                "Review the following requirements draft:\n\n{last_draft_artifact}"
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

            // Commit review
            let review_payload_id = format!("{run_id}-review-{question_round}-r{revision}");
            let review_artifact_id = format!("{run_id}-review-art-{question_round}-r{revision}");
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

            let prior_review_id = run.latest_review_id.clone();
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
                let _ = self.store.remove_payload_artifact_pair(
                    base_dir,
                    &run_id,
                    &review_payload_id,
                    &review_artifact_id,
                );
                run.latest_review_id = prior_review_id;
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

            match review_payload.outcome {
                RequirementsReviewOutcome::Approved
                | RequirementsReviewOutcome::ConditionallyApproved => {
                    // Approval — proceed to seed generation
                    let follow_ups = if review_payload.outcome
                        == RequirementsReviewOutcome::ConditionallyApproved
                    {
                        review_payload.follow_ups.clone()
                    } else {
                        Vec::new()
                    };

                    return self
                        .generate_and_commit_seed(
                            base_dir,
                            run,
                            &last_draft_artifact,
                            &follow_ups,
                            seq,
                        )
                        .await;
                }
                RequirementsReviewOutcome::RequestChanges => {
                    // Request changes — revise the draft
                    revision += 1;
                    run.quick_revision_count = revision;

                    if revision > MAX_QUICK_REVISIONS {
                        let msg = format!(
                            "quick-mode revision limit reached ({MAX_QUICK_REVISIONS} revisions)"
                        );
                        self.fail_run(base_dir, run, seq, &msg).await?;
                        return Err(AppError::InvalidRequirementsState {
                            run_id: run_id.clone(),
                            details: msg,
                        });
                    }

                    // Journal the revision request — must succeed to maintain
                    // the rollback invariant for Slice 1 transitions.
                    let rev_event = journal_event(
                        seq,
                        Utc::now(),
                        RequirementsJournalEventType::RevisionRequested,
                        run,
                    );
                    if let Err(e) = self
                        .store
                        .append_journal_event(base_dir, &run_id, &rev_event)
                    {
                        self.fail_run(
                            base_dir,
                            run,
                            seq,
                            &format!("journal append failed for revision_requested: {e}"),
                        )
                        .await?;
                        return Err(e);
                    }
                    seq += 1;

                    // Generate revised draft with feedback
                    let revision_prompt = format!(
                        "Revise the following requirements draft based on review feedback:\n\nDraft:\n{last_draft_artifact}\n\nReview findings:\n{}\n\nRevision notes: address the findings above.",
                        review_payload.findings.join("\n- ")
                    );

                    let revised = match self
                        .invoke_stage(
                            &run_root,
                            RequirementsStageId::RequirementsDraft,
                            BackendRole::Planner,
                            &revision_prompt,
                        )
                        .await
                    {
                        Ok(b) => b,
                        Err(e) => {
                            self.fail_run(
                                base_dir,
                                run,
                                seq,
                                &format!("draft revision {revision}: {e}"),
                            )
                            .await?;
                            return Err(e);
                        }
                    };

                    // Commit revised draft
                    let RequirementsPayload::Draft(ref revised_payload) = revised.payload else {
                        unreachable!();
                    };
                    let revised_payload_id =
                        format!("{run_id}-draft-{question_round}-r{revision}");
                    let revised_artifact_id =
                        format!("{run_id}-draft-art-{question_round}-r{revision}");
                    let revised_json = serde_json::to_value(revised_payload)?;

                    if let Err(e) = self.store.write_payload_artifact_pair_atomic(
                        base_dir,
                        &run_id,
                        &revised_payload_id,
                        &revised_json,
                        &revised_artifact_id,
                        &revised.artifact,
                    ) {
                        self.fail_run(
                            base_dir,
                            run,
                            seq,
                            &format!("revised draft persistence: {e}"),
                        )
                        .await?;
                        return Err(e);
                    }

                    let prior_draft_id = run.latest_draft_id.clone();
                    let prior_recommended_flow = run.recommended_flow;
                    run.latest_draft_id = Some(revised_payload_id.clone());
                    run.recommended_flow = Some(revised_payload.recommended_flow);
                    run.status_summary =
                        format!("drafting: reviewing requirements (revision {revision})");
                    run.updated_at = Utc::now();
                    self.store.write_run(base_dir, &run_id, run)?;

                    let rev_complete = journal_event(
                        seq,
                        Utc::now(),
                        RequirementsJournalEventType::RevisionCompleted,
                        run,
                    );
                    if let Err(e) = self
                        .store
                        .append_journal_event(base_dir, &run_id, &rev_complete)
                    {
                        // Roll back the revised draft
                        let _ = self.store.remove_payload_artifact_pair(
                            base_dir,
                            &run_id,
                            &revised_payload_id,
                            &revised_artifact_id,
                        );
                        run.latest_draft_id = prior_draft_id;
                        run.recommended_flow = prior_recommended_flow;
                        self.fail_run(
                            base_dir,
                            run,
                            seq,
                            &format!("journal append failed for revision_completed: {e}"),
                        )
                        .await?;
                        return Err(e);
                    }
                    seq += 1;

                    last_draft_artifact = revised.artifact.clone();
                    let _ = &revised; // consumed
                    // Continue loop for re-review
                }
                RequirementsReviewOutcome::Rejected => {
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
        }
    }

    // ── Seed generation (shared by full and quick modes) ────────────────

    async fn generate_and_commit_seed(
        &self,
        base_dir: &Path,
        run: &mut RequirementsRun,
        requirements_artifact: &str,
        follow_ups: &[String],
        mut seq: u64,
    ) -> AppResult<()> {
        let run_id = run.run_id.clone();
        let run_root = requirements_run_root(base_dir, &run_id);

        let seed_prompt = format!(
            "Generate a project seed from the following requirements:\n\n{}\n\nFollow-ups: {}",
            requirements_artifact,
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

        // Extract seed payload and set version/source metadata
        let mut seed_payload = match seed_bundle.payload {
            RequirementsPayload::Seed(p) => p,
            _ => unreachable!(),
        };

        // Set version and source metadata
        seed_payload.version = PROJECT_SEED_VERSION;
        seed_payload.source = Some(SeedSourceMetadata {
            mode: run.mode,
            run_id: run.run_id.clone(),
            question_rounds: run.question_round,
            quick_revisions: if run.mode == super::model::RequirementsMode::Quick {
                Some(run.quick_revision_count)
            } else {
                None
            },
        });

        if !follow_ups.is_empty() {
            for fu in follow_ups {
                if !seed_payload.follow_ups.contains(fu) {
                    seed_payload.follow_ups.push(fu.clone());
                }
            }
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

        // Re-render seed artifact with version/source metadata
        let seed_artifact = renderers::render_project_seed(&seed_payload);

        let seed_payload_id = format!("{run_id}-seed-1");
        let seed_artifact_id = format!("{run_id}-seed-art-1");
        let seed_json = serde_json::to_value(&seed_payload)?;

        // Write seed files BEFORE committing history
        let project_json = serde_json::to_value(&seed_payload)?;
        if let Err(e) =
            self.store
                .write_seed_pair(base_dir, &run_id, &project_json, &seed_payload.prompt_body)
        {
            self.fail_run(base_dir, run, seq, &format!("seed file write: {e}"))
                .await?;
            let _ = self.store.remove_seed_pair(base_dir, &run_id);
            return Err(AppError::SeedPersistenceFailed {
                run_id: run_id.clone(),
                details: e.to_string(),
            });
        }

        // Now commit the seed payload/artifact to history
        if let Err(e) = self.store.write_payload_artifact_pair_atomic(
            base_dir,
            &run_id,
            &seed_payload_id,
            &seed_json,
            &seed_artifact_id,
            &seed_artifact,
        ) {
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

        // Record seed in full-mode committed stages if in full mode
        if run.mode == super::model::RequirementsMode::Draft {
            run.committed_stages.insert(
                FullModeStage::ProjectSeed.as_str().to_owned(),
                CommittedStageEntry {
                    payload_id: seed_payload_id.clone(),
                    artifact_id: seed_artifact_id.clone(),
                    cache_key: None,
                },
            );
            run.current_stage = Some(FullModeStage::ProjectSeed);
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
            let _ = self.store.remove_payload_artifact_pair(
                base_dir,
                &run_id,
                &seed_payload_id,
                &seed_artifact_id,
            );
            // Restore canonical state to the last successful pre-seed boundary.
            run.latest_seed_id = None;
            run.committed_stages.remove(FullModeStage::ProjectSeed.as_str());
            if run.mode == super::model::RequirementsMode::Draft {
                // Reset current_stage to validation (last successful stage before seed).
                run.current_stage = Some(FullModeStage::Validation);
            }
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

    // ── Stage invocation ────────────────────────────────────────────────

    async fn invoke_stage(
        &self,
        run_root: &Path,
        stage_id: RequirementsStageId,
        role: BackendRole,
        prompt: &str,
    ) -> AppResult<RequirementsValidatedBundle> {
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

        let contract = match stage_id {
            RequirementsStageId::QuestionSet => RequirementsContract::question_set(),
            RequirementsStageId::RequirementsDraft => RequirementsContract::draft(),
            RequirementsStageId::RequirementsReview => RequirementsContract::review(),
            RequirementsStageId::ProjectSeed => RequirementsContract::seed(),
            RequirementsStageId::Ideation => RequirementsContract::ideation(),
            RequirementsStageId::Research => RequirementsContract::research(),
            RequirementsStageId::Synthesis => RequirementsContract::synthesis(),
            RequirementsStageId::ImplementationSpec => RequirementsContract::implementation_spec(),
            RequirementsStageId::GapAnalysis => RequirementsContract::gap_analysis(),
            RequirementsStageId::Validation => RequirementsContract::validation(),
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
        let _ = self
            .store
            .append_journal_event(base_dir, &run.run_id, &event);
        Ok(())
    }

    fn answers_already_durably_stored(&self, base_dir: &Path, run_id: &str) -> AppResult<bool> {
        let journal = self.store.read_journal(base_dir, run_id)?;
        if journal
            .iter()
            .any(|e| e.event_type == RequirementsJournalEventType::AnswersSubmitted)
        {
            return Ok(true);
        }

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
pub fn read_requirements_run_status(
    store: &dyn RequirementsStorePort,
    base_dir: &Path,
    run_id: &str,
) -> AppResult<RequirementsRun> {
    store.read_run(base_dir, run_id)
}

/// Check if a requirements run has completed and produced a seed.
pub fn is_requirements_run_complete(
    store: &dyn RequirementsStorePort,
    base_dir: &Path,
    run_id: &str,
) -> AppResult<bool> {
    let run = store.read_run(base_dir, run_id)?;
    Ok(run.status == RequirementsStatus::Completed)
}

/// Extract seed handoff data from a completed requirements run.
/// Validates seed version and fails clearly on unsupported versions.
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

    // Validate seed version
    if !SUPPORTED_SEED_VERSIONS.contains(&seed.version) {
        return Err(AppError::RequirementsHandoffFailed {
            task_id: run_id.to_owned(),
            details: format!(
                "unsupported seed version {}: supported versions are {:?}",
                seed.version, SUPPORTED_SEED_VERSIONS
            ),
        });
    }

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
        let mut first_prompt_line = true;
        for line in q.prompt.lines() {
            if first_prompt_line {
                out.push_str(&format!("# {}{}\n", line, required));
                first_prompt_line = false;
            } else {
                out.push_str(&format!("# {}\n", line));
            }
        }
        if first_prompt_line {
            out.push_str(&format!("# {}\n", required.trim()));
        }

        if !q.rationale.is_empty() {
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
        let escaped = toml_escape_basic_string(default_value);
        out.push_str(&format!("{} = \"{}\"\n\n", q.id, escaped));
    }

    out
}

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

fn parse_and_validate_answers<Q: RequirementsStorePort>(
    answers_raw: &str,
    base_dir: &Path,
    run_id: &str,
    store: &Q,
) -> AppResult<PersistedAnswers> {
    let table: toml::Table =
        toml::from_str(answers_raw).map_err(|e| AppError::AnswerValidationFailed {
            run_id: run_id.to_owned(),
            details: format!("invalid TOML: {e}"),
        })?;

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

    let valid_ids: HashSet<&str> = qs.questions.iter().map(|q| q.id.as_str()).collect();

    for key in table.keys() {
        if !valid_ids.contains(key.as_str()) {
            return Err(AppError::AnswerValidationFailed {
                run_id: run_id.to_owned(),
                details: format!("unknown question ID: '{key}'"),
            });
        }
    }

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
                            details: format!(
                                "required question '{}' has an empty answer",
                                q.id
                            ),
                        });
                    }
                }
            }
        }
    }

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
