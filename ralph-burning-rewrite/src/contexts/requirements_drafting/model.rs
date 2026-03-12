#![forbid(unsafe_code)]

//! Requirements run state model.
//!
//! Persisted at `.ralph-burning/requirements/<run-id>/run.json`.
//! Defines the requirements run lifecycle, journal events, and
//! stage identifiers independent of the workflow `StageId` enum.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::shared::domain::FlowPreset;

/// Requirements run mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequirementsMode {
    Draft,
    Quick,
}

impl std::fmt::Display for RequirementsMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Draft => f.write_str("draft"),
            Self::Quick => f.write_str("quick"),
        }
    }
}

/// Requirements run status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequirementsStatus {
    Drafting,
    AwaitingAnswers,
    Completed,
    Failed,
}

impl std::fmt::Display for RequirementsStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Drafting => f.write_str("drafting"),
            Self::AwaitingAnswers => f.write_str("awaiting_answers"),
            Self::Completed => f.write_str("completed"),
            Self::Failed => f.write_str("failed"),
        }
    }
}

/// Canonical requirements run state persisted in `run.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequirementsRun {
    pub run_id: String,
    pub idea: String,
    pub mode: RequirementsMode,
    pub status: RequirementsStatus,
    pub question_round: u32,
    pub latest_question_set_id: Option<String>,
    pub latest_draft_id: Option<String>,
    pub latest_review_id: Option<String>,
    pub latest_seed_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status_summary: String,
}

impl RequirementsRun {
    /// Create a new requirements run in draft mode.
    pub fn new_draft(run_id: String, idea: String, now: DateTime<Utc>) -> Self {
        Self {
            run_id,
            idea,
            mode: RequirementsMode::Draft,
            status: RequirementsStatus::Drafting,
            question_round: 0,
            latest_question_set_id: None,
            latest_draft_id: None,
            latest_review_id: None,
            latest_seed_id: None,
            created_at: now,
            updated_at: now,
            status_summary: "drafting: question generation".to_owned(),
        }
    }

    /// Create a new requirements run in quick mode.
    pub fn new_quick(run_id: String, idea: String, now: DateTime<Utc>) -> Self {
        Self {
            run_id,
            idea,
            mode: RequirementsMode::Quick,
            status: RequirementsStatus::Drafting,
            question_round: 0,
            latest_question_set_id: None,
            latest_draft_id: None,
            latest_review_id: None,
            latest_seed_id: None,
            created_at: now,
            updated_at: now,
            status_summary: "drafting: generating requirements".to_owned(),
        }
    }

    /// Whether this run is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            RequirementsStatus::Completed | RequirementsStatus::Failed
        )
    }
}

/// Requirements stage identifiers — separate from workflow `StageId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequirementsStageId {
    QuestionSet,
    RequirementsDraft,
    RequirementsReview,
    ProjectSeed,
}

impl RequirementsStageId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::QuestionSet => "question_set",
            Self::RequirementsDraft => "requirements_draft",
            Self::RequirementsReview => "requirements_review",
            Self::ProjectSeed => "project_seed",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::QuestionSet => "Question Set",
            Self::RequirementsDraft => "Requirements Draft",
            Self::RequirementsReview => "Requirements Review",
            Self::ProjectSeed => "Project Seed",
        }
    }
}

impl std::fmt::Display for RequirementsStageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Journal events specific to requirements runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequirementsJournalEventType {
    RunCreated,
    QuestionsGenerated,
    AnswersSubmitted,
    DraftGenerated,
    ReviewCompleted,
    SeedGenerated,
    RunCompleted,
    RunFailed,
}

/// A requirements journal event stored in `journal.ndjson`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequirementsJournalEvent {
    pub sequence: u64,
    pub timestamp: DateTime<Utc>,
    pub event_type: RequirementsJournalEventType,
    pub details: serde_json::Value,
}

/// Requirements review outcome — distinct from workflow `ReviewOutcome`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequirementsReviewOutcome {
    Approved,
    ConditionallyApproved,
    RequestChanges,
    Rejected,
}

impl RequirementsReviewOutcome {
    /// Whether this outcome allows completion (with or without follow-ups).
    pub fn allows_completion(self) -> bool {
        matches!(self, Self::Approved | Self::ConditionallyApproved)
    }
}

impl std::fmt::Display for RequirementsReviewOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Approved => f.write_str("approved"),
            Self::ConditionallyApproved => f.write_str("conditionally_approved"),
            Self::RequestChanges => f.write_str("request_changes"),
            Self::Rejected => f.write_str("rejected"),
        }
    }
}

// ── Structured payloads ─────────────────────────────────────────────────────

/// A single question in a question set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Question {
    pub id: String,
    pub prompt: String,
    pub rationale: String,
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggested_default: Option<String>,
}

/// Question set payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuestionSetPayload {
    pub questions: Vec<Question>,
}

/// Requirements draft payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequirementsDraftPayload {
    pub problem_summary: String,
    pub goals: Vec<String>,
    pub non_goals: Vec<String>,
    pub constraints: Vec<String>,
    pub acceptance_criteria: Vec<String>,
    pub risks_or_open_questions: Vec<String>,
    pub recommended_flow: FlowPreset,
}

/// Requirements review payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequirementsReviewPayload {
    pub outcome: RequirementsReviewOutcome,
    pub evidence: Vec<String>,
    pub findings: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub follow_ups: Vec<String>,
}

/// Project seed payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectSeedPayload {
    pub project_id: String,
    pub project_name: String,
    pub flow: FlowPreset,
    pub prompt_body: String,
    pub handoff_summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub follow_ups: Vec<String>,
}

/// Persisted answers (from `answers.toml`/`answers.json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedAnswers {
    pub answers: Vec<AnswerEntry>,
}

/// A single answer keyed by question ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnswerEntry {
    pub question_id: String,
    pub answer: String,
}
