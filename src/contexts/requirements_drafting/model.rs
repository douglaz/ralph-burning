#![forbid(unsafe_code)]

//! Requirements run state model.
//!
//! Persisted at `.ralph-burning/requirements/<run-id>/run.json`.
//! Defines the requirements run lifecycle, journal events, and
//! stage identifiers independent of the workflow `StageId` enum.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::contexts::milestone_record::bundle::MilestoneBundle;
use crate::shared::domain::FlowPreset;

/// Current version of the ProjectSeed schema.
pub const PROJECT_SEED_VERSION: u32 = 2;

/// Supported ProjectSeed versions for `extract_seed_handoff`.
pub const SUPPORTED_SEED_VERSIONS: &[u32] = &[1, 2];

/// Requirements run mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RequirementsMode {
    Draft,
    Quick,
    Milestone,
}

impl std::fmt::Display for RequirementsMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Draft => f.write_str("draft"),
            Self::Quick => f.write_str("quick"),
            Self::Milestone => f.write_str("milestone"),
        }
    }
}

/// Terminal output emitted by a requirements run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RequirementsOutputKind {
    ProjectSeed,
    MilestoneBundle,
}

impl std::fmt::Display for RequirementsOutputKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProjectSeed => f.write_str("project_seed"),
            Self::MilestoneBundle => f.write_str("milestone_bundle"),
        }
    }
}

fn default_output_kind() -> RequirementsOutputKind {
    RequirementsOutputKind::ProjectSeed
}

/// Requirements run status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_milestone_bundle_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub milestone_bundle: Option<MilestoneBundle>,
    #[serde(default = "default_output_kind")]
    pub output_kind: RequirementsOutputKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_question_count: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended_flow: Option<FlowPreset>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status_summary: String,

    // ── Slice 1: stage-aware fields ─────────────────────────────────────
    /// Current full-mode stage (None for quick mode or pre-pipeline).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_stage: Option<FullModeStage>,

    /// Latest committed payload ID per full-mode stage.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub committed_stages: BTreeMap<String, CommittedStageEntry>,

    /// Quick-mode revision count (how many draft→review cycles have run).
    #[serde(default)]
    pub quick_revision_count: u32,

    /// Whether the last stage transition was a cache reuse (not fresh execution).
    #[serde(default)]
    pub last_transition_cached: bool,
}

/// A committed stage entry tracking payload/artifact IDs and cache key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedStageEntry {
    pub payload_id: String,
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_key: Option<String>,
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
            latest_milestone_bundle_id: None,
            milestone_bundle: None,
            output_kind: RequirementsOutputKind::ProjectSeed,
            pending_question_count: None,
            recommended_flow: None,
            created_at: now,
            updated_at: now,
            status_summary: "drafting: question generation".to_owned(),
            current_stage: None,
            committed_stages: BTreeMap::new(),
            quick_revision_count: 0,
            last_transition_cached: false,
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
            latest_milestone_bundle_id: None,
            milestone_bundle: None,
            output_kind: RequirementsOutputKind::ProjectSeed,
            pending_question_count: None,
            recommended_flow: None,
            created_at: now,
            updated_at: now,
            status_summary: "drafting: generating requirements".to_owned(),
            current_stage: None,
            committed_stages: BTreeMap::new(),
            quick_revision_count: 0,
            last_transition_cached: false,
        }
    }

    /// Create a new requirements run in milestone mode.
    pub fn new_milestone(run_id: String, idea: String, now: DateTime<Utc>) -> Self {
        Self {
            run_id,
            idea,
            mode: RequirementsMode::Milestone,
            status: RequirementsStatus::Drafting,
            question_round: 0,
            latest_question_set_id: None,
            latest_draft_id: None,
            latest_review_id: None,
            latest_seed_id: None,
            latest_milestone_bundle_id: None,
            milestone_bundle: None,
            output_kind: RequirementsOutputKind::MilestoneBundle,
            pending_question_count: None,
            recommended_flow: None,
            created_at: now,
            updated_at: now,
            status_summary: "drafting: milestone planning".to_owned(),
            current_stage: None,
            committed_stages: BTreeMap::new(),
            quick_revision_count: 0,
            last_transition_cached: false,
        }
    }

    /// Whether this run is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            RequirementsStatus::Completed | RequirementsStatus::Failed
        )
    }

    /// Whether this run uses the full multi-stage pipeline.
    pub fn uses_full_mode_pipeline(&self) -> bool {
        !matches!(self.mode, RequirementsMode::Quick)
    }
}

// ── Full-mode stages ────────────────────────────────────────────────────────

/// Full-mode pipeline stages, ordered by execution sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FullModeStage {
    Ideation,
    Research,
    Synthesis,
    ImplementationSpec,
    GapAnalysis,
    Validation,
    ProjectSeed,
    MilestoneBundle,
}

impl FullModeStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ideation => "ideation",
            Self::Research => "research",
            Self::Synthesis => "synthesis",
            Self::ImplementationSpec => "implementation_spec",
            Self::GapAnalysis => "gap_analysis",
            Self::Validation => "validation",
            Self::ProjectSeed => "project_seed",
            Self::MilestoneBundle => "milestone_bundle",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::Ideation => "Ideation",
            Self::Research => "Research",
            Self::Synthesis => "Synthesis",
            Self::ImplementationSpec => "Implementation Spec",
            Self::GapAnalysis => "Gap Analysis",
            Self::Validation => "Validation",
            Self::ProjectSeed => "Project Seed",
            Self::MilestoneBundle => "Milestone Bundle",
        }
    }

    /// Full-mode pipeline order. Returns all stages in execution sequence.
    pub fn pipeline_order() -> &'static [FullModeStage] {
        &[
            Self::Ideation,
            Self::Research,
            Self::Synthesis,
            Self::ImplementationSpec,
            Self::GapAnalysis,
            Self::Validation,
            Self::ProjectSeed,
            Self::MilestoneBundle,
        ]
    }

    /// Returns the stages that are invalidated when this stage changes.
    /// All downstream stages (after this one) are invalidated.
    pub fn downstream_stages(self) -> &'static [FullModeStage] {
        match self {
            Self::Ideation => &[
                Self::Research,
                Self::Synthesis,
                Self::ImplementationSpec,
                Self::GapAnalysis,
                Self::Validation,
                Self::ProjectSeed,
                Self::MilestoneBundle,
            ],
            Self::Research => &[
                Self::Synthesis,
                Self::ImplementationSpec,
                Self::GapAnalysis,
                Self::Validation,
                Self::ProjectSeed,
                Self::MilestoneBundle,
            ],
            Self::Synthesis => &[
                Self::ImplementationSpec,
                Self::GapAnalysis,
                Self::Validation,
                Self::ProjectSeed,
                Self::MilestoneBundle,
            ],
            Self::ImplementationSpec => &[
                Self::GapAnalysis,
                Self::Validation,
                Self::ProjectSeed,
                Self::MilestoneBundle,
            ],
            Self::GapAnalysis => &[Self::Validation, Self::ProjectSeed, Self::MilestoneBundle],
            Self::Validation => &[Self::ProjectSeed, Self::MilestoneBundle],
            Self::ProjectSeed => &[],
            Self::MilestoneBundle => &[],
        }
    }

    /// Stages invalidated when a question round triggers re-synthesis.
    /// Ideation and research survive; synthesis and downstream are invalidated.
    pub fn question_round_invalidated() -> &'static [FullModeStage] {
        &[
            Self::Synthesis,
            Self::ImplementationSpec,
            Self::GapAnalysis,
            Self::Validation,
            Self::ProjectSeed,
            Self::MilestoneBundle,
        ]
    }
}

impl std::fmt::Display for FullModeStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Requirements stage identifiers — separate from workflow `StageId`.
/// Retained for backward compatibility with existing quick-mode contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RequirementsStageId {
    QuestionSet,
    RequirementsDraft,
    RequirementsReview,
    ProjectSeed,
    MilestoneBundle,
    // Full-mode stages
    Ideation,
    Research,
    Synthesis,
    ImplementationSpec,
    GapAnalysis,
    Validation,
}

impl RequirementsStageId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::QuestionSet => "question_set",
            Self::RequirementsDraft => "requirements_draft",
            Self::RequirementsReview => "requirements_review",
            Self::ProjectSeed => "project_seed",
            Self::MilestoneBundle => "milestone_bundle",
            Self::Ideation => "ideation",
            Self::Research => "research",
            Self::Synthesis => "synthesis",
            Self::ImplementationSpec => "implementation_spec",
            Self::GapAnalysis => "gap_analysis",
            Self::Validation => "validation",
        }
    }

    pub fn display_name(self) -> &'static str {
        match self {
            Self::QuestionSet => "Question Set",
            Self::RequirementsDraft => "Requirements Draft",
            Self::RequirementsReview => "Requirements Review",
            Self::ProjectSeed => "Project Seed",
            Self::MilestoneBundle => "Milestone Bundle",
            Self::Ideation => "Ideation",
            Self::Research => "Research",
            Self::Synthesis => "Synthesis",
            Self::ImplementationSpec => "Implementation Spec",
            Self::GapAnalysis => "Gap Analysis",
            Self::Validation => "Validation",
        }
    }

    /// Convert a FullModeStage to the corresponding RequirementsStageId.
    pub fn from_full_mode(stage: FullModeStage) -> Self {
        match stage {
            FullModeStage::Ideation => Self::Ideation,
            FullModeStage::Research => Self::Research,
            FullModeStage::Synthesis => Self::Synthesis,
            FullModeStage::ImplementationSpec => Self::ImplementationSpec,
            FullModeStage::GapAnalysis => Self::GapAnalysis,
            FullModeStage::Validation => Self::Validation,
            FullModeStage::ProjectSeed => Self::ProjectSeed,
            FullModeStage::MilestoneBundle => Self::MilestoneBundle,
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
    AutoMilestoneMaterializationFailed,
    RunCompleted,
    RunFailed,
    // Slice 1: full-mode stage events
    StageCompleted,
    StageReused,
    QuestionRoundOpened,
    // Slice 1: quick-mode revision events
    RevisionRequested,
    RevisionCompleted,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Question {
    pub id: String,
    pub prompt: String,
    pub rationale: String,
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggested_default: Option<String>,
}

/// Question set payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct QuestionSetPayload {
    pub questions: Vec<Question>,
}

/// Requirements draft payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RequirementsReviewPayload {
    pub outcome: RequirementsReviewOutcome,
    pub evidence: Vec<String>,
    pub findings: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub follow_ups: Vec<String>,
}

/// Structured revision feedback for quick-mode revision loop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RevisionFeedback {
    pub outcome: RequirementsReviewOutcome,
    pub revision_notes: Vec<String>,
    pub findings: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub follow_ups: Vec<String>,
}

// ── Full-mode stage payloads ────────────────────────────────────────────────

/// Ideation stage payload — brainstorming and idea exploration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct IdeationPayload {
    pub themes: Vec<String>,
    pub key_concepts: Vec<String>,
    pub initial_scope: String,
    pub open_questions: Vec<String>,
}

/// Research stage payload — background analysis and context gathering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ResearchPayload {
    pub findings: Vec<ResearchFinding>,
    pub constraints_discovered: Vec<String>,
    pub prior_art: Vec<String>,
    pub technical_context: String,
}

/// A single research finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ResearchFinding {
    pub area: String,
    pub summary: String,
    pub relevance: String,
}

/// Synthesis stage payload — consolidating ideation and research into requirements.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SynthesisPayload {
    pub problem_summary: String,
    pub goals: Vec<String>,
    pub non_goals: Vec<String>,
    pub constraints: Vec<String>,
    pub acceptance_criteria: Vec<String>,
    pub risks_or_open_questions: Vec<String>,
    pub recommended_flow: FlowPreset,
}

/// Implementation spec stage payload — detailed implementation blueprint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ImplementationSpecPayload {
    pub architecture_overview: String,
    pub components: Vec<ComponentSpec>,
    pub integration_points: Vec<String>,
    pub migration_notes: Vec<String>,
}

/// A component in the implementation spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ComponentSpec {
    pub name: String,
    pub responsibility: String,
    pub interfaces: Vec<String>,
}

/// Gap analysis stage payload — identifying gaps between synthesis and implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct GapAnalysisPayload {
    pub gaps: Vec<IdentifiedGap>,
    pub coverage_assessment: String,
    pub blocking_gaps: Vec<String>,
}

/// A single identified gap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct IdentifiedGap {
    pub area: String,
    pub description: String,
    pub severity: GapSeverity,
    pub suggested_resolution: String,
}

/// Gap severity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GapSeverity {
    Low,
    Medium,
    High,
    Blocking,
}

impl std::fmt::Display for GapSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Low => f.write_str("low"),
            Self::Medium => f.write_str("medium"),
            Self::High => f.write_str("high"),
            Self::Blocking => f.write_str("blocking"),
        }
    }
}

/// Validation stage payload — final review before seed generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ValidationPayload {
    pub outcome: ValidationOutcome,
    pub evidence: Vec<String>,
    pub blocking_issues: Vec<String>,
    pub missing_information: Vec<String>,
}

/// Validation outcome — determines whether the pipeline continues or pauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ValidationOutcome {
    /// All clear — proceed to project seed.
    Pass,
    /// Missing information requires a question round before proceeding.
    NeedsQuestions,
    /// Validation failed — run should fail.
    Fail,
}

impl std::fmt::Display for ValidationOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pass => f.write_str("pass"),
            Self::NeedsQuestions => f.write_str("needs_questions"),
            Self::Fail => f.write_str("fail"),
        }
    }
}

/// Project seed payload — versioned for handoff stability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProjectSeedPayload {
    /// Seed schema version. Current version is PROJECT_SEED_VERSION.
    #[serde(default = "default_seed_version")]
    pub version: u32,
    pub project_id: String,
    pub project_name: String,
    pub flow: FlowPreset,
    pub prompt_body: String,
    pub handoff_summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub follow_ups: Vec<String>,
    /// Source metadata: which mode and run produced this seed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<SeedSourceMetadata>,
}

fn default_seed_version() -> u32 {
    1
}

/// Metadata about the source of a project seed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SeedSourceMetadata {
    pub mode: RequirementsMode,
    pub run_id: String,
    pub question_rounds: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quick_revisions: Option<u32>,
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

// ── Cache key computation ───────────────────────────────────────────────────

/// Compute a deterministic cache key from stage input and upstream dependency outputs.
/// Uses `DefaultHasher` (SipHash) for fast, collision-resistant hashing within a
/// single process run. Not suitable for cross-process or persistent cache identity.
pub fn compute_stage_cache_key(stage: FullModeStage, inputs: &[&str]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    stage.as_str().hash(&mut hasher);
    for input in inputs {
        input.hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}
