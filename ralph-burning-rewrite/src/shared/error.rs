use std::path::PathBuf;

use thiserror::Error;

use crate::shared::domain::{FailureClass, StageId};

pub type AppResult<T> = Result<T, AppError>;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("workspace already initialized at {path}")]
    AlreadyInitialized { path: PathBuf },
    #[error(
        "cannot initialize workspace because {path} already exists without a workspace configuration"
    )]
    WorkspaceConflict { path: PathBuf },
    #[error("invalid {field}: must be greater than zero")]
    InvalidStageCursorField { field: &'static str },
    #[error("invalid identifier '{value}': identifiers must be non-empty single path segments")]
    InvalidIdentifier { value: String },
    #[error(
        "unknown flow preset '{flow_id}'. supported presets: standard, quick_dev, docs_change, ci_improvement"
    )]
    InvalidFlowPreset { flow_id: String },
    #[error("unsupported workspace version {version}; supported version is {supported}")]
    UnsupportedWorkspaceVersion { version: u32, supported: u32 },
    #[error(
        "unknown config key '{key}'. supported keys: prompt_review.enabled, default_flow, default_backend, default_model"
    )]
    UnknownConfigKey { key: String },
    #[error("invalid value '{value}' for config key '{key}': {reason}")]
    InvalidConfigValue {
        key: String,
        value: String,
        reason: String,
    },
    #[error("no active project selected; run `ralph-burning project select <id>`")]
    NoActiveProject,
    #[error(
        "project '{project_id}' was not found under .ralph-burning/projects/ or is missing project.toml"
    )]
    ProjectNotFound { project_id: String },
    #[error("editor '{editor}' failed{details}")]
    EditorFailed { editor: String, details: String },
    #[error("{command} is not yet implemented")]
    NotYetImplemented { command: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    TomlDeserialize(#[from] toml::de::Error),
    #[error(transparent)]
    TomlSerialize(#[from] toml::ser::Error),
    #[error(transparent)]
    TomlEdit(#[from] toml_edit::TomlError),
}

/// Errors specific to stage contract evaluation.
///
/// Each variant maps to a distinct [`FailureClass`] for retry/terminal policy.
#[derive(Debug, Error)]
pub enum ContractError {
    #[error("schema validation failed for stage '{stage_id}': {details}")]
    SchemaValidation { stage_id: StageId, details: String },

    #[error("domain validation failed for stage '{stage_id}': {details}")]
    DomainValidation { stage_id: StageId, details: String },

    #[error("rendering failed for stage '{stage_id}': {details}")]
    RenderError { stage_id: StageId, details: String },

    #[error("non-passing QA/review outcome for stage '{stage_id}': {outcome}")]
    QaReviewOutcome { stage_id: StageId, outcome: String },
}

impl ContractError {
    pub fn failure_class(&self) -> FailureClass {
        match self {
            Self::SchemaValidation { .. } => FailureClass::SchemaValidationFailure,
            Self::DomainValidation { .. } | Self::RenderError { .. } => {
                FailureClass::DomainValidationFailure
            }
            Self::QaReviewOutcome { .. } => FailureClass::QaReviewOutcomeFailure,
        }
    }

    pub fn stage_id(&self) -> StageId {
        match self {
            Self::SchemaValidation { stage_id, .. }
            | Self::DomainValidation { stage_id, .. }
            | Self::RenderError { stage_id, .. }
            | Self::QaReviewOutcome { stage_id, .. } => *stage_id,
        }
    }
}
