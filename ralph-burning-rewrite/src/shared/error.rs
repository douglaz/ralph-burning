use std::path::PathBuf;

use thiserror::Error;

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
    #[error("invalid identifier '{value}': identifiers must not be empty")]
    InvalidIdentifier { value: String },
    #[error(
        "unknown flow preset '{flow_id}'. supported presets: standard, quick_dev, docs_change, ci_improvement"
    )]
    InvalidFlowPreset { flow_id: String },
    #[error("unsupported workspace version {version}; supported version is {supported}")]
    UnsupportedWorkspaceVersion { version: u32, supported: u32 },
    #[error("{command} is not yet implemented")]
    NotYetImplemented { command: String },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    TomlDeserialize(#[from] toml::de::Error),
    #[error(transparent)]
    TomlSerialize(#[from] toml::ser::Error),
}
