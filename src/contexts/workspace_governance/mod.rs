pub mod config;
pub mod template_catalog;

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use crate::adapters::fs::FileSystem;
use crate::shared::domain::{ProjectId, WorkspaceConfig, CURRENT_WORKSPACE_VERSION};
use crate::shared::error::{AppError, AppResult};

pub use config::{
    CliBackendOverrides, ConfigEntry, ConfigValue, ConfigValueSource, EffectiveConfig,
    DEFAULT_FLOW_PRESET, DEFAULT_PROMPT_REVIEW_ENABLED,
};

pub const WORKSPACE_DIR: &str = ".ralph-burning";
pub const WORKSPACE_CONFIG_FILE: &str = "workspace.toml";
pub const ACTIVE_PROJECT_FILE: &str = "active-project";
pub const PROJECTS_DIR: &str = "projects";
pub const PROJECT_CONFIG_FILE: &str = "project.toml";
pub const REQUIRED_WORKSPACE_DIRECTORIES: &[&str] =
    &["projects", "requirements", "daemon/tasks", "daemon/leases"];

#[derive(Debug, Clone, PartialEq)]
pub struct WorkspaceInitialization {
    pub workspace_root: PathBuf,
    pub config: WorkspaceConfig,
}

pub fn initialize_workspace(
    base_dir: &Path,
    created_at: DateTime<Utc>,
) -> AppResult<WorkspaceInitialization> {
    let workspace_root = workspace_root(base_dir);
    let config_path = workspace_root.join(WORKSPACE_CONFIG_FILE);

    if config_path.exists() {
        return Err(AppError::AlreadyInitialized {
            path: workspace_root,
        });
    }

    if workspace_root.exists() {
        return Err(AppError::WorkspaceConflict {
            path: workspace_root,
        });
    }

    let config = WorkspaceConfig::new(created_at);
    let rendered = FileSystem::render_workspace_config(&config)?;
    FileSystem::create_workspace(&workspace_root, &rendered, REQUIRED_WORKSPACE_DIRECTORIES)?;

    Ok(WorkspaceInitialization {
        workspace_root,
        config,
    })
}

pub fn load_workspace_config(base_dir: &Path) -> AppResult<WorkspaceConfig> {
    let raw = FileSystem::read_to_string(&workspace_config_path(base_dir))?;
    parse_workspace_config(&raw)
}

pub fn ensure_supported_workspace_version(config: &WorkspaceConfig) -> AppResult<()> {
    if config.version != CURRENT_WORKSPACE_VERSION {
        return Err(AppError::UnsupportedWorkspaceVersion {
            version: config.version,
            supported: CURRENT_WORKSPACE_VERSION,
        });
    }

    Ok(())
}

pub fn resolve_active_project(base_dir: &Path) -> AppResult<ProjectId> {
    let _ = EffectiveConfig::load(base_dir)?;
    let active_project = FileSystem::read_active_project(&workspace_root(base_dir))?
        .ok_or(AppError::NoActiveProject)?;
    let project_id = ProjectId::new(active_project)?;
    validate_project_exists(base_dir, &project_id)?;
    Ok(project_id)
}

pub fn set_active_project(base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
    let _ = EffectiveConfig::load(base_dir)?;
    validate_project_exists(base_dir, project_id)?;
    FileSystem::write_active_project(&workspace_root(base_dir), project_id.as_str())
}

pub(crate) fn parse_workspace_config(raw: &str) -> AppResult<WorkspaceConfig> {
    let config: WorkspaceConfig = toml::from_str(raw)?;
    ensure_supported_workspace_version(&config)?;
    Ok(config)
}

pub(crate) fn workspace_root(base_dir: &Path) -> PathBuf {
    base_dir.join(WORKSPACE_DIR)
}

pub(crate) fn workspace_config_path(base_dir: &Path) -> PathBuf {
    workspace_root(base_dir).join(WORKSPACE_CONFIG_FILE)
}

fn validate_project_exists(base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
    let project_root = workspace_root(base_dir)
        .join(PROJECTS_DIR)
        .join(project_id.as_str());
    if !project_root.is_dir() {
        return Err(AppError::ProjectNotFound {
            project_id: project_id.to_string(),
        });
    }
    let config_path = project_root.join(PROJECT_CONFIG_FILE);
    if !config_path.is_file() {
        return Err(AppError::CorruptRecord {
            file: format!("projects/{}/project.toml", project_id),
            details: "project directory exists but canonical project.toml is missing".to_owned(),
        });
    }
    // Deserialize as the canonical ProjectRecord to validate both TOML syntax
    // and structural completeness. This catches corruption early so that
    // commands resolving the active project fail fast instead of silently
    // operating on corrupt canonical state (e.g. missing required fields).
    let raw = std::fs::read_to_string(&config_path).map_err(|e| AppError::CorruptRecord {
        file: format!("projects/{}/project.toml", project_id),
        details: format!("cannot read project.toml: {}", e),
    })?;
    let _: crate::contexts::project_run_record::model::ProjectRecord = toml::from_str(&raw)
        .map_err(|e| AppError::CorruptRecord {
            file: format!("projects/{}/project.toml", project_id),
            details: format!("project.toml has invalid canonical structure: {}", e),
        })?;
    Ok(())
}
