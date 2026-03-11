use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};

use crate::adapters::fs::FileSystem;
use crate::shared::domain::{WorkspaceConfig, CURRENT_WORKSPACE_VERSION};
use crate::shared::error::{AppError, AppResult};

pub const WORKSPACE_DIR: &str = ".ralph-burning";
pub const WORKSPACE_CONFIG_FILE: &str = "workspace.toml";
pub const REQUIRED_WORKSPACE_DIRECTORIES: &[&str] =
    &["projects", "requirements", "daemon/tasks", "daemon/leases"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceInitialization {
    pub workspace_root: PathBuf,
    pub config: WorkspaceConfig,
}

pub fn initialize_workspace(
    base_dir: &Path,
    created_at: DateTime<Utc>,
) -> AppResult<WorkspaceInitialization> {
    let workspace_root = base_dir.join(WORKSPACE_DIR);
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
    let rendered = FileSystem::render_workspace_config(&config);
    FileSystem::create_workspace(&workspace_root, &rendered, REQUIRED_WORKSPACE_DIRECTORIES)?;

    Ok(WorkspaceInitialization {
        workspace_root,
        config,
    })
}

pub fn load_workspace_config(base_dir: &Path) -> AppResult<WorkspaceConfig> {
    let config_path = base_dir.join(WORKSPACE_DIR).join(WORKSPACE_CONFIG_FILE);
    let raw = FileSystem::read_to_string(&config_path)?;
    let config: WorkspaceConfig = toml::from_str(&raw)?;
    ensure_supported_workspace_version(&config)?;
    Ok(config)
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
