use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::shared::domain::WorkspaceConfig;
use crate::shared::error::{AppError, AppResult};

const ACTIVE_PROJECT_FILE: &str = "active-project";

pub struct FileSystem;

impl FileSystem {
    pub fn create_workspace(
        workspace_root: &Path,
        workspace_config: &str,
        required_directories: &[&str],
    ) -> AppResult<()> {
        fs::create_dir_all(workspace_root)?;

        for directory in required_directories {
            fs::create_dir_all(workspace_root.join(directory))?;
        }

        Self::write_atomic(&workspace_root.join("workspace.toml"), workspace_config)?;
        Ok(())
    }

    pub fn read_to_string(path: &Path) -> AppResult<String> {
        Ok(fs::read_to_string(path)?)
    }

    pub fn write_atomic(path: &Path, contents: &str) -> AppResult<()> {
        let parent = path.parent().ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("path '{}' has no parent directory", path.display()),
            ))
        })?;
        fs::create_dir_all(parent)?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let temp_path = parent.join(format!(
            ".{}.{}.{}.tmp",
            path.file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("workspace"),
            std::process::id(),
            timestamp
        ));

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        drop(file);

        if let Err(error) = fs::rename(&temp_path, path) {
            let _ = fs::remove_file(&temp_path);
            return Err(error.into());
        }

        Ok(())
    }

    pub fn render_workspace_config(config: &WorkspaceConfig) -> AppResult<String> {
        Ok(toml::to_string_pretty(config)?)
    }

    pub fn read_active_project(workspace_root: &Path) -> AppResult<Option<String>> {
        let path = workspace_root.join(ACTIVE_PROJECT_FILE);
        match fs::read_to_string(path) {
            Ok(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(trimmed.to_owned()))
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub fn write_active_project(workspace_root: &Path, project_id: &str) -> AppResult<()> {
        Self::write_atomic(&workspace_root.join(ACTIVE_PROJECT_FILE), project_id)
    }

    pub fn open_editor(path: &Path) -> AppResult<()> {
        let editor = env::var("EDITOR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                env::var("VISUAL")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            })
            .unwrap_or_else(|| "vi".to_owned());

        let mut parts = editor.split_whitespace();
        let program = parts.next().ok_or_else(|| AppError::EditorFailed {
            editor: editor.clone(),
            details: ": empty editor command".to_owned(),
        })?;
        let status = Command::new(program).args(parts).arg(path).status()?;

        if status.success() {
            Ok(())
        } else {
            let details = match status.code() {
                Some(code) => format!(" with exit code {code}"),
                None => " after being terminated by a signal".to_owned(),
            };
            Err(AppError::EditorFailed { editor, details })
        }
    }
}
