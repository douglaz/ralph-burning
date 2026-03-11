use std::fs;
use std::path::Path;

use toml_edit::{value, DocumentMut};

use crate::shared::domain::WorkspaceConfig;
use crate::shared::error::AppResult;

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

        fs::write(workspace_root.join("workspace.toml"), workspace_config)?;
        Ok(())
    }

    pub fn read_to_string(path: &Path) -> AppResult<String> {
        Ok(fs::read_to_string(path)?)
    }

    pub fn render_workspace_config(config: &WorkspaceConfig) -> String {
        let mut document = DocumentMut::new();
        document["version"] = value(i64::from(config.version));
        document["created_at"] = value(config.created_at.to_rfc3339());
        document.to_string()
    }
}
