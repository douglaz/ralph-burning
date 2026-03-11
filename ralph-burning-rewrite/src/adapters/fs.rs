use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::contexts::project_run_record::journal;
use crate::contexts::project_run_record::model::{
    ArtifactRecord, JournalEvent, PayloadRecord, ProjectRecord, RunSnapshot,
    RuntimeLogEntry, SessionStore,
};
use crate::contexts::project_run_record::service::{
    ActiveProjectPort, ArtifactStorePort, JournalStorePort, ProjectStorePort,
    RuntimeLogStorePort, RunSnapshotPort,
};
use crate::shared::domain::{ProjectId, WorkspaceConfig};
use crate::shared::error::{AppError, AppResult};

const ACTIVE_PROJECT_FILE: &str = "active-project";
const WORKSPACE_DIR: &str = ".ralph-burning";
const PROJECTS_DIR: &str = "projects";
const PROJECT_CONFIG_FILE: &str = "project.toml";
const RUN_FILE: &str = "run.json";
const JOURNAL_FILE: &str = "journal.ndjson";
const SESSIONS_FILE: &str = "sessions.json";
const PROMPT_FILE: &str = "prompt.md";

/// Required subdirectories inside a project.
const PROJECT_SUBDIRS: &[&str] = &[
    "history/payloads",
    "history/artifacts",
    "runtime/logs",
    "runtime/backend",
    "runtime/temp",
    "amendments",
    "rollback",
];

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

    /// Compute a simple hash of content for prompt integrity tracking.
    pub fn prompt_hash(contents: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        contents.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    // ── Helpers for project filesystem layout ──

    fn workspace_root_path(base_dir: &Path) -> PathBuf {
        base_dir.join(WORKSPACE_DIR)
    }

    fn project_root(base_dir: &Path, project_id: &ProjectId) -> PathBuf {
        Self::workspace_root_path(base_dir)
            .join(PROJECTS_DIR)
            .join(project_id.as_str())
    }

    fn append_line(path: &Path, line: &str) -> AppResult<()> {
        let parent = path.parent().ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("path '{}' has no parent directory", path.display()),
            ))
        })?;
        fs::create_dir_all(parent)?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        writeln!(file, "{}", line)?;
        file.sync_all()?;
        Ok(())
    }
}

/// Filesystem-backed implementation of `ProjectStorePort`.
pub struct FsProjectStore;

impl ProjectStorePort for FsProjectStore {
    fn project_exists(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<bool> {
        let project_root = FileSystem::project_root(base_dir, project_id);
        Ok(project_root.is_dir() && project_root.join(PROJECT_CONFIG_FILE).is_file())
    }

    fn read_project_record(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<ProjectRecord> {
        let path = FileSystem::project_root(base_dir, project_id).join(PROJECT_CONFIG_FILE);
        let raw = fs::read_to_string(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                AppError::ProjectNotFound {
                    project_id: project_id.to_string(),
                }
            } else {
                e.into()
            }
        })?;
        toml::from_str(&raw).map_err(|e| AppError::CorruptRecord {
            file: format!("projects/{}/project.toml", project_id),
            details: e.to_string(),
        })
    }

    fn list_project_ids(&self, base_dir: &Path) -> AppResult<Vec<ProjectId>> {
        let projects_dir = FileSystem::workspace_root_path(base_dir).join(PROJECTS_DIR);
        if !projects_dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut ids = Vec::new();
        for entry in fs::read_dir(&projects_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Only include projects that have a valid project.toml
            if entry.path().join(PROJECT_CONFIG_FILE).is_file() {
                if let Ok(pid) = ProjectId::new(name_str.as_ref()) {
                    ids.push(pid);
                }
            }
        }

        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        Ok(ids)
    }

    fn delete_project(&self, base_dir: &Path, project_id: &ProjectId) -> AppResult<()> {
        let project_root = FileSystem::project_root(base_dir, project_id);
        if !project_root.is_dir() {
            return Err(AppError::ProjectNotFound {
                project_id: project_id.to_string(),
            });
        }
        fs::remove_dir_all(&project_root)?;
        Ok(())
    }

    fn create_project_atomic(
        &self,
        base_dir: &Path,
        record: &ProjectRecord,
        prompt_contents: &str,
        run_snapshot: &RunSnapshot,
        initial_journal_line: &str,
        sessions: &SessionStore,
    ) -> AppResult<()> {
        let project_root = FileSystem::project_root(base_dir, &record.id);

        // Stage into a temporary directory alongside the final location
        let staging_name = format!(".{}.staging.{}", record.id, std::process::id());
        let staging_root = project_root
            .parent()
            .expect("project root has parent")
            .join(&staging_name);

        // Clean up any leftover staging dir
        if staging_root.exists() {
            fs::remove_dir_all(&staging_root)?;
        }

        // Create staging directory and all subdirs
        fs::create_dir_all(&staging_root)?;
        for subdir in PROJECT_SUBDIRS {
            fs::create_dir_all(staging_root.join(subdir))?;
        }

        // Write all canonical files into staging
        let write_result = (|| -> AppResult<()> {
            // project.toml
            let project_toml = toml::to_string_pretty(record).map_err(|e| {
                AppError::Io(std::io::Error::other(e.to_string()))
            })?;
            fs::write(staging_root.join(PROJECT_CONFIG_FILE), project_toml)?;

            // prompt.md
            fs::write(staging_root.join(PROMPT_FILE), prompt_contents)?;

            // run.json
            let run_json = serde_json::to_string_pretty(run_snapshot)?;
            fs::write(staging_root.join(RUN_FILE), run_json)?;

            // journal.ndjson (with initial event)
            let mut journal_content = initial_journal_line.to_owned();
            journal_content.push('\n');
            fs::write(staging_root.join(JOURNAL_FILE), journal_content)?;

            // sessions.json
            let sessions_json = serde_json::to_string_pretty(sessions)?;
            fs::write(staging_root.join(SESSIONS_FILE), sessions_json)?;

            Ok(())
        })();

        if let Err(e) = write_result {
            // Clean up staging on failure
            let _ = fs::remove_dir_all(&staging_root);
            return Err(e);
        }

        // Atomic rename from staging to final location
        if let Err(e) = fs::rename(&staging_root, &project_root) {
            let _ = fs::remove_dir_all(&staging_root);
            return Err(e.into());
        }

        Ok(())
    }
}

/// Filesystem-backed implementation of `JournalStorePort`.
pub struct FsJournalStore;

impl JournalStorePort for FsJournalStore {
    fn read_journal(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<JournalEvent>> {
        let path = FileSystem::project_root(base_dir, project_id).join(JOURNAL_FILE);
        match fs::read_to_string(&path) {
            Ok(contents) => journal::parse_journal(&contents),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    fn append_event(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
        line: &str,
    ) -> AppResult<()> {
        let path = FileSystem::project_root(base_dir, project_id).join(JOURNAL_FILE);
        FileSystem::append_line(&path, line)
    }
}

/// Filesystem-backed implementation of `ArtifactStorePort`.
pub struct FsArtifactStore;

impl ArtifactStorePort for FsArtifactStore {
    fn list_payloads(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<PayloadRecord>> {
        let dir = FileSystem::project_root(base_dir, project_id).join("history/payloads");
        if !dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                let raw = fs::read_to_string(&path)?;
                let record: PayloadRecord = serde_json::from_str(&raw).map_err(|e| {
                    AppError::CorruptRecord {
                        file: format!(
                            "history/payloads/{}",
                            path.file_name()
                                .unwrap_or_default()
                                .to_string_lossy()
                        ),
                        details: e.to_string(),
                    }
                })?;
                records.push(record);
            }
        }

        records.sort_by_key(|r| r.created_at);
        Ok(records)
    }

    fn list_artifacts(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<ArtifactRecord>> {
        let dir = FileSystem::project_root(base_dir, project_id).join("history/artifacts");
        if !dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut records = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                let raw = fs::read_to_string(&path)?;
                let record: ArtifactRecord =
                    serde_json::from_str(&raw).map_err(|e| AppError::CorruptRecord {
                        file: format!(
                            "history/artifacts/{}",
                            path.file_name()
                                .unwrap_or_default()
                                .to_string_lossy()
                        ),
                        details: e.to_string(),
                    })?;
                records.push(record);
            }
        }

        records.sort_by_key(|r| r.created_at);
        Ok(records)
    }
}

/// Filesystem-backed implementation of `RuntimeLogStorePort`.
pub struct FsRuntimeLogStore;

impl RuntimeLogStorePort for FsRuntimeLogStore {
    fn read_runtime_logs(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<Vec<RuntimeLogEntry>> {
        let dir = FileSystem::project_root(base_dir, project_id).join("runtime/logs");
        if !dir.is_dir() {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();
        let mut paths: Vec<_> = fs::read_dir(&dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|ext| ext == "ndjson"))
            .collect();
        paths.sort();

        for path in paths {
            let contents = fs::read_to_string(&path)?;
            for line in contents.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                if let Ok(entry) = serde_json::from_str::<RuntimeLogEntry>(trimmed) {
                    entries.push(entry);
                }
            }
        }

        Ok(entries)
    }
}

/// Filesystem-backed implementation of `RunSnapshotPort`.
pub struct FsRunSnapshotStore;

impl RunSnapshotPort for FsRunSnapshotStore {
    fn read_run_snapshot(
        &self,
        base_dir: &Path,
        project_id: &ProjectId,
    ) -> AppResult<RunSnapshot> {
        let path = FileSystem::project_root(base_dir, project_id).join(RUN_FILE);
        match fs::read_to_string(&path) {
            Ok(raw) => serde_json::from_str(&raw).map_err(|e| AppError::CorruptRecord {
                file: format!("projects/{}/run.json", project_id),
                details: e.to_string(),
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(RunSnapshot::initial()),
            Err(e) => Err(e.into()),
        }
    }
}

/// Filesystem-backed implementation of `ActiveProjectPort`.
pub struct FsActiveProjectStore;

impl ActiveProjectPort for FsActiveProjectStore {
    fn read_active_project_id(&self, base_dir: &Path) -> AppResult<Option<String>> {
        FileSystem::read_active_project(&FileSystem::workspace_root_path(base_dir))
    }

    fn clear_active_project(&self, base_dir: &Path) -> AppResult<()> {
        let path = FileSystem::workspace_root_path(base_dir).join(ACTIVE_PROJECT_FILE);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}
